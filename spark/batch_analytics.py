"""
Spark BATCH processing job: the system's analytics (DIT manual objective 7's
batch half — spark/streaming_job.py is the continuous streaming half). Reads
images, predictions and users from PostgreSQL with spark.read.jdbc(...),
computes every figure the dashboard shows with Spark DataFrame operations,
and writes them to the analytics tables (storage/database.py
ANALYTICS_TABLES), which the dashboard reads (spark/analytics_reader.py):

    analytics_summary                 headline counts, last 24 h, pipeline timings
    analytics_disease_distribution    photos per disease
    analytics_risk_distribution       photos per risk level
    analytics_high_risk_by_location   HIGH-risk share per municipality
    analytics_disease_trend           photos per municipality, day and disease
    analytics_disease_daily           photos per day and disease
    batch_location_daily_summary      per municipality and upload day

and, for the scalability test page, one set per test run from the CNN
results kept in scale_test_items (storage/database.py AnalyticsScaleTest*):

    analytics_scale_test_disease_by_location     test photos per municipality and disease
    analytics_scale_test_disease_distribution    test photos per disease
    analytics_scale_test_high_risk_by_location   HIGH-risk share per municipality

Definitions (the same as the dashboard always used): scalability-test images
are left out; each image counts once, with its latest prediction (newest
predicted_at, ties broken by prediction_id); a blank location is "Unknown
location"; days are Timor-Leste days (LOCAL_TZ_OFFSET_HOURS).

Requires PostgreSQL (DATABASE_URL=postgresql+psycopg2://user:pass@host/db):
Spark's JDBC reader needs a real JDBC driver, and this project doesn't ship
a SQLite JDBC jar. The PostgreSQL driver is downloaded from Maven Central the
first time (spark.jars.packages), like the Kafka connector.

Run:
    python -m spark.batch_analytics            # compute once
    python -m spark.batch_analytics --watch    # keep the tables up to date
                                               # (run_all.py's EWS-SparkAnalytics)
"""

import argparse
import re
import sys
import time

from config.settings import (
    DATABASE_URL, LOCAL_TZ_OFFSET_HOURS, ANALYTICS_CHECK_SECONDS, ANALYTICS_MAX_AGE_SECONDS,
)

UNKNOWN_LOCATION = "Unknown location"
SCALE_TEST_SOURCE = "scalability_test"      # storage/database.SCALE_TEST_SOURCE
_POSTGRES_URL_RE = re.compile(r"postgresql\+psycopg2://(?:([^:]+):([^@]*)@)?([^/]+)/(.+)")


def jdbc_url_and_properties() -> tuple:
    """Converts the SQLAlchemy DATABASE_URL (config/settings.py) into what
    spark.read.jdbc/write.jdbc need: a jdbc:postgresql://... URL plus a
    {driver, user, password} properties dict. Raises a clear error (not a
    silent SQLite fallback) if DATABASE_URL isn't PostgreSQL."""
    match = _POSTGRES_URL_RE.match(DATABASE_URL)
    if not match:
        raise RuntimeError(
            "spark/batch_analytics.py requires PostgreSQL "
            "(DATABASE_URL=postgresql+psycopg2://user:pass@host:port/db) — "
            f"got {DATABASE_URL!r}. Spark's JDBC reader needs a real JDBC "
            "driver, and this project doesn't ship a SQLite JDBC jar."
        )
    user, password, host_port, dbname = match.groups()
    jdbc_url = f"jdbc:postgresql://{host_port}/{dbname}"
    properties = {"driver": "org.postgresql.Driver"}
    if user:
        properties["user"] = user
    if password is not None:
        properties["password"] = password
    return jdbc_url, properties


def build_spark():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("ImageSystem-CropDiseaseEWS-Analytics")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        # Timestamps are instants; days are taken in Timor-Leste Time
        # explicitly (_local_day), whatever this machine's time zone is.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ----------------------------------------------------------------- inputs
# Only the columns the analytics need; never users' e-mails or password
# hashes. Enum columns are read as text.
_IMAGES_SQL = f"""(SELECT image_id, user_id, location, CAST(status AS VARCHAR(32)) AS status,
                          captured_at, uploaded_at, ingested_at, processed_at
                   FROM images WHERE source <> '{SCALE_TEST_SOURCE}') AS i"""
_PREDICTIONS_SQL = """(SELECT prediction_id, image_id, predicted_class, risk_score,
                              CAST(risk_level AS VARCHAR(16)) AS risk_level,
                              predicted_at, processing_latency_ms
                       FROM predictions) AS p"""
_USERS_SQL = "(SELECT user_id, is_deleted FROM users) AS u"
_SCALE_TEST_ITEMS_SQL = """(SELECT run_id, status, predicted_class, risk_level, location
                            FROM scale_test_items) AS s"""


def read_inputs(spark, jdbc_url, properties) -> tuple:
    return tuple(spark.read.jdbc(jdbc_url, sql, properties=properties)
                 for sql in (_IMAGES_SQL, _PREDICTIONS_SQL, _USERS_SQL, _SCALE_TEST_ITEMS_SQL))


# ----------------------------------------------------------- computation
def _local_day(column):
    """The Timor-Leste calendar day of a timestamp column."""
    from pyspark.sql.functions import col, expr, to_date
    return to_date(col(column) + expr(f"INTERVAL {int(LOCAL_TZ_OFFSET_HOURS)} HOURS"))


def _location():
    """Location cleaned like the streaming job's (case, spaces), so a row
    that never got Spark's cleaned value (registered but not finished, or
    from before the cleaning rule) groups with the rest; blanks are grouped
    under UNKNOWN_LOCATION."""
    from pyspark.sql.functions import coalesce, col, initcap, lit, regexp_replace, trim, when
    loc = initcap(regexp_replace(trim(coalesce(col("location"), lit(""))), r"\s+", " "))
    return when(loc == "", lit(UNKNOWN_LOCATION)).otherwise(loc)


def photos_with_latest_prediction(images, predictions):
    """One row per image, with its latest prediction's columns (null when it
    has none): newest predicted_at, ties broken by the larger prediction_id."""
    from pyspark.sql import Window
    from pyspark.sql.functions import col, row_number
    newest = Window.partitionBy("image_id").orderBy(col("predicted_at").desc_nulls_last(),
                                                    col("prediction_id").desc())
    latest = (predictions.withColumn("_rn", row_number().over(newest))
              .filter(col("_rn") == 1).drop("_rn", "prediction_id"))
    return images.join(latest, "image_id", "left")


def _count_if(cond):
    """How many rows `cond` is true for (SUM of 1/0), as a long."""
    from pyspark.sql import functions as F
    return F.sum(F.when(cond, 1).otherwise(0)).cast("long")


def _count_photos(df, *keys):
    """Rows per `keys`, in a column named photos."""
    from pyspark.sql import functions as F
    return df.groupBy(*keys).agg(F.count("*").alias("photos"))


def _high_risk_by(df, *keys):
    """Per `keys`: analysed photos (with a risk level) as total, the HIGH
    ones as high, and high_pct (0-100, one decimal)."""
    from pyspark.sql import functions as F
    return (df.filter(F.col("risk_level").isNotNull()).groupBy(*keys)
            .agg(_count_if(F.col("risk_level") == "HIGH").alias("high"), F.count("*").alias("total"))
            .withColumn("high_pct", F.bround(F.col("high") / F.col("total") * 100, 1)))


def _stamped(tables: dict) -> dict:
    """Each table with the generated_at column every analytics table ends with."""
    from pyspark.sql import functions as F
    now = F.current_timestamp()
    return {name: df.withColumn("generated_at", now) for name, df in tables.items()}


def compute(photos, users) -> dict:
    """{table name: DataFrame} for every analytics table, from
    photos_with_latest_prediction() and the users. Pure DataFrame work, so it
    can be checked against local data without a database."""
    from pyspark.sql import functions as F

    cls = F.lower(F.col("predicted_class"))
    has_class = F.col("predicted_class").isNotNull()
    is_healthy, is_uncertain = cls == "healthy", cls == "uncertain"

    def avg_seconds(later, earlier):
        diff = F.col(later).cast("double") - F.col(earlier).cast("double")
        return F.bround(F.avg(diff), 1)

    distribution = _count_photos(photos.filter(has_class), "predicted_class")
    most_detected = (distribution.filter(F.lower(F.col("predicted_class")) != "healthy")
                     .orderBy(F.col("photos").desc(), F.col("predicted_class"))
                     .limit(1).select(F.col("predicted_class").alias("most_detected_disease")))

    recent = F.col("uploaded_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")
    totals = photos.agg(
        F.count("*").alias("total_images"),
        _count_if(has_class & is_healthy).alias("total_healthy"),
        _count_if(has_class & ~is_healthy & ~is_uncertain).alias("total_diseased"),
        _count_if(has_class & is_uncertain).alias("total_uncertain"),
        _count_if(~has_class).alias("total_unclassified"),
        _count_if(F.col("risk_level") == "HIGH").alias("total_high_risk"),
        _count_if(F.col("predicted_at").isNotNull()).alias("total_processed"),
        _count_if(F.col("status") == "FAILED").alias("total_failed"),
        _count_if(recent).alias("submissions_24h"),
        F.countDistinct(F.when(recent, F.col("user_id"))).alias("active_farmers_24h"),
        F.bround(F.avg("processing_latency_ms"), 1).alias("avg_processing_latency_ms"),
        avg_seconds("uploaded_at", "captured_at").alias("avg_capture_to_upload_seconds"),
        avg_seconds("ingested_at", "uploaded_at").alias("avg_upload_to_ingestion_seconds"),
        avg_seconds("predicted_at", "uploaded_at").alias("avg_end_to_end_latency_seconds"),
    )
    active_users = users.agg(_count_if(~F.coalesce(F.col("is_deleted"), F.lit(False))).alias("total_users"))
    summary = (totals.crossJoin(active_users)
               .join(most_detected, how="left")               # no rows -> null
               .withColumn("summary_id", F.lit(1)))

    located = photos.withColumn("location", _location())
    classified = (located.filter(F.col("predicted_at").isNotNull() & has_class)
                  .withColumn("day", _local_day("predicted_at")))
    location_daily = (located.withColumn("summary_date", _local_day("uploaded_at"))
                      .groupBy("location", "summary_date")
                      .agg(F.count("*").alias("total_images"),
                           _count_if(F.col("predicted_class") == "healthy").alias("total_healthy"),
                           _count_if(has_class & (F.col("predicted_class") != "healthy")
                                     & (F.col("predicted_class") != "uncertain")).alias("total_diseased"),
                           F.avg("risk_score").alias("avg_risk_score"),
                           _count_if(F.col("risk_level") == "HIGH").alias("high_risk_count")))

    return _stamped({
        "analytics_summary": summary,
        "analytics_disease_distribution": distribution,
        "analytics_risk_distribution": _count_photos(photos.filter(F.col("risk_level").isNotNull()), "risk_level"),
        "analytics_high_risk_by_location": _high_risk_by(located, "location"),
        "analytics_disease_trend": _count_photos(classified, "location", "day", "predicted_class"),
        "analytics_disease_daily": _count_photos(classified, "day", "predicted_class"),
        "batch_location_daily_summary": location_daily,
    })


def compute_scale_test(items) -> dict:
    """{table name: DataFrame} for the scalability test page, per run_id,
    from scale_test_items: only COMPLETED photos with a CNN result count,
    and HIGH risk is counted like the farmer figures (_high_risk_by)."""
    from pyspark.sql import functions as F
    done = (items.filter((F.col("status") == "COMPLETED") & F.col("predicted_class").isNotNull())
            .withColumn("location", _location()))
    return _stamped({
        "analytics_scale_test_disease_by_location": _count_photos(done, "run_id", "location", "predicted_class"),
        "analytics_scale_test_disease_distribution": _count_photos(done, "run_id", "predicted_class"),
        "analytics_scale_test_high_risk_by_location": _high_risk_by(done, "run_id", "location"),
    })


# ----------------------------------------------------------------- output
def write_tables(tables: dict, jdbc_url, properties) -> None:
    """Replaces every table's rows in ONE transaction, so the dashboard never
    reads a half-written or empty table: Spark writes each result into a
    <name>_staging copy (same columns and types), then DELETE + INSERT ...
    SELECT swaps all of them in at once — readers see the old rows until the
    commit. The tables themselves (types, keys, the Tableau login's grants)
    stay as init_db() created them."""
    from sqlalchemy import text
    from storage.database import engine
    with engine.begin() as conn:
        for name in tables:
            conn.execute(text(f"DROP TABLE IF EXISTS {name}_staging"))
            conn.execute(text(f"CREATE TABLE {name}_staging (LIKE {name})"))
    for name, df in tables.items():
        df.write.mode("append").jdbc(jdbc_url, f"{name}_staging", properties=properties)
    with engine.begin() as conn:
        for name in tables:
            conn.execute(text(f"DELETE FROM {name}"))
            conn.execute(text(f"INSERT INTO {name} SELECT * FROM {name}_staging"))
            conn.execute(text(f"DROP TABLE {name}_staging"))


def refresh(spark) -> float:
    """One full recompute; returns the seconds it took."""
    started = time.monotonic()
    jdbc_url, properties = jdbc_url_and_properties()
    images, predictions, users, scale_test_items = read_inputs(spark, jdbc_url, properties)
    photos = photos_with_latest_prediction(images, predictions).cache()
    try:
        write_tables({**compute(photos, users), **compute_scale_test(scale_test_items)}, jdbc_url, properties)
    finally:
        photos.unpersist()
    return time.monotonic() - started


def run(spark=None) -> float:
    """Compute once (python -m spark.batch_analytics)."""
    from storage.database import init_db
    jdbc_url_and_properties()                  # fail fast on SQLite, before starting a JVM
    init_db()                                  # the analytics tables exist with their types
    owns_spark = spark is None
    spark = spark or build_spark()
    try:
        return refresh(spark)
    finally:
        if owns_spark:
            spark.stop()


# ------------------------------------------------------------------ watch
def data_signature() -> tuple:
    """A cheap fingerprint of everything the analytics depend on: it changes
    when photos, predictions, statuses, users or scalability-test results change."""
    from sqlalchemy import func
    from storage.database import SessionLocal, Image, Prediction, User, ScaleTestItem
    with SessionLocal() as s:
        images = s.query(func.count(Image.image_id), func.max(Image.created_at),
                         func.max(Image.ingested_at), func.max(Image.processed_at)).one()
        predictions = s.query(func.count(Prediction.prediction_id), func.max(Prediction.predicted_at)).one()
        users = s.query(func.count(User.user_id)).scalar()
        active_users = s.query(func.count(User.user_id)).filter(User.is_deleted.is_(False)).scalar()
        scale_test = s.query(func.count(ScaleTestItem.image_id), func.max(ScaleTestItem.completed_at)).one()
        scale_test_done = (s.query(func.count(ScaleTestItem.image_id))
                           .filter(ScaleTestItem.status == "COMPLETED").scalar())
    return tuple(images) + tuple(predictions) + (users, active_users) + tuple(scale_test) + (scale_test_done,)


def watch() -> int:
    """Keeps the analytics tables current for the dashboard: checks the
    database every ANALYTICS_CHECK_SECONDS and recomputes when anything
    changed, or at least every ANALYTICS_MAX_AGE_SECONDS (the "last 24 h"
    figures move with time). One SparkSession for the whole run."""
    from storage.database import init_db
    jdbc_url_and_properties()
    init_db()
    spark = build_spark()
    print(f"[spark-analytics] Watching the database every {ANALYTICS_CHECK_SECONDS:.0f}s; "
          f"recomputing the dashboard's analytics with Spark when it changes", flush=True)
    last_signature, last_run = None, 0.0
    try:
        while True:
            try:
                signature = data_signature()
                stale = time.monotonic() - last_run >= ANALYTICS_MAX_AGE_SECONDS
                if signature != last_signature or stale:
                    seconds = refresh(spark)
                    last_signature, last_run = signature, time.monotonic()
                    print(f"[spark-analytics] {time.strftime('%H:%M:%S')} analytics recomputed "
                          f"({signature[0]} photos) in {seconds:.1f}s", flush=True)
            except Exception as e:  # noqa: BLE001 - database or Spark hiccup: try again next round
                print(f"[spark-analytics] recompute failed, retrying: {e}", flush=True)
            time.sleep(ANALYTICS_CHECK_SECONDS)
    except KeyboardInterrupt:
        return 0
    finally:
        spark.stop()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--watch", action="store_true", help="keep the analytics tables up to date")
    args = p.parse_args(argv)
    if args.watch:
        return watch()
    print(f"[spark-analytics] analytics recomputed in {run():.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
