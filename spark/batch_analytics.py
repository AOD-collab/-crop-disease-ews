"""
Spark BATCH processing job (DIT manual objective 7's batch half —
spark/streaming_job.py is the continuous streaming half). Reads images,
predictions and users via spark.read.jdbc(...), joins them, aggregates by
location and day, and writes the result back to
batch_location_daily_summary via df.write.jdbc(..., mode="overwrite").

Distinct from spark/analytics.py (pandas, used by the live dashboard for
fast interactive queries) — this is the distributed batch aggregation the
manual asks for, run on demand / on a schedule rather than per page load.

Requires PostgreSQL (DATABASE_URL=postgresql+psycopg2://user:pass@host/db).
Spark's JDBC reader needs a real JDBC driver, and this project doesn't ship
a SQLite JDBC jar — SQLite is dev/test-only (see storage/database.py).

Run:
    spark-submit \\
      --packages org.postgresql:postgresql:42.7.3 \\
      spark/batch_analytics.py

REQUIRES EXTERNAL INFRASTRUCTURE: a running PostgreSQL server, and (for
--packages to resolve) internet access to Maven Central the first time it
runs — same caveat as spark/streaming_job.py's Kafka connector package.
"""

import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, current_timestamp, sum as _sum, to_date, when

from config.settings import DATABASE_URL

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


def aggregate_location_daily(images_df, predictions_df):
    """The actual distributed aggregation, factored out from run() so it's
    testable against local DataFrames without a live Postgres server. One row per (location, day):
    total_images, total_healthy, total_diseased, avg_risk_score,
    high_risk_count."""
    joined = images_df.join(
        predictions_df, images_df.image_id == predictions_df.image_id, "left"
    ).withColumn("summary_date", to_date(images_df.uploaded_at))

    summary = (
        joined.groupBy(images_df.location.alias("location"), "summary_date")
        .agg(
            count(images_df.image_id).alias("total_images"),
            _sum(when(predictions_df.predicted_class == "healthy", 1).otherwise(0)).alias("total_healthy"),
            _sum(
                when(
                    predictions_df.predicted_class.isNotNull()
                    & (predictions_df.predicted_class != "healthy")
                    & (predictions_df.predicted_class != "uncertain"),
                    1,
                ).otherwise(0)
            ).alias("total_diseased"),
            avg(predictions_df.risk_score).alias("avg_risk_score"),
            _sum(when(predictions_df.risk_level == "HIGH", 1).otherwise(0)).alias("high_risk_count"),
        )
        .withColumn("generated_at", current_timestamp())
    )
    return summary


def build_spark():
    return (
        SparkSession.builder
        .appName("ImageSystem-CropDiseaseEWS-BatchAnalytics")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def run(spark=None):
    owns_spark = spark is None
    spark = spark or build_spark()
    try:
        jdbc_url, properties = jdbc_url_and_properties()

        images_df = spark.read.jdbc(jdbc_url, "images", properties=properties)
        predictions_df = spark.read.jdbc(jdbc_url, "predictions", properties=properties)

        summary = aggregate_location_daily(images_df, predictions_df)

        (
            summary.write
            .mode("overwrite")
            .jdbc(jdbc_url, "batch_location_daily_summary", properties=properties)
        )
        return summary
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    run()
