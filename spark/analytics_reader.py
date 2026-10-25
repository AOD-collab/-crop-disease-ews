"""
Data Analytics (DIT manual section 46) for the dashboard. The figures are
computed by Spark (spark/batch_analytics.py, run by the EWS-SparkAnalytics
service) and stored in the analytics tables; the functions here only read
them for the dashboard. Nothing is invented: a
table Spark hasn't filled yet reads as empty (or None).

Also here: images_with_predictions(), the row-level list behind the
dashboard's tables (recent submissions, HIGH-risk photos, predictions), and
the scale_test_* readers for one scalability-test run's figures.
"""

import pandas as pd
from sqlalchemy import text

from storage.database import engine, LATEST_PREDICTION_CTE
from config.localtime import series_to_local


def _read_sql(query: str, params: dict = None) -> pd.DataFrame:
    return pd.read_sql(text(query), engine, params=params)


class SparkTable:
    """One Spark analytics table as the dashboard reads it: `cols` in
    `order_by` order, `rename`d, with `dates` as datetimes — or an empty frame
    with the same columns when the table doesn't exist yet (SQLite, or the
    Spark analytics never ran). Called like a function; tables with one set
    of rows per scalability-test run take the run_id."""

    def __init__(self, name: str, cols: list = None, order_by: str = None, rename: dict = None, dates=()):
        self.name, self.cols, self.order_by = name, cols, order_by
        self.rename, self.dates = rename or {}, dates

    def __call__(self, run_id: str = None) -> pd.DataFrame:
        query, params = f"SELECT * FROM {self.name}", None
        if run_id is not None:
            query, params = query + " WHERE run_id = :run_id", {"run_id": run_id}
        if self.order_by:
            query += f" ORDER BY {self.order_by}"
        try:
            df = _read_sql(query, params).rename(columns=self.rename)
        except Exception:  # noqa: BLE001 - missing table
            df = pd.DataFrame()
        if self.cols is None:
            return df
        if df.empty:
            return pd.DataFrame(columns=self.cols)
        for col in self.dates:
            df[col] = pd.to_datetime(df[col])
        return df[self.cols].reset_index(drop=True)


def images_with_predictions() -> pd.DataFrame:
    """One row per image, left-joined with its latest prediction (if any):
    the row-level list behind the dashboard's tables. The figures come from
    the Spark tables below, not from this list.

    An image can legitimately carry more than one Prediction row (Spark
    Structured Streaming is at-least-once, so a replayed batch inserts a
    second prediction for the same image_id — see spark/streaming_job.py;
    the manual "Process" action can also re-submit a row). The query keeps
    only the newest one per image (LATEST_PREDICTION_CTE, the rule
    spark/batch_analytics.py uses), so each image is one row (the dashboard gallery's `df.set_index("image_id")`
    relies on it).

    Scalability-test images (source 'scalability_test', scalability/run.py)
    are left out: they are copies of farmer photos made for a benchmark and
    would otherwise double-count every disease and location."""
    query = f"""
        {LATEST_PREDICTION_CTE}
        SELECT
            i.image_id, i.location, i.user_id, i.source, i.status,
            i.captured_at, i.uploaded_at, i.ingested_at, i.processed_at,
            i.gps_latitude, i.gps_longitude, i.camera_make, i.camera_model,
            p.predicted_class, p.prediction_confidence,
            p.lesion_score, p.discoloration_pct, p.texture_irregularity,
            p.leaf_area_affected_pct, p.risk_score, p.risk_level,
            p.model_version, p.predicted_at, p.processing_latency_ms
        FROM images i
        LEFT JOIN latest p ON p.image_id = i.image_id AND p.rn = 1
        WHERE i.source <> 'scalability_test'
    """
    df = _read_sql(query)
    for col in ["captured_at", "uploaded_at", "ingested_at", "processed_at", "predicted_at"]:
        if col in df.columns:
            # stored UTC, shown in Timor-Leste Time (same instants, so every
            # comparison / latency below is unchanged)
            df[col] = series_to_local(df[col])
    return df


def kpi_summary() -> dict | None:
    """The headline figures Spark computed, or None before its first run.
    Keys: total_users, total_images, total_healthy, total_diseased,
    total_uncertain, total_unclassified, total_high_risk, submissions_24h,
    active_farmers_24h, most_detected_disease, avg_processing_latency_ms."""
    summary = SparkTable("analytics_summary")()
    if summary.empty:
        return None
    row = summary.iloc[0]
    keys = ("total_users", "total_images", "total_healthy", "total_diseased", "total_uncertain",
            "total_unclassified", "total_high_risk", "submissions_24h", "active_farmers_24h")
    out = {k: int(row[k]) for k in keys}
    out["most_detected_disease"] = None if pd.isna(row["most_detected_disease"]) else row["most_detected_disease"]
    latency = row["avg_processing_latency_ms"]
    out["avg_processing_latency_ms"] = None if pd.isna(latency) else float(latency)
    return out


# Photos per disease / per risk level — most photos first.
disease_distribution = SparkTable("analytics_disease_distribution", ["predicted_class", "count"],
                                  "photos DESC, predicted_class", rename={"photos": "count"})
risk_distribution = SparkTable("analytics_risk_distribution", ["risk_level", "count"],
                               "photos DESC, risk_level", rename={"photos": "count"})
# Per municipality: high (n), total (N), high_pct (0-100) — most HIGH first, then highest share.
high_risk_locations = SparkTable("analytics_high_risk_by_location", ["location", "high", "total", "high_pct"],
                                 "high DESC, high_pct DESC, location")
# Photos per municipality, day (Timor-Leste) and disease; per day and disease.
disease_trend_by_location = SparkTable("analytics_disease_trend", ["location", "day", "predicted_class", "photos"],
                                       "location, day, predicted_class", dates=("day",))
disease_daily = SparkTable("analytics_disease_daily", ["day", "predicted_class", "photos"],
                           "day, predicted_class", dates=("day",))

# One scalability-test run (called with its run_id): the test page's disease
# figures for its own photos (scale_test_items), kept apart from the farmer
# figures above. Municipality with the most photos first; most photos first;
# most HIGH first.
scale_test_disease_by_location = SparkTable(
    "analytics_scale_test_disease_by_location", ["location", "predicted_class", "photos"],
    "SUM(photos) OVER (PARTITION BY location) DESC, location, predicted_class")
scale_test_disease_distribution = SparkTable(
    "analytics_scale_test_disease_distribution", ["predicted_class", "photos"], "photos DESC, predicted_class")
scale_test_high_risk_by_location = SparkTable(
    "analytics_scale_test_high_risk_by_location", ["location", "high", "total", "high_pct"],
    "high DESC, high_pct DESC, location")
