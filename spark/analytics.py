"""
Data Analytics (DIT manual section 46) for the dashboard. The figures are
computed by Spark (spark/batch_analytics.py, run by the EWS-SparkAnalytics
service) and stored in the analytics tables; the functions here only read
them for the dashboard and scripts/perf_report.py. Nothing is invented: a
table Spark hasn't filled yet reads as empty (or None).

Also here: images_with_predictions(), the row-level list behind the
dashboard's tables (recent submissions, HIGH-risk photos, predictions), and
high_risk_by_location() for one scalability-test run's own rows.
"""

import pandas as pd

from storage.database import engine
from config.localtime import series_to_local, to_local


def _read_sql(query: str) -> pd.DataFrame:
    return pd.read_sql(query, engine)


def _spark_table(name: str, order_by: str = None) -> pd.DataFrame:
    """A Spark analytics table, or an empty frame when it doesn't exist yet
    (SQLite, or the Spark analytics never ran)."""
    try:
        return _read_sql(f"SELECT * FROM {name}" + (f" ORDER BY {order_by}" if order_by else ""))
    except Exception:  # noqa: BLE001 - missing table
        return pd.DataFrame()


def images_with_predictions() -> pd.DataFrame:
    """One row per image, left-joined with its latest prediction (if any).
    The single source of truth every other analytics function derives from.

    An image can legitimately carry more than one Prediction row (Spark
    Structured Streaming is at-least-once, so a replayed batch inserts a
    second prediction for the same image_id — see spark/streaming_job.py;
    the manual "Process" action can also re-submit a row). The raw LEFT
    JOIN therefore yields one row per (image, prediction) pair, which
    breaks the "one row per image" contract every caller here relies on
    (and crashes `df.set_index("image_id")` in the dashboard gallery). We
    collapse to the most recent prediction per image after loading.

    Scalability-test images (source 'scalability_test', scalability/run.py)
    are left out: they are copies of farmer photos made for a benchmark and
    would otherwise double-count every disease and location."""
    query = """
        SELECT
            i.image_id, i.location, i.user_id, i.source, i.status,
            i.captured_at, i.uploaded_at, i.ingested_at, i.processed_at,
            i.gps_latitude, i.gps_longitude, i.camera_make, i.camera_model,
            p.prediction_id,
            p.predicted_class, p.prediction_confidence,
            p.lesion_score, p.discoloration_pct, p.texture_irregularity,
            p.leaf_area_affected_pct, p.risk_score, p.risk_level,
            p.model_version, p.predicted_at, p.processing_latency_ms
        FROM images i
        LEFT JOIN predictions p ON p.image_id = i.image_id
        WHERE i.source <> 'scalability_test'
    """
    df = _read_sql(query)
    for col in ["captured_at", "uploaded_at", "ingested_at", "processed_at", "predicted_at"]:
        if col in df.columns:
            # stored UTC, shown in Timor-Leste Time (same instants, so every
            # comparison / latency below is unchanged)
            df[col] = series_to_local(df[col])

    if not df.empty and df["image_id"].duplicated().any():
        # Keep the newest prediction per image; predicted_at NaT (images
        # with no prediction) sorts first and is only ever a lone row, so
        # keep="last" leaves those untouched. prediction_id is a stable
        # tiebreak when two predictions share a predicted_at.
        df = (
            df.sort_values(["predicted_at", "prediction_id"], na_position="first")
            .drop_duplicates(subset="image_id", keep="last")
            .reset_index(drop=True)
        )

    return df.drop(columns=["prediction_id"], errors="ignore")


def analytics_generated_at():
    """When Spark last computed the analytics (Timor-Leste Time), or None."""
    summary = _spark_table("analytics_summary")
    if summary.empty:
        return None
    return to_local(pd.Timestamp(summary["generated_at"].iloc[0]).to_pydatetime())


def kpi_summary() -> dict | None:
    """The headline figures Spark computed, or None before its first run.
    Keys: total_users, total_images, total_healthy, total_diseased,
    total_uncertain, total_unclassified, total_high_risk, submissions_24h,
    active_farmers_24h, most_detected_disease, avg_processing_latency_ms."""
    summary = _spark_table("analytics_summary")
    if summary.empty:
        return None
    row = summary.iloc[0]
    keys = ("total_users", "total_images", "total_healthy", "total_diseased", "total_uncertain",
            "total_unclassified", "total_high_risk", "submissions_24h", "active_farmers_24h")
    out = {k: int(row[k]) for k in keys}
    out["most_detected_disease"] = None if pd.isna(row["most_detected_disease"]) else row["most_detected_disease"]
    out["avg_processing_latency_ms"] = None if pd.isna(row["avg_processing_latency_ms"])         else float(row["avg_processing_latency_ms"])
    return out


def performance_metrics() -> dict:
    """Pipeline timings Spark computed from the recorded timestamps
    (None = that stage never happened), or {} before its first run."""
    summary = _spark_table("analytics_summary")
    if summary.empty:
        return {}
    row = summary.iloc[0]
    out = {}
    for k in ("avg_capture_to_upload_seconds", "avg_upload_to_ingestion_seconds",
              "avg_processing_latency_ms", "avg_end_to_end_latency_seconds"):
        out[k] = None if pd.isna(row[k]) else float(row[k])
    out["total_processed"], out["total_failed"] = int(row["total_processed"]), int(row["total_failed"])
    return out


def disease_distribution() -> pd.DataFrame:
    """Columns predicted_class, count — most photos first."""
    df = _spark_table("analytics_disease_distribution", "photos DESC, predicted_class")
    return df.rename(columns={"photos": "count"})[["predicted_class", "count"]] if not df.empty         else pd.DataFrame(columns=["predicted_class", "count"])


def risk_distribution() -> pd.DataFrame:
    """Columns risk_level, count — most photos first."""
    df = _spark_table("analytics_risk_distribution", "photos DESC, risk_level")
    return df.rename(columns={"photos": "count"})[["risk_level", "count"]] if not df.empty         else pd.DataFrame(columns=["risk_level", "count"])


def high_risk_locations() -> pd.DataFrame:
    """Per municipality: location, high (n), total (N), high_pct (0-100).
    Most HIGH first, then highest share."""
    df = _spark_table("analytics_high_risk_by_location", "high DESC, high_pct DESC, location")
    cols = ["location", "high", "total", "high_pct"]
    return df[cols].reset_index(drop=True) if not df.empty else pd.DataFrame(columns=cols)


def disease_trend_by_location() -> pd.DataFrame:
    """Columns location, day (Timor-Leste), predicted_class, photos —
    sorted by location, then day."""
    df = _spark_table("analytics_disease_trend", "location, day, predicted_class")
    cols = ["location", "day", "predicted_class", "photos"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    df["day"] = pd.to_datetime(df["day"])
    return df[cols].reset_index(drop=True)


def disease_daily() -> pd.DataFrame:
    """Columns day (Timor-Leste), predicted_class, photos — by day."""
    df = _spark_table("analytics_disease_daily", "day, predicted_class")
    cols = ["day", "predicted_class", "photos"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    df["day"] = pd.to_datetime(df["day"])
    return df[cols].reset_index(drop=True)


UNKNOWN_LOCATION = "Unknown location"


def high_risk_by_location(df: pd.DataFrame) -> pd.DataFrame:
    """Per municipality: how many of `df`'s analysed photos are HIGH risk —
    for one scalability-test run's own rows (dashboard/scalability_page.py),
    which aren't in the images table. The dashboard's farmer figures come
    from Spark instead (high_risk_locations), with the same rules.
    Columns: location, high (n), total (N), high_pct (0-100). "Analysed"
    means the photo has a prediction — photos rejected as not a leaf / not
    maize have none and don't count. Photos without a location are grouped
    under UNKNOWN_LOCATION. Sorted by most HIGH first, then highest share."""
    analysed = df.dropna(subset=["risk_level"])
    if analysed.empty:
        return pd.DataFrame(columns=["location", "high", "total", "high_pct"])
    location = analysed["location"].fillna("").str.strip().replace("", UNKNOWN_LOCATION)
    out = (analysed.assign(location=location, is_high=analysed["risk_level"] == "HIGH")
           .groupby("location")
           .agg(high=("is_high", "sum"), total=("is_high", "size"))
           .reset_index())
    out["high"] = out["high"].astype(int)
    out["high_pct"] = (out["high"] / out["total"] * 100).round(1)
    return out.sort_values(["high", "high_pct", "location"], ascending=[False, False, True]).reset_index(drop=True)
