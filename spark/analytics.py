"""
Data Analytics (DIT manual section 46): descriptive analytics, trend
analysis, and performance metrics, computed from what's actually in the
database — no invented numbers. Used by dashboard/app.py.

Uses pandas over the ORM's underlying SQL engine for these batch
aggregations (appropriate here since dashboard queries are small,
already-persisted result sets — the heavy per-image processing itself runs
in Spark, per spark/streaming_job.py). A Spark-based batch version of these
same aggregations would read the same tables via spark.read.jdbc(...) for
larger deployments; the query logic is identical.
"""

import pandas as pd

from storage.database import engine
from config.localtime import series_to_local


def _read_sql(query: str) -> pd.DataFrame:
    return pd.read_sql(query, engine)


def _clean_record(record: dict) -> dict:
    """pandas NaT/NaN don't serialize to JSON the way FastAPI's default
    encoder expects (NaT becomes the literal string "NaT", not null) —
    normalize both to None, and pandas Timestamps to ISO strings, before a
    record crosses the API boundary."""
    cleaned = {}
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            cleaned[key] = None if pd.isna(value) else value.isoformat()
        elif pd.isna(value) if not isinstance(value, (list, dict)) else False:
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def to_json_safe_records(df: pd.DataFrame) -> list:
    """Convert a DataFrame to a list of dicts safe to return from a JSON
    API endpoint. pandas aggregations (e.g. the mean of an all-NaN group —
    routine right after an upload, before Spark has produced any
    prediction yet) can leave NaN/NaT in the result; Python's `json.dumps`
    rejects NaN outright (`ValueError: Out of range float values are not
    JSON compliant: nan`), so every record must go through `_clean_record`
    before crossing the API boundary. Dashboard call sites that need a
    DataFrame for plotly should keep calling the un-cleaned functions
    below directly."""
    return [_clean_record(r) for r in df.to_dict(orient="records")]


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


def kpi_summary() -> dict:
    df = images_with_predictions()
    # Soft-deleted USERs are kept for historical integrity (their images/
    # predictions stay fully queryable — see admin_delete_user in
    # api/server.py) but shouldn't inflate a "how many active accounts do
    # we have" headline count, consistent with admin_list_users excluding
    # them by default.
    total_users = pd.read_sql("SELECT COUNT(*) AS n FROM users WHERE NOT is_deleted", engine)["n"].iloc[0]

    predicted_class_lower = df["predicted_class"].astype(str).str.lower()
    has_class = df["predicted_class"].notna()
    healthy = int((predicted_class_lower == "healthy").sum())
    uncertain = int((predicted_class_lower == "uncertain").sum())
    diseased = int(has_class.sum() - healthy - uncertain)
    # No predicted_class at all: not processed yet (pending/failed).
    # Surfaced explicitly so healthy+diseased+uncertain
    # not summing to total_images is never a silent, unexplained gap on the
    # dashboard.
    unclassified = int(len(df) - has_class.sum())
    high_risk = int((df["risk_level"] == "HIGH").sum())
    avg_latency = df["processing_latency_ms"].dropna().mean()

    latest_processed = df.dropna(subset=["predicted_at"]).sort_values("predicted_at", ascending=False)
    latest_row = _clean_record(latest_processed.iloc[0].to_dict()) if not latest_processed.empty else None

    return {
        "total_users": int(total_users),
        "total_images": int(len(df)),
        "total_healthy": healthy,
        "total_diseased": diseased,
        "total_uncertain": uncertain,
        "total_unclassified": unclassified,
        "total_high_risk": high_risk,
        "avg_processing_latency_ms": None if pd.isna(avg_latency) else round(float(avg_latency), 1),
        "latest_processed_image": latest_row,
    }


def disease_distribution() -> pd.DataFrame:
    df = images_with_predictions()
    dist = df.dropna(subset=["predicted_class"])["predicted_class"].value_counts().reset_index()
    dist.columns = ["predicted_class", "count"]
    return dist


def risk_distribution() -> pd.DataFrame:
    df = images_with_predictions()
    dist = df.dropna(subset=["risk_level"])["risk_level"].value_counts().reset_index()
    dist.columns = ["risk_level", "count"]
    return dist


UNKNOWN_LOCATION = "Unknown location"


def high_risk_by_location(df: pd.DataFrame = None) -> pd.DataFrame:
    """Per municipality: how many of its analysed maize photos are HIGH risk.
    Columns: location, high (n), total (N), high_pct (0-100). "Analysed"
    means the photo has a prediction — photos rejected as not a leaf / not
    maize have none and don't count. Photos without a location are grouped
    under UNKNOWN_LOCATION. Sorted by most HIGH first, then highest share."""
    df = images_with_predictions() if df is None else df
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


def risk_trend() -> pd.DataFrame:
    df = images_with_predictions().dropna(subset=["predicted_at", "risk_score"])
    return df.sort_values("predicted_at")[["predicted_at", "location", "risk_score", "risk_level"]]


def disease_trend() -> pd.DataFrame:
    df = images_with_predictions().dropna(subset=["predicted_at", "predicted_class"])
    return df.sort_values("predicted_at")[["predicted_at", "location", "predicted_class"]]


def location_summary() -> pd.DataFrame:
    df = images_with_predictions()
    if df.empty:
        return df
    grouped = df.groupby(["location"], dropna=False).agg(
        total_images=("image_id", "count"),
        high_risk_count=("risk_level", lambda s: (s == "HIGH").sum()),
        avg_risk_score=("risk_score", "mean"),
        last_upload=("uploaded_at", "max"),
    ).reset_index()
    return grouped.sort_values("avg_risk_score", ascending=False)


def performance_metrics() -> dict:
    """Latency/throughput metrics computed from real recorded timestamps.
    Stages that never happened (e.g. no images captured with EXIF) are
    reported as None/unavailable rather than estimated."""
    df = images_with_predictions()
    if df.empty:
        return {}

    metrics = {}
    has_capture = df["captured_at"].notna()
    if has_capture.any():
        cap_to_upload = (df.loc[has_capture, "uploaded_at"] - df.loc[has_capture, "captured_at"]).dt.total_seconds()
        metrics["avg_capture_to_upload_seconds"] = round(float(cap_to_upload.mean()), 1)
    else:
        metrics["avg_capture_to_upload_seconds"] = None

    has_ingest = df["ingested_at"].notna()
    if has_ingest.any():
        upload_to_ingest = (df.loc[has_ingest, "ingested_at"] - df.loc[has_ingest, "uploaded_at"]).dt.total_seconds()
        metrics["avg_upload_to_ingestion_seconds"] = round(float(upload_to_ingest.mean()), 1)
    else:
        metrics["avg_upload_to_ingestion_seconds"] = None

    has_latency = df["processing_latency_ms"].notna()
    metrics["avg_processing_latency_ms"] = (
        round(float(df.loc[has_latency, "processing_latency_ms"].mean()), 1) if has_latency.any() else None
    )

    has_pred = df["predicted_at"].notna()
    if has_pred.any():
        e2e = (df.loc[has_pred, "predicted_at"] - df.loc[has_pred, "uploaded_at"]).dt.total_seconds()
        metrics["avg_end_to_end_latency_seconds"] = round(float(e2e.mean()), 1)
    else:
        metrics["avg_end_to_end_latency_seconds"] = None

    metrics["total_processed"] = int(has_pred.sum())
    metrics["total_failed"] = int((df["status"] == "FAILED").sum())
    return metrics
