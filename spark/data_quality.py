"""
Data quality checks (DIT manual section 14C): duplicate-record detection,
outlier checks, and missing-value counts, computed from what's actually in
the database — no simulated numbers. Complements spark/analytics.py's
descriptive metrics with data-integrity checks a capstone report/demo can
point at directly.
"""

import pandas as pd

from storage.database import engine


def data_quality_report() -> dict:
    images = pd.read_sql("SELECT image_id, location, captured_at, content_hash FROM images", engine)
    predictions = pd.read_sql(
        "SELECT prediction_id, image_id, risk_score, processing_latency_ms FROM predictions", engine
    )

    # --- duplicate records ---
    # Same uploaded bytes under (possibly) different filenames/uploads —
    # see Image.content_hash / api/server.py's duplicate-flagging.
    hash_counts = images.dropna(subset=["content_hash"]).groupby("content_hash").size()
    duplicate_hash_groups = hash_counts[hash_counts > 1]
    # More than one Prediction row for the same image — Structured
    # Streaming's at-least-once delivery can (before persist_result's
    # delete-then-insert idempotency fix) produce this; kept as a check
    # here too since old/legacy rows can still exist.
    prediction_counts = predictions.groupby("image_id").size()
    duplicate_prediction_groups = prediction_counts[prediction_counts > 1]

    # --- outliers ---
    risk_score_out_of_range = int(
        ((predictions["risk_score"] < 0) | (predictions["risk_score"] > 1)).sum()
    )
    latency = predictions["processing_latency_ms"].dropna()
    latency_outliers = 0
    if len(latency) >= 4:
        q1, q3 = latency.quantile(0.25), latency.quantile(0.75)
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        latency_outliers = int(((latency < lower) | (latency > upper)).sum())

    # --- missing-value counts ---
    missing_value_counts = {
        "images.location": int(images["location"].isna().sum()),
        "images.captured_at": int(images["captured_at"].isna().sum()),
        "images.content_hash": int(images["content_hash"].isna().sum()),
        "predictions.risk_score": int(predictions["risk_score"].isna().sum()),
        "predictions.processing_latency_ms": int(predictions["processing_latency_ms"].isna().sum()),
    }

    return {
        "total_images": int(len(images)),
        "total_predictions": int(len(predictions)),
        "duplicate_content_hash_groups": int(len(duplicate_hash_groups)),
        "duplicate_images_count": int(duplicate_hash_groups.sum()) if not duplicate_hash_groups.empty else 0,
        "duplicate_prediction_rows_groups": int(len(duplicate_prediction_groups)),
        "risk_score_out_of_range": risk_score_out_of_range,
        "processing_latency_outliers": latency_outliers,
        "missing_value_counts": missing_value_counts,
    }
