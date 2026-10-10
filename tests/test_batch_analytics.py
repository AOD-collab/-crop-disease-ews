"""
spark/batch_analytics.py — the batch half of manual objective 7 (streaming
is spark/streaming_job.py). jdbc_url_and_properties() is pure Python and
tested directly; aggregate_location_daily() is a real Spark DataFrame
transformation, tested against local DataFrames built from JSON files (the
same Windows-safe pattern used by tests/test_streaming_pipeline.py —
spark.createDataFrame() from a Python list spawns an executor-side Python
worker that dies with EOFException in this environment's local-mode Spark;
reading JSON from disk is pure-JVM and avoids that).
"""

import json

import pytest

from spark.batch_analytics import jdbc_url_and_properties


# --------------------------------------------------------------------------
# jdbc_url_and_properties: pure Python, no Spark/JVM needed
# --------------------------------------------------------------------------
def test_jdbc_url_parses_a_postgres_database_url(monkeypatch):
    monkeypatch.setattr(
        "spark.batch_analytics.DATABASE_URL",
        "postgresql+psycopg2://ews_user:s3cret@localhost:5432/crop_disease",
    )
    url, properties = jdbc_url_and_properties()
    assert url == "jdbc:postgresql://localhost:5432/crop_disease"
    assert properties == {
        "driver": "org.postgresql.Driver",
        "user": "ews_user",
        "password": "s3cret",
    }


def test_jdbc_url_rejects_sqlite(monkeypatch):
    monkeypatch.setattr("spark.batch_analytics.DATABASE_URL", "sqlite:///storage/app.db")
    with pytest.raises(RuntimeError, match="requires PostgreSQL"):
        jdbc_url_and_properties()


# --------------------------------------------------------------------------
# aggregate_location_daily: real Spark DataFrame transformation
# --------------------------------------------------------------------------
pyspark = pytest.importorskip("pyspark")

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from spark.batch_analytics import aggregate_location_daily

IMAGES_SCHEMA = StructType([
    StructField("image_id", StringType()),
    StructField("location", StringType()),
    StructField("uploaded_at", TimestampType()),
])

PREDICTIONS_SCHEMA = StructType([
    StructField("image_id", StringType()),
    StructField("predicted_class", StringType()),
    StructField("risk_score", DoubleType()),
    StructField("risk_level", StringType()),
])


@pytest.fixture(scope="module")
def spark():
    try:
        session = (
            SparkSession.builder.appName("test-batch-analytics")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"local SparkSession unavailable: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _read_json_df(spark, tmp_path, name, schema, *rows):
    path = tmp_path / name
    path.write_text("\n".join(json.dumps(r) for r in rows))
    return spark.read.schema(schema).json(str(path))


def test_aggregate_location_daily_groups_by_location_and_day(spark, tmp_path):
    images = _read_json_df(
        spark, tmp_path, "images.json", IMAGES_SCHEMA,
        {"image_id": "img1", "location": "Dili", "uploaded_at": "2026-09-01T08:00:00Z"},
        {"image_id": "img2", "location": "Dili", "uploaded_at": "2026-09-01T09:00:00Z"},
        {"image_id": "img3", "location": "Baucau", "uploaded_at": "2026-09-01T10:00:00Z"},
    )
    predictions = _read_json_df(
        spark, tmp_path, "predictions.json", PREDICTIONS_SCHEMA,
        {"image_id": "img1", "predicted_class": "healthy", "risk_score": 0.1, "risk_level": "LOW"},
        {"image_id": "img2", "predicted_class": "gray_leaf_spot", "risk_score": 0.8, "risk_level": "HIGH"},
        {"image_id": "img3", "predicted_class": "healthy", "risk_score": 0.05, "risk_level": "LOW"},
    )

    summary = aggregate_location_daily(images, predictions).orderBy("location").collect()

    assert len(summary) == 2
    baucau, dili = summary
    assert baucau["location"] == "Baucau"
    assert baucau["total_images"] == 1
    assert baucau["total_healthy"] == 1
    assert baucau["total_diseased"] == 0

    assert dili["location"] == "Dili"
    assert dili["total_images"] == 2
    assert dili["total_healthy"] == 1
    assert dili["total_diseased"] == 1
    assert dili["high_risk_count"] == 1
    assert dili["generated_at"] is not None


def test_aggregate_location_daily_handles_images_with_no_prediction_yet(spark, tmp_path):
    images = _read_json_df(
        spark, tmp_path, "images2.json", IMAGES_SCHEMA,
        {"image_id": "img1", "location": "Dili", "uploaded_at": "2026-09-01T08:00:00Z"},
    )
    predictions = _read_json_df(spark, tmp_path, "predictions2.json", PREDICTIONS_SCHEMA)  # empty

    summary = aggregate_location_daily(images, predictions).collect()
    assert len(summary) == 1
    assert summary[0]["total_images"] == 1
    assert summary[0]["total_healthy"] == 0
    assert summary[0]["total_diseased"] == 0
