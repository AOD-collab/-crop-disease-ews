"""
Exercises the ACTUAL Spark Structured Streaming job (spark/streaming_job.py)
in Spark **local mode** — its event-parsing projection (`parse_events`), its
`foreachBatch` sink, and a full file-source → foreachBatch → database run
with checkpointing — without needing a Kafka broker or a multi-node cluster.

This is the automated evidence behind the DIT-manual "Apache Spark
(obrigatóriu)" requirement: previously only the shared, pyspark-free
`spark/preprocessing.py` helpers were covered by tests, and the streaming
job's own code path (Spark session, `readStream`, `from_json` against
`EVENT_SCHEMA`, the `DataFrame.isEmpty()` guard, `row.asDict()` fan-out) ran
unverified.

Portability: auto-skips where pyspark — or a working local SparkSession
(needs a JVM) — isn't available, so the rest of the suite keeps its
"no external infrastructure" property.

Windows note (memory: spark-jobs-windows): local-mode Spark on the dev box
cannot run executor-side Python workers at all — anything that spawns one
(`DataFrame.rdd`, Python UDFs, **and `spark.createDataFrame` from a local
Python list**, which parallelizes through a worker) dies with an
`EOFException`. So every DataFrame here is built by reading JSON files from
disk (pure JVM), and only JVM-side ops are used (`from_json`,
`DataFrame.isEmpty()`, `.collect()`). The per-event Python work
(`process_event`, `persist_result`) runs in the driver inside
`foreachBatch`, exactly as in production.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

# Spark must launch its Python processes with the same interpreter running
# the tests — not a bare `python`, which on Windows hits the Microsoft Store
# app-execution alias stub and fails to start.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

from spark.streaming_job import EVENT_SCHEMA, foreach_batch_sink, parse_events
from config.settings import IMAGE_STORAGE_PATH
from storage.database import (
    Base,
    Image,
    Prediction,
    ProcessingEvent,
    ProcessingStatus,
    Role,
    SessionLocal,
    User,
    UserStatus,
    engine,
    init_db,
)

KAFKA_VALUE_SCHEMA = StructType([StructField("value", StringType())])


@pytest.fixture(scope="module")
def spark():
    try:
        session = (
            SparkSession.builder.appName("test-streaming-pipeline")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            # match spark/streaming_job.build_spark(): the Windows-safe
            # checkpoint file manager, so the e2e checkpoint test actually
            # exercises what production uses.
            .config("spark.sql.streaming.checkpointFileManagerClass",
                    "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
            .getOrCreate()
        )
    except Exception as exc:  # no JVM / Java on PATH, etc.
        pytest.skip(f"local SparkSession unavailable: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture()
def real_jpeg_factory():
    """Writes a small but structurally valid JPEG under IMAGE_STORAGE_PATH,
    named the way a real upload names it (<image_id>.<ext>) —
    process_event() reads these bytes and runs the heuristic CV model over
    them, and spark.preprocessing.clean_event() requires both the
    IMAGE_STORAGE_PATH containment and the image_id/filename correspondence
    that a real api/image_storage.save_image() upload always produces."""
    from io import BytesIO

    import numpy as np
    from PIL import Image as PILImage

    def _make(image_id: str) -> Path:
        arr = (np.random.rand(48, 48, 3) * 255).astype("uint8")
        buf = BytesIO()
        PILImage.fromarray(arr, mode="RGB").save(buf, format="JPEG")
        IMAGE_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
        path = IMAGE_STORAGE_PATH / f"{image_id}.jpg"
        path.write_bytes(buf.getvalue())
        return path

    return _make


def _read_json_df(spark, tmp_path, name, schema, *json_lines):
    """Build a DataFrame the executor-worker-free way: write newline-
    delimited JSON to disk and read it back with an explicit schema (a
    pure-JVM file scan). An empty file yields a 0-row DataFrame."""
    path = tmp_path / name
    path.write_text("\n".join(json_lines))
    return spark.read.schema(schema).json(str(path))


def _seed_image(image_path, image_id="img-stream-1"):
    """Insert the users/images rows the API would have created before the
    event ever reached Kafka."""
    session = SessionLocal()
    try:
        session.add(
            User(
                user_id="u-stream-1",
                username="streamfarmer",
                full_name="Stream Farmer",
                email="streamfarmer@gmail.com",
                password_hash="x",
                role=Role.USER,
                status=UserStatus.ACTIVE,
                email_verified=True,
            )
        )
        session.add(
            Image(
                image_id=image_id,
                user_id="u-stream-1",
                location="Dili",
                original_filename="leaf.jpg",
                image_path=str(image_path),
                uploaded_at=datetime.now(timezone.utc),
                status=ProcessingStatus.UPLOADED,
            )
        )
        session.commit()
    finally:
        session.close()
    return image_id


def _event(image_id, image_path):
    """The JSON payload the API publishes to the `maize-disease-images` topic."""
    return {
        "image_id": image_id,
        "user_id": "u-stream-1",
        "location": "Dili",
        "image_path": str(image_path),
        "captured_at": None,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------------------------------
# parse_events: the exact projection run() feeds into foreachBatch
# --------------------------------------------------------------------------
def test_parse_events_extracts_typed_columns_from_kafka_value(spark, tmp_path):
    event = _event("img-parse-1", "/does/not/matter.jpg")
    raw = _read_json_df(
        spark, tmp_path, "kv.json", KAFKA_VALUE_SCHEMA,
        json.dumps({"value": json.dumps(event)}),
    )

    parsed = parse_events(raw)

    assert parsed.columns == [f.name for f in EVENT_SCHEMA.fields]
    row = parsed.collect()[0].asDict()
    assert row["image_id"] == "img-parse-1"
    assert row["location"] == "Dili"
    assert row["user_id"] == "u-stream-1"


def test_parse_events_filters_out_unparseable_value(spark, tmp_path):
    """An event that fails to parse into EVENT_SCHEMA yields an all-null row
    from from_json — the DataFrame-level cleaning stage (manual 5.4) filters
    that out entirely (missing required fields) rather than passing a row of
    nulls downstream to foreachBatch."""
    raw = _read_json_df(
        spark, tmp_path, "kv_bad.json", KAFKA_VALUE_SCHEMA,
        json.dumps({"value": "not-json-at-all"}),
    )

    assert parse_events(raw).count() == 0


# --------------------------------------------------------------------------
# foreach_batch_sink: the real Spark sink, driven with real DataFrames
# --------------------------------------------------------------------------
def test_foreach_batch_sink_persists_a_real_prediction(spark, tmp_path, real_jpeg_factory):
    image_id = "img-stream-1"
    real_jpeg = real_jpeg_factory(image_id)
    image_id = _seed_image(real_jpeg, image_id=image_id)
    batch = _read_json_df(
        spark, tmp_path, "evt.json", EVENT_SCHEMA, json.dumps(_event(image_id, real_jpeg))
    )

    foreach_batch_sink(batch, 0)

    session = SessionLocal()
    try:
        img = session.get(Image, image_id)
        preds = session.query(Prediction).filter_by(image_id=image_id).all()
        events = session.query(ProcessingEvent).filter_by(image_id=image_id).all()
    finally:
        session.close()

    assert img.status == ProcessingStatus.COMPLETED
    assert img.processed_at is not None
    assert len(preds) == 1
    assert preds[0].risk_level in {"LOW", "MEDIUM", "HIGH"}
    assert preds[0].risk_score is not None
    assert preds[0].model_version  # traceability string was recorded
    assert len(events) == 1
    assert events[0].status == ProcessingStatus.COMPLETED


def test_foreach_batch_sink_skips_empty_batch_without_touching_db(spark, tmp_path):
    empty = _read_json_df(spark, tmp_path, "empty.json", EVENT_SCHEMA)  # 0-byte file
    assert empty.isEmpty()

    # The JVM-only DataFrame.isEmpty() guard must short-circuit here — no
    # crash, no rows written. (This is the line the Windows .rdd workaround
    # in the memory note specifically changed.)
    foreach_batch_sink(empty, 0)

    session = SessionLocal()
    try:
        assert session.query(Prediction).count() == 0
        assert session.query(ProcessingEvent).count() == 0
    finally:
        session.close()


def test_foreach_batch_sink_marks_invalid_event_failed(spark, tmp_path):
    missing = tmp_path / "never_written.jpg"  # image row exists, file does not
    image_id = _seed_image(missing)
    batch = _read_json_df(
        spark, tmp_path, "evt_bad.json", EVENT_SCHEMA, json.dumps(_event(image_id, missing))
    )

    foreach_batch_sink(batch, 0)

    session = SessionLocal()
    try:
        img = session.get(Image, image_id)
        preds = session.query(Prediction).filter_by(image_id=image_id).all()
    finally:
        session.close()

    assert img.status == ProcessingStatus.FAILED
    assert img.error_message and "not found" in img.error_message.lower()
    assert preds == []  # never a fabricated prediction on failure


# --------------------------------------------------------------------------
# Full streaming run: file source -> parse_events -> foreachBatch -> DB,
# with a real checkpoint, driven by Structured Streaming's micro-batch engine
# --------------------------------------------------------------------------
def test_end_to_end_file_stream_reaches_the_database(spark, tmp_path, real_jpeg_factory):
    real_jpeg = real_jpeg_factory("img-e2e-1")
    image_id = _seed_image(real_jpeg, image_id="img-e2e-1")

    source_dir = tmp_path / "events"
    source_dir.mkdir()
    checkpoint_dir = tmp_path / "checkpoint"
    (source_dir / "batch-0.json").write_text(
        json.dumps({"value": json.dumps(_event(image_id, real_jpeg))})
    )

    raw = spark.readStream.schema(KAFKA_VALUE_SCHEMA).format("json").load(str(source_dir))
    query = (
        parse_events(raw)
        .writeStream.foreachBatch(foreach_batch_sink)
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )
    assert query.awaitTermination(180)  # returns True once availableNow drains

    session = SessionLocal()
    try:
        img = session.get(Image, image_id)
        preds = session.query(Prediction).filter_by(image_id=image_id).all()
    finally:
        session.close()

    assert img.status == ProcessingStatus.COMPLETED
    assert len(preds) == 1
    assert (checkpoint_dir / "offsets").exists()  # checkpoint really advanced
