"""
The scalability test's own Spark Structured Streaming engine
(scalability/spark_engine.py), in Spark local mode: its event parsing, its
foreachBatch sink, and a full test run through scalability/run.py where a
real streaming query (file source standing in for the test's Kafka topic)
reads the events and processes the photos.

Windows note (see tests/test_streaming_pipeline.py): DataFrames are built by
reading JSON files from disk, never spark.createDataFrame from a Python list.
"""

import json
import os
import threading
import uuid

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import StructType, StructField, StringType  # noqa: E402

import config.settings as settings  # noqa: E402
from storage.database import SessionLocal, ScaleTestItem, ScaleTestRun  # noqa: E402
from scalability import spark_engine, worker, run as runner  # noqa: E402
from tests.test_drive_flow import fresh_db  # noqa: E402,F401 - autouse fixture
from tests.test_scalability import (  # noqa: E402,F401 - fixtures
    photo_folder, _production_counts, _production_queue_lines,
)

KAFKA_VALUE_SCHEMA = StructType([StructField("value", StringType())])


@pytest.fixture(scope="module")
def spark():
    try:
        session = (
            SparkSession.builder.appName("test-scalability-spark")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.streaming.checkpointFileManagerClass",
                    "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
            .getOrCreate()
        )
    except Exception as exc:  # no JVM / Java on PATH, etc.
        pytest.skip(f"local SparkSession unavailable: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _write_events(folder, events, name=None):
    """Kafka-shaped records ({"value": <json>}) as a JSON-lines file, written
    elsewhere first and moved in, so a file stream never sees half a file."""
    folder.mkdir(parents=True, exist_ok=True)
    name = name or f"{uuid.uuid4().hex}.json"
    tmp = folder.parent / f".{name}.tmp"
    tmp.write_text("".join(json.dumps({"value": json.dumps(e) if isinstance(e, dict) else e}) + "\n"
                           for e in events))
    os.replace(tmp, folder / name)


def _batch(spark, tmp_path, events):
    folder = tmp_path / f"batch-{uuid.uuid4().hex[:6]}"
    _write_events(folder, events)
    return spark.read.schema(KAFKA_VALUE_SCHEMA).json(str(folder))


def test_parse_keeps_only_this_runs_events(spark, tmp_path):
    raw = _batch(spark, tmp_path, [
        {"run_id": "r1", "item_id": "a"},
        {"run_id": "r1", "item_id": "b"},
        {"run_id": "other-run", "item_id": "c"},
        {"run_id": "r1", "item_id": ""},
        {"run_id": "r1"},
        "not json",
    ])
    rows = spark_engine.parse_test_events(raw, "r1").collect()
    assert sorted(r.item_id for r in rows) == ["a", "b"]


def test_sink_processes_each_photo_once_and_only_this_runs(spark, tmp_path):
    handled, done = [], []
    lock = threading.Lock()

    def handle(item_id, received_at):
        with lock:
            handled.append(item_id)
        return item_id, ("COMPLETED", None, {"received_at": received_at}, None)

    stream = spark_engine.SparkTestStream("r1", ["a", "b", "c"], handle, done.append, tmp_path / "cp", workers=2)
    parse = lambda events: spark_engine.parse_test_events(_batch(spark, tmp_path, events), "r1")  # noqa: E731
    stream.sink(parse([{"run_id": "r1", "item_id": "a"}, {"run_id": "r1", "item_id": "a"},
                       {"run_id": "r1", "item_id": "b"}, {"run_id": "r1", "item_id": "not-staged"}]), 0)
    stream.sink(parse([{"run_id": "r1", "item_id": "b"}, {"run_id": "r1", "item_id": "c"}]), 1)   # b redelivered
    stream.sink(parse([{"run_id": "elsewhere", "item_id": "x"}]), 2)                               # empty after filter

    assert sorted(handled) == ["a", "b", "c"]
    assert sorted(i for i, _ in done) == ["a", "b", "c"]
    assert sorted(stream.take_received()) == ["a", "b", "c"] and stream.take_received() == []


def test_production_topic_is_refused():
    stream = spark_engine.SparkTestStream("r1", [], None, None, "unused")
    with pytest.raises(ValueError):
        stream.start(settings.KAFKA_TOPIC)


class FileTopic:
    """The test topic as a folder of JSON files: what the runner publishes
    is what the streaming query reads."""

    def __init__(self, folder):
        self.folder = folder
        self.folder.mkdir(parents=True, exist_ok=True)
        self.sent = []

    def send_many(self, events):
        self.sent.extend(events)
        _write_events(self.folder, events)
        return {}

    def poll(self, timeout=0.5):
        raise AssertionError("with the Spark engine the runner must not consume the topic itself")

    def close(self):
        pass


class FileSourceStream(spark_engine.SparkTestStream):
    folder = None

    def _read_stream(self, spark, topic):
        return spark.readStream.schema(KAFKA_VALUE_SCHEMA).option("maxFilesPerTrigger", 1).json(str(self.folder))


@pytest.fixture()
def spark_topic(spark, tmp_path, monkeypatch):
    """Spark engine, with a file source standing in for the Kafka topic, and
    proof that the production path is never used."""
    monkeypatch.setattr(settings, "SCALABILITY_ENGINE", "spark")
    monkeypatch.setattr(settings, "SPARK_TRIGGER_SECONDS", 0.2)
    topic = FileTopic(tmp_path / "topic")
    FileSourceStream.folder = topic.folder
    calls = {"consume": []}

    def make_queue(consume=True):
        calls["consume"].append(consume)
        return topic

    def make_stream(run_id, ids, handle, on_done):
        return FileSourceStream(run_id, ids, handle, on_done, tmp_path / f"cp-{run_id}").start(settings.SCALABILITY_KAFKA_TOPIC)

    monkeypatch.setattr(runner, "_test_queue", make_queue)
    monkeypatch.setattr(runner, "_test_stream", make_stream)
    monkeypatch.setattr(runner, "_live_gate", lambda: worker.LiveGate(check=lambda: False, interval=0.05))

    def forbidden(*a, **k):
        raise AssertionError("the scalability test used the production pipeline")
    import kafka.producer
    import spark.streaming_job
    monkeypatch.setattr(kafka.producer.KafkaBackend, "__init__", forbidden)
    monkeypatch.setattr(spark.streaming_job, "_process_one", forbidden)
    topic.calls = calls
    return topic


def test_full_run_through_the_tests_own_spark_query(photo_folder, spark_topic):  # noqa: F811
    before = (_production_counts(), _production_queue_lines())
    run_id = runner.start(10, source_dir=str(photo_folder))

    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()
    summary = json.loads(run.summary_json)
    assert run.status == "COMPLETED"
    assert summary["completed"] == 4 and summary["failed"] == 0
    assert summary["config"]["ENGINE"] == "spark"
    assert all(i.status == "COMPLETED" and i.predicted_class for i in items)
    assert spark_topic.calls["consume"] == [False]                  # Spark reads the topic, not the runner
    sent = [e["item_id"] for e in spark_topic.sent]
    assert sent[0] == spark_engine.WARM_UP_ID                       # Spark warmed up before the timer
    assert set(sent[1:]) == {i.image_id for i in items}
    assert (_production_counts(), _production_queue_lines()) == before      # live pipeline untouched
    assert not (runner.run_dir(run_id) / "spark_checkpoint").exists()


def test_engine_is_validated(monkeypatch):
    monkeypatch.setattr(settings, "SCALABILITY_ENGINE", "nope")
    with pytest.raises(runner.RunError):
        runner.engine()
    with pytest.raises(runner.RunError):
        runner.set_engine("nope")
