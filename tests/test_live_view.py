"""
The live console view of the real-time pipeline: spark/live_log.py (EWS-Spark
window lines) and kafka/monitor.py (the read-only EWS-KafkaMonitor window).
"""
import json

import pytest

from spark import live_log
from storage.database import init_db, SessionLocal, Base, engine, User, Role, UserStatus, ProcessingStatus


@pytest.fixture(autouse=True)
def fresh_db(monkeypatch):
    Base.metadata.drop_all(engine)
    init_db()
    live_log.farmer_label.cache_clear()
    monkeypatch.setattr(live_log, "ENABLED", True)
    yield
    Base.metadata.drop_all(engine)


def _farmer() -> str:
    with SessionLocal() as s:
        u = User(username="ana@gmail.com", email="ana@gmail.com", password_hash="x",
                 role=Role.USER, status=UserStatus.ACTIVE)
        s.add(u)
        s.commit()
        return u.user_id


def _event(uid, image_id="0f1e2d3c-aaaa-bbbb-cccc-000000000001"):
    return {"image_id": image_id, "user_id": uid, "location": "Baucau",
            "image_path": "gdrive:file123", "drive_file_id": "file123",
            "uploaded_at": "2026-10-10T08:00:00+00:00"}


def test_photo_line_shows_farmer_place_result_and_stage_times():
    uid = _farmer()
    result = {"predicted_class": "common_rust", "prediction_confidence": 0.912, "risk_level": "HIGH",
              "timings": {"read_s": 1.93, "preprocess_s": 0.14, "cnn_s": 0.05, "render_s": 0.3}}
    line = live_log.photo_line(_event(uid), "COMPLETED", result, seconds=2.81)
    assert line.startswith("[photo]  0f1e2d3c  ana@gmail.com  Baucau  -> common_rust 91%  risk HIGH")
    assert "download 1.93s" in line and "cnn 0.05s" in line and "total 2.81s" in line
    failed = live_log.photo_line(_event(uid), "FAILED", error="Image file not found in Google Drive")
    assert "FAILED: Image file not found" in failed


def test_lines_can_be_turned_off(capsys, monkeypatch):
    monkeypatch.setattr(live_log, "ENABLED", False)
    live_log.batch_started(3, 2)
    assert capsys.readouterr().out == ""
    monkeypatch.setattr(live_log, "ENABLED", True)
    live_log.batch_started(3, 2)
    assert "batch 3: 2 photo event(s) received from Kafka" in capsys.readouterr().out


def test_spark_prints_each_photo_and_its_drive_delivery(capsys, monkeypatch):
    import spark.streaming_job as job
    uid = _farmer()
    result = {"predicted_class": "healthy", "prediction_confidence": 0.97, "risk_level": "LOW",
              "timings": {"read_s": 0.5, "cnn_s": 0.04}}
    monkeypatch.setattr(job, "replay_state", lambda image_id: None)
    monkeypatch.setattr(job, "mark_processing", lambda image_id: None)
    monkeypatch.setattr(job, "process_event", lambda event: result)
    monkeypatch.setattr(job, "persist_result", lambda *a, **k: None)
    delivered = {"file": "result-file-id"}
    monkeypatch.setattr(job, "publish_result_to_drive_safely", lambda *a, **k: delivered["file"])

    assert job._process_one(_event(uid)).result() == ProcessingStatus.COMPLETED
    out = capsys.readouterr().out
    assert "[photo]  0f1e2d3c  ana@gmail.com  Baucau  -> healthy 97%  risk LOW" in out
    assert "delivered to ana@gmail.com's Drive" in out

    delivered["file"] = None                                  # Drive down: publish only logged it
    job._process_one(_event(uid)).result()
    assert "result NOT delivered to Drive" in capsys.readouterr().out


def test_kafka_monitor_line_and_read_only_consumer():
    from kafka import monitor
    uid = _farmer()
    line = monitor.describe(json.dumps(_event(uid)).encode(), 0, 812)
    assert line.startswith("p0 offset 812: photo 0f1e2d3c from ana@gmail.com (Baucau)")
    assert "Drive file" in line and "waiting for Spark" in line
    assert "unreadable message" in monitor.describe(b"\xff not json", 0, 1)

    a, b = monitor.consumer_config(False), monitor.consumer_config(True)
    assert a["enable.auto.commit"] is False and a["auto.offset.reset"] == "latest"
    assert b["auto.offset.reset"] == "earliest"
    assert a["group.id"] == monitor.GROUP_ID == "ews-kafka-monitor"           # never Spark's or the test's


def test_run_all_opens_the_kafka_monitor_window():
    import run_all
    src = open(run_all.__file__, encoding="utf-8").read()
    assert 'start_service("EWS-KafkaMonitor", ["python", "-m", "kafka.monitor"])' in src
    assert "--no-kafka-monitor" in src


def test_test_topic_summary_counts_runs_offsets_and_position():
    from kafka import monitor
    summary = monitor.TestTopicSummary()
    assert summary.line() is None                                       # nothing arrived
    for offset in range(100, 160):
        summary.add(json.dumps({"run_id": "scale-A", "item_id": str(offset)}).encode(), offset)
    line = summary.line(consumed_to=130, engine="python")
    assert "+60 event(s) (offsets 100-159; 60 for scale-A); 60 since start" in line
    assert "committed up to offset 130 (30 behind" in line and "not Spark" in line
    summary.add(json.dumps({"run_id": "scale-B", "item_id": "x"}).encode(), 160)
    line = summary.line(consumed_to=130, engine="spark")              # Spark has no group position
    assert "committed" not in line and "own Spark query, not the live Spark job" in line
    assert summary.line() is None                                       # reset after each line
    summary.add(b"not json", 160)
    assert "1 for unreadable" in summary.line()


class _FakeConsumer:
    def __init__(self, topics):
        self.topics, self.assigned = topics, []

    def list_topics(self, timeout=None):
        from types import SimpleNamespace
        return SimpleNamespace(topics={t: SimpleNamespace(error=None, partitions={p: None for p in parts})
                                       for t, parts in self.topics.items()})

    def incremental_assign(self, tps):
        self.assigned += [(tp.topic, tp.partition, tp.offset) for tp in tps]


def test_monitor_assigns_partitions_without_a_group_and_catches_new_topics():
    from confluent_kafka import OFFSET_BEGINNING, OFFSET_END
    from kafka import monitor
    fake = _FakeConsumer({"live": [0]})
    assigner = monitor.Assigner(fake, ["live", "test"], from_start=False)
    assert assigner.refresh() and fake.assigned == [("live", 0, OFFSET_END)]   # existing: newest only
    assert assigner.missing() == ["test"]
    fake.topics["test"] = [0]                                                  # the test creates its topic
    assigner.refresh()
    assert ("test", 0, OFFSET_BEGINNING) in fake.assigned                       # all of it is new
    assert assigner.missing() == [] and assigner.refresh() == []               # nothing twice
