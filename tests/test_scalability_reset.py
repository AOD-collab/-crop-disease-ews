"""
The Delete test button's complete reset (scalability/reset.py): everything
of the scalability test goes, nothing else is touched, a failure is never
reported as a complete reset, and running it again is harmless.

Kafka is a fake admin client (no broker in the test suite); it records what
the reset asked for and refuses anything but the test's own topic/groups.
"""

import json
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

import config.settings as settings  # noqa: E402
from storage.database import (  # noqa: E402
    SessionLocal, ScaleTestItem, ScaleTestRun, User, StorageAuditLog, SCALE_TEST_USERNAME,
)
from scalability import reset, run as runner  # noqa: E402
from tests.conftest import make_leaf_jpeg_bytes  # noqa: E402
from tests.test_drive_flow import fresh_db, drive  # noqa: E402,F401 - fixtures
from tests.test_scalability import (  # noqa: E402,F401 - fixtures
    photo_folder, test_topic, python_engine, _production_counts, _production_queue_lines, _snapshot,
)


class FakeFuture:
    def __init__(self, error=None):
        self.error = error

    def result(self, timeout=None):
        if self.error:
            raise self.error
        return None


class FakeKafkaAdmin:
    def __init__(self, events=5, groups=("scalability-test",)):
        self.events = {settings.SCALABILITY_KAFKA_TOPIC: events, settings.KAFKA_TOPIC: 42}
        self.groups = set(groups) | {"some-production-group"}
        self.deleted_records, self.deleted_groups = [], []

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics={t: SimpleNamespace(partitions={0: None}) for t in self.events})

    def watermarks(self, topic, partitions):
        return {0: (0, self.events[topic])}

    def delete_records(self, tps):
        for tp in tps:
            assert tp.topic == settings.SCALABILITY_KAFKA_TOPIC, "the reset touched another topic"
            self.deleted_records.append((tp.topic, tp.partition, tp.offset))
            self.events[tp.topic] = 0
        return {tp: FakeFuture() for tp in tps}

    def list_consumer_groups(self):
        valid = [SimpleNamespace(group_id=g) for g in self.groups]
        return SimpleNamespace(result=lambda timeout=None: SimpleNamespace(valid=valid))

    def delete_consumer_groups(self, groups):
        for g in groups:
            assert g.startswith(settings.SCALABILITY_CONSUMER_GROUP), "the reset touched another group"
            self.deleted_groups.append(g)
            self.groups.discard(g)
        return {g: FakeFuture() for g in groups}


@pytest.fixture()
def kafka(monkeypatch):
    admin = FakeKafkaAdmin()
    steps = tuple((name, (lambda: reset.clear_kafka(admin, admin.watermarks)) if fn is reset.clear_kafka else fn)
                  for name, fn in reset.STEPS)
    monkeypatch.setattr(reset, "STEPS", steps)
    monkeypatch.setattr(reset.reset_all, "__defaults__", (steps, None))
    return admin


def _shared_files(tmp_path):
    """Files in every shared folder the reset must leave alone."""
    files = []
    for folder in (settings.DOWNLOADED_PHOTOS_DIR, settings.IMAGE_STORAGE_PATH, settings.SPARK_CHECKPOINT_ROOT):
        folder.mkdir(parents=True, exist_ok=True)
        f = folder / "keep_me.jpg"
        f.write_bytes(make_leaf_jpeg_bytes(77))
        files.append(f)
    bench = runner.REPORTS_DIR / "inference_benchmarks" / "x" / "result.csv"   # not the test's
    bench.parent.mkdir(parents=True, exist_ok=True)
    bench.write_text("kept")
    files.append(bench)
    return files


def test_reset_removes_every_test_and_nothing_else(photo_folder, test_topic, kafka, tmp_path):  # noqa: F811
    shared = _shared_files(tmp_path)
    before_photos = _snapshot(photo_folder)
    runner.start_series(10, 2, source_dir=str(photo_folder))      # two iterations
    runner.start(10, source_dir=str(photo_folder))                 # and an older-style single test
    (runner.REPORTS_DIR / "logs").mkdir(exist_ok=True)
    (runner.REPORTS_DIR / "logs" / "start_x.log").write_text("log")
    (runner.REPORTS_DIR / "_prepare_status.json").write_text("{}")
    with SessionLocal() as s:
        runner._test_user_id(s)                                    # the legacy system account
    production = (_production_counts(), _production_queue_lines())
    assert len(list(runner.REPORTS_DIR.glob("scale-*"))) >= 3        # (earlier tests may have left more)

    result = reset.reset_all()

    assert result["ok"], result
    with SessionLocal() as s:
        assert s.query(ScaleTestRun).count() == 0 and s.query(ScaleTestItem).count() == 0
        assert s.query(User).filter(User.username == SCALE_TEST_USERNAME).count() == 0
        audit = s.query(StorageAuditLog).filter(StorageAuditLog.action == "scalability_reset").one()
        assert audit.outcome == "done"
    assert not list(runner.REPORTS_DIR.glob("scale-*"))
    assert not (runner.REPORTS_DIR / "logs").exists() and not (runner.REPORTS_DIR / "_prepare_status.json").exists()
    assert kafka.deleted_records == [(settings.SCALABILITY_KAFKA_TOPIC, 0, 5)]
    assert kafka.events[settings.KAFKA_TOPIC] == 42                          # production topic untouched
    assert kafka.deleted_groups == ["scalability-test"] and "some-production-group" in kafka.groups
    assert _snapshot(photo_folder) == before_photos                          # originals (hard-linked) intact
    assert all(f.exists() for f in shared)                                   # shared folders intact
    assert (_production_counts(), _production_queue_lines()) == production   # production rows untouched


def test_reset_twice_and_with_nothing_to_delete_is_harmless(kafka):
    first = reset.reset_all()
    second = reset.reset_all()
    assert first["ok"] and second["ok"]


def test_a_failed_step_is_never_reported_as_a_complete_reset(photo_folder, test_topic, kafka,  # noqa: F811
                                                             monkeypatch):
    runner.start(10, source_dir=str(photo_folder))

    def locked(*a, **k):
        raise PermissionError("file is open in another program")
    monkeypatch.setattr(reset.shutil, "rmtree", locked)

    result = reset.reset_all()

    assert not result["ok"]
    files = next(s for s in result["steps"] if s["step"].startswith("Delete test images"))
    assert not files["ok"] and "could not be deleted" in files["detail"]
    assert any("test files" in left for left in result["left"])
    assert next(s for s in result["steps"] if s["step"] == "Delete test records")["ok"]   # the rest still ran
    with SessionLocal() as s:
        assert s.query(StorageAuditLog).filter(StorageAuditLog.action == "scalability_reset").one().outcome \
            == "failed"


def test_kafka_down_is_reported(monkeypatch):
    def unreachable(*a, **k):
        raise RuntimeError("broker not reachable")
    monkeypatch.setattr(reset, "_kafka_admin", unreachable)
    result = reset.reset_all()
    kafka_step = next(s for s in result["steps"] if "Kafka" in s["step"])
    assert not result["ok"] and not kafka_step["ok"] and "not reachable" in kafka_step["detail"]


def test_production_topic_and_shared_folders_are_refused(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "SCALABILITY_KAFKA_TOPIC", settings.KAFKA_TOPIC)
    with pytest.raises(reset.UnsafeReset):
        reset.clear_kafka(FakeKafkaAdmin(), lambda t, p: {0: (0, 1)})
    monkeypatch.undo()

    for shared in (settings.BASE_DIR, settings.BASE_DIR / "storage", settings.IMAGE_STORAGE_PATH):
        monkeypatch.setattr(settings, "SCALABILITY_REPORTS_DIR", shared)
        result = reset.reset_all()
        assert not result["ok"] and result["steps"][0]["step"] == "Safety check"


def test_only_scalability_test_processes_are_stopped():
    """A test runner is stopped; a process with a different command (here a
    stand-in for the live Spark job) is not."""
    code = "import time; time.sleep(60)"
    runner_like = subprocess.Popen([sys.executable, "-c", code, "-m", "scalability.run", "start"])
    other = subprocess.Popen([sys.executable, "-c", code, "-m", "spark.streaming_job"])
    try:
        # the stand-in's command line is "python -c <code> -m scalability.run start"
        assert any(p.pid == runner_like.pid for p in reset.test_processes())
        assert reset.stop_test_processes().startswith("stopped")
        assert runner_like.wait(timeout=15) is not None
        assert other.poll() is None                                  # still running
    finally:
        for p in (runner_like, other):
            if p.poll() is None:
                p.kill()


def test_dashboard_delete_test_resets_the_page(photo_folder, test_topic, kafka, drive):  # noqa: F811
    from tests.test_dashboard_users import _logged_in, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    runner.start_series(10, 2, source_dir=str(photo_folder))
    at = _logged_in("Scalability Test")
    assert len(at.metric) == 2

    at.checkbox(key="scale_reset_confirm").check().run()
    at.button(key="scale_reset").click().run()

    assert not at.exception
    assert any("Scalability test deleted" in s.value for s in at.success)
    assert any("Status: Ready" in i.value for i in at.info)
    assert len(at.metric) == 0                                        # averages and totals cleared
    assert not [c for el in at.get("image") for c in (el.captions or []) if c in ("Before", "After")]
    with SessionLocal() as s:
        assert s.query(ScaleTestRun).count() == 0


def test_dashboard_shows_a_failed_reset_as_failed(photo_folder, test_topic, kafka, drive,  # noqa: F811
                                                  monkeypatch):
    from tests.test_dashboard_users import _logged_in, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    runner.start(10, source_dir=str(photo_folder))
    monkeypatch.setattr(reset.shutil, "rmtree", lambda *a, **k: (_ for _ in ()).throw(PermissionError("locked")))
    at = _logged_in("Scalability Test")
    at.checkbox(key="scale_reset_confirm").check().run()
    at.button(key="scale_reset").click().run()

    assert not at.success
    assert any("NOT completely deleted" in e.value for e in at.error)
