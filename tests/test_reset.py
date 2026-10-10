"""Factory reset: what it clears, and that the optional Kafka / Google Drive
steps are opt-in and never abort the rest of the reset."""

import pytest

from config.settings import SPARK_CHECKPOINT_ROOT
from storage import reset
from storage.database import (
    Base, engine, init_db, SessionLocal, User, Image, RejectedEvent,
)


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


def _seed():
    with SessionLocal() as s:
        user = User(username="farmer1", email="farmer1@example.com", password_hash="x")
        s.add(user)
        s.flush()
        s.add(Image(user_id=user.user_id, image_path="drive:abc"))
        s.add(RejectedEvent(image_id=None, raw_event="{}", reason="no image_id"))
        s.commit()


def test_reset_clears_rejected_events_and_keeps_users():
    _seed()
    summary = reset.factory_reset()
    with SessionLocal() as s:
        assert s.query(RejectedEvent).count() == 0
        assert s.query(Image).count() == 0
        assert s.query(User).count() == 1
    assert summary["rejected_events_deleted"] == 1
    # opt-in steps are skipped unless asked for
    assert summary["kafka_topic"] is None
    assert summary["drive_files"] is None


def test_tests_never_touch_the_real_spark_checkpoints():
    assert "crop_disease_test_" in str(SPARK_CHECKPOINT_ROOT)
    assert reset.SPARK_CHECKPOINT_DIR == SPARK_CHECKPOINT_ROOT


def test_cli_refuses_while_spark_job_runs(monkeypatch):
    _seed()
    monkeypatch.setattr(reset, "spark_job_running", lambda: True)
    with pytest.raises(SystemExit):
        reset._main(["--yes"])
    with SessionLocal() as s:
        assert s.query(Image).count() == 1  # nothing was wiped


def test_cli_refuses_while_drive_watcher_runs(monkeypatch):
    _seed()
    monkeypatch.setattr(reset, "spark_job_running", lambda: False)
    monkeypatch.setattr(reset, "drive_watcher_running", lambda: True)
    with pytest.raises(SystemExit):
        reset._main(["--yes"])
    with SessionLocal() as s:
        assert s.query(Image).count() == 1  # nothing was wiped


def test_kafka_failure_does_not_abort_reset(monkeypatch):
    import confluent_kafka.admin as kadmin

    class DeadAdmin:
        def __init__(self, conf):
            raise RuntimeError("broker down")

    monkeypatch.setattr(kadmin, "AdminClient", DeadAdmin)
    _seed()
    summary = reset.factory_reset(clear_kafka=True)
    assert summary["kafka_topic"].startswith("FAILED")
    assert summary["images_deleted"] == 1  # DB part still happened


class _Resp:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = ""


class _FakeDrive:
    """Results (central-owned) can be trashed; farmer-owned photos answer
    403 to a trash request and must instead be detached from their folder."""

    def __init__(self):
        self.calls = []

    def patch(self, url, params=None, json=None):
        self.calls.append((url.rsplit("/", 1)[-1], params, json))
        file_id = url.rsplit("/", 1)[-1]
        if file_id == "farmerPhoto0001" and json == {"trashed": True}:
            return _Resp(403)
        return _Resp(200)


def test_drive_reset_trashes_results_and_detaches_farmer_photos(monkeypatch):
    from storage import drive_client

    fake = _FakeDrive()
    monkeypatch.setattr(drive_client, "_authed_session", lambda: fake)
    monkeypatch.setattr(drive_client, "images_folder_id", lambda: "imagesRoot0001")
    monkeypatch.setattr(drive_client, "results_folder_id", lambda: "resultsRoot001")
    monkeypatch.setattr(drive_client, "list_subfolders",
                        lambda root: [{"id": "imgFolder0001"}] if root == "imagesRoot0001" else [{"id": "resFolder0001"}])
    monkeypatch.setattr(drive_client, "list_files_in_folders", lambda ids: [
        {"id": "farmerPhoto0001", "name": "a.jpg", "parents": ["imgFolder0001", "elsewhere0001"]},
        {"id": "resultJson0001", "name": "a.json", "parents": ["resFolder0001"]},
    ])

    markers = []
    monkeypatch.setattr(drive_client, "upload_json",
                        lambda name, data, parent, props, may_exist=True: markers.append((name, parent, props)))

    summary = reset.clear_drive_files()

    assert summary == {"trashed": 1, "removed_from_folder": 1, "reset_markers": 1, "errors": []}
    detach = [c for c in fake.calls if c[0] == "farmerPhoto0001" and c[1].get("removeParents")]
    # only the farmer's /images/<email>/ folder is removed, never other parents
    assert detach and detach[0][1]["removeParents"] == "imgFolder0001"
    # the phone's reset signal goes to the results folder only, with no image_id
    # (so the app's result listings never mistake it for a photo's result)
    [(name, parent, props)] = markers
    assert (name, parent) == (reset.RESET_MARKER_NAME, "resFolder0001")
    assert props["kind"] == "reset_marker" and props["reset_at"] and "image_id" not in props


def test_reset_marker_failure_is_reported_not_raised(monkeypatch):
    from storage import drive_client

    def offline(*args, **kwargs):
        raise ConnectionError("Drive down")

    monkeypatch.setattr(drive_client, "upload_json", offline)
    summary = {"reset_markers": 0, "errors": []}
    reset.write_reset_markers(["resFolder0001"], summary)
    assert summary["reset_markers"] == 0 and "resFolder0001" in summary["errors"][0]


def test_drive_unreachable_is_reported_not_raised(monkeypatch):
    from storage import drive_client

    def offline():
        raise ConnectionError("Failed to resolve 'www.googleapis.com'")

    monkeypatch.setattr(drive_client, "_authed_session", offline)
    summary = reset.clear_drive_files()
    assert summary["trashed"] == 0 and summary["errors"]
