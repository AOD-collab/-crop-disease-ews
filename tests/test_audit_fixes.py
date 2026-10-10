"""
Regression tests for the 2026-10-10 audit fixes:

- watcher-rejected uploads can't be re-sent through the pipeline (dashboard
  Reprocess / Retry used to bypass the rejection);
- storage.requeue_stuck also finds PROCESSING rows and COMPLETED Drive photos
  whose result never reached Drive (both were stranded for good), and
  filters in SQL;
- spark.preprocessing.replay_state gives the same answers as the three
  separate checks it replaces in the Spark sink;
- GET /images/{id}/file serves Google Drive photos (it answered 404).
"""
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from api.auth import hash_password
from api.server import app
from storage.database import (
    init_db, SessionLocal, Base, engine, Image, User, Role, UserStatus, ProcessingStatus, now_utc,
    SCALE_TEST_SOURCE,
)
from tests.conftest import make_leaf_jpeg_bytes


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


def _user(username="farmer@gmail.com", role=Role.USER, password="password123") -> str:
    with SessionLocal() as s:
        u = User(username=username, email=username, password_hash=hash_password(password),
                 role=role, status=UserStatus.ACTIVE)
        s.add(u)
        s.commit()
        return u.user_id


def _image(user_id, image_id, status, *, path=None, age_minutes=10, published=False,
           error=None, source="android_drive_upload") -> str:
    t = now_utc() - timedelta(minutes=age_minutes)
    with SessionLocal() as s:
        s.add(Image(image_id=image_id, user_id=user_id, image_path=path or f"gdrive:file-{image_id}",
                    status=status, source=source, uploaded_at=t, ingested_at=t,
                    processed_at=t if status in (ProcessingStatus.COMPLETED, ProcessingStatus.FAILED) else None,
                    result_published_at=t if published else None, error_message=error))
        s.commit()
    return image_id


# ------------------------------------------------ rejected uploads stay rejected
class _Backend:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)


def test_watcher_rejected_upload_cannot_be_resubmitted():
    from storage.pipeline_submit import submit_image_for_processing, NotResubmittable, UPLOAD_REJECTED_PREFIX
    uid = _user()
    _image(uid, "rej", ProcessingStatus.FAILED,
           error=UPLOAD_REJECTED_PREFIX + "your account is not active — contact the administrator")
    backend = _Backend()
    with SessionLocal() as s:
        img = s.get(Image, "rej")
        with pytest.raises(NotResubmittable):
            submit_image_for_processing(s, img, backend=backend)
    assert backend.sent == []
    with SessionLocal() as s:
        img = s.get(Image, "rej")
        assert img.status == ProcessingStatus.FAILED and img.error_message.startswith(UPLOAD_REJECTED_PREFIX)


def test_other_failed_images_can_still_be_reprocessed():
    from storage.pipeline_submit import submit_image_for_processing
    uid = _user()
    _image(uid, "cnnfail", ProcessingStatus.FAILED, error="CUDA error: out of memory")
    backend = _Backend()
    with SessionLocal() as s:
        submit_image_for_processing(s, s.get(Image, "cnnfail"), backend=backend)
    assert [m["image_id"] for m in backend.sent] == ["cnnfail"]
    with SessionLocal() as s:
        assert s.get(Image, "cnnfail").status == ProcessingStatus.INGESTED


def test_drive_watcher_uses_the_shared_rejection_prefix():
    from kafka import drive_watcher
    from storage.pipeline_submit import UPLOAD_REJECTED_PREFIX
    assert drive_watcher.UPLOAD_REJECTED_PREFIX is UPLOAD_REJECTED_PREFIX


# ------------------------------------------------ requeue_stuck selection
def test_find_stuck_includes_processing_and_undelivered_results():
    from storage.requeue_stuck import find_stuck, count_owed
    uid = _user()
    _image(uid, "ingested", ProcessingStatus.INGESTED)
    _image(uid, "processing", ProcessingStatus.PROCESSING)                    # Spark died mid-photo
    _image(uid, "undelivered", ProcessingStatus.COMPLETED)                    # Drive upload failed
    _image(uid, "delivered", ProcessingStatus.COMPLETED, published=True)
    _image(uid, "local_done", ProcessingStatus.COMPLETED, path="storage/images/x.jpg")   # no Drive result
    _image(uid, "fresh_undelivered", ProcessingStatus.COMPLETED, age_minutes=0)  # upload may be running
    _image(uid, "rejected", ProcessingStatus.FAILED, error="Upload rejected: empty file")
    _image(uid, "kafka_fail", ProcessingStatus.FAILED, error="Kafka publish failed: broker down")
    _image(uid, "scale", ProcessingStatus.INGESTED, source=SCALE_TEST_SOURCE)
    _image(uid, "no_source", ProcessingStatus.INGESTED, source=None)          # NULL source still counts

    with SessionLocal() as s:
        assert {i.image_id for i in find_stuck(s, 120, include_failed=False, ignore_age=False)} == {
            "ingested", "processing", "undelivered", "no_source"}
        assert {i.image_id for i in find_stuck(s, 120, include_failed=True, ignore_age=False)} == {
            "ingested", "processing", "undelivered", "no_source", "kafka_fail"}
        assert "fresh_undelivered" in {i.image_id for i in find_stuck(s, 120, False, ignore_age=True)}
        assert count_owed(s) == {"waiting": 3, "undelivered": 1}


def test_requeued_undelivered_result_is_processed_again_by_spark():
    """The event requeue_stuck re-sends for an undelivered result is NOT
    skipped by the Spark sink as an already-COMPLETED replay."""
    from spark.preprocessing import replay_state
    uid = _user()
    _image(uid, "undelivered", ProcessingStatus.COMPLETED)
    assert replay_state("undelivered") == "result_missing"


# ------------------------------------------------ replay_state == the old three checks
@pytest.mark.parametrize("status,path,published", [
    (ProcessingStatus.PENDING_REVIEW, None, False),
    (ProcessingStatus.COMPLETED, None, True),
    (ProcessingStatus.COMPLETED, None, False),
    (ProcessingStatus.COMPLETED, "storage/images/x.jpg", False),
    (ProcessingStatus.INGESTED, None, False),
    (ProcessingStatus.FAILED, None, False),
    (ProcessingStatus.PROCESSING, None, False),
])
def test_replay_state_matches_the_separate_checks(status, path, published):
    from spark.preprocessing import replay_state, held_for_review, already_completed, needs_result_upload
    uid = _user()
    _image(uid, "img", status, path=path, published=published)
    if held_for_review("img"):
        expected = "held"
    elif already_completed("img"):
        expected = "result_missing" if needs_result_upload("img") else "completed"
    else:
        expected = None
    assert replay_state("img") == expected
    assert replay_state("missing-id") is None and replay_state(None) is None


# ------------------------------------------------ /images/{id}/file for Drive photos
def _token(client, username):
    resp = client.post("/auth/login", json={"username": username, "password": "password123"})
    assert resp.status_code == 200, resp.text
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


def test_image_file_serves_the_local_copy_of_a_drive_photo():
    from api.image_storage import store_downloaded_photo
    client = TestClient(app)
    uid = _user()
    _image(uid, "drivepic", ProcessingStatus.COMPLETED)
    photo = make_leaf_jpeg_bytes(80)
    store_downloaded_photo(uid, "drivepic", photo)
    headers = _token(client, "farmer@gmail.com")

    resp = client.get("/images/drivepic/file", headers=headers)
    assert resp.status_code == 200 and resp.content == photo
    thumb = client.get("/images/drivepic/file?max_size=32", headers=headers)
    assert thumb.status_code == 200 and thumb.headers["content-type"] == "image/jpeg"


def test_image_file_falls_back_to_google_drive(monkeypatch):
    import api.server as server
    client = TestClient(app)
    uid = _user()
    _image(uid, "drivepic2", ProcessingStatus.COMPLETED)
    photo = make_leaf_jpeg_bytes(64)
    asked = []
    monkeypatch.setattr(server, "load_image_bytes", lambda stored: asked.append(stored) or photo)
    headers = _token(client, "farmer@gmail.com")

    resp = client.get("/images/drivepic2/file", headers=headers)
    assert resp.status_code == 200 and resp.content == photo
    assert asked == ["gdrive:file-drivepic2"]

    def missing(stored):
        raise FileNotFoundError(stored)
    monkeypatch.setattr(server, "load_image_bytes", missing)
    assert client.get("/images/drivepic2/file", headers=headers).status_code == 404


def test_image_file_of_a_drive_photo_still_checks_ownership(monkeypatch):
    import api.server as server
    client = TestClient(app)
    owner = _user("owner@gmail.com")
    _user("other@gmail.com")
    _image(owner, "private", ProcessingStatus.COMPLETED)
    monkeypatch.setattr(server, "load_image_bytes", lambda stored: make_leaf_jpeg_bytes(32))
    assert client.get("/images/private/file", headers=_token(client, "other@gmail.com")).status_code == 403
