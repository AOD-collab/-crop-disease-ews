"""
Batch image upload (POST /upload-images) — the Android app's multi-select
counterpart to /upload-image (section 11).

Each photo in the batch is processed independently: one rejected or failed
file must not stop the rest, and the response carries a per-file result so
the client can show partial success. The endpoint otherwise honors the same
manual/automatic processing-mode switch as the single-image path (see
test_manual_mode.py).
"""

import pytest
from io import BytesIO

import numpy as np
from PIL import Image as PILImage
from fastapi.testclient import TestClient

from tests.conftest import make_leaf_jpeg_bytes
from storage.database import (
    init_db, SessionLocal, Base, engine, Image, User, Role, ProcessingStatus,
)
from api.server import app, _MAX_BATCH_UPLOAD
from api.auth import hash_password


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture()
def client():
    return TestClient(app)


def _make_user(username, role=Role.USER, password="password123"):
    session = SessionLocal()
    try:
        user = User(username=username, email=f"{username}@example.com",
                    password_hash=hash_password(password), role=role)
        session.add(user)
        session.commit()
        session.refresh(user)
        session.expunge(user)
        return user
    finally:
        session.close()


def _login(client, username, password="password123"):
    resp = client.post("/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _leaf_jpeg_bytes():
    return make_leaf_jpeg_bytes(64)


def _status_of(image_id):
    session = SessionLocal()
    try:
        row = session.get(Image, image_id)
        return row.status if row is not None else None
    finally:
        session.close()


def _batch_upload(client, token, count, location="Dili"):
    files = [("photos", (f"leaf{i}.jpg", _leaf_jpeg_bytes(), "image/jpeg")) for i in range(count)]
    return client.post(
        "/upload-images",
        headers=_auth(token),
        data={"location": location},
        files=files,
    )


def test_batch_upload_requires_auth(client):
    resp = client.post("/upload-images", files=[("photos", ("a.jpg", b"x", "image/jpeg"))])
    assert resp.status_code == 401


def test_batch_upload_rejects_missing_photos(client):
    """`photos` is a required field — FastAPI itself 422s when it's absent
    (there's no way to submit a "present but empty" file list over
    multipart, so the app's own `if not photos` guard is unreachable in
    practice but kept as a defensive backstop)."""
    _make_user("farmer1")
    token = _login(client, "farmer1")
    resp = client.post("/upload-images", headers=_auth(token), data={"location": "Dili"})
    assert resp.status_code == 422


def test_batch_upload_rejects_over_cap(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    files = [("photos", (f"leaf{i}.jpg", b"x", "image/jpeg")) for i in range(_MAX_BATCH_UPLOAD + 1)]
    resp = client.post("/upload-images", headers=_auth(token), files=files)
    assert resp.status_code == 400


def test_batch_upload_multiple_images_accepted_in_auto_mode(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")

    resp = _batch_upload(client, token, count=3)
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    assert len(results) == 3
    assert all(r["status"] == "accepted" for r in results)
    assert all(_status_of(r["image_id"]) == ProcessingStatus.INGESTED for r in results)


def test_batch_upload_honors_manual_mode(client):
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "MANUAL"})

    resp = _batch_upload(client, user_token, count=2)
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    assert all(r["status"] == "pending_review" for r in results)
    assert all(_status_of(r["image_id"]) == ProcessingStatus.PENDING_REVIEW for r in results)

    pending = client.get("/admin/pending-images", headers=_auth(admin_token)).json()
    assert {i["image_id"] for i in pending} == {r["image_id"] for r in results}


def test_batch_upload_isolates_a_bad_file_from_the_rest(client):
    """One non-image file in the batch is reported as rejected — the other,
    valid photos in the same request still go through."""
    _make_user("farmer1")
    token = _login(client, "farmer1")

    files = [
        ("photos", ("leaf1.jpg", _leaf_jpeg_bytes(), "image/jpeg")),
        ("photos", ("notes.txt", b"hello world", "text/plain")),
        ("photos", ("leaf2.jpg", _leaf_jpeg_bytes(), "image/jpeg")),
    ]
    resp = client.post("/upload-images", headers=_auth(token), data={"location": "Dili"}, files=files)
    assert resp.status_code == 200, resp.text
    results = resp.json()["results"]
    assert len(results) == 3

    by_name = {r["filename"]: r for r in results}
    assert by_name["notes.txt"]["status"] == "rejected"
    assert by_name["notes.txt"]["image_id"] is None
    assert by_name["leaf1.jpg"]["status"] == "accepted"
    assert by_name["leaf2.jpg"]["status"] == "accepted"
    assert _status_of(by_name["leaf1.jpg"]["image_id"]) == ProcessingStatus.INGESTED
    assert _status_of(by_name["leaf2.jpg"]["image_id"]) == ProcessingStatus.INGESTED


def test_batch_upload_shares_location_across_all_photos(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")

    resp = _batch_upload(client, token, count=2, location="Baucau")
    assert resp.status_code == 200, resp.text
    for r in resp.json()["results"]:
        session = SessionLocal()
        try:
            row = session.get(Image, r["image_id"])
            assert row.location == "Baucau"
        finally:
            session.close()
