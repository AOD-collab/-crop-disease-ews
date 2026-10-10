"""
Duplicate-photo detection (SHA-256 content hash on Image.content_hash).

A repeat upload of the exact same bytes (possibly renamed) is flagged via
`duplicate_of` in the response rather than silently creating an
indistinguishable second row. This is a flag, not a hard reject — a farmer
legitimately re-uploading the same photo (e.g. after a failed submission)
should not be blocked outright.
"""

from fastapi.testclient import TestClient

import pytest

from storage.database import init_db, SessionLocal, Base, engine, Image, User, Role
from api.server import app
from api.auth import hash_password
from tests.conftest import make_leaf_jpeg_bytes


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture()
def client():
    return TestClient(app)


def _make_user(username="farmer1", password="password123"):
    session = SessionLocal()
    try:
        user = User(username=username, email=f"{username}@example.com",
                    password_hash=hash_password(password), role=Role.USER)
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


def test_content_hash_is_stored_on_upload(client):
    _make_user()
    token = _login(client, "farmer1")
    photo_bytes = make_leaf_jpeg_bytes(64)

    resp = client.post(
        "/upload-image", headers=_auth(token), data={"location": "Dili"},
        files={"photo": ("leaf.jpg", photo_bytes, "image/jpeg")},
    )
    assert resp.status_code == 200, resp.text
    image_id = resp.json()["image_id"]

    session = SessionLocal()
    try:
        image = session.get(Image, image_id)
        assert image.content_hash is not None
        assert len(image.content_hash) == 64  # SHA-256 hex digest
    finally:
        session.close()


def test_repeat_upload_is_flagged_as_duplicate(client):
    _make_user()
    token = _login(client, "farmer1")
    photo_bytes = make_leaf_jpeg_bytes(64)

    first = client.post(
        "/upload-image", headers=_auth(token), data={"location": "Dili"},
        files={"photo": ("leaf.jpg", photo_bytes, "image/jpeg")},
    )
    assert first.status_code == 200, first.text
    assert first.json()["duplicate_of"] is None
    first_id = first.json()["image_id"]

    # Same bytes, different filename — still a duplicate.
    second = client.post(
        "/upload-image", headers=_auth(token), data={"location": "Dili"},
        files={"photo": ("leaf_renamed.jpg", photo_bytes, "image/jpeg")},
    )
    assert second.status_code == 200, second.text
    # The upload is NOT rejected — it's flagged, not blocked.
    assert second.json()["status"] == "accepted"
    assert second.json()["duplicate_of"] == first_id


def test_different_photos_are_not_flagged_as_duplicates(client):
    _make_user()
    token = _login(client, "farmer1")

    first = client.post(
        "/upload-image", headers=_auth(token), data={"location": "Dili"},
        files={"photo": ("leaf1.jpg", make_leaf_jpeg_bytes(64), "image/jpeg")},
    )
    second = client.post(
        "/upload-image", headers=_auth(token), data={"location": "Dili"},
        files={"photo": ("leaf2.jpg", make_leaf_jpeg_bytes(64), "image/jpeg")},
    )
    assert first.status_code == 200 and second.status_code == 200
    assert second.json()["duplicate_of"] is None
