"""
GET /images/{image_id}/file — serves the actual uploaded photo (owner or
admin only). Added so the Android app's history/recent-diagnoses screens can
show a real thumbnail of what was uploaded, not just prediction data.
"""

from io import BytesIO

import pytest
from PIL import Image as PILImage
from fastapi.testclient import TestClient

from storage.database import init_db, SessionLocal, Base, engine, Role
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


def _make_user(username, role=Role.USER, password="password123"):
    session = SessionLocal()
    try:
        from storage.database import User
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


def _leaf_jpeg_bytes(size=(200, 200)):
    return make_leaf_jpeg_bytes(size[0])


def _upload(client, token, location="Dili"):
    return client.post(
        "/upload-image",
        headers=_auth(token),
        data={"location": location},
        files={"photo": ("leaf.jpg", _leaf_jpeg_bytes(), "image/jpeg")},
    )


def test_requires_auth(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    image_id = _upload(client, token).json()["image_id"]
    assert client.get(f"/images/{image_id}/file").status_code == 401


def test_owner_can_fetch_their_own_image(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    image_id = _upload(client, token).json()["image_id"]

    resp = client.get(f"/images/{image_id}/file", headers=_auth(token))
    assert resp.status_code == 200
    assert PILImage.open(BytesIO(resp.content)).size == (200, 200)


def test_other_user_is_forbidden(client):
    _make_user("farmer1")
    _make_user("farmer2")
    token1 = _login(client, "farmer1")
    token2 = _login(client, "farmer2")
    image_id = _upload(client, token1).json()["image_id"]

    resp = client.get(f"/images/{image_id}/file", headers=_auth(token2))
    assert resp.status_code == 403


def test_admin_can_fetch_any_image(client):
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")
    image_id = _upload(client, user_token).json()["image_id"]

    resp = client.get(f"/images/{image_id}/file", headers=_auth(admin_token))
    assert resp.status_code == 200


def test_max_size_resizes_thumbnail(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    image_id = _upload(client, token).json()["image_id"]

    resp = client.get(f"/images/{image_id}/file?max_size=64", headers=_auth(token))
    assert resp.status_code == 200
    thumb = PILImage.open(BytesIO(resp.content))
    assert max(thumb.size) <= 64
    assert thumb.format == "JPEG"


def test_max_size_is_capped(client):
    """A caller can't ask for an oversized thumbnail — it's clamped, not
    rejected, since a client-supplied cap is just a hint, not a boundary
    worth erroring over."""
    _make_user("farmer1")
    token = _login(client, "farmer1")
    image_id = _upload(client, token).json()["image_id"]

    resp = client.get(f"/images/{image_id}/file?max_size=999999", headers=_auth(token))
    assert resp.status_code == 200
    thumb = PILImage.open(BytesIO(resp.content))
    assert max(thumb.size) <= 1024


def test_unknown_image_is_404(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    assert client.get("/images/does-not-exist/file", headers=_auth(token)).status_code == 404
