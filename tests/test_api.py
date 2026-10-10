import pytest
from fastapi.testclient import TestClient

from storage.database import init_db, SessionLocal, Base, engine
from api.server import app
from api.auth import hash_password
from storage.database import User, Role, UserStatus


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture()
def client():
    return TestClient(app)


def _make_user(session, username, role=Role.USER, password="password123"):
    user = User(username=username, email=f"{username}@example.com",
                password_hash=hash_password(password), role=role)
    session.add(user)
    session.commit()
    session.refresh(user)
    session.expunge(user)  # detach with attributes already loaded, so callers can read them after session.close()
    return user


def _login(client, username, password="password123"):
    resp = client.post("/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_login_rejects_wrong_password(client):
    session = SessionLocal()
    _make_user(session, "farmer1")
    session.close()
    resp = client.post("/auth/login", json={"username": "farmer1", "password": "wrong"})
    assert resp.status_code == 401


def test_login_rejects_deactivated_account(client):
    session = SessionLocal()
    _make_user(session, "farmer1")
    db_user = session.query(User).filter(User.username == "farmer1").one()
    db_user.status = UserStatus.INACTIVE
    session.commit()
    session.close()
    resp = client.post("/auth/login", json={"username": "farmer1", "password": "password123"})
    assert resp.status_code == 403


def test_upload_requires_auth(client):
    resp = client.post("/upload-image", data={"location": "Dili"})
    assert resp.status_code == 401


def test_upload_accepts_any_location_string(client, sample_leaf_image_bytes):
    """Location is a free-text display/filter label, not an access-control
    boundary (the old field-assignment restriction this replaced no longer
    exists) — any authenticated, active USER can upload with any location,
    including an unrecognized one."""
    session = SessionLocal()
    _make_user(session, "farmer1")
    session.close()
    token = _login(client, "farmer1")

    resp = client.post(
        "/upload-image",
        headers={"Authorization": f"Bearer {token}"},
        data={"location": "Somewhere Made Up"},
        files={"photo": ("leaf.jpg", sample_leaf_image_bytes, "image/jpeg")},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["location"] == "Somewhere Made Up"


def test_upload_without_location_is_allowed(client, sample_leaf_image_bytes):
    """location is nullable — omitting it entirely must not break the
    upload workflow."""
    session = SessionLocal()
    _make_user(session, "farmer1")
    session.close()
    token = _login(client, "farmer1")

    resp = client.post(
        "/upload-image",
        headers={"Authorization": f"Bearer {token}"},
        files={"photo": ("leaf.jpg", sample_leaf_image_bytes, "image/jpeg")},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["location"] is None


def test_full_upload_workflow(client, sample_leaf_image_bytes):
    session = SessionLocal()
    _make_user(session, "farmer1")
    session.close()
    token = _login(client, "farmer1")

    resp = client.post(
        "/upload-image",
        headers={"Authorization": f"Bearer {token}"},
        data={"location": "Dili"},
        files={"photo": ("leaf.jpg", sample_leaf_image_bytes, "image/jpeg")},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["location"] == "Dili"
    # server generates uploaded_at itself
    assert body["uploaded_at"] is not None

    # USER can see it in their own history
    my_images = client.get("/my/images", headers={"Authorization": f"Bearer {token}"})
    assert my_images.status_code == 200
    assert len(my_images.json()) == 1
    assert my_images.json()[0]["location"] == "Dili"


def test_upload_rejects_non_image_file(client):
    session = SessionLocal()
    _make_user(session, "farmer1")
    session.close()
    token = _login(client, "farmer1")

    resp = client.post(
        "/upload-image",
        headers={"Authorization": f"Bearer {token}"},
        data={"location": "Dili"},
        files={"photo": ("notes.txt", b"hello world", "text/plain")},
    )
    assert resp.status_code == 400


def test_user_cannot_access_another_users_image(client, sample_leaf_image_bytes):
    session = SessionLocal()
    _make_user(session, "farmer1")
    _make_user(session, "farmer2")
    session.close()

    token1 = _login(client, "farmer1")
    token2 = _login(client, "farmer2")

    upload = client.post(
        "/upload-image",
        headers={"Authorization": f"Bearer {token1}"},
        data={"location": "Dili"},
        files={"photo": ("leaf.jpg", sample_leaf_image_bytes, "image/jpeg")},
    )
    image_id = upload.json()["image_id"]

    # farmer2 must NOT be able to read farmer1's image
    resp = client.get(f"/images/{image_id}", headers={"Authorization": f"Bearer {token2}"})
    assert resp.status_code == 403


def test_user_cannot_reach_admin_endpoints(client):
    session = SessionLocal()
    _make_user(session, "farmer1")
    session.close()
    token = _login(client, "farmer1")

    resp = client.get("/admin/users", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 403


def test_admin_can_manage_users(client):
    session = SessionLocal()
    _make_user(session, "admin1", role=Role.ADMIN)
    session.close()
    token = _login(client, "admin1")

    create_user = client.post(
        "/admin/users", headers={"Authorization": f"Bearer {token}"},
        json={"username": "farmer2", "full_name": "Farmer Two", "email": "f2@example.com",
              "password": "abc12345", "role": "USER", "status": "ACTIVE"},
    )
    assert create_user.status_code == 200, create_user.text
    user_id = create_user.json()["user_id"]

    # new user can log in and use the client-side of the system immediately
    user_token = _login(client, "farmer2", password="abc12345")
    my_images = client.get("/my/images", headers={"Authorization": f"Bearer {user_token}"})
    assert my_images.status_code == 200

    listing = client.get("/admin/users", headers={"Authorization": f"Bearer {token}"})
    assert listing.status_code == 200
    assert any(u["user_id"] == user_id for u in listing.json()["items"])


def test_admin_analytics_survives_unprocessed_image(client, sample_leaf_image_bytes):
    """Regression test: an image with no Prediction row yet (the normal
    state right after upload, before Spark has caught up) must not crash
    /admin/analytics. location_summary()'s per-location avg_risk_score is
    the mean of an all-NaN group in this case, and NaN isn't valid JSON —
    it must be cleaned before crossing the API boundary, not just when a
    fully-processed dataset happens to be under test."""
    session = SessionLocal()
    user = _make_user(session, "farmer1")
    _make_user(session, "admin1", role=Role.ADMIN)
    session.close()
    token = _login(client, "farmer1")

    upload = client.post(
        "/upload-image",
        headers={"Authorization": f"Bearer {token}"},
        data={"location": "Dili"},
        files={"photo": ("leaf.jpg", sample_leaf_image_bytes, "image/jpeg")},
    )
    assert upload.status_code == 200, upload.text

    admin_token = _login(client, "admin1")
    resp = client.get("/admin/analytics", headers={"Authorization": f"Bearer {admin_token}"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert any(row["location"] == "Dili" for row in body["location_summary"])


def test_client_cannot_forge_admin_role_via_request_body(client):
    """A USER-supplied 'role' in a request must never grant privileges —
    the role always comes from the DB row tied to the authenticated token."""
    session = SessionLocal()
    _make_user(session, "farmer1")
    session.close()
    token = _login(client, "farmer1")

    # Even if a malicious client tries to smuggle role=ADMIN into an
    # unrelated authenticated request, admin-only routes stay blocked.
    resp = client.get("/admin/users", headers={"Authorization": f"Bearer {token}"},
                       params={"role": "ADMIN"})
    assert resp.status_code == 403
