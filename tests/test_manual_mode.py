"""
Manual vs. automatic image-processing mode.

AUTOMATIC (default): /upload-image publishes to Kafka immediately and the
image is marked INGESTED — the original behaviour, which these tests pin so
a regression is caught.

MANUAL: /upload-image stores the image at PENDING_REVIEW and publishes
nothing; an admin then releases it with POST /admin/images/{id}/process,
which runs the identical publish step.

The test suite forces the local Kafka file-queue fallback (see
tests/conftest.py), so "publish" here means "appended to the local queue
file" and the INGESTED transition is still exercised without a broker.
"""

import pytest
from fastapi.testclient import TestClient

from storage.database import (
    init_db, SessionLocal, Base, engine, Image, User, Role, ProcessingStatus,
)
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


def _upload(client, token, location="Dili"):
    return client.post(
        "/upload-image",
        headers=_auth(token),
        data={"location": location},
        files={"photo": ("leaf.jpg", make_leaf_jpeg_bytes(64), "image/jpeg")},
    )


def _status_of(image_id):
    session = SessionLocal()
    try:
        row = session.get(Image, image_id)
        return row.status if row is not None else None
    finally:
        session.close()


# ---------------------------------------------------------------------------
# mode read / switch
# ---------------------------------------------------------------------------
def test_default_mode_is_auto(client):
    _make_user("admin1", role=Role.ADMIN)
    token = _login(client, "admin1")
    resp = client.get("/admin/processing-mode", headers=_auth(token))
    assert resp.status_code == 200
    assert resp.json()["mode"] == "AUTOMATIC"


def test_admin_can_switch_mode(client):
    _make_user("admin1", role=Role.ADMIN)
    token = _login(client, "admin1")

    assert client.put("/admin/processing-mode", headers=_auth(token),
                      json={"mode": "MANUAL"}).json()["mode"] == "MANUAL"
    assert client.get("/admin/processing-mode", headers=_auth(token)).json()["mode"] == "MANUAL"

    # case-insensitive, and switches back
    assert client.put("/admin/processing-mode", headers=_auth(token),
                      json={"mode": "automatic"}).json()["mode"] == "AUTOMATIC"


def test_non_admin_cannot_switch_mode(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    assert client.get("/admin/processing-mode", headers=_auth(token)).status_code == 403
    assert client.put("/admin/processing-mode", headers=_auth(token),
                      json={"mode": "MANUAL"}).status_code == 403


def test_switch_mode_rejects_garbage(client):
    _make_user("admin1", role=Role.ADMIN)
    token = _login(client, "admin1")
    resp = client.put("/admin/processing-mode", headers=_auth(token), json={"mode": "banana"})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# upload behaviour per mode
# ---------------------------------------------------------------------------
def test_auto_mode_upload_still_acked_and_ingested(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")

    resp = _upload(client, token)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "accepted"
    assert _status_of(body["image_id"]) == ProcessingStatus.INGESTED


def test_manual_upload_is_held_at_pending(client):
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "MANUAL"})

    resp = _upload(client, user_token)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "pending_review"
    assert _status_of(body["image_id"]) == ProcessingStatus.PENDING_REVIEW

    pending = client.get("/admin/pending-images", headers=_auth(admin_token)).json()
    assert [i["image_id"] for i in pending] == [body["image_id"]]


def test_switching_manual_to_automatic_drains_the_backlog(client):
    """Switching MANUAL -> AUTOMATIC releases everything held at
    PENDING_REVIEW into the pipeline (so the admin doesn't also have to
    click 'Process all'), and new uploads then go straight through."""
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    # 1. MANUAL: three uploads pile up
    client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "MANUAL"})
    held_ids = []
    for _ in range(3):
        r = _upload(client, user_token)
        assert r.json()["status"] == "pending_review"
        held_ids.append(r.json()["image_id"])
    assert all(_status_of(i) == ProcessingStatus.PENDING_REVIEW for i in held_ids)

    # 2. switch to AUTOMATIC -> the backlog drains
    switched = client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "AUTOMATIC"})
    assert switched.status_code == 200
    body = switched.json()
    assert body["mode"] == "AUTOMATIC"
    assert body["released"] == 3
    assert body["released_failed"] == 0

    assert all(_status_of(i) == ProcessingStatus.INGESTED for i in held_ids)
    assert client.get("/admin/pending-images", headers=_auth(admin_token)).json() == []

    # 3. new uploads now go straight through
    new_resp = _upload(client, user_token)
    assert new_resp.json()["status"] == "accepted"
    assert _status_of(new_resp.json()["image_id"]) == ProcessingStatus.INGESTED


def test_switch_to_manual_never_drains(client):
    """The drain only happens on the AUTOMATIC side — switching MANUAL
    reports released=0 and touches nothing."""
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    _upload(client, user_token)  # AUTOMATIC by default -> INGESTED
    resp = client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "MANUAL"})
    assert resp.json() == {"mode": "MANUAL", "released": 0, "released_failed": 0}


def test_mode_switch_round_trips(client):
    """MANUAL -> AUTOMATIC -> MANUAL leaves the setting where it was put,
    each read reflecting the last write."""
    _make_user("admin1", role=Role.ADMIN)
    token = _login(client, "admin1")

    for want in ["MANUAL", "AUTOMATIC", "MANUAL", "AUTOMATIC"]:
        put = client.put("/admin/processing-mode", headers=_auth(token), json={"mode": want})
        assert put.json()["mode"] == want
        assert client.get("/admin/processing-mode", headers=_auth(token)).json()["mode"] == want


# ---------------------------------------------------------------------------
# the "Process" button
# ---------------------------------------------------------------------------
def test_admin_process_releases_a_pending_image(client):
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "MANUAL"})
    image_id = _upload(client, user_token).json()["image_id"]

    resp = client.post(f"/admin/images/{image_id}/process", headers=_auth(admin_token))
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "INGESTED"
    assert _status_of(image_id) == ProcessingStatus.INGESTED

    assert client.get("/admin/pending-images", headers=_auth(admin_token)).json() == []


def test_process_already_ingested_is_rejected(client):
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    # default AUTOMATIC -> already INGESTED
    image_id = _upload(client, user_token).json()["image_id"]
    resp = client.post(f"/admin/images/{image_id}/process", headers=_auth(admin_token))
    assert resp.status_code == 409


def test_process_unknown_image_is_404(client):
    _make_user("admin1", role=Role.ADMIN)
    token = _login(client, "admin1")
    assert client.post("/admin/images/does-not-exist/process",
                       headers=_auth(token)).status_code == 404


def test_non_admin_cannot_process(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    assert client.post("/admin/images/whatever/process", headers=_auth(token)).status_code == 403


def test_manual_mode_survives_factory_reset(client):
    """After a factory reset (all pipeline data wiped, users kept) the
    processing mode must still be whatever the admin set it to — it is
    system config, not pipeline data — and a brand-new upload must still be
    HELD at PENDING_REVIEW, not auto-ingested."""
    from storage.reset import factory_reset

    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "MANUAL"})
    first_id = _upload(client, user_token).json()["image_id"]
    assert _status_of(first_id) == ProcessingStatus.PENDING_REVIEW

    factory_reset()

    assert client.get("/admin/processing-mode", headers=_auth(admin_token)).json()["mode"] == "MANUAL"
    assert _status_of(first_id) is None  # the held image row was wiped

    resp = _upload(client, user_token)
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "pending_review"
    assert _status_of(resp.json()["image_id"]) == ProcessingStatus.PENDING_REVIEW


def test_admin_images_status_filter(client):
    _make_user("admin1", role=Role.ADMIN)
    _make_user("farmer1")
    admin_token = _login(client, "admin1")
    user_token = _login(client, "farmer1")

    # AUTOMATIC upload first (-> INGESTED), then switch to MANUAL and upload
    # one that stays held — ordering matters because switching to AUTOMATIC
    # now drains anything held.
    ingested_id = _upload(client, user_token).json()["image_id"]

    client.put("/admin/processing-mode", headers=_auth(admin_token), json={"mode": "MANUAL"})
    held_id = _upload(client, user_token).json()["image_id"]

    pending = client.get("/admin/images?status_filter=PENDING_REVIEW", headers=_auth(admin_token)).json()
    assert {i["image_id"] for i in pending} == {held_id}

    ingested = client.get("/admin/images?status_filter=INGESTED", headers=_auth(admin_token)).json()
    assert ingested_id in {i["image_id"] for i in ingested}

    bad = client.get("/admin/images?status_filter=NONSENSE", headers=_auth(admin_token))
    assert bad.status_code == 400
