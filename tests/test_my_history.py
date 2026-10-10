"""
GET /my/history — a user's own upload history, scoped strictly to their own
images, with each image tagged "healthy"/"diseased"/"pending"/"failed" and a
summary count across the user's WHOLE history (not just the current page).

This is the per-user counterpart to the admin dashboard: same idea (browse
uploads by outcome) but scoped to one user's own images, and with the
outcome classification done server-side instead of left to the client.
"""

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from storage.database import (
    init_db, SessionLocal, Base, engine, User, Image, Prediction,
    Role, ProcessingStatus, RiskLevel,
)
from api.server import app
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


def _make_user(username, password="password123"):
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


def _seed_image(user_id, image_id, status=ProcessingStatus.COMPLETED, uploaded_at=None):
    session = SessionLocal()
    try:
        session.add(Image(
            image_id=image_id, user_id=user_id, location="Dili",
            image_path="storage/images/x.jpg", status=status,
            uploaded_at=uploaded_at or datetime.now(timezone.utc),
        ))
        session.commit()
    finally:
        session.close()


def _seed_prediction(image_id, prediction_id, predicted_class, risk_level=RiskLevel.LOW, predicted_at=None):
    session = SessionLocal()
    try:
        session.add(Prediction(
            prediction_id=prediction_id, image_id=image_id,
            predicted_class=predicted_class, prediction_confidence=0.9,
            risk_score=0.2, risk_level=risk_level,
            predicted_at=predicted_at or datetime.now(timezone.utc),
        ))
        session.commit()
    finally:
        session.close()


def test_requires_auth(client):
    assert client.get("/my/history").status_code == 401


def test_classifies_healthy_diseased_pending_and_failed(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    session = SessionLocal()
    user_id = session.query(User).filter_by(username="farmer1").first().user_id
    session.close()

    _seed_image(user_id, "img_healthy", status=ProcessingStatus.COMPLETED)
    _seed_prediction("img_healthy", "p1", "healthy")

    _seed_image(user_id, "img_diseased", status=ProcessingStatus.COMPLETED)
    _seed_prediction("img_diseased", "p2", "gray_leaf_spot", risk_level=RiskLevel.HIGH)

    _seed_image(user_id, "img_pending", status=ProcessingStatus.INGESTED)

    _seed_image(user_id, "img_failed", status=ProcessingStatus.FAILED)

    resp = client.get("/my/history", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    body = resp.json()

    by_id = {item["image_id"]: item for item in body["items"]}
    assert by_id["img_healthy"]["outcome"] == "healthy"
    assert by_id["img_diseased"]["outcome"] == "diseased"
    assert by_id["img_pending"]["outcome"] == "pending"
    assert by_id["img_failed"]["outcome"] == "failed"

    assert body["summary"] == {"healthy": 1, "diseased": 1, "pending": 1, "failed": 1}
    assert body["total"] == 4


def test_scoped_to_own_images_only(client):
    """The whole point of this endpoint: farmer1 must never see farmer2's
    uploads, unlike the admin dashboard which mixes every user together."""
    _make_user("farmer1")
    _make_user("farmer2")
    token1 = _login(client, "farmer1")
    session = SessionLocal()
    uid1 = session.query(User).filter_by(username="farmer1").first().user_id
    uid2 = session.query(User).filter_by(username="farmer2").first().user_id
    session.close()

    _seed_image(uid1, "mine", status=ProcessingStatus.COMPLETED)
    _seed_prediction("mine", "p1", "healthy")
    _seed_image(uid2, "not_mine", status=ProcessingStatus.COMPLETED)
    _seed_prediction("not_mine", "p2", "healthy")

    resp = client.get("/my/history", headers=_auth(token1))
    body = resp.json()
    assert body["total"] == 1
    assert [i["image_id"] for i in body["items"]] == ["mine"]
    assert body["summary"]["healthy"] == 1


def test_legacy_duplicate_predictions_use_the_latest_one(client):
    """An image with more than one Prediction row (the pre-idempotency-fix
    replay scenario) must be classified — and counted — using only its
    newest prediction, not double-counted."""
    _make_user("farmer1")
    token = _login(client, "farmer1")
    session = SessionLocal()
    user_id = session.query(User).filter_by(username="farmer1").first().user_id
    session.close()

    _seed_image(user_id, "img1", status=ProcessingStatus.COMPLETED)
    _seed_prediction("img1", "p_old", "healthy",
                      predicted_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    _seed_prediction("img1", "p_new", "gray_leaf_spot", risk_level=RiskLevel.HIGH,
                      predicted_at=datetime(2026, 6, 1, tzinfo=timezone.utc))

    resp = client.get("/my/history", headers=_auth(token))
    body = resp.json()
    assert body["total"] == 1  # one image, not one row per prediction
    assert body["items"][0]["outcome"] == "diseased"
    assert body["items"][0]["prediction"]["predicted_class"] == "gray_leaf_spot"
    assert body["summary"] == {"healthy": 0, "diseased": 1, "pending": 0, "failed": 0}


def test_outcome_filter(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    session = SessionLocal()
    user_id = session.query(User).filter_by(username="farmer1").first().user_id
    session.close()

    _seed_image(user_id, "img_healthy", status=ProcessingStatus.COMPLETED)
    _seed_prediction("img_healthy", "p1", "healthy")
    _seed_image(user_id, "img_diseased", status=ProcessingStatus.COMPLETED)
    _seed_prediction("img_diseased", "p2", "gray_leaf_spot")

    resp = client.get("/my/history?outcome_filter=diseased", headers=_auth(token))
    body = resp.json()
    assert body["total"] == 1
    assert [i["image_id"] for i in body["items"]] == ["img_diseased"]
    # summary still reflects the WHOLE history, not just the filtered page
    assert body["summary"] == {"healthy": 1, "diseased": 1, "pending": 0, "failed": 0}


def test_bad_outcome_filter_is_400(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    resp = client.get("/my/history?outcome_filter=nonsense", headers=_auth(token))
    assert resp.status_code == 400


def test_pagination(client):
    _make_user("farmer1")
    token = _login(client, "farmer1")
    session = SessionLocal()
    user_id = session.query(User).filter_by(username="farmer1").first().user_id
    session.close()

    for i in range(5):
        _seed_image(user_id, f"img{i}", status=ProcessingStatus.INGESTED,
                    uploaded_at=datetime(2026, 1, 1 + i, tzinfo=timezone.utc))

    resp = client.get("/my/history?limit=2&offset=0", headers=_auth(token))
    body = resp.json()
    assert body["total"] == 5
    assert [i["image_id"] for i in body["items"]] == ["img4", "img3"]  # newest first

    resp2 = client.get("/my/history?limit=2&offset=2", headers=_auth(token))
    assert [i["image_id"] for i in resp2.json()["items"]] == ["img2", "img1"]
