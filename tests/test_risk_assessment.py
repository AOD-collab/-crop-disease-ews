import uuid

import pytest

from config.settings import IMAGE_STORAGE_PATH
from spark.preprocessing import compute_risk, clean_event, InvalidEvent
from storage.database import init_db, SessionLocal, Base, engine, User, Role
from api.auth import hash_password


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


def _make_user():
    session = SessionLocal()
    try:
        suffix = uuid.uuid4().hex[:8]
        user = User(username=f"farmer{suffix}", email=f"farmer{suffix}@example.com",
                    password_hash=hash_password("password123"), role=Role.USER)
        session.add(user)
        session.commit()
        session.refresh(user)
        session.expunge(user)
        return user
    finally:
        session.close()


def _write_test_image() -> tuple:
    """Writes a fake-bytes file under IMAGE_STORAGE_PATH, named the way a
    real upload names it (<image_id>.jpg) — clean_event() requires both the
    IMAGE_STORAGE_PATH containment and the image_id/filename correspondence
    that api/image_storage.save_image() always produces."""
    IMAGE_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    image_id = str(uuid.uuid4())
    path = IMAGE_STORAGE_PATH / f"{image_id}.jpg"
    path.write_bytes(b"fake-bytes-just-for-path-existence-check")
    return image_id, str(path)


def test_likely_healthy_gives_low_risk():
    score, level = compute_risk({"lesion_score": 0.02})
    assert level == "LOW"
    assert score == 0.02


def test_middle_probability_gives_medium_risk():
    _, level = compute_risk({"lesion_score": 0.45})
    assert level == "MEDIUM"


def test_likely_diseased_gives_high_risk():
    score, level = compute_risk({"lesion_score": 0.9})
    assert level == "HIGH"
    assert score == 0.9


def test_risk_comes_from_the_model_only():
    """The score is the model's P(diseased); old pixel-math features and
    the top-class confidence don't move it."""
    plain, _ = compute_risk({"lesion_score": 0.7})
    extra, _ = compute_risk({
        "lesion_score": 0.7, "discoloration_pct": 90.0, "texture_irregularity": 0.9,
        "leaf_area_affected_pct": 95.0, "model_confidence": 0.3,
    })
    assert plain == extra == 0.7


def test_uncertain_result_cannot_reach_high_risk():
    """A low-confidence ("uncertain") CNN read must not raise a HIGH-risk
    alert, even if its P(diseased) alone would score as HIGH."""
    certain_score, certain_level = compute_risk({"lesion_score": 0.9}, is_uncertain=False)
    uncertain_score, uncertain_level = compute_risk({"lesion_score": 0.9}, is_uncertain=True)
    assert certain_level == "HIGH"
    assert uncertain_level != "HIGH"
    assert certain_score == uncertain_score  # the score itself is unchanged, only the level is capped


def test_clean_event_rejects_missing_fields():
    with pytest.raises(InvalidEvent):
        clean_event({"image_id": "x"})


def test_clean_event_rejects_missing_image_file():
    user = _make_user()
    event = {
        "image_id": str(uuid.uuid4()), "user_id": user.user_id, "location": "Dili",
        "image_path": str(IMAGE_STORAGE_PATH / "does_not_exist.jpg"),
        "uploaded_at": "2026-08-30T00:00:00Z",
    }
    with pytest.raises(InvalidEvent):
        clean_event(event)


def test_clean_event_accepts_valid_event():
    user = _make_user()
    image_id, image_path = _write_test_image()
    event = {
        "image_id": image_id, "user_id": user.user_id, "location": "  dili ",
        "image_path": image_path, "uploaded_at": "2026-08-30T00:00:00Z",
    }
    cleaned = clean_event(event)
    assert cleaned["image_id"] == image_id
    assert cleaned["location"] == "Dili"  # case/whitespace-normalized


def test_clean_event_rejects_unknown_user():
    image_id, image_path = _write_test_image()
    event = {
        "image_id": image_id, "user_id": "does-not-exist", "location": "Dili",
        "image_path": image_path, "uploaded_at": "2026-08-30T00:00:00Z",
    }
    with pytest.raises(InvalidEvent):
        clean_event(event)


def test_clean_event_rejects_future_captured_at():
    user = _make_user()
    image_id, image_path = _write_test_image()
    event = {
        "image_id": image_id, "user_id": user.user_id, "location": "Dili",
        "image_path": image_path, "uploaded_at": "2026-08-30T00:00:00Z",
        "captured_at": "2099-01-01T00:00:00Z",
    }
    with pytest.raises(InvalidEvent):
        clean_event(event)


def test_clean_event_rejects_invalid_uploaded_at():
    user = _make_user()
    image_id, image_path = _write_test_image()
    event = {
        "image_id": image_id, "user_id": user.user_id, "location": "Dili",
        "image_path": image_path, "uploaded_at": "not-a-timestamp",
    }
    with pytest.raises(InvalidEvent):
        clean_event(event)


def test_clean_event_rejects_path_outside_storage(tmp_path):
    user = _make_user()
    outside_file = tmp_path / "leaf.jpg"
    outside_file.write_bytes(b"fake-bytes")
    event = {
        "image_id": str(uuid.uuid4()), "user_id": user.user_id, "location": "Dili",
        "image_path": str(outside_file), "uploaded_at": "2026-08-30T00:00:00Z",
    }
    with pytest.raises(InvalidEvent):
        clean_event(event)
