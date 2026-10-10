"""
spark/preprocessing.persist_result() — the Prediction-row write is made
idempotent on image_id because Structured Streaming's checkpoint only
guarantees at-least-once delivery to foreachBatch (see
spark/streaming_job.py docstring): a Spark crash-and-replay can call
persist_result() twice for the same event. These tests seed an Image and
call persist_result() directly (it only depends on SQLAlchemy, not pyspark)
to pin that a replay overwrites the Prediction instead of duplicating it,
while ProcessingEvent rows (an append-only audit log) are NOT deduped.
"""

from datetime import datetime, timezone

import pytest

from storage.database import (
    init_db, SessionLocal, Base, engine, User, Image, Prediction,
    ProcessingEvent, RejectedEvent, Role, ProcessingStatus, RiskLevel,
)
from spark.preprocessing import persist_result


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


def _seed_image(image_id="img1"):
    session = SessionLocal()
    try:
        session.add(User(user_id="u1", username="farmer", email="farmer@example.com",
                          password_hash="x", role=Role.USER))
        session.add(Image(image_id=image_id, user_id="u1", location="Dili",
                           image_path="storage/images/x.jpg",
                           uploaded_at=datetime.now(timezone.utc)))
        session.commit()
    finally:
        session.close()


def _event(image_id="img1"):
    return {"image_id": image_id, "user_id": "u1", "location": "Dili",
            "image_path": "storage/images/x.jpg",
            "uploaded_at": datetime.now(timezone.utc).isoformat()}


def _result(image_id="img1", predicted_class="gray_leaf_spot", risk_score=0.8):
    return {
        "image_id": image_id,
        "predicted_class": predicted_class,
        "prediction_confidence": 0.9,
        "lesion_score": 0.5,
        "discoloration_pct": 20.0,
        "texture_irregularity": 0.3,
        "leaf_area_affected_pct": 15.0,
        "risk_score": risk_score,
        "risk_level": RiskLevel.HIGH if risk_score >= 0.7 else RiskLevel.LOW,
        "model_version": "v1",
        "predicted_at": datetime.now(timezone.utc),
    }


def _predictions_for(image_id):
    session = SessionLocal()
    try:
        return session.query(Prediction).filter(Prediction.image_id == image_id).all()
    finally:
        session.close()


def _events_for(image_id):
    session = SessionLocal()
    try:
        return session.query(ProcessingEvent).filter(ProcessingEvent.image_id == image_id).all()
    finally:
        session.close()


def test_single_call_persists_one_prediction():
    _seed_image()
    persist_result(_event(), _result(), ProcessingStatus.COMPLETED)

    predictions = _predictions_for("img1")
    assert len(predictions) == 1
    assert predictions[0].predicted_class == "gray_leaf_spot"


def test_replayed_event_overwrites_instead_of_duplicating():
    """Simulates a Spark crash-and-replay: the exact same event is
    processed twice. Only one Prediction row must survive, and it must
    reflect the second (replayed) result — not both."""
    _seed_image()
    event = _event()

    persist_result(event, _result(predicted_class="healthy", risk_score=0.1), ProcessingStatus.COMPLETED)
    persist_result(event, _result(predicted_class="gray_leaf_spot", risk_score=0.8), ProcessingStatus.COMPLETED)

    predictions = _predictions_for("img1")
    assert len(predictions) == 1
    assert predictions[0].predicted_class == "gray_leaf_spot"
    assert predictions[0].risk_score == 0.8


def test_processing_events_are_not_deduped_on_replay():
    """Unlike Prediction, ProcessingEvent is an append-only audit log — a
    replay should still leave two rows behind."""
    _seed_image()
    event = _event()

    persist_result(event, _result(), ProcessingStatus.COMPLETED)
    persist_result(event, _result(), ProcessingStatus.COMPLETED)

    assert len(_events_for("img1")) == 2


def test_failed_status_with_no_result_does_not_touch_predictions():
    """A FAILED call (result=None) must not wipe out a prediction an
    earlier successful call already wrote for the same image."""
    _seed_image()
    event = _event()

    persist_result(event, _result(), ProcessingStatus.COMPLETED)
    persist_result(event, None, ProcessingStatus.FAILED, error_message="boom")

    predictions = _predictions_for("img1")
    assert len(predictions) == 1

    session = SessionLocal()
    try:
        image = session.get(Image, "img1")
        assert image.status == ProcessingStatus.FAILED
        assert image.error_message == "boom"
    finally:
        session.close()


def test_unknown_image_id_is_logged_as_a_rejected_event():
    """No Image row exists for this id (should never happen in practice —
    the API always creates it first) — persist_result must not crash, must
    not fabricate a Prediction, and (the fix) must leave a trace in
    RejectedEvent instead of just a stdout print()."""
    session = SessionLocal()
    try:
        assert session.query(RejectedEvent).count() == 0
    finally:
        session.close()

    persist_result(_event(image_id="does-not-exist"), _result(image_id="does-not-exist"),
                    ProcessingStatus.COMPLETED, error_message="no matching image row")
    assert _predictions_for("does-not-exist") == []

    session = SessionLocal()
    try:
        rejected = session.query(RejectedEvent).all()
        assert len(rejected) == 1
        assert rejected[0].image_id == "does-not-exist"
        assert "no matching image row" in rejected[0].reason
    finally:
        session.close()
