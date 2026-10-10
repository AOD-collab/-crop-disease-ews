"""
Regression tests for spark/analytics.images_with_predictions().

The function documents "one row per image", but an image can carry more
than one Prediction row (Spark Structured Streaming is at-least-once, so a
replayed batch inserts a second prediction for the same image_id — see
spark/streaming_job.py). The raw LEFT JOIN then produced a duplicate
image_id, which crashed `df.set_index("image_id")` in the dashboard gallery
with "DataFrame index must be unique for orient='index'".
"""

from datetime import datetime, timedelta, timezone

import pytest

from storage.database import (
    init_db, SessionLocal, Base, engine, User, Image, Prediction,
    Role, ProcessingStatus, RiskLevel,
)
from spark import analytics


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


def _seed_image_with_two_predictions():
    session = SessionLocal()
    try:
        session.add(User(user_id="u1", username="farmer", email="farmer@example.com",
                         password_hash="x", role=Role.USER))
        session.add(Image(image_id="img1", user_id="u1", location="Dili",
                          image_path="storage/images/x.jpg",
                          uploaded_at=datetime.now(timezone.utc),
                          status=ProcessingStatus.COMPLETED))
        old = datetime.now(timezone.utc) - timedelta(hours=1)
        new = datetime.now(timezone.utc)
        session.add(Prediction(prediction_id="p_old", image_id="img1",
                               predicted_class="healthy", prediction_confidence=0.4,
                               risk_score=0.1, risk_level=RiskLevel.LOW, predicted_at=old))
        session.add(Prediction(prediction_id="p_new", image_id="img1",
                               predicted_class="gray_leaf_spot", prediction_confidence=0.9,
                               risk_score=0.8, risk_level=RiskLevel.HIGH, predicted_at=new))
        session.commit()
    finally:
        session.close()


def test_duplicate_predictions_collapse_to_one_row():
    _seed_image_with_two_predictions()

    df = analytics.images_with_predictions()

    assert len(df) == 1
    assert df["image_id"].is_unique
    # the newest prediction wins
    row = df.iloc[0]
    assert row["predicted_class"] == "gray_leaf_spot"
    assert row["risk_level"] == "HIGH"
    # prediction_id is an internal tiebreak column, not part of the contract
    assert "prediction_id" not in df.columns


def test_set_index_image_id_does_not_raise_with_duplicate_predictions():
    """The exact operation the dashboard gallery performs."""
    _seed_image_with_two_predictions()
    df = analytics.images_with_predictions()
    meta = df.set_index("image_id").to_dict("index")  # used to raise ValueError
    assert set(meta.keys()) == {"img1"}


def test_counts_are_not_inflated_by_duplicate_predictions():
    _seed_image_with_two_predictions()
    kpi = analytics.kpi_summary()
    assert kpi["total_images"] == 1
    assert kpi["total_diseased"] == 1
    assert kpi["total_healthy"] == 0  # the stale "healthy" prediction must not count
    assert kpi["total_high_risk"] == 1


def _seed_photos(rows):
    """rows: (location, risk_level or None for a photo rejected before analysis)."""
    session = SessionLocal()
    try:
        session.add(User(user_id="u1", username="farmer", email="farmer@example.com",
                         password_hash="x", role=Role.USER))
        for i, (location, risk) in enumerate(rows):
            session.add(Image(image_id=f"img{i}", user_id="u1", location=location,
                              image_path=f"storage/images/{i}.jpg", uploaded_at=datetime.now(timezone.utc),
                              status=ProcessingStatus.COMPLETED if risk else ProcessingStatus.FAILED))
            if risk:
                session.add(Prediction(prediction_id=f"p{i}", image_id=f"img{i}", predicted_class="common_rust",
                                       prediction_confidence=0.9, risk_score=0.5, risk_level=risk,
                                       predicted_at=datetime.now(timezone.utc)))
        session.commit()
    finally:
        session.close()


def test_high_risk_by_location_counts_n_of_n_per_municipality():
    H, M, L = RiskLevel.HIGH, RiskLevel.MEDIUM, RiskLevel.LOW
    _seed_photos([("Dili", H), ("Dili", H), ("Dili", M), ("Dili", L), ("Dili", None),   # None: not maize
                  ("Baucau", H), ("Baucau", L),
                  ("Ermera", L), ("Ermera", M),
                  (None, H), ("  ", L)])

    out = analytics.high_risk_by_location()

    rows = {r.location: (r.high, r.total, r.high_pct) for r in out.itertuples()}
    assert rows == {"Dili": (2, 4, 50.0),                        # the rejected photo doesn't count
                    "Baucau": (1, 2, 50.0),
                    analytics.UNKNOWN_LOCATION: (1, 2, 50.0),
                    "Ermera": (0, 2, 0.0)}
    assert list(out["location"])[0] == "Dili" and list(out["location"])[-1] == "Ermera"  # most HIGH first


def test_high_risk_by_location_empty():
    assert analytics.high_risk_by_location().empty


def test_empty_db_still_returns_expected_columns():
    df = analytics.images_with_predictions()
    assert df.empty
    assert "image_id" in df.columns
    assert "prediction_id" not in df.columns
