from datetime import datetime, timezone

import pytest

from storage.database import (
    init_db, SessionLocal, Base, engine, User, Image, Prediction,
    Role, ProcessingStatus, RiskLevel,
)
from spark.data_quality import data_quality_report


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


def _seed():
    session = SessionLocal()
    try:
        session.add(User(user_id="u1", username="farmer", email="farmer@example.com",
                          password_hash="x", role=Role.USER))

        # Two images with the SAME content_hash (a duplicate upload).
        session.add(Image(image_id="img1", user_id="u1", location="Dili",
                           image_path="storage/images/a.jpg", content_hash="hash-a",
                           captured_at=datetime.now(timezone.utc),
                           uploaded_at=datetime.now(timezone.utc),
                           status=ProcessingStatus.COMPLETED))
        session.add(Image(image_id="img2", user_id="u1", location="Dili",
                           image_path="storage/images/b.jpg", content_hash="hash-a",
                           uploaded_at=datetime.now(timezone.utc),
                           status=ProcessingStatus.COMPLETED))
        # An image with no location/captured_at at all (missing values).
        session.add(Image(image_id="img3", user_id="u1", location=None,
                           image_path="storage/images/c.jpg", content_hash=None,
                           uploaded_at=datetime.now(timezone.utc),
                           status=ProcessingStatus.UPLOADED))

        session.add(Prediction(prediction_id="p1", image_id="img1", predicted_class="healthy",
                                risk_score=0.1, risk_level=RiskLevel.LOW,
                                processing_latency_ms=500,
                                predicted_at=datetime.now(timezone.utc)))
        session.add(Prediction(prediction_id="p2", image_id="img2", predicted_class="healthy",
                                risk_score=0.2, risk_level=RiskLevel.LOW,
                                processing_latency_ms=520,
                                predicted_at=datetime.now(timezone.utc)))
        # An out-of-range risk_score (data-quality bug, e.g. a formula
        # regression) and a duplicate Prediction row for the same image.
        session.add(Prediction(prediction_id="p3", image_id="img2", predicted_class="healthy",
                                risk_score=1.5, risk_level=RiskLevel.HIGH,
                                processing_latency_ms=100000,  # a latency outlier
                                predicted_at=datetime.now(timezone.utc)))
        session.commit()
    finally:
        session.close()


def test_data_quality_report_runs_cleanly_on_empty_db():
    report = data_quality_report()
    assert report["total_images"] == 0
    assert report["duplicate_content_hash_groups"] == 0
    assert report["risk_score_out_of_range"] == 0


def test_data_quality_report_detects_duplicate_content_hash():
    _seed()
    report = data_quality_report()
    assert report["duplicate_content_hash_groups"] == 1
    assert report["duplicate_images_count"] == 2


def test_data_quality_report_detects_duplicate_prediction_rows():
    _seed()
    report = data_quality_report()
    assert report["duplicate_prediction_rows_groups"] == 1  # img2 has 2 predictions


def test_data_quality_report_detects_out_of_range_risk_score():
    _seed()
    report = data_quality_report()
    assert report["risk_score_out_of_range"] == 1  # the 1.5 risk_score row


def test_data_quality_report_counts_missing_values():
    _seed()
    report = data_quality_report()
    missing = report["missing_value_counts"]
    assert missing["images.location"] == 1  # img3
    assert missing["images.content_hash"] == 1  # img3
