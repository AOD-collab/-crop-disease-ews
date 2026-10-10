"""
The administrator's review decisions (table image_reviews): a CONFIRMED
disease label, or "discarded". Written by the Label photos page
(dashboard/labelling_page.py) next to its labels_log.csv. A model
prediction is never written here.
"""

import csv
from datetime import datetime, timezone
from pathlib import Path

from config import settings
from storage.database import SessionLocal, ImageReview, now_utc

LOG_NAME = "labels_log.csv"


def record(image_id: str, decision: str, confirmed_class: str | None, predicted_class: str | None,
           reviewed_by: str | None, reviewed_at: datetime | None = None) -> None:
    """Store the latest decision for an image (a newer one replaces it)."""
    if decision not in ("labelled", "discarded"):
        raise ValueError(f"unknown decision {decision!r}")
    with SessionLocal() as s:
        row = s.get(ImageReview, image_id) or ImageReview(image_id=image_id)
        row.decision = decision
        row.confirmed_class = confirmed_class if decision == "labelled" else None
        row.predicted_class_at_review = predicted_class
        row.reviewed_by = reviewed_by
        row.reviewed_at = reviewed_at or now_utc()
        s.merge(row)
        s.commit()


def import_labels_log(path: Path | None = None) -> int:
    """One-way backfill from LABELLED_TRAIN_DIR/labels_log.csv: adds the
    latest logged decision of every image that has no image_reviews row yet.
    Existing rows are never changed. Returns how many were added."""
    path = Path(path or Path(settings.LABELLED_TRAIN_DIR) / LOG_NAME)
    if not path.exists():
        return 0
    latest = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("image_id"):
                latest[row["image_id"]] = row             # the log is in time order: last wins
    with SessionLocal() as s:
        known = {r[0] for r in s.query(ImageReview.image_id).all()}
        added = 0
        for image_id, row in latest.items():
            if image_id in known:
                continue
            label = row.get("label") or ""
            discarded = label == "discarded"
            try:
                # the log wrote this computer's local time, without an offset
                when = datetime.fromisoformat(row.get("labelled_at") or "").astimezone(timezone.utc)
            except ValueError:
                when = now_utc()
            s.add(ImageReview(image_id=image_id, decision="discarded" if discarded else "labelled",
                              confirmed_class=None if discarded else label,
                              predicted_class_at_review=row.get("ai_prediction") or None,
                              reviewed_by=row.get("labelled_by") or None, reviewed_at=when))
            added += 1
        s.commit()
    return added
