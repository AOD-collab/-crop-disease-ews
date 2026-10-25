"""
The same photo (same bytes) sent twice by the same farmer is analysed once;
the copy is recorded FAILED as a duplicate.
"""

import hashlib
import threading

from storage.database import SessionLocal, Image, Prediction, ProcessingStatus
from spark.preprocessing import InvalidEvent


class DuplicateImage(InvalidEvent):
    """Same bytes as an earlier photo from the same farmer."""


# False switches the duplicate check off
SKIP_DUPLICATE_PHOTOS = True


DUPLICATE_OF = "Duplicate photo: identical to "      # + the kept photo's image_id


# the lock makes "store the fingerprint + look for a copy" one step across Spark's
# threads; the set holds copies accepted by this process
_DUPLICATE_LOCK = threading.Lock()


_ACCEPTED_IMAGE_IDS = set()


def check_duplicate_by_hash(event: dict, original_bytes: bytes) -> None:
    """Stores the photo's SHA-256 and raises DuplicateImage if the same farmer
    already has it (COMPLETED, or accepted and in progress). Any other error
    only logs. Skips photos with no row or with a prediction (a Reprocess)."""
    if not SKIP_DUPLICATE_PHOTOS:
        return
    try:
        with _DUPLICATE_LOCK, SessionLocal() as session:
            image = session.get(Image, event["image_id"])
            if image is None:
                return      # no row to protect; persist_result already handles this case
            if session.query(Prediction.prediction_id).filter(
                    Prediction.image_id == image.image_id).first() is not None:
                return      # already diagnosed once (a Reprocess / model re-run): never reject it now
            # reuse a stored hash; otherwise hash the downloaded bytes
            digest = image.content_hash or hashlib.sha256(original_bytes).hexdigest()
            if not image.content_hash:
                image.content_hash = digest
                session.commit()
            # same photo, same farmer: COMPLETED, or accepted and in progress
            copies = (
                session.query(Image.image_id, Image.status)
                .filter(
                    Image.user_id == event["user_id"],
                    Image.content_hash == digest,
                    Image.image_id != event["image_id"],
                    Image.status.in_([ProcessingStatus.COMPLETED, ProcessingStatus.PROCESSING]),
                )
                .order_by(Image.uploaded_at, Image.image_id)
                .all()
            )
            earlier = next((c for c in copies if c.status == ProcessingStatus.COMPLETED
                            or c.image_id in _ACCEPTED_IMAGE_IDS), None)
            if earlier is None:
                _ACCEPTED_IMAGE_IDS.add(event["image_id"])
    except Exception as e:  # noqa: BLE001 - closing the session rolls it back
        print(f"[duplicates] Duplicate check skipped for {event.get('image_id')}: {e}")
        return
    if earlier:
        raise DuplicateImage(f"{DUPLICATE_OF}{earlier.image_id}")


def resend_duplicates_of(failed_ids: list, backend=None) -> int:
    """Re-sends copies rejected as duplicates of a photo that then FAILED,
    so one of them gets a result. Returns how many were sent."""
    if not failed_ids:
        return 0
    from storage.pipeline_submit import submit_all
    with SessionLocal() as session:
        failed = [i for (i,) in session.query(Image.image_id).filter(
            Image.image_id.in_(failed_ids), Image.status == ProcessingStatus.FAILED)]
        copies = session.query(Image).filter(
            Image.status == ProcessingStatus.FAILED,
            Image.error_message.in_([DUPLICATE_OF + i for i in failed])).order_by(Image.uploaded_at).all()
        return submit_all(session, copies, backend, "[duplicates]")
