"""
Duplicate photos: the same photo (same bytes) sent twice by the same farmer
is analysed once. Spark's job (spark/streaming_job.process_event) calls
check_duplicate_by_hash on the downloaded bytes; a copy is recorded FAILED
with DUPLICATE_OF + the kept photo's image_id.
"""

import hashlib
import threading

from storage.database import SessionLocal, Image, Prediction, ProcessingStatus
from spark.preprocessing import InvalidEvent


class DuplicateImage(InvalidEvent):
    """The photo's bytes are identical to an earlier photo from the same user
    (already completed, or being processed). A subclass of InvalidEvent, so every existing
    `except InvalidEvent` handler treats it like any other rejected event."""


# Set to False to switch the duplicate check off without touching anything else.
SKIP_DUPLICATE_PHOTOS = True


DUPLICATE_OF = "Duplicate photo: identical to "      # + the kept photo's image_id


# The Spark job checks several photos of a micro-batch at once (streaming_job's
# thread pool). The lock makes "store the fingerprint + look for a copy" one step,
# so two copies of the same photo can't both look before either is stored; the set
# holds the photos that PASSED the check in this process, so a copy that is being
# processed only counts once it was accepted (two copies can't reject each other).
_DUPLICATE_LOCK = threading.Lock()


_ACCEPTED_IMAGE_IDS = set()


def check_duplicate_by_hash(event: dict, original_bytes: bytes) -> None:
    """Fingerprint (SHA-256) the ORIGINAL photo bytes, store it on the Image
    row, and raise DuplicateImage when the same user already has a COMPLETED
    photo with the same fingerprint, or a copy that another thread of this
    Spark job accepted and is processing right now. Whichever copy is checked
    first is kept; every later copy is rejected.

    Never breaks a diagnosis by itself: if anything goes wrong in here (for
    example the database is unreachable), it only logs and lets the photo
    continue. The one deliberate exception is DuplicateImage.

    It leaves alone (returns without checking) a photo that has no Image row,
    and a photo that already has a prediction (a Reprocess / model re-run)."""
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
            # A stored hash (older rows) is reused so every row is compared the same way;
            # a Drive photo gets its hash here, from the bytes Spark just downloaded.
            digest = image.content_hash or hashlib.sha256(original_bytes).hexdigest()
            if not image.content_hash:
                image.content_hash = digest
                session.commit()
            # Same photo, same farmer: already COMPLETED, or accepted by this job and
            # being processed right now (oldest upload named first).
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
    """A copy is rejected as a duplicate of a photo that was still being
    processed. If that photo then FAILED (a model error, say), the copy
    would stay FAILED as a "duplicate" of a photo without a result — so it
    is sent through the pipeline again: its duplicate check runs again and
    keeps one copy (the others become its duplicates). A copy whose send
    fails is left FAILED with "Kafka publish failed", which
    requeue_stuck --include-failed retries. Returns how many were sent."""
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
