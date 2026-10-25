"""
Sends a photo that is already in the database through the pipeline again
(Reprocess / Retry, requeue_stuck): a "submit" event to Kafka, row INGESTED.
"""

from storage.database import ProcessingStatus, now_utc
from storage.image_storage import is_drive_path, drive_file_id_from_path
from kafka.config import KAFKA_TOPIC
from kafka.producer import get_backend


def build_upload_event(image) -> dict:
    """The Kafka event for a stored Image row (a reference, never the bytes)."""
    return {
        "kind": "submit",
        "image_id": image.image_id,
        "user_id": image.user_id,
        "location": image.location,
        "image_path": image.image_path,
        "drive_file_id": drive_file_id_from_path(image.image_path) if is_drive_path(image.image_path) else None,
        "captured_at": image.captured_at.isoformat() if image.captured_at else None,
        "uploaded_at": image.uploaded_at.isoformat() if image.uploaded_at else None,
    }


# error_message prefix of an upload refused at registration
UPLOAD_REJECTED_PREFIX = "Upload rejected: "


class NotResubmittable(ValueError):
    """A refused upload; it must not be analysed."""


def is_upload_rejected(image) -> bool:
    return (image.status == ProcessingStatus.FAILED
            and (image.error_message or "").startswith(UPLOAD_REJECTED_PREFIX))


def submit_image_for_processing(session, image, backend=None) -> None:
    """Marks the image INGESTED, then publishes its event. On a publish error
    the row is marked FAILED and the error re-raised. Refuses a rejected upload."""
    if is_upload_rejected(image):
        raise NotResubmittable(f"{image.error_message} — this upload can't be processed")
    backend = backend or get_backend(KAFKA_TOPIC)
    # commit INGESTED before publishing, so Spark's result can't be overwritten
    if image.status == ProcessingStatus.FAILED:
        image.status = ProcessingStatus.INGESTED
        image.error_message = None
    image.ingested_at = now_utc()
    session.commit()
    try:
        backend.send(build_upload_event(image))
    except Exception as e:
        image.status = ProcessingStatus.FAILED
        image.error_message = f"Kafka publish failed: {e}"
        session.commit()
        raise


def submit_all(session, images, backend=None, log_prefix="[pipeline_submit]") -> int:
    """Submits each image; a failure only logs. Returns how many were sent."""
    sent = 0
    for image in images:
        try:
            backend = backend or get_backend(KAFKA_TOPIC)
            submit_image_for_processing(session, image, backend)
            sent += 1
        except Exception as e:  # noqa: BLE001 - recorded on the row; the others still go
            print(f"{log_prefix} Could not re-send {image.image_id}: {e}")
    return sent
