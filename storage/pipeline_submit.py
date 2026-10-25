"""
Sending a photo that is ALREADY in the database into the streaming pipeline
again: the dashboard's Reprocess / Retry of a FAILED photo and
storage/requeue_stuck. It publishes a "submit" event to Kafka and marks the
row INGESTED.

New photos don't come through here: the Drive watcher (and the dashboard's
Pending Review in MANUAL mode) publish a "drive_upload" event built from
the file's Drive metadata, and Spark creates the row (spark/ingestion.py).
"""

from storage.database import ProcessingStatus, now_utc
from storage.image_storage import is_drive_path, drive_file_id_from_path
from kafka.config import KAFKA_TOPIC
from kafka.producer import get_backend


def build_upload_event(image) -> dict:
    """The upload event published to Kafka, built from a stored Image row (storage/requeue_stuck.py re-publishes with it too).

    For a Google Drive upload, image_path is "gdrive:<file id>" and
    drive_file_id carries the same id explicitly: Kafka transports only
    this reference + metadata, never the image bytes. drive_file_id is None
    for legacy local-disk images."""
    return {
        # A photo already in the database, sent again (Process of a FAILED
        # row's Reprocess/Retry, requeue_stuck). New photos come from Drive as
        # "drive_upload" events (kafka/drive_watcher.build_drive_event).
        "kind": "submit",
        "image_id": image.image_id,
        "user_id": image.user_id,
        "location": image.location,
        "image_path": image.image_path,
        "drive_file_id": drive_file_id_from_path(image.image_path) if is_drive_path(image.image_path) else None,
        "captured_at": image.captured_at.isoformat() if image.captured_at else None,
        "uploaded_at": image.uploaded_at.isoformat() if image.uploaded_at else None,
    }


# error_message prefix Spark's ingestion step (spark/ingestion.register_upload)
# gives an upload it refused (inactive account, not the farmer's own folder,
# wrong name/type/size).
UPLOAD_REJECTED_PREFIX = "Upload rejected: "


class NotResubmittable(ValueError):
    """The image was refused at upload time; sending it to the pipeline
    would analyse a photo the system decided not to accept."""


def is_upload_rejected(image) -> bool:
    return (image.status == ProcessingStatus.FAILED
            and (image.error_message or "").startswith(UPLOAD_REJECTED_PREFIX))


def submit_image_for_processing(session, image, backend=None) -> None:
    """Publish `image`'s upload event and mark it INGESTED, committing on
    `session`. On a publish failure the row is marked FAILED and the
    exception is re-raised for the caller to report.

    `backend` defaults to kafka.producer.get_backend(KAFKA_TOPIC); callers
    that already hold a backend (e.g. the Drive watcher's) should pass it.

    Raises NotResubmittable (nothing changed) for an upload the Drive watcher
    rejected: the dashboard's Reprocess / Retry used to send those through
    anyway, bypassing the watcher's account and folder checks.
    """
    if is_upload_rejected(image):
        raise NotResubmittable(f"{image.error_message} — this upload can't be processed")
    backend = backend or get_backend(KAFKA_TOPIC)
    # Mark INGESTED and commit BEFORE publishing: Spark can then never
    # finish the image before this commit lands and have its COMPLETED/
    # FAILED stomped back to INGESTED. A FAILED image is reset too — that is
    # the dashboard's "Reprocess"; before, it stayed FAILED with the old
    # error while the new event was in flight.
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
    """submit_image_for_processing for each image, one backend for all; one
    that can't be sent only logs (its row says why) and the others still go.
    Returns how many were sent."""
    sent = 0
    for image in images:
        try:
            backend = backend or get_backend(KAFKA_TOPIC)
            submit_image_for_processing(session, image, backend)
            sent += 1
        except Exception as e:  # noqa: BLE001 - recorded on the row; the others still go
            print(f"{log_prefix} Could not re-send {image.image_id}: {e}")
    return sent
