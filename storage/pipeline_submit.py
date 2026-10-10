"""
The single "submit an uploaded image into the streaming pipeline" step.

AUTOMATIC mode (api/server.py `upload_image`) publishes the upload event to
Kafka inline the moment the image is stored, and marks the row INGESTED.
MANUAL mode stores the image at ProcessingStatus.PENDING_REVIEW and does
nothing else until an admin clicks "Process" — which calls
`submit_image_for_processing()` here.

Either way the event that lands on the `maize-disease-images` topic, and
everything downstream of it (spark/streaming_job.py), is identical. This is
factored out so the manual path runs exactly the automatic path's step
without duplicating it.
"""

from storage.database import ProcessingStatus, now_utc
from api.image_storage import is_drive_path, drive_file_id_from_path
from kafka.config import KAFKA_TOPIC
from kafka.producer import get_backend


def build_upload_event(image) -> dict:
    """The exact event dict api/server.py publishes on upload, rebuilt from
    a stored Image row (mirrors storage/requeue_stuck._event_from_image).

    For a Google Drive upload, image_path is "gdrive:<file id>" and
    drive_file_id carries the same id explicitly: Kafka transports only
    this reference + metadata, never the image bytes. drive_file_id is None
    for legacy local-disk images."""
    return {
        "image_id": image.image_id,
        "user_id": image.user_id,
        "location": image.location,
        "image_path": image.image_path,
        "drive_file_id": drive_file_id_from_path(image.image_path) if is_drive_path(image.image_path) else None,
        "captured_at": image.captured_at.isoformat() if image.captured_at else None,
        "uploaded_at": image.uploaded_at.isoformat() if image.uploaded_at else None,
    }


def submit_image_for_processing(session, image, backend=None) -> None:
    """Publish `image`'s upload event and mark it INGESTED, committing on
    `session`. On a publish failure the row is marked FAILED (same contract
    as the automatic path in api/server.upload_image) and the exception is
    re-raised for the caller to report.

    `backend` defaults to kafka.producer.get_backend(KAFKA_TOPIC); callers
    that already hold a backend (e.g. the API's cached singleton) should
    pass it.
    """
    backend = backend or get_backend(KAFKA_TOPIC)
    # Mark INGESTED and commit BEFORE publishing: Spark can then never
    # finish the image before this commit lands and have its COMPLETED/
    # FAILED stomped back to INGESTED. A FAILED image is reset too — that is
    # the dashboard's "Reprocess"; before, it stayed FAILED with the old
    # error while the new event was in flight.
    if image.status in (ProcessingStatus.UPLOADED, ProcessingStatus.PENDING_REVIEW,
                        ProcessingStatus.FAILED):
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
