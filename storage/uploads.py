"""
Registers a new Drive upload: finds (or creates) the farmer from the Drive
owner, checks the upload, skips one already registered, and inserts the
row as INGESTED. A refused upload still gets a row and fails with the reason.
"""

import json

from sqlalchemy.exc import IntegrityError

from config.settings import MAX_IMAGE_SIZE, ALLOWED_IMAGE_CONTENT_TYPES
from storage.auth import normalize_email
from storage.database import SessionLocal, Image, RejectedEvent, ProcessingStatus, UserStatus, now_utc
from storage.farmers import get_or_create_farmer
from storage.pipeline_submit import UPLOAD_REJECTED_PREFIX
from spark.preprocessing import InvalidEvent
from kafka.drive_watcher import _UUID_RE, _parse_time


class UploadRejected(InvalidEvent):
    """The upload breaks a rule (inactive account, wrong folder, name, type or size)."""


def _float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def register_upload(event: dict) -> str:
    """Returns "new" (row created), "duplicate" (already registered) or
    "conflict" (image_id used by another file). Raises for a refused upload."""
    image_id = event.get("image_id") or ""
    if not _UUID_RE.match(image_id):
        raise InvalidEvent("file has no valid image_id (not uploaded by the Android app)")
    with SessionLocal() as session:
        existing = session.get(Image, image_id)
        if existing is not None:
            if existing.image_path == event.get("image_path"):
                return "duplicate"
            if session.query(RejectedEvent).filter(
                    RejectedEvent.raw_event.contains(event.get("drive_file_id") or "")).first() is None:
                session.add(RejectedEvent(image_id=image_id, raw_event=json.dumps(event, default=str),
                                          reason=f"image_id {image_id} already belongs to another file"))
                session.commit()
            return "conflict"

        owner_email = event.get("owner_email")
        user = get_or_create_farmer(session, owner_email, event.get("owner_name"))
        event["user_id"] = user.user_id

        problems = []
        if user.status != UserStatus.ACTIVE or getattr(user, "is_deleted", False):
            problems.append("your account is not active — contact the administrator")
        folder_email = event.get("folder_email")
        if folder_email is not None and normalize_email(folder_email) != normalize_email(owner_email):
            problems.append("photo was not uploaded into your own folder")
        if not (event.get("file_name") or "").startswith(image_id):
            problems.append("file name does not match its image_id")
        if (event.get("mime_type") or "").lower() not in ALLOWED_IMAGE_CONTENT_TYPES:
            problems.append(f"unsupported image type {event.get('mime_type')}")
        try:
            size = int(event.get("size_bytes") or 0)
        except ValueError:
            size = 0
        if size <= 0:
            problems.append("empty file")
        elif size > MAX_IMAGE_SIZE:
            problems.append(f"file exceeds max size of {MAX_IMAGE_SIZE} bytes")

        captured_at = _parse_time(event.get("captured_at"))
        lat, lon = _float(event.get("device_gps_lat")), _float(event.get("device_gps_lon"))
        device_gps = lat is not None and lon is not None
        now = now_utc()
        session.add(Image(
            image_id=image_id,
            location=event.get("location"),     # already cleaned by parse_events (case, spaces)
            user_id=user.user_id,
            original_filename=event.get("original_filename") or event.get("file_name"),
            image_path=event.get("image_path"),
            source="android_drive_upload",
            content_hash=event.get("content_hash"),
            captured_at=captured_at,
            captured_at_source="client_trusted" if captured_at else None,
            uploaded_at=_parse_time(event.get("uploaded_at")) or now,  # Google's clock, not the phone's
            ingested_at=now,
            gps_latitude=lat if device_gps else None,
            gps_longitude=lon if device_gps else None,
            gps_source="device" if device_gps else None,
            status=ProcessingStatus.INGESTED,
        ))
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return "duplicate"      # another copy of the event registered it a moment ago

    if problems:
        raise UploadRejected(UPLOAD_REJECTED_PREFIX + "; ".join(problems))
    return "new"
