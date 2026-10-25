"""
Spark's job for one photo: check the event, get the photo, read its EXIF,
preprocess it for the CNN and store the processed image.
"""

import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from PIL import Image as PILImage, ImageOps

from config.settings import (
    IMAGE_STORAGE_PATH, REVIEW_DIR, SAVE_PHOTOS_FOR_REVIEW, PROCESSED_IMAGES_DIR,
    ANALYSIS_MAX_PX, SAVE_DOWNLOADED_PHOTOS, SPARK_RECHECK_DRIVE_FILE,
)
from storage.image_storage import resolve_image_path, is_drive_path, drive_file_id_from_path, store_downloaded_photo
from storage.exif_utils import extract_image_metadata, GPS_COLUMNS
from storage.results import as_aware_utc
from kafka.drive_watcher import _parse_time
from storage import drive_client
from ml.inference import input_spec
from storage.database import SessionLocal, Image, User, ScaleTestItem, ScaleTestRun


REQUIRED_EVENT_FIELDS = ["image_id", "user_id", "image_path", "uploaded_at"]


def _parse_timestamp(value, field_name: str):
    """ISO-8601 text -> aware datetime; raises InvalidEvent if it doesn't parse."""
    dt = _parse_time(value)
    if dt is None and value:
        raise InvalidEvent(f"Invalid {field_name}: not a valid ISO-8601 timestamp ({value!r})")
    return as_aware_utc(dt)


def normalize_location(location) -> str:
    """Same rule as parse_events: trimmed, single spaces, Title Case."""
    if not location:
        return None
    collapsed = re.sub(r"\s+", " ", str(location).strip())
    return collapsed.title() if collapsed else None


class InvalidEvent(Exception):
    pass


def clean_event(event: dict) -> dict:
    """Validates and cleans one event; raises InvalidEvent if it is malformed."""
    for key in REQUIRED_EVENT_FIELDS:
        if not event.get(key):
            raise InvalidEvent(f"Missing required field: {key}")

    now = datetime.now(timezone.utc)
    times = {field: _parse_timestamp(event.get(field), field) for field in ("uploaded_at", "captured_at")}
    for field, value in times.items():
        if value is not None and value > now:
            raise InvalidEvent(f"{field} is in the future: {value.isoformat()}")
    event.update({field: value.isoformat() if value else None for field, value in times.items()})
    event["location"] = normalize_location(event.get("location"))

    with SessionLocal() as session:
        if session.get(User, event["user_id"]) is None:
            raise InvalidEvent(f"Unknown user_id: {event['user_id']}")

    if is_drive_path(event["image_path"]):
        return _clean_drive_event(event)

    try:
        resolved = resolve_image_path(event["image_path"])
    except FileNotFoundError:
        raise InvalidEvent(f"Image file not found on disk: {event['image_path']}")

    try:
        resolved.resolve().relative_to(Path(IMAGE_STORAGE_PATH).resolve())
    except ValueError:
        raise InvalidEvent(f"Resolved image path escapes IMAGE_STORAGE_PATH: {resolved}")

    if event["image_id"] not in resolved.name:
        raise InvalidEvent(
            f"image_id {event['image_id']!r} is not reflected in the resolved "
            f"file name {resolved.name!r}"
        )

    event["image_path"] = str(resolved)
    return event


def _clean_drive_event(event: dict) -> dict:
    """The Drive file must exist, not be trashed, and carry this image_id."""
    file_id = drive_file_id_from_path(event["image_path"])
    if event.get("drive_file_id") and event["drive_file_id"] != file_id:
        raise InvalidEvent("drive_file_id does not match image_path")
    event["drive_file_id"] = file_id
    if not SPARK_RECHECK_DRIVE_FILE and _registered_from_drive(event):
        return event        # just checked at registration; asking Drive again is slow
    try:
        meta = drive_client.get_file_metadata(file_id)
    except (drive_client.DriveError, drive_client.DriveNotConfigured) as e:
        raise InvalidEvent(f"Image file not found in Google Drive: {file_id} ({e})")
    if meta.get("trashed"):
        raise InvalidEvent(f"Image file was deleted from Google Drive: {file_id}")
    if (meta.get("appProperties") or {}).get("image_id") != event["image_id"]:
        raise InvalidEvent(
            f"Google Drive file {file_id} is not tagged with image_id {event['image_id']!r}"
        )
    return event


def _registered_from_drive(event: dict) -> bool:
    """True when this exact Drive file was registered for this user."""
    with SessionLocal() as session:
        image = session.get(Image, event["image_id"])
        return (image is not None and image.source == "android_drive_upload"
                and image.image_path == event["image_path"] and image.user_id == event["user_id"])


_METADATA_COLUMNS = ("camera_make", "camera_model", "image_width", "image_height")


def extract_metadata(event: dict, image_bytes: bytes) -> dict:
    """Saves the ORIGINAL photo's EXIF (camera, GPS, capture time) on its row.
    EXIF GPS wins; without it the phone's own position is kept."""
    meta = extract_image_metadata(image_bytes)
    with SessionLocal() as session:
        image = session.get(Image, event["image_id"])
        if image is not None:
            for name in _METADATA_COLUMNS:
                setattr(image, name, meta[name])
            exif_gps = meta["gps_latitude"] is not None and meta["gps_longitude"] is not None
            if exif_gps or image.gps_source != "device":
                for name in GPS_COLUMNS:
                    setattr(image, name, meta[name] if exif_gps else None)
                image.gps_source = "exif" if exif_gps else None
            if image.captured_at is None and meta["captured_at"] is not None:
                image.captured_at = meta["captured_at"]
                image.captured_at_source = meta["captured_at_source"]
                event["captured_at"] = meta["captured_at"].isoformat()
            session.commit()
    return meta


def model_input(original_bytes: bytes, prepared_bytes: bytes = None):
    """The CNN input tensor: resize the shorter side to S, centre S x S crop,
    normalise. A centre crop scored better than squeezing (95.67% vs 94.74%)."""
    from torchvision import transforms
    spec = input_spec()
    size = spec["img_size"]
    img = _prepared_rgb(original_bytes, prepared_bytes)
    to_model = transforms.Compose([
        transforms.Resize(size),
        transforms.CenterCrop(size),
        transforms.ToTensor(),
        transforms.Normalize(mean=spec["mean"], std=spec["std"]),
    ])
    return to_model(img)


def model_input_preview(original_bytes: bytes, prepared_bytes: bytes = None):
    """The square the CNN sees, as a picture (model_input before normalising)."""
    from torchvision import transforms
    size = input_spec()["img_size"]
    img = _prepared_rgb(original_bytes, prepared_bytes)
    return transforms.CenterCrop(size)(transforms.Resize(size)(img))


def _prepared_rgb(original_bytes, prepared_bytes=None):
    data = prepared_bytes if prepared_bytes is not None else prepare_image(original_bytes)
    return ImageOps.exif_transpose(PILImage.open(BytesIO(data)).convert("RGB"))


def prepare_image(image_bytes: bytes) -> bytes:
    """Upright, at most ANALYSIS_MAX_PX, re-encoded without EXIF. Every later
    step starts from this copy. An unreadable image is returned unchanged."""
    try:
        img = PILImage.open(BytesIO(image_bytes))
        img.draft("RGB", (ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))
        img = ImageOps.exif_transpose(img).convert("RGB")
    except Exception:  # noqa: BLE001
        return image_bytes
    img.thumbnail((ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def scale_test_run_for(event: dict):
    """The scalability-test run this event belongs to, or None (checked in the database)."""
    run_id = event.get("run_id")
    if not run_id:
        return None
    with SessionLocal() as session:
        item = session.get(ScaleTestItem, event.get("image_id")) if event.get("image_id") else None
        run = session.get(ScaleTestRun, run_id) if item is not None and item.run_id == run_id else None
        if run is not None:
            session.expunge(run)
        return run


def save_downloaded_photo(user_id: str, image_id: str, original_bytes: bytes) -> None:
    """Keeps a local copy of the Drive photo. Never fails the diagnosis."""
    if not SAVE_DOWNLOADED_PHOTOS:
        return
    try:
        store_downloaded_photo(user_id, image_id, original_bytes)
    except Exception as e:  # noqa: BLE001
        print(f"[preprocessing] Could not save downloaded photo {image_id}: {e}")


def save_for_review(image_id: str, image_bytes: bytes) -> None:
    """Copies the photo (no EXIF, so no GPS) to REVIEW_DIR for the Label photos page."""
    if not SAVE_PHOTOS_FOR_REVIEW:
        return
    try:
        img = _prepared_rgb(None, image_bytes)
        REVIEW_DIR.mkdir(parents=True, exist_ok=True)
        tmp = REVIEW_DIR / f"{image_id}.jpg.tmp"
        img.save(tmp, format="JPEG", quality=95)
        tmp.replace(REVIEW_DIR / f"{image_id}.jpg")
    except Exception as e:  # noqa: BLE001
        print(f"[preprocessing] Could not save {image_id} for labelling: {e}")


def store_processed_image(image_id: str, original_bytes: bytes, prepared_bytes: bytes) -> None:
    """Stores the square the CNN sees as PROCESSED_IMAGES_DIR/<image_id>.png."""
    try:
        PROCESSED_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        tmp = PROCESSED_IMAGES_DIR / f"{image_id}.png.tmp"
        model_input_preview(original_bytes, prepared_bytes).save(tmp, format="PNG")
        tmp.replace(PROCESSED_IMAGES_DIR / f"{image_id}.png")
    except Exception as e:  # noqa: BLE001
        print(f"[preprocessing] Could not store the processed image of {image_id}: {e}")
