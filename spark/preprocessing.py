"""
Spark's work on one photo: check the Kafka event, get the photo, read its
EXIF, and PREPROCESS it for the CNN (upright, at most ANALYSIS_MAX_PX,
resize + centre crop + normalise: model_input), then store the processed
image (store_processed_image) and the labelling copy (save_for_review).

What happens after preprocessing is not Spark's job and lives elsewhere:
    the CNN classifies the image      ml/inference.py
    the risk level                    ml/risk.py
    the marked-up image               ml/render.py
    saving + the result in Drive      storage/results.py
    duplicate photos                  storage/duplicates.py
spark/streaming_job.py runs the steps in that order for every photo.
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
    """Parses an ISO-8601 string (what every event field is serialized as)
    into a timezone-aware datetime. Raises InvalidEvent for anything that
    doesn't parse, instead of silently passing a bad string through."""
    dt = _parse_time(value)
    if dt is None and value:
        raise InvalidEvent(f"Invalid {field_name}: not a valid ISO-8601 timestamp ({value!r})")
    return as_aware_utc(dt)


def normalize_location(location) -> str:
    """Case/whitespace-normalized location string — mirrors the
    initcap(trim(...)) normalization applied at the DataFrame level in
    spark/streaming_job.py's parse_events(), so a direct process_event()
    call (unit tests, storage.requeue_stuck --inline) enforces the same rule."""
    if not location:
        return None
    collapsed = re.sub(r"\s+", " ", str(location).strip())
    return collapsed.title() if collapsed else None


class InvalidEvent(Exception):
    pass


def clean_event(event: dict) -> dict:
    """Data cleaning + validation (section 23/41). Raises InvalidEvent for
    anything malformed rather than silently coercing/faking values."""
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
        # resolve_image_path copes with a DB moved between hosts/folders
        # (stored path may be absolute-and-stale or relative to the project)
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
    """Validation for a Google Drive image (the Drive counterpart of the
    local-disk checks in clean_event): the file must still exist in Drive,
    not be trashed, and carry this event's image_id (set by the phone when it
    uploaded the photo) — so a Kafka event can't point Spark at an
    arbitrary Drive file."""
    file_id = drive_file_id_from_path(event["image_path"])
    if event.get("drive_file_id") and event["drive_file_id"] != file_id:
        raise InvalidEvent("drive_file_id does not match image_path")
    event["drive_file_id"] = file_id
    if not SPARK_RECHECK_DRIVE_FILE and _registered_from_drive(event):
        # storage/uploads checked this very file moments ago from its Drive
        # metadata (owner, own folder, image_id tag, name, type, size) and
        # recorded it under this image_id. Asking Drive again cost 0.5 s per
        # photo, and up to 2 min when Google throttled a large batch. If the
        # file has gone since, the download fails and the photo is marked FAILED.
        return event
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
    """True when the database has this image_id as a Drive upload registered
    (storage/uploads.register_upload, from the file's Drive metadata) for
    exactly this Drive file, by this user. An event naming any other file
    gets the full Drive check."""
    with SessionLocal() as session:
        image = session.get(Image, event["image_id"])
        return (image is not None and image.source == "android_drive_upload"
                and image.image_path == event["image_path"] and image.user_id == event["user_id"])


_METADATA_COLUMNS = ("camera_make", "camera_model", "image_width", "image_height")


def extract_metadata(event: dict, image_bytes: bytes) -> dict:
    """Metadata extraction stage: reads where/when/with-what the photo was
    taken from the ORIGINAL file's EXIF (storage.exif_utils.extract_image_metadata)
    and stores it on the Image row. Must run before prepare_image(), which
    re-encodes the photo without EXIF.

    uploaded_at is never touched (it is the server/Google upload time, a
    different fact from the capture time). captured_at is only filled when
    nothing set it yet, same rule as before.

    GPS: the photo's own EXIF tags win (that is where it was taken). With
    no GPS tags, the phone position the app sent (gps_source "device", set
    by kafka/drive_watcher.py) is kept instead of being blanked."""
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
    """The CNN's input for one photo, as a float32 (3, S, S) tensor. All of
    the image preprocessing happens here in Spark; the CNN only classifies:
        prepare_image (upright, at most ANALYSIS_MAX_PX) -> decode ->
        resize the SHORTER side to S (aspect kept) -> centre S x S crop ->
        to [0, 1] -> (x - mean) / std.
    S, mean and std are the deployed checkpoint's (ml.inference.input_spec),
    so a retrained model is followed automatically.

    Why a centre crop and not the training evaluation's squeeze to S x S:
    on the model's held-out test split (646 photos, 2026-10-24) the crop
    scored 95.67% and the squeeze 94.74%; on its 78 real field photos 78.2%
    against 70.5% (padding 64.1%, three crops averaged 75.6%). Squeezing a
    wide field photo distorts the leaf. The crop leaves out the edges of a
    non-square photo, so the leaf should be in the middle of the picture.

    `prepared_bytes`: prepare_image(original_bytes) when the caller already
    has it (spark/streaming_job.process_event)."""
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
    """The picture the CNN receives, viewable: model_input() before the
    normalisation (the centre S x S square), as a PIL image. Display only
    (scalability test previews)."""
    from torchvision import transforms
    size = input_spec()["img_size"]
    img = _prepared_rgb(original_bytes, prepared_bytes)
    return transforms.CenterCrop(size)(transforms.Resize(size)(img))


def _prepared_rgb(original_bytes, prepared_bytes=None):
    """prepare_image()'s copy of the photo, decoded as upright RGB."""
    data = prepared_bytes if prepared_bytes is not None else prepare_image(original_bytes)
    return ImageOps.exif_transpose(PILImage.open(BytesIO(data)).convert("RGB"))


def prepare_image(image_bytes: bytes) -> bytes:
    """The photo every later step works on: upright (EXIF orientation
    applied), at most ANALYSIS_MAX_PX on its longer side, re-encoded without
    EXIF — the CNN input (model_input), the farmer's marked-up image and the
    labelling copy all start from it, instead of each decoding the full
    8-12 MP phone photo. JPEG draft mode decodes straight at a reduced
    scale, so even this one decode is cheap. Run extract_metadata() on the
    ORIGINAL bytes first. An unreadable image is passed through unchanged
    for the later steps to reject."""
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
    """The scalability-test run (storage.database.ScaleTestRun) this event
    belongs to, or None for every normal upload. Decided by the database —
    the batch loader's ScaleTestItem row — not by the event alone, so a
    stray run_id can't change how a farmer's photo is handled."""
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
    """Keep the photo downloaded from Google Drive on local disk for good
    (storage.image_storage.store_downloaded_photo). A reprocess rewrites the same
    file. Never fails the diagnosis."""
    if not SAVE_DOWNLOADED_PHOTOS:
        return
    try:
        store_downloaded_photo(user_id, image_id, original_bytes)
    except Exception as e:  # noqa: BLE001
        print(f"[preprocessing] Could not save downloaded photo {image_id}: {e}")


def save_for_review(image_id: str, image_bytes: bytes) -> None:
    """Copy an analysed farmer photo to REVIEW_DIR/<image_id>.jpg for the
    dashboard's "Label photos" page, where an admin puts it in the right
    class for the next training. The original photo (not the
    background-cleaned one), re-encoded upright WITHOUT its EXIF so the
    farmer's GPS location doesn't end up in the training data. Never fails
    the diagnosis."""
    if not SAVE_PHOTOS_FOR_REVIEW:
        return
    try:
        img = _prepared_rgb(None, image_bytes)       # already upright and EXIF-free
        REVIEW_DIR.mkdir(parents=True, exist_ok=True)
        tmp = REVIEW_DIR / f"{image_id}.jpg.tmp"
        img.save(tmp, format="JPEG", quality=95)
        tmp.replace(REVIEW_DIR / f"{image_id}.jpg")
    except Exception as e:  # noqa: BLE001
        print(f"[preprocessing] Could not save {image_id} for labelling: {e}")


def store_processed_image(image_id: str, original_bytes: bytes, prepared_bytes: bytes) -> None:
    """Stores the processed image, the picture the CNN receives (the centre
    S x S square, before normalisation), as PROCESSED_IMAGES_DIR/<image_id>.png
    (lossless). Never fails the diagnosis."""
    try:
        PROCESSED_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        tmp = PROCESSED_IMAGES_DIR / f"{image_id}.png.tmp"
        model_input_preview(original_bytes, prepared_bytes).save(tmp, format="PNG")
        tmp.replace(PROCESSED_IMAGES_DIR / f"{image_id}.png")
    except Exception as e:  # noqa: BLE001
        print(f"[preprocessing] Could not store the processed image of {image_id}: {e}")
