"""
Data cleaning, validation, image retrieval, image preprocessing for the CNN
(model_input: resize + centre crop + normalise), CNN inference, and
rule-based risk assessment for one Kafka event. Used by spark/streaming_job.py inside
its foreachBatch sink (Spark's Python UDFs can't easily call out to
torch/PIL per-row inside the JVM-side DataFrame execution, so the actual
model call happens in the driver's foreachBatch — a standard, documented
pattern for bringing Python ML into Structured Streaming).

Google Drive (Android -> Drive -> Kafka -> Spark -> Drive -> Android):
    For events whose image_path is "gdrive:<file id>", the image is
    downloaded from Google Drive (not read from local disk), the EXIF
    capture time is read here (the original bytes only reach the
    laptop here), and after the result is stored, publish_result_to_drive() writes
    /results/<image_id>.json to Google Drive for the Android app to read.

Classification vs risk (DIT manual section 26):
    CNN  -> predicted_class, prediction_confidence      (disease identity)
    Risk -> risk_score, risk_level                        (rule-based
             thresholds on the model's P(diseased), not its top-class
             confidence; see compute_risk)
"""

import hashlib
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    RISK_THRESHOLDS, IMAGE_STORAGE_PATH, REVIEW_DIR, SAVE_PHOTOS_FOR_REVIEW,
    ANALYSIS_MAX_PX, SAVE_DOWNLOADED_PHOTOS, SPARK_RECHECK_DRIVE_FILE,
)
from config.localtime import format_local
from storage.image_storage import (
    resolve_image_path, is_drive_path, drive_file_id_from_path, load_image_bytes,
    downloaded_photo_path, store_downloaded_photo,
)
from storage.exif_utils import extract_image_metadata
from ml.render import render_processed_image
from storage import drive_client
from ml.inference import predict as run_model, model_version as get_model_version, disease_heatmap, input_spec
from storage.database import (
    SessionLocal, Image, Prediction, ProcessingStatus, ProcessingEvent,
    RejectedEvent, User, ScaleTestItem, ScaleTestRun, SCALE_TEST_SOURCE,
)


REQUIRED_EVENT_FIELDS = ["image_id", "user_id", "image_path", "uploaded_at"]

# Photo downloads running alongside their metadata check (process_event).
# Network waits only, so a few more than SPARK_PARALLEL_PHOTOS is fine.
_DOWNLOADS = ThreadPoolExecutor(max_workers=16, thread_name_prefix="drive-download")


def _as_aware_utc(dt):
    """SQLite has no native timezone-aware datetime type, so a value stored
    as UTC and read back through SQLAlchemy loses its tzinfo (PostgreSQL
    preserves it correctly via TIMESTAMPTZ). Every value written by this
    codebase IS already UTC (config/settings.now_utc / datetime.now(utc)),
    so a naive value read back is safely re-attached as UTC rather than
    assumed to be local time."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_timestamp(value, field_name: str):
    """Parses an ISO-8601 string (what every event field is serialized as)
    into a timezone-aware datetime. Raises InvalidEvent for anything that
    doesn't parse, instead of silently passing a bad string through."""
    if value is None or value == "":
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise InvalidEvent(f"Invalid {field_name}: not a valid ISO-8601 timestamp ({value!r})")
    return _as_aware_utc(dt)


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


class DuplicateImage(InvalidEvent):
    """The photo's bytes are identical to an earlier photo from the same user
    (already completed, or being processed). A subclass of InvalidEvent, so every existing
    `except InvalidEvent` handler treats it like any other rejected event."""


# Set to False to switch the duplicate check off without touching anything else.
SKIP_DUPLICATE_PHOTOS = True

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
        print(f"[preprocessing] Duplicate check skipped for {event.get('image_id')}: {e}")
        return
    if earlier:
        raise DuplicateImage(f"Duplicate photo: identical to {earlier.image_id}")


def clean_event(event: dict) -> dict:
    """Data cleaning + validation (section 23/41). Raises InvalidEvent for
    anything malformed rather than silently coercing/faking values."""
    for key in REQUIRED_EVENT_FIELDS:
        if not event.get(key):
            raise InvalidEvent(f"Missing required field: {key}")

    uploaded_at = _parse_timestamp(event.get("uploaded_at"), "uploaded_at")
    captured_at = _parse_timestamp(event.get("captured_at"), "captured_at")

    now = datetime.now(timezone.utc)
    if uploaded_at is not None and uploaded_at > now:
        raise InvalidEvent(f"uploaded_at is in the future: {uploaded_at.isoformat()}")
    if captured_at is not None and captured_at > now:
        raise InvalidEvent(f"captured_at is in the future: {captured_at.isoformat()}")

    event["uploaded_at"] = uploaded_at.isoformat() if uploaded_at else event.get("uploaded_at")
    event["captured_at"] = captured_at.isoformat() if captured_at else None
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
        # spark/ingestion checked this very file moments ago from its Drive
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
    (spark/ingestion.register_upload, from the file's Drive metadata) for
    exactly this Drive file, by this user. An event naming any other file
    gets the full Drive check."""
    with SessionLocal() as session:
        image = session.get(Image, event["image_id"])
        return (image is not None and image.source == "android_drive_upload"
                and image.image_path == event["image_path"] and image.user_id == event["user_id"])


_GPS_COLUMNS = ("gps_latitude", "gps_longitude", "gps_altitude_m")
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
            if meta["gps_latitude"] is not None and meta["gps_longitude"] is not None:
                for name in _GPS_COLUMNS:
                    setattr(image, name, meta[name])
                image.gps_source = "exif"
            elif image.gps_source != "device":
                for name in _GPS_COLUMNS:
                    setattr(image, name, None)
                image.gps_source = None
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
    has it (process_event)."""
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


def heatmap_on_photo(cam, prepared_bytes: bytes):
    """Places the CNN's Grad-CAM (ml.inference.disease_heatmap: the map of
    the S x S square model_input() cut out) on the photo itself: an (H, W)
    float array in [0, 1] at the photo's size, the map stretched over the
    centre square the CNN saw and 0 on the cropped-off edges. None stays
    None (the model saw no disease)."""
    if cam is None:
        return None
    import numpy as np
    import torch
    import torch.nn.functional as F
    w, h = _prepared_rgb(None, prepared_bytes).size
    side = min(w, h)
    cam = torch.as_tensor(cam, dtype=torch.float32)[None, None]
    cam = F.interpolate(cam, size=(side, side), mode="bilinear", align_corners=False)[0, 0]
    cam = (cam / cam.max()).numpy()
    heat = np.zeros((h, w), dtype=np.float32)
    top, left = (h - side) // 2, (w - side) // 2
    heat[top:top + side, left:left + side] = cam
    return heat


def _prepared_rgb(original_bytes, prepared_bytes=None):
    """prepare_image()'s copy of the photo, decoded as upright RGB."""
    from io import BytesIO
    from PIL import Image as PILImage, ImageOps
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
    from io import BytesIO
    from PIL import Image as PILImage, ImageOps
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


def compute_risk(features: dict, is_uncertain: bool = False) -> tuple:
    """Rule-based, explainable risk indicator: the model's probability that
    the leaf is diseased at all (lesion_score = 1 - P(healthy)), put into
    LOW / MEDIUM / HIGH by RISK_THRESHOLDS. Deliberately NOT the CNN's
    top-class confidence (section 26): a confident "healthy" is low risk.

    `is_uncertain` (set when the CNN's top-class confidence fell below
    IMAGE_MODEL["confidence_threshold"] — see ml/inference.py) caps the
    result below HIGH: an unreliable read must not raise a HIGH-risk alert.
    """
    score = round(min(max(features.get("lesion_score") or 0.0, 0.0), 1.0), 3)

    if score >= RISK_THRESHOLDS["high"]:
        level = "HIGH"
    elif score >= RISK_THRESHOLDS["medium"]:
        level = "MEDIUM"
    else:
        level = "LOW"

    if is_uncertain and level == "HIGH":
        level = "MEDIUM"

    return score, level


def process_event(event: dict) -> dict:
    """Full per-image processing: validate -> read image -> extract EXIF
    metadata (GPS, capture time, camera) -> prepare the CNN input
    (model_input) -> CNN inference -> risk assessment. Returns a dict ready
    to persist as a Prediction row.
    Raises InvalidEvent / whatever ml.inference raises (e.g.
    ModelNotAvailable) on failure — callers must NOT substitute a fake
    prediction, per section 24/43."""
    # A Drive photo's download (~2 s) starts now, alongside clean_event's
    # Drive metadata check (~0.7 s) instead of after it. The bytes are only
    # used once that check has passed.
    # A photo already saved locally (save_downloaded_photo) is read from
    # disk instead of being downloaded again — e.g. a Reprocess, or a demo.
    # Seconds spent in each step (result["timings"]), for the scalability
    # test report (scalability/report.py). Measuring changes nothing else.
    timings = {}
    clock = time.perf_counter()

    def lap(name):
        nonlocal clock
        now = time.perf_counter()
        timings[name] = round(now - clock, 4)
        clock = now

    download = None
    local_copy = None
    if is_drive_path(event.get("image_path") or ""):
        local_copy = downloaded_photo_path(event.get("user_id"), event.get("image_id"))
        if local_copy is None:
            download = _DOWNLOADS.submit(load_image_bytes, event["image_path"])
    try:
        event = clean_event(event)
    except Exception:
        if download is not None:
            download.cancel()
        raise
    lap("validate_s")
    # A scalability-test photo (scalability/run.py): no copy for labelling,
    # and the marked-up image only when that run publishes results to Drive.
    scale_run = scale_test_run_for(event)

    prediction_started_at = datetime.now(timezone.utc)
    if local_copy is not None:
        original_bytes = local_copy.read_bytes()                 # the local copy of the Drive photo
    elif download is not None:
        original_bytes = download.result()                       # download from Google Drive
        save_downloaded_photo(event["user_id"], event["image_id"], original_bytes)
    else:
        original_bytes = Path(event["image_path"]).read_bytes()
    lap("read_s")
    if scale_run is None:                         # scalability tests deliberately reuse the same photo
        check_duplicate_by_hash(event, original_bytes)
    extract_metadata(event, original_bytes)       # EXIF: GPS, capture time, camera — needs the original
    lap("metadata_s")
    image_bytes = prepare_image(original_bytes)
    cnn_input = model_input(original_bytes, image_bytes)   # Spark's preprocessing for the CNN
    lap("preprocess_s")
    features = run_model(cnn_input)
    lap("cnn_s")
    used = features.get("inference") or {}
    timings["cnn_batch_size"] = used.get("batch_size")      # batch this photo ran in
    timings["cnn_precision"] = used.get("precision")
    timings["cnn_input_s"] = used.get("input_s")            # checking the prepared input, inside cnn_s
    timings["cnn_forward_s"] = used.get("forward_s")        # waiting for + running the forward pass
    if scale_run is None:
        save_for_review(event["image_id"], image_bytes)

    is_uncertain = features.get("is_uncertain", False)
    risk_score, risk_level = compute_risk(features, is_uncertain=is_uncertain)

    predicted_at = datetime.now(timezone.utc)

    result = {
        "image_id": event["image_id"],
        "predicted_class": features.get("predicted_class"),
        "prediction_confidence": features.get("predicted_class_confidence"),
        "lesion_score": features.get("lesion_score"),
        "discoloration_pct": features.get("discoloration_pct"),
        "texture_irregularity": features.get("texture_irregularity"),
        "leaf_area_affected_pct": features.get("leaf_area_affected_pct"),
        "risk_score": risk_score,
        "risk_level": risk_level,
        "is_uncertain": is_uncertain,
        "model_version": get_model_version(),
        "predicted_at": predicted_at,
        "prediction_started_at": prediction_started_at,
        "timings": timings,
    }
    lap("risk_s")
    wants_processed_image = (scale_run.publish_results if scale_run is not None
                             else is_drive_path(event["image_path"]))
    if wants_processed_image:
        # The marked-up leaf the farmer sees (publish_result_to_drive puts it
        # in Drive next to the result). A drawing problem never fails the
        # diagnosis itself.
        try:
            heatmap = heatmap_on_photo(disease_heatmap(cnn_input), image_bytes) if not is_uncertain else None
            result["processed_image_jpeg"] = render_processed_image(image_bytes, result, heatmap)
        except Exception as e:
            print(f"[preprocessing] Could not draw processed image for {event['image_id']}: {e}")
        lap("render_s")
    return result


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
        from io import BytesIO
        from PIL import Image as PILImage, ImageOps
        img = ImageOps.exif_transpose(PILImage.open(BytesIO(image_bytes))).convert("RGB")
        REVIEW_DIR.mkdir(parents=True, exist_ok=True)
        tmp = REVIEW_DIR / f"{image_id}.jpg.tmp"
        img.save(tmp, format="JPEG", quality=95)
        tmp.replace(REVIEW_DIR / f"{image_id}.jpg")
    except Exception as e:  # noqa: BLE001
        print(f"[preprocessing] Could not save {image_id} for labelling: {e}")


def replay_state(image_id: str):
    """The Spark sink's replay check, in ONE database lookup per event
    (~3 ms on the Docker PostgreSQL). Returns:
      "completed"      - already has its result; a replay is skipped. Asking the
                         database (instead of Spark's old dropDuplicates) still
                         lets a real Reprocess through: its status was reset.
      "result_missing" - a COMPLETED Drive photo whose result never reached
                         Drive (Spark stopped in between): upload it now.
      None             - process it."""
    if not image_id:
        return None
    with SessionLocal() as session:
        image = session.get(Image, image_id)
        if image is None:
            return None
        if image.status == ProcessingStatus.COMPLETED:
            if is_drive_path(image.image_path) and image.result_published_at is None:
                return "result_missing"
            return "completed"
        return None


def mark_processing(image_id: str) -> None:
    """Flips an Image to PROCESSING right before inference starts, so a
    long-running CNN call is visible in the dashboard as in-flight rather
    than silently still showing INGESTED. Best-effort: an unknown image_id
    is a no-op (persist_result's own guard handles that case properly)."""
    with SessionLocal() as session:
        image = session.get(Image, image_id)
        if image is not None:
            image.status = ProcessingStatus.PROCESSING
            session.commit()


def persist_result(
    event: dict,
    result: dict,
    status: ProcessingStatus,
    error_message: str = None,
    *,
    kafka_received_at=None,
    spark_started_at=None,
):
    """Writes a Prediction (+ ProcessingEvent) row and updates the parent
    Image's status/timestamps. Used by spark/streaming_job.py (Spark's
    foreachBatch sink); kept in this module — which only depends on
    SQLAlchemy, not pyspark — so the persistence logic is isolated from the
    Spark runtime and directly unit-testable.

    `kafka_received_at`/`spark_started_at` are the genuine Spark-side
    timestamps (captured by foreach_batch_sink before calling
    process_event); if omitted (e.g. a direct unit-test call) they default
    to "now" so this function's own behavior stays self-contained.

    Idempotent on image_id for the Prediction row: Structured Streaming's
    checkpoint only guarantees at-least-once delivery to foreachBatch (see
    spark/streaming_job.py docstring), so a crash-and-replay can call this
    twice for the same event. Any existing Prediction for this image_id is
    deleted before the new one is inserted, so a replay overwrites rather
    than duplicates. ProcessingEvent rows are an append-only audit log and
    are deliberately NOT deduped — a replay is itself another processing
    attempt worth recording.

    If image_id doesn't correspond to any Image row at all (a malformed or
    corrupted event, or one referencing a since-deleted image), nothing was
    previously recorded anywhere except a stdout print() — now a
    RejectedEvent row is written instead, so the event leaves a trace."""
    with SessionLocal() as session:
        image_id = event.get("image_id")
        image = session.get(Image, image_id) if image_id else None
        if image is None:
            session.add(RejectedEvent(
                image_id=image_id,
                raw_event=json.dumps(event, default=str),
                reason=error_message or "No matching Image row for this image_id",
            ))
            session.commit()
            return

        now = datetime.now(timezone.utc)
        kafka_received_at = _as_aware_utc(kafka_received_at) or now
        spark_started_at = _as_aware_utc(spark_started_at) or now
        spark_completed_at = datetime.now(timezone.utc)

        if image.ingested_at is None:
            image.ingested_at = now
        if "location" in event:
            # Spark's cleaned location (parse_events / clean_event), so a row
            # registered before the cleaning rule existed is corrected too.
            image.location = event["location"]
        image.processed_at = now
        image.status = status
        image.error_message = error_message

        if result is not None:
            latency_ms = None
            ingested_at = _as_aware_utc(image.ingested_at)
            if ingested_at:
                latency_ms = int((result["predicted_at"] - ingested_at).total_seconds() * 1000)

            # Delete-then-insert: makes a replayed event idempotent instead
            # of piling up a second Prediction row for the same image.
            session.query(Prediction).filter(Prediction.image_id == image_id).delete()

            prediction = Prediction(
                image_id=image_id,
                predicted_class=result["predicted_class"],
                prediction_confidence=result["prediction_confidence"],
                lesion_score=result["lesion_score"],
                discoloration_pct=result["discoloration_pct"],
                texture_irregularity=result["texture_irregularity"],
                leaf_area_affected_pct=result["leaf_area_affected_pct"],
                risk_score=result["risk_score"],
                risk_level=result["risk_level"],
                is_uncertain=result.get("is_uncertain", False),
                model_version=result["model_version"],
                predicted_at=result["predicted_at"],
                processing_latency_ms=latency_ms,
            )
            session.add(prediction)
            if result["risk_level"] == "HIGH":
                print(f"[ALERT] image={image_id} location={event.get('location')} "
                      f"risk={result['risk_score']} class={result['predicted_class']}")

        total_latency_ms = int((spark_completed_at - kafka_received_at).total_seconds() * 1000)

        session.add(ProcessingEvent(
            image_id=image_id,
            api_published_at=image.ingested_at,
            kafka_received_at=kafka_received_at,
            spark_started_at=spark_started_at,
            spark_completed_at=spark_completed_at,
            prediction_started_at=result.get("prediction_started_at") if result else None,
            prediction_completed_at=result["predicted_at"] if result else None,
            total_latency_ms=total_latency_ms,
            status=status,
            error_message=error_message,
        ))
        session.commit()


# ---------------------------------------------------------------------------
# Spark -> Google Drive /results
# ---------------------------------------------------------------------------
def _prediction_to_dict(p: Prediction) -> dict:
    """The prediction fields of a Drive result file, in the shape the Android
    app parses (DriveRest.kt)."""
    return {
        "prediction_id": p.prediction_id, "image_id": p.image_id,
        "predicted_class": p.predicted_class, "prediction_confidence": p.prediction_confidence,
        "lesion_score": p.lesion_score, "discoloration_pct": p.discoloration_pct,
        "texture_irregularity": p.texture_irregularity, "leaf_area_affected_pct": p.leaf_area_affected_pct,
        "risk_score": p.risk_score, "risk_level": p.risk_level.value if p.risk_level else None,
        "is_uncertain": p.is_uncertain,
        "model_version": p.model_version,
        "predicted_at": p.predicted_at.isoformat() if p.predicted_at else None,
        "processing_latency_ms": p.processing_latency_ms,
    }


def publish_result_to_drive(event: dict, status: ProcessingStatus, error_message: str = None,
                            processed_image: bytes = None):
    """Writes the result for one image into the uploader's private Drive
    folder /results/<email>/ (shared only with them):
        <image_id>.json            the result, built from what persist_result()
                                   just stored, so Drive and database agree
        <image_id>_processed.jpg   the marked-up leaf (when `processed_image`
                                   is given), shown in the app
    Only for Google Drive images; returns the JSON file id, or None for
    legacy local-disk images. Re-processing the same image overwrites the
    same two files (drive_client.upload_bytes), matching persist_result's
    idempotency."""
    image_id = event.get("image_id")
    test_folder = None
    with SessionLocal() as session:
        image = session.get(Image, image_id) if image_id else None
        if image is None:
            return None
        if image.source == SCALE_TEST_SOURCE:
            # Scalability test: only when its run publishes results, and into
            # that run's own Drive folder — never a farmer's results folder.
            item = session.get(ScaleTestItem, image_id)
            run = session.get(ScaleTestRun, item.run_id) if item else None
            if run is None or not run.publish_results or not run.drive_results_folder_id:
                return None
            test_folder = run.drive_results_folder_id
            drive_image_file_id = item.source_drive_file_id
        elif not is_drive_path(image.image_path):
            return None
        else:
            drive_image_file_id = drive_file_id_from_path(image.image_path)
        prediction = (
            session.query(Prediction).filter(Prediction.image_id == image_id)
            .order_by(Prediction.predicted_at.desc()).first()
        )
        owner = session.get(User, image.user_id)
        owner_email = owner.email if owner else ""
        payload = {
            "image_id": image.image_id,
            "user_id": image.user_id,
            "drive_image_file_id": drive_image_file_id,
            "processed_file_id": None,
            "location": image.location,
            "captured_at": image.captured_at.isoformat() if image.captured_at else None,
            "uploaded_at": image.uploaded_at.isoformat() if image.uploaded_at else None,
            # same two times, ready to show (Timor-Leste Time)
            "captured_at_local": format_local(image.captured_at) or None,
            "uploaded_at_local": format_local(image.uploaded_at) or None,
            "gps_latitude": image.gps_latitude,
            "gps_longitude": image.gps_longitude,
            "gps_altitude_m": image.gps_altitude_m,
            "gps_source": image.gps_source,
            "camera_make": image.camera_make,
            "camera_model": image.camera_model,
            "status": status.value,
            "error_message": error_message,
            "prediction": _prediction_to_dict(prediction) if (prediction and status == ProcessingStatus.COMPLETED) else None,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

    folder = test_folder or drive_client.user_folder_id(
        owner_email, drive_client.USER_RESULTS_KIND, create=True, share_role="reader"
    )
    if processed_image and status == ProcessingStatus.COMPLETED:
        name = f"{image_id}_processed.jpg"
        payload["processed_file_id"] = _RESULT_FILES.remember(folder, name, drive_client.upload_bytes(
            name, processed_image, "image/jpeg", folder,
            {"image_id": image_id, "user_email": owner_email, "kind": "processed"},
            may_exist=False, existing_id=_RESULT_FILES.existing(folder, name),
        ))
    pred = payload["prediction"] or {}
    # Short summary in Drive appProperties: lets the Android app list all
    # of its results (Home / History) with ONE Drive request, without
    # downloading every JSON file.
    summary = {
        "image_id": image_id,
        "user_id": payload["user_id"],
        "user_email": owner_email,
        "kind": "result",
        "status": status.value,
        "drive_image_file_id": payload["drive_image_file_id"],
        "processed_file_id": payload["processed_file_id"] or "",
        "location": payload["location"] or "",
        "uploaded_at": payload["uploaded_at"] or "",
        "predicted_class": pred.get("predicted_class") or "",
        "prediction_confidence": pred.get("prediction_confidence") if pred else "",
        "risk_level": pred.get("risk_level") or "",
        "risk_score": pred.get("risk_score") if pred else "",
        "error_message": (error_message or "")[:80],
    }
    name = f"{image_id}.json"
    file_id = _RESULT_FILES.remember(folder, name, drive_client.upload_json(
        name, payload, folder, summary, may_exist=False, existing_id=_RESULT_FILES.existing(folder, name)))
    _mark_result_published(image_id)
    return file_id


class _ResultFolderIndex:
    """name -> file id of the result files already in each results folder,
    listed from Drive ONCE per folder per Spark process (one list request
    per 100 files) and kept up to date as Spark writes. A result is then
    updated in place when it exists and created only when it doesn't, with
    no "does it exist?" search per file.

    It replaced a guess from the database ("first processing attempt ->
    can't be in Drive yet"), which a factory reset made wrong: the database
    forgot the photos but Drive still had their results, and 1,163 duplicate
    result files were created on 2026-10-09. Only Spark writes into these
    folders, and the factory reset (which trashes them) runs with Spark
    stopped, so the index can't go stale behind its back."""

    def __init__(self):
        self._folders = {}
        self._lock = threading.Lock()
        self._loading = {}

    def _index(self, folder: str) -> dict:
        with self._lock:
            if folder in self._folders:
                return self._folders[folder]
            loading = self._loading.setdefault(folder, threading.Lock())
        with loading:                          # one listing per folder, even with 8 threads
            with self._lock:
                if folder in self._folders:
                    return self._folders[folder]
            names = {}
            for f in drive_client.list_files_in_folders([folder]):    # oldest first: newest copy wins
                names[f["name"]] = f["id"]
            with self._lock:
                self._folders[folder] = names
            return names

    def existing(self, folder: str, name: str):
        return self._index(folder).get(name)

    def remember(self, folder: str, name: str, file_id: str) -> str:
        with self._lock:
            self._folders.setdefault(folder, {})[name] = file_id
        return file_id

    def clear(self):
        with self._lock:
            self._folders.clear()


_RESULT_FILES = _ResultFolderIndex()


def _mark_result_published(image_id: str) -> None:
    """Images.result_published_at = now: this photo's result is in Drive."""
    with SessionLocal() as session:
        image = session.get(Image, image_id)
        if image is not None:
            image.result_published_at = datetime.now(timezone.utc)
            session.commit()


def publish_result_to_drive_safely(event: dict, status: ProcessingStatus, error_message: str = None,
                                   processed_image: bytes = None):
    """publish_result_to_drive, but a Drive outage only logs: the result is
    already safe in the database (the dashboard still shows it), and
    re-queuing the image writes it to Drive again."""
    try:
        return publish_result_to_drive(event, status, error_message, processed_image)
    except Exception as e:
        print(f"[preprocessing] Could not write result to Google Drive for {event.get('image_id')}: {e}")
        return None
