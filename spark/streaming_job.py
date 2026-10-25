"""
Spark Structured Streaming job (DIT manual section 22). For each photo event
from Kafka:

    register the upload          storage/uploads
    check, get, preprocess and
    store the image              spark/preprocessing
    classify                     ml/inference (CNN)
    risk level                   ml/risk
    save + send result to Drive  storage/results

The checkpoint gives at-least-once delivery, so a replayed event is safe:
persist_result replaces the earlier prediction.

Run: python -m spark.streaming_job (needs Kafka at KAFKA_BOOTSTRAP_SERVERS).
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from spark.session import kafka_session    # before pyspark: the Windows Hadoop workaround
from pyspark.sql.functions import (
    coalesce, col, concat, current_timestamp, from_json, initcap, lit, regexp_replace, to_timestamp, trim, when,
)
from pyspark.sql.types import StructType, StructField, StringType

from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from config.settings import (
    SPARK_CHECKPOINT_ROOT, SPARK_MAX_EVENTS_PER_BATCH, SPARK_TRIGGER_SECONDS, SPARK_PARALLEL_PHOTOS,
    SPARK_DRIVE_UPLOAD_WORKERS, SPARK_AUTO_RESEND_SECONDS, SPARK_AUTO_RESEND_AFTER_SECONDS, SPARK_AUTO_RESEND_MAX,
)
from spark.preprocessing import (
    InvalidEvent, clean_event, extract_metadata, prepare_image, model_input, store_processed_image,
    save_for_review, save_downloaded_photo, scale_test_run_for,
)
from ml.inference import predict as run_model, model_version as get_model_version, disease_heatmap
from ml.risk import compute_risk
from ml.render import render_processed_image, heatmap_on_photo
from storage.database import ProcessingStatus, init_db
from storage.duplicates import check_duplicate_by_hash, resend_duplicates_of
from storage.image_storage import is_drive_path, load_image_bytes, downloaded_photo_path
from storage.results import (
    FEATURE_FIELDS, persist_result, mark_processing, replay_state, publish_result_to_drive_safely,
)
from storage.uploads import register_upload
from spark import live_log

CHECKPOINT_DIR = str(SPARK_CHECKPOINT_ROOT / KAFKA_TOPIC)

EVENT_SCHEMA = StructType([
    # "drive_upload" = a new Drive photo; "submit" = a photo already in the database, sent again
    StructField("kind", StringType()),
    StructField("image_id", StringType()),
    StructField("user_id", StringType()),         # submit only
    StructField("location", StringType()),
    StructField("image_path", StringType()),
    StructField("drive_file_id", StringType()),
    StructField("captured_at", StringType()),
    StructField("uploaded_at", StringType()),
    # drive_upload only: the file's Drive metadata
    StructField("file_name", StringType()),
    StructField("mime_type", StringType()),
    StructField("size_bytes", StringType()),
    StructField("owner_email", StringType()),
    StructField("owner_name", StringType()),
    StructField("folder_email", StringType()),
    StructField("original_filename", StringType()),
    StructField("content_hash", StringType()),
    StructField("device_gps_lat", StringType()),
    StructField("device_gps_lon", StringType()),
    StructField("run_id", StringType()),          # old scalability-test events only; skipped
])

DRIVE_UPLOAD = "drive_upload"


def build_spark():
    return kafka_session("ImageSystem-CropDiseaseEWS")


def parse_events(raw):
    """Kafka JSON -> typed rows, validated with Spark SQL. A bad event is not
    dropped: it gets `rejected_reason` and is recorded as FAILED later."""
    parsed = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        col("value").cast("string").alias("raw_value"),
    ).select("data.*", "raw_value")

    def missing(field):
        return col(field).isNull() | (col(field) == "")

    is_upload = coalesce(col("kind"), lit("")) == DRIVE_UPLOAD
    rejected_reason = when(lit(False), lit(None))
    for field in ("image_id", "image_path", "uploaded_at"):
        rejected_reason = rejected_reason.when(missing(field), lit(f"Missing required field: {field}"))
    rejected_reason = (rejected_reason
                       .when(is_upload & missing("owner_email"), lit("Missing required field: owner_email"))
                       .when(~is_upload & missing("user_id"), lit("Missing required field: user_id")))
    rejected_reason = rejected_reason.when(
        to_timestamp(col("captured_at")) > current_timestamp(),
        concat(lit("captured_at is in the future: "), col("captured_at")),
    )

    return (
        parsed
        .withColumn("location", initcap(regexp_replace(trim(col("location")), r"\s+", " ")))
        .withColumn("rejected_reason", rejected_reason)
        # keep the raw record only for a rejected event
        .withColumn("raw_value", when(col("rejected_reason").isNotNull(), col("raw_value")))
    )


_DOWNLOADS = ThreadPoolExecutor(max_workers=16, thread_name_prefix="drive-download")


def process_event(event: dict) -> dict:
    """One photo: Spark preprocesses it, the CNN classifies it, then the risk
    level. Returns the result to save; raises on failure (never a fake result)."""
    timings = {}        # seconds per step, for the scalability report
    clock = time.perf_counter()

    def lap(name):
        nonlocal clock
        now = time.perf_counter()
        timings[name] = round(now - clock, 4)
        clock = now

    # start the Drive download while the event is checked; use a local copy if there is one
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
    scale_run = scale_test_run_for(event)

    prediction_started_at = datetime.now(timezone.utc)
    if local_copy is not None:
        original_bytes = local_copy.read_bytes()
    elif download is not None:
        original_bytes = download.result()
        save_downloaded_photo(event["user_id"], event["image_id"], original_bytes)
    else:
        original_bytes = Path(event["image_path"]).read_bytes()
    lap("read_s")
    if scale_run is None:                         # scalability tests reuse the same photos on purpose
        check_duplicate_by_hash(event, original_bytes)
    extract_metadata(event, original_bytes)       # needs the original (EXIF)
    lap("metadata_s")
    image_bytes = prepare_image(original_bytes)
    cnn_input = model_input(original_bytes, image_bytes)
    if scale_run is None:
        store_processed_image(event["image_id"], original_bytes, image_bytes)
    lap("preprocess_s")
    features = run_model(cnn_input)
    lap("cnn_s")
    used = features.get("inference") or {}
    timings.update({f"cnn_{k}": used.get(k) for k in ("batch_size", "precision", "input_s", "forward_s")})
    if scale_run is None:
        save_for_review(event["image_id"], image_bytes)

    is_uncertain = features.get("is_uncertain", False)
    risk_score, risk_level = compute_risk(features, is_uncertain=is_uncertain)

    predicted_at = datetime.now(timezone.utc)

    result = {
        "image_id": event["image_id"],
        "predicted_class": features.get("predicted_class"),
        "prediction_confidence": features.get("predicted_class_confidence"),
        **{k: features.get(k) for k in FEATURE_FIELDS},
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
        # the marked-up leaf for the farmer; a drawing error never fails the diagnosis
        try:
            heatmap = heatmap_on_photo(disease_heatmap(cnn_input), image_bytes) if not is_uncertain else None
            result["processed_image_jpeg"] = render_processed_image(image_bytes, result, heatmap)
        except Exception as e:
            print(f"[streaming_job] Could not draw processed image for {event['image_id']}: {e}")
        lap("render_s")
    return result


def process_and_persist(event: dict, rejected_reason: str = None, **stamps) -> tuple:
    """process_event, then save COMPLETED or FAILED. Never raises.
    Returns (result, status, error)."""
    result = None
    try:
        if rejected_reason:
            raise InvalidEvent(rejected_reason)
        result = process_event(event)
        persist_result(event, result, ProcessingStatus.COMPLETED, **stamps)
        return result, ProcessingStatus.COMPLETED, None
    except Exception as e:
        print(f"[streaming_job] Invalid event, skipping: {e}" if isinstance(e, InvalidEvent)
              else f"[streaming_job] Processing failed for {event.get('image_id')}: {e}")
        try:
            persist_result(event, None, ProcessingStatus.FAILED, error_message=str(e), **stamps)
        except Exception as db_error:  # noqa: BLE001 - the photo stays PROCESSING; requeue_stuck retries it
            print(f"[streaming_job] Could not record the failure for {event.get('image_id')}: {db_error}")
        return result, ProcessingStatus.FAILED, str(e)


def foreach_batch_sink(batch_df, batch_id):
    if batch_df.isEmpty():          # JVM-side check; .rdd would start a Python worker (crashes on Windows)
        return
    # dedupe this batch by image_id; rows without one are kept, each is rejected separately
    named = col("image_id").isNotNull() & (col("image_id") != "")
    deduped = batch_df.filter(named).dropDuplicates(["image_id"]).unionByName(batch_df.filter(~named))
    events = [row.asDict() for row in deduped.collect()]
    started = time.monotonic()
    live_log.batch_started(batch_id, len(events))
    if len(events) <= 1 or SPARK_PARALLEL_PHOTOS <= 1:
        uploads = [_process_one(event) for event in events]
    else:
        with ThreadPoolExecutor(max_workers=min(SPARK_PARALLEL_PHOTOS, len(events))) as pool:
            uploads = list(pool.map(_process_one, events))
    # commit the batch only after every Drive upload was attempted, so a crash replays it
    statuses = [upload.result() if upload is not None else None for upload in uploads]
    failed = [e.get("image_id") for e, s in zip(events, statuses) if s == ProcessingStatus.FAILED and e.get("image_id")]
    if resent := resend_duplicates_of(failed):
        live_log.say(f"[dup]    {resent} copy/copies of a photo that failed sent again, for their own result")
    live_log.batch_committed(batch_id, ok=statuses.count(ProcessingStatus.COMPLETED),
                             failed=statuses.count(ProcessingStatus.FAILED), skipped=statuses.count(None),
                             seconds=time.monotonic() - started)


_UPLOADS = ThreadPoolExecutor(max_workers=max(1, SPARK_DRIVE_UPLOAD_WORKERS), thread_name_prefix="drive-upload")


def _upload_result(event, status, error, processed_image):
    """Sends one result to Drive. Never raises; returns the photo's status."""
    started = time.monotonic()
    file_id = publish_result_to_drive_safely(event, status, error, processed_image=processed_image)
    if is_drive_path(event.get("image_path") or ""):
        live_log.result_delivered(event, file_id is not None, time.monotonic() - started)
    return status


def _process_one(event: dict):
    """One photo. Its Drive upload runs in the background; returns that
    Future, or None when the event is skipped. Never raises."""
    if event.get("run_id"):
        print(f"[streaming_job] {event.get('image_id')} is a scalability-test event "
              f"(run {event.get('run_id')}), skipping")
        return None
    rejected_reason = event.pop("rejected_reason", None)
    raw_value = event.pop("raw_value", None)
    if rejected_reason and not event.get("image_id"):
        event["raw_value"] = raw_value
    if (event.get("kind") == DRIVE_UPLOAD
            and all(event.get(f) for f in ("image_id", "image_path", "owner_email"))):
        # a refused upload still gets its row and fails below with the reason
        try:
            outcome = register_upload(event)
        except InvalidEvent as e:
            outcome, rejected_reason = "new", str(e)
        if outcome != "new":
            why = ("already registered" if outcome == "duplicate"
                   else "its image_id belongs to another Drive file (see rejected_events)")
            print(f"[streaming_job] {event.get('image_id')}: Drive file {event.get('drive_file_id')} "
                  f"{why}, skipping")
            return None
    state = replay_state(event.get("image_id"))
    if state == "completed":
        print(f"[streaming_job] {event.get('image_id')} already COMPLETED, skipping replayed event")
        return None
    if state == "result_missing":
        print(f"[streaming_job] {event.get('image_id')} COMPLETED but not in Drive yet, processing again")
    kafka_received_at = datetime.now(timezone.utc)
    photo_started = time.monotonic()
    image_id = event.get("image_id")
    if image_id:
        mark_processing(image_id)
    spark_started_at = datetime.now(timezone.utc)
    result, status, error = process_and_persist(event, rejected_reason, kafka_received_at=kafka_received_at,
                                                spark_started_at=spark_started_at)
    live_log.photo_done(event, status.value, result, error, time.monotonic() - photo_started)
    processed = result.get("processed_image_jpeg") if result else None
    return _UPLOADS.submit(_upload_result, event, status, error, processed)


def _warm_up_model() -> None:
    """Loads the CNN and runs one dummy photo, so the first farmer doesn't wait.
    Also imports torch before pyarrow (WinError 1114 on Windows)."""
    from io import BytesIO
    from PIL import Image as PILImage
    from ml import inference
    from spark.preprocessing import model_input
    buf = BytesIO()
    PILImage.new("RGB", (256, 256), (60, 140, 50)).save(buf, format="JPEG")
    x = model_input(buf.getvalue())
    inference.predict(x)
    inference.disease_heatmap(x)


def _auto_resend() -> None:
    """Background thread: re-sends photos still owed a result, every SPARK_AUTO_RESEND_SECONDS."""
    from storage.requeue_stuck import resend_owed
    while True:
        time.sleep(SPARK_AUTO_RESEND_SECONDS)
        try:
            if sent := resend_owed(SPARK_AUTO_RESEND_AFTER_SECONDS, SPARK_AUTO_RESEND_MAX):
                live_log.say(f"[resend] {sent} photo(s) still owed a result sent through the pipeline again")
        except Exception as e:  # noqa: BLE001 - try again next round
            print(f"[streaming_job] Automatic re-send failed, trying again later: {e}")


def run():
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)     # Spark's lazy creation fails on Windows
    init_db()
    _warm_up_model()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", SPARK_MAX_EVENTS_PER_BATCH)
        .load()
    )
    parsed = parse_events(raw)

    query = (
        parsed.writeStream
        .foreachBatch(foreach_batch_sink)
        .outputMode("append")
        .trigger(processingTime=f"{SPARK_TRIGGER_SECONDS} seconds")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )
    if SPARK_AUTO_RESEND_SECONDS > 0:
        threading.Thread(target=_auto_resend, name="auto-resend", daemon=True).start()
    query.awaitTermination()


if __name__ == "__main__":
    run()
