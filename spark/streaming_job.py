"""
Apache Spark Structured Streaming job — the main streaming-processing
engine (DIT manual section 22).

Pipeline:
    Kafka (maize-disease-images)
      -> Read Stream
      -> Parse JSON
      -> Register a new upload: farmer, upload checks, duplicate skip,
         first database row         (storage/uploads.register_upload)
      -> Validate / Data Cleaning   (spark/preprocessing.clean_event)
      -> Retrieve Image             (Google Drive, or the local copy)
      -> Duplicate photo check      (storage/duplicates)
      -> Metadata Extraction: EXIF GPS location, capture time, camera
                                     (spark/preprocessing.extract_metadata)
      -> Image Preprocessing: upright, resize, centre crop to the model's
         size, normalise, and store the processed image
                                     (spark/preprocessing.model_input,
                                      store_processed_image)
      -> CNN classifies the image   (ml/inference.predict)
      -> Risk level                 (ml/risk.compute_risk)
      -> Write predictions + update image status to PostgreSQL
                                     (storage/results.persist_result)
      -> Result to the farmer's Google Drive folder
                                     (storage/results.publish_result_to_drive)
process_event below runs these steps for one photo.

Checkpointing: this job writes to CHECKPOINT_DIR (below) so a restart
resumes from the last committed Kafka offset instead of silently skipping
whatever was uploaded while it was down. One honest caveat that comes with
that: Structured Streaming's checkpoint gives *at-least-once* delivery to
foreachBatch, not exactly-once — if Spark crashes mid-batch and replays it
after restart, the same event can reach storage/results.persist_result()
twice. persist_result() copes with this by deleting any existing Prediction
row for the image_id before inserting the new one (delete-then-insert), so a
replay overwrites rather than duplicates — see its docstring. ProcessingEvent
rows are an append-only audit log and are NOT deduped this way; a replay is
itself another processing attempt worth recording there.

Run (needs a running Kafka broker at KAFKA_BOOTSTRAP_SERVERS):
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      spark/streaming_job.py

REQUIRES EXTERNAL INFRASTRUCTURE for a real deployment: a running Kafka
broker and (for --packages to resolve) internet access to Maven Central the
first time it runs.

Runs in Spark **local mode** on this laptop; an actual multi-node cluster
has not been exercised.
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
    # "drive_upload": a new photo, from its Drive metadata (kafka/drive_watcher.
    # build_drive_event) — storage/uploads registers it. "submit" (or no kind,
    # older events): a photo already in the database is sent again
    # (storage/pipeline_submit.build_upload_event).
    StructField("kind", StringType()),
    StructField("image_id", StringType()),
    StructField("user_id", StringType()),         # submit only; ingestion resolves it for drive_upload
    StructField("location", StringType()),
    StructField("image_path", StringType()),
    StructField("drive_file_id", StringType()),   # Google Drive file id (None for legacy local images)
    StructField("captured_at", StringType()),
    StructField("uploaded_at", StringType()),
    # drive_upload only: what the Drive watcher read from the file's metadata
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
    # Only on scalability-test events published here before the test got its
    # own topic (2026-10-10); _process_one skips them. Null for every upload.
    StructField("run_id", StringType()),
])

DRIVE_UPLOAD = "drive_upload"


def build_spark():
    return kafka_session("ImageSystem-CropDiseaseEWS")


def parse_events(raw):
    """Kafka record value (JSON bytes) -> one typed row per upload event,
    then a genuine distributed-DataFrame cleaning stage (DIT manual 5.4):
    check the required fields (owner_email for a drive_upload, user_id for
    a submit event), validate captured_at as a real timestamp
    that is not in the future, and normalize location casing/whitespace.
    This runs as real Spark SQL transformations on the streaming DataFrame
    — NOT per-row Python — before foreachBatch's per-row CNN inference /
    persistence work (which can't be expressed as a DataFrame
    transformation) ever sees a row.

    An event that fails a check is NOT filtered out: it keeps flowing with
    `rejected_reason` set (EIP Invalid Message Channel). Filtering it used
    to drop it silently, so a photo whose phone clock ran ahead (a future
    captured_at) sat at INGESTED for ever, and storage.requeue_stuck only
    re-sent it to be dropped again. _process_one records a rejected event
    as FAILED with its reason, and the farmer gets that result in Drive;
    one too broken to name an image lands in rejected_events with its raw
    Kafka value (the dead-letter table).

    spark/preprocessing.clean_event() enforces the same rules again,
    per-row, so a direct process_event() call (unit tests, or
    storage.requeue_stuck --inline, which doesn't go through this
    DataFrame) can't skip validation.

    Duplicate image_ids are NOT dropped here any more: a streaming
    dropDuplicates without a watermark keeps every image_id in state
    forever, and silently dropped a Reprocess of an image Kafka had seen
    before (the image then stayed INGESTED for good). foreach_batch_sink
    dedupes each micro-batch and skips images that are already COMPLETED.

    Extracted from run() so the exact projection the streaming query uses
    can be exercised without a Kafka broker."""
    parsed = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        col("value").cast("string").alias("raw_value"),
    ).select("data.*", "raw_value")

    def missing(field):
        return col(field).isNull() | (col(field) == "")

    # A drive_upload names its uploader by Google account (ingestion resolves
    # the user_id); a submit event already carries the user_id.
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
        # the raw record is only kept for a rejected event (rejected_events.raw_event)
        .withColumn("raw_value", when(col("rejected_reason").isNotNull(), col("raw_value")))
    )


# Photo downloads running alongside their metadata check (process_event).
# Network waits only, so a few more than SPARK_PARALLEL_PHOTOS is fine.
_DOWNLOADS = ThreadPoolExecutor(max_workers=16, thread_name_prefix="drive-download")


def process_event(event: dict) -> dict:
    """One photo, each step by the part that owns it:
        Spark   check the event, get the photo, EXIF, preprocess it and
                store the processed image          (spark/preprocessing.py)
        CNN     classify it                        (ml/inference.predict)
        risk    from the CNN's output              (ml/risk.compute_risk)
    Returns a dict ready to persist as a Prediction row (storage/results.py).
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
    if scale_run is None:
        store_processed_image(event["image_id"], original_bytes, image_bytes)
    lap("preprocess_s")
    features = run_model(cnn_input)
    lap("cnn_s")
    # The batch this photo ran in, its precision, checking the prepared input
    # (inside cnn_s), and waiting for + running the forward pass.
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
        # The marked-up leaf the farmer sees (publish_result_to_drive puts it
        # in Drive next to the result). A drawing problem never fails the
        # diagnosis itself.
        try:
            heatmap = heatmap_on_photo(disease_heatmap(cnn_input), image_bytes) if not is_uncertain else None
            result["processed_image_jpeg"] = render_processed_image(image_bytes, result, heatmap)
        except Exception as e:
            print(f"[streaming_job] Could not draw processed image for {event['image_id']}: {e}")
        lap("render_s")
    return result


def process_and_persist(event: dict, rejected_reason: str = None, **stamps) -> tuple:
    """process_event + persist_result for one photo (the Spark sink and
    storage.requeue_stuck --inline): COMPLETED with its result, or FAILED
    with the error — an InvalidEvent too, e.g. `rejected_reason` from
    parse_events. Never raises: if even the failure can't be saved, the
    photo stays PROCESSING and storage/requeue_stuck.py sends it again.
    `stamps`: persist_result's kafka_received_at / spark_started_at.
    Returns (result, status, error)."""
    result = None
    try:
        if rejected_reason:
            raise InvalidEvent(rejected_reason)
        result = process_event(event)
        persist_result(event, result, ProcessingStatus.COMPLETED, **stamps)
        return result, ProcessingStatus.COMPLETED, None
    except Exception as e:                     # an InvalidEvent too, only reported differently
        print(f"[streaming_job] Invalid event, skipping: {e}" if isinstance(e, InvalidEvent)
              else f"[streaming_job] Processing failed for {event.get('image_id')}: {e}")
        try:
            persist_result(event, None, ProcessingStatus.FAILED, error_message=str(e), **stamps)
        except Exception as db_error:  # noqa: BLE001 - don't take the other photos (or the job) down with it
            print(f"[streaming_job] Could not record the failure for {event.get('image_id')}: {db_error}")
        return result, ProcessingStatus.FAILED, str(e)


def foreach_batch_sink(batch_df, batch_id):
    # DataFrame.isEmpty() (Spark 3.3+) is a JVM-only check. batch_df.rdd
    # would force an RDD conversion that spawns an executor-side Python
    # worker — unnecessary here (all per-event work below runs in the
    # driver) and a frequent source of "Python worker crashed" failures
    # on Windows.
    if batch_df.isEmpty():
        return
    # Stateless dedupe of THIS micro-batch only (see parse_events).
    # Records with no image_id (rejected ones) are kept apart: dropDuplicates
    # would fold them all into one and only one would reach rejected_events.
    named = col("image_id").isNotNull() & (col("image_id") != "")
    deduped = batch_df.filter(named).dropDuplicates(["image_id"]).unionByName(batch_df.filter(~named))
    events = [row.asDict() for row in deduped.collect()]
    started = time.monotonic()
    live_log.batch_started(batch_id, len(events))
    # Several photos at once: they overlap instead of queueing. dropDuplicates
    # above means no two threads ever get the same image_id.
    if len(events) <= 1 or SPARK_PARALLEL_PHOTOS <= 1:
        uploads = [_process_one(event) for event in events]
    else:
        with ThreadPoolExecutor(max_workers=min(SPARK_PARALLEL_PHOTOS, len(events))) as pool:
            uploads = list(pool.map(_process_one, events))
    # The batch (and its Kafka offsets) only commits once every photo's
    # result is in the database and its Drive upload has been ATTEMPTED, so
    # a crash can't lose a phone's result: Spark replays the batch instead.
    # An upload that failed (Drive down) only logs; the photo is left
    # COMPLETED with result_published_at empty, which System Status counts
    # and `python -m storage.requeue_stuck` re-sends.
    statuses = [upload.result() if upload is not None else None for upload in uploads]
    # Copies rejected as duplicates of a photo that has now FAILED get their own turn.
    failed = [e.get("image_id") for e, s in zip(events, statuses) if s == ProcessingStatus.FAILED and e.get("image_id")]
    if resent := resend_duplicates_of(failed):
        live_log.say(f"[dup]    {resent} copy/copies of a photo that failed sent again, for their own result")
    live_log.batch_committed(batch_id, ok=statuses.count(ProcessingStatus.COMPLETED),
                             failed=statuses.count(ProcessingStatus.FAILED), skipped=statuses.count(None),
                             seconds=time.monotonic() - started)


# Drive result uploads run here, in the background (see _process_one).
_UPLOADS = ThreadPoolExecutor(max_workers=max(1, SPARK_DRIVE_UPLOAD_WORKERS), thread_name_prefix="drive-upload")


def _upload_result(event, status, error, processed_image):
    """Sends one photo's result (or failure) to Drive. Never raises.
    Returns the photo's status (for the batch line in the console)."""
    started = time.monotonic()
    file_id = publish_result_to_drive_safely(event, status, error, processed_image=processed_image)
    if is_drive_path(event.get("image_path") or ""):
        live_log.result_delivered(event, file_id is not None, time.monotonic() - started)
    return status


def _process_one(event: dict):
    """One photo: inference and database here; its Drive upload is handed
    to the background upload pool, so this thread can start the next photo
    instead of waiting for Google. Returns that upload's Future (None when
    the event was skipped). Never raises — a failure is recorded as FAILED
    for this photo only."""
    if event.get("run_id"):
        # A scalability-test event. The test has its own topic and worker
        # (scalability/worker.py); one still in this topic is from before
        # that, and is never processed as a farmer photo.
        print(f"[streaming_job] {event.get('image_id')} is a scalability-test event "
              f"(run {event.get('run_id')}), skipping")
        return None
    # Set by parse_events' cleaning stage; the raw record only for a rejected event.
    rejected_reason = event.pop("rejected_reason", None)
    raw_value = event.pop("raw_value", None)
    if rejected_reason and not event.get("image_id"):
        event["raw_value"] = raw_value       # rejected_events.raw_event keeps what Kafka carried
    if (event.get("kind") == DRIVE_UPLOAD
            and all(event.get(f) for f in ("image_id", "image_path", "owner_email"))):
        # A new photo from Drive: farmer, upload checks, duplicate skip and its
        # first database row (storage/uploads.py). A refused upload gets its
        # row and fails below with the reason, like any invalid event — also
        # one parse_events refused for its content (captured_at in the
        # future): without a row it would stay in Pending Review and
        # requeue_stuck --from-drive forever. Only an event missing what
        # registration needs ends in rejected_events alone.
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
    state = replay_state(event.get("image_id"))      # one lookup instead of three
    if state == "completed":
        print(f"[streaming_job] {event.get('image_id')} already COMPLETED, skipping replayed event")
        return None
    if state == "result_missing":
        # Saved, but its result never reached Drive (Spark stopped mid-upload):
        # do it again — saving the prediction again is idempotent.
        print(f"[streaming_job] {event.get('image_id')} COMPLETED but not in Drive yet, processing again")
    # Genuinely Spark-side timestamps, captured here (not at persist
    # time, after inference already ran) — kafka_received_at is when
    # this row actually reached the foreachBatch sink; spark_started_at
    # is right before per-row processing begins.
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
    """Load the CNN (onto the GPU when there is one, ml/inference.py) and
    run one dummy photo through it before the stream starts, so the first
    farmer doesn't pay for CUDA start-up. Also makes sure torch is imported
    before anything pulls in pandas/pyarrow: on Windows torch's DLLs fail to
    load after pyarrow's (WinError 1114, see dashboard/model_ops.py)."""
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
    """Background thread: every SPARK_AUTO_RESEND_SECONDS, send the photos
    still owed a result through the pipeline again (storage/requeue_stuck.
    resend_owed) — a lost Kafka event, a photo left PROCESSING, a result
    that didn't reach Drive — so nobody has to run requeue_stuck by hand."""
    from storage.requeue_stuck import resend_owed
    while True:
        time.sleep(SPARK_AUTO_RESEND_SECONDS)
        try:
            if sent := resend_owed(SPARK_AUTO_RESEND_AFTER_SECONDS, SPARK_AUTO_RESEND_MAX):
                live_log.say(f"[resend] {sent} photo(s) still owed a result sent through the pipeline again")
        except Exception as e:  # noqa: BLE001 - database or Kafka hiccup: next round
            print(f"[streaming_job] Automatic re-send failed, trying again later: {e}")


def run():
    # Make sure the checkpoint root exists before the query starts — the
    # metadata subdirs (offsets/, commits/, sources/) are created lazily by
    # Spark, and on Windows that lazy creation is exactly what was failing.
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)
    init_db()   # adds any new columns (e.g. the EXIF metadata ones) before the first write
    _warm_up_model()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        # thousands of uploads at once -> many small committed batches
        # instead of one huge one (config/settings.py)
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