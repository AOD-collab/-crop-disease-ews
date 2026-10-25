"""
Apache Spark Structured Streaming job — the main streaming-processing
engine (DIT manual section 22).

Pipeline:
    Kafka (maize-disease-images)
      -> Read Stream
      -> Parse JSON
      -> Register a new upload: farmer, upload checks, duplicate skip,
         first database row         (spark/ingestion.register_upload)
      -> Validate / Data Cleaning   (spark/preprocessing.clean_event)
      -> Retrieve Image             (Google Drive, or the local copy)
      -> Metadata Extraction: EXIF GPS location, capture time, camera
                                     (spark/preprocessing.extract_metadata)
      -> Image Preprocessing: upright, resize, centre crop to the model's
         size, normalise — the CNN only classifies
                                     (spark/preprocessing.model_input)
      -> CNN Inference (ml/inference.predict) + Risk Assessment
                                     (spark/preprocessing.process_event)
      -> Write predictions + update image status to PostgreSQL
      -> Result to the farmer's Google Drive folder

Checkpointing: this job writes to CHECKPOINT_DIR (below) so a restart
resumes from the last committed Kafka offset instead of silently skipping
whatever was uploaded while it was down. One honest caveat that comes with
that: Structured Streaming's checkpoint gives *at-least-once* delivery to
foreachBatch, not exactly-once — if Spark crashes mid-batch and replays it
after restart, the same event can reach spark/preprocessing.persist_result()
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
from functools import partial
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
    process_event, persist_result, mark_processing, InvalidEvent, publish_result_to_drive_safely,
    replay_state, resend_duplicates_of,
)
from storage.database import ProcessingStatus, init_db
from storage.image_storage import is_drive_path
from spark import live_log
from spark.ingestion import register_upload

CHECKPOINT_DIR = str(SPARK_CHECKPOINT_ROOT / KAFKA_TOPIC)

EVENT_SCHEMA = StructType([
    # "drive_upload": a new photo, from its Drive metadata (kafka/drive_watcher.
    # build_drive_event) — spark/ingestion registers it. "submit" (or no kind,
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
        # first database row (spark/ingestion.py). A refused upload gets its
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
    save = partial(persist_result, event, kafka_received_at=kafka_received_at, spark_started_at=spark_started_at)
    result, status, error = None, ProcessingStatus.FAILED, None
    try:
        if rejected_reason:
            raise InvalidEvent(rejected_reason)
        result = process_event(event)
        save(result, ProcessingStatus.COMPLETED)
        status = ProcessingStatus.COMPLETED
    except Exception as e:                     # an InvalidEvent too, only reported differently
        print(f"[streaming_job] Invalid event, skipping: {e}" if isinstance(e, InvalidEvent)
              else f"[streaming_job] Processing failed for {event.get('image_id')}: {e}")
        error = str(e)
        try:
            save(None, ProcessingStatus.FAILED, error_message=error)
        except Exception as db_error:  # noqa: BLE001 - don't take the other photos (or the job) down with it
            # The photo stays PROCESSING; storage/requeue_stuck.py sends it again.
            print(f"[streaming_job] Could not record the failure for {image_id}: {db_error}")

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