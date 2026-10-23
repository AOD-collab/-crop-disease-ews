"""
Apache Spark Structured Streaming job — the main streaming-processing
engine (DIT manual section 22).

Pipeline:
    Kafka (maize-disease-images)
      -> Read Stream
      -> Parse JSON
      -> Validate / Data Cleaning   (spark/preprocessing.clean_event)
      -> Retrieve Image
      -> Metadata Extraction: EXIF GPS location, capture time, camera
                                     (spark/preprocessing.extract_metadata)
      -> Image Cleaning: drop non-maize leaves, remove background
                                     (spark/preprocessing.clean_image)
      -> CNN Inference + Risk Assessment
                                     (spark/preprocessing.process_event)
      -> Write predictions + update image status to PostgreSQL

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
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

# Windows-only workaround (winutils.exe location for Hadoop's local
# filesystem shim) — only set if not already configured, and never on a
# non-Windows deployment box, where C:\hadoop doesn't exist and shouldn't be
# assumed.
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    _hadoop_bin = r"C:\hadoop\bin"
    if _hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] += os.pathsep + _hadoop_bin

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, initcap, trim, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from config.settings import (
    SPARK_CHECKPOINT_ROOT, SPARK_MAX_EVENTS_PER_BATCH, SPARK_TRIGGER_SECONDS, SPARK_PARALLEL_PHOTOS,
    SPARK_DRIVE_UPLOAD_WORKERS,
)
from spark.preprocessing import (
    process_event, persist_result, mark_processing, InvalidEvent, publish_result_to_drive_safely,
    replay_state,
)
from storage.database import ProcessingStatus, init_db
from storage.image_storage import is_drive_path
from spark import live_log

CHECKPOINT_DIR = str(SPARK_CHECKPOINT_ROOT / KAFKA_TOPIC)

EVENT_SCHEMA = StructType([
    StructField("image_id", StringType()),
    StructField("user_id", StringType()),
    StructField("location", StringType()),
    StructField("image_path", StringType()),
    StructField("drive_file_id", StringType()),   # Google Drive file id (None for legacy local images)
    StructField("captured_at", StringType()),
    StructField("uploaded_at", StringType()),
    # Only on scalability-test events published here before the test got its
    # own topic (2026-10-10); _process_one skips them. Null for every upload.
    StructField("run_id", StringType()),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("ImageSystem-CropDiseaseEWS")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        # Spark 3.x defaults to FileContextBasedCheckpointFileManager, whose
        # atomic temp-file + rename against a Windows *local* filesystem
        # intermittently fails with
        #   FileNotFoundException: .../<checkpoint>/offsets does not exist
        # and terminates the streaming query (STREAM_FAILED). The older
        # FileSystem-based manager uses FileSystem.create + rename, which
        # works correctly on Windows local disk. Harmless on Linux/HDFS.
        .config("spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
        .getOrCreate()
    )


def parse_events(raw):
    """Kafka record value (JSON bytes) -> one typed row per upload event,
    then a genuine distributed-DataFrame cleaning stage (DIT manual 5.4):
    drop rows missing a required field, parse/validate uploaded_at and
    captured_at as real timestamps, normalize location casing/whitespace,
    and reject events with a future captured_at. This runs as real Spark
    SQL transformations on
    the streaming DataFrame — NOT per-row Python — before foreachBatch's
    per-row CNN inference / persistence work (which can't be expressed as a
    DataFrame transformation) ever sees a row.

    spark/preprocessing.clean_event() enforces the same rules again,
    per-row, so a direct process_event() call (unit tests, or the
    MANUAL-mode admin release path, which doesn't go through this
    DataFrame) can't skip validation.

    Duplicate image_ids are NOT dropped here any more: a streaming
    dropDuplicates without a watermark keeps every image_id in state
    forever, and silently dropped a Reprocess of an image Kafka had seen
    before (the image then stayed INGESTED for good). foreach_batch_sink
    dedupes each micro-batch and skips images that are already COMPLETED.

    Extracted from run() so the exact projection the streaming query uses
    can be exercised without a Kafka broker."""
    parsed = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    cleaned = (
        parsed
        .filter(
            col("image_id").isNotNull() & (col("image_id") != "")
            & col("user_id").isNotNull() & (col("user_id") != "")
            & col("image_path").isNotNull() & (col("image_path") != "")
            & col("uploaded_at").isNotNull() & (col("uploaded_at") != "")
        )
        .withColumn("location", initcap(trim(col("location"))))
        .withColumn("_captured_at_ts", to_timestamp(col("captured_at")))
        .filter(col("_captured_at_ts").isNull() | (col("_captured_at_ts") <= current_timestamp()))
        .drop("_captured_at_ts")
    )
    return cleaned


def foreach_batch_sink(batch_df, batch_id):
    # DataFrame.isEmpty() (Spark 3.3+) is a JVM-only check. batch_df.rdd
    # would force an RDD conversion that spawns an executor-side Python
    # worker — unnecessary here (all per-event work below runs in the
    # driver) and a frequent source of "Python worker crashed" failures
    # on Windows.
    if batch_df.isEmpty():
        return
    # Stateless dedupe of THIS micro-batch only (see parse_events).
    events = [row.asDict() for row in batch_df.dropDuplicates(["image_id"]).collect()]
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
    state = replay_state(event.get("image_id"))      # one lookup instead of three
    if state == "held":
        # MANUAL mode holds this photo; an admin's Process sets it INGESTED
        # before publishing, so this is an old event (a replay) — skip it.
        print(f"[streaming_job] {event.get('image_id')} is held for manual review, skipping old event")
        return None
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
    result, status, error = None, ProcessingStatus.FAILED, None
    try:
        result = process_event(event)
        persist_result(
            event, result, ProcessingStatus.COMPLETED,
            kafka_received_at=kafka_received_at, spark_started_at=spark_started_at,
        )
        status = ProcessingStatus.COMPLETED
    except InvalidEvent as e:
        print(f"[streaming_job] Invalid event, skipping: {e}")
        error = str(e)
        persist_result(
            event, None, ProcessingStatus.FAILED, error_message=str(e),
            kafka_received_at=kafka_received_at, spark_started_at=spark_started_at,
        )
    except Exception as e:
        print(f"[streaming_job] Processing failed for {event.get('image_id')}: {e}")
        error = str(e)
        try:
            persist_result(
                event, None, ProcessingStatus.FAILED, error_message=str(e),
                kafka_received_at=kafka_received_at, spark_started_at=spark_started_at,
            )
        except Exception as db_error:  # noqa: BLE001 - don't take the other photos down with it
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
    buf = BytesIO()
    PILImage.new("RGB", (256, 256), (60, 140, 50)).save(buf, format="JPEG")
    inference.predict(buf.getvalue())
    inference.disease_heatmap(buf.getvalue())


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
    query.awaitTermination()


if __name__ == "__main__":
    run()