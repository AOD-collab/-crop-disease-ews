"""
The scalability test's own Apache Spark Structured Streaming job.

    Kafka (the TEST topic, SCALABILITY_KAFKA_TOPIC)
      -> Spark readStream (maxOffsetsPerTrigger = SPARK_MAX_EVENTS_PER_BATCH,
                           trigger = SPARK_TRIGGER_SECONDS, as the live job)
      -> Parse JSON + keep only this run's events   (Spark SQL, parse_test_events)
      -> foreachBatch: dedupe the micro-batch, then preprocessing + CNN for each
         photo, SCALABILITY_WORKERS at a time (scalability/worker.process_photo,
         the live pipeline's own functions); the batch commits once every
         photo in it is done, like spark/streaming_job.foreach_batch_sink

It runs inside the test runner's process (scalability/run.py), in its own
SparkSession with its own checkpoint folder per run, and is a SEPARATE query
from the live job: it never reads the production topic, and it never writes
images, predictions or Drive results. spark/streaming_job.py is not imported
or changed by this module.

Started and warmed up (JVM, Kafka connection, a first micro-batch carrying
one warm-up event) BEFORE the test's timer starts, the same way the CNN is
warmed up: start-up is not image processing. Every later micro-batch's own
overhead (Kafka read, parse, dedupe, commit) is inside the timer.
"""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# Same Windows-only winutils workaround as spark/streaming_job.py.
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    _hadoop_bin = r"C:\hadoop\bin"
    if _hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] += os.pathsep + _hadoop_bin

from config import settings  # noqa: E402
from storage.database import now_utc  # noqa: E402

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"     # same as the live job
START_TIMEOUT_S = 180
WARM_UP_ID = "__spark-warm-up__"     # never a staged item id (those are uuids)


def event_schema():
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType([StructField("run_id", StringType()), StructField("item_id", StringType())])


def build_spark():
    """The test's SparkSession: the live job's settings (Kafka package,
    Windows-safe checkpoint manager), its own application name."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("ImageSystem-ScalabilityTest")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_test_events(raw, run_id: str):
    """Kafka record value (JSON) -> one row per test photo of THIS run, as
    Spark SQL transformations: events of other runs and rows without an
    item_id are dropped here, before foreachBatch sees them."""
    from pyspark.sql.functions import col, from_json, lit
    return (
        raw.select(from_json(col("value").cast("string"), event_schema()).alias("e"))
        .select("e.*")
        .filter((col("run_id") == lit(run_id)) & col("item_id").isNotNull() & (col("item_id") != ""))
    )


def topic_end_offsets(topic: str) -> str:
    """startingOffsets JSON = the topic's current end, read before anything
    of this run is published: the query reads exactly this run's events and
    never earlier runs' leftovers."""
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                  "group.id": f"{settings.SCALABILITY_CONSUMER_GROUP}-spark-offsets",
                  "enable.auto.commit": False})
    try:
        meta = c.list_topics(topic, timeout=10).topics.get(topic)
        if meta is None or meta.error is not None or not meta.partitions:
            raise RuntimeError(f"Kafka topic {topic} not found")
        ends = {str(p): c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in meta.partitions}
    finally:
        c.close()
    return json.dumps({topic: ends})


class SparkTestStream:
    """One run's streaming query. `handle(item_id, received_at)` processes
    one photo and returns (item_id, outcome) without raising; `on_done` gets
    that tuple. The runner's loop reads newly received ids with
    take_received() and calls raise_if_failed() so a dead query is not
    mistaken for a slow one."""

    def __init__(self, run_id: str, item_ids, handle, on_done, checkpoint_dir, workers: int = None):
        self.run_id = run_id
        self._wanted = set(item_ids)
        self._handle, self._on_done = handle, on_done
        self._workers = max(1, workers or settings.SCALABILITY_WORKERS)
        self.checkpoint_dir = checkpoint_dir
        self._lock = threading.Lock()
        self._seen = set()
        self._new = []
        self.batches = 0
        self.query = None

    # ---------------------------------------------------------- foreachBatch
    def sink(self, batch_df, batch_id):
        if batch_df.isEmpty():                 # JVM-only check, as the live job
            return
        rows = batch_df.dropDuplicates(["item_id"]).collect()
        with self._lock:
            ids = [r.item_id for r in rows if r.item_id in self._wanted and r.item_id not in self._seen]
            self._seen.update(ids)
            self._new.extend(ids)
            self.batches += 1
        if not ids:
            return
        received_at = now_utc()

        def one(item_id):
            self._on_done(self._handle(item_id, received_at))

        # the micro-batch (and its Kafka offsets) commits only after every
        # photo in it is processed, as in the live job
        with ThreadPoolExecutor(max_workers=min(self._workers, len(ids)), thread_name_prefix="spark-test") as pool:
            list(pool.map(one, ids))

    # ---------------------------------------------------------- lifecycle
    def start(self, topic: str = None):
        topic = topic or settings.SCALABILITY_KAFKA_TOPIC
        if topic == settings.KAFKA_TOPIC:
            raise ValueError("the scalability test must not read the production Kafka topic")
        spark = build_spark()
        raw = self._read_stream(spark, topic)
        self.query = (
            parse_test_events(raw, self.run_id).writeStream
            .queryName(f"scalability_{self.run_id.replace('-', '_')}")
            .foreachBatch(self.sink)
            .outputMode("append")
            .trigger(processingTime=f"{settings.SPARK_TRIGGER_SECONDS} seconds")
            .option("checkpointLocation", str(self.checkpoint_dir))
            .start()
        )
        self._wait_ready()
        return self

    def _read_stream(self, spark, topic: str):
        """The Kafka source (tests replace it with a file source)."""
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", topic_end_offsets(topic))
            .option("maxOffsetsPerTrigger", settings.SPARK_MAX_EVENTS_PER_BATCH)
            .load()
        )

    def _wait_ready(self):
        """Blocks until the query has finished its first (empty) trigger, so
        the timer never includes Spark or Kafka start-up."""
        deadline = time.monotonic() + START_TIMEOUT_S
        while time.monotonic() < deadline:
            self.raise_if_failed()
            status = self.query.status
            if self.query.lastProgress is not None or (
                    not status.get("isTriggerActive") and str(status.get("message", "")).startswith("Waiting for")):
                return
            time.sleep(0.2)
        raise TimeoutError(f"the scalability Spark query was not ready within {START_TIMEOUT_S} s")

    def warm_up(self, send) -> None:
        """Sends one event of this run that is not a photo (WARM_UP_ID) and
        waits until a micro-batch has read it: Spark's first job on real data
        (the Kafka reader, code generation for the parse/filter/dedupe plan)
        takes several seconds once, and that is start-up, not image
        processing. The sink sees the event and skips it (not a staged id)."""
        before = self.batches
        failures = send([{"run_id": self.run_id, "item_id": WARM_UP_ID}])
        if failures:
            raise RuntimeError(f"could not send the Spark warm-up event: {failures}")
        deadline = time.monotonic() + START_TIMEOUT_S
        while self.batches == before:
            self.raise_if_failed()
            if time.monotonic() > deadline:
                raise TimeoutError(f"the scalability Spark query did not read its warm-up event within "
                                   f"{START_TIMEOUT_S} s")
            time.sleep(0.05)

    def take_received(self) -> list:
        with self._lock:
            out, self._new = self._new, []
        return out

    def raise_if_failed(self) -> None:
        if self.query is not None and not self.query.isActive:
            err = self.query.exception()
            raise RuntimeError(f"the scalability Spark query stopped: {err}" if err else
                               "the scalability Spark query stopped")

    def stop(self) -> None:
        if self.query is not None:
            try:
                self.query.stop()
            except Exception as e:  # noqa: BLE001
                print(f"[scalability] could not stop the Spark query: {e}", flush=True)
            self.query = None
