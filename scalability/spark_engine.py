"""
The scalability test's own Spark Structured Streaming query: reads the TEST
topic with the live job's settings, keeps this run's events, and runs
preprocessing + CNN for each photo. Separate from the live job; started and
warmed up before the timer.
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from spark.session import kafka_session    # before pyspark: the Windows Hadoop workaround
from config import settings
from storage.database import now_utc

START_TIMEOUT_S = 180
WARM_UP_ID = "__spark-warm-up__"     # never a staged item id (those are uuids)


def event_schema():
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType([StructField("run_id", StringType()), StructField("item_id", StringType())])


def build_spark():
    """The test's SparkSession (the live job's settings, its own name)."""
    spark = kafka_session("ImageSystem-ScalabilityTest", console_progress=False)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_test_events(raw, run_id: str):
    """Kafka JSON -> one row per photo of THIS run (others dropped)."""
    from pyspark.sql.functions import col, from_json, lit
    return (
        raw.select(from_json(col("value").cast("string"), event_schema()).alias("e"))
        .select("e.*")
        .filter((col("run_id") == lit(run_id)) & col("item_id").isNotNull() & (col("item_id") != ""))
    )


def topic_end_offsets(topic: str) -> str:
    """The topic's current end, so the query reads only this run's events."""
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
    """One run's streaming query; `handle` processes a photo, `on_done` gets the result."""

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

        # the batch commits only after every photo in it is processed
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
        """Waits for the first (empty) trigger, so start-up isn't timed."""
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
        """Sends one warm-up event and waits until Spark has read it (Spark's
        first real batch is slow once; that is start-up, not processing)."""
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
