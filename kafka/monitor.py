"""
The EWS-KafkaMonitor window: one line per farmer-photo event, and a summary
per second for the scalability-test topic. Read-only: never commits offsets.

    python -m kafka.monitor [--from-start] [--live-only]
"""

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime

from config.settings import SCALABILITY_KAFKA_TOPIC, SCALABILITY_CONSUMER_GROUP, SCALABILITY_ENGINE
from config.localtime import format_local
from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, broker_reachable

GROUP_ID = "ews-kafka-monitor"          # never used by Spark or the scalability test
TEST_SUMMARY_EVERY_S = 1.0


def _say(line: str) -> None:
    print(f"[kafka {datetime.now().strftime('%H:%M:%S')}] {line}", flush=True)


def describe(value: bytes, partition: int, offset: int) -> str:
    """One console line for one Kafka record of the farmer-photo topic."""
    try:
        event = json.loads(value)
    except (TypeError, ValueError):
        return f"p{partition} offset {offset}: unreadable message ({len(value or b'')} bytes)"
    from spark.live_log import farmer_label
    uploaded = format_local(event.get("uploaded_at")) or event.get("uploaded_at") or ""
    source = "Drive file" if event.get("drive_file_id") else "local file"
    farmer = event.get("owner_email") or farmer_label(event.get("user_id"))
    kind = "new photo" if event.get("kind") == "drive_upload" else "photo sent again"
    return (f"p{partition} offset {offset}: {kind} {(event.get('image_id') or '?')[:8]} "
            f"from {farmer} ({event.get('location') or 'no location'}), "
            f"uploaded {uploaded}, {source} -> waiting for Spark")


class TestTopicSummary:
    """Collects the scalability topic's events between two summary lines."""

    def __init__(self):
        self.runs = Counter()           # run_id -> events since the last line
        self.first = self.last = None   # offset range since the last line
        self.total = 0
        self.end_offset = None          # newest offset seen (+1), for the read position

    def add(self, value: bytes, offset: int) -> None:
        try:
            run_id = json.loads(value).get("run_id") or "?"
        except (TypeError, ValueError):
            run_id = "unreadable"
        self.runs[run_id] += 1
        self.first = offset if self.first is None else min(self.first, offset)
        self.last = offset if self.last is None else max(self.last, offset)
        self.total += 1
        self.end_offset = max(self.end_offset or 0, offset + 1)

    def line(self, consumed_to=None, engine: str = SCALABILITY_ENGINE) -> str | None:
        """The summary since the last call, or None if nothing arrived."""
        if not self.runs:
            return None
        runs = ", ".join(f"{n} for {run}" for run, n in self.runs.items())
        text = (f"TEST topic '{SCALABILITY_KAFKA_TOPIC}': +{sum(self.runs.values())} event(s) "
                f"(offsets {self.first}-{self.last}; {runs}); {self.total} since start")
        if engine == "spark":
            text += " — read by the scalability test's own Spark query, not the live Spark job"
        else:
            if consumed_to is not None and self.end_offset is not None:
                text += (f"; test worker committed up to offset {consumed_to} "
                         f"({max(0, self.end_offset - consumed_to)} behind; it commits every few seconds)")
            text += " — processed by the scalability test's worker, not Spark"
        self.runs.clear()
        self.first = self.last = None
        return text


def test_consumer_position(admin) -> int | None:
    """The test consumer's committed offset (summed), or None."""
    try:
        from confluent_kafka import ConsumerGroupTopicPartitions
        futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(SCALABILITY_CONSUMER_GROUP)])
        result = futures[SCALABILITY_CONSUMER_GROUP].result(timeout=2)
        offsets = [tp.offset for tp in result.topic_partitions
                   if tp.topic == SCALABILITY_KAFKA_TOPIC and tp.offset >= 0]
        return sum(offsets) if offsets else None
    except Exception:  # noqa: BLE001 - display only
        return None


class Assigner:
    """Reads the topics by direct partition assignment (no group, no rebalance).
    A topic created later is read from its beginning."""

    def __init__(self, consumer, topics, from_start: bool):
        self.consumer, self.topics, self.from_start = consumer, topics, from_start
        self.assigned = set()
        self.first_check = True

    def refresh(self) -> list:
        """Assigns partitions that appeared since the last call; returns them."""
        from confluent_kafka import TopicPartition, OFFSET_BEGINNING, OFFSET_END
        try:
            meta = self.consumer.list_topics(timeout=2)
        except Exception:  # noqa: BLE001 - try again next time
            return []
        new = []
        for topic in self.topics:
            info = meta.topics.get(topic)
            if info is None or info.error is not None:
                continue
            start = OFFSET_BEGINNING if (self.from_start or not self.first_check) else OFFSET_END
            for partition in info.partitions:
                if (topic, partition) not in self.assigned:
                    self.assigned.add((topic, partition))
                    new.append(TopicPartition(topic, partition, start))
        self.first_check = False
        if new:
            self.consumer.incremental_assign(new)
        return new

    def missing(self) -> list:
        return [t for t in self.topics if not any(a[0] == t for a in self.assigned)]


def consumer_config(from_start: bool) -> dict:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if from_start else "latest",
        # notice a new topic within seconds
        "topic.metadata.refresh.interval.ms": 5000,
    }


def run(from_start: bool = False, live_only: bool = False) -> int:
    from confluent_kafka import Consumer, KafkaError
    from confluent_kafka.admin import AdminClient
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.kernel32.SetConsoleTitleW("EWS-KafkaMonitor")
    while not broker_reachable():
        _say(f"Kafka broker not reachable at {KAFKA_BOOTSTRAP_SERVERS}; retrying in 5 s")
        time.sleep(5)
    topics = [KAFKA_TOPIC] + ([] if live_only else [SCALABILITY_KAFKA_TOPIC])
    consumer = Consumer(consumer_config(from_start))
    assigner = Assigner(consumer, topics, from_start)
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    _say(f"Watching '{KAFKA_TOPIC}' (farmer photos -> Spark)"
         + ("" if live_only else f" and '{SCALABILITY_KAFKA_TOPIC}' (scalability test -> its own worker)")
         + f" on {KAFKA_BOOTSTRAP_SERVERS}. Read-only: no consumer group, no offsets committed.")
    assigner.refresh()
    if assigner.missing():
        _say(f"(not created yet: {', '.join(assigner.missing())} — picked up as soon as it exists)")
    seen = 0
    tests = TestTopicSummary()
    last_quiet = last_summary = last_refresh = time.monotonic()
    try:
        while True:
            msg = consumer.poll(0.25)
            now = time.monotonic()
            if now - last_refresh >= 3 and assigner.missing():
                last_refresh = now
                for tp in assigner.refresh():
                    _say(f"now watching '{tp.topic}' partition {tp.partition} (it was just created)")
            if now - last_summary >= TEST_SUMMARY_EVERY_S:
                last_summary = now
                if tests.runs:
                    _say(tests.line(test_consumer_position(admin)))
            if msg is None:
                if now - last_quiet > 60:
                    _say(f"… no new events in the last minute ({seen} farmer, {tests.total} test seen since start)")
                    last_quiet = now
                continue
            if msg.error():
                code = msg.error().code()
                if code not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    _say(f"Kafka error: {msg.error()}")
                continue
            last_quiet = now
            if msg.topic() == SCALABILITY_KAFKA_TOPIC:
                tests.add(msg.value(), msg.offset())
                continue
            seen += 1
            _say(describe(msg.value(), msg.partition(), msg.offset()))
    except KeyboardInterrupt:
        return 0
    finally:
        consumer.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from-start", action="store_true", help="also show the events already in the topics")
    p.add_argument("--live-only", action="store_true", help="only the real-time topic, not the scalability test's")
    args = p.parse_args()
    for stream in (sys.stdout, sys.stderr):        # "—" etc. in any console or redirected log
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    return run(args.from_start, args.live_only)


if __name__ == "__main__":
    sys.exit(main())
