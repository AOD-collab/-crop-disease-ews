"""
Watch the real-time Kafka topic live (the EWS-KafkaMonitor window that
run_all.py opens): one line per farmer-photo event as it arrives on
KAFKA_TOPIC, with partition/offset, farmer, place and upload time.

READ-ONLY and harmless to the pipeline: its own consumer group, it never
commits offsets, and it starts at the newest message. Spark does not use
consumer-group offsets at all — it tracks its own position in its checkpoint
(spark/streaming_job.py) — so this window can't move, slow down or take away
any event Spark processes.

    python -m kafka.monitor              # new events from now on
    python -m kafka.monitor --from-start # also everything still in the topic
"""

import argparse
import json
import sys
import time
from datetime import datetime

from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, broker_reachable

GROUP_ID = "ews-kafka-monitor"          # never used by Spark or the scalability test


def _say(line: str) -> None:
    print(f"[kafka {datetime.now().strftime('%H:%M:%S')}] {line}", flush=True)


def describe(value: bytes, partition: int, offset: int) -> str:
    """One console line for one Kafka record of the farmer-photo topic."""
    try:
        event = json.loads(value)
    except (TypeError, ValueError):
        return f"p{partition} offset {offset}: unreadable message ({len(value or b'')} bytes)"
    from spark.live_log import farmer_label
    uploaded = event.get("uploaded_at") or ""
    try:
        from config.localtime import format_local
        uploaded = format_local(datetime.fromisoformat(uploaded.replace("Z", "+00:00"))) or uploaded
    except (ValueError, ImportError):
        pass
    source = "Drive file" if event.get("drive_file_id") else "local file"
    return (f"p{partition} offset {offset}: photo {(event.get('image_id') or '?')[:8]} "
            f"from {farmer_label(event.get('user_id'))} ({event.get('location') or 'no location'}), "
            f"uploaded {uploaded}, {source} -> waiting for Spark")


def consumer_config(from_start: bool) -> dict:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # its own group, which never commits a position (so it never inherits one either)
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if from_start else "latest",
    }


def run(from_start: bool = False) -> int:
    from confluent_kafka import Consumer, KafkaError
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.kernel32.SetConsoleTitleW("EWS-KafkaMonitor")
    while not broker_reachable():
        _say(f"Kafka broker not reachable at {KAFKA_BOOTSTRAP_SERVERS}; retrying in 5 s")
        time.sleep(5)
    consumer = Consumer(consumer_config(from_start))
    consumer.subscribe([KAFKA_TOPIC])
    _say(f"Watching topic '{KAFKA_TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS} (read-only, no offsets committed; "
         f"Spark keeps its own position). New farmer photos appear below.")
    seen = 0
    last_quiet = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_quiet > 60:
                    _say(f"… no new events in the last minute ({seen} seen since start)")
                    last_quiet = time.monotonic()
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    _say(f"Kafka error: {msg.error()}")
                continue
            seen += 1
            last_quiet = time.monotonic()
            _say(describe(msg.value(), msg.partition(), msg.offset()))
    except KeyboardInterrupt:
        return 0
    finally:
        consumer.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from-start", action="store_true", help="also show the events already in the topic")
    return run(p.parse_args().from_start)


if __name__ == "__main__":
    sys.exit(main())
