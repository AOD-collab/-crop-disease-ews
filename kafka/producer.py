"""
Publishes real image-upload events to the Kafka topic `maize-disease-images`
(DIT manual section 21).

Uses confluent-kafka (NOT kafka-python) deliberately: this project's own
package is named `kafka/`, which would shadow kafka-python's `kafka` import
on sys.path. confluent-kafka imports as `confluent_kafka`, so there's no
collision.

Falls back to a local JSON-lines file queue ONLY when a broker genuinely
isn't reachable — this is an infrastructure fallback (so the pipeline is
runnable without installing a Kafka cluster) not a data simulation; the
events themselves are always real upload events, never fabricated.
"""

import json
import os
import threading
import time
import uuid

from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_USE_LOCAL_FALLBACK, LOCAL_QUEUE_DIR

LOCAL_QUEUE_DIR.mkdir(parents=True, exist_ok=True)

# One append at a time within this process: a publisher may send from
# several threads, and two threads appending to the queue file at once could
# interleave their lines.
_APPEND_LOCK = threading.Lock()


def _parse_lines(lines):
    """(messages, bad_lines) of a JSON-lines queue. A line that isn't valid
    JSON (a process killed half-way through an append) is returned apart
    instead of making the whole file unreadable."""
    messages, bad = [], []
    for line in lines:
        if not line.strip():
            continue
        try:
            messages.append(json.loads(line))
        except ValueError:
            bad.append(line)
    return messages, bad


class KafkaBackend:
    def __init__(self, topic: str):
        from confluent_kafka import Producer
        self.topic = topic
        self._producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        # Fail fast if the broker is unreachable rather than silently
        # queuing forever — lets get_backend() fall back cleanly.
        self._producer.list_topics(timeout=3)

    def send(self, message: dict):
        # produce() only *queues* the record; delivery success/failure is
        # reported asynchronously via this callback, NOT by an exception.
        # Without checking it, a broker that goes down after get_backend()
        # succeeded makes send() return cleanly while the event is never
        # delivered — which is exactly how uploads end up marked INGESTED
        # but never processed. Surface it so the caller
        # (storage/pipeline_submit.py) can tell the image wasn't queued.
        from confluent_kafka import KafkaException
        errors = []

        def _on_delivery(err, _msg):
            if err is not None:
                errors.append(err)

        self._producer.produce(
            self.topic,
            json.dumps(message).encode("utf-8"),
            callback=_on_delivery,
        )
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            raise KafkaException(
                f"Kafka delivery not acknowledged within 10s "
                f"({remaining} message(s) still queued) — broker unreachable?"
            )
        if errors:
            raise KafkaException(f"Kafka delivery failed: {errors[0]}")

    def send_many(self, messages: list, timeout: float = 120) -> dict:
        """Publishes many events with one flush at the end instead of one
        per event (scalability/run.py). Returns {index: error text} for the
        events Kafka did not acknowledge; an empty dict means all delivered."""
        failed = {}
        acked = set()

        def _callback(index):
            def _on_delivery(err, _msg):
                if err is not None:
                    failed[index] = str(err)
                else:
                    acked.add(index)
            return _on_delivery

        for index, message in enumerate(messages):
            payload = json.dumps(message).encode("utf-8")
            while True:
                try:
                    self._producer.produce(self.topic, payload, callback=_callback(index))
                    break
                except BufferError:          # local send queue full: let it drain
                    self._producer.poll(0.5)
            self._producer.poll(0)
        self._producer.flush(timeout=timeout)
        for index in range(len(messages)):
            if index not in acked and index not in failed:
                failed[index] = f"not acknowledged within {timeout:.0f}s"
        return failed


class LocalFileBackend:
    """Infrastructure fallback for environments without a Kafka broker
    installed: an append-only JSON-lines file that stands in for the topic
    so uploads are still durably recorded; ResilientBackend drains it into
    Kafka once the broker is back."""

    def __init__(self, topic: str):
        self.path = LOCAL_QUEUE_DIR / f"{topic}.jsonl"

    def send(self, message: dict):
        line = json.dumps(message) + "\n"
        with _APPEND_LOCK, open(self.path, "a") as f:
            f.write(line)


class ResilientBackend:
    """Kafka when the broker is reachable, the local file queue when it is
    not — decided on EVERY send, not once at process start.

    Long-running publishers (kafka/drive_watcher, the dashboard)
    used to call get_backend() once at startup. If Kafka happened to be down
    at that moment they stayed on LocalFileBackend for their whole lifetime:
    every later upload was appended to storage/local_queue/*.jsonl and marked
    INGESTED, but Spark only reads Kafka, so those images were never
    analysed. This class keeps retrying the broker (at most every
    RETRY_SECONDS) and, once it is back, drains the file backlog into Kafka
    before sending anything new, so fallback events are delayed, not lost."""

    RETRY_SECONDS = 30

    def __init__(self, topic: str):
        self.topic = topic
        self._local = LocalFileBackend(topic)
        self._kafka = None
        self._next_attempt = 0.0
        self._connect()

    @property
    def active_name(self) -> str:
        return "KafkaBackend" if self._kafka is not None else "LocalFileBackend"

    def _connect(self):
        if self._kafka is not None:
            return self._kafka
        if time.monotonic() < self._next_attempt:
            return None
        try:
            self._kafka = KafkaBackend(self.topic)
            print(f"[kafka.producer] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            self._next_attempt = time.monotonic() + self.RETRY_SECONDS
            print(f"[kafka.producer] Kafka unavailable ({e}); using local file-queue fallback "
                  f"(will retry in {self.RETRY_SECONDS}s). Spark does NOT read this queue — start "
                  f"the broker at {KAFKA_BOOTSTRAP_SERVERS} and the backlog is forwarded automatically.")
        return self._kafka

    def _lost_kafka(self, e: Exception):
        print(f"[kafka.producer] Kafka send failed ({e}); falling back to local file queue.")
        self._kafka = None
        self._next_attempt = time.monotonic() + self.RETRY_SECONDS

    def send(self, message: dict):
        kafka = self._connect()
        if kafka is not None:
            self.drain()  # older queued events go first
            # drain() gives up on the broker when it can't deliver; then the
            # new event goes straight to the file instead of waiting out a
            # second delivery timeout on the same dead connection.
            if self._kafka is kafka:
                try:
                    kafka.send(message)
                    return
                except Exception as e:
                    self._lost_kafka(e)
        self._local.send(message)

    # A claimed file still there after this long belongs to a process that
    # died while draining it (its claim time is set when it is claimed).
    ORPHAN_CLAIM_SECONDS = 600

    def _claim_files(self) -> list:
        """Claims the queue file plus any orphaned claim, each by renaming it
        to a fresh .draining-<uuid> name. The rename is the lock: the Drive watcher
        and the dashboard (separate processes sharing the file) never both
        forward the same backlog; a rename that fails means someone else
        holds that file — it is skipped."""
        path = self._local.path
        sources = [path] if path.exists() else []
        cutoff = time.time() - self.ORPHAN_CLAIM_SECONDS
        for old in sorted(path.parent.glob(f"{path.name}.draining-*")):
            try:
                if old.stat().st_mtime < cutoff:
                    sources.append(old)
            except OSError:
                continue
        claimed = []
        for src in sources:
            dst = path.with_name(f"{path.name}.draining-{uuid.uuid4().hex}")
            try:
                os.replace(src, dst)
                os.utime(dst)          # claim time, for the orphan check above
            except OSError:
                continue
            claimed.append(dst)
        return claimed

    def drain(self) -> int:
        """Forward every event sitting in the local file queue to Kafka.
        Returns how many were forwarded. Safe to call often: it is a no-op
        when Kafka is down or the queue file is absent.

        A claimed file is deleted only after its events were delivered or
        put back in the queue, so a crash part-way never loses them (at
        worst some are delivered twice, which Spark tolerates: a COMPLETED
        image is skipped). A line that isn't valid JSON is moved to
        <queue>.corrupt instead of blocking the rest."""
        kafka = self._connect()
        if kafka is None:
            return 0
        claimed = self._claim_files()
        sent = 0
        for i, path in enumerate(claimed):
            try:
                with open(path) as f:
                    messages, bad = _parse_lines(f)
            except OSError as e:
                print(f"[kafka.producer] Could not read {path.name}: {e}")
                continue
            if bad:
                with _APPEND_LOCK, open(self._local.path.with_suffix(".corrupt"), "a") as f:
                    f.writelines(line if line.endswith("\n") else line + "\n" for line in bad)
                print(f"[kafka.producer] {len(bad)} unreadable line(s) in {path.name} moved to "
                      f"{self._local.path.with_suffix('.corrupt').name}")
            done = 0
            try:
                for m in messages:
                    kafka.send(m)
                    done += 1
            except Exception as e:
                # Put the unsent remainder back so nothing is lost; claims not
                # read yet go back as they are.
                for m in messages[done:]:
                    self._local.send(m)
                for rest in claimed[i + 1:]:
                    with open(rest) as f:
                        for m in _parse_lines(f)[0]:
                            self._local.send(m)
                    rest.unlink()
                path.unlink()
                sent += done
                self._lost_kafka(e)
                break
            path.unlink()
            sent += done
        if sent:
            print(f"[kafka.producer] Forwarded {sent} queued event(s) from {self._local.path.name} to Kafka.")
        return sent


def get_backend(topic: str):
    if KAFKA_USE_LOCAL_FALLBACK:
        return ResilientBackend(topic)
    else:
        return KafkaBackend(topic)
