"""
Sends upload events to Kafka (confluent-kafka: kafka-python would clash with
this project's kafka/ package). When the broker is down, events wait in a
local file queue and are forwarded when it is back.
"""

import json
import os
import threading
import time
import uuid

from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_USE_LOCAL_FALLBACK, LOCAL_QUEUE_DIR

LOCAL_QUEUE_DIR.mkdir(parents=True, exist_ok=True)

# one append at a time within this process (several threads may send)
_APPEND_LOCK = threading.Lock()


def _append(path, text: str) -> None:
    """Appends to a queue file under an OS lock, so two processes can't mix their lines."""
    with _APPEND_LOCK, open(path, "a") as f:
        if os.name == "nt":
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)   # waits up to ~10 s, then raises
            f.write(text)
            f.flush()
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)      # released when the file closes
            f.write(text)


def _parse_lines(lines):
    """(messages, bad_lines) of a JSON-lines queue."""
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
        # fail fast when the broker is down
        self._producer.list_topics(timeout=3)

    def send(self, message: dict):
        # raise if Kafka didn't confirm delivery, so the caller knows
        from confluent_kafka import KafkaException
        if failed := self.send_many([message], timeout=10):
            raise KafkaException(f"Kafka delivery failed: {failed[0]} — broker unreachable?")

    def send_many(self, messages: list, timeout: float = 120) -> dict:
        """Sends many events with one flush. Returns {index: error} for those not delivered."""
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
                except BufferError:          # send buffer full: let it drain
                    self._producer.poll(0.5)
            self._producer.poll(0)
        self._producer.flush(timeout=timeout)
        for index in range(len(messages)):
            if index not in acked and index not in failed:
                failed[index] = f"not acknowledged within {timeout:.0f}s"
        return failed


class LocalFileBackend:
    """The local file queue used while Kafka is down."""

    def __init__(self, topic: str):
        self.path = LOCAL_QUEUE_DIR / f"{topic}.jsonl"

    def send(self, message: dict):
        _append(self.path, json.dumps(message) + "\n")


class ResilientBackend:
    """Kafka when it is up, the local file queue when it is down (checked on
    every send). Once Kafka is back the queue is forwarded first."""

    RETRY_SECONDS = 30

    def __init__(self, topic: str):
        self.topic = topic
        self._local = LocalFileBackend(topic)
        self._kafka = None
        self._next_attempt = 0.0
        self._connect()

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
            if self._kafka is kafka:
                try:
                    kafka.send(message)
                    return
                except Exception as e:
                    self._lost_kafka(e)
        self._local.send(message)

    # a claimed file older than this was left by a process that died
    ORPHAN_CLAIM_SECONDS = 600

    def _claim_files(self) -> list:
        """Claims the queue file (and orphaned claims) by renaming it; the
        rename is the lock between processes."""
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
                os.utime(dst)          # claim time
            except OSError:
                continue
            claimed.append(dst)
        return claimed

    def drain(self) -> int:
        """Forwards the local queue to Kafka; returns how many were sent.
        Unreadable lines go to <queue>.corrupt."""
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
                _append(self._local.path.with_suffix(".corrupt"),
                        "".join(line if line.endswith("\n") else line + "\n" for line in bad))
                print(f"[kafka.producer] {len(bad)} unreadable line(s) in {path.name} moved to "
                      f"{self._local.path.with_suffix('.corrupt').name}")
            done = 0
            try:
                for m in messages:
                    kafka.send(m)
                    done += 1
            except Exception as e:
                # put everything not sent back in the queue
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
    return (ResilientBackend if KAFKA_USE_LOCAL_FALLBACK else KafkaBackend)(topic)
