"""
The scalability test's own processing, isolated from the live pipeline: its
own Kafka topic, the same preprocessing + CNN code, results kept only in
scale_test_items. Farmer photos come first: the test waits while they are
processed and runs at lower OS priority.
"""

import json
import threading
import time
from datetime import timedelta, timezone
from pathlib import Path

from config import settings
from storage.database import SessionLocal, Image, ProcessingStatus, SCALE_TEST_SOURCE, now_utc

LIVE_STATUSES = (ProcessingStatus.INGESTED, ProcessingStatus.PROCESSING)


# --------------------------------------------------------------- priority to real time
def live_processing_active() -> bool:
    """True while a farmer photo is INGESTED or PROCESSING (recently; older ones are stuck)."""
    cutoff = now_utc() - timedelta(minutes=settings.SCALABILITY_LIVE_WINDOW_MINUTES)
    with SessionLocal() as s:
        rows = (s.query(Image.ingested_at, Image.uploaded_at)
                .filter(Image.source != SCALE_TEST_SOURCE, Image.status.in_(LIVE_STATUSES)).all())
    times = (ingested or uploaded for ingested, uploaded in rows)
    # SQLite returns naive UTC
    return any((t if t.tzinfo else t.replace(tzinfo=timezone.utc)) >= cutoff for t in times if t is not None)


class LiveGate:
    """Blocks wait() while farmer photos are processed, and records those pauses."""

    def __init__(self, check=live_processing_active, interval: float = 1.0):
        self._check = check
        self.interval = interval
        self._clear = threading.Event()
        self._clear.set()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.pauses = []          # [start, end or None], time.monotonic()
        self._thread = None

    def _poll(self):
        try:
            active = bool(self._check())
        except Exception as e:  # noqa: BLE001 - keep the last state, don't stall or race
            print(f"[scalability] could not check live processing: {e}", flush=True)
            return
        now = time.monotonic()
        with self._lock:
            if active and self._clear.is_set():
                self.pauses.append([now, None])
                self._clear.clear()
            elif not active and not self._clear.is_set():
                self.pauses[-1][1] = now
                self._clear.set()

    def start(self):
        self._poll()
        self._thread = threading.Thread(target=self._loop, name="live-gate", daemon=True)
        self._thread.start()
        return self

    def _loop(self):
        while not self._stop.wait(self.interval):
            self._poll()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        with self._lock:
            if self.pauses and self.pauses[-1][1] is None:
                self.pauses[-1][1] = time.monotonic()
        self._clear.set()

    @property
    def paused(self) -> bool:
        return not self._clear.is_set()

    def wait(self, timeout=None) -> bool:
        return self._clear.wait(timeout)

    def overlapped(self, t0: float, t1: float) -> bool:
        """Was live processing active at any moment between t0 and t1?"""
        with self._lock:
            return any(start < t1 and (end is None or end > t0) for start, end in self.pauses)

    def paused_seconds(self, t0: float, t1: float) -> float:
        with self._lock:
            return sum(max(0.0, min(end or t1, t1) - max(start, t0)) for start, end in self.pauses)


def lower_priority() -> None:
    """Below-normal OS priority, so the live pipeline gets the CPU first."""
    try:
        import psutil
        p = psutil.Process()
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS if hasattr(psutil, "BELOW_NORMAL_PRIORITY_CLASS") else 10)
    except Exception as e:  # noqa: BLE001
        print(f"[scalability] could not lower process priority: {e}", flush=True)


# --------------------------------------------------------------- the test's own Kafka topic
class KafkaTestQueue:
    """Producer + consumer on the test's own topic (subscribed before sending,
    so nothing is missed). consume=False: producer only."""

    def __init__(self, topic: str = None, group: str = None, consume: bool = True):
        from confluent_kafka import Consumer, KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
        from kafka.producer import KafkaBackend

        topic = topic or settings.SCALABILITY_KAFKA_TOPIC
        group = group or settings.SCALABILITY_CONSUMER_GROUP
        if topic == settings.KAFKA_TOPIC:
            raise ValueError("the scalability test must not use the production Kafka topic")
        servers = settings.KAFKA_BOOTSTRAP_SERVERS
        admin = AdminClient({"bootstrap.servers": servers})
        if topic not in admin.list_topics(timeout=5).topics:
            for future in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).values():
                try:
                    future.result(10)
                except KafkaException as e:
                    if "TOPIC_ALREADY_EXISTS" not in str(e):
                        raise
        self.topic = topic
        self._producer = KafkaBackend(topic)
        self._consumer = None
        if not consume:
            return
        self._consumer = Consumer({"bootstrap.servers": servers, "group.id": group,
                                   "auto.offset.reset": "earliest", "enable.auto.commit": True})
        assigned = threading.Event()
        self._consumer.subscribe([topic], on_assign=lambda consumer, parts: assigned.set())
        deadline = time.monotonic() + 30
        while not assigned.is_set() and time.monotonic() < deadline:
            self._consumer.poll(0.2)          # earlier runs' leftovers only: nothing of this run is sent yet
        if not assigned.is_set():
            raise TimeoutError(f"no partition of Kafka topic {topic} was assigned within 30 s")

    def send_many(self, events: list) -> dict:
        return self._producer.send_many(events)

    def poll(self, timeout: float = 0.5) -> list:
        if self._consumer is None:
            return []
        out = []
        for msg in self._consumer.consume(num_messages=500, timeout=timeout):
            if msg.error():
                continue
            try:
                out.append(json.loads(msg.value()))
            except ValueError:
                continue
        return out

    def close(self) -> None:
        if self._consumer is None:
            return
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------- one photo
def warm_up() -> None:
    """Loads the CNN and runs one dummy photo before the timer starts."""
    from io import BytesIO
    from PIL import Image as PILImage
    from spark.preprocessing import model_input
    from ml.inference import predict
    buf = BytesIO()
    PILImage.new("RGB", (256, 256), (60, 140, 50)).save(buf, format="JPEG")
    predict(model_input(buf.getvalue()))


def process_photo(path) -> tuple:
    """The live pipeline's preprocessing + CNN on one staged photo. Returns
    (timings, prediction); only those two steps are timed."""
    from spark.preprocessing import model_input
    from ml.risk import compute_risk
    from ml.inference import predict
    original = Path(path).read_bytes()
    t0 = time.perf_counter()
    cnn_input = model_input(original)
    t1 = time.perf_counter()
    features = predict(cnn_input)
    t2 = time.perf_counter()
    is_uncertain = bool(features.get("is_uncertain", False))
    prediction = {
        "predicted_class": features.get("predicted_class"),
        "prediction_confidence": features.get("predicted_class_confidence"),
        "is_uncertain": is_uncertain,
        "risk_level": compute_risk(features, is_uncertain=is_uncertain)[1],
    }
    return {"preprocess_s": round(t1 - t0, 4), "cnn_s": round(t2 - t1, 4),
            "processing_s": round(t2 - t0, 4), "_t0": t0, "_t1": t2}, prediction


def photo_location(path, location=None, device_gps=None) -> dict:
    """Where a photo was taken: its municipality and GPS (EXIF first, else the phone's)."""
    from storage.exif_utils import extract_image_metadata
    lat = lon = None
    try:
        meta = extract_image_metadata(Path(path).read_bytes())
        lat, lon = meta.get("gps_latitude"), meta.get("gps_longitude")
    except Exception:  # noqa: BLE001 - unreadable EXIF: no GPS from the file
        pass
    if (lat is None or lon is None) and device_gps:
        lat, lon = device_gps
    return {"location": location, "gps_latitude": lat, "gps_longitude": lon}


PREVIEW_PX = 320
PHOTO_PATHS_FILE = "photo_paths.json"      # {"img_size", "photos": {item_id: original}}, written by run.start


def preview_paths(out_dir: Path, item_id: str) -> tuple:
    folder = Path(out_dir) / "previews"
    return folder / f"{item_id}_before.jpg", folder / f"{item_id}_after.jpg"


def model_input_square(original: bytes, img_size: int):
    """model_input_preview without torch (the dashboard can't load torch); same pixels."""
    from PIL import Image as PILImage
    from spark.preprocessing import _prepared_rgb
    img = _prepared_rgb(original)
    w, h = img.size
    nw, nh = (img_size, int(img_size * h / w)) if w <= h else (int(img_size * w / h), img_size)
    img = img.resize((nw, nh), PILImage.BILINEAR)
    left, top = int(round((nw - img_size) / 2.0)), int(round((nh - img_size) / 2.0))
    return img.crop((left, top, left + img_size, top + img_size))


def save_previews(original_path, out_dir: Path, item_id: str, img_size: int) -> None:
    """The gallery's two small pictures: the original and the square the CNN gets."""
    from io import BytesIO
    from PIL import Image as PILImage, ImageOps
    before_path, after_path = preview_paths(out_dir, item_id)
    before_path.parent.mkdir(parents=True, exist_ok=True)
    original = Path(original_path).read_bytes()
    img = PILImage.open(BytesIO(original))
    img.draft("RGB", (PREVIEW_PX, PREVIEW_PX))
    img = ImageOps.exif_transpose(img).convert("RGB")
    img.thumbnail((PREVIEW_PX, PREVIEW_PX))
    img.save(before_path, format="JPEG", quality=85)
    model_input_square(original, img_size).save(after_path, format="JPEG", quality=90)


def ensure_previews(out_dir: Path, item_id: str, original_path, img_size: int) -> tuple:
    """(before, after) previews, made once and kept; (None, None) if the original is gone."""
    before, after = preview_paths(out_dir, item_id)
    if before.is_file() and after.is_file():
        return before, after
    if not original_path:
        return None, None
    try:
        save_previews(original_path, out_dir, item_id, img_size)
    except Exception:  # noqa: BLE001 - e.g. the original was moved or deleted
        return None, None
    return before, after


def save_comparison(original_path, original_bytes: bytes, out_dir: Path) -> dict:
    """Saves the run's before/after pair; returns their paths relative to `out_dir`."""
    import shutil
    from spark.preprocessing import model_input_preview
    out_dir.mkdir(parents=True, exist_ok=True)
    model_input_preview(original_bytes).save(out_dir / "after_preprocessing.png")
    original_path = Path(original_path)
    if not original_path.resolve().is_relative_to(Path(out_dir).resolve()):
        before = Path(out_dir) / f"before_preprocessing{original_path.suffix.lower()}"
        shutil.copy2(original_path, before)
        original_path = before
    return {"before": str(original_path.relative_to(out_dir)), "after": "after_preprocessing.png"}
