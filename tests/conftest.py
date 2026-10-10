"""
Test configuration. Sets environment variables BEFORE any project module is
imported, so the whole test suite runs against an isolated temp SQLite DB,
temp image storage directory, the trained CNN checkpoint shipped at
ml/trained_model/saved_model_cnn.pt, and the local Kafka fallback — no
external infrastructure needed.
"""

import os
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_tmp_dir = Path(tempfile.mkdtemp(prefix="crop_disease_test_"))

os.environ["DATABASE_URL"] = f"sqlite:///{_tmp_dir / 'test.db'}"
os.environ["IMAGE_STORAGE_PATH"] = str(_tmp_dir / "images")
os.environ["KAFKA_USE_LOCAL_FALLBACK"] = "true"
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:19999"  # deliberately unreachable -> forces fallback
os.environ["LOCAL_QUEUE_DIR"] = str(_tmp_dir / "local_queue")
os.environ["MAIL_OUTBOX_DIR"] = str(_tmp_dir / "mail_outbox")  # no MAIL_HOST in tests -> local fallback
os.environ["SPARK_CHECKPOINT_ROOT"] = str(_tmp_dir / "spark_checkpoints")  # factory_reset() tests must not wipe the real job's
os.environ["REVIEW_DIR"] = str(_tmp_dir / "training_data" / "review")  # never the real training project
os.environ["LABELLED_TRAIN_DIR"] = str(_tmp_dir / "training_data" / "train")
os.environ["DOWNLOADED_PHOTOS_DIR"] = str(_tmp_dir / "downloaded_photos")
os.environ["SCALABILITY_REPORTS_DIR"] = str(_tmp_dir / "scalability_reports")
os.environ["JWT_SECRET"] = "test-secret-key-not-for-production"
os.environ.setdefault("BOOTSTRAP_ADMIN_PASSWORD", "")

# Import torch as the very first thing, before pytest collects/imports the
# rest of the suite (FastAPI, SQLAlchemy, confluent-kafka, pyspark, ...).
# Observed on this Windows machine: torch's native DLL (torch/lib/c10.dll)
# intermittently fails to load ("OSError: WinError 1114 - DLL
# initialization routine failed") when it's first imported only after many
# other test modules/native libraries are already loaded in the same
# process — but never when it's the first native extension loaded. Forcing
# the import here, at collection time, dodges that ordering-sensitive
# failure.
import torch  # noqa: F401,E402

import pytest


def make_leaf_jpeg_bytes(size: int = 64) -> bytes:
    """A small synthetic-but-plausible leaf photo: a green base (healthy
    leaf tissue) with a few darker lesion-like spots and yellow/brown
    discoloration patches drawn on top. Used everywhere the test suite
    needs a leaf-like JPEG to send through the model. This is test fixture data for the
    test suite itself, not a production data source — the application
    never generates images."""
    import random
    from io import BytesIO
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (size, size), color=(60, 150, 40))
    draw = ImageDraw.Draw(img)
    # Unseeded RNG: each call produces slightly different (still
    # leaf-colored) bytes, so tests that upload multiple "different" photos
    # don't all collide on the new content-hash duplicate-detection check.
    rng = random.Random()
    for _ in range(max(2, size // 16)):  # lesion-like dark spots
        x, y = rng.randint(0, size - 1), rng.randint(0, size - 1)
        r = max(1, size // 16)
        draw.ellipse((x - r, y - r, x + r, y + r), fill=(30, 25, 20))
    for _ in range(max(2, size // 16)):  # yellow/brown discoloration patches
        x, y = rng.randint(0, size - 1), rng.randint(0, size - 1)
        r = max(1, size // 16)
        draw.ellipse((x - r, y - r, x + r, y + r), fill=(150, 110, 50))
    buf = BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def pytest_collection_modifyitems(config, items):
    """Windows-specific ordering fix: torch's native DLL loading
    (torch/lib/c10.dll) can fail with OSError WinError 1114 ("DLL
    initialization routine failed") if a local-mode PySpark JVM session
    (test_streaming_pipeline.py, test_batch_analytics.py) already started
    and stopped earlier in the SAME pytest process — an observed
    interaction between PySpark's py4j JVM gateway and torch's native
    library loading on this platform, not a bug in either library on its
    own (each passes cleanly when run alone). Reordering so torch-needing
    tests always run first, in a clean process, avoids it without adding a
    process-isolation plugin dependency."""
    def sort_key(item):
        path = str(item.fspath)
        if "test_trained_backend" in path:
            return 0
        if "test_streaming_pipeline" in path or "test_batch_analytics" in path:
            return 2
        return 1

    items.sort(key=sort_key)


@pytest.fixture(autouse=True)
def _reset_rate_limits():
    """The auth rate limiter (api/rate_limit.py) is a per-process in-memory
    counter, so without resetting it between tests, unrelated tests that
    each call /auth/login a few times would eventually trip each other's
    limits. Real traffic doesn't reset between requests — only the test
    suite needs this isolation."""
    from api.rate_limit import _hits
    _hits.clear()
    yield
    _hits.clear()


@pytest.fixture(autouse=True)
def _kafka_unreachable_fast(monkeypatch):
    """KAFKA_BOOTSTRAP_SERVERS points at a port nothing listens on, so every
    KafkaBackend() spent 3 s in list_topics(timeout=3) before
    ResilientBackend fell back to the local file queue — and it retries
    every 30 s, so the cost repeated throughout the run. Fail the connect
    instantly instead; the fallback path taken is identical. Tests that
    exercise Kafka behaviour (test_resilient_backend, test_scalability)
    install their own fakes on top of this."""
    import kafka.producer

    def _unreachable(self, topic):
        raise ConnectionError(f"test suite: no broker at {os.environ['KAFKA_BOOTSTRAP_SERVERS']}")

    monkeypatch.setattr(kafka.producer.KafkaBackend, "__init__", _unreachable)


@pytest.fixture()
def sample_leaf_image_bytes():
    """A small synthetic-but-valid leaf-like JPEG for exercising the
    upload/validation path in tests."""
    return make_leaf_jpeg_bytes(64)
