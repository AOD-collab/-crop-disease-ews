"""
Scalability test (DIT manual section 14D): measures pipeline throughput at
increasing image volumes by calling spark.preprocessing.process_event() +
persist_result() directly against real seeded Image rows and real
(synthetic, in-memory-generated) JPEG bytes — the same per-image work the
Spark streaming job does, minus HTTP/Kafka transport overhead, so the
numbers reflect the CNN/CV + DB-write cost the manual is actually asking
about.

Usage:
    python scripts/scalability_test.py --volumes 100 500 1000

The manual's target volumes are 10,000 / 50,000 / 100,000+. This script
takes arbitrary volumes as CLI args rather than hard-coding them — run it
locally only at small volumes (the default); a full-volume run belongs on a
machine that can handle the load, not a laptop that might crash under it.
"""

import argparse
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from PIL import Image as PILImage, ImageDraw

from config.settings import IMAGE_STORAGE_PATH
from storage.database import init_db, SessionLocal, User, Image, Role, ProcessingStatus
from spark.preprocessing import process_event, persist_result, InvalidEvent


def make_test_user() -> str:
    session = SessionLocal()
    try:
        user_id = str(uuid.uuid4())
        session.add(User(
            user_id=user_id, username=f"scale-test-{user_id[:8]}",
            email=f"scale-test-{user_id[:8]}@example.com",
            password_hash="x", role=Role.USER,
        ))
        session.commit()
        return user_id
    finally:
        session.close()


def _synthetic_jpeg_bytes() -> bytes:
    """A cheap, real (not corrupted) leaf-like JPEG."""
    img = PILImage.new("RGB", (64, 64), color=(60, 150, 40))
    draw = ImageDraw.Draw(img)
    rng = random.Random()
    for _ in range(4):
        x, y = rng.randint(0, 63), rng.randint(0, 63)
        draw.ellipse((x - 3, y - 3, x + 3, y + 3), fill=(30, 25, 20))
    buf = BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def _make_test_image(user_id: str) -> str:
    IMAGE_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    image_id = str(uuid.uuid4())
    path = IMAGE_STORAGE_PATH / f"{image_id}.jpg"
    path.write_bytes(_synthetic_jpeg_bytes())

    now = datetime.now(timezone.utc)
    session = SessionLocal()
    try:
        # ingested_at set here, matching what a real upload+publish already
        # does before Spark ever sees the event — otherwise persist_result()
        # would set it at persist-time (after process_event already ran),
        # producing a meaningless near-zero/negative "processing latency".
        session.add(Image(
            image_id=image_id, user_id=user_id, location="Dili", image_path=str(path),
            uploaded_at=now, ingested_at=now, status=ProcessingStatus.INGESTED,
        ))
        session.commit()
    finally:
        session.close()
    return image_id, str(path)


def run_volume(volume: int, user_id: str) -> dict:
    started = time.monotonic()
    completed, failed = 0, 0
    for _ in range(volume):
        image_id, image_path = _make_test_image(user_id)
        event = {
            "image_id": image_id, "user_id": user_id, "location": "Dili",
            "image_path": image_path, "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            result = process_event(event)
            persist_result(event, result, ProcessingStatus.COMPLETED)
            completed += 1
        except InvalidEvent as e:
            persist_result(event, None, ProcessingStatus.FAILED, error_message=str(e))
            failed += 1
    elapsed = time.monotonic() - started
    throughput = volume / elapsed if elapsed > 0 else float("inf")
    return {
        "volume": volume,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_images_per_sec": round(throughput, 2),
        "completed": completed,
        "failed": failed,
    }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--volumes", type=int, nargs="+", default=[100, 500, 1000],
        help="Image counts to test, in increasing order (default: 100 500 1000). "
             "The manual's real targets (10000/50000/100000+) should be run on a "
             "machine that can handle the load, not passed here by default.",
    )
    args = parser.parse_args()

    init_db()
    user_id = make_test_user()

    print(f"[scalability_test] Testing volumes: {args.volumes}")
    results = []
    for volume in args.volumes:
        print(f"[scalability_test] Running volume={volume}...")
        result = run_volume(volume, user_id)
        results.append(result)
        print(f"[scalability_test]   -> {result}")

    print("\n[scalability_test] Summary:")
    print(f"{'volume':>10} {'elapsed_s':>12} {'img/sec':>10} {'completed':>10} {'failed':>8}")
    for r in results:
        print(f"{r['volume']:>10} {r['elapsed_seconds']:>12} {r['throughput_images_per_sec']:>10} "
              f"{r['completed']:>10} {r['failed']:>8}")


if __name__ == "__main__":
    main()
