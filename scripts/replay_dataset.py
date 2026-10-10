"""
Replay a local folder of labeled leaf images through the REAL API (and from
there, Kafka -> Spark -> DB) — simulating real-time field data at a
configurable volume (DIT manual: "at least 10,000" images; the manual's
scalability targets in scripts/scalability_test.py use the same idea).

This never fabricates a Kafka event or DB row directly — every image goes
through the actual POST /upload-image endpoint, exactly like a real
Android/API client, so it exercises the whole real pipeline end to end.

Usage:
    python scripts/replay_dataset.py --dataset-dir /path/to/dataset \\
        --username admin --password admin123 --limit 200

--limit defaults to a small number DELIBERATELY. Point this at a full
10,000+ image dataset only on a machine that can handle the load — a small
local run (the default) is enough to prove the tooling and the pipeline
work; save a full-volume run for a more capable machine or demo day.
"""

import argparse
import sys
import time
from pathlib import Path

import requests

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}


def find_images(dataset_dir: Path, limit: int) -> list:
    """Recursively finds image files under dataset_dir (works for both a
    flat folder and a PlantVillage-style per-class-subfolder layout), up to
    `limit`."""
    images = []
    for path in sorted(dataset_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS:
            images.append(path)
            if len(images) >= limit:
                break
    return images


def login(base_url: str, username: str, password: str) -> str:
    resp = requests.post(
        f"{base_url}/auth/login", json={"username": username, "password": password}, timeout=10
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def replay(base_url: str, token: str, images: list, location: str, delay_seconds: float) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    outcomes = {}
    started = time.monotonic()

    for i, path in enumerate(images, 1):
        try:
            with open(path, "rb") as f:
                resp = requests.post(
                    f"{base_url}/upload-image",
                    headers=headers,
                    data={"location": location},
                    files={"photo": (path.name, f, "image/jpeg")},
                    timeout=30,
                )
            if resp.status_code == 200:
                status = resp.json().get("status", "accepted")
            else:
                status = f"http_{resp.status_code}"
        except requests.RequestException as e:
            status = "request_error"
            print(f"[replay_dataset] {path.name}: {e}", file=sys.stderr)

        outcomes[status] = outcomes.get(status, 0) + 1

        if i % 25 == 0 or i == len(images):
            elapsed = time.monotonic() - started
            rate = i / elapsed if elapsed > 0 else float("inf")
            print(f"[replay_dataset] {i}/{len(images)} sent in {elapsed:.1f}s ({rate:.2f} img/s)")

        if delay_seconds:
            time.sleep(delay_seconds)

    elapsed = time.monotonic() - started
    print(f"\n[replay_dataset] Done: {len(images)} images in {elapsed:.1f}s "
          f"({len(images) / elapsed:.2f} img/s)" if elapsed > 0 else "[replay_dataset] Done instantly")
    print(f"[replay_dataset] Outcomes: {outcomes}")
    return outcomes


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dataset-dir", required=True, type=Path,
                         help="Folder of labeled leaf images (flat or per-class subfolders).")
    parser.add_argument("--limit", type=int, default=200,
                         help="Max images to replay (default: 200, deliberately small).")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--location", default="Dili")
    parser.add_argument("--delay-seconds", type=float, default=0.05,
                         help="Delay between uploads, to avoid hammering a local machine (default 0.05).")
    args = parser.parse_args()

    if not args.dataset_dir.is_dir():
        parser.error(f"--dataset-dir {args.dataset_dir} is not a directory")

    images = find_images(args.dataset_dir, args.limit)
    if not images:
        parser.error(f"No images with extensions {sorted(IMAGE_EXTENSIONS)} found under {args.dataset_dir}")

    print(f"[replay_dataset] Found {len(images)} images under {args.dataset_dir} "
          f"(limit={args.limit}) — replaying against {args.base_url}")

    token = login(args.base_url, args.username, args.password)
    replay(args.base_url, token, images, args.location, args.delay_seconds)


if __name__ == "__main__":
    main()
