"""
Performance report (DIT manual section 14B): throughput, CPU, and memory
usage during a pipeline run. Extends spark/analytics.performance_metrics()
(which reports LATENCY from timestamps already recorded in the database)
with process-level CPU/memory usage sampled via psutil, which the database
has no way to record on its own.

Usage:
    python scripts/perf_report.py --volume 200
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psutil

from scripts.scalability_test import run_volume, make_test_user
from storage.database import init_db
from spark import analytics


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--volume", type=int, default=200,
        help="Number of images to process while sampling resource usage "
             "(default: 200, deliberately small for local runs).",
    )
    args = parser.parse_args()

    init_db()
    user_id = make_test_user()

    process = psutil.Process()
    process.cpu_percent()  # first call always returns 0.0 — primes the measurement window

    started = time.monotonic()
    result = run_volume(args.volume, user_id)
    elapsed = time.monotonic() - started

    cpu_percent = process.cpu_percent()  # % averaged over the interval since the call above
    mem_rss_mb = process.memory_info().rss / (1024 * 1024)

    print(f"[perf_report] Processed {args.volume} images in {elapsed:.2f}s")
    if elapsed > 0:
        print(f"[perf_report] Throughput: {args.volume / elapsed:.2f} images/sec")
    print(f"[perf_report] CPU usage over run: {cpu_percent:.1f}%")
    print(f"[perf_report] Resident memory (RSS) at end of run: {mem_rss_mb:.1f} MB")
    print(f"[perf_report] Completed: {result['completed']}, Failed: {result['failed']}")

    print("\n[perf_report] Recorded pipeline latency (from DB timestamps, whole-DB not just this run):")
    for key, value in analytics.performance_metrics().items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
