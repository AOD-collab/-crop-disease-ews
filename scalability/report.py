"""
Scalability-test measurements and files. Everything is computed from what a
run actually recorded (scalability/run.py, scalability/worker.py): the
runner's monotonic clock per iteration and each photo's perf_counter
timings in scale_test_items. Nothing is estimated, and a missing value is
left out, never counted as zero.

Timer boundaries:
  Execution time (one iteration) — time.monotonic() from right before the
      first event is published to the test's Kafka topic (every photo is
      already found, downloaded, verified and staged) until the last photo
      is COMPLETED or FAILED. Includes the test topic, reading each staged
      file, preprocessing, CNN inference and recording the measurement.
  Processing time (one photo) — time.perf_counter() around
      spark/preprocessing.model_input + ml/inference.predict only. Queue
      waiting, file reading, Google Drive and database time are excluded.
"""

import csv
import json
import statistics

from config.settings import SCALABILITY_REPORTS_DIR
from scalability.monitor import summarize, FIELDS

REPORTS_DIR = SCALABILITY_REPORTS_DIR


def summarize_samples(samples: list) -> dict:
    return summarize(samples) if samples else {"resource_samples": 0}


def processing_seconds(item):
    """The photo's preprocessing + CNN seconds, or None when it has no
    valid measurement (failed, not finished, measured while farmer photos
    were being processed, or a run from before the isolation)."""
    if item.status != "COMPLETED" or not item.timings_json:
        return None
    t = json.loads(item.timings_json)
    if t.get("contended") or t.get("processing_s") is None:
        return None
    return t["processing_s"]


def valid_iteration(run) -> bool:
    """An iteration whose execution time counts: it finished, every photo
    succeeded, and no farmer photo was being processed meanwhile."""
    if run.status != "COMPLETED" or not run.summary_json:
        return False
    s = json.loads(run.summary_json)
    return (s.get("execution_seconds") is not None and not s.get("live_overlap")
            and not s.get("failed") and not s.get("not_finished"))


def _total_end_to_end(runs: list):
    if not runs or not all(valid_iteration(r) for r in runs):
        return None
    values = [json.loads(r.summary_json).get("end_to_end_seconds") for r in runs]
    return sum(values) if all(v is not None for v in values) else None


def series_averages(runs: list, items: list) -> dict:
    """The two results the dashboard shows for a test (one or more
    iterations, scalability/run.start_series):

      average_execution_s  = sum of the valid iterations' execution times
                             / number of valid iterations
      average_processing_s = mean processing time of every validly measured
                             photo of the test's finished iterations
      total_end_to_end_s   = the page's Total Execution Time: sum of every
                             iteration's end_to_end_seconds (from pressing
                             Start test to the last photo finished); None
                             unless EVERY iteration is valid and has it
      total_execution_s    = sum of the valid iterations' execution times
      total_processing_s   = sum of those photos' processing times (photos run
                             several at a time, so this is more than the
                             total execution time)

    `runs`: the iterations' ScaleTestRun rows; `items`: their
    ScaleTestItem rows. A value is None when nothing valid was measured."""
    valid = [r for r in runs if valid_iteration(r)]
    durations = [json.loads(r.summary_json)["execution_seconds"] for r in valid]
    finished = {r.run_id for r in runs if r.status == "COMPLETED" and r.summary_json
                and json.loads(r.summary_json).get("execution_seconds") is not None}
    per_photo = [v for v in (processing_seconds(i) for i in items if i.run_id in finished) if v is not None]
    return {
        "iterations_measured": len(durations),
        "photos_measured": len(per_photo),
        "average_execution_s": sum(durations) / len(durations) if durations else None,
        "average_processing_s": statistics.fmean(per_photo) if per_photo else None,
        "total_execution_s": sum(durations) if durations else None,
        "total_end_to_end_s": _total_end_to_end(runs),
        "total_processing_s": sum(per_photo) if per_photo else None,
    }


ITEM_COLUMNS = ["image_id", "source_image_id", "source_drive_file_id", "local_copy_status", "download_seconds",
                "size_bytes", "status", "error_message", "published_at", "spark_received_at", "completed_at",
                "predicted_class", "prediction_confidence", "is_uncertain", "risk_level",
                "location", "gps_latitude", "gps_longitude"]
TIMING_COLUMNS = ["preprocess_s", "cnn_s", "processing_s", "contended"]

UNKNOWN_LOCATION = "Unknown location"


def items_frame(items: list):
    """One row per test photo (pandas DataFrame) with what the disease
    charts need: status, CNN result, municipality ("Unknown location" when
    the photo has none, e.g. a custom-folder photo) and GPS."""
    import pandas as pd
    cols = ["image_id", "run_id", "status", "predicted_class", "prediction_confidence", "is_uncertain",
            "risk_level", "location", "gps_latitude", "gps_longitude", "error_message"]
    df = pd.DataFrame([{c: getattr(i, c) for c in cols} for i in items], columns=cols)
    df["location"] = df["location"].fillna("").astype(str).str.strip().replace("", UNKNOWN_LOCATION)
    return df


def write_reports(run, items: list, samples: list, summary: dict):
    """SCALABILITY_REPORTS_DIR/<run_id>/: summary.csv, items.csv (one row per
    photo, with its timings) and resources.csv (CPU/RAM/GPU every second)."""
    out = REPORTS_DIR / run.run_id
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "summary.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        for key, value in summary.items():
            w.writerow([key, json.dumps(value) if isinstance(value, (dict, list)) else value])
    with open(out / "items.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(ITEM_COLUMNS + TIMING_COLUMNS)
        for i in items:
            t = json.loads(i.timings_json) if i.timings_json else {}
            w.writerow([getattr(i, c) for c in ITEM_COLUMNS] + [t.get(k) for k in TIMING_COLUMNS])
    if samples:
        with open(out / "resources.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(samples)
    return out
