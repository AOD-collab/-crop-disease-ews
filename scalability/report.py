"""
Scalability-test results, only from what a run recorded (a missing value is
left out, never zero).
  Execution time (iteration): first event sent -> last photo finished.
  Processing time (photo): preprocessing + CNN only.
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
    """The photo's preprocessing + CNN seconds, or None if not validly measured."""
    if item.status != "COMPLETED" or not item.timings_json:
        return None
    t = json.loads(item.timings_json)
    if t.get("contended") or t.get("processing_s") is None:
        return None
    return t["processing_s"]


def valid_iteration(run) -> bool:
    """Counts only if it finished, every photo succeeded and no farmer photo ran meanwhile."""
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
    """A test's results: average execution time per valid iteration, average
    processing time per photo, and the totals (None when nothing valid)."""
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
    """One row per test photo for the disease charts."""
    import pandas as pd
    cols = ["image_id", "run_id", "status", "predicted_class", "prediction_confidence", "is_uncertain",
            "risk_level", "location", "gps_latitude", "gps_longitude", "error_message"]
    df = pd.DataFrame([{c: getattr(i, c) for c in cols} for i in items], columns=cols)
    df["location"] = df["location"].fillna("").astype(str).str.strip().replace("", UNKNOWN_LOCATION)
    return df


def _write_csv(path, header: list, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def write_reports(run, items: list, samples: list, summary: dict):
    """Writes summary.csv, items.csv and resources.csv for the run."""
    out = REPORTS_DIR / run.run_id
    out.mkdir(parents=True, exist_ok=True)
    _write_csv(out / "summary.csv", ["metric", "value"],
               ([key, json.dumps(value) if isinstance(value, (dict, list)) else value]
                for key, value in summary.items()))
    _write_csv(out / "items.csv", ITEM_COLUMNS + TIMING_COLUMNS,
               ([getattr(i, c) for c in ITEM_COLUMNS] + [t.get(k) for k in TIMING_COLUMNS]
                for i in items for t in (json.loads(i.timings_json) if i.timings_json else {},)))
    if samples:
        _write_csv(out / "resources.csv", FIELDS, ([sample.get(k) for k in FIELDS] for sample in samples))
    return out
