"""
Factory reset: clears photos, predictions, queues, checkpoints and history,
keeping every user account. Optionally empties the Kafka topic and the
farmers' Drive folders. Keeps downloaded photos, labels, the audit log and
scalability-test results. Stop Spark and the Drive watcher first.

    python -m storage.reset --yes [--kafka] [--drive]
"""

import argparse
import shutil
import sys
import time

from config.settings import (
    IMAGE_STORAGE_PATH, LOCAL_QUEUE_DIR, SPARK_CHECKPOINT_ROOT,
)
from storage.database import (
    SessionLocal, init_db, Image, Prediction, ProcessingEvent, RejectedEvent, ScaleTestRun, now_utc,
)

SPARK_CHECKPOINT_DIR = SPARK_CHECKPOINT_ROOT


def _empty_dir(path) -> int:
    """Deletes everything inside `path`; returns how many entries."""
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return 0
    removed = 0
    for entry in path.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)
        else:
            entry.unlink(missing_ok=True)
        removed += 1
    return removed


def spark_job_running():
    """True if the Spark job is running (None if unknown). Resetting under
    it corrupts its checkpoint."""
    return _process_running("spark.streaming_job", "streaming_job.py")


def drive_watcher_running():
    """True if the Drive watcher is running (None if unknown)."""
    return _process_running("kafka.drive_watcher", "drive_watcher.py")


def _process_running(*names):
    """True if a Python command line names any of `names` (None if unknown)."""
    out = _python_command_lines()
    return None if out is None else any(n in out for n in names)


def _python_command_lines():
    """Command lines of running Python processes, or None."""
    import subprocess
    try:
        if sys.platform == "win32":
            cmd = ["powershell", "-NoProfile", "-Command",
                   "Get-CimInstance Win32_Process -Filter \"Name LIKE 'python%'\" | "
                   "ForEach-Object { $_.CommandLine }"]
        else:
            cmd = ["ps", "-eo", "args"]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout
    except Exception:
        return None


def clear_kafka_topic(timeout: float = 30.0) -> str:
    """Deletes and recreates the image topic empty. Never raises."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        from confluent_kafka import KafkaException
        from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        topic = admin.list_topics(timeout=10).topics.get(KAFKA_TOPIC)
        partitions = len(topic.partitions) if topic and topic.partitions else 1
        if topic is not None:
            admin.delete_topics([KAFKA_TOPIC], operation_timeout=timeout)[KAFKA_TOPIC].result()

        # deletion is asynchronous: retry until the topic can be created
        deadline = time.monotonic() + timeout
        while True:
            try:
                admin.create_topics([NewTopic(KAFKA_TOPIC, partitions, 1)])[KAFKA_TOPIC].result()
                return f"topic '{KAFKA_TOPIC}' emptied ({partitions} partition(s))"
            except KafkaException:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
    except Exception as e:
        return f"FAILED: {e} (is the Kafka broker running?)"


def clear_drive_files() -> dict:
    """Empties every farmer's Drive images and results folders (farmer-owned
    photos are removed from the folder) and leaves a reset marker so each
    phone clears its history. Never raises."""
    from storage import drive_client

    summary = {"trashed": 0, "removed_from_folder": 0, "reset_markers": 0, "errors": []}
    try:
        session = drive_client._authed_session()
        image_folders = [f["id"] for f in drive_client.list_subfolders(drive_client.images_folder_id())]
        result_folders = [f["id"] for f in drive_client.list_subfolders(drive_client.results_folder_id())]
        folders = image_folders + result_folders
        files = drive_client.list_files_in_folders(folders) if folders else []
    except Exception as e:
        summary["errors"].append(f"could not list Drive folders: {e}")
        return summary

    folder_set = set(folders)
    for meta in files:
        file_id = meta["id"]
        url = f"{drive_client.DRIVE_API}/files/{file_id}"
        try:
            resp = session.patch(url, params={"fields": "id"}, json={"trashed": True})
            if resp.status_code == 403:  # not the owner -> detach from the farmer folder
                parents = ",".join(p for p in meta.get("parents", []) if p in folder_set)
                resp = session.patch(url, params={"removeParents": parents, "fields": "id"}, json={})
                drive_client._check(resp, f"remove {file_id} from folder")
                summary["removed_from_folder"] += 1
            else:
                drive_client._check(resp, f"trash {file_id}")
                summary["trashed"] += 1
        except Exception as e:
            summary["errors"].append(f"{meta.get('name', file_id)}: {e}")

    write_reset_markers(result_folders, summary)
    return summary


# the app sees a new reset_at and reloads its history from Drive
RESET_MARKER_NAME = "_reset_marker.json"


def write_reset_markers(result_folders: list, summary: dict) -> None:
    from datetime import datetime, timezone
    from storage import drive_client

    reset_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    for folder_id in result_folders:
        try:
            drive_client.upload_json(RESET_MARKER_NAME, {"reset_at": reset_at}, folder_id,
                                     {"kind": "reset_marker", "reset_at": reset_at})
            summary["reset_markers"] += 1
        except Exception as e:
            summary["errors"].append(f"reset marker in {folder_id}: {e}")


def _count_files(path) -> int:
    return sum(1 for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def reset_preview() -> dict:
    """What a reset will delete and keep, counted now. Changes nothing."""
    from storage.database import User, ImageReview, StorageAuditLog, ScaleTestItem
    from storage import image_cache
    init_db()
    with SessionLocal() as s:
        deleted, kept_db = ({t.__tablename__: s.query(t).count() for t in tables} for tables in (
            (Image, Prediction, ProcessingEvent, RejectedEvent),
            (User, ImageReview, StorageAuditLog, ScaleTestRun, ScaleTestItem)))
    usage = {u["category"]: u for u in image_cache.usage()}
    return {
        "deleted_rows": deleted,
        "deleted_files": {"legacy uploads (storage/images)": _count_files(IMAGE_STORAGE_PATH),
                          "Kafka local file queue": _count_files(LOCAL_QUEUE_DIR),
                          "Spark checkpoints": _count_files(SPARK_CHECKPOINT_DIR)},
        "kept_rows": kept_db,
        "kept_files": {image_cache.CATEGORIES[c]: usage[c]["files"]
                       for c in ("original_cache", "review", "training", "scalability")},
        "kept_note": (f"After the reset, the {usage['original_cache']['files']} cached original photo(s) have no "
                      "database record until Spark registers their Drive originals again; any that "
                      "stay unmatched show as potentially orphaned on the Image management page, where they "
                      "can be reviewed — they are never deleted automatically."),
    }


def factory_reset(clear_kafka: bool = False, clear_drive: bool = False,
                  performed_by: str = None) -> dict:
    """Performs the reset; returns a summary. Users are never touched."""
    init_db()  # make sure the tables exist before we DELETE from them

    # no commit on error: nothing is half done
    with SessionLocal() as session:
        # predictions and processing_events reference images, so they go first
        predictions_deleted = session.query(Prediction).delete(synchronize_session=False)
        events_deleted = session.query(ProcessingEvent).delete(synchronize_session=False)
        rejected_deleted = session.query(RejectedEvent).delete(synchronize_session=False)
        images_deleted = session.query(Image).delete(synchronize_session=False)
        session.query(ScaleTestRun).filter(ScaleTestRun.data_cleaned_at.is_(None)).update(
            {ScaleTestRun.data_cleaned_at: now_utc()}, synchronize_session=False)
        session.commit()

    summary = {
        "predictions_deleted": predictions_deleted,
        "processing_events_deleted": events_deleted,
        "rejected_events_deleted": rejected_deleted,
        "images_deleted": images_deleted,
        "image_files_cleared": _empty_dir(IMAGE_STORAGE_PATH),
        "kafka_queue_cleared": _empty_dir(LOCAL_QUEUE_DIR),
        "spark_checkpoints_cleared": _empty_dir(SPARK_CHECKPOINT_DIR),
        "kafka_topic": clear_kafka_topic() if clear_kafka else None,
        "drive_files": clear_drive_files() if clear_drive else None,
        "drive_watcher_bookmark": _clear_watcher_bookmark(),
    }
    # report what stayed on disk; nothing is deleted
    from storage import image_cache
    try:
        records = image_cache.reconcile()
        summary["retained_cached_originals"] = sum(1 for r in records if r.verdict not in ("missing",))
        summary["retained_unmatched_now"] = sum(1 for r in records
                                                if r.verdict in ("orphan", "orphan_duplicate", "wrong_folder"))
    except Exception as e:  # noqa: BLE001 - the reset itself already succeeded
        summary["retained_files_report"] = f"FAILED: {e}"
    # always recorded, even when the retained-files report failed
    image_cache.audit("factory_reset", "done", detail={k: v for k, v in summary.items() if v is not None},
                      performed_by=performed_by)
    return summary


def _clear_watcher_bookmark() -> str:
    """Deletes the watcher's bookmark so every photo still in Drive is sent again."""
    from kafka import drive_watcher
    path = drive_watcher.STATE_FILE
    if not path.exists():
        return "none"
    path.unlink()
    return "cleared (every photo still in Drive is processed again)"


def _main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true",
                        help="required — confirm the destructive reset")
    parser.add_argument("--kafka", action="store_true",
                        help="also delete every message in the Kafka image topic")
    parser.add_argument("--drive", action="store_true",
                        help="also remove every farmer's photos and results from Google Drive")
    args = parser.parse_args(argv)

    if not args.yes:
        parser.error("refusing to run without --yes (this deletes all pipeline data)")
    if spark_job_running():
        parser.error("the Spark streaming job is running — stop it first (Ctrl+C in its "
                     "window), otherwise its checkpoint is corrupted")
    if drive_watcher_running():
        parser.error("the Drive watcher is running — stop it first, otherwise an upload that "
                     "arrives during the reset can be lost")

    summary = factory_reset(clear_kafka=args.kafka, clear_drive=args.drive)
    print("[reset] Factory reset complete. Users/admins were kept. Summary:")
    for key, value in summary.items():
        if value is None:
            continue
        print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
