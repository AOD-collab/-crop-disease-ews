"""
Factory reset — wipe all pipeline data and history back to a clean slate,
while KEEPING every user and admin account intact.

What it clears:
  * DB rows in `predictions`, `processing_events`, `images`, `rejected_events`
    (the `users` table is never touched)
  * uploaded photo files under IMAGE_STORAGE_PATH (storage/images/)
  * the Kafka local file-queue under LOCAL_QUEUE_DIR (storage/local_queue/)
  * Spark Structured Streaming checkpoints under storage/spark_checkpoints/
  * optionally the Kafka topic's messages (topic deleted + recreated empty).
    Without this, Spark — whose checkpoints were just cleared and which reads
    from "earliest" — replays every old message and recreates the history.
  * optionally every farmer's photos and results in Google Drive (moved to
    the central account's trash, recoverable for 30 days; the per-farmer
    folders and their sharing are kept so the app keeps working), plus a
    reset marker that makes each phone clear its saved history copy
  * optionally the dev mail outbox under MAIL_OUTBOX_DIR
  * the Drive watcher's bookmark (storage/drive_watcher_state.json): after
    the restart every photo still in Drive is registered and processed
    again, from its local copy (no new download)

What it never touches: the local copies of downloaded Drive photos under
DOWNLOADED_PHOTOS_DIR (storage/downloaded_photos/), the review and training
images, the administrator's label decisions (image_reviews), the storage
audit log, and the scalability-test measurements (scale_test_runs /
scale_test_items, storage/scalability_reports/). reset_preview() counts
both sides before the reset; afterwards the retained cached photos are
reconciled (storage/image_cache.reconcile) and reported, never deleted, and
the reset is written to storage_audit_log.

Stop the Spark job and drive watcher before resetting, then start them again.

Used by the Admin dashboard's "Factory reset" button (dashboard/app.py) and
runnable on its own:

    python -m storage.reset --yes                    # DB + local files only
    python -m storage.reset --yes --kafka --drive    # full clean start
    python -m storage.reset --yes --mail             # also clear the mail outbox
"""

import argparse
import shutil
import sys
import time

from config.settings import (
    IMAGE_STORAGE_PATH, LOCAL_QUEUE_DIR, MAIL_OUTBOX_DIR, SPARK_CHECKPOINT_ROOT,
)
from storage.database import (
    SessionLocal, init_db, Image, Prediction, ProcessingEvent, RejectedEvent, ScaleTestRun, now_utc,
)

SPARK_CHECKPOINT_DIR = SPARK_CHECKPOINT_ROOT


def _empty_dir(path) -> int:
    """Delete everything inside `path` (keeping the directory itself).
    Returns the number of top-level entries removed."""
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
    """True if a `spark.streaming_job` process is running on this machine,
    None if that can't be checked. Resetting under a live job corrupts its
    checkpoint: the job keeps writing batch N into the emptied folder, and
    the next start fails with "batch N-1 doesn't exist"."""
    out = _python_command_lines()
    return None if out is None else ("spark.streaming_job" in out or "streaming_job.py" in out)


def drive_watcher_running():
    """True if the Drive watcher (kafka.drive_watcher) is running, None if
    that can't be checked. A watcher running during the reset can register a
    new upload after the tables are emptied and publish it just before the
    Kafka topic is cleared — in AUTOMATIC mode that photo then stays
    INGESTED for good."""
    out = _python_command_lines()
    return None if out is None else ("kafka.drive_watcher" in out or "drive_watcher.py" in out)


def _python_command_lines():
    """Command lines of the running Python processes, or None if they can't
    be read. Also used by the dashboard's System Status checks."""
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
    """Delete the image topic and recreate it empty with the same partition
    count. Returns a short status string; never raises (the broker may
    simply not be running, which shouldn't block the rest of the reset)."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        from confluent_kafka import KafkaException
        from kafka.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        topic = admin.list_topics(timeout=10).topics.get(KAFKA_TOPIC)
        partitions = len(topic.partitions) if topic and topic.partitions else 1
        if topic is not None:
            admin.delete_topics([KAFKA_TOPIC], operation_timeout=timeout)[KAFKA_TOPIC].result()

        # Deletion finishes asynchronously; creating too early fails with
        # "marked for deletion", so retry until the broker lets it through.
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
    """Remove every file inside each farmer's Drive images/<email>/ and
    results/<email>/ folder. Files the central account owns (results) go to
    its trash; photos the FARMER owns can't be trashed by anyone else, so
    they're taken out of the folder instead — the watcher and the app no
    longer see them. Then leaves a RESET_MARKER_NAME file in every
    results/<email>/ folder, so each phone drops its saved history copy.
    Never raises; errors are reported in the summary."""
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


# Read by the Android app (DriveRest.resetMarker): a reset_at it hasn't seen
# yet makes the phone delete its saved history and reload it from Drive.
# No image_id appProperty, so the app's result listings skip it.
RESET_MARKER_NAME = "_reset_marker.json"


def write_reset_markers(result_folders: list, summary: dict) -> None:
    from datetime import datetime, timezone
    from storage import drive_client

    reset_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    for folder_id in result_folders:
        try:
            # may_exist: an old marker whose trashing failed is overwritten.
            drive_client.upload_json(RESET_MARKER_NAME, {"reset_at": reset_at}, folder_id,
                                     {"kind": "reset_marker", "reset_at": reset_at})
            summary["reset_markers"] += 1
        except Exception as e:
            summary["errors"].append(f"reset marker in {folder_id}: {e}")


def _count_files(path) -> int:
    return sum(1 for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def reset_preview() -> dict:
    """What a factory reset WILL delete and what it KEEPS, counted now.
    Changes nothing. Shown on the dashboard before the reset is confirmed."""
    from storage.database import User, ImageReview, StorageAuditLog, ScaleTestItem
    from storage import image_cache
    init_db()
    with SessionLocal() as s:
        deleted = {"images": s.query(Image).count(), "predictions": s.query(Prediction).count(),
                   "processing_events": s.query(ProcessingEvent).count(),
                   "rejected_events": s.query(RejectedEvent).count()}
        kept_db = {"users": s.query(User).count(), "image_reviews": s.query(ImageReview).count(),
                   "storage_audit_log": s.query(StorageAuditLog).count(),
                   "scale_test_runs": s.query(ScaleTestRun).count(),
                   "scale_test_items": s.query(ScaleTestItem).count()}
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
                      "database record until the Drive watcher registers their Drive originals again; any that "
                      "stay unmatched show as potentially orphaned on the Image management page, where they "
                      "can be reviewed — they are never deleted automatically."),
    }


def factory_reset(clear_mail_outbox: bool = False, clear_kafka: bool = False,
                  clear_drive: bool = False, performed_by: str = None) -> dict:
    """Perform the reset. Returns a summary of what was removed.

    Users and admins are intentionally left untouched — only pipeline
    data and its on-disk/queue/checkpoint history is cleared (plus, when
    asked, the Kafka topic and the farmers' Drive files).
    """
    init_db()  # make sure the tables exist before we DELETE from them

    session = SessionLocal()
    try:
        # Order matters: predictions and processing_events both FK to images.
        predictions_deleted = session.query(Prediction).delete(synchronize_session=False)
        events_deleted = session.query(ProcessingEvent).delete(synchronize_session=False)
        rejected_deleted = session.query(RejectedEvent).delete(synchronize_session=False)
        images_deleted = session.query(Image).delete(synchronize_session=False)
        # Images of scalability runs from before the isolation went with
        # them; all test measurements (scale_test_runs/items) are kept, and
        # Spark skips every test event left in its topic (run_id set).
        session.query(ScaleTestRun).filter(ScaleTestRun.data_cleaned_at.is_(None)).update(
            {ScaleTestRun.data_cleaned_at: now_utc()}, synchronize_session=False)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

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
        "mail_outbox_cleared": _empty_dir(MAIL_OUTBOX_DIR) if clear_mail_outbox else None,
        "drive_watcher_bookmark": _clear_watcher_bookmark(),
    }
    # What stayed on disk, matched against the now-empty catalog: every
    # cached original is reported (unmatched until re-registered), nothing
    # of it is deleted.
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
    """Deletes the Drive watcher's bookmark, so that after the reset it
    registers EVERY photo still in the farmers' Drive folders again (with its
    original upload time, location and phone GPS from Drive) and Spark
    processes them from their local copies. Without this, only the photos
    in the watcher's 2-minute look-back window came back. With --drive the
    photos are gone from Drive, so nothing comes back."""
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
    parser.add_argument("--mail", action="store_true",
                        help="also clear the dev mail outbox (storage/mail_outbox/)")
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

    summary = factory_reset(clear_mail_outbox=args.mail, clear_kafka=args.kafka,
                            clear_drive=args.drive)
    print("[reset] Factory reset complete. Users/admins were kept. Summary:")
    for key, value in summary.items():
        if value is None:
            continue
        print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
