"""
Finds photos still owed a result (stuck INGESTED/PROCESSING, or COMPLETED
but not delivered to Drive) and sends them again.

    python -m storage.requeue_stuck --dry-run
    python -m storage.requeue_stuck --all --inline        # process here, without Kafka/Spark
    python -m storage.requeue_stuck --older-than 300      # re-send rows stuck > 5 min
    python -m storage.requeue_stuck --all --include-failed
    python -m storage.requeue_stuck --from-drive          # Drive photos Spark never registered
"""

import argparse
import sys
from datetime import timedelta

from sqlalchemy import and_, or_

from storage.database import (
    SessionLocal, Image, ProcessingStatus, now_utc, init_db, SCALE_TEST_SOURCE,
)
from storage.image_storage import resolve_image_path, to_stored_path, is_drive_path, DRIVE_PATH_PREFIX
from storage.pipeline_submit import build_upload_event

# statuses that still owe the farmer a result
STUCK_STATUSES = {ProcessingStatus.INGESTED, ProcessingStatus.PROCESSING}

# FAILED rows worth retrying: the failure was Kafka or finding the file, not the photo
_RETRYABLE_FAILURE_MARKERS = (
    "Kafka publish failed",
    "Image file not found on disk",
    "Image file missing on disk",
    "Image file not found in Google Drive",
)


def result_not_delivered():
    """SQL condition: a COMPLETED Drive photo whose result never reached Drive."""
    return and_(Image.status == ProcessingStatus.COMPLETED,
                Image.result_published_at.is_(None),
                Image.image_path.like(DRIVE_PATH_PREFIX + "%"))


def _not_scale_test():
    # belongs to a scalability-test run; re-sending it would skew that run
    return or_(Image.source.is_(None), Image.source != SCALE_TEST_SOURCE)


def find_stuck(session, older_than_seconds: int, include_failed: bool, ignore_age: bool):
    cutoff = now_utc() - timedelta(seconds=older_than_seconds)
    eligible = [Image.status.in_(STUCK_STATUSES), result_not_delivered()]
    if include_failed:
        eligible.append(and_(Image.status == ProcessingStatus.FAILED,
                             or_(*[Image.error_message.like(m + "%") for m in _RETRYABLE_FAILURE_MARKERS])))
    rows = session.query(Image).filter(_not_scale_test(), or_(*eligible)).all()
    out = []
    for img in rows:
        if not ignore_age:
            if img.status == ProcessingStatus.COMPLETED:
                # its Drive upload may still be running
                stamp = max((t for t in (img.processed_at, img.ingested_at) if t), default=None)
            else:
                stamp = img.ingested_at or img.uploaded_at or img.created_at
            # stored naive-UTC in SQLite; compare on the same basis
            stamp_cmp = stamp.replace(tzinfo=cutoff.tzinfo) if stamp and stamp.tzinfo is None else stamp
            if stamp_cmp and stamp_cmp > cutoff:
                continue
        out.append(img)
    return out


def count_owed(session, older_than_seconds: int = 120) -> dict:
    """Counts {"waiting", "undelivered"} older than `older_than_seconds`."""
    owed = find_stuck(session, older_than_seconds, include_failed=False, ignore_age=False)
    undelivered = sum(1 for i in owed if i.status == ProcessingStatus.COMPLETED)
    return {"waiting": len(owed) - undelivered, "undelivered": undelivered}


def _mark_failed(session, image_id: str, message: str):
    img = session.get(Image, image_id)
    if img is None:
        return
    img.status = ProcessingStatus.FAILED
    img.error_message = message
    session.commit()


def rebase_paths(session, images, dry_run: bool) -> int:
    """Rewrites stale local image paths to the project-relative form; returns how many."""
    healed = 0
    for img in images:
        if is_drive_path(img.image_path):
            continue  # Google Drive images have no local path to rebase
        try:
            real = resolve_image_path(img.image_path)
        except FileNotFoundError:
            continue
        portable = to_stored_path(real)
        if portable != img.image_path:
            print(f"  path rebased  {img.image_id}: {img.image_path} -> {portable}")
            if not dry_run:
                img.image_path = portable
            healed += 1
    if healed and not dry_run:
        session.commit()
    return healed


def _listed_only(images, dry_run: bool, verb: str) -> bool:
    """True when there is nothing to do (no images, or a dry run that lists them)."""
    if dry_run:
        for img in images:
            print(f"  would {verb}  {img.image_id}  ({img.status.value})")
    return not images or dry_run


def republish(images, dry_run: bool) -> int:
    if _listed_only(images, dry_run, "re-publish"):
        return 0

    from kafka.config import KAFKA_TOPIC
    from kafka.producer import get_backend

    backend = get_backend(KAFKA_TOPIC)
    done = 0
    for img in images:
        try:
            if not is_drive_path(img.image_path):  # Drive files are checked by Spark
                resolve_image_path(img.image_path)
        except FileNotFoundError:
            print(f"  SKIP {img.image_id}: image file missing at {img.image_path} -> marking FAILED")
            with SessionLocal() as s:
                _mark_failed(s, img.image_id, f"Image file missing on disk at re-queue time: {img.image_path}")
            continue
        backend.send(build_upload_event(img))
        print(f"  re-published  {img.image_id}")
        done += 1
    print(f"\nRe-published {done} event(s) to Kafka topic '{KAFKA_TOPIC}'.")
    print("A Spark consumer (python -m spark.streaming_job) must be running to turn these into predictions.")
    return done


def process_inline(images, dry_run: bool) -> int:
    if _listed_only(images, dry_run, "process inline"):
        return 0

    from spark.streaming_job import process_and_persist
    from storage.results import publish_result_to_drive_safely
    from storage.duplicates import resend_duplicates_of

    done = 0
    for img in images:
        event = build_upload_event(img)
        result, status, error = process_and_persist(event)
        if status == ProcessingStatus.COMPLETED:
            print(f"  processed  {img.image_id}  -> {result['predicted_class']} "
                  f"(risk {result['risk_level']} {result['risk_score']})")
            done += 1
        publish_result_to_drive_safely(event, status, error,
                                       processed_image=(result or {}).get("processed_image_jpeg"))
    print(f"\nProcessed inline: {done} completed, {len(images) - done} failed.")
    if resent := resend_duplicates_of([img.image_id for img in images]):   # it picks the FAILED ones
        print(f"Sent {resent} copy/copies of failed photo(s) to Kafka for their own result.")
    return done


def resend_owed(older_than_seconds: int, limit: int, backend=None) -> int:
    """Re-sends up to `limit` photos owed a result for `older_than_seconds`
    (the Spark job runs this by itself). Skips undelivered results while Drive
    is down. Returns how many were sent."""
    from storage.pipeline_submit import submit_all
    from storage import drive_client
    with SessionLocal() as session:
        owed = find_stuck(session, older_than_seconds, include_failed=False, ignore_age=False)
        owed.sort(key=lambda i: i.ingested_at or i.uploaded_at)        # uploaded_at is never empty
        if any(i.status == ProcessingStatus.COMPLETED for i in owed):
            try:
                drive_client.account_info()
            except Exception:  # noqa: BLE001 - Drive down: deliver those later
                owed = [i for i in owed if i.status != ProcessingStatus.COMPLETED]
        return submit_all(session, owed[:limit], backend, "[requeue_stuck]")


def republish_from_drive(dry_run: bool) -> int:
    """Re-publishes Drive photos that have no database row yet."""
    from kafka.config import KAFKA_TOPIC
    from kafka.producer import get_backend
    from storage import drive_inventory
    with SessionLocal() as session:
        photos = drive_inventory.unprocessed_photos(session)
    if not photos:
        print("Every photo in the farmers' Drive folders is registered. Nothing to do.")
        return 0
    print(f"Found {len(photos)} photo(s) in Google Drive with no database row:")
    for meta, folder_email in photos:
        print(f"  {(meta.get('appProperties') or {}).get('image_id')}  ({folder_email})")
    if dry_run:
        return 0
    sent, failed = drive_inventory.send_for_processing(photos, get_backend(KAFKA_TOPIC))
    print(f"\nPublished {sent} drive_upload event(s) to Kafka topic '{KAFKA_TOPIC}'"
          + (f", {failed} failed." if failed else "."))
    return sent


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--older-than", type=int, default=120,
                   help="only touch rows stuck for at least this many seconds (default: 120)")
    p.add_argument("--all", action="store_true",
                   help="ignore --older-than; act on every stuck row regardless of age")
    p.add_argument("--include-failed", action="store_true",
                   help="also retry rows that FAILED at the Kafka-publish step")
    p.add_argument("--inline", action="store_true",
                   help="process now via spark.preprocessing instead of re-publishing to Kafka")
    p.add_argument("--dry-run", action="store_true",
                   help="list what would be done and exit")
    p.add_argument("--from-drive", action="store_true",
                   help="re-publish the Drive photos that have no database row (lost events / MANUAL mode)")
    args = p.parse_args()
    init_db()

    if args.from_drive:
        republish_from_drive(args.dry_run)
        return 0

    with SessionLocal() as session:
        summary = [(i.image_id, i.status.value + (" (result not in Drive yet)"
                                                  if i.status == ProcessingStatus.COMPLETED else ""))
                   for i in find_stuck(session, args.older_than, args.include_failed, args.all)]

    if not summary:
        print("No stuck images found. Nothing to do.")
        return 0

    print(f"Found {len(summary)} stuck image(s):")
    for image_id, status in summary:
        print(f"  {image_id}  {status}")
    print()

    ids = [iid for iid, _ in summary]

    with SessionLocal() as session:
        images = [session.get(Image, iid) for iid in ids]
        if healed := rebase_paths(session, images, args.dry_run):
            print(f"  ({healed} stale image_path value(s) rebased to this project)\n")
        if args.inline:
            process_inline(images, args.dry_run)
        else:
            republish(images, args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
