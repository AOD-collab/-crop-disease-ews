"""
Reconciliation / re-queue tool for images stranded between ingestion and
processing.

Background
----------
`storage/pipeline_submit.py` marks an image `INGESTED` the moment its upload
event is published to Kafka. Turning that into a prediction (`COMPLETED`) is a
separate process — `spark/streaming_job.py`. If the Spark consumer is not
running when the event is published (crash, never started, killed by
`run_all.py --stop`), or if the Kafka topic is wiped underneath a stale
Spark checkpoint, the image sits at `INGESTED` forever with no event left
on the transport to replay.

This tool finds those rows and gets them unstuck. Two modes:

    (default)  re-publish the upload event to Kafka via kafka.producer.
               Use this when a Spark consumer IS running (or is about to
               be started with an empty/fresh checkpoint) — the event goes
               back on the topic and Spark processes it normally.

    --inline   process the image here and now, in-process, using the exact
               same spark/preprocessing.process_event + persist_result the
               Spark foreachBatch sink calls. No broker, no Spark JVM, no
               checkpoint involved. Use this for a one-shot backfill of
               events that are genuinely gone from Kafka.

Rows touched: non-terminal ones (INGESTED, PROCESSING — the
last is a photo Spark stopped in the middle of), COMPLETED Google Drive
photos whose result never reached the farmer's Drive folder
(result_published_at is empty: the Drive upload failed after the result was
saved), and — with --include-failed — rows that FAILED specifically at the
Kafka-publish step or at finding the file. Re-sending any of them is safe:
saving a prediction again replaces the old one (persist_result), and Spark
uploads the result to Drive again.

Examples
--------
    python -m storage.requeue_stuck --dry-run
    python -m storage.requeue_stuck --all --inline        # backfill everything now
    python -m storage.requeue_stuck --older-than 300        # re-publish rows stuck > 5 min
    python -m storage.requeue_stuck --all --include-failed  # also retry Kafka-publish failures
    python -m storage.requeue_stuck --from-drive            # Drive photos Spark never registered

The database only has a row for a photo Spark registered (spark/ingestion.py),
so a photo whose drive_upload event never reached Spark is found by
--from-drive (Drive minus database, storage/drive_inventory.py), not by the
row-based modes above.
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

# Statuses that mean "the pipeline still owes this image a result".
# PROCESSING: Spark stopped in the middle of the photo; its Kafka event may be
# gone (topic wiped / checkpoint reset), so nothing would ever finish it.
STUCK_STATUSES = {ProcessingStatus.INGESTED, ProcessingStatus.PROCESSING}

# FAILED rows worth retrying with --include-failed: the failure was in the
# transport or in locating the file, not in the image itself — a retry
# (plus path rebasing) can legitimately still produce a prediction.
_RETRYABLE_FAILURE_MARKERS = (
    "Kafka publish failed",
    "Image file not found on disk",
    "Image file missing on disk",
    "Image file not found in Google Drive",
)


def result_not_delivered():
    """SQL condition: a COMPLETED Google Drive photo whose result never
    reached the farmer's Drive folder. spark/preprocessing.
    publish_result_to_drive_safely only logs a Drive failure, so without
    this the phone would wait for that result forever."""
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
    # Filtered in the database: this used to load every image row and test
    # each one in Python.
    rows = session.query(Image).filter(_not_scale_test(), or_(*eligible)).all()
    out = []
    for img in rows:
        if not ignore_age:
            if img.status == ProcessingStatus.COMPLETED:
                stamp = img.processed_at or img.ingested_at   # its Drive upload may still be running
            else:
                stamp = img.ingested_at or img.uploaded_at or img.created_at
            # stored naive-UTC in SQLite; compare on the same basis
            stamp_cmp = stamp.replace(tzinfo=cutoff.tzinfo) if stamp and stamp.tzinfo is None else stamp
            if stamp_cmp and stamp_cmp > cutoff:
                continue
        out.append(img)
    return out


def count_owed(session, older_than_seconds: int = 120) -> dict:
    """{"waiting": photos the pipeline still owes a result, "undelivered":
    COMPLETED Drive photos whose result never reached Drive}, counting only
    rows older than `older_than_seconds` — what find_stuck would pick up."""
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
    """Heal stale image_path values (project moved between hosts/folders):
    if the stored path can be resolved to a real file, rewrite it to the
    portable project-relative form. Returns how many rows were healed.
    `images` must be live objects on `session`."""
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
    """True when there is nothing to do: no images, or a dry run (which
    lists what it would `verb`)."""
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

    from spark.preprocessing import (
        process_event, persist_result, InvalidEvent, publish_result_to_drive_safely, resend_duplicates_of,
    )

    done = 0
    failed = 0
    for img in images:
        event = build_upload_event(img)
        try:
            result = process_event(event)
            persist_result(event, result, ProcessingStatus.COMPLETED)
            publish_result_to_drive_safely(
                event, ProcessingStatus.COMPLETED, processed_image=result.get("processed_image_jpeg"),
            )
            print(f"  processed  {img.image_id}  -> {result['predicted_class']} "
                  f"(risk {result['risk_level']} {result['risk_score']})")
            done += 1
        except Exception as e:                     # an InvalidEvent too, only labelled differently
            print(f"  {'FAILED (invalid)' if isinstance(e, InvalidEvent) else 'FAILED'}  {img.image_id}: {e}")
            failed += 1
            try:
                persist_result(event, None, ProcessingStatus.FAILED, error_message=str(e))
                publish_result_to_drive_safely(event, ProcessingStatus.FAILED, str(e))
            except Exception as db_error:  # noqa: BLE001 - keep going with the other photos
                print(f"  could not record the failure of {img.image_id}: {db_error}")
    print(f"\nProcessed inline: {done} completed, {failed} failed.")
    with SessionLocal() as s:
        ids = [img.image_id for img in images
               if (row := s.get(Image, img.image_id)) is not None and row.status == ProcessingStatus.FAILED]
    if resent := resend_duplicates_of(ids):
        print(f"Sent {resent} copy/copies of failed photo(s) to Kafka for their own result.")
    return done


def republish_from_drive(dry_run: bool) -> int:
    """Photos in a farmer's Drive images/<email>/ folder that Spark never
    registered (no `images` row): their Kafka event was lost, or they were
    uploaded in MANUAL mode and not sent. Re-publishes the Drive watcher's
    drive_upload event for each. Returns how many were sent."""
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
        # a lightweight summary, so the session isn't held open during processing
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
        live = [session.get(Image, iid) for iid in ids]
        healed = rebase_paths(session, live, args.dry_run)
    if healed:
        print(f"  ({healed} stale image_path value(s) rebased to this project)\n")

    with SessionLocal() as session:
        images = [session.get(Image, iid) for iid in ids]
        if args.inline:
            process_inline(images, args.dry_run)
        else:
            republish(images, args.dry_run)

    return 0


if __name__ == "__main__":
    sys.exit(main())
