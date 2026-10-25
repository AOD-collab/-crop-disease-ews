"""
One photo's result, after the CNN has classified it: the replay check
before processing, saving the Prediction (+ ProcessingEvent) in the
database, and writing the result into the farmer's Drive results folder
for the Android app (Spark -> Drive -> Android).
"""

import json
import threading
from datetime import datetime, timezone

from config.localtime import format_local
from storage import drive_client
from storage.database import (
    SessionLocal, Image, Prediction, ProcessingStatus, ProcessingEvent,
    RejectedEvent, User, ScaleTestItem, ScaleTestRun, SCALE_TEST_SOURCE,
)
from storage.exif_utils import GPS_COLUMNS
from storage.image_storage import is_drive_path, drive_file_id_from_path

# The CNN's image features, and every Prediction column a result carries.
FEATURE_FIELDS = ("lesion_score", "discoloration_pct", "texture_irregularity", "leaf_area_affected_pct")
PREDICTION_FIELDS = ("predicted_class", "prediction_confidence", *FEATURE_FIELDS,
                     "risk_score", "risk_level", "model_version", "predicted_at")


def as_aware_utc(dt):
    """SQLite has no native timezone-aware datetime type, so a value stored
    as UTC and read back through SQLAlchemy loses its tzinfo (PostgreSQL
    preserves it correctly via TIMESTAMPTZ). Every value written by this
    codebase IS already UTC (config/settings.now_utc / datetime.now(utc)),
    so a naive value read back is safely re-attached as UTC rather than
    assumed to be local time."""
    return dt.replace(tzinfo=timezone.utc) if dt is not None and dt.tzinfo is None else dt


def replay_state(image_id: str):
    """The Spark sink's replay check, in ONE database lookup per event
    (~3 ms on the Docker PostgreSQL). Returns:
      "completed"      - already has its result; a replay is skipped. Asking the
                         database (instead of Spark's old dropDuplicates) still
                         lets a real Reprocess through: its status was reset.
      "result_missing" - a COMPLETED Drive photo whose result never reached
                         Drive (Spark stopped in between): upload it now.
      None             - process it."""
    if not image_id:
        return None
    with SessionLocal() as session:
        image = session.get(Image, image_id)
        if image is None:
            return None
        if image.status == ProcessingStatus.COMPLETED:
            if is_drive_path(image.image_path) and image.result_published_at is None:
                return "result_missing"
            return "completed"
        return None


def mark_processing(image_id: str) -> None:
    """Flips an Image to PROCESSING right before inference starts, so a
    long-running CNN call is visible in the dashboard as in-flight rather
    than silently still showing INGESTED. Best-effort: an unknown image_id
    is a no-op (persist_result's own guard handles that case properly)."""
    _update_image(image_id, status=ProcessingStatus.PROCESSING)


def _update_image(image_id: str, **fields) -> None:
    """Sets `fields` on one Image row and commits; an unknown image_id is a no-op."""
    with SessionLocal() as session:
        image = session.get(Image, image_id)
        if image is not None:
            for name, value in fields.items():
                setattr(image, name, value)
            session.commit()


def persist_result(
    event: dict,
    result: dict,
    status: ProcessingStatus,
    error_message: str = None,
    *,
    kafka_received_at=None,
    spark_started_at=None,
):
    """Writes a Prediction (+ ProcessingEvent) row and updates the parent
    Image's status/timestamps. Used by spark/streaming_job.py (Spark's
    foreachBatch sink); kept in this module — which only depends on
    SQLAlchemy, not pyspark — so the persistence logic is isolated from the
    Spark runtime and directly unit-testable.

    `kafka_received_at`/`spark_started_at` are the genuine Spark-side
    timestamps (captured by foreach_batch_sink before calling
    process_event); if omitted (e.g. a direct unit-test call) they default
    to "now" so this function's own behavior stays self-contained.

    Idempotent on image_id for the Prediction row: Structured Streaming's
    checkpoint only guarantees at-least-once delivery to foreachBatch (see
    spark/streaming_job.py docstring), so a crash-and-replay can call this
    twice for the same event. Any existing Prediction for this image_id is
    deleted before the new one is inserted, so a replay overwrites rather
    than duplicates. ProcessingEvent rows are an append-only audit log and
    are deliberately NOT deduped — a replay is itself another processing
    attempt worth recording.

    If image_id doesn't correspond to any Image row at all (a malformed or
    corrupted event, or one referencing a since-deleted image), nothing was
    previously recorded anywhere except a stdout print() — now a
    RejectedEvent row is written instead, so the event leaves a trace."""
    with SessionLocal() as session:
        image_id = event.get("image_id")
        image = session.get(Image, image_id) if image_id else None
        if image is None:
            session.add(RejectedEvent(
                image_id=image_id,
                raw_event=json.dumps(event, default=str),
                reason=error_message or "No matching Image row for this image_id",
            ))
            session.commit()
            return

        now = datetime.now(timezone.utc)
        kafka_received_at = as_aware_utc(kafka_received_at) or now
        spark_started_at = as_aware_utc(spark_started_at) or now
        spark_completed_at = datetime.now(timezone.utc)

        if image.ingested_at is None:
            image.ingested_at = now
        if "location" in event:
            # Spark's cleaned location (parse_events / clean_event), so a row
            # registered before the cleaning rule existed is corrected too.
            image.location = event["location"]
        image.processed_at = now
        image.status = status
        image.error_message = error_message

        if result is not None:
            ingested_at = as_aware_utc(image.ingested_at)
            latency_ms = int((result["predicted_at"] - ingested_at).total_seconds() * 1000) if ingested_at else None

            # Delete-then-insert: makes a replayed event idempotent instead
            # of piling up a second Prediction row for the same image.
            session.query(Prediction).filter(Prediction.image_id == image_id).delete()

            session.add(Prediction(image_id=image_id, is_uncertain=result.get("is_uncertain", False),
                                   processing_latency_ms=latency_ms, **{k: result[k] for k in PREDICTION_FIELDS}))
            if result["risk_level"] == "HIGH":
                print(f"[ALERT] image={image_id} location={event.get('location')} "
                      f"risk={result['risk_score']} class={result['predicted_class']}")

        total_latency_ms = int((spark_completed_at - kafka_received_at).total_seconds() * 1000)

        session.add(ProcessingEvent(
            image_id=image_id,
            api_published_at=image.ingested_at,
            kafka_received_at=kafka_received_at,
            spark_started_at=spark_started_at,
            spark_completed_at=spark_completed_at,
            prediction_started_at=result.get("prediction_started_at") if result else None,
            prediction_completed_at=result["predicted_at"] if result else None,
            total_latency_ms=total_latency_ms,
            status=status,
            error_message=error_message,
        ))
        session.commit()


# ---------------------------------------------------------------------------
# Spark -> Google Drive /results
# ---------------------------------------------------------------------------
def _prediction_to_dict(p: Prediction) -> dict:
    """The prediction fields of a Drive result file, in the shape the Android
    app parses (DriveRest.kt)."""
    return {
        "prediction_id": p.prediction_id, "image_id": p.image_id,
        **{k: getattr(p, k) for k in PREDICTION_FIELDS},
        "risk_level": p.risk_level.value if p.risk_level else None,
        "predicted_at": p.predicted_at.isoformat() if p.predicted_at else None,
        "is_uncertain": p.is_uncertain, "processing_latency_ms": p.processing_latency_ms,
    }


def publish_result_to_drive(event: dict, status: ProcessingStatus, error_message: str = None,
                            processed_image: bytes = None):
    """Writes the result for one image into the uploader's private Drive
    folder /results/<email>/ (shared only with them):
        <image_id>.json            the result, built from what persist_result()
                                   just stored, so Drive and database agree
        <image_id>_processed.jpg   the marked-up leaf (when `processed_image`
                                   is given), shown in the app
    Only for Google Drive images; returns the JSON file id, or None for
    legacy local-disk images. Re-processing the same image overwrites the
    same two files (drive_client.upload_bytes), matching persist_result's
    idempotency."""
    image_id = event.get("image_id")
    test_folder = None
    with SessionLocal() as session:
        image = session.get(Image, image_id) if image_id else None
        if image is None:
            return None
        if image.source == SCALE_TEST_SOURCE:
            # Scalability test: only when its run publishes results, and into
            # that run's own Drive folder — never a farmer's results folder.
            item = session.get(ScaleTestItem, image_id)
            run = session.get(ScaleTestRun, item.run_id) if item else None
            if run is None or not run.publish_results or not run.drive_results_folder_id:
                return None
            test_folder = run.drive_results_folder_id
            drive_image_file_id = item.source_drive_file_id
        elif not is_drive_path(image.image_path):
            return None
        else:
            drive_image_file_id = drive_file_id_from_path(image.image_path)
        prediction = (
            session.query(Prediction).filter(Prediction.image_id == image_id)
            .order_by(Prediction.predicted_at.desc()).first()
        )
        owner = session.get(User, image.user_id)
        owner_email = owner.email if owner else ""
        payload = {
            "image_id": image.image_id,
            "user_id": image.user_id,
            "drive_image_file_id": drive_image_file_id,
            "processed_file_id": None,
            "location": image.location,
            "captured_at": image.captured_at.isoformat() if image.captured_at else None,
            "uploaded_at": image.uploaded_at.isoformat() if image.uploaded_at else None,
            # same two times, ready to show (Timor-Leste Time)
            "captured_at_local": format_local(image.captured_at) or None,
            "uploaded_at_local": format_local(image.uploaded_at) or None,
            **{c: getattr(image, c) for c in GPS_COLUMNS + ("gps_source", "camera_make", "camera_model")},
            "status": status.value,
            "error_message": error_message,
            "prediction": _prediction_to_dict(prediction) if (prediction and status == ProcessingStatus.COMPLETED) else None,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

    folder = test_folder or drive_client.user_folder_id(
        owner_email, drive_client.USER_RESULTS_KIND, create=True, share_role="reader"
    )
    if processed_image and status == ProcessingStatus.COMPLETED:
        name = f"{image_id}_processed.jpg"
        payload["processed_file_id"] = _RESULT_FILES.upload(folder, name, lambda existing: drive_client.upload_bytes(
            name, processed_image, "image/jpeg", folder,
            {"image_id": image_id, "user_email": owner_email, "kind": "processed"},
            may_exist=False, existing_id=existing))
    pred = payload["prediction"] or {}
    # Short summary in Drive appProperties: lets the Android app list all
    # of its results (Home / History) with ONE Drive request, without
    # downloading every JSON file.
    summary = {
        "image_id": image_id,
        "user_id": payload["user_id"],
        "user_email": owner_email,
        "kind": "result",
        "status": status.value,
        "drive_image_file_id": payload["drive_image_file_id"],
        **{k: payload[k] or "" for k in ("processed_file_id", "location", "uploaded_at")},
        **{k: pred.get(k) or "" for k in ("predicted_class", "risk_level")},
        **{k: pred.get(k) if pred else "" for k in ("prediction_confidence", "risk_score")},
        "error_message": (error_message or "")[:80],
    }
    name = f"{image_id}.json"
    file_id = _RESULT_FILES.upload(folder, name, lambda existing: drive_client.upload_json(
        name, payload, folder, summary, may_exist=False, existing_id=existing))
    _update_image(image_id, result_published_at=datetime.now(timezone.utc))   # this result is in Drive
    return file_id


class _ResultFolderIndex:
    """name -> file id of the result files already in each results folder,
    listed from Drive ONCE per folder per Spark process (one list request
    per 100 files) and kept up to date as Spark writes. A result is then
    updated in place when it exists and created only when it doesn't, with
    no "does it exist?" search per file.

    It replaced a guess from the database ("first processing attempt ->
    can't be in Drive yet"), which a factory reset made wrong: the database
    forgot the photos but Drive still had their results, and 1,163 duplicate
    result files were created on 2026-10-09. Only Spark writes into these
    folders, and the factory reset (which trashes them) runs with Spark
    stopped, so the index can't go stale behind its back."""

    def __init__(self):
        self._folders = {}
        self._lock = threading.Lock()
        self._loading = {}

    def _index(self, folder: str) -> dict:
        with self._lock:
            if folder in self._folders:
                return self._folders[folder]
            loading = self._loading.setdefault(folder, threading.Lock())
        with loading:                          # one listing per folder, even with 8 threads
            with self._lock:
                if folder in self._folders:
                    return self._folders[folder]
            # oldest first: the newest copy of a name wins
            names = {f["name"]: f["id"] for f in drive_client.list_files_in_folders([folder])}
            with self._lock:
                self._folders[folder] = names
            return names

    def existing(self, folder: str, name: str):
        return self._index(folder).get(name)

    def remember(self, folder: str, name: str, file_id: str) -> str:
        with self._lock:
            self._folders.setdefault(folder, {})[name] = file_id
        return file_id

    def upload(self, folder: str, name: str, send) -> str:
        """send(existing_id) writes `name` into `folder` (in place when the
        folder already has it); the file id it returns is remembered."""
        return self.remember(folder, name, send(self.existing(folder, name)))

    def clear(self):
        with self._lock:
            self._folders.clear()


_RESULT_FILES = _ResultFolderIndex()


def publish_result_to_drive_safely(event: dict, status: ProcessingStatus, error_message: str = None,
                                   processed_image: bytes = None):
    """publish_result_to_drive, but a Drive outage only logs: the result is
    already safe in the database (the dashboard still shows it), and
    re-queuing the image writes it to Drive again."""
    try:
        return publish_result_to_drive(event, status, error_message, processed_image)
    except Exception as e:
        print(f"[results] Could not write result to Google Drive for {event.get('image_id')}: {e}")
        return None
