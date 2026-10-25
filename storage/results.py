"""
One photo's result after the CNN classified it: the replay check, saving it
in the database, and writing it to the farmer's Drive results folder.
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

# the CNN's image features, and every Prediction column a result carries
FEATURE_FIELDS = ("lesion_score", "discoloration_pct", "texture_irregularity", "leaf_area_affected_pct")
PREDICTION_FIELDS = ("predicted_class", "prediction_confidence", *FEATURE_FIELDS,
                     "risk_score", "risk_level", "model_version", "predicted_at")


def as_aware_utc(dt):
    """Treats a naive datetime (SQLite) as UTC; everything is stored in UTC."""
    return dt.replace(tzinfo=timezone.utc) if dt is not None and dt.tzinfo is None else dt


def replay_state(image_id: str):
    """Replay check: "completed" (skip), "result_missing" (COMPLETED but not
    in Drive yet: upload again) or None (process it)."""
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
    """Marks the image PROCESSING, so the dashboard shows it in progress."""
    _update_image(image_id, status=ProcessingStatus.PROCESSING)


def _update_image(image_id: str, **fields) -> None:
    """Sets `fields` on one Image row; unknown image_id = no-op."""
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
    """Saves the result: replaces the image's prediction (a replayed event
    overwrites, never duplicates), updates its status and adds a
    ProcessingEvent. An unknown image_id is saved in rejected_events."""
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
            # Spark's cleaned location
            image.location = event["location"]
        image.processed_at = now
        image.status = status
        image.error_message = error_message

        if result is not None:
            ingested_at = as_aware_utc(image.ingested_at)
            latency_ms = int((result["predicted_at"] - ingested_at).total_seconds() * 1000) if ingested_at else None

            # delete-then-insert: a replay replaces the prediction
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
    """Prediction fields in the shape the Android app reads."""
    return {
        "prediction_id": p.prediction_id, "image_id": p.image_id,
        **{k: getattr(p, k) for k in PREDICTION_FIELDS},
        "risk_level": p.risk_level.value if p.risk_level else None,
        "predicted_at": p.predicted_at.isoformat() if p.predicted_at else None,
        "is_uncertain": p.is_uncertain, "processing_latency_ms": p.processing_latency_ms,
    }


def publish_result_to_drive(event: dict, status: ProcessingStatus, error_message: str = None,
                            processed_image: bytes = None):
    """Writes <image_id>.json (and <image_id>_processed.jpg) into the farmer's
    /results/<email>/ folder. Returns the JSON file id, or None for a local image."""
    image_id = event.get("image_id")
    test_folder = None
    with SessionLocal() as session:
        image = session.get(Image, image_id) if image_id else None
        if image is None:
            return None
        if image.source == SCALE_TEST_SOURCE:
            # scalability test: only when the run publishes results, into its own folder
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
            # the same two times, in Timor-Leste Time
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
    # short summary in appProperties: the app lists all results with one request
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
    """name -> file id of the files already in each results folder, listed once
    per folder, so a result is updated in place instead of duplicated."""

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
        """send(existing_id) writes `name`; the returned file id is remembered."""
        return self.remember(folder, name, send(self.existing(folder, name)))

    def clear(self):
        with self._lock:
            self._folders.clear()


_RESULT_FILES = _ResultFolderIndex()


def publish_result_to_drive_safely(event: dict, status: ProcessingStatus, error_message: str = None,
                                   processed_image: bytes = None):
    """publish_result_to_drive; a Drive error only logs (the result is saved)."""
    try:
        return publish_result_to_drive(event, status, error_message, processed_image)
    except Exception as e:
        print(f"[results] Could not write result to Google Drive for {event.get('image_id')}: {e}")
        return None
