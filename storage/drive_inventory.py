"""
Farmer photos in Drive not sent for processing yet: photos with the app's
image_id tag and no `images` row (Pending Review, switch to AUTOMATIC,
requeue_stuck --from-drive).
"""

from kafka.drive_watcher import _UUID_RE, build_drive_event
from storage import drive_client
from storage.database import Image

_CHUNK = 500


def farmer_photos() -> list:
    """(metadata, folder email) of every farmer photo in Drive, oldest first."""
    folders = drive_client.user_image_folders()
    if not folders:
        return []
    out = []
    for meta in drive_client.list_files_in_folders(list(folders)):
        parent = next((p for p in meta.get("parents") or [] if p in folders), None)
        out.append((meta, folders.get(parent)))
    return out


def uploaded_at(meta: dict):
    """Drive's createdTime as a datetime, or None."""
    from datetime import datetime
    try:
        return datetime.fromisoformat(str(meta.get("createdTime")).replace("Z", "+00:00"))
    except ValueError:
        return None


def _image_id(meta: dict) -> str:
    return (meta.get("appProperties") or {}).get("image_id") or ""


def unprocessed_photos(session) -> list:
    """The app's photos in Drive that have no `images` row yet."""
    photos = [(m, f) for m, f in farmer_photos() if _UUID_RE.match(_image_id(m))]
    ids = [_image_id(m) for m, _ in photos]
    known = set()
    for i in range(0, len(ids), _CHUNK):
        chunk = ids[i:i + _CHUNK]
        known.update(r[0] for r in session.query(Image.image_id).filter(Image.image_id.in_(chunk)))
    return [(m, f) for m, f in photos if _image_id(m) not in known]


def send_for_processing(photos: list, backend) -> tuple:
    """Publishes a drive_upload event for each; returns (sent, failed)."""
    sent = failed = 0
    for meta, folder_email in photos:
        try:
            backend.send(build_drive_event(meta, folder_email))
            sent += 1
        except Exception as e:  # noqa: BLE001 - keep sending the rest
            print(f"[drive_inventory] could not send {_image_id(meta)}: {e}")
            failed += 1
    return sent, failed
