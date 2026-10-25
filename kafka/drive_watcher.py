"""
Drive -> Kafka: watches every farmer's Drive /images/<email>/ folder and
publishes one Kafka event per new photo (metadata only, never the image).
No database. In MANUAL mode nothing is published.

Run: python -m kafka.drive_watcher [--once]
"""
import argparse
import json
import os
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import BASE_DIR, KAFKA_TOPIC  # noqa: E402
from storage import drive_client  # noqa: E402
from storage.image_storage import to_drive_path  # noqa: E402
from storage.processing_mode import ProcessingMode, get_processing_mode  # noqa: E402

# one change-feed request per check: under 2% of the Drive quota at 1 s
POLL_SECONDS = float(os.getenv("DRIVE_WATCH_INTERVAL_SECONDS", "1"))        # while uploads arrive
IDLE_POLL_SECONDS = float(os.getenv("DRIVE_WATCH_IDLE_SECONDS", "3"))       # slowest, when quiet
FULL_SCAN_SECONDS = float(os.getenv("DRIVE_WATCH_FULL_SCAN_SECONDS", "600"))  # safety-net folder search
STATE_FILE = Path(os.getenv("DRIVE_WATCHER_STATE_FILE", str(BASE_DIR / "storage" / "drive_watcher_state.json")))
# re-scan a little before the last seen file, in case Drive lists it late
OVERLAP = timedelta(minutes=2)

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# Drive files already published by this process (both scans can report one)
_PUBLISHED: "OrderedDict[str, None]" = OrderedDict()
_PUBLISHED_MAX = 20000


def _device_gps(props: dict):
    """The phone's (lat, lon) from appProperties, or None if missing or invalid."""
    try:
        lat, lon = float(props["gps_lat"]), float(props["gps_lon"])
    except (KeyError, TypeError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
        return None
    return lat, lon


def _parse_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def build_drive_event(meta: dict, folder_email: str = None) -> dict:
    """The Kafka event for one Drive photo, from its metadata. All values are text or None."""
    props = meta.get("appProperties") or {}
    owners = meta.get("owners") or []
    owner = owners[0] if owners else {}
    file_id = meta.get("id") or ""
    captured_at = _parse_time(props.get("captured_at_client"))
    gps = _device_gps(props)
    return {
        "kind": "drive_upload",
        "image_id": props.get("image_id") or None,
        "user_id": None,                                   # found from owner_email
        "drive_file_id": file_id,
        "image_path": to_drive_path(file_id),
        "file_name": meta.get("name"),
        "mime_type": meta.get("mimeType"),
        "size_bytes": str(meta["size"]) if meta.get("size") is not None else None,
        "owner_email": owner.get("emailAddress"),
        "owner_name": owner.get("displayName"),
        "folder_email": folder_email,
        "uploaded_at": meta.get("createdTime"),            # Google's clock, not the phone's
        "captured_at": captured_at.isoformat() if captured_at else None,
        "location": props.get("location"),
        "original_filename": props.get("original_filename") or meta.get("name"),
        "content_hash": meta.get("sha256Checksum"),
        "device_gps_lat": f"{gps[0]}" if gps else None,
        "device_gps_lon": f"{gps[1]}" if gps else None,
    }


def publish_drive_file(meta: dict, backend=None, folder_email: str = None) -> dict:
    """Publishes one Drive photo. Status: published | held (MANUAL) | already_published."""
    file_id = meta.get("id", "")
    image_id = (meta.get("appProperties") or {}).get("image_id") or None
    if file_id in _PUBLISHED:
        return {"image_id": image_id, "status": "already_published"}
    if get_processing_mode() == ProcessingMode.MANUAL:
        return {"image_id": image_id, "status": "held"}
    event = build_drive_event(meta, folder_email)
    backend.send(event)
    _PUBLISHED[file_id] = None
    while len(_PUBLISHED) > _PUBLISHED_MAX:
        _PUBLISHED.popitem(last=False)
    print(f"[drive_watcher] new photo {(image_id or '?')[:8]} from {event['owner_email']} "
          f"({event['location'] or 'no location'}) -> Kafka topic '{KAFKA_TOPIC}' (Drive file {file_id})")
    return {"image_id": image_id, "status": "published"}


def _publish_safely(meta: dict, backend, folder_email) -> dict:
    """publish_drive_file that never raises, so one bad file doesn't stop the scan."""
    try:
        return publish_drive_file(meta, backend=backend, folder_email=folder_email)
    except Exception as e:  # noqa: BLE001 - this file only
        image_id = (meta.get("appProperties") or {}).get("image_id") or None
        print(f"[drive_watcher] Could not publish Drive file {meta.get('id', '')}: "
              f"{type(e).__name__}: {str(e).splitlines()[0][:300]}")
        return {"image_id": image_id, "status": "error"}


def _load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state))


def scan_once(backend=None) -> list:
    """Full folder search for files since the last scan; publishes the new ones."""
    state = _load_state()
    created_after = None
    last = _parse_time(state.get("last_created_time"))
    if last is not None:
        created_after = (last - OVERLAP).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    user_folders = drive_client.user_image_folders()
    if not user_folders:
        return []
    files = drive_client.list_files_in_folders(list(user_folders), created_after=created_after)
    outcomes = []
    for meta in files:
        parent = next((p for p in meta.get("parents") or [] if p in user_folders), None)
        outcomes.append(_publish_safely(meta, backend, user_folders.get(parent)))

    newest = max((f.get("createdTime") for f in files if f.get("createdTime")), default=None)
    if newest and (last is None or _parse_time(newest) > last):
        state["last_created_time"] = newest
        _save_state(state)
    return outcomes


class NoBookmark(RuntimeError):
    """No change-feed bookmark yet (first start, or after a factory reset)."""


def scan_changes(user_folders: dict, backend=None) -> list:
    """Publishes new photos from Drive's change feed (one request). Adds new
    farmer folders to `user_folders`."""
    state = _load_state()
    token = state.get("changes_page_token")
    if not token:
        raise NoBookmark("no change-feed bookmark yet — start_change_feed() first")
    changes, new_token = drive_client.list_changes(token)
    images_root = drive_client.images_folder_id()
    outcomes = []
    for change in changes:
        meta = change.get("file") or {}
        if change.get("removed") or not meta or meta.get("trashed"):
            continue
        parents = meta.get("parents") or []
        if meta.get("mimeType") == drive_client.FOLDER_MIME_TYPE:
            if images_root in parents:                       # a new farmer
                user_folders[meta["id"]] = meta.get("name")
            continue
        parent = next((p for p in parents if p in user_folders), None)
        if parent is None:                                   # not a farmer photo
            continue
        outcomes.append(_publish_safely(meta, backend, user_folders[parent]))
    state["changes_page_token"] = new_token
    _save_state(state)
    return outcomes


def start_change_feed(backend=None) -> dict:
    """Bookmarks the change feed (if needed), then one full scan to catch up.
    Returns {folder id: email}."""
    state = _load_state()
    if not state.get("changes_page_token"):
        state["changes_page_token"] = drive_client.get_start_page_token()
        _save_state(state)
    scan_once(backend=backend)
    return drive_client.user_image_folders()


def next_interval(current: float, found_new: bool) -> float:
    """Fast while photos arrive; slows down to IDLE_POLL_SECONDS when quiet."""
    if found_new:
        return POLL_SECONDS
    return min(IDLE_POLL_SECONDS, current * 1.5)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--once", action="store_true", help="scan once and exit")
    args = p.parse_args()

    from kafka.producer import get_backend
    backend = get_backend(KAFKA_TOPIC)
    if args.once:
        try:
            print(f"[drive_watcher] {scan_once(backend=backend)}")
            return 0
        except drive_client.DriveNotConfigured as e:
            print(f"[drive_watcher] {e}")
            return 1

    print(f"[drive_watcher] Following Google Drive's change feed for new photos in every farmer's "
          f"/images/<email>/ folder (every {POLL_SECONDS}s while uploads arrive, up to "
          f"{IDLE_POLL_SECONDS}s when quiet) -> Kafka '{KAFKA_TOPIC}'")
    user_folders, interval, last_full_scan = None, POLL_SECONDS, time.monotonic()
    while True:
        found_new = False
        try:
            # forward what was queued locally while Kafka was down
            if hasattr(backend, "drain"):
                backend.drain()
            if user_folders is None:
                user_folders = start_change_feed(backend=backend)
                last_full_scan = time.monotonic()
            outcomes = scan_changes(user_folders, backend=backend)
            if time.monotonic() - last_full_scan >= FULL_SCAN_SECONDS:
                # safety net for anything the change feed missed
                outcomes += scan_once(backend=backend)
                last_full_scan = time.monotonic()
            new = [o for o in outcomes if o["status"] == "published"]
            found_new = bool(new)
            if new:
                print(f"[drive_watcher] {len(new)} new file(s) published")
        except drive_client.DriveNotConfigured as e:
            print(f"[drive_watcher] {e}")
            return 1
        except NoBookmark:
            print("[drive_watcher] Bookmark gone (factory reset?) — publishing every photo in Drive again")
            _PUBLISHED.clear()
            user_folders, found_new = None, True
        except Exception as e:  # network hiccup etc. — keep watching
            print(f"[drive_watcher] scan failed, retrying: {e}")
        interval = next_interval(interval, found_new)
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
