"""
Google Drive -> Kafka producer (the "Drive -> Kafka" link of the
architecture). Runs on the laptop/server next to Kafka and Spark.

    Android --upload--> Drive /images/<email>/<image_id>.jpg  (the farmer's own folder)
    THIS SCRIPT: follows the central account's Drive change feed (one
                 request per check, every 1 s while uploads arrive, up to
                 3 s when quiet) for new files in EVERY farmer's
                 /images/<email>/ folder and publishes one Kafka event per
                 photo, built from the file's Drive metadata (never the
                 image bytes). A full folder search (scan_once) runs at
                 start-up and every 10 minutes as a safety net.
    Kafka -> Spark (spark/streaming_job.py) -> Drive /results -> Android

It is the system's only way in for photos: the phone never talks to a
server of ours — only to Google Drive.

Stateless: no database. The watcher only reads Drive and publishes. Spark's
ingestion step (storage/uploads.register_upload) resolves the farmer,
checks the upload, skips duplicates and writes the first database row.
In MANUAL mode (storage/processing_mode.json) nothing is published: the
dashboard's Pending Review page lists the Drive photos that have no
database row yet, and the administrator sends them.

Trust model: the image_id / location / capture time come from the phone
(Drive appProperties), but WHO uploaded the file comes from Google itself
(the file's owner in Drive metadata), so one user can't upload in someone
else's name; the event carries that owner and the folder the photo is in.

Run:
    python -m kafka.drive_watcher           # loop forever (run_all.py starts it)
    python -m kafka.drive_watcher --once    # one scan, then exit
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

# Drive API quota: each check is ONE change-feed request (~100 units of the
# 325,000/minute per-user limit), whatever the number of farmers.
# At 1 s that is ~6,000 units/minute, under 2% of the limit.
POLL_SECONDS = float(os.getenv("DRIVE_WATCH_INTERVAL_SECONDS", "1"))        # while uploads arrive
IDLE_POLL_SECONDS = float(os.getenv("DRIVE_WATCH_IDLE_SECONDS", "3"))       # slowest, when quiet
FULL_SCAN_SECONDS = float(os.getenv("DRIVE_WATCH_FULL_SCAN_SECONDS", "600"))  # safety-net folder search
STATE_FILE = Path(os.getenv("DRIVE_WATCHER_STATE_FILE", str(BASE_DIR / "storage" / "drive_watcher_state.json")))
# Re-scan a small window before the last seen createdTime, so a file whose
# listing appears slightly late in Drive's index is never missed. Files seen
# twice are not published twice (_PUBLISHED below; Spark also skips them).
OVERLAP = timedelta(minutes=2)

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# Drive files this process already published: the change feed and the
# safety-net scan can both report the same upload. Bounded; Spark's
# ingestion step is the real duplicate check.
_PUBLISHED: "OrderedDict[str, None]" = OrderedDict()
_PUBLISHED_MAX = 20000


def _device_gps(props: dict):
    """(lat, lon) the Android app attached for a photo taken with its camera
    (appProperties gps_lat / gps_lon), or None if absent or not a real
    coordinate. Client-supplied, so only range-checked and used when the
    photo has no GPS tags of its own (see spark/preprocessing.extract_metadata)."""
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
    """The Kafka event for one Drive photo, from its Drive metadata alone
    (kind "drive_upload"). Used by this watcher, the dashboard's Process
    button and storage/requeue_stuck --from-drive, so all three publish the
    same event. Every value is a string or None (spark/streaming_job's
    EVENT_SCHEMA reads them as strings)."""
    props = meta.get("appProperties") or {}
    owners = meta.get("owners") or []
    owner = owners[0] if owners else {}
    file_id = meta.get("id") or ""
    captured_at = _parse_time(props.get("captured_at_client"))
    gps = _device_gps(props)
    return {
        "kind": "drive_upload",
        "image_id": props.get("image_id") or None,
        "user_id": None,                                   # Spark resolves it from owner_email
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
        "location": props.get("location"),                 # as the phone sent it; Spark cleans it
        "original_filename": props.get("original_filename") or meta.get("name"),
        "content_hash": meta.get("sha256Checksum"),
        "device_gps_lat": f"{gps[0]}" if gps else None,
        "device_gps_lon": f"{gps[1]}" if gps else None,
    }


def publish_drive_file(meta: dict, backend=None, folder_email: str = None) -> dict:
    """Publishes ONE file found in a farmer's /images/<email>/ folder
    (`folder_email` = that folder's email). Returns {"image_id", "status"}
    where status is: published | held (MANUAL mode) | already_published."""
    file_id = meta.get("id", "")
    image_id = (meta.get("appProperties") or {}).get("image_id") or None
    if file_id in _PUBLISHED:
        return {"image_id": image_id, "status": "already_published"}
    if get_processing_mode() == ProcessingMode.MANUAL:
        # Nothing is sent; the dashboard's Pending Review page lists it
        # straight from Drive until the administrator presses Process.
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
    """publish_drive_file for one file of a scan, never raising: one file
    that can't be sent (Kafka down with the file fallback off) must not stop
    the scan — the change feed's bookmark would never move otherwise. The
    10-minute scan, or requeue_stuck --from-drive, sends it later."""
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
    """Lists every farmer's /images/<email>/ folder for files created since
    the last scan (minus a small overlap) and publishes each new one.
    Returns the per-file outcomes."""
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
    """The state file has no change-feed bookmark: first start, or a factory
    reset deleted it (storage/reset.py) so every photo in Drive is processed
    again."""


def scan_changes(user_folders: dict, backend=None) -> list:
    """Publishes the new photos in Drive's change feed since the saved
    bookmark: ONE request (up to 1000 changes) however many farmers there
    are, and no search-index delay, unlike scan_once(). `user_folders`
    ({folder id: email} of /images/<email>/) is updated in place when the
    feed shows a new farmer folder. Returns the per-file outcomes."""
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
            if images_root in parents:                       # a newly registered farmer
                user_folders[meta["id"]] = meta.get("name")
            continue
        parent = next((p for p in parents if p in user_folders), None)
        if parent is None:                                   # our own results, other files
            continue
        outcomes.append(_publish_safely(meta, backend, user_folders[parent]))
    state["changes_page_token"] = new_token
    _save_state(state)
    return outcomes


def start_change_feed(backend=None) -> dict:
    """Bookmarks "now" in the change feed if there's no bookmark yet, THEN
    runs one full scan_once() to catch up on anything uploaded while the
    watcher was off (bookmark first, so nothing slips through between the
    two). Returns the {folder id: email} map of farmer folders."""
    state = _load_state()
    if not state.get("changes_page_token"):
        state["changes_page_token"] = drive_client.get_start_page_token()
        _save_state(state)
    scan_once(backend=backend)
    return drive_client.user_image_folders()


def next_interval(current: float, found_new: bool) -> float:
    """Check every POLL_SECONDS (fast) while photos are arriving; when it's
    quiet, slow down step by step to IDLE_POLL_SECONDS so an idle night
    costs few Drive requests."""
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
            # Forward anything that landed in the local file queue while
            # Kafka was down, even if no new photos arrive this round.
            if hasattr(backend, "drain"):
                backend.drain()
            if user_folders is None:
                user_folders = start_change_feed(backend=backend)
                last_full_scan = time.monotonic()
            outcomes = scan_changes(user_folders, backend=backend)
            if time.monotonic() - last_full_scan >= FULL_SCAN_SECONDS:
                # Safety net: the old folder search, rarely. Catches anything
                # the change feed ever missed; already-published files are skipped.
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
            # A factory reset removed the bookmark while we were running:
            # start over (new bookmark + full scan of every farmer's folder).
            print("[drive_watcher] Bookmark gone (factory reset?) — publishing every photo in Drive again")
            _PUBLISHED.clear()
            user_folders, found_new = None, True
        except Exception as e:  # network hiccup etc. — keep watching
            print(f"[drive_watcher] scan failed, retrying: {e}")
        interval = next_interval(interval, found_new)
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
