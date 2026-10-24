"""
Google Drive -> Kafka producer (the "Drive -> Kafka" link of the
architecture). Runs on the laptop/server next to Kafka and Spark.

    Android --upload--> Drive /images/<email>/<image_id>.jpg  (the farmer's own folder)
    THIS SCRIPT: follows the central account's Drive change feed (one
                 request per check, every 1 s while uploads arrive, up to
                 3 s when quiet) for new files in EVERY farmer's
                 /images/<email>/ folder, registers each one in the database and
                 publishes a Kafka event carrying the Drive file id +
                 metadata (never the image bytes). A full folder search
                 (scan_once) runs at start-up and every 10 minutes as a
                 safety net.
    Kafka -> Spark (spark/streaming_job.py) -> Drive /results -> Android

It is the system's only way in for photos: the phone never talks to a
server of ours — only to Google Drive.

Trust model: the image_id / location / capture time come from the phone
(Drive appProperties), but WHO uploaded the file comes from Google itself
(the file's owner in Drive metadata), so one user can't upload in someone
else's name. A new owner email becomes a normal USER account in the
database automatically (the admin already approved them by sharing the
/images folder — scripts/drive_share_user.py). An admin can still
deactivate that account in the dashboard, and the watcher then rejects the
account's uploads. A photo is also rejected unless it sits in its owner's
own folder, so one farmer's upload can never be filed under another's.

Run:
    python -m kafka.drive_watcher           # loop forever (run_all.py starts it)
    python -m kafka.drive_watcher --once    # one scan, then exit
"""
import argparse
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (  # noqa: E402
    BASE_DIR, KAFKA_TOPIC, MAX_IMAGE_SIZE, ALLOWED_IMAGE_CONTENT_TYPES,
)
from storage import drive_client  # noqa: E402
from storage.database import (  # noqa: E402
    SessionLocal, init_db, Image, User, Role, UserStatus, ProcessingStatus, ProcessingMode,
    RejectedEvent, get_processing_mode, now_utc,
)
from storage.pipeline_submit import submit_image_for_processing, UPLOAD_REJECTED_PREFIX  # noqa: E402
from storage.image_storage import to_drive_path  # noqa: E402
from storage.auth import hash_password, normalize_email  # noqa: E402
# Spark's cleaning rules: the location is stored the way Spark normalises it,
# and a refused upload's FAILED result is written to Drive the way Spark does.
from spark.preprocessing import normalize_location, publish_result_to_drive_safely  # noqa: E402

# Drive API quota: each check is ONE change-feed request (~100 units of the
# 325,000/minute per-user limit), whatever the number of farmers.
# At 1 s that is ~6,000 units/minute, under 2% of the limit.
POLL_SECONDS = float(os.getenv("DRIVE_WATCH_INTERVAL_SECONDS", "1"))        # while uploads arrive
IDLE_POLL_SECONDS = float(os.getenv("DRIVE_WATCH_IDLE_SECONDS", "3"))       # slowest, when quiet
FULL_SCAN_SECONDS = float(os.getenv("DRIVE_WATCH_FULL_SCAN_SECONDS", "600"))  # safety-net folder search
STATE_FILE = Path(os.getenv("DRIVE_WATCHER_STATE_FILE", str(BASE_DIR / "storage" / "drive_watcher_state.json")))
# Re-scan a small window before the last seen createdTime, so a file whose
# listing appears slightly late in Drive's index is never missed. Files seen
# twice are skipped by image_id, so the overlap is harmless.
OVERLAP = timedelta(minutes=2)

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


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


def _get_or_create_user(session, email: str, display_name: str):
    email = normalize_email(email)
    user = session.query(User).filter(User.email == email).first()
    if user is not None:
        return user
    user = User(
        username=email,
        full_name=display_name or email,
        email=email,
        # Drive users sign in with Google on the phone; this random password
        # is never shown to anyone, so the account can't log in anywhere else.
        password_hash=hash_password(secrets.token_urlsafe(32)),
        role=Role.USER,
        status=UserStatus.ACTIVE,
        email_verified=True,  # verified by Google sign-in
    )
    session.add(user)
    session.commit()
    print(f"[drive_watcher] New user from Google Drive: {email}")
    return user


# Public name, used by the dashboard's "Register farmer" button so a farmer
# shows up in Farmer Management before their first upload.
get_or_create_farmer = _get_or_create_user


def _find_duplicate(session, content_hash, image_id):
    if not content_hash:
        return None
    row = (
        session.query(Image.image_id)
        .filter(Image.content_hash == content_hash, Image.image_id != image_id)
        .order_by(Image.created_at.asc())
        .first()
    )
    return row[0] if row else None


def _publish_failure(image_id: str) -> None:
    """Writes the FAILED result to Drive /results so the phone shows why."""
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        message = img.error_message if img else None
    publish_result_to_drive_safely({"image_id": image_id}, ProcessingStatus.FAILED, message)


def register_drive_file(meta: dict, backend=None, folder_email: str = None) -> dict:
    """Registers ONE file found in a farmer's /images/<email>/ folder
    (`folder_email` = that folder's email). Returns {"image_id", "status"}
    where status is: registered | pending_review | failed | rejected |
    already_registered. Never raises for a bad file."""
    file_id = meta.get("id", "")
    props = meta.get("appProperties") or {}
    image_id = props.get("image_id", "")
    owners = meta.get("owners") or []
    owner_email = owners[0].get("emailAddress") if owners else None
    owner_name = owners[0].get("displayName") if owners else None

    session = SessionLocal()
    try:
        existing = session.get(Image, image_id) if image_id else None
        if existing is not None:
            if existing.image_path == to_drive_path(file_id):
                return {"image_id": image_id, "status": "already_registered"}
            reason = f"image_id {image_id} already belongs to another file"
        elif not _UUID_RE.match(image_id or ""):
            reason = "file has no valid image_id (not uploaded by the Android app)"
        elif not owner_email:
            reason = "could not determine the uploader's Google account"
        else:
            reason = None

        if reason:
            # No usable Image row can be made: keep a trace, but only once per file.
            already = session.query(RejectedEvent).filter(RejectedEvent.raw_event.contains(file_id)).first()
            if already is None:
                session.add(RejectedEvent(image_id=image_id or None,
                                          raw_event=json.dumps(meta, default=str), reason=reason))
                session.commit()
                print(f"[drive_watcher] Rejected Drive file {file_id}: {reason}")
            return {"image_id": image_id or None, "status": "rejected"}

        user = _get_or_create_user(session, owner_email, owner_name)

        problems = []
        if user.status != UserStatus.ACTIVE or getattr(user, "is_deleted", False):
            problems.append("your account is not active — contact the administrator")
        if folder_email is not None and normalize_email(folder_email) != normalize_email(owner_email):
            problems.append("photo was not uploaded into your own folder")
        if not (meta.get("name") or "").startswith(image_id):
            problems.append("file name does not match its image_id")
        if (meta.get("mimeType") or "").lower() not in ALLOWED_IMAGE_CONTENT_TYPES:
            problems.append(f"unsupported image type {meta.get('mimeType')}")
        size = int(meta.get("size") or 0)
        if size <= 0:
            problems.append("empty file")
        elif size > MAX_IMAGE_SIZE:
            problems.append(f"file exceeds max size of {MAX_IMAGE_SIZE} bytes")

        captured_at = _parse_time(props.get("captured_at_client"))
        uploaded_at = _parse_time(meta.get("createdTime")) or now_utc()  # Google's clock, not the phone's
        content_hash = meta.get("sha256Checksum")
        device_gps = _device_gps(props)

        image = Image(
            image_id=image_id,
            location=normalize_location(props.get("location")),
            user_id=user.user_id,
            original_filename=props.get("original_filename") or meta.get("name"),
            image_path=to_drive_path(file_id),
            source="android_drive_upload",
            content_hash=content_hash,
            captured_at=captured_at,
            captured_at_source="client_trusted" if captured_at else None,
            uploaded_at=uploaded_at,
            gps_latitude=device_gps[0] if device_gps else None,
            gps_longitude=device_gps[1] if device_gps else None,
            gps_source="device" if device_gps else None,
            status=ProcessingStatus.UPLOADED,
        )
        session.add(image)
        session.commit()

        if problems:
            image.status = ProcessingStatus.FAILED
            image.error_message = UPLOAD_REJECTED_PREFIX + "; ".join(problems)
            session.commit()
            print(f"[drive_watcher] {image_id} rejected: {image.error_message}")
            session.close()
            _publish_failure(image_id)
            return {"image_id": image_id, "status": "failed"}

        duplicate_of = _find_duplicate(session, content_hash, image_id)
        if duplicate_of:
            print(f"[drive_watcher] {image_id} has the same content as {duplicate_of}")

        if get_processing_mode(session) == ProcessingMode.MANUAL:
            image.status = ProcessingStatus.PENDING_REVIEW
            session.commit()
            print(f"[drive_watcher] {image_id} held for manual review")
            return {"image_id": image_id, "status": "pending_review"}

        try:
            submit_image_for_processing(session, image, backend=backend)
            print(f"[drive_watcher] new photo {image_id[:8]} from {owner_email} "
                  f"({image.location or 'no location'}) -> Kafka topic '{KAFKA_TOPIC}' (Drive file {file_id})")
            return {"image_id": image_id, "status": "registered"}
        except Exception as e:  # submit_image_for_processing already marked it FAILED
            print(f"[drive_watcher] {image_id} could not be published to Kafka: {e}")
            return {"image_id": image_id, "status": "failed"}
    finally:
        session.close()


def _register_safely(meta: dict, backend, folder_email) -> dict:
    """register_drive_file for one file of a scan, never raising: a file that
    can't be registered (e.g. its uploader's e-mail is already some other
    account's username -> IntegrityError) is logged once as a RejectedEvent
    and skipped. Before, the exception aborted the whole scan, so the change
    feed's bookmark never moved and no farmer's photos got registered again."""
    try:
        return register_drive_file(meta, backend=backend, folder_email=folder_email)
    except Exception as e:  # noqa: BLE001 - this file only
        file_id = meta.get("id", "")
        image_id = (meta.get("appProperties") or {}).get("image_id") or None
        reason = f"could not register: {type(e).__name__}: {str(e).splitlines()[0][:300]}"
        print(f"[drive_watcher] Skipped Drive file {file_id}: {reason}")
        try:
            with SessionLocal() as s:
                if s.query(RejectedEvent).filter(RejectedEvent.raw_event.contains(file_id)).first() is None:
                    s.add(RejectedEvent(image_id=image_id, raw_event=json.dumps(meta, default=str), reason=reason))
                    s.commit()
        except Exception as log_error:  # noqa: BLE001 - e.g. the database itself is down
            print(f"[drive_watcher] Could not record the skipped file {file_id}: {log_error}")
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
    the last scan (minus a small overlap) and registers each new one.
    Returns the per-file outcomes."""
    state = _load_state()
    created_after = None
    last = _parse_time(state.get("last_created_time"))
    if last is not None:
        created_after = (last - OVERLAP).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    user_folders = {
        f["id"]: f["name"]
        for f in drive_client.list_subfolders(drive_client.images_folder_id())
    }
    if not user_folders:
        return []
    files = drive_client.list_files_in_folders(list(user_folders), created_after=created_after)
    outcomes = []
    for meta in files:
        parent = next((p for p in meta.get("parents") or [] if p in user_folders), None)
        outcomes.append(_register_safely(meta, backend, user_folders.get(parent)))

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
    """Registers the new photos in Drive's change feed since the saved
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
        outcomes.append(_register_safely(meta, backend, user_folders[parent]))
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
    return {f["id"]: f["name"] for f in drive_client.list_subfolders(drive_client.images_folder_id())}


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

    init_db()
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
                # the change feed ever missed; already-registered files are skipped.
                outcomes += scan_once(backend=backend)
                last_full_scan = time.monotonic()
            new = [o for o in outcomes if o["status"] != "already_registered"]
            found_new = bool(new)
            if new:
                print(f"[drive_watcher] {len(new)} new file(s): {new}")
        except drive_client.DriveNotConfigured as e:
            print(f"[drive_watcher] {e}")
            return 1
        except NoBookmark:
            # A factory reset removed the bookmark while we were running:
            # start over (new bookmark + full scan of every farmer's folder).
            print("[drive_watcher] Bookmark gone (factory reset?) — registering every photo in Drive again")
            user_folders, found_new = None, True
        except Exception as e:  # network hiccup etc. — keep watching
            print(f"[drive_watcher] scan failed, retrying: {e}")
        interval = next_interval(interval, found_new)
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
