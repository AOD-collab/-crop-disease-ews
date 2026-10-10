"""
Tests for the Android -> Google Drive -> Kafka -> Spark -> Google Drive ->
Android flow (no API between phone and laptop).

Google Drive is replaced by an in-memory FakeDrive patched over
storage.drive_client's functions, so these run with no internet and no
Google account — the same way the rest of the suite uses the local Kafka
fallback instead of a real broker. `FakeDrive.phone_upload` does what the
Android app does (DriveRest.uploadImage): a file in /images named
<image_id>.jpg, owned by the farmer's Google account, tagged with
appProperties.
"""
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from api.auth import hash_password
from config.settings import LOCAL_QUEUE_DIR, KAFKA_TOPIC
from storage import drive_client
from storage.database import (
    Base, engine, init_db, SessionLocal, User, Role, UserStatus, Image, ProcessingStatus,
    ProcessingMode, RejectedEvent, set_processing_mode, Prediction,
)
from tests.conftest import make_leaf_jpeg_bytes

IMAGES_FOLDER = "IMAGESfolder0001"
RESULTS_FOLDER = "RESULTSfolder001"


FOLDER = "application/vnd.google-apps.folder"


class FakeDrive:
    def __init__(self):
        self.files = {}
        self.shares = []
        self.searches_skipped = 0
        self._clock = datetime(2026, 9, 1, tzinfo=timezone.utc)

    def _tick(self):
        self._clock += timedelta(seconds=1)
        return self._clock.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _new_id(self, prefix):
        return prefix + uuid.uuid4().hex[:20]

    # --- helpers for the tests -----------------------------------------
    def folder_of(self, email, root):
        for f in self.files.values():
            m = f["meta"]
            if m["mimeType"] == FOLDER and m["name"] == email and root in m["parents"]:
                return m["id"]
        return None

    def phone_upload(self, owner_email, content, image_id=None, location="Dili",
                     mime="image/jpeg", name=None, extra_props=None, folder_email=None):
        """What the Android app does: upload into the farmer's OWN
        /images/<email>/ folder (folder_email lets a test misbehave)."""
        folder = self.folder_of(folder_email or owner_email, IMAGES_FOLDER)
        assert folder, "farmer's folder not created — call share(email) first"
        image_id = image_id or str(uuid.uuid4())
        file_id = self._new_id("DRV")
        props = {"image_id": image_id, "location": location, "original_filename": "leaf.jpg"}
        props.update(extra_props or {})
        self.files[file_id] = {"meta": {
            "id": file_id, "name": name or f"{image_id}.jpg", "mimeType": mime,
            "size": str(len(content)), "parents": [folder], "appProperties": props,
            "sha256Checksum": hashlib.sha256(content).hexdigest(), "trashed": False,
            "createdTime": self._tick(),
            "owners": [{"emailAddress": owner_email, "displayName": owner_email.split("@")[0]}],
        }, "content": content}
        return image_id, file_id

    def phone_read_result(self, image_id, email):
        """What the app reads: /results/<email>/<image_id>.json"""
        folder = self.folder_of(email, RESULTS_FOLDER)
        found = self.find_file_in_folder(f"{image_id}.json", folder) if folder else None
        return (json.loads(self.download_file(found["id"])), found["appProperties"]) if found else (None, None)

    def files_in(self, folder_id):
        return [f for f in self.files.values() if folder_id in f["meta"]["parents"]]

    # --- storage.drive_client API -------------------------------------
    def get_file_metadata(self, file_id):
        if file_id not in self.files:
            raise drive_client.DriveError("not found", status_code=404)
        return json.loads(json.dumps(self.files[file_id]["meta"]))

    def download_file(self, file_id):
        if file_id not in self.files:
            raise drive_client.DriveError("not found", status_code=404)
        return self.files[file_id]["content"]

    def find_file_in_folder(self, name, parent_id):
        for f in self.files.values():
            m = f["meta"]
            if m["name"] == name and parent_id in m["parents"] and not m["trashed"]:
                return {"id": m["id"], "name": m["name"], "appProperties": dict(m["appProperties"])}
        return None

    def list_subfolders(self, parent_id):
        return [{"id": f["meta"]["id"], "name": f["meta"]["name"], "appProperties": f["meta"]["appProperties"]}
                for f in self.files.values()
                if f["meta"]["mimeType"] == FOLDER and parent_id in f["meta"]["parents"]]

    def list_files_in_folders(self, parent_ids, created_after=None):
        out = [json.loads(json.dumps(f["meta"])) for f in self.files.values()
               if f["meta"]["mimeType"] != FOLDER and not f["meta"]["trashed"]
               and set(parent_ids) & set(f["meta"]["parents"])
               and (created_after is None or f["meta"]["createdTime"][:19] > created_after)]
        return sorted(out, key=lambda m: m["createdTime"])

    def ensure_folder(self, name, parent_id=None, app_properties=None):
        existing = self.folder_of(name, parent_id)
        if existing:
            return existing
        fid = self._new_id("FLD")
        self.files[fid] = {"meta": {"id": fid, "name": name, "mimeType": FOLDER,
                                    "parents": [parent_id], "appProperties": dict(app_properties or {}),
                                    "trashed": False, "createdTime": self._tick()}, "content": b""}
        return fid

    def upload_bytes(self, name, payload, mime_type, parent_id, app_properties, may_exist=True,
                     existing_id=None):
        props = drive_client._short_app_properties(app_properties)
        existing = self.find_file_in_folder(name, parent_id)
        if existing_id:
            assert existing and existing["id"] == existing_id, f"existing_id {existing_id} is not {name}"
            may_exist = True
        if not may_exist:
            self.searches_skipped += 1
            # the real client would create a second file next to this one
            assert existing is None, f"may_exist=False but {name} already exists: duplicate result file"
        if existing:
            self.files[existing["id"]]["content"] = payload
            self.files[existing["id"]]["meta"]["appProperties"] = props
            return existing["id"]
        file_id = self._new_id("RES")
        self.files[file_id] = {"meta": {
            "id": file_id, "name": name, "mimeType": mime_type, "size": str(len(payload)),
            "parents": [parent_id], "appProperties": props, "trashed": False,
            "createdTime": self._tick(), "owners": [{"emailAddress": "central@gmail.com"}]},
            "content": payload}
        return file_id

    def share_with(self, file_id, email, role):
        self.shares.append((file_id, email, role))

    # Change feed: every file ever created, in order; a token is "how many
    # had been created when you last looked" (files only grow in this fake).
    def get_start_page_token(self):
        return str(len(self.files))

    def list_changes(self, page_token):
        new = list(self.files.values())[int(page_token):]
        changes = [{"fileId": f["meta"]["id"], "removed": False, "file": json.loads(json.dumps(f["meta"]))}
                   for f in new]
        return changes, str(len(self.files))


def share(email):
    """The admin registers a farmer (scripts/drive_share_user.py)."""
    from scripts.drive_share_user import main
    assert main([email]) == 0


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    Base.metadata.drop_all(engine)
    init_db()
    from kafka import drive_watcher
    monkeypatch.setattr(drive_watcher, "STATE_FILE", tmp_path / "watcher_state.json")
    (LOCAL_QUEUE_DIR / f"{KAFKA_TOPIC}.jsonl").unlink(missing_ok=True)  # fresh "topic" per test
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture()
def drive(monkeypatch):
    fake = FakeDrive()
    for name in ("get_file_metadata", "download_file", "find_file_in_folder", "list_subfolders",
                 "list_files_in_folders", "ensure_folder", "upload_bytes", "share_with",
                 "get_start_page_token", "list_changes"):
        monkeypatch.setattr(drive_client, name, getattr(fake, name))
    monkeypatch.setattr(drive_client, "_user_folder_cache", {})
    monkeypatch.setattr(drive_client, "images_folder_id", lambda: IMAGES_FOLDER)
    monkeypatch.setattr(drive_client, "results_folder_id", lambda: RESULTS_FOLDER)
    return fake


def _scan():
    from kafka.drive_watcher import scan_once
    return scan_once()


def _kafka_events():
    path = LOCAL_QUEUE_DIR / f"{KAFKA_TOPIC}.jsonl"
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def _event_for(image_id):
    return [e for e in _kafka_events() if e["image_id"] == image_id][-1]


def _run_spark(event):
    """What spark/streaming_job.foreach_batch_sink does for one event."""
    from spark.preprocessing import (
        process_event, persist_result, publish_result_to_drive_safely, InvalidEvent,
    )
    try:
        result = process_event(event)
        persist_result(event, result, ProcessingStatus.COMPLETED)
        publish_result_to_drive_safely(event, ProcessingStatus.COMPLETED,
                                       processed_image=result.get("processed_image_jpeg"))
        return result
    except InvalidEvent as e:
        persist_result(event, None, ProcessingStatus.FAILED, error_message=str(e))
        publish_result_to_drive_safely(event, ProcessingStatus.FAILED, str(e))
        return None


# --------------------------------------------------------- Drive -> Kafka
def test_watcher_publishes_drive_file_id_not_bytes(drive):
    share("farmer1@gmail.com")
    content = make_leaf_jpeg_bytes()
    image_id, file_id = drive.phone_upload("farmer1@gmail.com", content, location="  viqueque ")
    outcomes = _scan()
    assert outcomes == [{"image_id": image_id, "status": "registered"}]

    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert img.image_path == f"gdrive:{file_id}"
        assert img.status == ProcessingStatus.INGESTED
        assert img.location == "Viqueque"
        assert img.content_hash == hashlib.sha256(content).hexdigest()
        user = s.get(User, img.user_id)
        assert user.email == "farmer1@gmail.com" and user.status == UserStatus.ACTIVE

    event = _event_for(image_id)
    assert event["drive_file_id"] == file_id and event["image_path"] == f"gdrive:{file_id}"
    assert len(json.dumps(event)) < 1000  # metadata only — no image bytes through Kafka


def test_watcher_checks_every_farmers_folder(drive):
    for email in ("alice@gmail.com", "bob@gmail.com", "carla@gmail.com"):
        share(email)
    ids = {email: drive.phone_upload(email, make_leaf_jpeg_bytes())[0]
           for email in ("alice@gmail.com", "bob@gmail.com", "carla@gmail.com")}
    assert sorted(o["status"] for o in _scan()) == ["registered"] * 3
    with SessionLocal() as s:
        for email, image_id in ids.items():
            assert s.get(User, s.get(Image, image_id).user_id).email == email


def test_change_feed_finds_new_uploads_of_every_farmer(drive):
    from kafka.drive_watcher import start_change_feed, scan_changes
    share("alice@gmail.com")
    before_id, _ = drive.phone_upload("alice@gmail.com", make_leaf_jpeg_bytes())  # uploaded while watcher was off
    user_folders = start_change_feed()                     # bookmark + one catch-up scan
    with SessionLocal() as s:
        assert s.get(Image, before_id) is not None

    share("bob@gmail.com")                                 # a new farmer after start-up
    ids = [drive.phone_upload(e, make_leaf_jpeg_bytes())[0] for e in ("alice@gmail.com", "bob@gmail.com")]
    outcomes = scan_changes(user_folders)
    assert sorted(o["image_id"] for o in outcomes) == sorted(ids)
    assert all(o["status"] == "registered" for o in outcomes)
    assert scan_changes(user_folders) == []                # nothing new: nothing re-registered


def test_change_feed_ignores_our_own_results_and_folders(drive):
    from kafka.drive_watcher import start_change_feed, scan_changes
    share("alice@gmail.com")
    user_folders = start_change_feed()
    image_id, _ = drive.phone_upload("alice@gmail.com", make_leaf_jpeg_bytes(256))
    scan_changes(user_folders)
    _run_spark(_event_for(image_id))                       # writes /results/<email>/ files
    assert scan_changes(user_folders) == []
    assert len(_kafka_events()) == 1


def test_watcher_polls_fast_while_busy_and_slows_down_when_quiet():
    from kafka.drive_watcher import next_interval, POLL_SECONDS, IDLE_POLL_SECONDS
    interval = POLL_SECONDS
    for _ in range(20):
        interval = next_interval(interval, found_new=False)
    assert interval == IDLE_POLL_SECONDS
    assert next_interval(interval, found_new=True) == POLL_SECONDS


def test_watcher_registers_each_file_once(drive):
    share("farmer1@gmail.com")
    drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    outcomes = _scan()
    # the first file is inside the 2-minute re-scan overlap: seen again, skipped
    assert sorted(o["status"] for o in outcomes) == ["already_registered", "registered"]
    assert all(o["status"] == "already_registered" for o in _scan())
    assert len(_kafka_events()) == 2


def test_uploader_identity_comes_from_google_not_the_phone(drive):
    share("real.owner@gmail.com")
    image_id, _ = drive.phone_upload("real.owner@gmail.com", make_leaf_jpeg_bytes(),
                                     extra_props={"user_email": "someone.else@gmail.com"})
    _scan()
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert s.get(User, img.user_id).email == "real.owner@gmail.com"


def test_photo_in_someone_elses_folder_is_refused(drive):
    share("alice@gmail.com")
    share("bob@gmail.com")
    image_id, _ = drive.phone_upload("bob@gmail.com", make_leaf_jpeg_bytes(), folder_email="alice@gmail.com")
    assert _scan() == [{"image_id": image_id, "status": "failed"}]
    assert _kafka_events() == []
    result, _ = drive.phone_read_result(image_id, "bob@gmail.com")   # told in HIS own folder
    assert "not uploaded into your own folder" in result["error_message"]
    assert drive.phone_read_result(image_id, "alice@gmail.com") == (None, None)


def test_watcher_rejects_file_not_from_the_app(drive):
    share("x@gmail.com")
    folder = drive.folder_of("x@gmail.com", IMAGES_FOLDER)
    fid = "DRVmanual0000000001"
    drive.files[fid] = {"meta": {"id": fid, "name": "holiday.jpg", "mimeType": "image/jpeg", "size": "10",
                                 "parents": [folder], "appProperties": {}, "trashed": False,
                                 "createdTime": "2026-09-01T00:00:59.000Z",
                                 "owners": [{"emailAddress": "x@gmail.com"}]}, "content": b"x"}
    assert _scan() == [{"image_id": None, "status": "rejected"}]
    assert _kafka_events() == []
    _scan()
    with SessionLocal() as s:
        assert s.query(RejectedEvent).count() == 1  # logged once, not every scan


def test_watcher_fails_bad_upload_and_tells_the_phone(drive):
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", b"GIF89a....", mime="image/gif")
    assert _scan() == [{"image_id": image_id, "status": "failed"}]
    assert _kafka_events() == []
    result, props = drive.phone_read_result(image_id, "farmer1@gmail.com")
    assert result["status"] == "FAILED" and "unsupported image type" in result["error_message"]
    assert props["user_email"] == "farmer1@gmail.com"


def test_deactivated_user_is_blocked(drive):
    with SessionLocal() as s:
        s.add(User(username="blocked@gmail.com", email="blocked@gmail.com",
                   password_hash=hash_password("x-password-1"), role=Role.USER,
                   status=UserStatus.INACTIVE))
        s.commit()
    share("blocked@gmail.com")
    image_id, _ = drive.phone_upload("blocked@gmail.com", make_leaf_jpeg_bytes())
    assert _scan()[0]["status"] == "failed"
    result, _ = drive.phone_read_result(image_id, "blocked@gmail.com")
    assert "not active" in result["error_message"]


def test_manual_mode_holds_drive_upload(drive):
    with SessionLocal() as s:
        set_processing_mode(s, ProcessingMode.MANUAL)
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    assert _scan() == [{"image_id": image_id, "status": "pending_review"}]
    assert _kafka_events() == []
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert img.status == ProcessingStatus.PENDING_REVIEW
        from storage.pipeline_submit import submit_image_for_processing
        submit_image_for_processing(s, img)            # what the dashboard "Process" button does
    assert _event_for(image_id)["drive_file_id"]


def test_replayed_event_does_not_bypass_manual_mode(drive):
    """A factory reset that keeps Kafka's messages makes Spark replay old
    events; in MANUAL mode the re-registered photo is held, and the old
    event must not process it. The admin's Process still works."""
    from spark.streaming_job import _process_one
    from storage.pipeline_submit import build_upload_event, submit_image_for_processing
    with SessionLocal() as s:
        set_processing_mode(s, ProcessingMode.MANUAL)
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    with SessionLocal() as s:
        old_event = build_upload_event(s.get(Image, image_id))   # what Kafka still holds from before

    assert _process_one(dict(old_event)) is None
    with SessionLocal() as s:
        assert s.get(Image, image_id).status == ProcessingStatus.PENDING_REVIEW
        assert s.query(Prediction).filter(Prediction.image_id == image_id).count() == 0

    with SessionLocal() as s:
        submit_image_for_processing(s, s.get(Image, image_id))  # the admin presses Process
    upload = _process_one(dict(_event_for(image_id)))
    upload.result()
    with SessionLocal() as s:
        assert s.get(Image, image_id).status == ProcessingStatus.COMPLETED


# ------------------------------------------------ privacy: private folders
def test_share_script_creates_private_folders_shared_only_with_that_farmer(drive):
    share("Farmer@Gmail.com")
    images = drive.folder_of("farmer@gmail.com", IMAGES_FOLDER)
    results = drive.folder_of("farmer@gmail.com", RESULTS_FOLDER)
    assert images and results
    assert (images, "farmer@gmail.com", "writer") in drive.shares
    assert (results, "farmer@gmail.com", "reader") in drive.shares
    # the parent folders holding EVERYONE's folders are never shared
    assert not [s for s in drive.shares if s[0] in (IMAGES_FOLDER, RESULTS_FOLDER)]
    share("farmer@gmail.com")                       # safe to run again
    assert len(drive.list_subfolders(IMAGES_FOLDER)) == 1


def test_two_users_results_land_only_in_their_own_folders(drive):
    share("alice@gmail.com")
    share("bob@gmail.com")
    a, _ = drive.phone_upload("alice@gmail.com", make_leaf_jpeg_bytes())
    b, _ = drive.phone_upload("bob@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    _run_spark(_event_for(a))
    _run_spark(_event_for(b))
    alice_results = {f["meta"]["name"] for f in drive.files_in(drive.folder_of("alice@gmail.com", RESULTS_FOLDER))}
    bob_results = {f["meta"]["name"] for f in drive.files_in(drive.folder_of("bob@gmail.com", RESULTS_FOLDER))}
    assert alice_results == {f"{a}.json", f"{a}_processed.jpg"}
    assert bob_results == {f"{b}.json", f"{b}_processed.jpg"}
    assert drive.phone_read_result(a, "bob@gmail.com") == (None, None)
    assert drive.phone_read_result(b, "alice@gmail.com") == (None, None)


# ------------------------------------------------ Kafka -> Spark -> Drive
def test_spark_writes_result_and_processed_image_for_the_phone(drive):
    share("farmer1@gmail.com")
    image_id, file_id = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes(256))
    _scan()
    result = _run_spark(_event_for(image_id))

    data, props = drive.phone_read_result(image_id, "farmer1@gmail.com")   # what the app reads
    assert data["status"] == "COMPLETED"
    assert data["prediction"]["predicted_class"] == result["predicted_class"]
    assert data["drive_image_file_id"] == file_id
    # summary the app lists in one request (Home / History)
    assert props["user_email"] == "farmer1@gmail.com" and props["status"] == "COMPLETED"
    assert props["predicted_class"] == (result["predicted_class"] or "")
    assert props["risk_level"] == (result["risk_level"] or "")

    # the processed (marked-up) image, next to the result
    processed_id = data["processed_file_id"]
    assert processed_id and props["processed_file_id"] == processed_id
    processed = drive.files[processed_id]
    assert processed["meta"]["name"] == f"{image_id}_processed.jpg"
    assert processed["meta"]["mimeType"] == "image/jpeg"
    assert processed["content"][:2] == b"\xff\xd8"                        # a real JPEG

    _run_spark(_event_for(image_id))                        # replay overwrites, never duplicates
    assert len(drive.files_in(drive.folder_of("farmer1@gmail.com", RESULTS_FOLDER))) == 2


def test_results_are_not_duplicated_after_a_factory_reset(drive):
    """The database forgets the photo, Drive still has its result: the next
    processing must update those files, not create a second pair. (Before,
    1,163 duplicates appeared this way.)"""
    from spark import preprocessing
    from storage.reset import factory_reset
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes(256))
    _scan()
    _run_spark(_event_for(image_id))
    results = drive.folder_of("farmer1@gmail.com", RESULTS_FOLDER)
    before = {f["meta"]["id"] for f in drive.files_in(results)}

    factory_reset()
    preprocessing._RESULT_FILES.clear()          # a restarted Spark job starts with no index
    from kafka.drive_watcher import start_change_feed
    start_change_feed()
    _run_spark(_event_for(image_id))
    after = {f["meta"]["id"] for f in drive.files_in(results)}
    assert after - before <= {f["meta"]["id"] for f in drive.files_in(results)
                              if f["meta"]["name"] == "_reset_marker.json"}
    names = [f["meta"]["name"] for f in drive.files_in(results)]
    assert len(names) == len(set(names))


def test_spark_keeps_a_local_copy_of_the_downloaded_photo(drive, monkeypatch):
    from config.settings import DOWNLOADED_PHOTOS_DIR
    from storage.reset import factory_reset
    share("farmer1@gmail.com")
    content = make_leaf_jpeg_bytes(256)
    image_id, _ = drive.phone_upload("farmer1@gmail.com", content)
    _scan()
    event = _event_for(image_id)
    _run_spark(event)

    saved = DOWNLOADED_PHOTOS_DIR / event["user_id"] / f"{image_id}.jpg"
    assert saved.read_bytes() == content                 # exactly what the farmer uploaded

    def no_download(*a, **k):
        raise AssertionError("photo was downloaded again")
    monkeypatch.setattr(drive_client, "download_file", no_download)
    assert _run_spark(event) is not None                  # second run uses the local copy

    factory_reset()
    assert saved.exists()                                 # a reset never deletes it


def test_factory_reset_makes_every_drive_photo_come_back(drive):
    """After a reset the watcher registers ALL photos still in Drive again
    (not only the last 2 minutes' worth), with their original upload time."""
    from kafka import drive_watcher
    from storage.reset import factory_reset
    share("farmer1@gmail.com")
    ids = [drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())[0] for _ in range(3)]
    drive._clock += timedelta(hours=5)          # a later batch, long after the first three
    ids += [drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())[0] for _ in range(2)]
    drive_watcher.start_change_feed()
    with SessionLocal() as s:
        uploaded = {i: s.get(Image, i).uploaded_at for i in ids}

    factory_reset()
    with SessionLocal() as s:
        assert s.query(Image).count() == 0
    assert not drive_watcher.STATE_FILE.exists()

    drive_watcher.start_change_feed()            # what the restarted watcher does first
    with SessionLocal() as s:
        assert {i.image_id for i in s.query(Image).all()} == set(ids)
        assert all(s.get(Image, i).uploaded_at == uploaded[i] for i in ids)


def test_watcher_notices_a_deleted_bookmark(drive):
    from kafka import drive_watcher
    share("farmer1@gmail.com")
    folders = drive_watcher.start_change_feed()
    drive_watcher.STATE_FILE.unlink()
    with pytest.raises(drive_watcher.NoBookmark):     # the main loop then starts over
        drive_watcher.scan_changes(folders)


def test_spark_extracts_photo_metadata_and_keeps_upload_time(drive):
    from tests.test_photo_metadata import jpeg_with_metadata
    share("farmer1@gmail.com")
    content = jpeg_with_metadata(base=make_leaf_jpeg_bytes(256))
    image_id, _ = drive.phone_upload("farmer1@gmail.com", content)
    _scan()
    with SessionLocal() as s:
        uploaded_before = s.get(Image, image_id).uploaded_at
    _run_spark(_event_for(image_id))

    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert img.status == ProcessingStatus.COMPLETED
        assert abs(img.gps_latitude - -8.5569) < 1e-4 and abs(img.gps_longitude - 125.5603) < 1e-4
        assert img.camera_model == "SM-A155F"
        assert img.captured_at_source == "exif"
        assert img.uploaded_at == uploaded_before              # upload time is kept, not replaced
    data, _ = drive.phone_read_result(image_id, "farmer1@gmail.com")
    assert data["gps_latitude"] is not None
    assert data["captured_at_local"] == "20/09/2026 08:15 TLT"
    assert data["uploaded_at_local"].endswith("TLT")


def test_reprocess_of_failed_image_is_not_stuck_at_ingested(drive):
    """Dashboard 'Reprocess': the image goes back to INGESTED right away
    and Spark processes the re-published event (it used to be dropped by
    the streaming dropDuplicates)."""
    from spark.preprocessing import already_completed
    from storage.pipeline_submit import submit_image_for_processing
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes(256))
    _scan()
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        img.status, img.error_message = ProcessingStatus.FAILED, "Connection aborted"
        s.commit()
        submit_image_for_processing(s, img)
        assert img.status == ProcessingStatus.INGESTED and img.error_message is None

    assert not already_completed(image_id)                   # Spark will process it
    _run_spark(_event_for(image_id))
    with SessionLocal() as s:
        assert s.get(Image, image_id).status == ProcessingStatus.COMPLETED
    assert already_completed(image_id)                       # a replay is now skipped


def _leaf_on_dark_background():
    """300x300 photo: near-black background (soil/shadow), a green leaf
    ellipse in the middle, and one dark lesion inside the leaf."""
    from PIL import Image as PILImage, ImageDraw as PILDraw
    img = PILImage.new("RGB", (300, 300), (20, 20, 20))
    d = PILDraw.Draw(img)
    d.ellipse((50, 50, 250, 250), fill=(40, 140, 40))
    d.ellipse((140, 140, 160, 160), fill=(25, 20, 15))
    return img


def _render(img, prediction, heatmap=None):
    from io import BytesIO
    from PIL import Image as PILImage
    from ml.render import render_processed_image
    buf = BytesIO(); img.save(buf, format="PNG")
    out = PILImage.open(BytesIO(render_processed_image(buf.getvalue(), prediction, heatmap)))
    return out, out.height - img.height                   # image, banner height


def test_processed_image_is_a_jpeg_with_a_banner():
    out, banner = _render(_leaf_on_dark_background(),
                          {"predicted_class": "common_rust", "prediction_confidence": 0.8, "risk_level": "HIGH"})
    assert out.format == "JPEG" and out.width == 300 and banner > 0


def test_processed_image_draws_the_model_heatmap():
    import numpy as np
    heat = np.zeros((300, 300), dtype=np.float32)
    heat[:, 150:] = 1.0                                   # model saw the disease on the right half
    out, banner = _render(_leaf_on_dark_background(),
                          {"predicted_class": "common_rust", "prediction_confidence": 0.9,
                           "risk_level": "HIGH"}, heat)
    hot = out.getpixel((200, banner + 150))
    cold = out.getpixel((100, banner + 150))
    assert hot[0] > hot[1]                                # tinted red where the model looked
    assert cold[1] > cold[0]                              # green leaf left alone elsewhere


def test_processed_image_without_heatmap_has_no_red():
    """Model predicted healthy -> no heatmap -> nothing is painted red, not
    even the dark spot (no colour rules decide anything any more)."""
    out, banner = _render(_leaf_on_dark_background(),
                          {"predicted_class": "healthy", "risk_level": "LOW"})
    lesion = out.getpixel((150, banner + 150))
    assert lesion[0] < 80


def test_spark_rejects_event_for_untagged_drive_file(drive, monkeypatch):
    """With SPARK_RECHECK_DRIVE_FILE on, Spark re-checks Drive and catches a
    tag changed after the watcher registered the photo."""
    from spark import preprocessing
    monkeypatch.setattr(preprocessing, "SPARK_RECHECK_DRIVE_FILE", True)
    share("farmer1@gmail.com")
    image_id, file_id = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    drive.files[file_id]["meta"]["appProperties"]["image_id"] = "someone-else"
    with pytest.raises(preprocessing.InvalidEvent, match="not tagged"):
        preprocessing.process_event(_event_for(image_id))


def test_spark_trusts_the_watchers_check_and_skips_the_drive_call(drive, monkeypatch):
    from spark.preprocessing import process_event
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    calls = []
    monkeypatch.setattr(drive_client, "get_file_metadata", lambda fid: calls.append(fid) or drive.get_file_metadata(fid))
    assert process_event(_event_for(image_id))["predicted_class"]
    assert calls == []


def test_event_for_a_file_the_watcher_never_registered_gets_the_full_check(drive, monkeypatch):
    """An event naming another Drive file than the database row still goes
    to Drive — and fails there, because that file isn't tagged with it."""
    from spark.preprocessing import process_event, InvalidEvent
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _, other_file = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    event = dict(_event_for(image_id), image_path=f"gdrive:{other_file}", drive_file_id=other_file)
    calls = []
    monkeypatch.setattr(drive_client, "get_file_metadata", lambda fid: calls.append(fid) or drive.get_file_metadata(fid))
    with pytest.raises(InvalidEvent, match="not tagged"):
        process_event(event)
    assert calls == [other_file]


def test_replay_uploads_a_result_that_never_reached_drive(drive, monkeypatch):
    """Spark stopped after saving a photo but before its upload: the replayed
    event is not skipped as COMPLETED, it gets the result into Drive."""
    pytest.importorskip("pyspark")
    from spark import streaming_job
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    real_upload = drive_client.upload_json

    def drive_down(*a, **k):
        raise drive_client.DriveError("Drive is down", status_code=503)
    monkeypatch.setattr(drive_client, "upload_json", drive_down)
    streaming_job._process_one(_event_for(image_id)).result()
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert img.status == ProcessingStatus.COMPLETED and img.result_published_at is None
    assert drive.phone_read_result(image_id, "farmer1@gmail.com") == (None, None)

    monkeypatch.setattr(drive_client, "upload_json", real_upload)
    upload = streaming_job._process_one(_event_for(image_id))       # the replay
    assert upload is not None
    upload.result()
    assert drive.phone_read_result(image_id, "farmer1@gmail.com")[0]["status"] == "COMPLETED"
    with SessionLocal() as s:
        assert s.get(Image, image_id).result_published_at is not None
    assert streaming_job._process_one(_event_for(image_id)) is None  # now really done: skipped


def test_drive_upload_has_a_timeout_and_retries_without_duplicating(monkeypatch):
    import requests
    calls = []

    class Session:
        def get(self, url, **kw):          # the retry's "does it exist?" search
            calls.append(("GET", kw.get("timeout")))
            return _Resp(body={"files": [{"id": "RESexisting01", "name": "a.json", "appProperties": {}}]})

        def post(self, url, **kw):         # first attempt: Google never answers
            calls.append(("POST", kw.get("timeout")))
            raise requests.exceptions.ReadTimeout("read timed out")

        def patch(self, url, **kw):
            calls.append(("PATCH", kw.get("timeout")))
            return _Resp(body={"id": "RESexisting01"})
    monkeypatch.setattr(drive_client, "_authed_session", lambda: Session())
    monkeypatch.setattr(drive_client, "_sleep", lambda s: None)
    file_id = drive_client.upload_json("a.json", {"x": 1}, "RESULTSfolder001", {}, may_exist=False)
    assert file_id == "RESexisting01"
    assert [c[0] for c in calls] == ["POST", "GET", "PATCH"]      # retried as an update, no second create
    assert all(c[1] for c in calls)                                 # every call had a timeout


def test_spark_moves_on_while_the_result_uploads_and_the_batch_waits_for_it(drive, monkeypatch):
    pytest.importorskip("pyspark")
    import threading
    from spark import streaming_job
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    release, published = threading.Event(), []
    real = streaming_job.publish_result_to_drive_safely

    def slow_drive(*a, **k):
        release.wait(10)                       # Google being slow
        published.append(a[0]["image_id"])
        return real(*a, **k)
    monkeypatch.setattr(streaming_job, "publish_result_to_drive_safely", slow_drive)

    upload = streaming_job._process_one(_event_for(image_id))
    with SessionLocal() as s:
        assert s.get(Image, image_id).status == ProcessingStatus.COMPLETED   # result already in the DB
    assert not upload.done() and published == []                            # thread free, upload pending
    release.set()
    upload.result(timeout=10)
    assert published == [image_id]
    data, _ = drive.phone_read_result(image_id, "farmer1@gmail.com")
    assert data["status"] == "COMPLETED"


# ------------------------------------- drive_client HTTP requests (no network)
class _Resp:
    def __init__(self, status=200, body=None, headers=None, content=b""):
        self.status_code, self._body, self.headers, self.content = status, body or {}, headers or {}, content
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


class _RecordingSession:
    def __init__(self, responses):
        self.calls, self.responses = [], list(responses)

    def _do(self, method, url, **kw):
        self.calls.append((method, url, kw))
        return self.responses.pop(0)

    def get(self, url, **kw): return self._do("GET", url, **kw)
    def post(self, url, **kw): return self._do("POST", url, **kw)
    def patch(self, url, **kw): return self._do("PATCH", url, **kw)


def test_drive_client_lists_many_user_folders_in_one_query(monkeypatch):
    sess = _RecordingSession([
        _Resp(200, {"files": [{"id": "A", "createdTime": "2"}], "nextPageToken": "p2"}),
        _Resp(200, {"files": [{"id": "B", "createdTime": "1"}]}),
    ])
    monkeypatch.setattr(drive_client, "_session", sess)
    folders = ["FOLDERalice00001", "FOLDERbob0000001"]
    files = drive_client.list_files_in_folders(folders, created_after="2026-09-01T00:00:00")
    assert [f["id"] for f in files] == ["B", "A"]                    # sorted oldest first
    q = sess.calls[0][2]["params"]["q"]
    assert "'FOLDERalice00001' in parents or 'FOLDERbob0000001' in parents" in q
    assert "createdTime > '2026-09-01T00:00:00'" in q
    assert "owners" in sess.calls[0][2]["params"]["fields"]
    assert sess.calls[1][2]["params"]["pageToken"] == "p2"


def test_drive_client_upload_json_creates_then_updates_with_summary(monkeypatch):
    sess = _RecordingSession([
        _Resp(200, {"files": []}), _Resp(200, {"id": "RESULTfile000001"}),
        _Resp(200, {"files": [{"id": "RESULTfile000001"}]}), _Resp(200, {"id": "RESULTfile000001"}),
    ])
    monkeypatch.setattr(drive_client, "_session", sess)
    drive_client.upload_json("x.json", {"a": 1}, RESULTS_FOLDER, {"status": "COMPLETED"})
    assert sess.calls[1][0] == "POST" and "uploadType=multipart" in sess.calls[1][1]
    assert b'"status": "COMPLETED"' in sess.calls[1][2]["data"]
    drive_client.upload_json("x.json", {"a": 2}, RESULTS_FOLDER, {"status": "FAILED"})
    assert sess.calls[3][0] == "PATCH" and "uploadType=multipart" in sess.calls[3][1]
    assert b'"status": "FAILED"' in sess.calls[3][2]["data"] and b'"a": 2' in sess.calls[3][2]["data"]


def _rate_limited():
    return _Resp(403, {"error": {"errors": [{"reason": "userRateLimitExceeded"}]}})


def test_drive_client_waits_and_retries_when_google_says_slow_down(monkeypatch):
    sess = _RecordingSession([_rate_limited(), _Resp(429), _Resp(503), _Resp(200, {"id": "RESULTfile000001"})])
    waits = []
    monkeypatch.setattr(drive_client, "_session", sess)
    monkeypatch.setattr(drive_client, "_sleep", waits.append)
    assert drive_client.upload_json("x.json", {}, RESULTS_FOLDER, {}, may_exist=False) == "RESULTfile000001"
    assert [c[0] for c in sess.calls] == ["POST"] * 4
    assert len(waits) == 3 and waits[0] < waits[1] < waits[2]          # exponential backoff


def test_drive_client_does_not_retry_a_real_permission_error(monkeypatch):
    sess = _RecordingSession([_Resp(403, {"error": {"errors": [{"reason": "insufficientFilePermissions"}]}})])
    monkeypatch.setattr(drive_client, "_session", sess)
    monkeypatch.setattr(drive_client, "_sleep", lambda s: pytest.fail("must not wait"))
    with pytest.raises(drive_client.DriveError) as e:
        drive_client.get_file_metadata("DRVfile000000001")
    assert e.value.status_code == 403 and len(sess.calls) == 1


def test_drive_client_gives_up_after_the_last_retry(monkeypatch):
    sess = _RecordingSession([_rate_limited() for _ in range(drive_client._RATE_LIMIT_ATTEMPTS)])
    monkeypatch.setattr(drive_client, "_session", sess)
    monkeypatch.setattr(drive_client, "_sleep", lambda s: None)
    with pytest.raises(drive_client.DriveError):
        drive_client.list_changes("token1")
    assert len(sess.calls) == drive_client._RATE_LIMIT_ATTEMPTS


def test_drive_client_change_feed_follows_pages(monkeypatch):
    sess = _RecordingSession([
        _Resp(200, {"changes": [{"fileId": "A"}], "nextPageToken": "t2"}),
        _Resp(200, {"changes": [{"fileId": "B"}], "newStartPageToken": "t3"}),
    ])
    monkeypatch.setattr(drive_client, "_session", sess)
    changes, token = drive_client.list_changes("t1")
    assert [c["fileId"] for c in changes] == ["A", "B"] and token == "t3"
    assert sess.calls[1][2]["params"]["pageToken"] == "t2"


def test_drive_client_trims_long_app_properties():
    props = drive_client._short_app_properties({"error_message": "x" * 500, "none": None})
    assert len(("error_message" + props["error_message"]).encode()) <= 124
    assert "none" not in props


def test_drive_client_rejects_malformed_ids_and_escapes_queries(monkeypatch):
    sess = _RecordingSession([_Resp(200, {"files": []})])
    monkeypatch.setattr(drive_client, "_session", sess)
    with pytest.raises(drive_client.DriveError):
        drive_client.download_file("../../etc/passwd")
    drive_client.find_file_in_folder("it's.json", RESULTS_FOLDER)
    assert "it\\'s.json" in sess.calls[0][2]["params"]["q"]
