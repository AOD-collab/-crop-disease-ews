"""
The scalability-test dataset: the original photos farmers uploaded to the
Google Drive collection (images/<email>/), checked with the same rules the
Drive watcher applies to a new upload, plus a verified local copy of each in
DOWNLOADED_PHOTOS_DIR/<user_id>/<image_id>.jpg (the copy Spark itself keeps,
storage/image_storage.store_downloaded_photo).

Google Drive stays the source: a local copy only counts when its size and
SHA-256 match what Drive reports for the original. Missing or mismatched
copies are downloaded again; valid ones never are.

A run can instead take its photos from a custom folder on this computer
(enumerate_folder): one source per run, never mixed.
"""

import hashlib
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from config.settings import MAX_IMAGE_SIZE, ALLOWED_IMAGE_CONTENT_TYPES
from storage import drive_client, drive_inventory
from storage.database import SessionLocal, User
from storage.auth import normalize_email
from storage.image_storage import downloaded_photo_path, store_downloaded_photo
from kafka.drive_watcher import _UUID_RE, _device_gps
from spark.preprocessing import normalize_location


@dataclass
class Candidate:
    """One valid original photo in Drive."""
    image_id: str               # the farmer's image_id (Drive appProperties)
    drive_file_id: str
    user_id: str                # owner's account = folder of the local copy
    name: str
    mime_type: str
    size: int
    sha256: str | None
    md5: str | None
    created_time: str
    location: str | None
    device_gps: tuple | None
    # filled by prepare_local_copies()
    local_path: str | None = None
    local_copy_status: str | None = None     # verified | downloaded
    download_seconds: float | None = None
    error: str | None = None


@dataclass
class Collection:
    candidates: list
    total_files: int
    excluded: Counter = field(default_factory=Counter)   # reason -> count
    enumerate_seconds: float = 0.0

    def as_dict(self) -> dict:
        return {"total_files": self.total_files, "valid": len(self.candidates),
                "excluded": sum(self.excluded.values()), "excluded_reasons": dict(self.excluded),
                "enumerate_seconds": round(self.enumerate_seconds, 3)}


def _problem(meta: dict, folder_email: str, users: dict) -> str | None:
    """Why this Drive file can't be in the dataset (the upload rules Spark's
    ingestion step applies, spark/ingestion.register_upload), or None."""
    props = meta.get("appProperties") or {}
    image_id = props.get("image_id") or ""
    owners = meta.get("owners") or []
    owner = normalize_email(owners[0].get("emailAddress")) if owners else ""
    if not _UUID_RE.match(image_id):
        return "no valid image_id (not uploaded by the app)"
    if not owner:
        return "uploader unknown"
    if folder_email is None or normalize_email(folder_email) != owner:
        return "not in its uploader's own folder"
    if owner not in users:
        return "uploader has no account in the database"
    if not (meta.get("name") or "").startswith(image_id):
        return "file name does not match its image_id"
    if (meta.get("mimeType") or "").lower() not in ALLOWED_IMAGE_CONTENT_TYPES:
        return f"unsupported type {meta.get('mimeType')}"
    size = int(meta.get("size") or 0)
    if size <= 0:
        return "empty file"
    if size > MAX_IMAGE_SIZE:
        return "larger than MAX_IMAGE_SIZE"
    return None


def enumerate_collection() -> Collection:
    """Lists every original photo in every farmer's images/<email>/ folder
    (oldest first), keeps the valid ones and counts why the rest were left
    out. A photo is included once: repeated Drive ids, repeated image_ids and
    repeated content (same SHA-256 uploaded twice) are excluded."""
    started = time.monotonic()
    photos = drive_inventory.farmer_photos()
    with SessionLocal() as s:
        users = {normalize_email(e): uid for uid, e in s.query(User.user_id, User.email).all()}

    excluded = Counter()
    seen_files, seen_ids, seen_content = set(), set(), set()
    candidates = []
    for meta, folder_email in photos:          # oldest upload first
        reason = _problem(meta, folder_email, users)
        props = meta.get("appProperties") or {}
        sha = meta.get("sha256Checksum")
        if reason is None:
            if meta["id"] in seen_files:
                reason = "duplicate Drive file"
            elif props["image_id"] in seen_ids:
                reason = "duplicate image_id"
            elif sha and sha in seen_content:
                reason = "same photo uploaded more than once"
        if reason:
            excluded[reason] += 1
            continue
        seen_files.add(meta["id"])
        seen_ids.add(props["image_id"])
        if sha:
            seen_content.add(sha)
        owner = normalize_email(meta["owners"][0]["emailAddress"])
        candidates.append(Candidate(
            image_id=props["image_id"], drive_file_id=meta["id"], user_id=users[owner],
            name=meta.get("name"), mime_type=meta.get("mimeType"), size=int(meta["size"]),
            sha256=sha, md5=meta.get("md5Checksum"), created_time=meta.get("createdTime"),
            location=normalize_location(props.get("location")),
            device_gps=_device_gps(props),
        ))
    return Collection(candidates=candidates, total_files=len(photos), excluded=excluded,
                      enumerate_seconds=time.monotonic() - started)


def _matches(c: Candidate, data: bytes) -> bool:
    if len(data) != c.size:
        return False
    if c.sha256:
        return hashlib.sha256(data).hexdigest() == c.sha256
    if c.md5:
        return hashlib.md5(data).hexdigest() == c.md5
    return True     # Drive gave no checksum: the size is all we can compare


# A single photo download may not take longer than this in total. Socket
# timeouts alone don't bound it: a connection that keeps trickling bytes, or
# a half-closed one, kept one download hanging for over 10 minutes.
DOWNLOAD_DEADLINE_SECONDS = 120


def _download(file_id: str) -> bytes:
    """drive_client.download_file, streamed, with an overall deadline."""
    if drive_client.download_file is not _REAL_DOWNLOAD:      # tests replace Drive with a fake
        return drive_client.download_file(file_id)
    drive_client._require_valid_id(file_id)
    resp = drive_client._request("GET", f"{drive_client.DRIVE_API}/files/{file_id}",
                                 params={"alt": "media"}, timeout=(15, 30), stream=True)
    try:
        drive_client._check(resp, f"download {file_id}")
        started, buf = time.monotonic(), bytearray()
        for chunk in resp.iter_content(chunk_size=256 * 1024):
            buf += chunk
            if time.monotonic() - started > DOWNLOAD_DEADLINE_SECONDS:
                raise TimeoutError(f"download took longer than {DOWNLOAD_DEADLINE_SECONDS}s")
        return bytes(buf)
    finally:
        resp.close()


_REAL_DOWNLOAD = drive_client.download_file


def _prepare_one(c: Candidate) -> Candidate:
    if c.drive_file_id.startswith(FOLDER_SOURCE_PREFIX):
        return c                # a custom-folder photo is already local
    path = downloaded_photo_path(c.user_id, c.image_id)
    if path is not None:
        try:
            if _matches(c, path.read_bytes()):
                c.local_path, c.local_copy_status = str(path), "verified"
                return c
        except OSError:
            pass
    # missing or not identical to the Drive original -> download it
    last_error = None
    for attempt in range(1, 4):    # drive_client._request also retries rate limits / 5xx
        started = time.monotonic()
        try:
            data = _download(c.drive_file_id)
            c.download_seconds = round(time.monotonic() - started, 3)
            if not _matches(c, data):
                last_error = "downloaded bytes do not match Drive's size/checksum"
                continue
            c.local_path = str(store_downloaded_photo(c.user_id, c.image_id, data))
            c.local_copy_status = "downloaded"
            return c
        except Exception as e:  # noqa: BLE001
            last_error = f"{type(e).__name__}: {e}"
        if attempt < 3:
            time.sleep(2 * attempt)
    c.error = f"download failed: {last_error}"[:500]
    return c


def prepare_local_copies(candidates: list, workers: int = 8, progress=None) -> dict:
    """Makes sure each candidate has a verified local copy; downloads only
    the missing/invalid ones (`workers` at a time). `progress(done, total)`
    is called as they finish. Returns timing and counts; each candidate gets
    local_path / local_copy_status, or error."""
    started = time.monotonic()
    done = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        # counted as each one finishes, so one slow photo doesn't freeze the count
        for _ in as_completed([pool.submit(_prepare_one, c) for c in candidates]):
            done += 1
            if progress:
                progress(done, len(candidates))
    downloaded = [c for c in candidates if c.local_copy_status == "downloaded"]
    return {
        "prepare_seconds": round(time.monotonic() - started, 3),
        "verified_local": sum(c.local_copy_status == "verified" for c in candidates),
        "downloaded": len(downloaded),
        "download_failed": sum(c.error is not None for c in candidates),
        "download_seconds_total": round(sum(c.download_seconds or 0 for c in downloaded), 3),
        "downloaded_mb": round(sum(c.size for c in downloaded) / 1e6, 2),
        "ready": sum(c.local_path is not None for c in candidates),
        "download_errors": dict(Counter(c.error for c in candidates if c.error).most_common(10)),
    }


# --------------------------------------------------------------- custom folder
# The other photo source: any folder on this computer (searched with its
# subfolders). Nothing needs downloading, so each photo is "ready" as soon as
# it passes the same type/size rules as an upload. Files are only read: the
# run hard-links them into its own staging folder.
FOLDER_SOURCE_PREFIX = "local:"
_MAGIC = {b"\xff\xd8\xff": "image/jpeg", b"\x89PNG\r\n\x1a\n": "image/png"}
_FOLDER_SUFFIXES = {".jpg", ".jpeg", ".png"}


def _folder_candidate(path, root, staging_root):
    """(Candidate, None) or (None, reason) for one file in a custom folder."""
    try:
        if staging_root is not None and path.resolve().is_relative_to(staging_root):
            return None, "already a staged test image"
        size = path.stat().st_size
        if size <= 0:
            return None, "empty file"
        if size > MAX_IMAGE_SIZE:
            return None, "larger than MAX_IMAGE_SIZE"
        data = path.read_bytes()
    except OSError as e:
        return None, f"unreadable ({type(e).__name__})"
    mime = next((m for magic, m in _MAGIC.items() if data.startswith(magic)), None)
    if mime is None or mime not in ALLOWED_IMAGE_CONTENT_TYPES:
        return None, "not a JPEG or PNG image"
    sha = hashlib.sha256(data).hexdigest()
    rel = path.relative_to(root).as_posix()
    # The content hash is the photo's identity: replay finds it again even if
    # the file was renamed or moved inside the folder. It also stays short
    # enough for a Drive appProperty when results are published.
    return Candidate(image_id=rel, drive_file_id=FOLDER_SOURCE_PREFIX + sha, user_id=None, name=path.name,
                     mime_type=mime, size=size, sha256=sha, md5=None,
                     created_time=None, location=None, device_gps=None,
                     local_path=str(path), local_copy_status="local folder"), None


def enumerate_folder(folder, staging_root=None, workers: int = 8) -> Collection:
    """Every JPEG/PNG photo in `folder` and its subfolders, sorted by path.
    Each photo is included once (the same content under two names counts
    once); files that can't be used are counted by reason."""
    started = time.monotonic()
    root = Path(folder).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"folder not found: {folder}")
    staging = Path(staging_root).resolve() if staging_root else None
    paths = sorted((p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in _FOLDER_SUFFIXES),
                   key=lambda p: p.relative_to(root).as_posix().lower())
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        results = list(pool.map(lambda p: _folder_candidate(p, root, staging), paths))
    excluded, seen, candidates = Counter(), set(), []
    for cand, reason in results:
        if cand is not None and cand.sha256 in seen:
            cand, reason = None, "same photo more than once"
        if reason:
            excluded[reason] += 1
            continue
        seen.add(cand.sha256)
        candidates.append(cand)
    return Collection(candidates=candidates, total_files=len(paths), excluded=excluded,
                      enumerate_seconds=time.monotonic() - started)


def local_copy_counts(candidates: list) -> dict:
    """Quick look (no hashing): how many candidates already have a local
    file. Used by the dashboard preview."""
    present = sum(downloaded_photo_path(c.user_id, c.image_id) is not None for c in candidates)
    return {"with_local_copy": present, "missing_local_copy": len(candidates) - present}


# --------------------------------------------------------------- local photos (default)
# The default photo source: the farmer photos already on this laptop, i.e. the
# copies Spark keeps of every Drive upload (DOWNLOADED_PHOTOS_DIR/<user_id>/
# <image_id>.jpg, storage/image_storage.store_downloaded_photo). Google Drive is
# never contacted: the photos are read from disk, and where each was taken
# (municipality, the phone's GPS) comes from its row in the database.
LOCAL_SOURCE = "local"


def local_photos_dir() -> Path:
    from config.settings import DOWNLOADED_PHOTOS_DIR
    return Path(DOWNLOADED_PHOTOS_DIR)


def enumerate_local_photos(staging_root=None, workers: int = 8) -> Collection:
    """Every valid photo in local_photos_dir() (same type/size rules and
    same-content check as a custom folder), oldest upload first. Each keeps
    the farmer's image_id, user_id, location and device GPS from the images
    table when it still has a row there (a factory reset removes the rows,
    not the photos: those photos are kept, without a location)."""
    from storage.database import Image
    root = local_photos_dir()
    if not root.is_dir():
        return Collection(candidates=[], total_files=0)
    col = enumerate_folder(root, staging_root=staging_root, workers=workers)
    image_ids = [Path(c.image_id).stem for c in col.candidates]
    rows = {}
    with SessionLocal() as s:
        for start in range(0, len(image_ids), 500):
            for img in s.query(Image).filter(Image.image_id.in_(image_ids[start:start + 500])):
                rows[img.image_id] = img
            s.expunge_all()
    for c in col.candidates:
        rel = Path(c.image_id)                                # "<user_id>/<image_id>.jpg"
        c.image_id = rel.stem
        c.user_id = rel.parent.name if rel.parent.name else None
        c.local_copy_status = "local copy"
        img = rows.get(c.image_id)
        if img is not None:
            c.user_id = img.user_id
            c.location = img.location
            c.created_time = img.uploaded_at.isoformat() if img.uploaded_at else None
            if img.gps_source == "device" and img.gps_latitude is not None and img.gps_longitude is not None:
                c.device_gps = (img.gps_latitude, img.gps_longitude)
    # oldest upload first (like the Drive collection); photos without a row last
    col.candidates.sort(key=lambda c: (c.created_time is None, c.created_time or "", c.image_id))
    return col
