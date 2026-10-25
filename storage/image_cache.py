"""
The laptop's copies of farmer photos, managed as a cache. Drive keeps the
originals; a local copy is trusted only when its SHA-256 matches. Nothing is
deleted on its own: cleanup() removes only what the admin confirmed (never
review or training photos) and logs every outcome.
"""

import hashlib
import os
import threading
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

from config import settings
from storage.image_storage import (
    downloaded_photo_path, store_downloaded_photo, is_drive_path, drive_file_id_from_path, _SAFE_ID_RE,
)
from storage.database import (
    SessionLocal, StorageAuditLog, ScaleTestRun,
)

CATEGORIES = {
    "original_cache": "Original photo cache",
    "temporary": "Temporary files",
    "processed": "Processed / marked-up images",
    "review": "Review images (waiting to be labelled)",
    "training": "Confirmed training images",
    "scalability": "Scalability-test images",
    "legacy_uploads": "Photos from the previous design",
}
PROTECTED = {"review", "training"}              # never offered for clean-up here
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}
ACTIVE_STATUSES = {"INGESTED", "PROCESSING"}
TEMP_MIN_AGE_SECONDS = 3600                      # a .tmp younger than this may still be being written


def roots() -> dict:
    """Category -> folder."""
    return {
        "original_cache": Path(settings.DOWNLOADED_PHOTOS_DIR),
        "review": Path(settings.REVIEW_DIR),
        "training": Path(settings.LABELLED_TRAIN_DIR),
        "scalability": Path(settings.SCALABILITY_REPORTS_DIR),
        "legacy_uploads": Path(settings.IMAGE_STORAGE_PATH),
    }


# --------------------------------------------------------------- verification
_hash_memo: dict = {}
_memo_lock = threading.Lock()


def file_sha256(path: Path) -> str:
    """SHA-256 of a file, cached until the file changes."""
    st = path.stat()
    key = (str(path), st.st_size, st.st_mtime_ns)
    with _memo_lock:
        if key in _hash_memo:
            return _hash_memo[key]
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    digest = h.hexdigest()
    with _memo_lock:
        _hash_memo[key] = digest
    return digest


@dataclass
class CacheCheck:
    status: str          # valid | missing | invalid | unverified
    path: Path | None

    @property
    def label(self) -> str:
        return {"valid": "Cached, verified", "missing": "Not cached",
                "invalid": "Cached copy does not match the original",
                "unverified": "Cached, no checksum to verify"}[self.status]


def check_cached(user_id, image_id, content_hash) -> CacheCheck:
    """Is there a correct local copy of this image?"""
    path = downloaded_photo_path(user_id, image_id)
    if path is None:
        return CacheCheck("missing", None)
    if not content_hash:
        return CacheCheck("unverified", path)
    try:
        ok = file_sha256(path) == content_hash
    except OSError:
        return CacheCheck("missing", None)
    return CacheCheck("valid" if ok else "invalid", path)


def audit(action: str, outcome: str, *, category=None, target=None, reason=None, detail=None,
          performed_by=None) -> None:
    with SessionLocal() as s:
        s.add(StorageAuditLog(action=action, category=category, target=target, reason=reason, outcome=outcome,
                              detail=(str(detail)[:4000] if detail is not None else None),
                              performed_by=performed_by))
        s.commit()


def recent_audit(limit: int = 50) -> list[dict]:
    with SessionLocal() as s:
        rows = s.query(StorageAuditLog).order_by(StorageAuditLog.created_at.desc()).limit(limit).all()
        return [{"time": r.created_at, "action": r.action, "category": r.category, "file": r.target,
                 "reason": r.reason, "outcome": r.outcome, "detail": r.detail, "by": r.performed_by}
                for r in rows]


# --------------------------------------------------------------- original for display
@dataclass
class Original:
    data: object         # a local file path (str) or bytes, for st.image
    source: str          # where it came from, in words


def original_for_display(row: dict, performed_by=None, download=None) -> Original | None:
    """The original photo: the verified local copy, else from Drive (checked
    and stored locally). None if it can't be found."""
    check = check_cached(row["user_id"], row["image_id"], row.get("content_hash"))
    if check.status in ("valid", "unverified"):
        return Original(str(check.path), "Local copy" + (" (verified)" if check.status == "valid" else ""))
    image_path = row.get("image_path") or ""
    if not is_drive_path(image_path):
        from storage.image_storage import resolve_image_path
        try:
            return Original(str(resolve_image_path(image_path)), "Local upload (previous design)")
        except (FileNotFoundError, OSError):
            return None
    from storage import drive_client
    if download is None:
        if not drive_client.is_configured():
            return None
        download = drive_client.download_file
    try:
        data = download(drive_file_id_from_path(image_path))
    except Exception:  # noqa: BLE001 - shown as "not found"
        return None
    if row.get("content_hash") and hashlib.sha256(data).hexdigest() != row["content_hash"]:
        return Original(data, "Google Drive (does not match the recorded checksum, not cached)")
    try:
        store_downloaded_photo(row["user_id"], row["image_id"], data)
        if check.status == "invalid":
            audit("cache_repair", "done", category="original_cache",
                  target=f"{row['user_id']}/{row['image_id']}",
                  reason="cached copy did not match the original; replaced with the verified Drive original",
                  performed_by=performed_by)
        return Original(data, "Google Drive (verified, now cached)")
    except Exception:  # noqa: BLE001 - still show it
        return Original(data, "Google Drive (verified)")


_thumb_memo: dict = {}


def thumbnail(source, max_px: int = 320) -> bytes | None:
    """A small JPEG preview; `source` is a path or bytes."""
    from PIL import Image, ImageOps
    try:
        if isinstance(source, (bytes, bytearray)):
            key = ("b", hashlib.sha1(source).hexdigest(), max_px)
        else:
            st = Path(source).stat()
            key = (str(source), st.st_size, st.st_mtime_ns, max_px)
        with _memo_lock:
            if key in _thumb_memo:
                return _thumb_memo[key]
        img = Image.open(BytesIO(source) if isinstance(source, (bytes, bytearray)) else source)
        img.draft("RGB", (max_px * 2, max_px * 2))
        img = ImageOps.exif_transpose(img).convert("RGB")
        img.thumbnail((max_px, max_px))
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        with _memo_lock:
            if len(_thumb_memo) > 2000:
                _thumb_memo.clear()
            _thumb_memo[key] = buf.getvalue()
        return buf.getvalue()
    except Exception:  # noqa: BLE001 - unreadable file: no preview
        return None


# --------------------------------------------------------------- review state
def review_state(image_id: str, decision: str | None) -> str:
    """awaiting_label | labelled | discarded | not_in_review."""
    if _SAFE_ID_RE.match(image_id or "") and (Path(settings.REVIEW_DIR) / f"{image_id}.jpg").is_file():
        return "awaiting_label"
    return decision or "not_in_review"


REVIEW_LABELS = {"awaiting_label": "Waiting to be labelled", "labelled": "Label confirmed",
                 "discarded": "Discarded from training", "not_in_review": "Not in review"}


# --------------------------------------------------------------- usage
def _walk(root: Path):
    if not root.exists():
        return
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            yield Path(dirpath) / name


def usage() -> list[dict]:
    """Files and bytes per category (a hard-linked file counted once)."""
    seen = set()
    rows = []
    r = roots()
    for cat in CATEGORIES:
        files = size = shared = 0
        if cat == "processed":
            rows.append({"category": cat, "files": 0, "bytes": 0, "shared_bytes": 0,
                         "note": "Kept in Google Drive only (results/<farmer>/)."})
            continue
        if cat == "temporary":
            paths = [p for c in ("original_cache", "legacy_uploads") for p in _walk(r[c]) if p.suffix == ".tmp"]
        else:
            paths = [p for p in _walk(r[cat]) if p.suffix.lower() in IMAGE_SUFFIXES]
            if cat == "scalability":
                paths = [p for p in paths if p.parent.name == "images" or p.name.startswith("after_")]
            if cat == "legacy_uploads":
                paths = [p for p in paths if "scalability" not in p.relative_to(r[cat]).parts]
        for p in paths:
            try:
                st = p.stat()
            except OSError:
                continue
            files += 1
            ident = (st.st_dev, st.st_ino) if st.st_ino else (str(p),)
            if ident in seen:
                shared += st.st_size
                continue
            seen.add(ident)
            size += st.st_size
        rows.append({"category": cat, "files": files, "bytes": size, "shared_bytes": shared,
                     "note": _NOTES[cat]})
    return rows


_NOTES = {
    "original_cache": "Copies of Drive originals; a missing one is fetched from Drive again.",
    "temporary": "Left by an interrupted write; safe to remove once old.",
    "review": "Managed on the Label photos page; never cleaned here.",
    "training": "Confirmed training data; never cleaned here.",
    "scalability": "Staged test photos; the measurements are kept in the database.",
    "legacy_uploads": "Cleared by the factory reset.",
}


# --------------------------------------------------------------- reconciliation
@dataclass
class FileRecord:
    category: str
    rel: str                  # path inside its category folder (shown to the admin)
    size: int | None
    image_id: str | None
    verdict: str              # matched | unverified | invalid | wrong_folder | orphan | orphan_duplicate | temporary | missing
    reason: str
    in_use: bool = False
    path: Path | None = None  # internal only, never shown

    def as_row(self) -> dict:
        return {"category": CATEGORIES[self.category], "file": self.rel, "size_kb": round((self.size or 0) / 1024, 1),
                "image_id": self.image_id or "", "verdict": VERDICTS[self.verdict], "reason": self.reason,
                "in_use": self.in_use}


VERDICTS = {
    "matched": "Matches its database record", "unverified": "Matches a record (no checksum)",
    "invalid": "Content differs from the original", "wrong_folder": "Record belongs to another farmer",
    "orphan": "No database record", "orphan_duplicate": "No record, same content as a registered photo",
    "temporary": "Temporary file", "missing": "Database record without a local copy",
}


def reconcile(catalog_ids: dict | None = None, reviews: set | None = None, training: set | None = None) -> list:
    """Matches every cached file against the catalog, plus catalog images with
    no local copy. Read-only."""
    from storage import image_catalog
    ids = catalog_ids if catalog_ids is not None else image_catalog.all_ids()
    by_hash = {}
    for image_id, (_uid, h, _st) in ids.items():
        if h:
            by_hash.setdefault(h, image_id)
    training = training if training is not None else training_ids()
    root = roots()["original_cache"]
    out, present = [], set()
    for p in _walk(root):
        rel = p.relative_to(root).as_posix()
        try:
            size = p.stat().st_size
        except OSError:
            continue
        if p.suffix == ".tmp":
            out.append(FileRecord("temporary", rel, size, None, "temporary", "interrupted write", path=p))
            continue
        if p.suffix.lower() not in IMAGE_SUFFIXES:
            continue
        image_id, owner = p.stem, p.parent.name

        def add(verdict, reason, in_use=False):
            out.append(FileRecord("original_cache", rel, size, image_id, verdict, reason, in_use, p))
        rec = ids.get(image_id)
        if rec is not None:
            uid, h, status = rec
            in_use = status in ACTIVE_STATUSES
            if uid != owner:
                add("wrong_folder", "the record belongs to another farmer's folder", in_use)
                continue
            if h:
                try:
                    digest = file_sha256(p)
                except OSError:              # removed or locked while we looked
                    continue
            present.add(image_id)
            if not h:
                add("unverified", "no checksum recorded", in_use)
            elif digest == h:
                add("matched", "", in_use)
            else:
                add("invalid", "SHA-256 differs from the recorded original", in_use)
            continue
        note = " Also in the training data." if image_id in training else ""
        try:
            same = by_hash.get(file_sha256(p))
        except OSError:                      # removed or locked while we looked
            continue
        if same:
            add("orphan_duplicate", f"same bytes as registered photo {same}.{note}")
        else:
            add("orphan", "no database record (e.g. from before a factory reset); its Drive "
                          f"original may no longer exist.{note}")
    misplaced = {r.image_id for r in out if r.verdict == "wrong_folder"}
    for image_id, (uid, _h, status) in ids.items():
        if image_id not in present and image_id not in misplaced:
            out.append(FileRecord("original_cache", f"{uid}/{image_id}.*", None, image_id, "missing",
                                  "fetched from Google Drive when it is next viewed or processed",
                                  status in ACTIVE_STATUSES))
    return out


def training_ids() -> set:
    root = roots()["training"]
    return {p.stem for p in _walk(root) if p.suffix.lower() in IMAGE_SUFFIXES}


def review_ids() -> set:
    root = roots()["review"]
    return {p.stem for p in root.glob("*.jpg")} if root.exists() else set()


# --------------------------------------------------------------- clean-up
CLEANUP_KINDS = {
    "temporary": "Temporary files older than one hour",
    "invalid_cache": "Cached copies that do not match their original (fetched again from Drive when needed)",
    "orphan_cache": "Cached copies with no database record (cannot be fetched again by this app)",
    "scalability_images": "Staged photos of finished scalability tests (measurements are kept)",
}


@dataclass
class Candidate:
    kind: str
    category: str
    rel: str
    size: int
    reason: str
    path: Path

    def as_row(self) -> dict:
        return {"file": f"{CATEGORIES[self.category]}: {self.rel}", "size_kb": round(self.size / 1024, 1),
                "reason": self.reason}


def cleanup_preview(kinds) -> list:
    """The files a clean-up of `kinds` would remove, and why. Deletes nothing."""
    kinds = set(kinds)
    out = []
    if kinds & {"temporary", "invalid_cache", "orphan_cache"}:
        records = reconcile()
        for r in records:
            if r.path is None or r.in_use:
                continue
            if r.verdict == "temporary" and "temporary" in kinds:
                try:
                    old = time.time() - r.path.stat().st_mtime > TEMP_MIN_AGE_SECONDS
                except OSError:
                    continue
                if old:
                    out.append(Candidate("temporary", "temporary", r.rel, r.size or 0,
                                         "temporary file older than one hour", r.path))
            elif r.verdict == "invalid" and "invalid_cache" in kinds:
                out.append(Candidate("invalid_cache", "original_cache", r.rel, r.size or 0, r.reason, r.path))
            elif r.verdict in ("orphan", "orphan_duplicate", "wrong_folder") and "orphan_cache" in kinds:
                out.append(Candidate("orphan_cache", "original_cache", r.rel, r.size or 0, r.reason, r.path))
    if "scalability_images" in kinds:
        out += _scalability_candidates()
    return out


def _scalability_candidates() -> list:
    import json
    root = roots()["scalability"]
    with SessionLocal() as s:
        runs = {r.run_id: r for r in s.query(ScaleTestRun).all()}
        s.expunge_all()
    out = []
    for run_id, run in runs.items():
        if run.status in ("PREPARING", "RUNNING"):
            continue                                   # a test is using them
        folder = root / run_id / "images"
        if not folder.is_dir():
            continue
        keep = (json.loads(run.summary_json or "{}").get("comparison") or {}).get("before")
        keep_name = Path(keep).name if keep else None
        for p in folder.iterdir():
            if p.is_file() and p.name != keep_name:
                out.append(Candidate("scalability_images", "scalability", f"{run_id}/images/{p.name}",
                                     p.stat().st_size, "staged copy of a finished scalability test", p))
    return out


def cleanup(kinds, confirmed: list, performed_by=None, dry_run: bool = False) -> dict:
    """Removes the confirmed files that are still candidates; logs each outcome."""
    fresh = {(c.category, c.rel): c for c in cleanup_preview(kinds)}
    wanted = set(confirmed)
    report = {"deleted": 0, "bytes": 0, "skipped": [], "failed": [], "dry_run": dry_run}
    allowed = [p.resolve() for c, p in roots().items() if c not in PROTECTED]
    for (category, rel), c in fresh.items():
        if rel not in wanted:
            continue
        wanted.discard(rel)
        if category in PROTECTED or not any(c.path.resolve().is_relative_to(a) for a in allowed):
            report["skipped"].append((rel, "outside the managed cache"))
            continue
        outcome, detail = "done", None
        if not dry_run:
            try:
                c.path.unlink()
            except FileNotFoundError:
                outcome, detail = "skipped", "already gone"
            except OSError as e:          # locked, permission denied, ...
                outcome, detail = "failed", e
            audit("cleanup_delete", outcome, category=category, target=rel, reason=c.reason,
                  detail=detail, performed_by=performed_by)
        if outcome == "done":
            report["deleted"] += 1
            report["bytes"] += c.size
        else:
            report[outcome].append((rel, str(detail)))
    for rel in sorted(wanted):        # confirmed but no longer a candidate
        report["skipped"].append((rel, "no longer eligible (changed, in use or repaired since the preview)"))
        if not dry_run:
            audit("cleanup_delete", "skipped", target=rel, detail="no longer eligible since the preview",
                  performed_by=performed_by)
    return report
