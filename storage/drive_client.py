"""
Google Drive access for the Android -> Drive -> Kafka -> Spark -> Drive ->
Android architecture. This module is the ONLY place in the project that
talks to Google Drive; the API (api/server.py), the Spark job
(spark/preprocessing.py) and the dashboard all go through it.

Uses the Google Drive REST API v3 directly (https://developers.google.com/drive/api/reference/rest/v3)
through google-auth's AuthorizedSession, so the only extra dependencies are
google-auth (+ google-auth-oauthlib for the one-time consent step in
scripts/drive_authorize.py).

Authentication (OAuth 2.0, one CENTRAL Google account):
  * You run `python scripts/drive_authorize.py` ONCE on the laptop/server,
    sign in to the central Google account in the browser and approve access.
  * That writes a refresh token to GOOGLE_DRIVE_TOKEN_FILE on the laptop.
    It is never sent to Android and never committed (see .gitignore).
  * Scope is full `drive`: the laptop must read photos that the FARMERS'
    accounts created in the shared /images folder, which the narrower
    `drive.file` scope cannot see. Only the laptop holds this token.

Who talks to Drive:
  * Android: each farmer signs in with their OWN Google account inside the
    app and uploads to THEIR OWN folder /images/<email>/, which the central
    account created and shared only with them (scripts/drive_share_user.py).
    The app reads its results from /results/<email>/ the same way. A farmer
    can never see another farmer's folders, not even in Google Drive
    itself. There is no API server between the phone and the laptop.
  * Laptop: kafka/drive_watcher.py lists new files in every farmer's
    /images/<email>/ folder and publishes them to Kafka; Spark downloads
    them and writes /results/<email>/<image_id>.json and
    <image_id>_processed.jpg (this module, central account).
"""
import json
import re
import threading
import uuid
from typing import Optional

from config.settings import (
    GOOGLE_DRIVE_TOKEN_FILE,
    GOOGLE_DRIVE_IMAGES_FOLDER_ID,
    GOOGLE_DRIVE_RESULTS_FOLDER_ID,
)

SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_API = "https://www.googleapis.com/drive/v3"
UPLOAD_API = "https://www.googleapis.com/upload/drive/v3"
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

FILE_METADATA_FIELDS = (
    "id,name,mimeType,size,parents,appProperties,md5Checksum,sha256Checksum,"
    "trashed,createdTime,owners(emailAddress,displayName)"
)

# Drive file IDs are URL-safe base64-ish strings. Validating them before they
# go into a URL or a search query prevents path/query injection.
_DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,200}$")


class DriveNotConfigured(Exception):
    """The OAuth token file or folder IDs are missing — run scripts/drive_authorize.py."""


class DriveError(Exception):
    """A Google Drive API call failed."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


def is_valid_drive_id(value: Optional[str]) -> bool:
    return bool(value) and bool(_DRIVE_ID_RE.match(value))


def _require_valid_id(value: str, what: str = "Drive file id") -> str:
    if not is_valid_drive_id(value):
        raise DriveError(f"Invalid {what}: {value!r}", status_code=400)
    return value


def images_folder_id() -> str:
    if not GOOGLE_DRIVE_IMAGES_FOLDER_ID:
        raise DriveNotConfigured("GOOGLE_DRIVE_IMAGES_FOLDER_ID is not set — run scripts/drive_authorize.py")
    return GOOGLE_DRIVE_IMAGES_FOLDER_ID


def results_folder_id() -> str:
    if not GOOGLE_DRIVE_RESULTS_FOLDER_ID:
        raise DriveNotConfigured("GOOGLE_DRIVE_RESULTS_FOLDER_ID is not set — run scripts/drive_authorize.py")
    return GOOGLE_DRIVE_RESULTS_FOLDER_ID


def is_configured() -> bool:
    from pathlib import Path
    return (
        Path(GOOGLE_DRIVE_TOKEN_FILE).exists()
        and bool(GOOGLE_DRIVE_IMAGES_FOLDER_ID)
        and bool(GOOGLE_DRIVE_RESULTS_FOLDER_ID)
    )


# ---------------------------------------------------------------------------
# Authenticated HTTP session (central account, server side only)
# ---------------------------------------------------------------------------
_session = None


def _authed_session():
    """Lazily builds one AuthorizedSession from the stored OAuth token.
    google-auth refreshes the short-lived access token automatically using
    the stored refresh token."""
    global _session
    if _session is not None:
        return _session
    from pathlib import Path
    if not Path(GOOGLE_DRIVE_TOKEN_FILE).exists():
        raise DriveNotConfigured(
            f"Google Drive token file not found at {GOOGLE_DRIVE_TOKEN_FILE} — "
            f"run `python scripts/drive_authorize.py` once on this machine."
        )
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import AuthorizedSession

    creds = Credentials.from_authorized_user_file(str(GOOGLE_DRIVE_TOKEN_FILE), SCOPES)
    _session = AuthorizedSession(creds)
    return _session


def _check(resp, action: str):
    if resp.status_code >= 400:
        raise DriveError(
            f"Google Drive {action} failed: HTTP {resp.status_code} {resp.text[:300]}",
            status_code=resp.status_code,
        )
    return resp


def _escape_query_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


# ---------------------------------------------------------------------------
# Upload (Android -> Drive), metadata, download (Drive -> Spark / API)
# ---------------------------------------------------------------------------
_GET_ATTEMPTS = 3
_GET_TIMEOUT = (15, 60)  # (connect, read) seconds
_REQUEST_TIMEOUT = (15, 60)   # every other call (uploads, searches, shares)
_UPLOAD_ATTEMPTS = 3          # uploads retried after a timeout / dropped connection
# "Slow down" answers from Google — retried with exponential backoff, as
# https://developers.google.com/workspace/drive/api/guides/limits says to.
_RATE_LIMIT_ATTEMPTS = 6           # waits ~1, 2, 4, 8, 16 s (+ jitter), ~30 s in all
_RATE_LIMIT_REASONS = ("rateLimitExceeded", "userRateLimitExceeded")
_RETRY_STATUSES = (429, 500, 502, 503, 504)
_sleep = __import__("time").sleep  # tests replace this


def _should_back_off(resp) -> bool:
    if resp.status_code in _RETRY_STATUSES:
        return True
    if resp.status_code == 403:  # a 403 is also "no permission" — only retry the rate-limit kind
        try:
            errors = (resp.json().get("error") or {}).get("errors") or []
        except ValueError:
            return False
        return any(e.get("reason") in _RATE_LIMIT_REASONS for e in errors)
    return False


def _request(method: str, url: str, **kwargs):
    """Every Drive call goes through here. When Google says "slow down"
    (403 rate limit, 429) or has a passing server error (5xx), wait and try
    again with exponential backoff instead of failing: with thousands of
    uploads at once a photo is then just delayed, never marked FAILED for
    it. The final response (even an error) is returned for _check().

    Every call has a timeout (_REQUEST_TIMEOUT unless the caller gives
    one): without it, an upload whose connection Google left open without
    answering waited forever, and stalled the whole Spark batch."""
    import random
    kwargs.setdefault("timeout", _REQUEST_TIMEOUT)
    for attempt in range(1, _RATE_LIMIT_ATTEMPTS + 1):
        resp = getattr(_authed_session(), method.lower())(url, **kwargs)
        if attempt == _RATE_LIMIT_ATTEMPTS or not _should_back_off(resp):
            return resp
        wait = 2 ** (attempt - 1) + random.random()
        print(f"[drive_client] Google Drive answered HTTP {resp.status_code} (busy/rate limit); "
              f"retrying in {wait:.1f}s")
        _sleep(wait)


def _get_with_retry(url: str, params: dict):
    """GET with a timeout and a few retries on transient network drops
    ("Connection aborted", "IncompleteRead", timeouts). Without this, one
    flaky mobile/Wi-Fi hiccup during a photo download marked the image
    FAILED permanently. Only for GETs: a POST whose answer got lost may
    have been applied, and repeating it would create a second file. Rate
    limits are retried by _request(); other HTTP errors go to _check()."""
    import requests
    for attempt in range(1, _GET_ATTEMPTS + 1):
        try:
            resp = _request("GET", url, params=params, timeout=_GET_TIMEOUT)
            resp.content  # force the full body read inside the retry loop
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.Timeout) as e:
            if attempt == _GET_ATTEMPTS:
                raise
            print(f"[drive_client] GET attempt {attempt} failed ({e}); retrying")
            _sleep(2 * attempt)


def get_file_metadata(file_id: str) -> dict:
    _require_valid_id(file_id)
    resp = _get_with_retry(f"{DRIVE_API}/files/{file_id}", {"fields": FILE_METADATA_FIELDS})
    _check(resp, f"get metadata for {file_id}")
    return resp.json()


def download_file(file_id: str) -> bytes:
    _require_valid_id(file_id)
    resp = _get_with_retry(f"{DRIVE_API}/files/{file_id}", {"alt": "media"})
    _check(resp, f"download {file_id}")
    return resp.content


def find_file_in_folder(name: str, parent_id: str) -> Optional[dict]:
    """Returns {id, name, appProperties} of the newest non-trashed file
    called `name` directly inside `parent_id`, or None."""
    _require_valid_id(parent_id, "parent folder id")
    q = (
        f"name = '{_escape_query_value(name)}' and "
        f"'{parent_id}' in parents and trashed = false"
    )
    resp = _request(
        "GET", f"{DRIVE_API}/files",
        params={
            "q": q,
            "spaces": "drive",
            "orderBy": "modifiedTime desc",
            "pageSize": 1,
            "fields": "files(id,name,appProperties)",
        },
    )
    _check(resp, f"search for {name}")
    files = resp.json().get("files", [])
    return files[0] if files else None


def _multipart_body(metadata: dict, payload: bytes, mime_type: str = "application/json"):
    boundary = f"=====crop-ews-{uuid.uuid4().hex}"
    body = (
        f"--{boundary}\r\n"
        "Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{json.dumps(metadata)}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8") + payload + f"\r\n--{boundary}--\r\n".encode("utf-8")
    return body, f"multipart/related; boundary={boundary}"


def _short_app_properties(app_properties: dict) -> dict:
    """Drive limits each appProperties key+value to 124 bytes; values are
    trimmed so a long location or error text can never make a write fail."""
    out = {}
    for k, v in app_properties.items():
        if v is None:
            continue
        value = str(v)
        while len((k + value).encode("utf-8")) > 124:
            value = value[:-1]
        out[k] = value
    return out


def upload_bytes(name: str, payload: bytes, mime_type: str, parent_id: str, app_properties: dict,
                 may_exist: bool = True, existing_id: str = None) -> str:
    """Creates `name` in `parent_id`, or overwrites it (content and
    appProperties) if it already exists — so re-processing the same image
    (Spark's at-least-once replay) updates one file instead of creating
    duplicates. Returns the Drive file id. Multipart upload: fine for the
    small files Spark writes (result JSON, processed JPEG < 5 MB).

    may_exist=False skips the "does it exist?" search (a files.list, the
    priciest call per photo in Drive quota) — only for a caller that knows
    the file was never written, e.g. a photo's first processing attempt."""
    import requests
    for attempt in range(1, _UPLOAD_ATTEMPTS + 1):
        try:
            # A retry always searches first: the timed-out attempt may have
            # created the file anyway, and must not get a twin.
            return _upload_once(name, payload, mime_type, parent_id, app_properties,
                                may_exist or attempt > 1, existing_id)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            if attempt == _UPLOAD_ATTEMPTS:
                raise
            print(f"[drive_client] upload of {name} failed ({e}); retrying")
            _sleep(2 * attempt)


def _upload_once(name, payload, mime_type, parent_id, app_properties, may_exist, existing_id=None):
    props = _short_app_properties(app_properties)
    if existing_id:                       # the caller already knows the file: update it directly
        existing = {"id": _require_valid_id(existing_id)}
    else:
        existing = find_file_in_folder(name, parent_id) if may_exist else None

    if existing:
        body, content_type = _multipart_body({"appProperties": props}, payload, mime_type)
        resp = _request(
            "PATCH", f"{UPLOAD_API}/files/{existing['id']}?uploadType=multipart&fields=id",
            headers={"Content-Type": content_type},
            data=body,
        )
        _check(resp, f"update {name}")
        return resp.json()["id"]

    metadata = {"name": name, "mimeType": mime_type, "parents": [parent_id], "appProperties": props}
    body, content_type = _multipart_body(metadata, payload, mime_type)
    resp = _request(
        "POST", f"{UPLOAD_API}/files?uploadType=multipart&fields=id",
        headers={"Content-Type": content_type},
        data=body,
    )
    _check(resp, f"create {name}")
    return resp.json()["id"]


def upload_json(name: str, data: dict, parent_id: str, app_properties: dict, may_exist: bool = True,
                existing_id: str = None) -> str:
    """A JSON result file; `app_properties` carry a short result summary
    the Android app can read with a single list request."""
    payload = json.dumps(data, indent=2, default=str).encode("utf-8")
    return upload_bytes(name, payload, "application/json", parent_id, app_properties, may_exist, existing_id)


def list_folder(parent_id: str, created_after: Optional[str] = None, page_size: int = 100) -> list:
    """All non-trashed files directly in `parent_id` (oldest first),
    optionally only those created after `created_after` (RFC 3339). Used by
    kafka/drive_watcher.py to find newly uploaded photos."""
    _require_valid_id(parent_id, "parent folder id")
    q = f"'{parent_id}' in parents and trashed = false"
    if created_after:
        q += f" and createdTime > '{_escape_query_value(created_after)}'"
    files, page_token = [], None
    while True:
        params = {
            "q": q, "spaces": "drive", "orderBy": "createdTime", "pageSize": page_size,
            "fields": f"nextPageToken,files({FILE_METADATA_FIELDS})",
        }
        if page_token:
            params["pageToken"] = page_token
        resp = _request("GET", f"{DRIVE_API}/files", params=params)
        _check(resp, "list folder")
        body = resp.json()
        files.extend(body.get("files", []))
        page_token = body.get("nextPageToken")
        if not page_token:
            return files


def get_start_page_token() -> str:
    """Bookmark for "now" in the account's change feed: list_changes() from
    this token returns only what changes afterwards."""
    resp = _request("GET", f"{DRIVE_API}/changes/startPageToken", params={"fields": "startPageToken"})
    _check(resp, "get changes start token")
    return resp.json()["startPageToken"]


def list_changes(page_token: str) -> tuple:
    """Everything that changed in the account's Drive since `page_token` —
    including photos farmers upload into their /images/<email>/ folders,
    which the central account owns — as (changes, new_token). One request
    for up to 1000 changes, however many farmers there are; the next call
    passes new_token. Each change: {fileId, removed, file: {...metadata}}.
    Used by kafka/drive_watcher.py instead of searching every folder."""
    changes = []
    while True:
        resp = _request("GET", f"{DRIVE_API}/changes", params={
            "pageToken": page_token, "pageSize": 1000, "spaces": "drive", "includeRemoved": "false",
            "fields": f"nextPageToken,newStartPageToken,changes(fileId,removed,file({FILE_METADATA_FIELDS}))",
        })
        _check(resp, "list changes")
        body = resp.json()
        changes.extend(body.get("changes", []))
        if body.get("newStartPageToken"):
            return changes, body["newStartPageToken"]
        page_token = body["nextPageToken"]


def list_subfolders(parent_id: str) -> list:
    """Folders directly inside `parent_id`: {id, name, appProperties}."""
    _require_valid_id(parent_id, "parent folder id")
    q = f"'{parent_id}' in parents and mimeType = '{FOLDER_MIME_TYPE}' and trashed = false"
    out, page_token = [], None
    while True:
        params = {"q": q, "spaces": "drive", "pageSize": 100,
                  "fields": "nextPageToken,files(id,name,appProperties)"}
        if page_token:
            params["pageToken"] = page_token
        resp = _request("GET", f"{DRIVE_API}/files", params=params)
        _check(resp, "list subfolders")
        body = resp.json()
        out.extend(body.get("files", []))
        page_token = body.get("nextPageToken")
        if not page_token:
            return out


def list_files_in_folders(parent_ids: list, created_after: Optional[str] = None) -> list:
    """New (non-folder) files in ANY of `parent_ids` — every farmer's
    /images/<email>/ folder — using one Drive query per 20 folders."""
    files = []
    for i in range(0, len(parent_ids), 20):
        chunk = [_require_valid_id(pid, "folder id") for pid in parent_ids[i:i + 20]]
        parents_q = " or ".join(f"'{pid}' in parents" for pid in chunk)
        q = f"({parents_q}) and mimeType != '{FOLDER_MIME_TYPE}' and trashed = false"
        if created_after:
            q += f" and createdTime > '{_escape_query_value(created_after)}'"
        page_token = None
        while True:
            params = {"q": q, "spaces": "drive", "orderBy": "createdTime", "pageSize": 100,
                      "fields": f"nextPageToken,files({FILE_METADATA_FIELDS})"}
            if page_token:
                params["pageToken"] = page_token
            resp = _request("GET", f"{DRIVE_API}/files", params=params)
            _check(resp, "list user folders")
            body = resp.json()
            files.extend(body.get("files", []))
            page_token = body.get("nextPageToken")
            if not page_token:
                break
    return sorted(files, key=lambda f: f.get("createdTime", ""))


# One private folder per farmer, named by their email, in each root:
#   /images/<email>/   shared ONLY with that farmer as writer  (uploads)
#   /results/<email>/  shared ONLY with that farmer as reader  (results)
# Tagged with appProperties kind + user_email so the phone can find them.
USER_IMAGES_KIND = "user_images"
USER_RESULTS_KIND = "user_results"
_user_folder_cache: dict = {}
_user_folder_lock = threading.Lock()


def user_folder_id(email: str, kind: str, create: bool = False, share_role: Optional[str] = None) -> Optional[str]:
    """The farmer's private images or results folder id (None if it
    doesn't exist and create=False). If it has to be created and
    `share_role` is given, it is shared with that farmer only. Cached per
    process."""
    email = (email or "").strip().lower()
    key = (email, kind)
    if key in _user_folder_cache:
        return _user_folder_cache[key]
    # Spark processes several photos at once (spark/streaming_job.py); two
    # photos of a new farmer must not both create a results folder.
    with _user_folder_lock:
        if key in _user_folder_cache:
            return _user_folder_cache[key]
        return _find_or_create_user_folder(email, kind, key, create, share_role)


def _find_or_create_user_folder(email, kind, key, create, share_role):
    root = images_folder_id() if kind == USER_IMAGES_KIND else results_folder_id()
    found = find_file_in_folder(email, root)
    if found is None and not create:
        return None
    if found:
        folder_id = found["id"]
    else:
        folder_id = ensure_folder(email, root, {"kind": kind, "user_email": email})
        if share_role:
            share_with(folder_id, email, share_role)
    _user_folder_cache[key] = folder_id
    return folder_id


def register_farmer(email: str) -> dict:
    """Gives one farmer their private folders — the Drive half of
    registration (used by scripts/drive_share_user.py and the dashboard's
    "Register farmer" button, so both behave identically):
        images/<email>/   shared ONLY with that farmer as writer (upload)
        results/<email>/  shared ONLY with that farmer as reader
    Safe to run again for the same email. The other half — adding the email
    as a *test user* in Google Cloud — can only be done by hand there."""
    email = (email or "").strip().lower()
    images = user_folder_id(email, USER_IMAGES_KIND, create=True)
    results = user_folder_id(email, USER_RESULTS_KIND, create=True)
    share_with(images, email, "writer")
    share_with(results, email, "reader")
    return {"email": email, "images_folder_id": images, "results_folder_id": results}


def account_info() -> dict:
    """Which Google account the laptop is connected as, and its storage use
    (bytes; limit is None for unlimited accounts)."""
    resp = _request(
        "GET", f"{DRIVE_API}/about", params={"fields": "user(emailAddress),storageQuota(limit,usage)"}
    )
    _check(resp, "about")
    body = resp.json()
    quota = body.get("storageQuota") or {}
    return {
        "email": (body.get("user") or {}).get("emailAddress"),
        "usage_bytes": int(quota["usage"]) if quota.get("usage") else None,
        "limit_bytes": int(quota["limit"]) if quota.get("limit") else None,
    }


def google_cloud_project_id() -> Optional[str]:
    """Project id from the Desktop OAuth client file (for console links)."""
    from config.settings import GOOGLE_DRIVE_CLIENT_SECRET_FILE
    try:
        data = json.loads(open(GOOGLE_DRIVE_CLIENT_SECRET_FILE, encoding="utf-8").read())
        return next(iter(data.values())).get("project_id")
    except (OSError, ValueError, StopIteration, AttributeError):
        return None


def share_with(file_id: str, email: str, role: str) -> None:
    """Gives `email` access to a file/folder ("writer" or "reader") without
    sending Google's notification email. Used by scripts/drive_share_user.py."""
    _require_valid_id(file_id, "folder id")
    if role not in ("reader", "writer"):
        raise DriveError(f"Invalid role: {role}")
    resp = _request(
        "POST", f"{DRIVE_API}/files/{file_id}/permissions",
        params={"sendNotificationEmail": "false", "fields": "id"},
        json={"type": "user", "role": role, "emailAddress": email},
    )
    _check(resp, f"share {file_id} with {email}")


def ensure_folder(name: str, parent_id: Optional[str] = None, app_properties: Optional[dict] = None) -> str:
    """Finds or creates a folder (used once by scripts/drive_authorize.py)."""
    q = (
        f"name = '{_escape_query_value(name)}' and mimeType = '{FOLDER_MIME_TYPE}' "
        f"and trashed = false"
    )
    if parent_id:
        _require_valid_id(parent_id, "parent folder id")
        q += f" and '{parent_id}' in parents"
    resp = _request("GET", f"{DRIVE_API}/files", params={"q": q, "fields": "files(id,name)", "pageSize": 1})
    _check(resp, f"search folder {name}")
    files = resp.json().get("files", [])
    if files:
        return files[0]["id"]
    body = {"name": name, "mimeType": FOLDER_MIME_TYPE}
    if parent_id:
        body["parents"] = [parent_id]
    if app_properties:
        body["appProperties"] = _short_app_properties(app_properties)
    resp = _request("POST", f"{DRIVE_API}/files?fields=id", json=body)
    _check(resp, f"create folder {name}")
    return resp.json()["id"]
