
#### D.2.6 `find_file_in_folder`, `_multipart_body`, `_short_app_properties`

**1. Kódigu orijinál**

{{code storage/drive_client.py 205-248}}

**2. Objetivu**

Buka arkivu foun liu ho naran ida iha pasta; harii body `multipart/related` (metadata JSON + bytes); no koto `appProperties` ba limite Drive (124 bytes kada chave+valór).

**3. Fundamentu Python**

- **String multi-liña ho `\r\n`** — HTTP uza CRLF.
- **`str.encode("utf-8") + bytes + ...`** — kombina bytes.
- **`while len(...) > 124: value = value[:-1]`** — koto karakter ida-ida to'o kabe.

**4. Esplikasaun liña-ba-liña**

- **Liña 205** — `def find_file_in_folder(name: str, parent_id: str) -> Optional[dict]:`
- **Liña 206** — Docstring: arkivu foun liu naran `name` iha `parent_id`, ka `None`.
- **Liña 207** — `_require_valid_id(parent_id, "parent folder id")`.
- **Liña 208** — `q = (`
- **Liña 209** — `f"name = '{_escape_query_value(name)}' and "` — naran ho escape.
- **Liña 210** — `f"'{parent_id}' in parents and trashed = false"`.
- **Liña 211** — `)`.
- **Liña 212** — `resp = _get_with_retry(`
- **Liña 213** — `f"{DRIVE_API}/files",`
- **Liña 214** — `{`
- **Liña 215** — `"q": q,`
- **Liña 216** — `"spaces": "drive",`
- **Liña 217** — `"orderBy": "modifiedTime desc",` — foun liu uluk.
- **Liña 218** — `"pageSize": 1,`
- **Liña 219** — `"fields": "files(id,name,appProperties)",`
- **Liña 220** — `},`
- **Liña 221** — `)`.
- **Liña 222** — `files = _check(resp, f"search for {name}").json().get("files", [])`.
- **Liña 223** — `return files[0] if files else None`.
- **Liña 226** — `def _multipart_body(metadata: dict, payload: bytes, mime_type: str = "application/json"):`
- **Liña 227** — `boundary = f"=====crop-ews-{uuid.uuid4().hex}"` — separadór úniku.
- **Liña 228** — `body = (`
- **Liña 229** — `f"--{boundary}\r\n"` — hahú parte 1.
- **Liña 230** — `"Content-Type: application/json; charset=UTF-8\r\n\r\n"`.
- **Liña 231** — `f"{json.dumps(metadata)}\r\n"` — metadata.
- **Liña 232** — `f"--{boundary}\r\n"` — hahú parte 2.
- **Liña 233** — `f"Content-Type: {mime_type}\r\n\r\n"`.
- **Liña 234** — `).encode("utf-8") + payload + f"\r\n--{boundary}--\r\n".encode("utf-8")` — bytes arkivu + remata.
- **Liña 235** — `return body, f"multipart/related; boundary={boundary}"`.
- **Liña 238** — `def _short_app_properties(app_properties: dict) -> dict:`
- **Liña 239** — Docstring: koto valór ba limite appProperties 124 bytes Drive.
- **Liña 240** — `out = {}`.
- **Liña 241** — `for k, v in app_properties.items():`
- **Liña 242** — `if v is None:`
- **Liña 243** — `continue` — la haruka `None`.
- **Liña 244** — `value = str(v)`.
- **Liña 245** — `while len((k + value).encode("utf-8")) > 124:`
- **Liña 246** — `value = value[:-1]`.
- **Liña 247** — `out[k] = value`.
- **Liña 248** — `return out`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `_short_app_properties({"image_id": "a1b2", "risk_score": 0.97, "x": None})` → `{"image_id": "a1b2", "risk_score": "0.97"}`. `_multipart_body({"name": "a.json"}, b'{"ok":1}')` → body ho parte 2 + `"multipart/related; boundary=====crop-ews-3f2a..."`.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `find_file_in_folder` | `dict ka None` |
| `_multipart_body` | `tuple[bytes, str]` |
| `_short_app_properties` | `dict[str, str]` |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun.)

**8. Impaktu integrasaun**

`_upload_once` uza rua; `find_file_in_folder` mós husi `_find_or_create_user_folder`.

**9. Checkpoint ba principiante**

**P:** Tanba sá konverte valór hotu ba `str` iha appProperties?
**R:** API Drive aseita appProperties hanesan pár string → string de'it.

#### D.2.7 `upload_bytes`, `_upload_once`, `upload_json`

**1. Kódigu orijinál**

{{code storage/drive_client.py 251-290}}

**2. Objetivu**

Harii arkivu ka **troka** (overwrite) se iha ona (replay atualiza arkivu ida, la harii rua). Koko fali sempre buka uluk (koko ne'ebé timeout bele harii ona). `upload_json` serializa dict ba JSON no upload.

**3. Fundamentu Python**

- **`lambda attempt: ...`** haruka ba `_with_network_retry`.
- **`may_exist or attempt > 1`** — koko segundu sempre buka.
- **`json.dumps(data, indent=2, default=str)`** — `default=str` konverte objetu la-JSON (ezemplu `datetime`) ba string.

**4. Esplikasaun liña-ba-liña**

- **Liña 251–252** — `def upload_bytes(name, payload, mime_type, parent_id, app_properties, may_exist=True, existing_id=None) -> str:`
- **Liña 253–254** — Docstring: harii `name` iha `parent_id`, ka troka se iha (replay atualiza arkivu ida); `may_exist=False` salta buka.
- **Liña 255** — Komentáriu: retry sempre buka uluk: koko ne'ebé timeout bele harii ona.
- **Liña 256–257** — `return _with_network_retry(_UPLOAD_ATTEMPTS, f"upload of {name}", lambda attempt: _upload_once(..., may_exist or attempt > 1, existing_id))`.
- **Liña 260** — `def _upload_once(name, payload, mime_type, parent_id, app_properties, may_exist, existing_id=None):`
- **Liña 261** — `props = _short_app_properties(app_properties)`.
- **Liña 262** — `if existing_id:` — komentáriu: chamadór hatene arkivu: atualiza direta.
- **Liña 263** — `existing = {"id": _require_valid_id(existing_id)}`.
- **Liña 264** — `else:`
- **Liña 265** — `existing = find_file_in_folder(name, parent_id) if may_exist else None`.
- **Liña 267** — `if existing:`
- **Liña 268** — `body, content_type = _multipart_body({"appProperties": props}, payload, mime_type)`.
- **Liña 269** — `resp = _request(`
- **Liña 270** — `"PATCH", f"{UPLOAD_API}/files/{existing['id']}?uploadType=multipart&fields=id",` — atualiza konteúdu + appProperties.
- **Liña 271** — `headers={"Content-Type": content_type},`
- **Liña 272** — `data=body,`
- **Liña 273** — `)`.
- **Liña 274** — `return _check(resp, f"update {name}").json()["id"]`.
- **Liña 276** — `metadata = {"name": name, "mimeType": mime_type, "parents": [parent_id], "appProperties": props}`.
- **Liña 277** — `body, content_type = _multipart_body(metadata, payload, mime_type)`.
- **Liña 278** — `resp = _request(`
- **Liña 279** — `"POST", f"{UPLOAD_API}/files?uploadType=multipart&fields=id",` — harii foun.
- **Liña 280** — `headers={"Content-Type": content_type},`
- **Liña 281** — `data=body,`
- **Liña 282** — `)`.
- **Liña 283** — `return _check(resp, f"create {name}").json()["id"]`.
- **Liña 286–287** — `def upload_json(name, data, parent_id, app_properties, may_exist=True, existing_id=None) -> str:`
- **Liña 288** — Docstring: arkivu JSON rezultadu; appProperties iha sumáriu badak ba app.
- **Liña 289** — `payload = json.dumps(data, indent=2, default=str).encode("utf-8")`.
- **Liña 290** — `return upload_bytes(name, payload, "application/json", parent_id, app_properties, may_exist, existing_id)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `upload_json("a1b2.json", payload, "1Res...", summary, may_exist=False, existing_id=None)` → koko 1: `existing = None` → POST → `"1XyZ..."`. Replay: `existing_id = "1XyZ..."` → PATCH → ID hanesan. Timeout iha koko 1 → koko 2 ho `may_exist=True` → `find_file_in_folder` hetan arkivu → PATCH (la iha duplikadu).

**6. Tipu dadus**

| Parámetru | Tipu |
|---|---|
| `name`, `mime_type`, `parent_id` | `str` |
| `payload` | `bytes` |
| `app_properties` | `dict` |
| `may_exist` | `bool` |
| `existing_id` | `str ka None` |
| return | `str` (ID arkivu Drive) |

**7. Ligasaun OOP**

Not implemented in the current codebase. Padraun **upsert idempotent** (prinsípiu): "harii ka atualiza" atu replay la duplika.

**8. Impaktu integrasaun**

`publish_result_to_drive` (D.6) uza `upload_bytes` (imajen) no `upload_json` (rezultadu); `write_reset_markers` uza `upload_json`.

**9. Checkpoint ba principiante**

**P:** Tanba sá koko segundu uza `may_exist=True` maske chamadór haruka `False`?
**R:** Koko dahuluk bele harii ona arkivu maibé resposta lakon (timeout). Buka uluk evita arkivu daruak.

#### D.2.8 Change feed no lista arkivu

**1. Kódigu orijinál**

{{code storage/drive_client.py 293-349}}

**2. Objetivu**

`get_start_page_token` fó bookmark "agora". `list_changes` fó mudansa hotu desde bookmark (ho paginasaun) + bookmark foun — pedidu ida ba agrikultór hotu. `_list_files` lista arkivu hotu ne'ebé tuir query (paginasaun). `list_subfolders`, `user_image_folders`, `list_files_in_folders` (query ida kada pasta 20).

**3. Fundamentu Python**

- **Paginasaun ho `while True`** + `nextPageToken`.
- **`list.extend`**.
- **`{**params, "pageToken": ...}`** — dict foun ho chave aumenta.
- **`range(0, n, 20)`** — 0, 20, 40...
- **`sorted(..., key=lambda f: ...)`**.

**4. Esplikasaun liña-ba-liña**

- **Liña 293** — `def get_start_page_token() -> str:`
- **Liña 294** — Docstring: bookmark change-feed ba "agora".
- **Liña 295** — `resp = _get_with_retry(f"{DRIVE_API}/changes/startPageToken", {"fields": "startPageToken"})`.
- **Liña 296** — `return _check(resp, ...).json()["startPageToken"]`.
- **Liña 299** — `def list_changes(page_token: str) -> tuple:`
- **Liña 300–301** — Docstring: buat hotu muda iha Drive desde `page_token`, hanesan (changes, new_token); pedidu ida, la importa agrikultór hira.
- **Liña 302** — `changes = []`.
- **Liña 303** — `while True:`
- **Liña 304** — `resp = _get_with_retry(f"{DRIVE_API}/changes", {`
- **Liña 305** — `"pageToken": page_token, "pageSize": 1000, "spaces": "drive", "includeRemoved": "false",`
- **Liña 306** — `"fields": f"nextPageToken,newStartPageToken,changes(fileId,removed,file({FILE_METADATA_FIELDS}))",`
- **Liña 307** — `})`.
- **Liña 308** — `body = _check(resp, "list changes").json()`.
- **Liña 309** — `changes.extend(body.get("changes", []))`.
- **Liña 310** — `if body.get("newStartPageToken"):` — pájina ikus:
- **Liña 311** — `return changes, body["newStartPageToken"]`.
- **Liña 312** — `page_token = body["nextPageToken"]` — pájina tuirmai.
- **Liña 315** — `def _list_files(q: str, fields: str, action: str, **extra) -> list:`
- **Liña 316** — Docstring: arkivu hotu ne'ebé tuir query `q`.
- **Liña 317–318** — `out, params = [], {"q": q, "spaces": "drive", **extra, "pageSize": 100, "fields": f"nextPageToken,files({fields})"}`.
- **Liña 319** — `while True:`
- **Liña 320** — `body = _check(_get_with_retry(f"{DRIVE_API}/files", params), action).json()`.
- **Liña 321** — `out.extend(body.get("files", []))`.
- **Liña 322** — `if not body.get("nextPageToken"):`
- **Liña 323** — `return out`.
- **Liña 324** — `params = {**params, "pageToken": body["nextPageToken"]}`.
- **Liña 327** — `def list_subfolders(parent_id: str) -> list:`
- **Liña 328** — Docstring: pasta direta iha `parent_id` nia laran.
- **Liña 329** — `_require_valid_id(parent_id, "parent folder id")`.
- **Liña 330** — `q = f"'{parent_id}' in parents and mimeType = '{FOLDER_MIME_TYPE}' and trashed = false"`.
- **Liña 331** — `return _list_files(q, "id,name,appProperties", "list subfolders")`.
- **Liña 334** — `def user_image_folders() -> dict:`
- **Liña 335** — Docstring: `{folder id: email agrikultór}` ba pasta `/images/<email>/` hotu.
- **Liña 336** — `return {f["id"]: f["name"] for f in list_subfolders(images_folder_id())}`.
- **Liña 339** — `def list_files_in_folders(parent_ids: list, created_after: Optional[str] = None) -> list:`
- **Liña 340** — Docstring: arkivu iha `parent_ids` ruma, query ida kada pasta 20.
- **Liña 341** — `files = []`.
- **Liña 342** — `for i in range(0, len(parent_ids), 20):`
- **Liña 343** — `chunk = [_require_valid_id(pid, "folder id") for pid in parent_ids[i:i + 20]]`.
- **Liña 344** — `parents_q = " or ".join(f"'{pid}' in parents" for pid in chunk)`.
- **Liña 345** — `q = f"({parents_q}) and mimeType != '{FOLDER_MIME_TYPE}' and trashed = false"`.
- **Liña 346** — `if created_after:`
- **Liña 347** — `q += f" and createdTime > '{_escape_query_value(created_after)}'"`.
- **Liña 348** — `files.extend(_list_files(q, FILE_METADATA_FIELDS, "list user folders", orderBy="createdTime"))`.
- **Liña 349** — `return sorted(files, key=lambda f: f.get("createdTime", ""))` — tuan liu uluk.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* agrikultór 45 → `list_files_in_folders` halo query 3 (20 + 20 + 5). `list_changes("1234")` → pájina 1 ho `nextPageToken` → pájina 2 ho `newStartPageToken = "1260"` → `(mudansa hotu, "1260")`.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `get_start_page_token` | `str` |
| `list_changes(page_token)` | `tuple[list[dict], str]` |
| `_list_files(q, fields, action, **extra)` | `list[dict]` |
| `list_subfolders(parent_id)` | `list[dict]` |
| `user_image_folders()` | `dict[str, str]` |
| `list_files_in_folders(parent_ids, created_after)` | `list[dict]` |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun ho paginasaun.)

**8. Impaktu integrasaun**

Watcher (`scan_changes`, `scan_once`, `start_change_feed`), `drive_inventory.farmer_photos`, `_ResultFolderIndex._index`, `reset.clear_drive_files`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `list_files_in_folders` fahe pasta ba grupu 20?
**R:** Query ho `or` naruk liu bele liu limite API; grupu 20 mantein query badak no pedidu uitoan.

#### D.2.9 Pasta agrikultór, `register_farmer`, `account_info`

**1. Kódigu orijinál**

{{code storage/drive_client.py 352-407}}

**2. Objetivu**

Hetan (ka harii no partilla) pasta `images`/`results` agrikultór ida; rai iha cache; lock garante fotografia rua husi agrikultór foun la harii pasta rua. `register_farmer` harii no partilla pasta rua. `account_info` hatudu konta no kuota.

**3. Fundamentu Python**

- **Double-checked locking**: kontrola cache, depois lock, depois kontrola fali.
- **Tuple hanesan chave dict**: `key = (email, kind)`.
- **Annotation variavel** `_user_folder_cache: dict = {}`.

**4. Esplikasaun liña-ba-liña**

- **Liña 352** — Komentáriu: pasta privadu ida kada agrikultór iha `/images` (writer) no `/results` (reader).
- **Liña 353** — `USER_IMAGES_KIND = "user_images"`.
- **Liña 354** — `USER_RESULTS_KIND = "user_results"`.
- **Liña 355** — `_user_folder_cache: dict = {}`.
- **Liña 356** — `_user_folder_lock = threading.Lock()`.
- **Liña 359** — `def user_folder_id(email: str, kind: str, create: bool = False, share_role: Optional[str] = None) -> Optional[str]:`
- **Liña 360** — Docstring: ID pasta images ka results agrikultór, harii no partilla se presiza; cache.
- **Liña 361** — `email = (email or "").strip().lower()`.
- **Liña 362** — `key = (email, kind)`.
- **Liña 363** — `if key in _user_folder_cache:`
- **Liña 364** — `return _user_folder_cache[key]` — lalais, la ho lock.
- **Liña 365** — Komentáriu: fotografia rua husi agrikultór foun labele harii pasta rua.
- **Liña 366** — `with _user_folder_lock:`
- **Liña 367** — `if key in _user_folder_cache:` — kontrola fali (thread seluk bele harii ona).
- **Liña 368** — `return _user_folder_cache[key]`.
- **Liña 369** — `return _find_or_create_user_folder(email, kind, key, create, share_role)`.
- **Liña 372** — `def _find_or_create_user_folder(email, kind, key, create, share_role):`
- **Liña 373** — `root = images_folder_id() if kind == USER_IMAGES_KIND else results_folder_id()`.
- **Liña 374** — `found = find_file_in_folder(email, root)`.
- **Liña 375** — `if found is None and not create:`
- **Liña 376** — `return None`.
- **Liña 377** — `if found:`
- **Liña 378** — `folder_id = found["id"]`.
- **Liña 379** — `else:`
- **Liña 380** — `folder_id = ensure_folder(email, root, {"kind": kind, "user_email": email})` — appProperties `kind` (Android uza atu hatene pasta ida ne'ebé).
- **Liña 381** — `if share_role:`
- **Liña 382** — `share_with(folder_id, email, share_role)`.
- **Liña 383** — `_user_folder_cache[key] = folder_id`.
- **Liña 384** — `return folder_id`.
- **Liña 387** — `def register_farmer(email: str) -> dict:`
- **Liña 388–389** — Docstring: harii no partilla pasta images no results agrikultór ida; seguru repete; test user Google Cloud halo ho liman.
- **Liña 390** — `email = (email or "").strip().lower()`.
- **Liña 391** — `images = user_folder_id(email, USER_IMAGES_KIND, create=True)`.
- **Liña 392** — `results = user_folder_id(email, USER_RESULTS_KIND, create=True)`.
- **Liña 393** — `share_with(images, email, "writer")`.
- **Liña 394** — `share_with(results, email, "reader")`.
- **Liña 395** — `return {"email": email, "images_folder_id": images, "results_folder_id": results}`.
- **Liña 398** — `def account_info() -> dict:`
- **Liña 399** — Docstring: konta Google konekta no uza armazenamentu.
- **Liña 400** — `resp = _get_with_retry(f"{DRIVE_API}/about", {"fields": "user(emailAddress),storageQuota(limit,usage)"})`.
- **Liña 401** — `body = _check(resp, "about").json()`.
- **Liña 402** — `quota = body.get("storageQuota") or {}`.
- **Liña 403** — `return {`
- **Liña 404** — `"email": (body.get("user") or {}).get("emailAddress"),`
- **Liña 405** — `"usage_bytes": int(quota["usage"]) if quota.get("usage") else None,`
- **Liña 406** — `"limit_bytes": int(quota["limit"]) if quota.get("limit") else None,`
- **Liña 407** — `}`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* thread 8 hakerek rezultadu agrikultór foun iha tempu hanesan → ida de'it tama lock no harii pasta `results/agrikultor@example.com` → cache → thread 7 seluk hetan ID husi cache (liña 367–368).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `_user_folder_cache` | `dict[tuple[str, str], str]` |
| `_user_folder_lock` | `threading.Lock` |
| `user_folder_id` | `-> str ka None` |
| `register_farmer` | `-> dict[str, str]` |
| `account_info` | `-> dict` (`email`, `usage_bytes`, `limit_bytes`) |

**7. Ligasaun OOP**

Not implemented in the current codebase: klase cache. Cache + lock nível modulu mak uza. Padraun **double-checked locking** (prinsípiu konkorrénsia).

**8. Impaktu integrasaun**

`publish_result_to_drive` bolu `user_folder_id(owner_email, USER_RESULTS_KIND, create=True, share_role="reader")`. `drive_share_user.py` bolu `register_farmer`. Android `myFolders` lee appProperties `kind`.

**9. Checkpoint ba principiante**

**P:** Tanba sá kontrola cache dala rua (liur no laran lock)?
**R:** Dala uluk lalais (la hein lock). Dala rua garante katak thread seluk la harii pasta bainhira ita hein lock.

#### D.2.10 `google_cloud_project_id`, `share_with`, `ensure_folder`

**1. Kódigu orijinál**

{{code storage/drive_client.py 410-458}}

**2. Objetivu**

Lee ID projetu Google Cloud husi arkivu client (ba link ajuda); partilla arkivu/pasta ho email (la haruka email notifikasaun); buka ka harii pasta (buka fali depois konesaun monu).

**3. Fundamentu Python**

- **`next(iter(data.values()))`** — valór dahuluk dict.
- **Kaer exception 4**.
- **`json=body`** iha `requests` — serializa dict automátiku.

**4. Esplikasaun liña-ba-liña**

- **Liña 410** — `def google_cloud_project_id() -> Optional[str]:`
- **Liña 411** — Docstring: ID projetu husi arkivu OAuth client.
- **Liña 412** — `from config.settings import GOOGLE_DRIVE_CLIENT_SECRET_FILE`.
- **Liña 413** — `try:`
- **Liña 414** — `data = json.loads(open(GOOGLE_DRIVE_CLIENT_SECRET_FILE, encoding="utf-8").read())`.
- **Liña 415** — `return next(iter(data.values())).get("project_id")` — `{"installed": {"project_id": ...}}`; só foti `project_id` (la'ós segredu).
- **Liña 416** — `except (OSError, ValueError, StopIteration, AttributeError):`
- **Liña 417** — `return None`.
- **Liña 420** — `def share_with(file_id: str, email: str, role: str) -> None:`
- **Liña 421** — Docstring: partilla ho `email` la ho email notifikasaun Google.
- **Liña 422** — `_require_valid_id(file_id, "folder id")`.
- **Liña 423** — `if role not in ("reader", "writer"):`
- **Liña 424** — `raise DriveError(f"Invalid role: {role}")`.
- **Liña 425** — Komentáriu: partilla fali la muda buat ida, entaun seguru koko fali.
- **Liña 426** — `resp = _with_network_retry(_UPLOAD_ATTEMPTS, f"share {file_id}", lambda _attempt: _request(`
- **Liña 427** — `"POST", f"{DRIVE_API}/files/{file_id}/permissions",`
- **Liña 428** — `params={"sendNotificationEmail": "false", "fields": "id"},`
- **Liña 429** — `json={"type": "user", "role": role, "emailAddress": email},`
- **Liña 430** — `))`.
- **Liña 431** — `_check(resp, f"share {file_id} with {email}")`.
- **Liña 434** — `def ensure_folder(name: str, parent_id: Optional[str] = None, app_properties: Optional[dict] = None) -> str:`
- **Liña 435** — Docstring: buka ka harii pasta (buka fali depois konesaun monu).
- **Liña 436–437** — `return _with_network_retry(_UPLOAD_ATTEMPTS, f"create folder {name}", lambda _attempt: _find_or_create_folder(name, parent_id, app_properties))`.
- **Liña 440** — `def _find_or_create_folder(name, parent_id, app_properties) -> str:`
- **Liña 441** — `q = (`
- **Liña 442** — `f"name = '{_escape_query_value(name)}' and mimeType = '{FOLDER_MIME_TYPE}' "`.
- **Liña 443** — `f"and trashed = false"`.
- **Liña 444** — `)`.
- **Liña 445** — `if parent_id:`
- **Liña 446** — `_require_valid_id(parent_id, "parent folder id")`.
- **Liña 447** — `q += f" and '{parent_id}' in parents"`.
- **Liña 448** — `resp = _get_with_retry(f"{DRIVE_API}/files", {"q": q, "fields": "files(id,name)", "pageSize": 1})`.
- **Liña 449** — `files = _check(resp, f"search folder {name}").json().get("files", [])`.
- **Liña 450** — `if files:`
- **Liña 451** — `return files[0]["id"]` — iha ona.
- **Liña 452** — `body = {"name": name, "mimeType": FOLDER_MIME_TYPE}`.
- **Liña 453** — `if parent_id:`
- **Liña 454** — `body["parents"] = [parent_id]`.
- **Liña 455** — `if app_properties:`
- **Liña 456** — `body["appProperties"] = _short_app_properties(app_properties)`.
- **Liña 457** — `resp = _request("POST", f"{DRIVE_API}/files?fields=id", json=body)`.
- **Liña 458** — `return _check(resp, f"create folder {name}").json()["id"]`.

**5. Walkthrough ezekusaun**

`ensure_folder("images", "1Root...")` → buka → la iha → POST harii → `"1Img..."`. Halai fali → buka hetan → `"1Img..."` (la harii foun). `share_with("1Img...", "agrikultor@example.com", "owner")` → `DriveError("Invalid role: owner")`.

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `google_cloud_project_id` | `str ka None` | la iha |
| `share_with(file_id, email, role)` | `None` | `DriveError` |
| `ensure_folder(name, parent_id, app_properties)` | `str` | `DriveError`, erru rede |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun.)

**8. Impaktu integrasaun**

`drive_authorize.py` (pasta raiz), `_find_or_create_user_folder` (pasta agrikultór), `register_farmer` (partilla).

**9. Checkpoint ba principiante**

**P:** Tanba sá `sendNotificationEmail=false`?
**R:** Atu agrikultór la simu email Google kada vez pasta partilla (script bele halai dala barak).
