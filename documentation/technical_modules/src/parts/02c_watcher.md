
### D.3 `kafka/drive_watcher.py` — produtór Drive → Kafka

*Nota:* arkivu ne'e mós esplika iha Modulu 03 husi lado Google Drive. Iha ne'e ita fokus ba lado **Kafka**: oinsá eventu harii no publika.

#### D.3.1 Docstring, import no konstante

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 1-37}}

**2. Objetivu**

Define watcher: haree pasta Drive `/images/<email>/` agrikultór hotu no publika eventu Kafka ida kada fotografia foun (metadata de'it). La uza base de dadus. Iha modu MANUAL la publika. Define intervalu polling, arkivu estadu no memória arkivu publika ona.

**3. Fundamentu Python**

- **`sys.path.insert(0, ...)`** molok import projetu (atu `python kafka/drive_watcher.py` mós funsiona).
- **`# noqa: E402`** — import la iha leten arkivu (tanba `sys.path` muda uluk).
- **Type hint string** `"OrderedDict[str, None]"` — hint iha aspas.
- **`timedelta(minutes=2)`** — durasaun tempu.
- **`float(os.getenv(..., "1"))`** — konfigurasaun husi variavel ambiente.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: Drive → Kafka; haree pasta `/images/<email>/` kada agrikultór; publika eventu Kafka ida kada fotografia foun (metadata de'it, nunka imajen); la iha base de dadus; modu MANUAL la publika.
- **Liña 6** — Oinsá halai: `python -m kafka.drive_watcher [--once]`.
- **Liña 7** — Remata docstring.
- **Liña 8** — `import argparse`.
- **Liña 9** — `import json` — arkivu estadu.
- **Liña 10** — `import os`.
- **Liña 11** — `import re`.
- **Liña 12** — `import sys`.
- **Liña 13** — `import time`.
- **Liña 14** — `from collections import OrderedDict`.
- **Liña 15** — `datetime, timedelta, timezone`.
- **Liña 16** — `Path`.
- **Liña 18** — `sys.path.insert(0, str(Path(__file__).resolve().parent.parent))` — aumenta pasta raiz projetu.
- **Liña 20** — `BASE_DIR`, `KAFKA_TOPIC` husi `config.settings`.
- **Liña 21** — `drive_client` (API Drive, Modulu 03).
- **Liña 22** — `to_drive_path` — ID → `"gdrive:<id>"`.
- **Liña 23** — `ProcessingMode`, `get_processing_mode` — modu AUTOMATIC/MANUAL.
- **Liña 25** — Komentáriu: pedidu change-feed ida kada kontrola: < 2% kuota Drive iha 1 s.
- **Liña 26** — `POLL_SECONDS` — default 1 s bainhira upload mai.
- **Liña 27** — `IDLE_POLL_SECONDS` — default 3 s bainhira nonok.
- **Liña 28** — `FULL_SCAN_SECONDS` — default 600 s, buka pasta tomak (rede seguransa).
- **Liña 29** — `STATE_FILE` — default `storage/drive_watcher_state.json`.
- **Liña 30** — Komentáriu: scan fali uitoan molok arkivu ikus, se Drive lista tarde.
- **Liña 31** — `OVERLAP = timedelta(minutes=2)`.
- **Liña 33** — `_UUID_RE = re.compile(...)` — padraun UUID (karakter hex 8-4-4-4-12).
- **Liña 35** — Komentáriu: arkivu Drive publika ona husi prosesu ne'e (scan rua bele hatudu ida).
- **Liña 36** — `_PUBLISHED: "OrderedDict[str, None]" = OrderedDict()` — uza hanesan "set ho orden".
- **Liña 37** — `_PUBLISHED_MAX = 20000` — limite memória.

**5. Walkthrough ezekusaun**

Ho variavel ambiente la define: `POLL_SECONDS = 1.0`, `IDLE_POLL_SECONDS = 3.0`, `FULL_SCAN_SECONDS = 600.0`, `OVERLAP = 0:02:00`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `POLL_SECONDS`, `IDLE_POLL_SECONDS`, `FULL_SCAN_SECONDS` | `float` |
| `STATE_FILE` | `Path` |
| `OVERLAP` | `timedelta` |
| `_UUID_RE` | `re.Pattern` |
| `_PUBLISHED` | `OrderedDict[str, None]` |
| `_PUBLISHED_MAX` | `int` |

**7. Ligasaun OOP**

`_PUBLISHED` mak objetu `OrderedDict` (sub-klase `dict` iha biblioteka). `ProcessingMode` mak enum (Modulu 03).

**8. Impaktu integrasaun**

`run_all.py` hahú `python -m kafka.drive_watcher` (Modulu 06). Topic = `KAFKA_TOPIC` hanesan Spark.

**9. Checkpoint ba principiante**

**P:** Tanba sá `_PUBLISHED` iha limite 20000?
**R:** Atu memória la sa'e ba nafatin; bainhira liu, ida tuan liu hasai (`popitem(last=False)`).

#### D.3.2 `_device_gps` no `_parse_time`

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 40-57}}

**2. Objetivu**

Limpeza dadus husi `appProperties` Drive: GPS telemóvel (válidu de'it) no tempu ISO.

**3. Fundamentu Python**

- **Kaer exception tolu** `(KeyError, TypeError, ValueError)`.
- **Komparasaun kadeia** `-90 <= lat <= 90`.
- **`datetime.fromisoformat`** — parse testu ISO; `"Z"` troka ba `"+00:00"` (Python antigu la aseita `Z`).

**4. Esplikasaun liña-ba-liña**

- **Liña 40** — `def _device_gps(props: dict):`
- **Liña 41** — Docstring: (lat, lon) telemóvel husi appProperties, ka `None` se la iha ka la válidu.
- **Liña 42** — `try:`
- **Liña 43** — `lat, lon = float(props["gps_lat"]), float(props["gps_lon"])`.
- **Liña 44** — `except (KeyError, TypeError, ValueError):` — chave la iha / `None` / la'ós númeru.
- **Liña 45** — `return None`.
- **Liña 46** — `if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):` — liur limite ka (0,0) (GPS falsu).
- **Liña 47** — `return None`.
- **Liña 48** — `return lat, lon`.
- **Liña 51** — `def _parse_time(value):`
- **Liña 52** — `if not value:`
- **Liña 53** — `return None`.
- **Liña 54** — `try:`
- **Liña 55** — `return datetime.fromisoformat(str(value).replace("Z", "+00:00"))`.
- **Liña 56** — `except ValueError:`
- **Liña 57** — `return None`.

**5. Walkthrough ezekusaun**

| Input | Output |
|---|---|
| `_device_gps({"gps_lat": "-8.7281", "gps_lon": "125.5664"})` | `(-8.7281, 125.5664)` |
| `_device_gps({"gps_lat": "0", "gps_lon": "0"})` | `None` |
| `_device_gps({})` | `None` |
| `_parse_time("2026-10-20T01:15:00.000Z")` | `datetime(2026,10,20,1,15, tzinfo=UTC)` |
| `_parse_time("ontem")` | `None` |

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `_device_gps(props: dict)` | `tuple[float, float] ka None` |
| `_parse_time(value)` | `datetime ka None` |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun puru.)

**8. Impaktu integrasaun**

`build_drive_event` uza rua. `_parse_time` mós import husi `spark/preprocessing.py` (Modulu 01).

**9. Checkpoint ba principiante**

**P:** Tanba sá (0, 0) rejeita?
**R:** Telemóvel ne'ebé la hetan GPS dalaruma haruka 0,0 — fatin iha tasi Atlántiku, la'ós Timor. Kódigu trata nia hanesan "la iha GPS".

#### D.3.3 `build_drive_event` — harii payload Kafka

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 60-87}}

**2. Objetivu**

Harii dict eventu `drive_upload` husi metadata Drive. Valór hotu mak testu ka `None` (hanesan `EVENT_SCHEMA` Spark ne'ebé hotu `StringType`).

**3. Fundamentu Python**

- **`(x or {})`**, **`owners[0] if owners else {}`** — evita erru bainhira la iha.
- **Dict literal naruk** ho espresaun iha valór.
- **f-string ba númeru** `f"{gps[0]}"` — konverte `float` ba `str`.

**4. Esplikasaun liña-ba-liña**

- **Liña 60** — `def build_drive_event(meta: dict, folder_email: str = None) -> dict:`
- **Liña 61** — Docstring: eventu Kafka ba fotografia Drive ida, husi metadata; valór hotu testu ka `None`.
- **Liña 62** — `props = meta.get("appProperties") or {}` — etiketa ne'ebé Android tau.
- **Liña 63** — `owners = meta.get("owners") or []`.
- **Liña 64** — `owner = owners[0] if owners else {}`.
- **Liña 65** — `file_id = meta.get("id") or ""`.
- **Liña 66** — `captured_at = _parse_time(props.get("captured_at_client"))` — tempu telemóvel.
- **Liña 67** — `gps = _device_gps(props)`.
- **Liña 68** — `return {`
- **Liña 69** — `"kind": "drive_upload",`
- **Liña 70** — `"image_id": props.get("image_id") or None,` — UUID husi Android.
- **Liña 71** — `"user_id": None,` — komentáriu: hetan husi `owner_email` (Spark `register_upload`).
- **Liña 72** — `"drive_file_id": file_id,`
- **Liña 73** — `"image_path": to_drive_path(file_id),` — `"gdrive:<id>"`.
- **Liña 74** — `"file_name": meta.get("name"),`
- **Liña 75** — `"mime_type": meta.get("mimeType"),`
- **Liña 76** — `"size_bytes": str(meta["size"]) if meta.get("size") is not None else None,`
- **Liña 77** — `"owner_email": owner.get("emailAddress"),`
- **Liña 78** — `"owner_name": owner.get("displayName"),`
- **Liña 79** — `"folder_email": folder_email,`
- **Liña 80** — `"uploaded_at": meta.get("createdTime"),` — komentáriu: relójiu Google, la'ós telemóvel.
- **Liña 81** — `"captured_at": captured_at.isoformat() if captured_at else None,`
- **Liña 82** — `"location": props.get("location"),`
- **Liña 83** — `"original_filename": props.get("original_filename") or meta.get("name"),`
- **Liña 84** — `"content_hash": meta.get("sha256Checksum"),`
- **Liña 85** — `"device_gps_lat": f"{gps[0]}" if gps else None,`
- **Liña 86** — `"device_gps_lon": f"{gps[1]}" if gps else None,`
- **Liña 87** — `}`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* `meta`: `{"id": "1AbCdEf", "name": "IMG_20261020.jpg", "mimeType": "image/jpeg", "size": "2457600", "createdTime": "2026-10-20T01:15:00.000Z", "owners": [{"emailAddress": "agrikultor@example.com", "displayName": "Agrikultor"}], "appProperties": {"image_id": "a1b2...", "location": "Aileu Vila", "gps_lat": "-8.7281", "gps_lon": "125.5664", "captured_at_client": "2026-10-20T10:14:52+09:00"}, "sha256Checksum": "9f86d0..."}` → payload hanesan iha C.3.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `meta` | `dict` (resposta API Drive) |
| `folder_email` | `str ka None` |
| `props`, `owner` | `dict` |
| `owners` | `list[dict]` |
| `captured_at` | `datetime ka None` |
| `gps` | `tuple ka None` |
| return | `dict[str, str ka None]` (kampu 18) |

**7. Ligasaun OOP**

Not implemented in the current codebase: klase eventu (`dataclass`). Eventu mak `dict` simples ne'ebé `json.dumps` bele serializa direta.

**8. Impaktu integrasaun**

Naran chave 18 tenke hanesan ho `EVENT_SCHEMA` (`spark/streaming_job.py` liña 52–74). Spark `parse_events` obriga `owner_email` ba `drive_upload`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `user_id` mak `None`?
**R:** Watcher la iha base de dadus (dezeñu "stateless"). Spark hetan `user_id` husi `owner_email` bainhira `register_upload`.

#### D.3.4 `publish_drive_file` no `_publish_safely`

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 90-116}}

**2. Objetivu**

Publika fotografia ida: salta se publika ona iha prosesu ne'e; salta (`held`) se modu MANUAL; se lae harii eventu, haruka ba backend, rai iha `_PUBLISHED`. `_publish_safely` falun nia atu arkivu aat ida la para scan tomak.

**3. Fundamentu Python**

- **`in` iha dict** — kontrola chave.
- **`type(e).__name__`** — naran klase erru.
- **`str(e).splitlines()[0][:300]`** — liña dahuluk mensajen, maximu karakter 300.

**4. Esplikasaun liña-ba-liña**

- **Liña 90** — `def publish_drive_file(meta: dict, backend=None, folder_email: str = None) -> dict:`
- **Liña 91** — Docstring: status `published` | `held` (MANUAL) | `already_published`.
- **Liña 92** — `file_id = meta.get("id", "")`.
- **Liña 93** — `image_id = (meta.get("appProperties") or {}).get("image_id") or None`.
- **Liña 94** — `if file_id in _PUBLISHED:`
- **Liña 95** — `return {"image_id": image_id, "status": "already_published"}`.
- **Liña 96** — `if get_processing_mode() == ProcessingMode.MANUAL:`
- **Liña 97** — `return {"image_id": image_id, "status": "held"}` — admin tenke klik "Process" iha dashboard.
- **Liña 98** — `event = build_drive_event(meta, folder_email)`.
- **Liña 99** — `backend.send(event)` — **publika** (ResilientBackend).
- **Liña 100** — `_PUBLISHED[file_id] = None`.
- **Liña 101** — `while len(_PUBLISHED) > _PUBLISHED_MAX:`
- **Liña 102** — `_PUBLISHED.popitem(last=False)` — hasai tuan liu.
- **Liña 103–104** — `print(...)` — "new photo <id> from <email> (<fatin>) -> Kafka topic '<topic>' (Drive file <id>)".
- **Liña 105** — `return {"image_id": image_id, "status": "published"}`.
- **Liña 108** — `def _publish_safely(meta: dict, backend, folder_email) -> dict:`
- **Liña 109** — Docstring: `publish_drive_file` ne'ebé nunka hamosu erru.
- **Liña 110** — `try:`
- **Liña 111** — `return publish_drive_file(meta, backend=backend, folder_email=folder_email)`.
- **Liña 112** — `except Exception as e:` — komentáriu: arkivu ne'e de'it.
- **Liña 113** — `image_id = ...`.
- **Liña 114–115** — `print(...)` — "Could not publish Drive file <id>: <Tipu>: <mensajen>".
- **Liña 116** — `return {"image_id": image_id, "status": "error"}`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* modu AUTOMATIC, arkivu foun → `{"image_id": "a1b2...", "status": "published"}`. Scan segundu hetan arkivu hanesan → `"already_published"`. Modu MANUAL → `"held"` (Kafka la simu buat ida).

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `publish_drive_file` | `dict` (`image_id`, `status`) | erru `backend.send` (ezemplu `KafkaException` ho `KafkaBackend`) |
| `_publish_safely` | `dict` | la iha |

**7. Ligasaun OOP**

`backend.send(event)` — **polimorfizmu duck typing**: `backend` bele `ResilientBackend` ka `KafkaBackend`. `ProcessingMode.MANUAL` — membru enum.

**8. Impaktu integrasaun**

Bolu husi `scan_once` no `scan_changes`. Iha modu MANUAL, fotografia hela iha Drive; dashboard hatudu nia hanesan "pending" (Drive menus base de dadus) no admin klik "Process" (Modulu 06).

**9. Checkpoint ba principiante**

**P:** Se `backend.send` hamosu erru ba arkivu ida, scan para?
**R:** Lae. `_publish_safely` kaer erru, print, no fó `"status": "error"`; scan kontinua ba arkivu tuirmai.

#### D.3.5 `_load_state`, `_save_state` no `scan_once`

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 119-152}}

**2. Objetivu**

Rai/lee estadu watcher iha arkivu JSON. `scan_once` buka arkivu iha pasta agrikultór hotu ne'ebé harii desde scan ikus (menus 2 minutu overlap), publika, no rai `createdTime` foun liu.

**3. Fundamentu Python**

- **`json.loads(path.read_text())`** / **`path.write_text(json.dumps(...))`**.
- **Generator ho `next(..., None)`** — elementu dahuluk ne'ebé loos, ka `None`.
- **`max(gerador, default=None)`**.
- **`strftime("%Y-%m-%dT%H:%M:%S")`**.

**4. Esplikasaun liña-ba-liña**

- **Liña 119** — `def _load_state() -> dict:`
- **Liña 120** — `try:`
- **Liña 121** — `return json.loads(STATE_FILE.read_text())`.
- **Liña 122** — `except (OSError, ValueError):` — arkivu la iha / JSON aat.
- **Liña 123** — `return {}`.
- **Liña 126** — `def _save_state(state: dict) -> None:`
- **Liña 127** — `STATE_FILE.parent.mkdir(parents=True, exist_ok=True)`.
- **Liña 128** — `STATE_FILE.write_text(json.dumps(state))`.
- **Liña 131** — `def scan_once(backend=None) -> list:`
- **Liña 132** — Docstring: buka pasta tomak ba arkivu desde scan ikus; publika foun.
- **Liña 133** — `state = _load_state()`.
- **Liña 134** — `created_after = None`.
- **Liña 135** — `last = _parse_time(state.get("last_created_time"))`.
- **Liña 136** — `if last is not None:`
- **Liña 137** — `created_after = (last - OVERLAP).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")` — 2 minutu molok, formatu ba query Drive.
- **Liña 139** — `user_folders = drive_client.user_image_folders()` — `{folder_id: email}`.
- **Liña 140** — `if not user_folders:`
- **Liña 141** — `return []`.
- **Liña 142** — `files = drive_client.list_files_in_folders(list(user_folders), created_after=created_after)`.
- **Liña 143** — `outcomes = []`.
- **Liña 144** — `for meta in files:`
- **Liña 145** — `parent = next((p for p in meta.get("parents") or [] if p in user_folders), None)` — pasta agrikultór husi arkivu.
- **Liña 146** — `outcomes.append(_publish_safely(meta, backend, user_folders.get(parent)))`.
- **Liña 148** — `newest = max((f.get("createdTime") for f in files if f.get("createdTime")), default=None)` — string ISO bele kompara.
- **Liña 149** — `if newest and (last is None or _parse_time(newest) > last):`
- **Liña 150** — `state["last_created_time"] = newest`.
- **Liña 151** — `_save_state(state)`.
- **Liña 152** — `return outcomes`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `state = {"last_created_time": "2026-10-20T01:15:00.000Z"}` → `created_after = "2026-10-20T01:13:00"`. Drive fó arkivu 2 (ida publika ona) → outcomes `[already_published, published]` → `last_created_time` muda ba arkivu foun liu.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `_load_state` | `dict` |
| `_save_state(state: dict)` | `None` |
| `scan_once(backend)` | `list[dict]` |
| `user_folders` | `dict[str, str]` |
| `files` | `list[dict]` |

**7. Ligasaun OOP**

Not implemented in the current codebase: klase watcher — estadu iha arkivu JSON no variavel modulu (`_PUBLISHED`).

**8. Impaktu integrasaun**

`main` bolu `scan_once` iha `--once`, iha `start_change_feed`, no kada 600 s (rede seguransa). Uza `drive_client` (Modulu 03).

**9. Checkpoint ba principiante**

**P:** Tanba sá `OVERLAP` 2 minutu? La halo publika dala rua?
**R:** Drive bele lista arkivu tarde. Overlap evita lakon. Dala rua la problema: `_PUBLISHED` salta iha prosesu hanesan, no Spark `register_upload` salta duplikadu.

#### D.3.6 `NoBookmark`, `scan_changes` no `start_change_feed`

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 155-195}}

**2. Objetivu**

Uza **change feed** Drive (lista mudansa desde "bookmark"/token) — pedidu ida kada kontrola. Publika fotografia foun; pasta agrikultór foun aumenta ba `user_folders`. `start_change_feed` harii bookmark (se presiza) no halo scan tomak ida atu hakat.

**3. Fundamentu Python**

- **Herança exception**: `class NoBookmark(RuntimeError)`.
- **Muda dict ne'ebé simu hanesan parámetru** (`user_folders[meta["id"]] = ...`) — efeitu sekundáriu ba chamadór (dict mak mutável).
- **`continue`** — salta ba elementu tuirmai.

**4. Esplikasaun liña-ba-liña**

- **Liña 155** — `class NoBookmark(RuntimeError):`
- **Liña 156** — Docstring: seidauk iha bookmark (hahú dahuluk, ka depois factory reset).
- **Liña 159** — `def scan_changes(user_folders: dict, backend=None) -> list:`
- **Liña 160–161** — Docstring: publika fotografia foun husi change feed (pedidu ida); aumenta pasta agrikultór foun ba `user_folders`.
- **Liña 162** — `state = _load_state()`.
- **Liña 163** — `token = state.get("changes_page_token")`.
- **Liña 164** — `if not token:`
- **Liña 165** — `raise NoBookmark(...)`.
- **Liña 166** — `changes, new_token = drive_client.list_changes(token)`.
- **Liña 167** — `images_root = drive_client.images_folder_id()` — pasta `/images`.
- **Liña 168** — `outcomes = []`.
- **Liña 169** — `for change in changes:`
- **Liña 170** — `meta = change.get("file") or {}`.
- **Liña 171** — `if change.get("removed") or not meta or meta.get("trashed"):` — hamoos/lixu:
- **Liña 172** — `continue`.
- **Liña 173** — `parents = meta.get("parents") or []`.
- **Liña 174** — `if meta.get("mimeType") == drive_client.FOLDER_MIME_TYPE:` — pasta:
- **Liña 175** — `if images_root in parents:` — komentáriu: agrikultór foun.
- **Liña 176** — `user_folders[meta["id"]] = meta.get("name")` — naran pasta = email.
- **Liña 177** — `continue`.
- **Liña 178** — `parent = next((p for p in parents if p in user_folders), None)`.
- **Liña 179** — `if parent is None:` — komentáriu: la'ós fotografia agrikultór.
- **Liña 180** — `continue`.
- **Liña 181** — `outcomes.append(_publish_safely(meta, backend, user_folders[parent]))`.
- **Liña 182** — `state["changes_page_token"] = new_token`.
- **Liña 183** — `_save_state(state)` — bookmark foun.
- **Liña 184** — `return outcomes`.
- **Liña 187** — `def start_change_feed(backend=None) -> dict:`
- **Liña 188–189** — Docstring: bookmark change feed (se presiza), depois scan tomak ida; fó `{folder id: email}`.
- **Liña 190** — `state = _load_state()`.
- **Liña 191** — `if not state.get("changes_page_token"):`
- **Liña 192** — `state["changes_page_token"] = drive_client.get_start_page_token()`.
- **Liña 193** — `_save_state(state)`.
- **Liña 194** — `scan_once(backend=backend)`.
- **Liña 195** — `return drive_client.user_image_folders()`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* token `"1234"` → `list_changes` fó mudansa 3: (a) pasta foun `agrikultor2@example.com` iha `/images` → aumenta ba `user_folders`; (b) fotografia iha pasta agrikultór → publika; (c) arkivu iha lixu → salta. Token foun `"1240"` rai.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `NoBookmark` | klase exception (`RuntimeError`) |
| `changes` | `list[dict]` |
| `new_token`, `token` | `str` |
| `scan_changes` return | `list[dict]` |
| `start_change_feed` return | `dict[str, str]` |

**7. Ligasaun OOP**

**Herança**: `NoBookmark` → `RuntimeError` → `Exception`. `main` kaer `except NoBookmark:` atu hahú fali. `user_folders` mak objetu dict partilla entre chamada (referénsia) — muda iha funsaun ida haree iha `main`.

**8. Impaktu integrasaun**

Factory reset (Modulu 03/06) hamoos arkivu estadu → `NoBookmark` → watcher publika fali fotografia hotu.

**9. Checkpoint ba principiante**

**P:** Tanba sá change feed di'ak liu duke buka pasta tomak kada segundu?
**R:** Change feed fó **mudansa de'it** ho pedidu ida — lalais no uza kuota Drive uitoan (< 2% iha 1 s tuir komentáriu liña 25).

#### D.3.7 `next_interval` no `main` — loop watcher

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 198-256}}

**2. Objetivu**

`next_interval`: polling lalais (1 s) bainhira fotografia mai, neineik to'o 3 s bainhira nonok. `main`: harii backend, opsaun `--once`, no loop infinitu: `drain` fila lokál, hahú change feed, `scan_changes`, scan tomak kada 600 s, trata erru, hein.

**3. Fundamentu Python**

- **`hasattr(backend, "drain")`** — duck typing (`KafkaBackend` la iha `drain`).
- **`outcomes += lista`** — aumenta lista.
- **`bool(lista)`** — `True` se la mamuk.
- **`except` barak** tuir orden: espesífiku uluk (`DriveNotConfigured`, `NoBookmark`), jerál ikus (`Exception`).

**4. Esplikasaun liña-ba-liña**

- **Liña 198** — `def next_interval(current: float, found_new: bool) -> float:`
- **Liña 199** — Docstring: lalais bainhira fotografia mai; neineik to'o `IDLE_POLL_SECONDS` bainhira nonok.
- **Liña 200** — `if found_new:`
- **Liña 201** — `return POLL_SECONDS`.
- **Liña 202** — `return min(IDLE_POLL_SECONDS, current * 1.5)` — aumenta 50% kada volta, maximu 3 s.
- **Liña 205** — `def main() -> int:`
- **Liña 206** — `p = argparse.ArgumentParser(...)`.
- **Liña 207** — `p.add_argument("--once", ...)` — scan dala ida no sai.
- **Liña 208** — `args = p.parse_args()`.
- **Liña 210** — `from kafka.producer import get_backend`.
- **Liña 211** — `backend = get_backend(KAFKA_TOPIC)` — `ResilientBackend` (default).
- **Liña 212** — `if args.once:`
- **Liña 213** — `try:`
- **Liña 214** — `print(f"[drive_watcher] {scan_once(backend=backend)}")`.
- **Liña 215** — `return 0`.
- **Liña 216** — `except drive_client.DriveNotConfigured as e:` — Drive seidauk autoriza.
- **Liña 217** — `print(f"[drive_watcher] {e}")`.
- **Liña 218** — `return 1`.
- **Liña 220–222** — `print(...)` — "Following Google Drive's change feed ... (every 1.0s ... up to 3.0s when quiet) -> Kafka '<topic>'".
- **Liña 223** — `user_folders, interval, last_full_scan = None, POLL_SECONDS, time.monotonic()`.
- **Liña 224** — `while True:`
- **Liña 225** — `found_new = False`.
- **Liña 226** — `try:`
- **Liña 227** — Komentáriu: haruka buat ne'ebé rai lokál bainhira Kafka mate.
- **Liña 228** — `if hasattr(backend, "drain"):`
- **Liña 229** — `backend.drain()`.
- **Liña 230** — `if user_folders is None:` — volta dahuluk ka depois reset:
- **Liña 231** — `user_folders = start_change_feed(backend=backend)`.
- **Liña 232** — `last_full_scan = time.monotonic()`.
- **Liña 233** — `outcomes = scan_changes(user_folders, backend=backend)`.
- **Liña 234** — `if time.monotonic() - last_full_scan >= FULL_SCAN_SECONDS:`
- **Liña 235** — Komentáriu: rede seguransa ba buat ne'ebé change feed lakon.
- **Liña 236** — `outcomes += scan_once(backend=backend)`.
- **Liña 237** — `last_full_scan = time.monotonic()`.
- **Liña 238** — `new = [o for o in outcomes if o["status"] == "published"]`.
- **Liña 239** — `found_new = bool(new)`.
- **Liña 240** — `if new:`
- **Liña 241** — `print(f"[drive_watcher] {len(new)} new file(s) published")`.
- **Liña 242** — `except drive_client.DriveNotConfigured as e:`
- **Liña 243** — `print(f"[drive_watcher] {e}")`.
- **Liña 244** — `return 1` — para (la iha autorizasaun).
- **Liña 245** — `except NoBookmark:`
- **Liña 246** — `print(...)` — "Bookmark gone (factory reset?) — publishing every photo in Drive again".
- **Liña 247** — `_PUBLISHED.clear()`.
- **Liña 248** — `user_folders, found_new = None, True` — hahú fali volta tuirmai.
- **Liña 249** — `except Exception as e:` — komentáriu: rede problema — kontinua haree.
- **Liña 250** — `print(f"[drive_watcher] scan failed, retrying: {e}")`.
- **Liña 251** — `interval = next_interval(interval, found_new)`.
- **Liña 252** — `time.sleep(interval)`.
- **Liña 255** — `if __name__ == "__main__":`
- **Liña 256** — `sys.exit(main())`.

**5. Walkthrough ezekusaun**

Intervalu iha tempu nonok: 1.0 → 1.5 → 2.25 → 3.0 → 3.0... Fotografia foun mai → fila ba 1.0. *Ezemplu ilustrativu:* internet monu → "scan failed, retrying: ..." → watcher kontinua.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `next_interval` | `(float, bool) -> float` |
| `backend` | `ResilientBackend ka KafkaBackend` |
| `user_folders` | `dict ka None` |
| `interval`, `last_full_scan` | `float` |
| `outcomes`, `new` | `list[dict]` |
| `main` return | `int` (kódigu saída) |

**7. Ligasaun OOP**

`hasattr` = **duck typing**: kódigu la pergunta "klase saida?" maibé "iha métodu `drain`?". Objetu `backend` uza durante loop tomak.

**8. Impaktu integrasaun**

Watcher mak produtór prinsipál topic. Bainhira Kafka mate, `ResilientBackend` rai eventu lokál, no liña 228–229 haruka sira bainhira Kafka fila.

**9. Checkpoint ba principiante**

**P:** Saida mak akontese se Drive seidauk autoriza (`DriveNotConfigured`)?
**R:** `main` print mensajen no fó `1` — watcher para, tanba koko fali la ajuda to'o ema autoriza Drive.
