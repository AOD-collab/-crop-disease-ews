
### D.3 `storage/image_storage.py` — dalan `gdrive:` no kópia lokál

#### D.3.1 Dalan lokál no dalan Drive

**1. Kódigu orijinál**

{{code storage/image_storage.py 1-62}}

**2. Objetivu**

Define oinsá sistema rai "fatin fotografia": fotografia Drive hanesan `"gdrive:<drive_file_id>"`, fotografia lokál tuan hanesan dalan. Funsaun konverte no hatene tipu dalan.

**3. Fundamentu Python**

- **`Path.parts`** — tuple parte dalan (`("storage", "images", "a.jpg")`).
- **`Path(*lista)`** — harii dalan husi parte.
- **`p.as_posix()`** — dalan ho `/`.
- **Slicing string** `stored[len(PREFIX):]`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: fatin bytes fotografia — fotografia Drive (`"gdrive:<file id>"`) ho kópia lokál iha `DOWNLOADED_PHOTOS_DIR`, no fotografia disku lokál tuan.
- **Liña 4** — Remata docstring.
- **Liña 6** — `import re`.
- **Liña 7** — `from pathlib import Path`.
- **Liña 9** — `BASE_DIR`, `IMAGE_STORAGE_PATH`, `DOWNLOADED_PHOTOS_DIR`.
- **Liña 12** — `def resolve_image_path(stored: str) -> Path:`
- **Liña 13–14** — Docstring: `images.image_path` → dalan loloos iha makina ne'e (mós hadi'a dalan husi makina seluk); hamosu `FileNotFoundError`.
- **Liña 15** — `p = Path(stored)`.
- **Liña 16** — `if p.exists():`
- **Liña 17** — `return p`.
- **Liña 19** — `if not p.is_absolute():` — dalan relativu:
- **Liña 20** — `candidate = (BASE_DIR / p).resolve()`.
- **Liña 21** — `if candidate.exists():`
- **Liña 22** — `return candidate`.
- **Liña 24** — `parts = p.parts`.
- **Liña 25** — `if "images" in parts:` — dalan husi makina seluk ho pasta `images`:
- **Liña 26** — `tail = Path(*parts[parts.index("images") + 1:])` — parte depois `images`.
- **Liña 27** — `candidate = (IMAGE_STORAGE_PATH / tail).resolve()`.
- **Liña 28** — `if candidate.exists():`
- **Liña 29** — `return candidate`.
- **Liña 31** — `raise FileNotFoundError(stored)`.
- **Liña 34** — `def to_stored_path(file_path: Path) -> str:`
- **Liña 35** — Docstring: forma relativu ba projetu ne'ebé rai iha `images.image_path`.
- **Liña 36** — `try:`
- **Liña 37** — `rel = Path(file_path).resolve().relative_to(BASE_DIR.resolve())`.
- **Liña 38** — `return rel.as_posix()`.
- **Liña 39** — `except ValueError:`
- **Liña 40** — Komentáriu: armazenamentu iha liur projetu (`IMAGE_STORAGE_PATH` rasik).
- **Liña 41** — `return str(Path(file_path).resolve())`.
- **Liña 44–46** — Komentáriu seksaun: armazenamentu Google Drive (upload foun husi Android).
- **Liña 47** — Komentáriu: fotografia Drive rai hanesan `"gdrive:<drive_file_id>"` iha `images.image_path`.
- **Liña 48** — `DRIVE_PATH_PREFIX = "gdrive:"`.
- **Liña 51** — `def is_drive_path(stored: str) -> bool:`
- **Liña 52** — `return bool(stored) and stored.startswith(DRIVE_PATH_PREFIX)`.
- **Liña 55** — `def to_drive_path(drive_file_id: str) -> str:`
- **Liña 56** — `return f"{DRIVE_PATH_PREFIX}{drive_file_id}"`.
- **Liña 59** — `def drive_file_id_from_path(stored: str) -> str:`
- **Liña 60** — `if not is_drive_path(stored):`
- **Liña 61** — `raise ValueError(f"Not a Google Drive image path: {stored!r}")`.
- **Liña 62** — `return stored[len(DRIVE_PATH_PREFIX):]`.

**5. Walkthrough ezekusaun**

`to_drive_path("1AbCdEf")` → `"gdrive:1AbCdEf"`; `is_drive_path("gdrive:1AbCdEf")` → `True`; `drive_file_id_from_path("gdrive:1AbCdEf")` → `"1AbCdEf"`; `drive_file_id_from_path("a.jpg")` → `ValueError`.

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `resolve_image_path(stored: str)` | `Path` | `FileNotFoundError` |
| `to_stored_path(file_path: Path)` | `str` | — |
| `is_drive_path(stored: str)` | `bool` | — |
| `to_drive_path(drive_file_id: str)` | `str` | — |
| `drive_file_id_from_path(stored: str)` | `str` | `ValueError` |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun; `Path` mak klase biblioteka.)

**8. Impaktu integrasaun**

Watcher (`to_drive_path` iha eventu), Spark (`is_drive_path`, `drive_file_id_from_path`), `pipeline_submit`, `results`.

**9. Checkpoint ba principiante**

**P:** Tanba sá sistema rai `"gdrive:<id>"` no la'ós URL tomak?
**R:** Prefiksu ki'ik fasil atu hatene tipu fatin (`startswith`) no ID mak buat ne'ebé API Drive presiza.

#### D.3.2 `load_image_bytes` no kópia lokál

**1. Kódigu orijinál**

{{code storage/image_storage.py 65-109}}

**2. Objetivu**

Foti bytes fotografia (Drive ka disku). Jere kópia lokál fotografia Drive iha `DOWNLOADED_PHOTOS_DIR/<user_id>/<image_id>.jpg|.png` — ID tenke seguru (la bele sai husi pasta).

**3. Fundamentu Python**

- **`raise FileNotFoundError(...) from e`** — troka tipu erru, rai kauza.
- **`bytes.startswith(b"\x89PNG")`** — assinatura PNG.
- **`base.with_name(base.name + ext)`** — aumenta estensaun.

**4. Esplikasaun liña-ba-liña**

- **Liña 65** — `def load_image_bytes(stored: str) -> bytes:`
- **Liña 66** — Docstring: bytes imajen husi Drive ka disku; hamosu `FileNotFoundError`.
- **Liña 67** — `if is_drive_path(stored):`
- **Liña 68** — `from storage import drive_client`.
- **Liña 69** — `try:`
- **Liña 70** — `return drive_client.download_file(drive_file_id_from_path(stored))`.
- **Liña 71** — `except drive_client.DriveError as e:`
- **Liña 72** — `raise FileNotFoundError(f"{stored} ({e})") from e` — tipu erru hanesan ba Drive no disku.
- **Liña 73** — `return resolve_image_path(stored).read_bytes()`.
- **Liña 76** — Komentáriu: ID tenke UUID simples, atu dalan labele hatudu liur `DOWNLOADED_PHOTOS_DIR`.
- **Liña 77** — `_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")`.
- **Liña 80** — `def _local_copy_base(user_id, image_id):`
- **Liña 81** — `if not (user_id and image_id and _SAFE_ID_RE.match(str(user_id)) and _SAFE_ID_RE.match(str(image_id))):`
- **Liña 82** — `return None`.
- **Liña 83** — `return Path(DOWNLOADED_PHOTOS_DIR) / str(user_id) / str(image_id)`.
- **Liña 86** — `def downloaded_photo_path(user_id, image_id):`
- **Liña 87** — Docstring: kópia lokál fotografia Drive, ka `None`.
- **Liña 88** — `base = _local_copy_base(user_id, image_id)`.
- **Liña 89** — `if base is None:`
- **Liña 90** — `return None`.
- **Liña 91** — `for ext in (".jpg", ".png"):`
- **Liña 92** — `path = base.with_name(base.name + ext)`.
- **Liña 93** — `if path.is_file():`
- **Liña 94** — `return path`.
- **Liña 95** — `return None`.
- **Liña 98** — `def store_downloaded_photo(user_id: str, image_id: str, original_bytes: bytes) -> Path:`
- **Liña 99** — Docstring: rai fotografia Drive la muda (liuhusi arkivu temp); hamosu erru se falla.
- **Liña 100** — `ext = ".png" if original_bytes.startswith(b"\x89PNG") else ".jpg"`.
- **Liña 101** — `base = _local_copy_base(user_id, image_id)`.
- **Liña 102** — `if base is None:`
- **Liña 103** — `raise ValueError(f"unsafe user_id/image_id for a local copy: ...")`.
- **Liña 104** — `target = base.with_name(base.name + ext)`.
- **Liña 105** — `target.parent.mkdir(parents=True, exist_ok=True)`.
- **Liña 106** — `tmp = target.with_name(target.name + ".tmp")`.
- **Liña 107** — `tmp.write_bytes(original_bytes)`.
- **Liña 108** — `tmp.replace(target)` — atómiku.
- **Liña 109** — `return target`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `store_downloaded_photo("u-9", "a1b2", b"\xff\xd8...")` → `storage/downloaded_photos/u-9/a1b2.jpg`. `downloaded_photo_path("u-9", "a1b2")` → dalan ne'e. `downloaded_photo_path("../x", "a1")` → `None` (la seguru).

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `load_image_bytes(stored)` | `bytes` | `FileNotFoundError` |
| `_local_copy_base` | `Path ka None` | — |
| `downloaded_photo_path` | `Path ka None` | — |
| `store_downloaded_photo` | `Path` | `ValueError`, `OSError` |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun.)

**8. Impaktu integrasaun**

Spark `process_event`: `downloaded_photo_path` (uza kópia se iha) no `load_image_bytes` (download); `save_downloaded_photo` → `store_downloaded_photo` (D.4). Kópia sira ne'e mak cache ne'ebé sistema la hamoos automátiku.

**9. Checkpoint ba principiante**

**P:** Tanba sá `load_image_bytes` troka `DriveError` ba `FileNotFoundError`?
**R:** Atu chamadór trata fotografia la iha ho maneira ida, la importa se husi Drive ka disku.

### D.4 Lado Drive iha Spark (download no kontrola)

#### D.4.1 `spark/streaming_job.py` 130–150 — download paralelu

**1. Kódigu orijinál**

{{code spark/streaming_job.py 130-150}}

**2. Objetivu**

Iha `process_event`, hahú download Drive **iha tempu hanesan** ho validasaun eventu; uza kópia lokál se iha; depois foti bytes no rai kópia.

**3. Fundamentu Python**

`ThreadPoolExecutor.submit` fó `Future`; `future.result()` hein; `future.cancel()` koko kansela (haree Modulu 01 A.3.17).

**4. Esplikasaun liña-ba-liña**

- **Liña 130** — Komentáriu: hahú download Drive bainhira kontrola eventu; uza kópia lokál se iha.
- **Liña 131** — `download = None`.
- **Liña 132** — `local_copy = None`.
- **Liña 133** — `if is_drive_path(event.get("image_path") or ""):` — fotografia Drive.
- **Liña 134** — `local_copy = downloaded_photo_path(event.get("user_id"), event.get("image_id"))`.
- **Liña 135** — `if local_copy is None:`
- **Liña 136** — `download = _DOWNLOADS.submit(load_image_bytes, event["image_path"])` — thread `drive-download` (16).
- **Liña 137** — `try:`
- **Liña 138** — `event = clean_event(event)` — inklui `_clean_drive_event` (D.4.2).
- **Liña 139** — `except Exception:`
- **Liña 140** — `if download is not None:`
- **Liña 141** — `download.cancel()`.
- **Liña 142** — `raise`.
- **Liña 143** — `lap("validate_s")`.
- **Liña 144** — `scale_run = scale_test_run_for(event)`.
- **Liña 146** — `prediction_started_at = datetime.now(timezone.utc)`.
- **Liña 147** — `if local_copy is not None:`
- **Liña 148** — `original_bytes = local_copy.read_bytes()` — la presiza Drive.
- **Liña 149** — `elif download is not None:`
- **Liña 150** — `original_bytes = download.result()` — hein download; `FileNotFoundError` sa'e se Drive falla.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* Reprocess fotografia ne'ebé download ona → `local_copy` iha → la iha chamada Drive. Fotografia foun → download iha thread (~1.2 s) paralelu ho validasaun (~0.05 s).

**6. Tipu dadus**

`download`: `Future[bytes] ka None`; `local_copy`: `Path ka None`; `original_bytes`: `bytes`.

**7. Ligasaun OOP**

`_DOWNLOADS` mak objetu `ThreadPoolExecutor` nível modulu; `Future` mak objetu rezultadu futuru.

**8. Impaktu integrasaun**

Chamada Drive iha ne'e: `load_image_bytes` → `download_file` (ho retry D.2.5).

**9. Checkpoint ba principiante**

**P:** Saida mak akontese se download falla?
**R:** `download.result()` hamosu `FileNotFoundError`; `process_and_persist` kaer no rai fotografia `FAILED`.

#### D.4.2 `spark/preprocessing.py` — `_clean_drive_event`, `_registered_from_drive`, `save_downloaded_photo`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 90-116}}

{{code spark/preprocessing.py 201-208}}

**2. Objetivu**

Kontrola arkivu Drive: ID hanesan ho dalan; se rejista ona husi watcher, la husu Drive fali; se lae, arkivu tenke iha, la iha lixu, no iha etiketa `image_id`. Rai kópia fotografia download (nunka halo diagnóstiku falla).

**3. Fundamentu Python**

`except (A, B) as e`; `(meta.get("appProperties") or {}).get("image_id")`.

**4. Esplikasaun liña-ba-liña**

- **Liña 90** — `def _clean_drive_event(event: dict) -> dict:`
- **Liña 91** — Docstring: arkivu Drive tenke iha, la iha lixu, no lori `image_id` ne'e.
- **Liña 92** — `file_id = drive_file_id_from_path(event["image_path"])`.
- **Liña 93** — `if event.get("drive_file_id") and event["drive_file_id"] != file_id:`
- **Liña 94** — `raise InvalidEvent("drive_file_id does not match image_path")`.
- **Liña 95** — `event["drive_file_id"] = file_id`.
- **Liña 96** — `if not SPARK_RECHECK_DRIVE_FILE and _registered_from_drive(event):`
- **Liña 97** — `return event` — komentáriu: kontrola ona; husu Drive fali neineik.
- **Liña 98** — `try:`
- **Liña 99** — `meta = drive_client.get_file_metadata(file_id)` — chamada API (D.2.5).
- **Liña 100** — `except (drive_client.DriveError, drive_client.DriveNotConfigured) as e:`
- **Liña 101** — `raise InvalidEvent(f"Image file not found in Google Drive: {file_id} ({e})")`.
- **Liña 102** — `if meta.get("trashed"):`
- **Liña 103** — `raise InvalidEvent(f"Image file was deleted from Google Drive: {file_id}")`.
- **Liña 104** — `if (meta.get("appProperties") or {}).get("image_id") != event["image_id"]:` — etiketa Android.
- **Liña 105–107** — `raise InvalidEvent(...)` — "not tagged with image_id ...".
- **Liña 108** — `return event`.
- **Liña 111** — `def _registered_from_drive(event: dict) -> bool:`
- **Liña 112** — Docstring: loos bainhira arkivu Drive ne'e rejista ona ba utilizadór ne'e.
- **Liña 113** — `with SessionLocal() as session:`
- **Liña 114** — `image = session.get(Image, event["image_id"])`.
- **Liña 115–116** — `return (image is not None and image.source == "android_drive_upload" and image.image_path == event["image_path"] and image.user_id == event["user_id"])`.
- **Liña 201** — `def save_downloaded_photo(user_id: str, image_id: str, original_bytes: bytes) -> None:`
- **Liña 202** — Docstring: rai kópia lokál fotografia Drive; nunka halo diagnóstiku falla.
- **Liña 203** — `if not SAVE_DOWNLOADED_PHOTOS:`
- **Liña 204** — `return`.
- **Liña 205** — `try:`
- **Liña 206** — `store_downloaded_photo(user_id, image_id, original_bytes)` — D.3.2.
- **Liña 207** — `except Exception as e:`
- **Liña 208** — `print(f"[preprocessing] Could not save downloaded photo {image_id}: {e}")`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* eventu `submit` antigu ba fotografia ne'ebé agrikultór hamoos iha Drive → `_registered_from_drive` loos maibé `SPARK_RECHECK_DRIVE_FILE=False` → la husu Drive; download depois falla → `FAILED`. Ho `SPARK_RECHECK_DRIVE_FILE=True` → `meta.trashed = True` → `InvalidEvent("Image file was deleted from Google Drive: ...")`.

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `_clean_drive_event(event: dict)` | `dict` | `InvalidEvent` |
| `_registered_from_drive(event: dict)` | `bool` | erru BD |
| `save_downloaded_photo(...)` | `None` | la iha |

**7. Ligasaun OOP**

`InvalidEvent` (herda `Exception`) — haree Modulu 01. `drive_client.DriveError` (herda `Exception`, ho `status_code`).

**8. Impaktu integrasaun**

`appProperties.image_id` hakerek husi Android (`DriveUploader` properties `"image_id"`, D.9.3) — kontrola ne'e liga telemóvel ho Spark.

**9. Checkpoint ba principiante**

**P:** Tanba sá `SPARK_RECHECK_DRIVE_FILE` default `False`?
**R:** Watcher no `register_upload` hetan ona metadata Drive bainhira rejista; husu dala rua neineik (chamada API ida kada fotografia).

### D.5 `kafka/drive_watcher.py` — deteta upload foun (lado Drive)

*Nota:* Modulu 02 esplika arkivu ne'e husi lado Kafka (payload, backend). Iha ne'e ita fokus ba **chamada Google Drive**: change feed, buka pasta, metadata, bookmark.

#### D.5.1 Konfigurasaun watcher, GPS no tempu

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 1-57}}

**2. Objetivu**

Define watcher Drive → Kafka (la iha base de dadus), intervalu polling (pedidu change-feed ida kada segundu < 2% kuota Drive), arkivu estadu (bookmark), overlap 2 minutu, regex UUID, memória arkivu publika ona; limpeza GPS no tempu husi `appProperties`.

**3. Fundamentu Python**

`OrderedDict`, `timedelta`, `float(os.getenv(...))`, `try/except (KeyError, TypeError, ValueError)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: Drive → Kafka; haree pasta `/images/<email>/` agrikultór hotu; publika eventu ida kada fotografia foun (metadata de'it); la iha BD; MANUAL la publika.
- **Liña 6** — Uza: `python -m kafka.drive_watcher [--once]`.
- **Liña 7** — Remata docstring.
- **Liña 8–16** — Import padraun: `argparse`, `json` (arkivu estadu), `os`, `re`, `sys`, `time`, `OrderedDict`, `datetime/timedelta/timezone`, `Path`.
- **Liña 18** — `sys.path.insert(...)` — pasta raiz projetu.
- **Liña 20** — `BASE_DIR`, `KAFKA_TOPIC`.
- **Liña 21** — `drive_client` — chamada Drive hotu.
- **Liña 22** — `to_drive_path`.
- **Liña 23** — `ProcessingMode`, `get_processing_mode`.
- **Liña 25** — Komentáriu: pedidu change-feed ida kada kontrola: < 2% kuota Drive iha 1 s.
- **Liña 26** — `POLL_SECONDS` (default 1).
- **Liña 27** — `IDLE_POLL_SECONDS` (default 3).
- **Liña 28** — `FULL_SCAN_SECONDS` (default 600) — buka pasta tomak hanesan rede seguransa.
- **Liña 29** — `STATE_FILE` — `storage/drive_watcher_state.json` (bookmark + `last_created_time`).
- **Liña 30** — Komentáriu: scan fali uitoan molok arkivu ikus, se Drive lista tarde.
- **Liña 31** — `OVERLAP = timedelta(minutes=2)`.
- **Liña 33** — `_UUID_RE` — formatu `image_id` Android.
- **Liña 35** — Komentáriu: arkivu Drive publika ona husi prosesu ne'e.
- **Liña 36** — `_PUBLISHED` — `OrderedDict` ID arkivu Drive.
- **Liña 37** — `_PUBLISHED_MAX = 20000`.
- **Liña 40** — `def _device_gps(props: dict):`
- **Liña 41** — Docstring: (lat, lon) telemóvel husi appProperties, ka `None`.
- **Liña 42** — `try:`
- **Liña 43** — `lat, lon = float(props["gps_lat"]), float(props["gps_lon"])` — chave ne'ebé `DriveUploader` hakerek.
- **Liña 44** — `except (KeyError, TypeError, ValueError):`
- **Liña 45** — `return None`.
- **Liña 46** — `if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):`
- **Liña 47** — `return None`.
- **Liña 48** — `return lat, lon`.
- **Liña 51** — `def _parse_time(value):`
- **Liña 52** — `if not value:`
- **Liña 53** — `return None`.
- **Liña 54** — `try:`
- **Liña 55** — `return datetime.fromisoformat(str(value).replace("Z", "+00:00"))` — formatu `createdTime` Drive (`...Z`).
- **Liña 56** — `except ValueError:`
- **Liña 57** — `return None`.

**5. Walkthrough ezekusaun**

`appProperties = {"gps_lat": "-8.728100", "gps_lon": "125.566400"}` (husi Android) → `_device_gps` → `(-8.7281, 125.5664)`.

**6. Tipu dadus**

Haree Modulu 02 D.3.1–D.3.2: `POLL_SECONDS` `float`, `STATE_FILE` `Path`, `_device_gps` → `tuple ka None`, `_parse_time` → `datetime ka None`.

**7. Ligasaun OOP**

`ProcessingMode` mak enum (D.6.1). La iha klase foun iha bloku ne'e.

**8. Impaktu integrasaun**

Chave `gps_lat`/`gps_lon`/`captured_at_client`/`image_id`/`location` mak kontratu ho aplikasaun Android (`DriveUploader`).

**9. Checkpoint ba principiante**

**P:** Tanba sá watcher la uza base de dadus?
**R:** Dezeñu "stateless": watcher só hatene Drive no Kafka. Spark mak "gatekeeper" ne'ebé rejista fotografia iha BD.

#### D.5.2 `build_drive_event`, `publish_drive_file`, `_publish_safely`

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 60-116}}

**2. Objetivu**

Husi metadata Drive (resposta `FILE_METADATA_FIELDS`), harii eventu no publika. Kampu Drive ne'ebé uza: `id`, `name`, `mimeType`, `size`, `owners[0].emailAddress/displayName`, `createdTime`, `sha256Checksum`, `appProperties`.

**3. Fundamentu Python**

`owners[0] if owners else {}`; `str(meta["size"]) if ... else None`; `type(e).__name__`.

**4. Esplikasaun liña-ba-liña**

- **Liña 60** — `def build_drive_event(meta: dict, folder_email: str = None) -> dict:`
- **Liña 61** — Docstring: eventu Kafka ba fotografia Drive ida husi metadata; valór hotu testu ka `None`.
- **Liña 62** — `props = meta.get("appProperties") or {}` — etiketa Android.
- **Liña 63** — `owners = meta.get("owners") or []` — na'in arkivu = agrikultór (nia upload ho nia konta).
- **Liña 64** — `owner = owners[0] if owners else {}`.
- **Liña 65** — `file_id = meta.get("id") or ""`.
- **Liña 66** — `captured_at = _parse_time(props.get("captured_at_client"))`.
- **Liña 67** — `gps = _device_gps(props)`.
- **Liña 68** — `return {`
- **Liña 69** — `"kind": "drive_upload",`
- **Liña 70** — `"image_id": props.get("image_id") or None,`
- **Liña 71** — `"user_id": None,` — hetan husi `owner_email`.
- **Liña 72** — `"drive_file_id": file_id,`
- **Liña 73** — `"image_path": to_drive_path(file_id),`
- **Liña 74** — `"file_name": meta.get("name"),` — `<image_id>.jpg`.
- **Liña 75** — `"mime_type": meta.get("mimeType"),`
- **Liña 76** — `"size_bytes": str(meta["size"]) if meta.get("size") is not None else None,`
- **Liña 77** — `"owner_email": owner.get("emailAddress"),`
- **Liña 78** — `"owner_name": owner.get("displayName"),`
- **Liña 79** — `"folder_email": folder_email,` — naran pasta (email).
- **Liña 80** — `"uploaded_at": meta.get("createdTime"),` — relójiu Google.
- **Liña 81** — `"captured_at": captured_at.isoformat() if captured_at else None,`
- **Liña 82** — `"location": props.get("location"),`
- **Liña 83** — `"original_filename": props.get("original_filename") or meta.get("name"),`
- **Liña 84** — `"content_hash": meta.get("sha256Checksum"),` — Google kalkula SHA-256.
- **Liña 85** — `"device_gps_lat": f"{gps[0]}" if gps else None,`
- **Liña 86** — `"device_gps_lon": f"{gps[1]}" if gps else None,`
- **Liña 87** — `}`.
- **Liña 90** — `def publish_drive_file(meta: dict, backend=None, folder_email: str = None) -> dict:`
- **Liña 91** — Docstring: publika fotografia ida; status `published` | `held` | `already_published`.
- **Liña 92** — `file_id = meta.get("id", "")`.
- **Liña 93** — `image_id = (meta.get("appProperties") or {}).get("image_id") or None`.
- **Liña 94** — `if file_id in _PUBLISHED:`
- **Liña 95** — `return {"image_id": image_id, "status": "already_published"}`.
- **Liña 96** — `if get_processing_mode() == ProcessingMode.MANUAL:` — lee arkivu modu (D.6.1).
- **Liña 97** — `return {"image_id": image_id, "status": "held"}`.
- **Liña 98** — `event = build_drive_event(meta, folder_email)`.
- **Liña 99** — `backend.send(event)`.
- **Liña 100** — `_PUBLISHED[file_id] = None`.
- **Liña 101** — `while len(_PUBLISHED) > _PUBLISHED_MAX:`
- **Liña 102** — `_PUBLISHED.popitem(last=False)`.
- **Liña 103–104** — `print(...)` — "new photo ... from <email> (...) -> Kafka topic ... (Drive file <id>)".
- **Liña 105** — `return {"image_id": image_id, "status": "published"}`.
- **Liña 108** — `def _publish_safely(meta: dict, backend, folder_email) -> dict:`
- **Liña 109** — Docstring: nunka hamosu erru, atu arkivu aat ida la para scan.
- **Liña 110** — `try:`
- **Liña 111** — `return publish_drive_file(meta, backend=backend, folder_email=folder_email)`.
- **Liña 112** — `except Exception as e:` — arkivu ne'e de'it.
- **Liña 113** — `image_id = ...`.
- **Liña 114–115** — `print(...)` — "Could not publish Drive file <id>: <Tipu>: <mensajen badak>".
- **Liña 116** — `return {"image_id": image_id, "status": "error"}`.

**5. Walkthrough ezekusaun**

Haree Modulu 02 C.5 no D.3.3 ba payload tomak husi `meta` ilustrativu.

**6. Tipu dadus**

`meta`: `dict` (JSON API Drive); `build_drive_event` → `dict` (kampu 18); `publish_drive_file`/`_publish_safely` → `dict` (`image_id`, `status`).

**7. Ligasaun OOP**

`backend.send` — duck typing (Modulu 02 E.3).

**8. Impaktu integrasaun**

`owner_email` ne'e Spark uza atu hetan/harii agrikultór (`farmers.get_or_create_farmer`, D.6.3).

**9. Checkpoint ba principiante**

**P:** Tanba sá `uploaded_at` uza `createdTime` Google no la'ós relójiu telemóvel?
**R:** Relójiu telemóvel bele sala; relójiu Google konsistente ba agrikultór hotu (komentáriu liña 80).

#### D.5.3 Estadu, `scan_once`, change feed

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 119-195}}

**2. Objetivu**

Rai bookmark iha arkivu JSON. `scan_once`: buka pasta agrikultór hotu ba arkivu foun (`createdTime > last - 2 min`). `scan_changes`: lee change feed Drive desde bookmark (pedidu ida), deteta pasta agrikultór foun no fotografia foun. `start_change_feed`: harii bookmark + scan tomak.

**3. Fundamentu Python**

`json.loads(path.read_text())`; `next(gerador, None)`; `max(..., default=None)`; herança `NoBookmark(RuntimeError)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 119** — `def _load_state() -> dict:`
- **Liña 120** — `try:`
- **Liña 121** — `return json.loads(STATE_FILE.read_text())`.
- **Liña 122** — `except (OSError, ValueError):`
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
- **Liña 137** — `created_after = (last - OVERLAP).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")`.
- **Liña 139** — `user_folders = drive_client.user_image_folders()` — **chamada Drive**: lista sub-pasta `/images`.
- **Liña 140** — `if not user_folders:`
- **Liña 141** — `return []`.
- **Liña 142** — `files = drive_client.list_files_in_folders(list(user_folders), created_after=created_after)` — **chamada Drive**.
- **Liña 143** — `outcomes = []`.
- **Liña 144** — `for meta in files:`
- **Liña 145** — `parent = next((p for p in meta.get("parents") or [] if p in user_folders), None)`.
- **Liña 146** — `outcomes.append(_publish_safely(meta, backend, user_folders.get(parent)))`.
- **Liña 148** — `newest = max((f.get("createdTime") for f in files if f.get("createdTime")), default=None)`.
- **Liña 149** — `if newest and (last is None or _parse_time(newest) > last):`
- **Liña 150** — `state["last_created_time"] = newest`.
- **Liña 151** — `_save_state(state)`.
- **Liña 152** — `return outcomes`.
- **Liña 155** — `class NoBookmark(RuntimeError):`
- **Liña 156** — Docstring: seidauk iha bookmark (hahú dahuluk, ka depois factory reset).
- **Liña 159** — `def scan_changes(user_folders: dict, backend=None) -> list:`
- **Liña 160–161** — Docstring: publika fotografia foun husi change feed (pedidu ida); aumenta pasta agrikultór foun.
- **Liña 162** — `state = _load_state()`.
- **Liña 163** — `token = state.get("changes_page_token")`.
- **Liña 164** — `if not token:`
- **Liña 165** — `raise NoBookmark(...)`.
- **Liña 166** — `changes, new_token = drive_client.list_changes(token)` — **chamada Drive**.
- **Liña 167** — `images_root = drive_client.images_folder_id()`.
- **Liña 168** — `outcomes = []`.
- **Liña 169** — `for change in changes:`
- **Liña 170** — `meta = change.get("file") or {}`.
- **Liña 171** — `if change.get("removed") or not meta or meta.get("trashed"):`
- **Liña 172** — `continue`.
- **Liña 173** — `parents = meta.get("parents") or []`.
- **Liña 174** — `if meta.get("mimeType") == drive_client.FOLDER_MIME_TYPE:`
- **Liña 175** — `if images_root in parents:` — agrikultór foun (pasta foun iha `/images`).
- **Liña 176** — `user_folders[meta["id"]] = meta.get("name")`.
- **Liña 177** — `continue`.
- **Liña 178** — `parent = next((p for p in parents if p in user_folders), None)`.
- **Liña 179** — `if parent is None:` — la'ós fotografia agrikultór.
- **Liña 180** — `continue`.
- **Liña 181** — `outcomes.append(_publish_safely(meta, backend, user_folders[parent]))`.
- **Liña 182** — `state["changes_page_token"] = new_token`.
- **Liña 183** — `_save_state(state)`.
- **Liña 184** — `return outcomes`.
- **Liña 187** — `def start_change_feed(backend=None) -> dict:`
- **Liña 188–189** — Docstring: bookmark change feed (se presiza), depois scan tomak ida; fó `{folder id: email}`.
- **Liña 190** — `state = _load_state()`.
- **Liña 191** — `if not state.get("changes_page_token"):`
- **Liña 192** — `state["changes_page_token"] = drive_client.get_start_page_token()` — **chamada Drive**.
- **Liña 193** — `_save_state(state)`.
- **Liña 194** — `scan_once(backend=backend)`.
- **Liña 195** — `return drive_client.user_image_folders()`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* `drive_watcher_state.json`: `{"changes_page_token": "1260", "last_created_time": "2026-10-20T01:15:00.000Z"}`. Kada segundu: `list_changes("1260")` → mudansa 0 → token hanesan. Agrikultór upload → mudansa 1 → publika → token `"1263"`.

**6. Tipu dadus**

Haree Modulu 02 D.3.5–D.3.6. `state`: `dict` (`changes_page_token: str`, `last_created_time: str`).

**7. Ligasaun OOP**

Herança `NoBookmark(RuntimeError)`; `main` kaer atu hahú fali.

**8. Impaktu integrasaun**

`reset._clear_watcher_bookmark` (D.7) hamoos `STATE_FILE` → `NoBookmark` → publika fali fotografia hotu.

**9. Checkpoint ba principiante**

**P:** Change feed fó mós arkivu iha pasta `results`. Watcher publika sira?
**R:** Lae. Liña 178–180: arkivu ne'ebé la iha pasta `user_folders` (pasta `/images/<email>`) salta.

#### D.5.4 `next_interval` no `main`

**1. Kódigu orijinál**

{{code kafka/drive_watcher.py 198-256}}

**2. Objetivu**

Loop watcher: haruka fila lokál, hahú change feed, `scan_changes` kada 1–3 s, scan tomak kada 600 s, trata erru Drive (`DriveNotConfigured` para; `NoBookmark` hahú fali; seluk koko fali).

**3. Fundamentu Python**

`hasattr`; `except` barak tuir orden; `min(a, b * 1.5)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 198** — `def next_interval(current: float, found_new: bool) -> float:`
- **Liña 199** — Docstring: lalais bainhira fotografia mai; neineik to'o `IDLE_POLL_SECONDS`.
- **Liña 200** — `if found_new:`
- **Liña 201** — `return POLL_SECONDS`.
- **Liña 202** — `return min(IDLE_POLL_SECONDS, current * 1.5)`.
- **Liña 205** — `def main() -> int:`
- **Liña 206** — `p = argparse.ArgumentParser(...)`.
- **Liña 207** — `--once`.
- **Liña 208** — `args = p.parse_args()`.
- **Liña 210** — `from kafka.producer import get_backend`.
- **Liña 211** — `backend = get_backend(KAFKA_TOPIC)`.
- **Liña 212** — `if args.once:`
- **Liña 213** — `try:`
- **Liña 214** — `print(f"[drive_watcher] {scan_once(backend=backend)}")`.
- **Liña 215** — `return 0`.
- **Liña 216** — `except drive_client.DriveNotConfigured as e:`
- **Liña 217** — `print(f"[drive_watcher] {e}")`.
- **Liña 218** — `return 1`.
- **Liña 220–222** — `print(...)` — "Following Google Drive's change feed ...".
- **Liña 223** — `user_folders, interval, last_full_scan = None, POLL_SECONDS, time.monotonic()`.
- **Liña 224** — `while True:`
- **Liña 225** — `found_new = False`.
- **Liña 226** — `try:`
- **Liña 227** — Komentáriu: haruka buat ne'ebé rai lokál bainhira Kafka mate.
- **Liña 228** — `if hasattr(backend, "drain"):`
- **Liña 229** — `backend.drain()`.
- **Liña 230** — `if user_folders is None:`
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
- **Liña 241** — `print(...)` — "N new file(s) published".
- **Liña 242** — `except drive_client.DriveNotConfigured as e:` — token/pasta la iha.
- **Liña 243** — `print(f"[drive_watcher] {e}")`.
- **Liña 244** — `return 1`.
- **Liña 245** — `except NoBookmark:`
- **Liña 246** — `print(...)` — bookmark lakon (factory reset?) — publika fali fotografia hotu.
- **Liña 247** — `_PUBLISHED.clear()`.
- **Liña 248** — `user_folders, found_new = None, True`.
- **Liña 249** — `except Exception as e:` — rede monu, `DriveError` — kontinua haree.
- **Liña 250** — `print(f"[drive_watcher] scan failed, retrying: {e}")`.
- **Liña 251** — `interval = next_interval(interval, found_new)`.
- **Liña 252** — `time.sleep(interval)`.
- **Liña 255** — `if __name__ == "__main__":`
- **Liña 256** — `sys.exit(main())`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* internet Timor monu minutu 2 → `list_changes` hamosu erru (depois retry D.2.5) → "scan failed, retrying" kada 1–3 s → internet fila → `scan_changes` kontinua husi bookmark → fotografia hotu iha minutu 2 ne'e publika (la lakon).

**6. Tipu dadus**

Haree Modulu 02 D.3.7.

**7. Ligasaun OOP**

`hasattr(backend, "drain")` — duck typing.

**8. Impaktu integrasaun**

`run_all.py` hahú watcher (Modulu 06).

**9. Checkpoint ba principiante**

**P:** Tanba sá scan tomak kada 600 s maske change feed iha?
**R:** Rede seguransa: se change feed lakon mudansa ruma, scan tomak hetan arkivu ho `createdTime` foun.

### D.6 Modu processing, inventáriu Drive no agrikultór

#### D.6.1 `storage/processing_mode.py`

**1. Kódigu orijinál**

{{code storage/processing_mode.py 1-38}}

**2. Objetivu**

Rai modu AUTOMATIC/MANUAL iha arkivu JSON atu watcher bele lee la ho base de dadus. MANUAL: fotografia hein iha "Pending Review".

**3. Fundamentu Python**

- **Enum ho herança múltipla** `class ProcessingMode(str, enum.Enum)` — membru mós `str`.
- **`ProcessingMode("MANUAL")`** — harii membru husi valór (erru se valór la iha).
- **Temp + rename** atómiku.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: modu AUTOMATIC / MANUAL iha arkivu JSON atu watcher lee la ho BD; MANUAL: fotografia hein iha Pending Review.
- **Liña 4** — Remata docstring.
- **Liña 6** — `import enum`.
- **Liña 7** — `import json`.
- **Liña 8** — `import os`.
- **Liña 9** — `from pathlib import Path`.
- **Liña 11** — `from config.settings import BASE_DIR`.
- **Liña 13** — `MODE_FILE = Path(os.getenv("PROCESSING_MODE_FILE", str(BASE_DIR / "storage" / "processing_mode.json")))`.
- **Liña 16** — `class ProcessingMode(str, enum.Enum):`
- **Liña 17** — Komentáriu: upload hotu prosesa kedas.
- **Liña 18** — `AUTOMATIC = "AUTOMATIC"`.
- **Liña 19** — Komentáriu: la prosesa to'o admin klik "Process".
- **Liña 20** — `MANUAL = "MANUAL"`.
- **Liña 23** — `def get_processing_mode() -> ProcessingMode:`
- **Liña 24** — Docstring: modu agora; arkivu la iha ka aat = AUTOMATIC.
- **Liña 25** — `try:`
- **Liña 26** — `return ProcessingMode(json.loads(MODE_FILE.read_text(encoding="utf-8"))["mode"])`.
- **Liña 27** — `except (OSError, ValueError, KeyError, TypeError):`
- **Liña 28** — `return ProcessingMode.AUTOMATIC`.
- **Liña 31** — `def set_processing_mode(mode) -> ProcessingMode:`
- **Liña 32** — Docstring: rai modu (temp + rename, atu leitór nunka haree arkivu sorin-balun).
- **Liña 33** — `mode = ProcessingMode(mode)` — valida.
- **Liña 34** — `MODE_FILE.parent.mkdir(parents=True, exist_ok=True)`.
- **Liña 35** — `tmp = MODE_FILE.with_name(MODE_FILE.name + ".tmp")`.
- **Liña 36** — `tmp.write_text(json.dumps({"mode": mode.value}), encoding="utf-8")`.
- **Liña 37** — `tmp.replace(MODE_FILE)`.
- **Liña 38** — `return mode`.

**5. Walkthrough ezekusaun**

`set_processing_mode("MANUAL")` → arkivu `{"mode": "MANUAL"}` → `get_processing_mode()` → `ProcessingMode.MANUAL` → watcher fó `"held"`. `set_processing_mode("X")` → `ValueError`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `MODE_FILE` | `Path` |
| `ProcessingMode.AUTOMATIC/MANUAL` | `ProcessingMode` (no `str`) |
| `get_processing_mode()` | `-> ProcessingMode` |
| `set_processing_mode(mode)` | `-> ProcessingMode`; raise `ValueError` |

**7. Ligasaun OOP**

- **Herança múltipla**: `ProcessingMode` herda `str` **no** `enum.Enum`. Rezultadu: `ProcessingMode.MANUAL == "MANUAL"` loos, no `json.dumps` bele uza `.value`.
- **Atributu klase**: `AUTOMATIC`, `MANUAL` mak membru enum (atributu klase espesiál; objetu ida kada membru — singleton kada valór).

**8. Impaktu integrasaun**

Watcher lee (`publish_drive_file`); dashboard muda (botaun modu, Modulu 06); `drive_inventory` uza ba "Pending Review".

**9. Checkpoint ba principiante**

**P:** Tanba sá modu rai iha arkivu JSON no la'ós iha base de dadus?
**R:** Watcher la ko'alia ho base de dadus (dezeñu stateless); arkivu JSON fasil atu lee husi prosesu hotu.

#### D.6.2 `storage/drive_inventory.py`

**1. Kódigu orijinál**

{{code storage/drive_inventory.py 1-60}}

**2. Objetivu**

Hetan fotografia agrikultór iha Drive ne'ebé seidauk haruka ba prosesa: fotografia ho etiketa `image_id` app no la iha liña `images` (Pending Review, troka ba AUTOMATIC, `requeue_stuck --from-drive`). Haruka sira hanesan eventu `drive_upload`.

**3. Fundamentu Python**

- **Lista tuple** `[(meta, email), ...]`.
- **`set.update(gerador)`**.
- **Query SQLAlchemy `in_(lista)`** iha grupu 500.
- **Atribuisaun kadeia** `sent = failed = 0`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: fotografia agrikultór iha Drive seidauk haruka: ho etiketa `image_id` app no la iha liña `images` (Pending Review, troka ba AUTOMATIC, `requeue_stuck --from-drive`).
- **Liña 5** — Remata docstring.
- **Liña 7** — `from kafka.drive_watcher import _UUID_RE, build_drive_event` — reuza.
- **Liña 8** — `from storage import drive_client`.
- **Liña 9** — `from storage.database import Image`.
- **Liña 11** — `_CHUNK = 500`.
- **Liña 14** — `def farmer_photos() -> list:`
- **Liña 15** — Docstring: (metadata, email pasta) fotografia agrikultór hotu iha Drive, tuan uluk.
- **Liña 16** — `folders = drive_client.user_image_folders()`.
- **Liña 17** — `if not folders:`
- **Liña 18** — `return []`.
- **Liña 19** — `out = []`.
- **Liña 20** — `for meta in drive_client.list_files_in_folders(list(folders)):`
- **Liña 21** — `parent = next((p for p in meta.get("parents") or [] if p in folders), None)`.
- **Liña 22** — `out.append((meta, folders.get(parent)))`.
- **Liña 23** — `return out`.
- **Liña 26** — `def uploaded_at(meta: dict):`
- **Liña 27** — Docstring: `createdTime` Drive hanesan datetime, ka `None`.
- **Liña 28** — `from datetime import datetime`.
- **Liña 29** — `try:`
- **Liña 30** — `return datetime.fromisoformat(str(meta.get("createdTime")).replace("Z", "+00:00"))`.
- **Liña 31** — `except ValueError:`
- **Liña 32** — `return None`.
- **Liña 35** — `def _image_id(meta: dict) -> str:`
- **Liña 36** — `return (meta.get("appProperties") or {}).get("image_id") or ""`.
- **Liña 39** — `def unprocessed_photos(session) -> list:`
- **Liña 40** — Docstring: fotografia app iha Drive ne'ebé seidauk iha liña `images`.
- **Liña 41** — `photos = [(m, f) for m, f in farmer_photos() if _UUID_RE.match(_image_id(m))]` — só fotografia ho UUID (husi app).
- **Liña 42** — `ids = [_image_id(m) for m, _ in photos]`.
- **Liña 43** — `known = set()`.
- **Liña 44** — `for i in range(0, len(ids), _CHUNK):`
- **Liña 45** — `chunk = ids[i:i + _CHUNK]`.
- **Liña 46** — `known.update(r[0] for r in session.query(Image.image_id).filter(Image.image_id.in_(chunk)))` — `image_id` ne'ebé iha ona iha BD.
- **Liña 47** — `return [(m, f) for m, f in photos if _image_id(m) not in known]`.
- **Liña 50** — `def send_for_processing(photos: list, backend) -> tuple:`
- **Liña 51** — Docstring: publika eventu `drive_upload` ba kada ida; fó (sent, failed).
- **Liña 52** — `sent = failed = 0`.
- **Liña 53** — `for meta, folder_email in photos:`
- **Liña 54** — `try:`
- **Liña 55** — `backend.send(build_drive_event(meta, folder_email))`.
- **Liña 56** — `sent += 1`.
- **Liña 57** — `except Exception as e:` — kontinua haruka seluk.
- **Liña 58** — `print(f"[drive_inventory] could not send {_image_id(meta)}: {e}")`.
- **Liña 59** — `failed += 1`.
- **Liña 60** — `return sent, failed`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* modu MANUAL, Drive iha fotografia 12, BD iha 9 → `unprocessed_photos` fó 3 → dashboard hatudu iha "Pending Review" → admin klik → `send_for_processing` (ka `submit` ida-ida) → `(3, 0)`.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `farmer_photos()` | `list[tuple[dict, str]]` |
| `uploaded_at(meta)` | `datetime ka None` |
| `_image_id(meta)` | `str` |
| `unprocessed_photos(session)` | `list[tuple[dict, str]]` |
| `send_for_processing(photos, backend)` | `tuple[int, int]` |

**7. Ligasaun OOP**

`Image.image_id.in_(chunk)` — métodu atributu klase ORM. `backend.send` — duck typing.

**8. Impaktu integrasaun**

Formula "PENDING = Drive menus BD" ba modu MANUAL. Eventu hanesan ho watcher (`build_drive_event`) — Spark trata hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá query BD iha grupu 500?
**R:** Lista `IN (...)` naruk liu bele neineik ka liu limite base de dadus; grupu 500 seguru.

#### D.6.3 `storage/farmers.py`

**1. Kódigu orijinál**

{{code storage/farmers.py 1-37}}

**2. Objetivu**

Konta agrikultór mai husi konta Google ne'ebé na'in upload iha Drive; agrikultór la iha password rasik. Hetan utilizadór tuir email, ka harii foun ho password aleatóriu ne'ebé ema ida la hatene.

**3. Fundamentu Python**

- **`secrets.token_urlsafe(32)`** — testu aleatóriu seguru (kriptográfiku).
- **`except IntegrityError:` + `session.rollback()`** — trata "race" (prosesu rua harii hamutuk).
- **Argumentu ho naran** iha `User(...)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: konta agrikultór mai husi konta Google ne'ebé na'in upload iha Drive; agrikultór nunka iha password rasik.
- **Liña 4** — Remata docstring.
- **Liña 6** — `import secrets`.
- **Liña 8** — `from sqlalchemy.exc import IntegrityError`.
- **Liña 10** — `hash_password`, `normalize_email`.
- **Liña 11** — `User`, `Role`, `UserStatus`.
- **Liña 14** — `def get_or_create_farmer(session, email: str, display_name: str):`
- **Liña 15** — `email = normalize_email(email)`.
- **Liña 16** — `user = session.query(User).filter(User.email == email).first()`.
- **Liña 17** — `if user is not None:`
- **Liña 18** — `return user`.
- **Liña 19** — `user = User(`
- **Liña 20** — `username=email,`
- **Liña 21** — `full_name=display_name or email,`
- **Liña 22** — `email=email,`
- **Liña 23** — Komentáriu: password aleatóriu ema ida la hatene: agrikultór login ho Google iha telemóvel.
- **Liña 24** — `password_hash=hash_password(secrets.token_urlsafe(32)),`
- **Liña 25** — `role=Role.USER,`
- **Liña 26** — `status=UserStatus.ACTIVE,`
- **Liña 27** — `email_verified=True,` — komentáriu: verifika husi login Google.
- **Liña 28** — `)`.
- **Liña 29** — `session.add(user)`.
- **Liña 30** — `try:`
- **Liña 31** — `session.commit()`.
- **Liña 32** — `except IntegrityError:`
- **Liña 33** — Komentáriu: fotografia seluk husi agrikultór foun hanesan harii nia momentu uluk.
- **Liña 34** — `session.rollback()`.
- **Liña 35** — `return session.query(User).filter(User.email == email).one()`.
- **Liña 36** — `print(f"[farmers] New user from Google Drive: {email}")`.
- **Liña 37** — `return user`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* eventu dahuluk ho `owner_email = "Agrikultor@Example.com"` → email normaliza `agrikultor@example.com` → la iha → harii `User` (role USER) → print "New user from Google Drive: agrikultor@example.com". Fotografia rua hamutuk → ida hetan `IntegrityError` → rollback → foti utilizadór ne'ebé harii ona.

**6. Tipu dadus**

`session`: SQLAlchemy `Session`; `email`, `display_name`: `str`; return `User`.

**7. Ligasaun OOP**

Harii **objetu ORM** `User(...)` ho argumentu ho naran (konstrutór automátiku SQLAlchemy); `session.add(user)` hatama objetu iha *Unit of Work*. `Role.USER`, `UserStatus.ACTIVE` — membru enum.

**8. Impaktu integrasaun**

`storage/uploads.register_upload` (Spark) bolu funsaun ne'e ho `owner_email`/`owner_name` husi eventu (`storage/uploads.py` liña 49–50), no depois tau `event["user_id"] = user.user_id`. Tabela `users` (Modulu 04).

**9. Checkpoint ba principiante**

**P:** Tanba sá harii password aleatóriu ba agrikultór?
**R:** Koluna `password_hash` presiza valór, maibé agrikultór la login ho password (login ho Google). Password aleatóriu ne'ebé ema ida la hatene garante katak ema la bele login ho password ne'e.
