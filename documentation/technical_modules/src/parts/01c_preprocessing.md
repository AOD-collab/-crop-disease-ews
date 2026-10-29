
### D.3 `spark/preprocessing.py` — Spark nia serbisu ba fotografia ida

#### D.3.1 Docstring, import no `REQUIRED_EVENT_FIELDS`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 1-26}}

**2. Objetivu**

Foti biblioteka imajen (Pillow), konfigurasaun, funsaun armazenamentu, cliente Drive, espesifikasaun input modelu no modelu base de dadus. Define lista kampu obrigatóriu ba eventu.

**3. Fundamentu Python**

- **Lista** `["image_id", ...]` — sekuénsia ho orden.
- **Import naran privadu**: `from kafka.drive_watcher import _parse_time` — naran ho `_` mak konvensaun "internu", maibé Python permite import. Ne'e reuza funsaun parsing tempu ne'ebé hanesan ho watcher (DRY: *Don't Repeat Yourself*).
- **`from storage import drive_client`** — import modulu tomak (uza hanesan `drive_client.get_file_metadata`).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: Spark nia serbisu ba fotografia ida — kontrola eventu, foti fotografia, lee EXIF, preprocess ba CNN no rai imajen ne'ebé prosesa.
- **Liña 6** — `import re` — espresaun regulár (regex).
- **Liña 7** — `datetime, timezone` — tempu UTC.
- **Liña 8** — `BytesIO` — arkivu iha memória.
- **Liña 9** — `Path` — dalan arkivu.
- **Liña 11** — `PILImage, ImageOps` husi Pillow — loke/rai imajen; `ImageOps.exif_transpose` roda imajen tuir EXIF.
- **Liña 13–16** — Konfigurasaun: `IMAGE_STORAGE_PATH` (pasta fotografia lokál), `REVIEW_DIR` (pasta revizaun), `SAVE_PHOTOS_FOR_REVIEW`, `PROCESSED_IMAGES_DIR`, `ANALYSIS_MAX_PX` (1280), `SAVE_DOWNLOADED_PHOTOS`, `SPARK_RECHECK_DRIVE_FILE` (default `False`).
- **Liña 17** — Funsaun dalan imajen: `resolve_image_path`, `is_drive_path`, `drive_file_id_from_path`, `store_downloaded_photo`.
- **Liña 18** — `extract_image_metadata` (lee EXIF) no `GPS_COLUMNS` (`gps_latitude`, `gps_longitude`, `gps_altitude_m`).
- **Liña 19** — `as_aware_utc` — tau fuso UTC ba datetime ne'ebé la iha fuso.
- **Liña 20** — `_parse_time` — testu ISO → datetime (ka `None`).
- **Liña 21** — Modulu `drive_client` (API Google Drive, Modulu 03).
- **Liña 22** — `input_spec` — fó `img_size`, `mean`, `std` husi checkpoint modelu.
- **Liña 23** — Modelu ORM: `SessionLocal` (fábrika sesaun), `Image`, `User`, `ScaleTestItem`, `ScaleTestRun`.
- **Liña 26** — `REQUIRED_EVENT_FIELDS = ["image_id", "user_id", "image_path", "uploaded_at"]` — kampu ne'ebé eventu tenke iha iha nível Python.

**5. Walkthrough ezekusaun**

Bainhira `spark.streaming_job` import modulu ne'e, Python halai liña 1–26 dala ida. `REQUIRED_EVENT_FIELDS` sai lista ho string 4.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `REQUIRED_EVENT_FIELDS` | `list[str]` |
| `ANALYSIS_MAX_PX` | `int` |
| `IMAGE_STORAGE_PATH`, `REVIEW_DIR`, `PROCESSED_IMAGES_DIR` | `Path` |
| `SAVE_*`, `SPARK_RECHECK_DRIVE_FILE` | `bool` |

**7. Ligasaun OOP**

`Image`, `User`, `ScaleTestItem`, `ScaleTestRun` mak **klase ORM** (SQLAlchemy) — kada klase reprezenta tabela ida; kada objetu reprezenta liña ida (Modulu 04). `SessionLocal` mak fábrika (Teoria: padraun *Factory*) ne'ebé harii objetu `Session`.

**8. Impaktu integrasaun**

Modulu ne'e liga Spark ho `storage/` (base de dadus, Drive), `kafka/` (parsing tempu) no `ml/` (espesifikasaun input CNN).

**9. Checkpoint ba principiante**

**P:** Diferensa saida entre `REQUIRED_EVENT_FIELDS` iha ne'e no kampu obrigatóriu iha `parse_events`?
**R:** `parse_events` (Spark SQL) obriga `image_id`, `image_path`, `uploaded_at` + `owner_email` (Drive) ka `user_id` (submit). Iha Python, depois `register_upload` hatama `user_id`, `clean_event` obriga kampu 4 ne'e — inklui `user_id` ba eventu hotu.

#### D.3.2 `_parse_timestamp`, `normalize_location` no klase `InvalidEvent`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 29-46}}

**2. Objetivu**

Funsaun ajuda rua ba limpeza dadus (tempu no fatin) no klase erru espesiál ba eventu aat.

**3. Fundamentu Python**

- **Herança (inheritance)**: `class InvalidEvent(Exception): pass` — klase foun ne'ebé **herda** hotu husi `Exception`. `pass` = "la iha buat ida tan". Ita bele `raise InvalidEvent("...")` no `except InvalidEvent`.
- **`{value!r}`** iha f-string — hatudu valór ho aspas.
- **`str.strip()`, `str.title()`** — hasai espasu rohan; Letra Boot iha liafuan kada ida.
- **`re.sub(padraun, troka, testu)`** — troka testu ne'ebé tuir regex. `\s+` = espasu ida ka liu.
- **Return sedu** `return None`.

**4. Esplikasaun liña-ba-liña**

- **Liña 29** — `def _parse_timestamp(value, field_name: str):` — privadu (`_`).
- **Liña 30** — Docstring: testu ISO-8601 → datetime aware; hamosu `InvalidEvent` se la bele parse.
- **Liña 31** — `dt = _parse_time(value)` — `None` se valór mamuk ka la válidu.
- **Liña 32** — `if dt is None and value:` — parse falla **maske** iha valór (testu aat):
- **Liña 33** — `raise InvalidEvent(f"Invalid {field_name}: not a valid ISO-8601 timestamp ({value!r})")`.
- **Liña 34** — `return as_aware_utc(dt)` — datetime ho fuso UTC (ka `None` se valór mamuk).
- **Liña 37** — `def normalize_location(location) -> str:` — publiku; hint `-> str` maibé bele fó `None`.
- **Liña 38** — Docstring: regra hanesan `parse_events` — trim, espasu ida, Title Case.
- **Liña 39** — `if not location:` — `None` ka `""`:
- **Liña 40** — `return None`.
- **Liña 41** — `collapsed = re.sub(r"\s+", " ", str(location).strip())` — konverte ba str, hasai espasu rohan, troka espasu barak ba ida.
- **Liña 42** — `return collapsed.title() if collapsed else None` — Title Case; se mamuk depois limpeza, `None`.
- **Liña 45** — `class InvalidEvent(Exception):` — klase erru eventu aat.
- **Liña 46** — `pass` — la aumenta métodu ka atributu.

**5. Walkthrough ezekusaun**

- `_parse_timestamp("2026-10-20T01:15:00Z", "uploaded_at")` → `datetime(2026, 10, 20, 1, 15, tzinfo=UTC)`.
- `_parse_timestamp("ontem", "captured_at")` → `InvalidEvent("Invalid captured_at: not a valid ISO-8601 timestamp ('ontem')")`.
- `_parse_timestamp(None, "captured_at")` → `None`.
- `normalize_location("  aileu   vila ")` → `"Aileu Vila"`; `normalize_location("   ")` → `None`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception |
|---|---|---|---|
| `_parse_timestamp` | `value` (`str`/`None`, la iha hint), `field_name: str` | `datetime` ka `None` | `InvalidEvent` |
| `normalize_location` | `location` (objetu saida de'it) | `str` ka `None` (hint hatete `str`) | la iha |
| `InvalidEvent` | mensajen `str` (husi `Exception.__init__`) | — | — |

**7. Ligasaun OOP**

**Herança implementadu**: `InvalidEvent` → `Exception` → `BaseException` → `object`. Klase la iha `__init__` rasik; nia uza `__init__` husi `Exception` (herda). Sub-klase seluk iha projetu: `UploadRejected(InvalidEvent)` iha `storage/uploads.py` no `DuplicateImage(InvalidEvent)` iha `storage/duplicates.py` — ne'e **hierarkia herança** loloos. Tanba ne'e `except InvalidEvent` iha `_process_one` mós kaer `UploadRejected` (**polimorfizmu** liuhusi herança).

**8. Impaktu integrasaun**

`InvalidEvent` uza iha `streaming_job` (liña 209, 244, 275). `normalize_location` uza iha `clean_event` (liña 61) no sira seluk ne'ebé presiza regra hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá `InvalidEvent` la iha kódigu iha laran (só `pass`), maibé útil?
**R:** Nia **tipu** mak informasaun. Kódigu bele kaer `except InvalidEvent` atu trata eventu aat diferente husi erru seluk (ezemplu, print "Invalid event, skipping").

#### D.3.3 Funsaun `clean_event` — validasaun no limpeza eventu

**1. Kódigu orijinál**

{{code spark/preprocessing.py 49-87}}

**2. Objetivu**

Validasaun nível Python ba eventu ida: kampu obrigatóriu, tempu la iha futuru, `location` limpu, `user_id` iha base de dadus, no dalan imajen seguru (fotografia Drive → `_clean_drive_event`; fotografia lokál → tenke iha laran `IMAGE_STORAGE_PATH` no naran arkivu iha `image_id`).

**3. Fundamentu Python**

- **Dict comprehension** `{field: f(field) for field in (...)}`.
- **`dict.items()`** — fó pár `(chave, valór)` atu loop.
- **`dict.update({...})`** — muda/aumenta chave barak.
- **`raise ... from`** la uza iha ne'e; `raise InvalidEvent(...)` iha `except` troka erru ba tipu ne'ebé job hatene.
- **`Path.relative_to(base)`** — hamosu `ValueError` se dalan la iha laran `base` (proteje kontra *path traversal*).

**4. Esplikasaun liña-ba-liña**

- **Liña 49** — `def clean_event(event: dict) -> dict:`
- **Liña 50** — Docstring: validasaun no limpeza; hamosu `InvalidEvent` se eventu aat.
- **Liña 51** — `for key in REQUIRED_EVENT_FIELDS:`
- **Liña 52** — `if not event.get(key):` — la iha ka mamuk:
- **Liña 53** — `raise InvalidEvent(f"Missing required field: {key}")`.
- **Liña 55** — `now = datetime.now(timezone.utc)`.
- **Liña 56** — `times = {field: _parse_timestamp(event.get(field), field) for field in ("uploaded_at", "captured_at")}` — parse tempu rua.
- **Liña 57** — `for field, value in times.items():`
- **Liña 58** — `if value is not None and value > now:` — tempu iha futuru:
- **Liña 59** — `raise InvalidEvent(f"{field} is in the future: {value.isoformat()}")`.
- **Liña 60** — `event.update({field: value.isoformat() if value else None for field, value in times.items()})` — hakerek fali tempu iha formatu ISO padraun (UTC).
- **Liña 61** — `event["location"] = normalize_location(event.get("location"))`.
- **Liña 63** — `with SessionLocal() as session:` — loke sesaun base de dadus.
- **Liña 64** — `if session.get(User, event["user_id"]) is None:` — buka utilizadór tuir chave primária.
- **Liña 65** — `raise InvalidEvent(f"Unknown user_id: {event['user_id']}")`.
- **Liña 67** — `if is_drive_path(event["image_path"]):` — fotografia Drive:
- **Liña 68** — `return _clean_drive_event(event)` — D.3.4.
- **Liña 70** — `try:`
- **Liña 71** — `resolved = resolve_image_path(event["image_path"])` — konverte dalan rai ba dalan loloos iha disku.
- **Liña 72** — `except FileNotFoundError:`
- **Liña 73** — `raise InvalidEvent(f"Image file not found on disk: {event['image_path']}")`.
- **Liña 75** — `try:`
- **Liña 76** — `resolved.resolve().relative_to(Path(IMAGE_STORAGE_PATH).resolve())` — dalan absolutu tenke iha pasta armazenamentu.
- **Liña 77** — `except ValueError:`
- **Liña 78** — `raise InvalidEvent(f"Resolved image path escapes IMAGE_STORAGE_PATH: {resolved}")` — seguransa.
- **Liña 80** — `if event["image_id"] not in resolved.name:` — naran arkivu tenke iha `image_id`:
- **Liña 81–84** — `raise InvalidEvent(...)` — "image_id ... is not reflected in the resolved file name ...".
- **Liña 86** — `event["image_path"] = str(resolved)` — rai dalan loloos.
- **Liña 87** — `return event`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `{"image_id": "a1", "user_id": "u-9", "image_path": "gdrive:1AbC", "uploaded_at": "2026-10-20T01:15:00Z", "captured_at": None, "location": " aileu "}`. Kampu OK; `uploaded_at` → `"2026-10-20T01:15:00+00:00"`; `location` → `"Aileu"`; `User u-9` iha; dalan Drive → `_clean_drive_event`.

Se `captured_at = "2030-01-01T00:00:00Z"` → `InvalidEvent("captured_at is in the future: 2030-01-01T00:00:00+00:00")`. Se `image_path = "../../etc/passwd"` (dalan lokál) → `InvalidEvent("Resolved image path escapes IMAGE_STORAGE_PATH: ...")` ka "not found".

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `event` | `dict` (muda iha fatin — efeitu sekundáriu: dict orijinál mós muda) |
| `now` | `datetime` |
| `times` | `dict[str, datetime ka None]` |
| `session` | `sqlalchemy.orm.Session` |
| `resolved` | `Path` |
| return | `dict` |
| Exception | `InvalidEvent` |

**7. Ligasaun OOP**

`session.get(User, id)` — métodu objetu `Session` ne'ebé fó objetu `User` (ka `None`). `with SessionLocal() as session` uza protokolu **context manager** (`__enter__`/`__exit__`) husi klase `Session`. **Enkapsulamentu**: detalle SQL subar iha ORM; kódigu ne'e la hakerek SQL.

**8. Impaktu integrasaun**

Bolu husi `process_event` (streaming_job liña 138). Konsulta tabela `users` (Modulu 04). Ba Drive, kontinua iha `_clean_drive_event`.

**9. Checkpoint ba principiante**

**P:** Tanba sá kódigu kontrola `relative_to(IMAGE_STORAGE_PATH)`?
**R:** Seguransa: eventu ida labele obriga sistema lee arkivu iha liur pasta fotografia (ezemplu arkivu sistema). Se dalan "fakar" sai husi pasta, eventu rejeita.

#### D.3.4 `_clean_drive_event` no `_registered_from_drive`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 90-116}}

**2. Objetivu**

Ba fotografia Drive: `drive_file_id` tenke hanesan ho dalan; se fotografia rejista ona husi watcher ho dadus hanesan (no `SPARK_RECHECK_DRIVE_FILE = False`), la husu Drive fali (lalais liu). Se lae, husu Drive: arkivu tenke iha, la iha lixu, no iha etiketa `image_id` hanesan.

**3. Fundamentu Python**

- **Kaer exception barak**: `except (A, B) as e:`.
- **`(x or {}).get(k)`** — se `x` mak `None`, uza dict mamuk.
- **Espresaun `bool` naruk** ho `and` iha parénteze ba liña barak.

**4. Esplikasaun liña-ba-liña**

- **Liña 90** — `def _clean_drive_event(event: dict) -> dict:`
- **Liña 91** — Docstring: arkivu Drive tenke iha, la iha lixu, no lori `image_id` ne'e.
- **Liña 92** — `file_id = drive_file_id_from_path(event["image_path"])` — `"gdrive:1AbC"` → `"1AbC"`.
- **Liña 93** — `if event.get("drive_file_id") and event["drive_file_id"] != file_id:` — kampu la hanesan:
- **Liña 94** — `raise InvalidEvent("drive_file_id does not match image_path")`.
- **Liña 95** — `event["drive_file_id"] = file_id`.
- **Liña 96** — `if not SPARK_RECHECK_DRIVE_FILE and _registered_from_drive(event):`
- **Liña 97** — `return event` — komentáriu: kontrola ona iha rejistu; husu Drive fali neineik.
- **Liña 98** — `try:`
- **Liña 99** — `meta = drive_client.get_file_metadata(file_id)` — chamada API Drive.
- **Liña 100** — `except (drive_client.DriveError, drive_client.DriveNotConfigured) as e:`
- **Liña 101** — `raise InvalidEvent(f"Image file not found in Google Drive: {file_id} ({e})")`.
- **Liña 102** — `if meta.get("trashed"):` — arkivu iha lixu:
- **Liña 103** — `raise InvalidEvent(f"Image file was deleted from Google Drive: {file_id}")`.
- **Liña 104** — `if (meta.get("appProperties") or {}).get("image_id") != event["image_id"]:` — etiketa `image_id` ne'ebé Android tau la hanesan:
- **Liña 105–107** — `raise InvalidEvent(...)` — "Google Drive file ... is not tagged with image_id ...".
- **Liña 108** — `return event`.
- **Liña 111** — `def _registered_from_drive(event: dict) -> bool:`
- **Liña 112** — Docstring: loos bainhira arkivu Drive ne'e rejista ona ba utilizadór ne'e.
- **Liña 113** — `with SessionLocal() as session:`
- **Liña 114** — `image = session.get(Image, event["image_id"])`.
- **Liña 115–116** — `return (image is not None and image.source == "android_drive_upload" and image.image_path == event["image_path"] and image.user_id == event["user_id"])` — kondisaun 4 hotu tenke loos.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* eventu Drive foun, `register_upload` hakerek ona liña `images` ho `source="android_drive_upload"`, `image_path="gdrive:1AbC"`, `user_id="u-9"`. `_registered_from_drive` → `True`; `SPARK_RECHECK_DRIVE_FILE=False` → return kedas (la iha chamada API). Ba eventu `submit` antigu ho dalan seluk, kódigu husu Drive; se `meta = {"trashed": True}` → `InvalidEvent("Image file was deleted from Google Drive: 1AbC")`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `file_id` | `str` |
| `meta` | `dict` (resposta API Drive) |
| `image` | `Image` (ORM) ka `None` |
| `_registered_from_drive` return | `bool` |
| Exception | `InvalidEvent` |

**7. Ligasaun OOP**

`image.source`, `image.image_path`, `image.user_id` — asesu **atributu** objetu ORM. `drive_client.DriveError` mak klase exception iha `storage/drive_client.py` (Modulu 03).

**8. Impaktu integrasaun**

Bolu husi `clean_event` (liña 68). Uza `drive_client.get_file_metadata` (Modulu 03) no tabela `images` (Modulu 04). Etiketa `appProperties.image_id` hakerek husi aplikasaun Android bainhira upload (Modulu 03).

**9. Checkpoint ba principiante**

**P:** Tanba sá kódigu kontrola etiketa `image_id` iha Drive?
**R:** Atu garante katak arkivu Drive ne'e duni fotografia ne'ebé eventu ko'alia ba — evita analiza arkivu sala.

#### D.3.5 Funsaun `extract_metadata` — EXIF ba base de dadus

**1. Kódigu orijinál**

{{code spark/preprocessing.py 119-141}}

**2. Objetivu**

Lee EXIF husi fotografia **orijinál** (kámara, tamañu, GPS, tempu foti) no rai iha liña `images`. Regra GPS: GPS EXIF manán; se la iha, mantein posisaun telemóvel (`gps_source = "device"`).

**3. Fundamentu Python**

- **Tuple konstante** `_METADATA_COLUMNS = (...)`.
- **`setattr(obj, naran, valór)`** — muda atributu ho naran iha variavel (ezemplu `setattr(image, "camera_make", "samsung")` = `image.camera_make = "samsung"`).
- **Espresaun kondisionál iha argumentu**: `meta[name] if exif_gps else None`.

**4. Esplikasaun liña-ba-liña**

- **Liña 119** — `_METADATA_COLUMNS = ("camera_make", "camera_model", "image_width", "image_height")`.
- **Liña 122** — `def extract_metadata(event: dict, image_bytes: bytes) -> dict:`
- **Liña 123–124** — Docstring: rai EXIF fotografia ORIJINÁL iha nia liña; GPS EXIF manán; la iha nia, posisaun telemóvel mantein.
- **Liña 125** — `meta = extract_image_metadata(image_bytes)` — dict ho chave hotu (valór `None` se la iha).
- **Liña 126** — `with SessionLocal() as session:`
- **Liña 127** — `image = session.get(Image, event["image_id"])`.
- **Liña 128** — `if image is not None:`
- **Liña 129** — `for name in _METADATA_COLUMNS:`
- **Liña 130** — `setattr(image, name, meta[name])` — kámara no tamañu.
- **Liña 131** — `exif_gps = meta["gps_latitude"] is not None and meta["gps_longitude"] is not None` — EXIF iha GPS kompletu?
- **Liña 132** — `if exif_gps or image.gps_source != "device":` — muda GPS se EXIF iha, ka se GPS ida agora la'ós husi telemóvel:
- **Liña 133** — `for name in GPS_COLUMNS:`
- **Liña 134** — `setattr(image, name, meta[name] if exif_gps else None)` — tau GPS EXIF, ka hamamuk.
- **Liña 135** — `image.gps_source = "exif" if exif_gps else None`.
- **Liña 136** — `if image.captured_at is None and meta["captured_at"] is not None:` — tempu foti seidauk iha maibé EXIF iha:
- **Liña 137** — `image.captured_at = meta["captured_at"]`.
- **Liña 138** — `image.captured_at_source = meta["captured_at_source"]`.
- **Liña 139** — `event["captured_at"] = meta["captured_at"].isoformat()` — muda mós eventu.
- **Liña 140** — `session.commit()` — rai mudansa iha base de dadus.
- **Liña 141** — `return meta`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* EXIF fó `{"camera_make": "samsung", "camera_model": "SM-A145F", "image_width": 4000, "image_height": 3000, "gps_latitude": None, "gps_longitude": None, ...}`. Liña `images` iha `gps_source = "device"` (husi telemóvel). `exif_gps = False`; kondisaun liña 132 sala → GPS telemóvel mantein. Kámara no tamañu rai.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `image_bytes` | `bytes` |
| `meta` | `dict` |
| `exif_gps` | `bool` |
| return | `dict` |
| Efeitu sekundáriu | UPDATE liña `images`; muda `event["captured_at"]` |

**7. Ligasaun OOP**

`setattr` muda atributu objetu ORM `Image`; SQLAlchemy deteta mudansa no hakerek `UPDATE` bainhira `commit()` (padraun *Unit of Work* iha ORM). Projetu la define klase iha ne'e.

**8. Impaktu integrasaun**

Bolu husi `process_event` (streaming_job liña 157). Koluna ne'ebé hakerek (`camera_make`, `gps_latitude`, `captured_at`...) hatudu iha dashboard no Tableau (`analytics_reader.images_with_predictions`).

**9. Checkpoint ba principiante**

**P:** Se fotografia la iha EXIF GPS maibé telemóvel haruka GPS, saida mak rai?
**R:** GPS telemóvel mantein (`gps_source` hela `"device"`), tanba kondisaun liña 132 sala.

#### D.3.6 `model_input`, `model_input_preview` no `_prepared_rgb` — imajen ba tensor

**1. Kódigu orijinál**

{{code spark/preprocessing.py 144-170}}

**2. Objetivu**

Konverte imajen ba **tensor** ne'ebé CNN simu: hadi'ak sisi badak ba `S` piksel, koto kuadradu `S×S` iha klaran, konverte ba tensor, no normaliza ho `mean`/`std` husi checkpoint modelu. Docstring hatete centre crop di'ak liu duke "squeeze" (95.67% vs 94.74%).

**3. Fundamentu Python**

- **`transforms.Compose([...])`** — kombina transformasaun barak ba objetu ida ne'ebé bele bolu hanesan funsaun.
- **Bolu objetu** `to_model(img)` — objetu `Compose` iha `__call__`.
- **Parámetru opsionál** `prepared_bytes: bytes = None`.
- **Chamada kadeia** `transforms.CenterCrop(size)(transforms.Resize(size)(img))` — harii objetu, bolu kedas.

**4. Esplikasaun liña-ba-liña**

- **Liña 144** — `def model_input(original_bytes: bytes, prepared_bytes: bytes = None):`
- **Liña 145–146** — Docstring: tensor input CNN — sisi badak ba S, koto centre S×S, normaliza; centre crop di'ak liu squeeze (95.67% vs 94.74%).
- **Liña 147** — `from torchvision import transforms` — import lazy (torchvision todan).
- **Liña 148** — `spec = input_spec()` — dict husi checkpoint modelu.
- **Liña 149** — `size = spec["img_size"]`.
- **Liña 150** — `img = _prepared_rgb(original_bytes, prepared_bytes)` — imajen PIL RGB.
- **Liña 151** — `to_model = transforms.Compose([`
- **Liña 152** — `transforms.Resize(size),` — sisi badak liu sai `size` piksel; proporsaun mantein.
- **Liña 153** — `transforms.CenterCrop(size),` — koto kuadradu `size×size` iha klaran.
- **Liña 154** — `transforms.ToTensor(),` — PIL (H×W×3, 0–255) → tensor `[3, H, W]` float 0.0–1.0.
- **Liña 155** — `transforms.Normalize(mean=spec["mean"], std=spec["std"]),` — kada kanál: $x' = (x - \mu_c)/\sigma_c$.
- **Liña 156** — `])`.
- **Liña 157** — `return to_model(img)` — tensor final.
- **Liña 160** — `def model_input_preview(original_bytes: bytes, prepared_bytes: bytes = None):`
- **Liña 161** — Docstring: kuadradu ne'ebé CNN haree, hanesan imajen (model_input molok normaliza).
- **Liña 162** — `from torchvision import transforms`.
- **Liña 163** — `size = input_spec()["img_size"]`.
- **Liña 164** — `img = _prepared_rgb(original_bytes, prepared_bytes)`.
- **Liña 165** — `return transforms.CenterCrop(size)(transforms.Resize(size)(img))` — Resize depois CenterCrop; fó imajen PIL (la'ós tensor).
- **Liña 168** — `def _prepared_rgb(original_bytes, prepared_bytes=None):`
- **Liña 169** — `data = prepared_bytes if prepared_bytes is not None else prepare_image(original_bytes)` — uza imajen prepara ona se iha; se lae, prepara agora.
- **Liña 170** — `return ImageOps.exif_transpose(PILImage.open(BytesIO(data)).convert("RGB"))` — loke bytes, konverte RGB, roda tuir EXIF (seguru maske EXIF la iha ona).

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* ho `size = 224` no `mean = [0.485, 0.456, 0.406]`, `std = [0.229, 0.224, 0.225]` (valór ImageNet tipiku — valór loloos mai husi checkpoint, haree Modulu 05):

1. Imajen 1280×960 → `Resize(224)` → 299×224 (sisi badak 224).
2. `CenterCrop(224)` → 224×224.
3. `ToTensor` → piksel kanál vermelhu 153 → $153/255 = 0.6$.
4. `Normalize` → $(0.6 - 0.485)/0.229 \approx 0.502$.
5. Rezultadu: tensor `[3, 224, 224]`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return |
|---|---|---|
| `model_input` | `original_bytes: bytes`, `prepared_bytes: bytes ka None` | `torch.Tensor` float32 `[3, S, S]` (inferidu) |
| `model_input_preview` | hanesan | `PIL.Image.Image` S×S |
| `_prepared_rgb` | bytes ka `None`, bytes ka `None` | `PIL.Image.Image` RGB |
| `spec` | `dict` ho `img_size` (`int`), `mean`/`std` (`list[float]`) | — |

**7. Ligasaun OOP**

`transforms.Resize`, `CenterCrop`, `ToTensor`, `Normalize`, `Compose` mak **klase** torchvision; kada ida iha `__call__`, entaun objetu bele bolu hanesan funsaun. `Compose` kaer lista objetu transformasaun (**komposisaun**) no bolu sira tuir orden. Ne'e mós padraun *Pipeline* — maibé implementa iha biblioteka, la'ós projetu.

**8. Impaktu integrasaun**

`process_event` bolu `model_input` (liña 160) no fó tensor ba `ml.inference.predict` (Modulu 05). `model_input_preview` uza iha `store_processed_image` (liña 230). `input_spec()` garante katak preprocessing hanesan ho treinu modelu.

**9. Checkpoint ba principiante**

**P:** Tanba sá `img_size`, `mean`, `std` mai husi `input_spec()` no la hakerek fixu iha kódigu?
**R:** Tanba valór sira ne'e tenke hanesan ho valór ne'ebé uza bainhira treina modelu. Rai iha checkpoint modelu garante katak modelu foun ho valór seluk sei funsiona kedas.

#### D.3.7 `prepare_image` no `scale_test_run_for`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 173-198}}

**2. Objetivu**

`prepare_image`: kópia imajen ne'ebé vertikál (roda tuir EXIF), maximu `ANALYSIS_MAX_PX` (1280) piksel, re-encode JPEG kualidade 95 la ho EXIF. Pasu hotu depois hahú husi kópia ne'e. `scale_test_run_for`: hatene se eventu pertense ba teste eskalabilidade.

**3. Fundamentu Python**

- **`except Exception: return image_bytes`** — se imajen la bele lee, fó fali orijinál (la monu).
- **`# noqa: BLE001`** — komentáriu ba ferramenta *linter* (la husu aviszu "except luan").
- **Espresaun kondisionál naruk** ho `if ... else None`.
- **`session.expunge(obj)`** — hasai objetu husi sesaun atu bele uza depois sesaun taka.

**4. Esplikasaun liña-ba-liña**

- **Liña 173** — `def prepare_image(image_bytes: bytes) -> bytes:`
- **Liña 174–175** — Docstring: vertikál, maximu `ANALYSIS_MAX_PX`, re-encode la ho EXIF; imajen la bele lee fó fali la muda.
- **Liña 176** — `try:`
- **Liña 177** — `img = PILImage.open(BytesIO(image_bytes))` — loke (lazy — seidauk descodifika tomak).
- **Liña 178** — `img.draft("RGB", (ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))` — husu descodificadór JPEG atu descodifika iha eskala ki'ik (lalais liu, poupa memória).
- **Liña 179** — `img = ImageOps.exif_transpose(img).convert("RGB")` — roda tuir orientasaun EXIF; konverte RGB.
- **Liña 180** — `except Exception:` — imajen aat.
- **Liña 181** — `return image_bytes` — fó fali orijinál (CNN/erru depois sei trata).
- **Liña 182** — `img.thumbnail((ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))` — hamenus to'o sisi boot ≤ 1280, proporsaun mantein; la hadi'ak imajen ki'ik.
- **Liña 183** — `buf = BytesIO()`.
- **Liña 184** — `img.save(buf, format="JPEG", quality=95)` — rai JPEG (EXIF la hakerek).
- **Liña 185** — `return buf.getvalue()` — bytes JPEG foun.
- **Liña 188** — `def scale_test_run_for(event: dict):`
- **Liña 189** — Docstring: run teste ne'ebé eventu pertense, ka `None` (kontrola iha base de dadus).
- **Liña 190** — `run_id = event.get("run_id")`.
- **Liña 191** — `if not run_id:`
- **Liña 192** — `return None` — eventu normál.
- **Liña 193** — `with SessionLocal() as session:`
- **Liña 194** — `item = session.get(ScaleTestItem, event.get("image_id")) if event.get("image_id") else None`.
- **Liña 195** — `run = session.get(ScaleTestRun, run_id) if item is not None and item.run_id == run_id else None` — item tenke pertense ba run ne'e.
- **Liña 196** — `if run is not None:`
- **Liña 197** — `session.expunge(run)` — objetu bele uza depois sesaun taka.
- **Liña 198** — `return run`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* JPEG 4000×3000 ho EXIF `Orientation=6` (roda 90°). `draft` descodifika ~1000×750 ka 2000×1500 (eskala JPEG 1/2, 1/4...). `exif_transpose` roda → vertikál. `thumbnail((1280,1280))` → ezemplu 960×1280. Rai JPEG q=95 → ~300 KB. Ba `scale_test_run_for`: iha streaming normál `run_id` la iha → `None` (no `_process_one` salta eventu ho `run_id` molok to'o iha ne'e).

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception |
|---|---|---|---|
| `prepare_image` | `image_bytes: bytes` | `bytes` | la iha (hotu kaer) |
| `scale_test_run_for` | `event: dict` | `ScaleTestRun` ka `None` | erru base de dadus (la kaer) |

**7. Ligasaun OOP**

`img` mak objetu `PIL.Image.Image`; `draft`, `thumbnail`, `save`, `convert` mak métodu. `thumbnail` muda objetu **iha fatin** (la fó objetu foun) — ne'e diferente husi `convert` ne'ebé fó objetu foun.

**8. Impaktu integrasaun**

`prepare_image` bolu husi `process_event` (liña 159) no `_prepared_rgb`. Kópia ne'e uza ba CNN, imajen revizaun, imajen prosesa, no desenhu rezultadu. `scale_test_run_for` bolu husi `process_event` (liña 144).

**9. Checkpoint ba principiante**

**P:** Tanba sá `prepare_image` hasai EXIF?
**R:** EXIF iha GPS (dadus privadu) no orientasaun. Imajen ne'ebé roda ona no la ho EXIF evita roda dala rua no la vaza GPS ba imajen revizaun.

#### D.3.8 `save_downloaded_photo`, `save_for_review` no `store_processed_image`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 201-233}}

**2. Objetivu**

Funsaun tolu ne'ebé rai kópia imajen iha disku. Sira hotu **nunka** halo diagnóstiku falla: erru só print. Rua uza padraun "hakerek ba `.tmp`, depois `replace`" atu arkivu nunka sai la kompletu.

**3. Fundamentu Python**

- **`-> None`** — la fó valór.
- **`Path.replace(destinu)`** — troka naran arkivu (atómiku iha sistema arkivu hanesan): leitór sei haree arkivu tuan ka foun, nunka sorin-balun.
- **f-string ba naran arkivu**: `f"{image_id}.jpg.tmp"`.

**4. Esplikasaun liña-ba-liña**

- **Liña 201** — `def save_downloaded_photo(user_id: str, image_id: str, original_bytes: bytes) -> None:`
- **Liña 202** — Docstring: rai kópia lokál fotografia Drive; nunka halo diagnóstiku falla.
- **Liña 203** — `if not SAVE_DOWNLOADED_PHOTOS:` — konfigurasaun hamate:
- **Liña 204** — `return`.
- **Liña 205** — `try:`
- **Liña 206** — `store_downloaded_photo(user_id, image_id, original_bytes)` — rai iha `storage/downloaded_photos/` (Modulu 03).
- **Liña 207** — `except Exception as e:`
- **Liña 208** — `print(f"[preprocessing] Could not save downloaded photo {image_id}: {e}")`.
- **Liña 211** — `def save_for_review(image_id: str, image_bytes: bytes) -> None:`
- **Liña 212** — Docstring: kopia fotografia (la ho EXIF, entaun la ho GPS) ba `REVIEW_DIR` ba pájina Label photos.
- **Liña 213** — `if not SAVE_PHOTOS_FOR_REVIEW:`
- **Liña 214** — `return`.
- **Liña 215** — `try:`
- **Liña 216** — `img = _prepared_rgb(None, image_bytes)` — imajen PIL husi bytes prepara ona.
- **Liña 217** — `REVIEW_DIR.mkdir(parents=True, exist_ok=True)`.
- **Liña 218** — `tmp = REVIEW_DIR / f"{image_id}.jpg.tmp"`.
- **Liña 219** — `img.save(tmp, format="JPEG", quality=95)`.
- **Liña 220** — `tmp.replace(REVIEW_DIR / f"{image_id}.jpg")` — troka naran atómiku.
- **Liña 221** — `except Exception as e:`
- **Liña 222** — `print(f"[preprocessing] Could not save {image_id} for labelling: {e}")`.
- **Liña 225** — `def store_processed_image(image_id: str, original_bytes: bytes, prepared_bytes: bytes) -> None:`
- **Liña 226** — Docstring: rai kuadradu ne'ebé CNN haree hanesan `PROCESSED_IMAGES_DIR/<image_id>.png`.
- **Liña 227** — `try:`
- **Liña 228** — `PROCESSED_IMAGES_DIR.mkdir(parents=True, exist_ok=True)`.
- **Liña 229** — `tmp = PROCESSED_IMAGES_DIR / f"{image_id}.png.tmp"`.
- **Liña 230** — `model_input_preview(original_bytes, prepared_bytes).save(tmp, format="PNG")` — PNG (la lakon kualidade).
- **Liña 231** — `tmp.replace(PROCESSED_IMAGES_DIR / f"{image_id}.png")`.
- **Liña 232** — `except Exception as e:`
- **Liña 233** — `print(f"[preprocessing] Could not store the processed image of {image_id}: {e}")`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `image_id = "a1b2c3d4"`. `store_processed_image` hakerek `storage/processed_images/a1b2c3d4.png.tmp`, depois troka naran ba `a1b2c3d4.png` (224×224 se `img_size=224`). `save_for_review` hakerek `<TRAINING_PROJECT_DIR>/data/review/a1b2c3d4.jpg`. Se disku nakonu, print "Could not store ..." no fotografia kontinua.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Efeitu sekundáriu |
|---|---|---|---|
| `save_downloaded_photo` | `str`, `str`, `bytes` | `None` | arkivu iha `storage/downloaded_photos/` |
| `save_for_review` | `str`, `bytes` | `None` | `REVIEW_DIR/<id>.jpg` |
| `store_processed_image` | `str`, `bytes`, `bytes` | `None` | `PROCESSED_IMAGES_DIR/<id>.png` |

**7. Ligasaun OOP**

`tmp` mak objetu `Path`; `.replace()` mak métodu. Not implemented in the current codebase: klase ba armazenamentu imajen (funsaun simples de'it).

**8. Impaktu integrasaun**

Bolu husi `process_event` (liña 151, 162, 169). `REVIEW_DIR` uza husi pájina dashboard "Label photos" (ema hili klase loloos → dadus treinu foun, Modulu 05). `PROCESSED_IMAGES_DIR` uza husi dashboard atu hatudu imajen ne'ebé CNN haree.

**9. Checkpoint ba principiante**

**P:** Tanba sá hakerek ba `.tmp` uluk, depois `replace`?
**R:** Se programa ka dashboard lee arkivu momentu hakerek, nia bele haree arkivu la kompletu. `replace` troka naran iha pasu ida — arkivu final sempre kompletu.
