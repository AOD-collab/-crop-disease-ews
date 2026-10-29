
### D.7 `storage/results.py` — Spark → Drive `/results`

#### D.7.1 `_prediction_to_dict` no `publish_result_to_drive`

**1. Kódigu orijinál**

{{code storage/results.py 129-216}}

**2. Objetivu**

Hakerek rezultadu ba pasta `/results/<email>/` agrikultór: `<image_id>_processed.jpg` (imajen ho heatmap, se COMPLETED) no `<image_id>.json` (rezultadu tomak) ho `appProperties` sumáriu badak (app lista rezultadu hotu ho pedidu ida). Marka `result_published_at`.

**3. Fundamentu Python**

- **`**{k: getattr(p, k) for k in ...}`** — kopia atributu barak husi objetu ba dict.
- **`.isoformat() if x else None`**.
- **`lambda existing: ...`** haruka ba `_RESULT_FILES.upload`.
- **Dict comprehension ho `or ""`** — valór mamuk sai `""`.

**4. Esplikasaun liña-ba-liña**

- **Liña 129–131** — Komentáriu seksaun: Spark → Google Drive `/results`.
- **Liña 132** — `def _prediction_to_dict(p: Prediction) -> dict:`
- **Liña 133** — Docstring: kampu prediksaun iha forma ne'ebé app Android lee.
- **Liña 134** — `return {`
- **Liña 135** — `"prediction_id": p.prediction_id, "image_id": p.image_id,`
- **Liña 136** — `**{k: getattr(p, k) for k in PREDICTION_FIELDS},` — klase, konfiansa, feature, risku, versaun, tempu.
- **Liña 137** — `"risk_level": p.risk_level.value if p.risk_level else None,` — enum → testu.
- **Liña 138** — `"predicted_at": p.predicted_at.isoformat() if p.predicted_at else None,`
- **Liña 139** — `"is_uncertain": p.is_uncertain, "processing_latency_ms": p.processing_latency_ms,`
- **Liña 140** — `}`.
- **Liña 143–144** — `def publish_result_to_drive(event, status, error_message=None, processed_image=None):`
- **Liña 145–146** — Docstring: hakerek `<image_id>.json` (no `<image_id>_processed.jpg`) iha `/results/<email>/`; fó ID arkivu JSON, ka `None` ba imajen lokál.
- **Liña 147** — `image_id = event.get("image_id")`.
- **Liña 148** — `test_folder = None`.
- **Liña 149** — `with SessionLocal() as session:`
- **Liña 150** — `image = session.get(Image, image_id) if image_id else None`.
- **Liña 151** — `if image is None:`
- **Liña 152** — `return None` — eventu la iha liña (ezemplu rejeitadu la ho `image_id`).
- **Liña 153** — `if image.source == SCALE_TEST_SOURCE:`
- **Liña 154** — Komentáriu: teste eskala: só bainhira run publika rezultadu, ba nia pasta rasik.
- **Liña 155** — `item = session.get(ScaleTestItem, image_id)`.
- **Liña 156** — `run = session.get(ScaleTestRun, item.run_id) if item else None`.
- **Liña 157** — `if run is None or not run.publish_results or not run.drive_results_folder_id:`
- **Liña 158** — `return None`.
- **Liña 159** — `test_folder = run.drive_results_folder_id`.
- **Liña 160** — `drive_image_file_id = item.source_drive_file_id`.
- **Liña 161** — `elif not is_drive_path(image.image_path):` — fotografia lokál:
- **Liña 162** — `return None` — la iha Drive atu hakerek.
- **Liña 163** — `else:`
- **Liña 164** — `drive_image_file_id = drive_file_id_from_path(image.image_path)`.
- **Liña 165** — `prediction = (`
- **Liña 166** — `session.query(Prediction).filter(Prediction.image_id == image_id)`
- **Liña 167** — `.order_by(Prediction.predicted_at.desc()).first()` — prediksaun foun liu.
- **Liña 168** — `)`.
- **Liña 169** — `owner = session.get(User, image.user_id)`.
- **Liña 170** — `owner_email = owner.email if owner else ""`.
- **Liña 171** — `payload = {` — konteúdu `<image_id>.json`:
- **Liña 172** — `"image_id": image.image_id,`
- **Liña 173** — `"user_id": image.user_id,`
- **Liña 174** — `"drive_image_file_id": drive_image_file_id,`
- **Liña 175** — `"processed_file_id": None,` — prenxe depois.
- **Liña 176** — `"location": image.location,`
- **Liña 177** — `"captured_at": ... .isoformat() ...,`
- **Liña 178** — `"uploaded_at": ... .isoformat() ...,`
- **Liña 179** — Komentáriu: tempu rua hanesan, iha Timor-Leste Time.
- **Liña 180** — `"captured_at_local": format_local(image.captured_at) or None,`
- **Liña 181** — `"uploaded_at_local": format_local(image.uploaded_at) or None,`
- **Liña 182** — `**{c: getattr(image, c) for c in GPS_COLUMNS + ("gps_source", "camera_make", "camera_model")},`
- **Liña 183** — `"status": status.value,` — `"COMPLETED"`/`"FAILED"`.
- **Liña 184** — `"error_message": error_message,`
- **Liña 185** — `"prediction": _prediction_to_dict(prediction) if (prediction and status == COMPLETED) else None,`
- **Liña 186** — `"processed_at": datetime.now(timezone.utc).isoformat(),`
- **Liña 187** — `}`.
- **Liña 189–191** — `folder = test_folder or drive_client.user_folder_id(owner_email, drive_client.USER_RESULTS_KIND, create=True, share_role="reader")` — pasta rezultadu agrikultór (harii + partilla reader se seidauk iha).
- **Liña 192** — `if processed_image and status == ProcessingStatus.COMPLETED:`
- **Liña 193** — `name = f"{image_id}_processed.jpg"`.
- **Liña 194–197** — `payload["processed_file_id"] = _RESULT_FILES.upload(folder, name, lambda existing: drive_client.upload_bytes(name, processed_image, "image/jpeg", folder, {"image_id": ..., "user_email": ..., "kind": "processed"}, may_exist=False, existing_id=existing))` — upload ka troka imajen.
- **Liña 198** — `pred = payload["prediction"] or {}`.
- **Liña 199** — Komentáriu: sumáriu badak iha appProperties: app lista rezultadu hotu ho pedidu ida.
- **Liña 200** — `summary = {`
- **Liña 201** — `"image_id": image_id,`
- **Liña 202** — `"user_id": payload["user_id"],`
- **Liña 203** — `"user_email": owner_email,`
- **Liña 204** — `"kind": "result",`
- **Liña 205** — `"status": status.value,`
- **Liña 206** — `"drive_image_file_id": payload["drive_image_file_id"],`
- **Liña 207** — `**{k: payload[k] or "" for k in ("processed_file_id", "location", "uploaded_at")},`
- **Liña 208** — `**{k: pred.get(k) or "" for k in ("predicted_class", "risk_level")},`
- **Liña 209** — `**{k: pred.get(k) if pred else "" for k in ("prediction_confidence", "risk_score")},`
- **Liña 210** — `"error_message": (error_message or "")[:80],` — badak (limite appProperties).
- **Liña 211** — `}`.
- **Liña 212** — `name = f"{image_id}.json"`.
- **Liña 213–214** — `file_id = _RESULT_FILES.upload(folder, name, lambda existing: drive_client.upload_json(name, payload, folder, summary, may_exist=False, existing_id=existing))`.
- **Liña 215** — `_update_image(image_id, result_published_at=datetime.now(timezone.utc))` — komentáriu: rezultadu ne'e iha Drive.
- **Liña 216** — `return file_id`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* `<image_id>.json` (valór la loos):

```json
{"image_id": "a1b2...", "user_id": "u-9", "drive_image_file_id": "1AbCdEf",
 "processed_file_id": "1PrOc...", "location": "Aileu Vila",
 "captured_at": "2026-10-20T01:14:52+00:00", "uploaded_at": "2026-10-20T01:15:00+00:00",
 "captured_at_local": "20/10/2026 10:14 TLT", "uploaded_at_local": "20/10/2026 10:15 TLT",
 "gps_latitude": -8.7281, "gps_longitude": 125.5664, "gps_altitude_m": null, "gps_source": "device",
 "camera_make": "samsung", "camera_model": "SM-A145F", "status": "COMPLETED", "error_message": null,
 "prediction": {"predicted_class": "blight", "prediction_confidence": 0.97, "risk_level": "HIGH", "...": "..."},
 "processed_at": "2026-10-20T01:15:04+00:00"}
```

appProperties (`summary`): `{"image_id": "a1b2...", "kind": "result", "status": "COMPLETED", "predicted_class": "blight", "risk_level": "HIGH", "prediction_confidence": "0.97", ...}` (koto ba 124 bytes kada pár).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `event` | `dict` |
| `status` | `ProcessingStatus` |
| `error_message` | `str ka None` |
| `processed_image` | `bytes ka None` |
| `payload`, `summary`, `pred` | `dict` |
| `folder` | `str` (ID pasta) |
| return | `str ka None` |
| Exception | `DriveError`, erru rede (kaer iha `..._safely`) |

**7. Ligasaun OOP**

`getattr(p, k)` lee atributu objetu ORM `Prediction`; `p.risk_level.value` — enum. `_RESULT_FILES` mak objetu klase `_ResultFolderIndex` (D.7.2) — **komposisaun nível modulu**.

**8. Impaktu integrasaun**

Bolu husi `streaming_job._upload_result` → `publish_result_to_drive_safely` (Modulu 01). appProperties `summary` mak buat ne'ebé Android `findResults`, `resultsSince`, `myHistory` lee (D.9.7–D.9.9). `result_published_at` uza husi `replay_state` ("result_missing") no `requeue_stuck`.

**9. Checkpoint ba principiante**

**P:** Tanba sá rai sumáriu iha appProperties maske JSON tomak iha arkivu?
**R:** App bele lista rezultadu hotu ho pedidu **ida** (lee appProperties) la ho download JSON kada arkivu.

#### D.7.2 Klase `_ResultFolderIndex` no `publish_result_to_drive_safely`

**1. Kódigu orijinál**

{{code storage/results.py 219-270}}

**2. Objetivu**

Rai mapa `naran → file id` ba arkivu iha kada pasta rezultadu (lista dala ida kada pasta), atu rezultadu atualiza iha fatin duke duplika. Thread-safe (thread 8–16). `..._safely` falun publikasaun atu erru Drive só log (rezultadu rai ona iha BD).

**3. Fundamentu Python**

- **Klase ho atributu privadu** (`self._folders`, `self._lock`, `self._loading`).
- **`dict.setdefault(k, default)`** — fó valór, ka tau default no fó.
- **Lock per-pasta** (`threading.Lock()` kada pasta) + lock jerál.

**4. Esplikasaun liña-ba-liña**

- **Liña 219** — `class _ResultFolderIndex:`
- **Liña 220–221** — Docstring: naran → file id arkivu iha kada pasta rezultadu, lista dala ida kada pasta, atu rezultadu atualiza iha fatin duke duplika.
- **Liña 223** — `def __init__(self):`
- **Liña 224** — `self._folders = {}` — `{folder_id: {naran: file_id}}`.
- **Liña 225** — `self._lock = threading.Lock()` — proteje `_folders`/`_loading`.
- **Liña 226** — `self._loading = {}` — `{folder_id: Lock}` ba lista.
- **Liña 228** — `def _index(self, folder: str) -> dict:`
- **Liña 229** — `with self._lock:`
- **Liña 230** — `if folder in self._folders:`
- **Liña 231** — `return self._folders[folder]`.
- **Liña 232** — `loading = self._loading.setdefault(folder, threading.Lock())`.
- **Liña 233** — `with loading:` — komentáriu: lista ida kada pasta, maske thread 8.
- **Liña 234** — `with self._lock:`
- **Liña 235** — `if folder in self._folders:` — thread seluk lista ona.
- **Liña 236** — `return self._folders[folder]`.
- **Liña 237** — Komentáriu: tuan uluk: kópia foun liu ho naran hanesan manán.
- **Liña 238** — `names = {f["name"]: f["id"] for f in drive_client.list_files_in_folders([folder])}` — **chamada Drive**.
- **Liña 239** — `with self._lock:`
- **Liña 240** — `self._folders[folder] = names`.
- **Liña 241** — `return names`.
- **Liña 243** — `def existing(self, folder: str, name: str):`
- **Liña 244** — `return self._index(folder).get(name)`.
- **Liña 246** — `def remember(self, folder: str, name: str, file_id: str) -> str:`
- **Liña 247** — `with self._lock:`
- **Liña 248** — `self._folders.setdefault(folder, {})[name] = file_id`.
- **Liña 249** — `return file_id`.
- **Liña 251** — `def upload(self, folder: str, name: str, send) -> str:`
- **Liña 252** — Docstring: `send(existing_id)` hakerek `name`; ID ne'ebé fó fali rai.
- **Liña 253** — `return self.remember(folder, name, send(self.existing(folder, name)))`.
- **Liña 255** — `def clear(self):`
- **Liña 256** — `with self._lock:`
- **Liña 257** — `self._folders.clear()`.
- **Liña 260** — `_RESULT_FILES = _ResultFolderIndex()` — objetu ida ba modulu.
- **Liña 263–264** — `def publish_result_to_drive_safely(event, status, error_message=None, processed_image=None):`
- **Liña 265** — Docstring: erru Drive só log (rezultadu rai ona).
- **Liña 266** — `try:`
- **Liña 267** — `return publish_result_to_drive(event, status, error_message, processed_image)`.
- **Liña 268** — `except Exception as e:`
- **Liña 269** — `print(f"[results] Could not write result to Google Drive for {event.get('image_id')}: {e}")`.
- **Liña 270** — `return None`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* rezultadu dahuluk ba pasta `1Res...` → `_index` lista pasta (Drive) → `{}` → `existing` = `None` → `upload_json(..., existing_id=None)` → POST → `"1XyZ"` → `remember` → `{"a1b2.json": "1XyZ"}`. Replay → `existing` = `"1XyZ"` → PATCH (la iha lista Drive foun, la iha duplikadu).

**6. Tipu dadus**

| Atributu / métodu | Tipu |
|---|---|
| `_folders` | `dict[str, dict[str, str]]` |
| `_lock` | `threading.Lock` |
| `_loading` | `dict[str, threading.Lock]` |
| `existing(folder, name)` | `str ka None` |
| `remember(...)` | `str` |
| `upload(folder, name, send)` | `str` (`send`: funsaun `str ka None -> str`) |
| `publish_result_to_drive_safely` | `str ka None` |

**7. Ligasaun OOP**

- **Klase + `__init__`**: atributu instánsia 3 privadu (`_`).
- **Enkapsulamentu**: estadu cache no lock subar; liur uza `existing`, `remember`, `upload`, `clear`.
- **Komposisaun**: objetu na'in `dict` no `Lock`.
- **Objetu nível modulu** `_RESULT_FILES` (singleton prátiku).
- **Callback**: `upload(folder, name, send)` simu funsaun `send` — objetu la hatene oinsá upload; nia só jere ID (separasaun responsabilidade).
- Herança: Not implemented in the current codebase.

**8. Impaktu integrasaun**

`publish_result_to_drive` uza `_RESULT_FILES.upload` ba arkivu rua. Métodu `clear()` iha, maibé kódigu iha repozitóriu la bolu nia husi liur `storage/results.py`.

**9. Checkpoint ba principiante**

**P:** Tanba sá lock per-pasta (`_loading`) no lock jerál (`_lock`)?
**R:** `_lock` proteje dict lalais. `_loading` garante katak lista Drive (neineik) halai **dala ida** kada pasta, maske thread barak husu iha tempu hanesan — thread seluk hein no uza rezultadu.

### D.8 `storage/reset.py` — factory reset Drive

#### D.8.1 `clear_drive_files`, `write_reset_markers`, `_clear_watcher_bookmark`

**1. Kódigu orijinál**

{{code storage/reset.py 99-153}}

{{code storage/reset.py 227-234}}

**2. Objetivu**

Hamamuk pasta images no results agrikultór hotu iha Drive (arkivu na'in konta sentrál → lixu; arkivu na'in agrikultór → hasai husi pasta), hakerek **reset marker** iha kada pasta results atu telemóvel hamoos nia istória, no hamoos bookmark watcher atu fotografia ne'ebé hela iha Drive prosesa fali.

**3. Fundamentu Python**

- **Dict sumáriu** ho kontajen no lista erru — funsaun "nunka hamosu erru".
- **`session.patch(url, params=..., json=...)`** — HTTP PATCH direta.
- **`strftime("%Y-%m-%dT%H:%M:%S.%fZ")`** — tempu ho mikrosegundu.

**4. Esplikasaun liña-ba-liña**

- **Liña 99** — `def clear_drive_files() -> dict:`
- **Liña 100–102** — Docstring: hamamuk pasta images no results agrikultór hotu (fotografia na'in agrikultór hasai husi pasta) no husik reset marker atu telemóvel kada ida hamoos istória; nunka hamosu erru.
- **Liña 103** — `from storage import drive_client`.
- **Liña 105** — `summary = {"trashed": 0, "removed_from_folder": 0, "reset_markers": 0, "errors": []}`.
- **Liña 106** — `try:`
- **Liña 107** — `session = drive_client._authed_session()`.
- **Liña 108** — `image_folders = [f["id"] for f in drive_client.list_subfolders(drive_client.images_folder_id())]`.
- **Liña 109** — `result_folders = [f["id"] for f in drive_client.list_subfolders(drive_client.results_folder_id())]`.
- **Liña 110** — `folders = image_folders + result_folders`.
- **Liña 111** — `files = drive_client.list_files_in_folders(folders) if folders else []`.
- **Liña 112** — `except Exception as e:`
- **Liña 113** — `summary["errors"].append(f"could not list Drive folders: {e}")`.
- **Liña 114** — `return summary`.
- **Liña 116** — `folder_set = set(folders)`.
- **Liña 117** — `for meta in files:`
- **Liña 118** — `file_id = meta["id"]`.
- **Liña 119** — `url = f"{drive_client.DRIVE_API}/files/{file_id}"`.
- **Liña 120** — `try:`
- **Liña 121** — `resp = session.patch(url, params={"fields": "id"}, json={"trashed": True})` — tau ba lixu.
- **Liña 122** — `if resp.status_code == 403:` — komentáriu: la'ós na'in → hasai husi pasta agrikultór.
- **Liña 123** — `parents = ",".join(p for p in meta.get("parents", []) if p in folder_set)`.
- **Liña 124** — `resp = session.patch(url, params={"removeParents": parents, "fields": "id"}, json={})`.
- **Liña 125** — `drive_client._check(resp, f"remove {file_id} from folder")`.
- **Liña 126** — `summary["removed_from_folder"] += 1`.
- **Liña 127** — `else:`
- **Liña 128** — `drive_client._check(resp, f"trash {file_id}")`.
- **Liña 129** — `summary["trashed"] += 1`.
- **Liña 130** — `except Exception as e:`
- **Liña 131** — `summary["errors"].append(f"{meta.get('name', file_id)}: {e}")`.
- **Liña 133** — `write_reset_markers(result_folders, summary)`.
- **Liña 134** — `return summary`.
- **Liña 137** — Komentáriu: app haree `reset_at` foun no karrega fali istória husi Drive.
- **Liña 138** — `RESET_MARKER_NAME = "_reset_marker.json"`.
- **Liña 141** — `def write_reset_markers(result_folders: list, summary: dict) -> None:`
- **Liña 142** — `from datetime import datetime, timezone`.
- **Liña 143** — `from storage import drive_client`.
- **Liña 145** — `reset_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")`.
- **Liña 146** — `for folder_id in result_folders:`
- **Liña 147** — `try:`
- **Liña 148–149** — `drive_client.upload_json(RESET_MARKER_NAME, {"reset_at": reset_at}, folder_id, {"kind": "reset_marker", "reset_at": reset_at})` — JSON + appProperties `reset_at`.
- **Liña 150** — `summary["reset_markers"] += 1`.
- **Liña 151** — `except Exception as e:`
- **Liña 152** — `summary["errors"].append(f"reset marker in {folder_id}: {e}")`.
- **Liña 227** — `def _clear_watcher_bookmark() -> str:`
- **Liña 228** — Docstring: hamoos bookmark watcher atu fotografia hotu ne'ebé hela iha Drive haruka fali.
- **Liña 229** — `from kafka import drive_watcher`.
- **Liña 230** — `path = drive_watcher.STATE_FILE`.
- **Liña 231** — `if not path.exists():`
- **Liña 232** — `return "none"`.
- **Liña 233** — `path.unlink()`.
- **Liña 234** — `return "cleared (every photo still in Drive is processed again)"`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* agrikultór 2, fotografia 10 (na'in agrikultór), rezultadu 20 (na'in konta sentrál). `clear_drive_files()` → fotografia: PATCH trashed → 403 → `removeParents` → `removed_from_folder = 10`; rezultadu → `trashed = 20`; marker → `reset_markers = 2` → `{"trashed": 20, "removed_from_folder": 10, "reset_markers": 2, "errors": []}`. Telemóvel `DriveRest.resetMarker` haree `reset_at` foun → hamoos istória lokál.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `clear_drive_files()` | `dict` (`trashed`, `removed_from_folder`, `reset_markers`: `int`; `errors`: `list[str]`) |
| `write_reset_markers(result_folders, summary)` | `None` (muda `summary`) |
| `_clear_watcher_bookmark()` | `str` |

**7. Ligasaun OOP**

Uza objetu sesaun (`session.patch`) no funsaun `drive_client` "privadu" (`_authed_session`, `_check`) — kódigu reuza helper interior (konvensaun `_` la blokeia). Not implemented in the current codebase: klase reset.

**8. Impaktu integrasaun**

Dashboard factory reset (Modulu 06) bolu funsaun sira ne'e. `RESET_MARKER_NAME` hanesan ho `DriveRest.RESET_MARKER_NAME` (Kotlin) — kontratu laptop ↔ telemóvel.

**9. Checkpoint ba principiante**

**P:** Tanba sá fotografia agrikultór la tama lixu maibé "hasai husi pasta"?
**R:** Arkivu na'in agrikultór (nia upload). Konta sentrál la iha permisaun hamoos (403); nia bele de'it hasai arkivu husi pasta sentrál (`removeParents`).
