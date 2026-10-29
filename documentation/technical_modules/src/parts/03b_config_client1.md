
## D. Esplikasaun Kódigu-ba-Kódigu

### D.1 Konfigurasaun no script setup

#### D.1.1 `config/settings.py` — konfigurasaun Google Drive

**1. Kódigu orijinál**

{{code config/settings.py 141-166}}

**2. Objetivu**

Define fatin arkivu segredu (client secret, token) no arkivu ID pasta, naran pasta raiz, no lee ID pasta `images`/`results` — husi variavel ambiente uluk, se lae husi arkivu ne'ebé `drive_authorize.py` hakerek.

**3. Fundamentu Python**

- **`Path(os.getenv("X", default))`** — dalan husi variavel ambiente ka default.
- **Import lokál** `import json` iha funsaun nia laran.
- **Kódigu nível modulu bolu funsaun** (liña 165–166) — halai dala ida bainhira import.

**4. Esplikasaun liña-ba-liña**

- **Liña 141–143** — Komentáriu seksaun: Google Drive (konta sentrál ida; token harii husi `scripts/drive_authorize.py`).
- **Liña 144–145** — `GOOGLE_DRIVE_CLIENT_SECRET_FILE` — default `storage/google_client_secret.json` (OAuth client "Desktop app"; segredu — la hatudu konteúdu).
- **Liña 146–147** — `GOOGLE_DRIVE_TOKEN_FILE` — default `storage/google_drive_token.json` (token OAuth; segredu).
- **Liña 148–149** — `GOOGLE_DRIVE_FOLDERS_FILE` — default `storage/google_drive_folders.json` (ID pasta, la'ós segredu).
- **Liña 150** — `GOOGLE_DRIVE_ROOT_FOLDER_NAME` — default `"CropDiseaseEWS"`.
- **Liña 153** — `def _drive_folder_id(env_name: str, key: str) -> str:`
- **Liña 154** — Docstring: ID pasta husi ambiente, se lae husi arkivu ne'ebé `drive_authorize.py` hakerek.
- **Liña 155** — `value = os.getenv(env_name, "")`.
- **Liña 156** — `if value:`
- **Liña 157** — `return value`.
- **Liña 158** — `try:`
- **Liña 159** — `import json`.
- **Liña 160** — `return json.loads(GOOGLE_DRIVE_FOLDERS_FILE.read_text()).get(key, "")`.
- **Liña 161** — `except (OSError, ValueError):` — arkivu la iha / JSON aat.
- **Liña 162** — `return ""` — "seidauk konfigura".
- **Liña 165** — `GOOGLE_DRIVE_IMAGES_FOLDER_ID = _drive_folder_id("GOOGLE_DRIVE_IMAGES_FOLDER_ID", "images_folder_id")`.
- **Liña 166** — `GOOGLE_DRIVE_RESULTS_FOLDER_ID = _drive_folder_id("GOOGLE_DRIVE_RESULTS_FOLDER_ID", "results_folder_id")`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* la iha variavel ambiente; `google_drive_folders.json` = `{"root_folder_id": "1Root...", "images_folder_id": "1Img...", "results_folder_id": "1Res..."}` → `GOOGLE_DRIVE_IMAGES_FOLDER_ID = "1Img..."`. Molok `drive_authorize.py` halai → `""`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `GOOGLE_DRIVE_*_FILE` | `Path` |
| `GOOGLE_DRIVE_ROOT_FOLDER_NAME` | `str` |
| `_drive_folder_id` | `(str, str) -> str` |
| `GOOGLE_DRIVE_IMAGES_FOLDER_ID`, `..._RESULTS_...` | `str` (bele `""`) |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Konstante no funsaun.)

**8. Impaktu integrasaun**

`drive_client` import token no ID pasta; `drive_authorize.py` hakerek `GOOGLE_DRIVE_FOLDERS_FILE`.

**9. Checkpoint ba principiante**

**P:** Tanba sá ID pasta bele mai husi variavel ambiente **ka** arkivu?
**R:** Variavel ambiente permite troka ba konta seluk la ho muda arkivu; arkivu halo setup automátiku depois `drive_authorize.py`.

#### D.1.2 `scripts/drive_authorize.py` — docstring no import

**1. Kódigu orijinál**

{{code scripts/drive_authorize.py 1-22}}

**2. Objetivu**

Script setup dala ida iha laptop (nunka iha Android): login ho konta Google sentrál, rai token, harii pasta.

**3. Fundamentu Python**

`sys.path.insert(0, ...)` + `# noqa: E402` (haree Modulu 02 D.3.1).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–2** — Docstring: setup Google Drive dala ida iha laptop (nunka Android).
- **Liña 3** — Liña mamuk docstring.
- **Liña 4** — Komando: `python scripts/drive_authorize.py`.
- **Liña 5** — Liña mamuk docstring.
- **Liña 6–9** — Login ho konta Google sentrál; token rai iha `GOOGLE_DRIVE_TOKEN_FILE` (rai privadu); harii pasta `CropDiseaseEWS/images` no `/results`; presiza OAuth client "Desktop app" JSON iha `GOOGLE_DRIVE_CLIENT_SECRET_FILE`.
- **Liña 10** — Remata docstring.
- **Liña 11** — `import json`.
- **Liña 12** — `import sys`.
- **Liña 13** — `from pathlib import Path`.
- **Liña 15** — `sys.path.insert(0, str(Path(__file__).resolve().parent.parent))` — pasta raiz projetu.
- **Liña 17–22** — Import konfigurasaun 4: client secret, token, arkivu pasta, naran raiz.

**5. Walkthrough ezekusaun**

`python scripts/drive_authorize.py` → Python halai liña 1–22, depois `main()` (D.1.3).

**6. Tipu dadus**

Konstante `Path` no `str` (haree D.1.1).

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

Script ne'e prepara buat hotu ne'ebé `drive_client` presiza.

**9. Checkpoint ba principiante**

**P:** Tanba sá script ne'e "nunka iha Android"?
**R:** Token konta sentrál fó asesu ba pasta agrikultór hotu — tenke rai iha laptop de'it. Telemóvel uza token agrikultór rasik.

#### D.1.3 `scripts/drive_authorize.py` — `main` (fluxu OAuth)

**1. Kódigu orijinál**

{{code scripts/drive_authorize.py 25-66}}

**2. Objetivu**

Halai fluxu OAuth "installed app": loke browser, utilizadór fó permisaun, rai token (ho refresh token), harii pasta `CropDiseaseEWS`, `images`, `results`, no rai ID sira.

**3. Fundamentu Python**

- **`InstalledAppFlow.from_client_secrets_file(...)`** — métodu klase ne'ebé harii objetu fluxu.
- **`flow.run_local_server(port=0, ...)`** — loke server lokál iha port livre atu simu resposta Google.
- **`json.dumps(..., indent=2)`** — JSON ho indentasaun.

**4. Esplikasaun liña-ba-liña**

- **Liña 25** — `def main() -> int:`
- **Liña 26** — `secret = Path(GOOGLE_DRIVE_CLIENT_SECRET_FILE)`.
- **Liña 27** — `if not secret.exists():`
- **Liña 28–30** — `print(...)` — arkivu client la iha; harii OAuth client "Desktop app" iha Google Cloud Console no rai.
- **Liña 31** — `return 1`.
- **Liña 33** — `from google_auth_oauthlib.flow import InstalledAppFlow`.
- **Liña 34** — `from storage import drive_client`.
- **Liña 36** — `flow = InstalledAppFlow.from_client_secrets_file(str(secret), drive_client.SCOPES)` — scope Drive tomak.
- **Liña 37** — `creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")` — `offline` = husu refresh token; `consent` = sempre hatudu ekran konsentimentu (garante refresh token).
- **Liña 39** — `token_path = Path(GOOGLE_DRIVE_TOKEN_FILE)`.
- **Liña 40** — `token_path.parent.mkdir(parents=True, exist_ok=True)`.
- **Liña 41** — `token_path.write_text(creds.to_json())` — rai token (segredu).
- **Liña 42** — `print(f"Saved OAuth token to {token_path}")`.
- **Liña 44** — Komentáriu: `drive_client` seidauk rai sesaun, entaun nia sei uza token foun.
- **Liña 45** — `root_id = drive_client.ensure_folder(GOOGLE_DRIVE_ROOT_FOLDER_NAME)`.
- **Liña 46** — `images_id = drive_client.ensure_folder("images", root_id)`.
- **Liña 47** — `results_id = drive_client.ensure_folder("results", root_id)`.
- **Liña 49** — `folders = {`
- **Liña 50** — `"root_folder_id": root_id,`
- **Liña 51** — `"images_folder_id": images_id,`
- **Liña 52** — `"results_folder_id": results_id,`
- **Liña 53** — `}`.
- **Liña 54** — `Path(GOOGLE_DRIVE_FOLDERS_FILE).write_text(json.dumps(folders, indent=2))`.
- **Liña 55** — `print(...)` — "Folders ready: CropDiseaseEWS/images = <id>".
- **Liña 56** — `print(...)` — ".../results = <id>".
- **Liña 57** — `print(...)` — "Saved folder IDs to ...".
- **Liña 58** — `print()` — liña mamuk.
- **Liña 59** — `print(...)` — pasu tuirmai: fó pasta privadu ba kada agrikultór.
- **Liña 60** — `print(...)` — `python scripts/drive_share_user.py farmer@gmail.com`.
- **Liña 61** — `print(...)` — app Android hetan nia pasta rasik.
- **Liña 62** — `return 0`.
- **Liña 65** — `if __name__ == "__main__":`
- **Liña 66** — `sys.exit(main())`.

**5. Walkthrough ezekusaun**

Admin halai script → browser loke → login konta sentrál → "Allow" → token rai → pasta harii (ka hetan se iha ona: `ensure_folder` buka uluk) → `google_drive_folders.json` rai ID 3.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `secret`, `token_path` | `Path` |
| `flow` | `InstalledAppFlow` |
| `creds` | `google.oauth2.credentials.Credentials` |
| `root_id`, `images_id`, `results_id` | `str` |
| `folders` | `dict[str, str]` |
| return | `int` |

**7. Ligasaun OOP**

`InstalledAppFlow.from_client_secrets_file` mak **métodu klase** (fábrika) ne'ebé fó objetu; `creds.to_json()` mak métodu objetu.

**8. Impaktu integrasaun**

Token ne'e uza husi `drive_client._authed_session` (D.2.3) ba chamada laptop hotu (watcher, Spark, results, reset).

**9. Checkpoint ba principiante**

**P:** Tanba sá `access_type="offline"`?
**R:** Atu Google fó refresh token — laptop bele hetan access token foun automátiku la ho login fali.

#### D.1.4 `scripts/drive_share_user.py`

**1. Kódigu orijinál**

{{code scripts/drive_share_user.py 1-33}}

**2. Objetivu**

Fó pasta privadu ba agrikultór: harii `images/<email>/` (writer) no `results/<email>/` (reader), partilla ho email ne'e de'it. Seguru atu repete.

**3. Fundamentu Python**

- **`sys.argv[1:]`** — argumentu liña komando (la inklui naran script).
- **`print(__doc__)`** — hatudu docstring modulu hanesan ajuda.
- **Espresaun kondisionál iha argumentu `print`**.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–2** — Docstring: fó pasta privadu ba agrikultór iha Drive sentrál.
- **Liña 3** — Liña mamuk docstring.
- **Liña 4** — Uza: `python scripts/drive_share_user.py farmer.name@gmail.com [more@gmail.com ...]`.
- **Liña 5** — Liña mamuk.
- **Liña 6–8** — Harii `images/<email>/` (writer) no `results/<email>/` (reader), partilla de'it ho agrikultór; seguru repete; aumenta mós email hanesan test user iha Google Cloud.
- **Liña 9** — Remata docstring.
- **Liña 10** — `import sys`.
- **Liña 11** — `from pathlib import Path`.
- **Liña 13** — `sys.path.insert(...)`.
- **Liña 15** — `from storage import drive_client`.
- **Liña 18** — `def main(emails) -> int:`
- **Liña 19** — `if not emails:`
- **Liña 20** — `print(__doc__)`.
- **Liña 21** — `return 1`.
- **Liña 22** — `for email in emails:`
- **Liña 23** — `email = drive_client.register_farmer(email)["email"]` — email normalizadu (letra ki'ik).
- **Liña 24** — `print(...)` — "<email>: images/<email>/ (upload) and results/<email>/ (read) — shared only with <email>".
- **Liña 25** — `project = drive_client.google_cloud_project_id()`.
- **Liña 26** — `print(...)` — aumenta kada email hanesan TEST USER (presiza bainhira app iha Testing).
- **Liña 27–28** — `print(URL console ho project if project else dalan menu)`.
- **Liña 29** — `return 0`.
- **Liña 32** — `if __name__ == "__main__":`
- **Liña 33** — `sys.exit(main(sys.argv[1:]))`.

**5. Walkthrough ezekusaun**

`python scripts/drive_share_user.py Agrikultor@Example.com` → `register_farmer` → email `agrikultor@example.com` → pasta 2 harii/partilla → print → link Google Cloud Console "Audience".

**6. Tipu dadus**

`emails`: `list[str]`; `project`: `str ka None`; return `int`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

Pasta ne'ebé harii iha ne'e mak pasta ne'ebé Android `DriveRest.myFolders` buka (`sharedWithMe`) no watcher `user_image_folders` lista.

**9. Checkpoint ba principiante**

**P:** Tanba sá pasta `results` partilla ho papél **reader**?
**R:** Agrikultór presiza lee rezultadu maibé labele muda ka hamoos rezultadu Spark.

### D.2 `storage/drive_client.py` — cliente API Drive

#### D.2.1 Docstring, import, konstante no `_DRIVE_ID_RE`

**1. Kódigu orijinál**

{{code storage/drive_client.py 1-31}}

**2. Objetivu**

Modulu úniku ne'ebé ko'alia ho Google Drive (REST API v3, konta sentrál). Define scope, URL API, tipu MIME pasta, kampu metadata, no regex atu valida ID Drive (proteje kontra *injection*).

**3. Fundamentu Python**

- **`from typing import Optional`**.
- **String adjasente** iha parénteze (liña 24–27) kombina automátiku.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–6** — Docstring: modulu úniku ne'ebé ko'alia ho Google Drive (REST v3, konta sentrál); halai `drive_authorize.py` dala ida; agrikultór kada ida iha pasta `/images/<email>/` (upload) no `/results/<email>/` (rezultadu).
- **Liña 7** — `import json`.
- **Liña 8** — `import re`.
- **Liña 9** — `import threading` — lock pasta agrikultór.
- **Liña 10** — `import uuid` — boundary multipart.
- **Liña 11** — `from typing import Optional`.
- **Liña 13–17** — Import token no ID pasta.
- **Liña 19** — `SCOPES = ["https://www.googleapis.com/auth/drive"]` — Drive tomak.
- **Liña 20** — `DRIVE_API = "https://www.googleapis.com/drive/v3"`.
- **Liña 21** — `UPLOAD_API = "https://www.googleapis.com/upload/drive/v3"`.
- **Liña 22** — `FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"`.
- **Liña 24–27** — `FILE_METADATA_FIELDS` — kampu ne'ebé husu: `id`, `name`, `mimeType`, `size`, `parents`, `appProperties`, `md5Checksum`, `sha256Checksum`, `trashed`, `createdTime`, `owners(emailAddress,displayName)`.
- **Liña 29** — Komentáriu: valida molok tama URL ka query (prevene injection).
- **Liña 30** — `_DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,200}$")` — letra, númeru, `_`, `-`; karakter 10–200.

**5. Walkthrough ezekusaun**

`_DRIVE_ID_RE.match("1AbCdEfGhIj")` → match; `_DRIVE_ID_RE.match("x' or '1'='1")` → `None`.

**6. Tipu dadus**

`SCOPES`: `list[str]`; `DRIVE_API`, `UPLOAD_API`, `FOLDER_MIME_TYPE`, `FILE_METADATA_FIELDS`: `str`; `_DRIVE_ID_RE`: `re.Pattern`.

**7. Ligasaun OOP**

Not implemented in the current codebase (bloku ne'e).

**8. Impaktu integrasaun**

`FILE_METADATA_FIELDS` determina kampu ne'ebé watcher hetan (no tama iha eventu Kafka): `owners`, `appProperties`, `sha256Checksum`, `createdTime`.

**9. Checkpoint ba principiante**

**P:** Tanba sá valida ID ho regex molok tau iha query?
**R:** Query Drive hanesan `'<id>' in parents`. Se ID iha `'`, ema bele muda query (injection). Regex aseita de'it karakter seguru.

#### D.2.2 Klase erru, validasaun ID, ID pasta raiz, `is_configured`

**1. Kódigu orijinál**

{{code storage/drive_client.py 33-75}}

**2. Objetivu**

Define klase erru rua (`DriveNotConfigured`, `DriveError` ho `status_code`), funsaun valida ID, funsaun fó ID pasta raiz (ka erru), no kontrola se Drive konfigura ona.

**3. Fundamentu Python**

- **Sub-klase ho `__init__` rasik** no **`super().__init__(message)`**.
- **`bool(x) and bool(y)`** — fó `True/False` puru.

**4. Esplikasaun liña-ba-liña**

- **Liña 33** — `class DriveNotConfigured(Exception):`
- **Liña 34** — Docstring: arkivu token ka ID pasta la iha — halai `drive_authorize.py`.
- **Liña 37** — `class DriveError(Exception):`
- **Liña 38** — Docstring: chamada API Drive falla.
- **Liña 40** — `def __init__(self, message: str, status_code: Optional[int] = None):`
- **Liña 41** — `super().__init__(message)` — `Exception` rai mensajen.
- **Liña 42** — `self.status_code = status_code` — atributu foun (kódigu HTTP).
- **Liña 45** — `def is_valid_drive_id(value: Optional[str]) -> bool:`
- **Liña 46** — `return bool(value) and bool(_DRIVE_ID_RE.match(value))`.
- **Liña 49** — `def _require_valid_id(value: str, what: str = "Drive file id") -> str:`
- **Liña 50** — `if not is_valid_drive_id(value):`
- **Liña 51** — `raise DriveError(f"Invalid {what}: {value!r}", status_code=400)`.
- **Liña 52** — `return value`.
- **Liña 55** — `def _root_folder_id(folder_id: str, setting: str) -> str:`
- **Liña 56** — `if not folder_id:`
- **Liña 57** — `raise DriveNotConfigured(f"{setting} is not set — run scripts/drive_authorize.py")`.
- **Liña 58** — `return folder_id`.
- **Liña 61** — `def images_folder_id() -> str:`
- **Liña 62** — `return _root_folder_id(GOOGLE_DRIVE_IMAGES_FOLDER_ID, "GOOGLE_DRIVE_IMAGES_FOLDER_ID")`.
- **Liña 65** — `def results_folder_id() -> str:`
- **Liña 66** — `return _root_folder_id(GOOGLE_DRIVE_RESULTS_FOLDER_ID, "GOOGLE_DRIVE_RESULTS_FOLDER_ID")`.
- **Liña 69** — `def is_configured() -> bool:`
- **Liña 70** — `from pathlib import Path`.
- **Liña 71** — `return (`
- **Liña 72** — `Path(GOOGLE_DRIVE_TOKEN_FILE).exists()`.
- **Liña 73** — `and bool(GOOGLE_DRIVE_IMAGES_FOLDER_ID)`.
- **Liña 74** — `and bool(GOOGLE_DRIVE_RESULTS_FOLDER_ID)`.
- **Liña 75** — `)`.

**5. Walkthrough ezekusaun**

`_require_valid_id("abc")` → `DriveError("Invalid Drive file id: 'abc'", status_code=400)` (menus karakter 10). `images_folder_id()` molok setup → `DriveNotConfigured(...)`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `DriveError.status_code` | `int ka None` |
| `is_valid_drive_id` | `Optional[str] -> bool` |
| `_require_valid_id` | `-> str`; raise `DriveError` |
| `images_folder_id`, `results_folder_id` | `-> str`; raise `DriveNotConfigured` |
| `is_configured` | `-> bool` |

**7. Ligasaun OOP**

- **Herança**: `DriveError(Exception)`, `DriveNotConfigured(Exception)`.
- **Konstrutór sub-klase + `super()`**: `DriveError.__init__` bolu `Exception.__init__` (`super().__init__(message)`) no aumenta atributu `status_code` — ezemplu klaru **estende** klase inan.
- **Polimorfizmu exception**: `except (drive_client.DriveError, drive_client.DriveNotConfigured)` iha `_clean_drive_event` kaer rua.

**8. Impaktu integrasaun**

`load_image_bytes` troka `DriveError` ba `FileNotFoundError`; watcher `main` para ho `DriveNotConfigured`.

**9. Checkpoint ba principiante**

**P:** Saida mak `super().__init__(message)` halo?
**R:** Bolu konstrutór `Exception` atu mensajen rai iha objetu erru (ezemplu `str(e)` fó mensajen), depois `DriveError` aumenta `status_code`.

#### D.2.3 Sesaun autentikadu, `_check`, `_escape_query_value`

**1. Kódigu orijinál**

{{code storage/drive_client.py 78-113}}

**2. Objetivu**

Harii `AuthorizedSession` **ida** husi token rai (Google refresh automátiku), kontrola resposta HTTP (≥ 400 → `DriveError`), no escape valór iha query Drive.

**3. Fundamentu Python**

- **`global _session`** — muda variavel modulu.
- **Cache simples** — se iha ona, fó fali.
- **`str.replace`** kadeia.

**4. Esplikasaun liña-ba-liña**

- **Liña 78–80** — Komentáriu seksaun: sesaun HTTP autentikadu (konta sentrál, lado server de'it).
- **Liña 81** — `_session = None`.
- **Liña 84** — `def _authed_session():`
- **Liña 85** — Docstring: `AuthorizedSession` ida husi token rai (refresh automátiku).
- **Liña 86** — `global _session`.
- **Liña 87** — `if _session is not None:`
- **Liña 88** — `return _session`.
- **Liña 89** — `from pathlib import Path`.
- **Liña 90** — `if not Path(GOOGLE_DRIVE_TOKEN_FILE).exists():`
- **Liña 91–94** — `raise DriveNotConfigured(...)` — token la iha; halai `drive_authorize.py`.
- **Liña 95** — `from google.oauth2.credentials import Credentials`.
- **Liña 96** — `from google.auth.transport.requests import AuthorizedSession`.
- **Liña 98** — `creds = Credentials.from_authorized_user_file(str(GOOGLE_DRIVE_TOKEN_FILE), SCOPES)` — lee token (access + refresh).
- **Liña 99** — `_session = AuthorizedSession(creds)` — sesaun `requests` ne'ebé tau header `Authorization: Bearer ...` no refresh token bainhira expira.
- **Liña 100** — `return _session`.
- **Liña 103** — `def _check(resp, action: str):`
- **Liña 104** — `if resp.status_code >= 400:`
- **Liña 105–108** — `raise DriveError(f"Google Drive {action} failed: HTTP {code} {resp.text[:300]}", status_code=...)`.
- **Liña 109** — `return resp`.
- **Liña 112** — `def _escape_query_value(value: str) -> str:`
- **Liña 113** — `return value.replace("\\", "\\\\").replace("'", "\\'")` — `\` → `\\`, `'` → `\'`.

**5. Walkthrough ezekusaun**

Chamada dahuluk `_authed_session()` → lee token → harii sesaun; chamada tuirmai → objetu hanesan. `_escape_query_value("O'Neil")` → `"O\'Neil"`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `_session` | `AuthorizedSession ka None` |
| `creds` | `Credentials` |
| `_check(resp, action)` | `requests.Response` → `Response`; raise `DriveError` |
| `_escape_query_value` | `str -> str` |

**7. Ligasaun OOP**

`_session` mak objetu **singleton nível modulu** (Teoria: *lazy singleton*) — harii dala ida, partilla husi thread hotu. `AuthorizedSession` herda husi `requests.Session` (biblioteka) — **komposisaun**: objetu kaer objetu `Credentials`.

**8. Impaktu integrasaun**

Chamada Drive hotu iha laptop liuhusi `_request` → `_authed_session()`. `reset.clear_drive_files` uza `_authed_session()` direta.

**9. Checkpoint ba principiante**

**P:** Tanba sá rai `_session` iha variavel global?
**R:** Atu lee token no harii konesaun dala ida de'it; harii sesaun foun kada chamada neineik.

#### D.2.4 Konstante retry, `_should_back_off`, `_request`

**1. Kódigu orijinál**

{{code storage/drive_client.py 116-154}}

**2. Objetivu**

Define timeout no númeru koko fali. `_request` mak dalan ba chamada Drive hotu: sempre ho timeout, no koko fali ho **backoff eksponensiál** bainhira Google hatete "neineik liu" (429, 403 rate limit) ka erru server (5xx).

**3. Fundamentu Python**

- **`__import__("time").sleep`** — import funsaun hanesan valór (teste bele troka `_sleep`).
- **`getattr(obj, method.lower())`** — "get"/"post"/"patch" → métodu sesaun.
- **`random.random()`** — 0.0–1.0 (jitter).
- **`any(gerador)`**.

**4. Esplikasaun liña-ba-liña**

- **Liña 116–118** — Komentáriu seksaun: upload (Android → Drive), metadata, download (Drive → Spark / API).
- **Liña 119** — `_GET_ATTEMPTS = 3`.
- **Liña 120** — `_GET_TIMEOUT = (15, 60)` — komentáriu: (konekta, lee) segundu.
- **Liña 121** — `_REQUEST_TIMEOUT = (15, 60)` — chamada seluk (upload, buka, partilla).
- **Liña 122** — `_UPLOAD_ATTEMPTS = 3` — upload koko fali depois timeout/konesaun monu.
- **Liña 123** — Komentáriu: resposta "slow down" Google, koko fali ho backoff.
- **Liña 124** — `_RATE_LIMIT_ATTEMPTS = 6` — hein ~1, 2, 4, 8, 16 s (+ jitter), ~30 s total.
- **Liña 125** — `_RATE_LIMIT_REASONS = ("rateLimitExceeded", "userRateLimitExceeded")`.
- **Liña 126** — `_RETRY_STATUSES = (429, 500, 502, 503, 504)`.
- **Liña 127** — `_sleep = __import__("time").sleep` — komentáriu: teste troka ida ne'e.
- **Liña 130** — `def _should_back_off(resp) -> bool:`
- **Liña 131** — `if resp.status_code in _RETRY_STATUSES:`
- **Liña 132** — `return True`.
- **Liña 133** — `if resp.status_code == 403:` — komentáriu: 403 mós "la iha permisaun" — koko fali só tipu rate-limit.
- **Liña 134** — `try:`
- **Liña 135** — `errors = (resp.json().get("error") or {}).get("errors") or []`.
- **Liña 136** — `except ValueError:` — body la'ós JSON.
- **Liña 137** — `return False`.
- **Liña 138** — `return any(e.get("reason") in _RATE_LIMIT_REASONS for e in errors)`.
- **Liña 139** — `return False`.
- **Liña 142** — `def _request(method: str, url: str, **kwargs):`
- **Liña 143–144** — Docstring: chamada Drive hotu: koko fali "slow down" no 5xx ho backoff, sempre timeout; fó resposta ikus ba `_check()`.
- **Liña 145** — `import random`.
- **Liña 146** — `kwargs.setdefault("timeout", _REQUEST_TIMEOUT)`.
- **Liña 147** — `for attempt in range(1, _RATE_LIMIT_ATTEMPTS + 1):` — 1..6.
- **Liña 148** — `resp = getattr(_authed_session(), method.lower())(url, **kwargs)` — ezemplu `session.get(url, params=..., timeout=...)`.
- **Liña 149** — `if attempt == _RATE_LIMIT_ATTEMPTS or not _should_back_off(resp):`
- **Liña 150** — `return resp`.
- **Liña 151** — `wait = 2 ** (attempt - 1) + random.random()` — 1, 2, 4, 8, 16 s + 0–1 s.
- **Liña 152–153** — `print(...)` — "Google Drive answered HTTP <kódigu> (busy/rate limit); retrying in X.Xs".
- **Liña 154** — `_sleep(wait)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `_request("GET", url)` → resposta 429 → hein ~1.3 s → 429 → ~2.7 s → 200 → return. Resposta 403 "insufficientPermissions" → `_should_back_off` = `False` → return kedas (la koko fali).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `_GET_TIMEOUT`, `_REQUEST_TIMEOUT` | `tuple[int, int]` |
| `_*_ATTEMPTS` | `int` |
| `_RETRY_STATUSES` | `tuple[int, ...]` |
| `_should_back_off` | `Response -> bool` |
| `_request` | `(str, str, **kwargs) -> Response` |
| `wait` | `float` |

**7. Ligasaun OOP**

`getattr(session, "get")` — **polimorfizmu dinámiku**: kódigu hili métodu objetu tuir string. Padraun **Retry with exponential backoff** (prinsípiu, implementa ho funsaun).

**8. Impaktu integrasaun**

Chamada hotu (`_get_with_retry`, upload, partilla, `ensure_folder`) liuhusi `_request`.

**9. Checkpoint ba principiante**

**P:** Tanba sá aumenta `random.random()` ba tempu hein?
**R:** "Jitter": se prosesu barak hetan 429 hamutuk, sira la koko fali iha segundu hanesan (evita sobrekarga foun).

#### D.2.5 Network retry, GET, metadata, download no thumbnail

**1. Kódigu orijinál**

{{code storage/drive_client.py 157-202}}

**2. Objetivu**

`_with_network_retry` koko fali chamada bainhira konesaun monu (2 s, 4 s...). `_get_with_retry` aplika ida ne'e ba GET (GET de'it — repete POST bele harii arkivu rua). Funsaun publiku: metadata, download bytes, thumbnail.

**3. Fundamentu Python**

- **Funsaun ne'ebé simu funsaun** (`call`) — *higher-order function*.
- **`label.format(attempt=attempt)`** — formata string ho `{attempt}`.
- **`resp.content`** — lee body tomak (forsa download iha loop retry).
- **`re.sub(r"=s\d+$", ...)`** — troka tamañu iha rohan URL.

**4. Esplikasaun liña-ba-liña**

- **Liña 157** — `def _with_network_retry(attempts: int, label: str, call):`
- **Liña 158** — Docstring: `call(attempt)`, koko fali depois konesaun monu (2 s, 4 s, ...).
- **Liña 159** — `import requests`.
- **Liña 160** — `for attempt in range(1, attempts + 1):`
- **Liña 161** — `try:`
- **Liña 162** — `return call(attempt)`.
- **Liña 163–165** — `except (ConnectionError, ChunkedEncodingError, Timeout) as e:` — konesaun monu, body la kompletu, timeout.
- **Liña 166** — `if attempt == attempts:`
- **Liña 167** — `raise` — koko ikus: hamosu erru.
- **Liña 168** — `print(...)` — "<label> failed (...); retrying".
- **Liña 169** — `_sleep(2 * attempt)` — 2 s, 4 s.
- **Liña 172** — `def _get_with_retry(url: str, params: dict):`
- **Liña 173–174** — Docstring: GET ho timeout no retry (GET de'it: repete POST bele harii arkivu segundu).
- **Liña 175** — `def get(_attempt):`
- **Liña 176** — `resp = _request("GET", url, params=params, timeout=_GET_TIMEOUT)`.
- **Liña 177** — `resp.content` — komentáriu: forsa lee body tomak iha loop retry.
- **Liña 178** — `return resp`.
- **Liña 179** — `return _with_network_retry(_GET_ATTEMPTS, "GET attempt {attempt}", get)`.
- **Liña 182** — `def _get_file(file_id: str, params: dict, action: str):`
- **Liña 183** — Docstring: GET `files/<file_id>` (ho retry), kontrola.
- **Liña 184** — `_require_valid_id(file_id)`.
- **Liña 185** — `return _check(_get_with_retry(f"{DRIVE_API}/files/{file_id}", params), f"{action} {file_id}")`.
- **Liña 188** — `def get_file_metadata(file_id: str) -> dict:`
- **Liña 189** — `return _get_file(file_id, {"fields": FILE_METADATA_FIELDS}, "get metadata for").json()`.
- **Liña 192** — `def download_file(file_id: str) -> bytes:`
- **Liña 193** — `return _get_file(file_id, {"alt": "media"}, "download").content` — `alt=media` = konteúdu arkivu.
- **Liña 196** — `def download_thumbnail(file_id: str, size_px: int = 220) -> Optional[bytes]:`
- **Liña 197** — Docstring: preview ki'ik Google, ka `None`.
- **Liña 198** — `link = _get_file(file_id, {"fields": "thumbnailLink"}, ...).json().get("thumbnailLink")`.
- **Liña 199** — `if not link:`
- **Liña 200** — `return None`.
- **Liña 201** — `resp = _get_with_retry(re.sub(r"=s\d+$", f"=s{int(size_px)}", link), {})`.
- **Liña 202** — `return resp.content if resp.status_code < 400 else None`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `download_file("1AbCdEf")` → `GET https://www.googleapis.com/drive/v3/files/1AbCdEf?alt=media` → koko 1: `ConnectionError` → hein 2 s → koko 2: 200 → `bytes` 2,457,600.

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `_with_network_retry(attempts, label, call)` | rezultadu `call` | erru rede ikus |
| `_get_with_retry(url, params)` | `Response` | erru rede |
| `get_file_metadata(file_id)` | `dict` | `DriveError` |
| `download_file(file_id)` | `bytes` | `DriveError` |
| `download_thumbnail(file_id, size_px)` | `bytes ka None` | `DriveError` |

**7. Ligasaun OOP**

`call` mak funsaun haruka hanesan objetu (first-class function); funsaun interior `get` mak closure. La iha klase.

**8. Impaktu integrasaun**

`download_file` ← `image_storage.load_image_bytes` ← Spark `_DOWNLOADS` (D.4). `get_file_metadata` ← `_clean_drive_event` (D.4).

**9. Checkpoint ba principiante**

**P:** Tanba sá network retry só ba GET, la'ós POST?
**R:** Se POST (harii arkivu) to'o Drive maibé resposta lakon, repete sei harii arkivu daruak. GET la muda buat ida, entaun seguru repete. (Upload iha nia proteksaun rasik: buka uluk — D.2.7.)
