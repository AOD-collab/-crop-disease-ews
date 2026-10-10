# Google Drive setup

Architecture: **Android → Google Drive → Kafka → Spark → Google Drive → Android**.
The phone talks **only to Google Drive**. The laptop talks only to Google Drive
and Kafka. There is no REST API between them and **no ngrok**.

```
Android (farmer signs in with Google) ──upload──► Drive  images/<email>/<image_id>.jpg
Laptop  kafka/drive_watcher.py  (Drive API, every 5 s, checks every farmer's folder)
        ──event: file id + metadata──► Kafka
Laptop  spark/streaming_job.py  ◄── Kafka;  downloads photo from Drive, runs the model
        ──► Drive  results/<email>/<image_id>.json
                   results/<email>/<image_id>_processed.jpg   (marked-up leaf)
Android ◄──reads── Drive results/<email>/   (original + processed shown side by side)
```

### Folder layout — every farmer has private folders

```
CropDiseaseEWS/                    (central account — shared with nobody)
├── images/                        (shared with nobody)
│   ├── alice@gmail.com/           shared ONLY with alice  (upload)
│   └── bob@gmail.com/             shared ONLY with bob    (upload)
└── results/                       (shared with nobody)
    ├── alice@gmail.com/           shared ONLY with alice  (read)
    │   ├── <image_id>.json
    │   └── <image_id>_processed.jpg
    └── bob@gmail.com/             shared ONLY with bob    (read)
```

A farmer can only ever see their own two folders — in the app and in
Google Drive itself.

## 1. Google Cloud project (once, ~10 minutes)

Sign in to <https://console.cloud.google.com/> with the **central Google account**.

1. Create a project, e.g. `crop-disease-ews`.
2. **APIs & Services → Library →** *Google Drive API* → **Enable**.
3. **OAuth consent screen** (Google Auth Platform):
   - User type **External**, app name `Crop Disease EWS`, your support email.
   - Scopes: add `https://www.googleapis.com/auth/drive`.
   - **Test users:** add the central account **and every farmer's Gmail**
     (up to 100). Only listed accounts can sign in while the app is in *Testing*.
     This is normal for an academic project; publishing a full-Drive app
     would need Google's verification, which you don't need here.
   - Note: in *Testing*, Google expires refresh tokens after 7 days. For the
     laptop this means re-running step 3 below weekly; the phones simply show
     Google's sign-in again when needed.
4. **Credentials → Create credentials → OAuth client ID**, twice, **in this
   same project** (the laptop and the phones share small file labels —
   Drive "appProperties" — which are only visible within one project):
   - **Desktop app** (for the laptop) → download the JSON → save as
     `storage/google_client_secret.json`.
   - **Android** (for the phones) → package name `com.cropdisease.app` and the
     **SHA-1** of the key that signs your APK. For a debug build, get it with
     `./gradlew signingReport` in `android_app/` (or from Android Studio's
     Gradle panel). No file to download: Google recognises the app by package
     name + SHA-1. **Nothing secret goes into the app.**

## 2. Laptop: install and authorize (once)

```
pip install -r requirements.txt
python scripts/drive_authorize.py
```

Sign in with the central account in the browser. The script saves the token
to `storage/google_drive_token.json` and creates `CropDiseaseEWS/images` and
`CropDiseaseEWS/results`.

These token/ID files stay on the laptop (they're in `.gitignore`).

## 3. Give each farmer their private folders

```
python scripts/drive_share_user.py farmer1@gmail.com farmer2@gmail.com
```

For each email this creates `images/<email>/` (shared with that farmer to
upload) and `results/<email>/` (shared with that farmer to read) — and
shares nothing else. Also add each email as a *test user* (step 1.3). To
block someone later, deactivate their user in the dashboard — the Drive
watcher then rejects their uploads — and/or remove their folder access.

## 4. Android: build

Nothing to configure in the app: after **Sign in with Google**, it finds the
two folders shared with that account by itself. Build and install as
before; on first launch choose the farmer's account and allow Drive access.

## 5. Run the laptop side

```
python run_all.py
```

This starts Kafka, Spark, the dashboard, the mail sender **and the Drive
watcher** (`EWS-DriveWatcher`). `--api` and `--ngrok` are no longer needed.

## How one photo flows

| Step | From → To | What travels |
|---|---|---|
| 1 | App → **Google Drive** | photo → `images/<email>/<image_id>.jpg`, tagged with `image_id`, location |
| 2 | **Drive watcher** (laptop) ← Drive | lists new files in every farmer's folder; uploader = Drive file owner (set by Google), must match the folder |
| 3 | Drive watcher → database | `images` row, `image_path = "gdrive:<file id>"` (user auto-created on first upload) |
| 4 | Drive watcher → **Kafka** | `image_id`, `user_id`, `drive_file_id`, location, timestamps — **no image bytes** |
| 5 | Kafka → **Spark** | the event |
| 6 | Spark ← Google Drive | downloads the photo; leaf check, EXIF, ResNet-18, risk; draws the processed image |
| 7 | Spark → **Google Drive** | `results/<email>/<image_id>.json` and `<image_id>_processed.jpg` |
| 8 | App ← **Google Drive** | shows original + processed side by side; History shows the processed image |

## Troubleshooting

| Symptom | Fix |
|---|---|
| App: *Your Google account cannot upload yet* | Run `scripts/drive_share_user.py <email>` and add the email as a test user. If you shared the folders only just now, sign out and in again in the app. |
| Google sign-in fails with *DEVELOPER_ERROR* / code 10 | The Android OAuth client's package name or SHA-1 doesn't match the installed APK. |
| Upload works but no result ever appears | Is `EWS-DriveWatcher` running? Its console shows `-> Kafka topic ...` for each photo. Then check `EWS-Spark`. |
| Watcher: *Google Drive not set up yet* | Run step 2. |
| Worked for a week, then the watcher fails | Token expired (Testing mode) — re-run `python scripts/drive_authorize.py`. |
| Photo stuck at INGESTED | `python -m storage.requeue_stuck --all` (works for Drive photos too). |
