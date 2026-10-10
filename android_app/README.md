# Crop Disease Early Warning — Android App (USER client)

A native Android (Kotlin) app for the **USER** role (DIT manual sections
5-6): register/log in, pick a location, take/choose one or more real leaf
photos, upload them, and see the disease/risk result for each once Kafka +
Spark + the CNN finish processing it — plus a full browsable history of
every photo ever sent, with a real thumbnail of the photo itself next to
each result.

Themed with a gold/amber palette (light + an explicit dark theme,
following the phone's day/night setting automatically) matching the admin
dashboard's look.

**Compiles cleanly in this environment** (`gradlew clean assembleDebug`) —
see "Build environment gotcha" below for the one thing that isn't
automatic. **Not run on an actual device/emulator** here, though — treat
the UI as reviewed-and-compiling, not click-tested.

## Screens

Login is the entry screen; Home/History/Settings are a bottom navigation
bar once signed in; Upload is reached from Home's scan button. The app
talks only to **Google Drive** (REST API v3) — never to a server of ours.

1. **Login** (`LoginActivity.kt`) — "Sign in with Google" (Google Identity
   Services, Drive access). The chosen Google account's email becomes the
   user's identity (`SessionStore`). No password is stored.
2. **Home** (`HomeActivity.kt`) — a greeting, a large "Scan Maize for
   Disease" button that launches Upload, and a "Recent Diagnoses" preview
   read from Drive, with a thumbnail of each recent photo.
3. **Upload** (`MainActivity.kt`) — a Location dropdown (fixed list in
   `res/values/strings.xml`; "Other" reveals a free-text field). Take
   photos, pick several from the gallery, or **choose a whole folder** —
   no limit on how many. Each photo gets a unique `image_id` (UUID) and goes
   into an upload queue saved on the phone (`UploadQueue.kt`, SQLite);
   `UploadWorker.kt` (WorkManager, foreground with an "Uploading X of N"
   notification) sends each one, unchanged, to the farmer's own Drive folder
   as `images/<email>/<image_id>.jpg` — 3 at a time, with retries, and it
   keeps going when the app is closed, after a reboot, or when the
   connection comes back. The laptop picks it up (Drive watcher → Kafka →
   Spark). For uploads of up to 20 photos the app polls `results/<email>/`
   every 2 seconds until `<image_id>.json` appears with `COMPLETED` — then
   it shows the disease class, confidence and risk, and the **original and
   processed (marked-up) photo side by side** — or `FAILED` (the recorded
   error, never a fake result). Bigger uploads show their progress here and
   their results in History.
4. **History** (`HistoryActivity.kt`) — every photo this user has uploaded,
   newest first (their files in Drive `/images` merged with their results
   in `/results`), each with its **processed** image (the original while
   still pending), a KPI row (healthy/diseased/pending/
   failed), filter chips and "Load more".
5. **Settings** (`SettingsActivity.kt`) — account info, English/Tetum
   language switch and Log out.

If Google access is revoked or expires, the next Drive call returns the
user to the sign-in screen.

## Build environment gotcha

`gradlew` needs **JDK 17+**. If your system's default `JAVA_HOME` is older
(common on a machine that also has an older JDK installed for other
projects), point it at a JDK 17+ install before building — Android
Studio's own bundled JBR works well for this (find it under Android
Studio's install directory, e.g. `.../Android Studio/jbr` on Windows).
Kotlin's incremental-compiler cache can print noisy
`IllegalArgumentException` stack traces against very new JDKs (25+) and
fall back to a non-daemon compile — harmless, the build still succeeds.

## How to open and run

1. Open Android Studio → **Open** → select this `android_app/` folder.
2. Let Gradle sync (downloads OkHttp, Material Components, etc from
   `app/build.gradle.kts`).
3. Run on an emulator or a physical phone (▶ button).

## Google Drive folders

Nothing to configure: after "Sign in with Google" the app finds the two
folders the administrator shared with that account
(`python scripts/drive_share_user.py <email>` on the laptop):
`images/<email>/` (it may upload there) and `results/<email>/` (read-only).
They are cached in `SessionStore` (`DriveRest.myFolders`).

## App icon

The launcher icon is a plain leaf silhouette on a green background,
defined entirely as vector XML (`app/src/main/res/drawable/
ic_launcher_background.xml` + `ic_launcher_foreground.xml`, wired up via
`mipmap-anydpi-v26/ic_launcher.xml` for the modern adaptive-icon system,
with a flattened `mipmap/ic_launcher.xml` fallback for pre-Android-8
devices) — no image-editing tools needed to change it.

To use your own icon instead, the easiest path is Android Studio's
**Image Asset Studio**: right-click `app/src/main/res` → **New → Image
Asset**, pick your PNG/SVG as the foreground layer, choose a background
color or image, and it regenerates all of the files above (including
proper legacy raster fallbacks) for you. If you'd rather hand-edit the
vector XML directly, just replace the `<path>` data in
`ic_launcher_foreground.xml` — the background/foreground split, safe-zone
sizing (icon content should stay within the inner ~66dp of the 108dp
canvas), and manifest wiring (`android:icon="@mipmap/ic_launcher"`) don't
need to change.

## Connecting to the system

The app does **not** connect to a server of ours. It talks only to
**Google Drive**, with each farmer's own Google sign-in:

1. Register the app in Google Cloud as an **Android OAuth client**
   (package `com.cropdisease.app` + your signing SHA-1).
2. The administrator gives each farmer their private folders:
   `python scripts/drive_share_user.py farmer@gmail.com`.

No server URL, ngrok, public IP or port forwarding is needed: the phone
and the laptop only make outgoing connections to Google Drive. Full steps:
[../DRIVE_SETUP.md](../DRIVE_SETUP.md).

## Project layout

```
app/src/main/java/com/cropdisease/app/
    LoginActivity.kt         "Sign in with Google" (Drive access via Google Identity Services)
    HomeActivity.kt              Post-login landing: scan CTA + recent diagnoses (from Drive)
    MainActivity.kt               Location selection, upload to Drive /images, result polling on Drive /results
    HistoryActivity.kt             Full upload history from Drive, KPI row, filter chips, thumbnails
    SettingsActivity.kt             Account info + language + log out
    SessionStore.kt                  Signed-in Google account (email, name) — no password or token stored
    DriveAuth.kt                       Google OAuth access token for Drive (AuthorizationClient)
    DriveRest.kt                        Drive REST API v3 calls: upload, results, history
    BottomNav.kt                       Shared Home/History/Settings tab-switching helper
    OutcomeUi.kt                        Shared "colored row + optional thumbnail" list-item builder
    ThumbnailLoader.kt                   Downloads the photo from Drive, downsampled, into an ImageView
app/src/main/res/layout/
    activity_login.xml
    activity_home.xml
    activity_main.xml
    activity_history.xml
    activity_settings.xml
    item_stat_tile.xml               A single KPI tile (used 4x in activity_history.xml)
app/src/main/res/values/
    colors.xml, values-night/colors.xml   Light + dark gold/amber palette
    themes.xml, values-night/themes.xml   Theme.CropDisease + shared button/text styles
    strings.xml                            Location dropdown options
app/src/main/res/menu/bottom_nav_menu.xml   Home/History/Settings nav items
app/build.gradle.kts                          Dependencies
```

## Not yet implemented (see main README "Future improvements")

- Running on an actual device/emulator (compiles here; never installed/clicked through).
- A custom in-app camera preview with a framing guide — currently launches
  the phone's stock camera app instead (reliable; doesn't match a
  from-scratch camera UI some mockups show).
- Offline queueing for photos taken with no signal.
- Push notifications for HIGH-risk results.
