"""
ONE-TIME Google Drive setup, run on the laptop/server (never on Android).

  python scripts/drive_authorize.py

What it does:
  1. Opens a browser for the normal Google OAuth consent screen. Sign in with
     the CENTRAL Google account that should store all images and results.
  2. Saves the resulting refresh token to GOOGLE_DRIVE_TOKEN_FILE
     (default storage/google_drive_token.json). Keep this file private:
     it is listed in .gitignore and must never go into the Android app.
  3. Creates (or finds) the folders
         CropDiseaseEWS/images    <- holds one private folder per farmer
         CropDiseaseEWS/results   <- holds one private folder per farmer
     (the per-farmer folders are made by scripts/drive_share_user.py)
     and saves their IDs to GOOGLE_DRIVE_FOLDERS_FILE
     (default storage/google_drive_folders.json), which config/settings.py
     reads automatically.

Prerequisite (see README "Google Drive setup"): a Google Cloud project with the Google
Drive API enabled and an OAuth client of type "Desktop app"; download its
JSON to GOOGLE_DRIVE_CLIENT_SECRET_FILE (default
storage/google_client_secret.json).
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (  # noqa: E402
    GOOGLE_DRIVE_CLIENT_SECRET_FILE,
    GOOGLE_DRIVE_TOKEN_FILE,
    GOOGLE_DRIVE_FOLDERS_FILE,
    GOOGLE_DRIVE_ROOT_FOLDER_NAME,
)


def main() -> int:
    secret = Path(GOOGLE_DRIVE_CLIENT_SECRET_FILE)
    if not secret.exists():
        print(f"OAuth client file not found: {secret}")
        print("Create a 'Desktop app' OAuth client in Google Cloud Console, download its JSON")
        print("and save it at that path (see README 'Google Drive setup').")
        return 1

    from google_auth_oauthlib.flow import InstalledAppFlow
    from storage import drive_client

    flow = InstalledAppFlow.from_client_secrets_file(str(secret), drive_client.SCOPES)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

    token_path = Path(GOOGLE_DRIVE_TOKEN_FILE)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())
    print(f"Saved OAuth token to {token_path}")

    # drive_client caches no session yet, so it will pick up the new token.
    root_id = drive_client.ensure_folder(GOOGLE_DRIVE_ROOT_FOLDER_NAME)
    images_id = drive_client.ensure_folder("images", root_id)
    results_id = drive_client.ensure_folder("results", root_id)

    folders = {
        "root_folder_id": root_id,
        "images_folder_id": images_id,
        "results_folder_id": results_id,
    }
    Path(GOOGLE_DRIVE_FOLDERS_FILE).write_text(json.dumps(folders, indent=2))
    print(f"Folders ready: {GOOGLE_DRIVE_ROOT_FOLDER_NAME}/images = {images_id}")
    print(f"               {GOOGLE_DRIVE_ROOT_FOLDER_NAME}/results = {results_id}")
    print(f"Saved folder IDs to {GOOGLE_DRIVE_FOLDERS_FILE}")
    print()
    print("Next step: give each farmer their own private folders:")
    print("       python scripts/drive_share_user.py farmer@gmail.com")
    print("(The Android app finds its folders by itself — nothing to paste into the app.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
