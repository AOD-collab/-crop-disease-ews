"""
Give a farmer their own private folders in the central Google Drive.

  python scripts/drive_share_user.py farmer.name@gmail.com [more@gmail.com ...]

For each email this creates (or reuses):

  CropDiseaseEWS/images/<email>/   shared ONLY with that farmer as "writer"
                                   (the app uploads their photos here)
  CropDiseaseEWS/results/<email>/  shared ONLY with that farmer as "reader"
                                   (Spark writes their results here; the app reads them)

The parent folders CropDiseaseEWS/images and /results are NOT shared with
anyone, so a farmer can only ever see their own two folders — in the app
and in Google Drive itself. Running it again for the same email is safe.

This is the "user registration" step of the Google Drive design. Also add
the same email as a *test user* on the OAuth consent screen while the
Google Cloud app is in Testing (see README "Google Drive setup"). To block someone
later, deactivate their user in the dashboard (the Drive watcher then
rejects their uploads) and/or remove their access to their folders.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storage import drive_client  # noqa: E402


def main(emails) -> int:
    if not emails:
        print(__doc__)
        return 1
    for email in emails:
        email = drive_client.register_farmer(email)["email"]
        print(f"{email}: images/{email}/ (upload) and results/{email}/ (read) — shared only with {email}")
    project = drive_client.google_cloud_project_id()
    print("\nAlso add each email as a TEST USER in Google Cloud (required while the app is in Testing):")
    print(f"  https://console.cloud.google.com/auth/audience?project={project}" if project
          else "  Google Cloud Console > Google Auth Platform > Audience > Test users")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
