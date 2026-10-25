"""
Give a farmer their own private folders in the central Google Drive.

  python scripts/drive_share_user.py farmer.name@gmail.com [more@gmail.com ...]

Creates images/<email>/ (writer) and results/<email>/ (reader), shared only
with that farmer. Safe to repeat. Also add the email as a test user in
Google Cloud (see README "Google Drive setup").
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
