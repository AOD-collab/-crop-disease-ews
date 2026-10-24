"""
Creates all tables (if missing) and, only if the users table is completely
empty, bootstraps a single ADMIN account from BOOTSTRAP_ADMIN_USERNAME /
BOOTSTRAP_ADMIN_PASSWORD (config/settings.py / env vars) so there is a way
to log in and create further users/fields.

Run:
    python -m storage.init_db
"""

import sys

from config.settings import (
    BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_PASSWORD, BOOTSTRAP_ADMIN_EMAIL, BOOTSTRAP_ADMIN_FULL_NAME,
)
from storage.database import init_db, SessionLocal, User, Role, UserStatus
from storage.auth import hash_password
from storage.processing_mode import migrate_from_database


def run():
    init_db()
    migrate_from_database()     # the processing mode's old system_settings row -> its file
    session = SessionLocal()
    try:
        existing = session.query(User).count()
        if existing > 0:
            print(f"[init_db] {existing} user(s) already exist — skipping admin bootstrap.")
            return

        if not BOOTSTRAP_ADMIN_PASSWORD:
            print(
                "[init_db] Tables created. No users exist yet, and "
                "BOOTSTRAP_ADMIN_PASSWORD is not set, so no admin account was "
                "created. Set BOOTSTRAP_ADMIN_USERNAME / BOOTSTRAP_ADMIN_PASSWORD "
                "and re-run, or insert a user manually."
            )
            return

        admin = User(
            username=BOOTSTRAP_ADMIN_USERNAME,
            full_name=BOOTSTRAP_ADMIN_FULL_NAME,
            email=BOOTSTRAP_ADMIN_EMAIL,
            password_hash=hash_password(BOOTSTRAP_ADMIN_PASSWORD),
            role=Role.ADMIN,
            status=UserStatus.ACTIVE,
            # This is a deliberate, config-driven administrative account —
            # not an arbitrary self-registered Gmail address — so marking
            # it pre-verified here doesn't conflict with the "never
            # auto-verify an arbitrary address" rule.
            email_verified=True,
        )
        session.add(admin)
        session.commit()
        print(f"[init_db] Tables created. Bootstrapped ADMIN account '{BOOTSTRAP_ADMIN_USERNAME}'.")
    finally:
        session.close()


if __name__ == "__main__":
    run()
    sys.exit(0)
