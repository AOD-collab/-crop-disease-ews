"""
Creates the tables and, if there are no users at all, the first ADMIN account
from BOOTSTRAP_ADMIN_USERNAME / BOOTSTRAP_ADMIN_PASSWORD.

Run: python -m storage.init_db
"""

import sys

from config.settings import (
    BOOTSTRAP_ADMIN_USERNAME, BOOTSTRAP_ADMIN_PASSWORD, BOOTSTRAP_ADMIN_EMAIL, BOOTSTRAP_ADMIN_FULL_NAME,
)
from storage.database import init_db, SessionLocal, User, Role, UserStatus
from storage.auth import hash_password


def run():
    init_db()
    with SessionLocal() as session:
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
            # a configured admin account, so it is marked verified
            email_verified=True,
        )
        session.add(admin)
        session.commit()
        print(f"[init_db] Tables created. Bootstrapped ADMIN account '{BOOTSTRAP_ADMIN_USERNAME}'.")


if __name__ == "__main__":
    run()
    sys.exit(0)
