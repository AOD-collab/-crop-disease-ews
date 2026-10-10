import pytest
from fastapi.testclient import TestClient

from storage.database import init_db, SessionLocal, Base, engine, User, Role, UserStatus
from api.server import app
from api.auth import hash_password
from api import verification as vcode
from api.mailer import MAIL_OUTBOX_DIR


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture()
def client():
    return TestClient(app)


def _make_admin(session, username="admin1", password="adminpass1"):
    user = User(username=username, full_name="Admin", email=f"{username}@example.com",
                password_hash=hash_password(password), role=Role.ADMIN, status=UserStatus.ACTIVE,
                email_verified=True)
    session.add(user)
    session.commit()
    session.refresh(user)
    session.expunge(user)
    return user


def _admin_token(client, username="admin1", password="adminpass1"):
    resp = client.post("/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]


def _latest_code_for(email: str) -> str:
    """Pull the plain code out of the DB by re-deriving nothing — instead
    read it from the local mail outbox fallback (no MAIL_HOST configured
    in tests, see conftest.py), which is what a real Gmail inbox would
    have received."""
    matches = sorted(MAIL_OUTBOX_DIR.glob(f"*_{email.replace('@', '_at_')}.txt"))
    assert matches, f"no outbox email found for {email}"
    content = matches[-1].read_text()
    for line in content.splitlines():
        line = line.strip()
        if line.isdigit() and len(line) == 6:
            return line
    raise AssertionError(f"couldn't find a 6-digit code in {content!r}")


def test_register_rejects_non_gmail_address(client):
    resp = client.post("/auth/register", json={
        "full_name": "Jane Farmer", "email": "jane@yahoo.com",
        "password": "goodpass1", "confirm_password": "goodpass1",
    })
    assert resp.status_code == 400


def test_register_rejects_mismatched_passwords(client):
    resp = client.post("/auth/register", json={
        "full_name": "Jane Farmer", "email": "jane.farmer@gmail.com",
        "password": "goodpass1", "confirm_password": "different1",
    })
    assert resp.status_code == 400


def test_register_rejects_weak_password(client):
    resp = client.post("/auth/register", json={
        "full_name": "Jane Farmer", "email": "jane.farmer@gmail.com",
        "password": "short", "confirm_password": "short",
    })
    assert resp.status_code == 400


def test_register_creates_pending_user_and_sends_code(client):
    resp = client.post("/auth/register", json={
        "full_name": "Jane Farmer", "email": "Jane.Farmer@gmail.com",
        "password": "goodpass1", "confirm_password": "goodpass1",
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["email"] == "jane.farmer@gmail.com"

    session = SessionLocal()
    user = session.query(User).filter(User.email == "jane.farmer@gmail.com").one()
    assert user.status == UserStatus.PENDING_VERIFICATION
    assert user.email_verified is False
    assert user.password_hash != "goodpass1"
    assert user.role == Role.USER
    session.close()

    # never returned in the response
    assert "password" not in resp.text
    assert "verification_code" not in resp.text


def test_register_rejects_duplicate_email(client):
    payload = {"full_name": "Jane Farmer", "email": "dup@gmail.com",
               "password": "goodpass1", "confirm_password": "goodpass1"}
    first = client.post("/auth/register", json=payload)
    assert first.status_code == 200
    second = client.post("/auth/register", json=payload)
    assert second.status_code == 409


def test_pending_user_cannot_login(client):
    email = "pending@gmail.com"
    client.post("/auth/register", json={"full_name": "P", "email": email,
                                         "password": "goodpass1", "confirm_password": "goodpass1"})
    resp = client.post("/auth/login", json={"username": email, "password": "goodpass1"})
    assert resp.status_code == 403


def test_full_verification_flow_then_login(client):
    email = "verifyme@gmail.com"
    client.post("/auth/register", json={"full_name": "V", "email": email,
                                         "password": "goodpass1", "confirm_password": "goodpass1"})
    code = _latest_code_for(email)

    wrong = client.post("/auth/verify-email", json={"email": email, "code": "000000"})
    assert wrong.status_code == 400

    ok = client.post("/auth/verify-email", json={"email": email, "code": code})
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "ACTIVE"

    session = SessionLocal()
    user = session.query(User).filter(User.email == email).one()
    assert user.email_verified is True
    assert user.status == UserStatus.ACTIVE
    assert user.verification_code_hash is None
    session.close()

    login = client.post("/auth/login", json={"username": email, "password": "goodpass1"})
    assert login.status_code == 200, login.text


def test_verification_code_is_single_use(client):
    email = "singleuse@gmail.com"
    client.post("/auth/register", json={"full_name": "S", "email": email,
                                         "password": "goodpass1", "confirm_password": "goodpass1"})
    code = _latest_code_for(email)
    first = client.post("/auth/verify-email", json={"email": email, "code": code})
    assert first.status_code == 200
    second = client.post("/auth/verify-email", json={"email": email, "code": code})
    assert second.status_code == 400


def test_verification_attempts_are_limited(client):
    email = "bruteforce@gmail.com"
    client.post("/auth/register", json={"full_name": "B", "email": email,
                                         "password": "goodpass1", "confirm_password": "goodpass1"})
    for _ in range(vcode.attempts_exhausted.__globals__["VERIFICATION_CODE_MAX_ATTEMPTS"]):
        resp = client.post("/auth/verify-email", json={"email": email, "code": "111111"})
        assert resp.status_code in (400, 429)
    final = client.post("/auth/verify-email", json={"email": email, "code": "111111"})
    assert final.status_code == 429


def test_resend_gives_generic_response_for_unknown_email(client):
    resp = client.post("/auth/resend-verification", json={"email": "nobody@gmail.com"})
    assert resp.status_code == 200
    assert "sent" in resp.json()["message"].lower()


def test_resend_issues_a_new_working_code(client):
    email = "resend@gmail.com"
    client.post("/auth/register", json={"full_name": "R", "email": email,
                                         "password": "goodpass1", "confirm_password": "goodpass1"})
    old_code = _latest_code_for(email)

    session = SessionLocal()
    user = session.query(User).filter(User.email == email).one()
    user.last_verification_sent_at = None  # bypass cooldown for the test
    session.commit()
    session.close()

    resend = client.post("/auth/resend-verification", json={"email": email})
    assert resend.status_code == 200

    old_attempt = client.post("/auth/verify-email", json={"email": email, "code": old_code})
    assert old_attempt.status_code == 400  # old code invalidated

    new_code = _latest_code_for(email)
    new_attempt = client.post("/auth/verify-email", json={"email": email, "code": new_code})
    assert new_attempt.status_code == 200


def test_admin_create_user_defaults_to_pending_and_sends_code(client):
    session = SessionLocal()
    _make_admin(session)
    session.close()
    token = _admin_token(client)

    resp = client.post("/admin/users", headers={"Authorization": f"Bearer {token}"}, json={
        "full_name": "New Farmer", "email": "agent@gmail.com", "password": "goodpass1", "role": "USER",
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "PENDING_VERIFICATION"
    assert body["email_verified"] is False

    login = client.post("/auth/login", json={"username": "agent@gmail.com", "password": "goodpass1"})
    assert login.status_code == 403  # not verified yet


def test_admin_create_user_with_explicit_active_status_skips_verification(client):
    session = SessionLocal()
    _make_admin(session)
    session.close()
    token = _admin_token(client)

    resp = client.post("/admin/users", headers={"Authorization": f"Bearer {token}"}, json={
        "full_name": "Trusted Agent", "email": "trusted@gmail.com", "password": "goodpass1",
        "role": "USER", "status": "ACTIVE",
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "ACTIVE"

    login = client.post("/auth/login", json={"username": "trusted@gmail.com", "password": "goodpass1"})
    assert login.status_code == 200


def test_admin_cannot_delete_own_account(client):
    session = SessionLocal()
    admin = _make_admin(session)
    session.close()
    token = _admin_token(client)

    resp = client.delete(f"/admin/users/{admin.user_id}?confirm=true",
                          headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 400


def test_admin_cannot_remove_own_admin_role(client):
    session = SessionLocal()
    admin = _make_admin(session)
    session.close()
    token = _admin_token(client)

    resp = client.put(f"/admin/users/{admin.user_id}", headers={"Authorization": f"Bearer {token}"},
                       json={"role": "USER"})
    assert resp.status_code == 400


def test_admin_delete_requires_confirmation(client):
    session = SessionLocal()
    admin = _make_admin(session)
    target = User(username="todelete@gmail.com", full_name="To Delete", email="todelete@gmail.com",
                  password_hash=hash_password("goodpass1"), role=Role.USER, status=UserStatus.ACTIVE,
                  email_verified=True)
    session.add(target)
    session.commit()
    session.refresh(target)
    target_id = target.user_id
    session.close()
    token = _admin_token(client)

    unconfirmed = client.delete(f"/admin/users/{target_id}", headers={"Authorization": f"Bearer {token}"})
    assert unconfirmed.status_code == 400

    confirmed = client.delete(f"/admin/users/{target_id}?confirm=true",
                               headers={"Authorization": f"Bearer {token}"})
    assert confirmed.status_code == 200

    session = SessionLocal()
    deleted = session.get(User, target_id)
    assert deleted.is_deleted is True
    assert deleted.status == UserStatus.INACTIVE
    session.close()

    # soft-deleted users are excluded from the default listing
    listing = client.get("/admin/users", headers={"Authorization": f"Bearer {token}"})
    assert all(u["user_id"] != target_id for u in listing.json()["items"])


def test_changing_email_resets_verification(client):
    session = SessionLocal()
    admin = _make_admin(session)
    target = User(username="olduser@gmail.com", full_name="Old User", email="olduser@gmail.com",
                  password_hash=hash_password("goodpass1"), role=Role.USER, status=UserStatus.ACTIVE,
                  email_verified=True)
    session.add(target)
    session.commit()
    session.refresh(target)
    target_id = target.user_id
    session.close()
    token = _admin_token(client)

    resp = client.put(f"/admin/users/{target_id}", headers={"Authorization": f"Bearer {token}"},
                       json={"email": "newaddress@gmail.com"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "PENDING_VERIFICATION"
    assert body["email_verified"] is False


def test_changing_email_keeps_username_in_sync_for_login(client):
    """Regression test: for a self-registered-style account (username ==
    email), changing the email via admin update must also update the
    username the person actually logs in with — otherwise they'd need to
    keep using an email address that no longer appears anywhere on their
    own profile."""
    session = SessionLocal()
    admin = _make_admin(session)
    target = User(username="olduser2@gmail.com", full_name="Old User Two", email="olduser2@gmail.com",
                  password_hash=hash_password("goodpass1"), role=Role.USER, status=UserStatus.ACTIVE,
                  email_verified=True)
    session.add(target)
    session.commit()
    target_id = target.user_id
    session.close()
    token = _admin_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    resp = client.put(f"/admin/users/{target_id}", headers=headers,
                       json={"email": "newaddress2@gmail.com"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["username"] == "newaddress2@gmail.com"

    session = SessionLocal()
    reloaded = session.get(User, target_id)
    assert reloaded.username == "newaddress2@gmail.com"
    session.close()

    # Prove it end-to-end: verify the new address, then log in *with the
    # new email* — this is what would have failed before the fix.
    code = _latest_code_for("newaddress2@gmail.com")
    verify = client.post("/auth/verify-email", json={"email": "newaddress2@gmail.com", "code": code})
    assert verify.status_code == 200, verify.text

    login = client.post("/auth/login", json={"username": "newaddress2@gmail.com", "password": "goodpass1"})
    assert login.status_code == 200, login.text

    # The stale old email must no longer work as a username.
    old_login = client.post("/auth/login", json={"username": "olduser2@gmail.com", "password": "goodpass1"})
    assert old_login.status_code == 401


def test_changing_email_does_not_touch_username_when_they_already_differ(client):
    """An admin-created account can have a username that's intentionally
    different from its email (CreateUserRequest.username override) —
    changing the email in that case must leave the distinct username
    alone, since it never mirrored the email in the first place."""
    session = SessionLocal()
    admin = _make_admin(session)
    target = User(username="custom_handle", full_name="Custom Handle", email="handle@gmail.com",
                  password_hash=hash_password("goodpass1"), role=Role.USER, status=UserStatus.ACTIVE,
                  email_verified=True)
    session.add(target)
    session.commit()
    target_id = target.user_id
    session.close()
    token = _admin_token(client)
    headers = {"Authorization": f"Bearer {token}"}

    resp = client.put(f"/admin/users/{target_id}", headers=headers,
                       json={"email": "newhandle@gmail.com"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["username"] == "custom_handle"  # unchanged
