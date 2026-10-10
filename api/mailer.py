"""
Outbound email for Gmail verification codes (registration feature, section
8). Real SMTP send when MAIL_HOST is configured; falls back to writing the
message to a local "outbox" file when it isn't, purely so the app is
runnable without SMTP credentials in local dev — mirroring
kafka/producer.py's local-queue fallback. This is an infrastructure
fallback, not a fake/simulated send: nothing about the *content* is
simulated, only the transport.

SMTP credentials always come from environment variables
(config/settings.py) — never hard-coded, never committed.
"""

import logging
import smtplib
from email.message import EmailMessage

from config.settings import (
    MAIL_HOST, MAIL_PORT, MAIL_USERNAME, MAIL_PASSWORD, MAIL_FROM,
    MAIL_FROM_NAME, MAIL_USE_TLS, MAIL_OUTBOX_DIR, SYSTEM_NAME,
)

logger = logging.getLogger(__name__)


class EmailSendError(Exception):
    pass


def _verification_email_body(full_name: str, code: str, ttl_minutes: int) -> str:
    return (
        f"Hello {full_name},\n\n"
        f"Thank you for registering with {SYSTEM_NAME}.\n\n"
        f"Your Gmail verification code is:\n\n"
        f"{code}\n\n"
        f"This code expires in {ttl_minutes} minutes and can only be used once.\n\n"
        f"If you did not create this account, you can safely ignore this email — "
        f"no account will be activated without this code.\n\n"
        f"Regards,\n{SYSTEM_NAME}"
    )


def send_verification_email(to_email: str, full_name: str, code: str, ttl_minutes: int) -> None:
    subject = f"{SYSTEM_NAME} — Your Gmail verification code"
    body = _verification_email_body(full_name, code, ttl_minutes)

    if not MAIL_HOST:
        _write_to_outbox(to_email, subject, body)
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{MAIL_FROM_NAME} <{MAIL_FROM}>" if MAIL_FROM_NAME else MAIL_FROM
    msg["To"] = to_email
    msg.set_content(body)

    try:
        with smtplib.SMTP(MAIL_HOST, MAIL_PORT, timeout=10) as smtp:
            if MAIL_USE_TLS:
                smtp.starttls()
            if MAIL_USERNAME and MAIL_PASSWORD:
                smtp.login(MAIL_USERNAME, MAIL_PASSWORD)
            smtp.send_message(msg)
    except (smtplib.SMTPException, OSError) as e:
        # Never leak SMTP internals to the caller/API response (section 11:
        # "Do not expose sensitive information in error messages or logs")
        # — log the detail server-side, raise a generic error upstream.
        logger.error("Failed to send verification email to %s: %s", to_email, e)
        raise EmailSendError("Could not send verification email") from e


def _write_to_outbox(to_email: str, subject: str, body: str) -> None:
    """Dev-only fallback used when MAIL_HOST is not configured."""
    MAIL_OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = to_email.replace("/", "_").replace("@", "_at_")
    import time
    path = MAIL_OUTBOX_DIR / f"{int(time.time() * 1000)}_{safe_name}.txt"
    path.write_text(f"To: {to_email}\nSubject: {subject}\n\n{body}\n", encoding="utf-8")
    logger.warning(
        "MAIL_HOST is not configured — verification email for %s was written to %s "
        "instead of being sent. Set MAIL_HOST/MAIL_USERNAME/MAIL_PASSWORD to send real email.",
        to_email, path,
    )
