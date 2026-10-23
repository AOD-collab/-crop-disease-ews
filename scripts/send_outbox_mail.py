import os
import sys
import time
import smtplib
from email.message import EmailMessage
from pathlib import Path

# Configuration
SENDER_EMAIL = "angelsarmento20@gmail.com"
APP_PASSWORD = "mjkm foct myeb dgky"  # Replace with your 16-character Google App Password

# Seconds to wait between folder checks when running in watch mode.
POLL_INTERVAL = int(os.getenv("MAIL_POLL_INTERVAL", "30"))

# Where api/mailer.py drops verification emails when MAIL_HOST is not configured.
OUTBOX_DIR = Path(os.getenv("MAIL_OUTBOX_DIR", str(Path(__file__).resolve().parent.parent / "storage" / "mail_outbox")))
SENT_DIR = OUTBOX_DIR / "sent"


def parse_outbox_file(path):
    """Parse an outbox .txt file into (recipient, subject, body).

    Format written by api/mailer.py._write_to_outbox:
        To: <email>
        Subject: <subject>
        <blank line>
        <body...>
    """
    text = path.read_text(encoding="utf-8")
    headers, _, body = text.partition("\n\n")

    recipient = None
    subject = None
    for line in headers.splitlines():
        if line.lower().startswith("to:"):
            recipient = line[3:].strip()
        elif line.lower().startswith("subject:"):
            subject = line[8:].strip()

    if not recipient:
        raise ValueError(f"{path.name}: no 'To:' header found")

    return recipient, subject or "(no subject)", body.rstrip("\n")


def send_bot_email(server, recipient, subject, body):
    # Construct the message
    msg = EmailMessage()
    msg["From"] = SENDER_EMAIL
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    server.send_message(msg)
    print(f"Email successfully sent to {recipient}!")


def flush_outbox():
    """Send every *.txt currently in the outbox. Returns the number sent."""
    files = sorted(OUTBOX_DIR.glob("*.txt"))
    if not files:
        return 0

    print(f"Found {len(files)} message(s) in {OUTBOX_DIR}")
    sent = 0

    try:
        # Connect to Gmail's SMTP server on port 587
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()  # Secure the connection using TLS
            server.login(SENDER_EMAIL, APP_PASSWORD)

            SENT_DIR.mkdir(parents=True, exist_ok=True)
            for path in files:
                try:
                    recipient, subject, body = parse_outbox_file(path)
                    send_bot_email(server, recipient, subject, body)
                    # Move it aside so it is not sent again on the next run.
                    path.rename(SENT_DIR / path.name)
                    sent += 1
                except Exception as e:
                    print(f"Failed to send {path.name}: {e}")

    except Exception as e:
        print(f"Failed to connect / log in: {e}")

    return sent


def main():
    once = "once" in sys.argv[1:]

    if once:
        if flush_outbox() == 0:
            print(f"No messages found in {OUTBOX_DIR}")
        return

    # Watch mode: check the folder forever. When files are there, send them,
    # then sleep; when the folder is empty, just sleep and check again.
    print(f"Watching {OUTBOX_DIR} every {POLL_INTERVAL}s. Press Ctrl+C to stop.")
    try:
        while True:
            flush_outbox()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
