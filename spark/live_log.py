"""
What the EWS-Spark console window shows for real-time farmer photos, so the
live pipeline can be watched like the scalability test: one line when a
micro-batch arrives from Kafka, one per photo when Spark has its result, one
when that result reaches the farmer's Google Drive, and one when the batch
commits.

SPARK_LIVE_LOG=false (.env) turns these lines off; errors, skips and
[ALERT] lines are printed either way. Each line is ONE locked write, so lines
from the parallel photo threads never interleave, and writing it costs
microseconds next to the seconds a photo spends in Drive and the CNN —
nothing about the processing itself changes.
"""

import os
import sys
import threading
from datetime import datetime
from functools import lru_cache

ENABLED = os.getenv("SPARK_LIVE_LOG", "true").strip().lower() not in ("0", "false", "no", "off")
_LOCK = threading.Lock()


def say(line: str) -> None:
    if not ENABLED:
        return
    text = f"[spark {datetime.now().strftime('%H:%M:%S')}] {line}\n"
    with _LOCK:
        try:
            sys.stdout.write(text)
            sys.stdout.flush()
        except (UnicodeEncodeError, OSError):      # a console that can't show a character / is gone
            pass


@lru_cache(maxsize=4096)
def farmer_label(user_id: str) -> str:
    """The farmer's Gmail for display (looked up once per farmer)."""
    if not user_id:
        return "?"
    try:
        from storage.database import SessionLocal, User
        with SessionLocal() as s:
            user = s.get(User, user_id)
            return (user.email or user.username) if user else user_id[:8]
    except Exception:  # noqa: BLE001 - display only
        return user_id[:8]


def batch_started(batch_id, n_events: int) -> None:
    say(f"[kafka]  batch {batch_id}: {n_events} photo event(s) received from Kafka")


def photo_line(event: dict, status: str, result: dict = None, error: str = None,
               seconds: float = None) -> str:
    head = (f"{(event.get('image_id') or '?')[:8]}  {farmer_label(event.get('user_id'))}  "
            f"{event.get('location') or 'no location'}")
    if status != "COMPLETED":
        return f"[photo]  {head}  FAILED: {(error or '')[:160]}"
    t = (result or {}).get("timings") or {}
    conf = result.get("prediction_confidence")
    verdict = (f"{result.get('predicted_class')}"
               + (f" {conf * 100:.0f}%" if isinstance(conf, (int, float)) else "")
               + f"  risk {result.get('risk_level')}")
    stages = "  ".join(f"{label} {t[key]:.2f}s" for key, label in
                       (("read_s", "download"), ("preprocess_s", "preprocess"), ("cnn_s", "cnn"),
                        ("render_s", "render")) if isinstance(t.get(key), (int, float)))
    total = f"  total {seconds:.2f}s" if seconds is not None else ""
    return f"[photo]  {head}  -> {verdict}  ({stages}{total})"


def photo_done(event: dict, status: str, result: dict = None, error: str = None, seconds: float = None) -> None:
    say(photo_line(event, status, result, error, seconds))


def result_delivered(event: dict, delivered: bool, seconds: float) -> None:
    image_id = (event.get("image_id") or "?")[:8]
    if delivered:
        say(f"[drive]  {image_id}  result + processed image delivered to "
            f"{farmer_label(event.get('user_id'))}'s Drive ({seconds:.2f}s)")
    else:
        say(f"[drive]  {image_id}  result NOT delivered to Drive — "
            f"`python -m storage.requeue_stuck` re-sends it")


def batch_committed(batch_id, ok: int, failed: int, skipped: int, seconds: float) -> None:
    say(f"[spark]  batch {batch_id} committed: {ok} ok, {failed} failed, {skipped} skipped "
        f"in {seconds:.2f}s")
