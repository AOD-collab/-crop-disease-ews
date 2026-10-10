"""
Minimal in-memory rate limiter for the auth endpoints (section 11:
"Implement rate limiting for registration, login, verification, and
resend-code requests").

This is intentionally simple: a per-process sliding-window counter keyed by
(bucket, key). It is NOT distributed — in a multi-process/multi-instance
deployment each process enforces its own limit, so treat this as a
best-effort abuse throttle layered on top of the per-account checks in
storage/database.py (verification_attempts, last_verification_sent_at),
not as the sole defense. Swap in Redis/a shared store for that case.
"""

import threading
import time
from collections import defaultdict, deque

_lock = threading.Lock()
_hits: dict[tuple[str, str], deque] = defaultdict(deque)


def allow(bucket: str, key: str, max_hits: int, window_seconds: int) -> bool:
    """Returns True and records a hit if under the limit, False (and
    records nothing extra) if the caller should be rejected."""
    now = time.monotonic()
    with _lock:
        q = _hits[(bucket, key)]
        cutoff = now - window_seconds
        while q and q[0] < cutoff:
            q.popleft()
        if len(q) >= max_hits:
            return False
        q.append(now)
        return True
