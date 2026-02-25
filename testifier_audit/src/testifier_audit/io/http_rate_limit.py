from __future__ import annotations

import threading
import time

GLOBAL_HTTP_MIN_INTERVAL_SECONDS = 1.0

_rate_limit_lock = threading.Lock()
_next_allowed_request_monotonic = 0.0


def wait_for_global_http_slot(
    *,
    min_interval_seconds: float = GLOBAL_HTTP_MIN_INTERVAL_SECONDS,
) -> None:
    interval = max(0.0, float(min_interval_seconds))
    if interval <= 0.0:
        return

    global _next_allowed_request_monotonic
    with _rate_limit_lock:
        now = time.monotonic()
        if now < _next_allowed_request_monotonic:
            time.sleep(_next_allowed_request_monotonic - now)
            now = time.monotonic()
        _next_allowed_request_monotonic = now + interval


def _reset_global_http_rate_limiter_for_tests() -> None:
    global _next_allowed_request_monotonic
    with _rate_limit_lock:
        _next_allowed_request_monotonic = 0.0
