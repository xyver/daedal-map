"""Small app-side security helpers for the hosted API."""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque
from typing import Deque

from fastapi import Request


def get_allowed_origins() -> list[str]:
    """Return the configured CORS allowlist for browser callers."""
    configured = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
    if configured:
        return [origin.strip() for origin in configured.split(",") if origin.strip()]

    return [
        "https://app.daedalmap.com",
        "https://daedalmap.io",
        "https://www.daedalmap.io",
        "https://daedalmap.com",
        "https://www.daedalmap.com",
        "http://localhost:7000",
        "http://localhost:8080",
        "http://localhost:8000",
        "http://localhost:8001",
        "http://127.0.0.1:7000",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:8000",
        "http://127.0.0.1:8001",
    ]


def is_https_request(request: Request) -> bool:
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip().lower()
    if forwarded_proto:
        return forwarded_proto == "https"
    return request.url.scheme == "https"


def get_client_ip(request: Request) -> str:
    """
    Best-effort client IP for app-side throttling.

    This prefers proxy-forwarded headers because Railway / Cloudflare deployments
    sit behind one or more reverse proxies in production.
    """
    for header in ("cf-connecting-ip", "x-forwarded-for", "x-real-ip"):
        raw = (request.headers.get(header) or "").strip()
        if raw:
            return raw.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


class SlidingWindowRateLimiter:
    """Thread-safe, in-memory sliding-window limiter."""

    def __init__(self):
        self._events: dict[str, Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
        now = time.time()
        cutoff = now - window_seconds

        with self._lock:
            bucket = self._events[key]
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= limit:
                retry_after = max(1, int(bucket[0] + window_seconds - now))
                return False, retry_after

            bucket.append(now)
            return True, 0


rate_limiter = SlidingWindowRateLimiter()
