"""
Helpers for optional Supabase-backed auth context on API requests.

This is intentionally lightweight:
- no auth requirement for public use
- verifies bearer tokens against Supabase when present
- caches verification briefly to avoid repeated auth round-trips
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests
from fastapi import Request

from . import logger


AUTH_CACHE_TTL_SECONDS = 300
_auth_cache: Dict[str, Dict[str, Any]] = {}


def _get_bearer_token(request: Request) -> Optional[str]:
    auth_header = request.headers.get("authorization", "").strip()
    if not auth_header.lower().startswith("bearer "):
        return None
    token = auth_header[7:].strip()
    return token or None


def _get_supabase_auth_config() -> Optional[Dict[str, str]]:
    url = os.getenv("SUPABASE_URL", "").strip()
    anon_key = os.getenv("SUPABASE_ANON_KEY", "").strip()
    if not url or not anon_key:
        return None
    return {"url": url.rstrip("/"), "anon_key": anon_key}


def _get_cached_user(token: str) -> Optional[Dict[str, Any]]:
    entry = _auth_cache.get(token)
    if not entry:
        return None
    if time.time() - entry["cached_at"] > AUTH_CACHE_TTL_SECONDS:
        _auth_cache.pop(token, None)
        return None
    return entry["user"]


def _cache_user(token: str, user: Optional[Dict[str, Any]]) -> None:
    _auth_cache[token] = {
        "cached_at": time.time(),
        "user": user,
    }


def get_authenticated_user(request: Request) -> Optional[Dict[str, Any]]:
    """
    Return verified Supabase user info if a bearer token is present and valid.
    Returns None for anonymous requests or verification failures.
    """
    token = _get_bearer_token(request)
    if not token:
        return None

    cached = _get_cached_user(token)
    if cached is not None:
        return cached

    config = _get_supabase_auth_config()
    if not config:
        return None

    try:
        response = requests.get(
            f"{config['url']}/auth/v1/user",
            headers={
                "apikey": config["anon_key"],
                "Authorization": f"Bearer {token}",
            },
            timeout=5,
        )
        if response.status_code != 200:
            _cache_user(token, None)
            return None

        user = response.json()
        _cache_user(token, user)
        return user
    except Exception as exc:
        logger.warning(f"Supabase user verification failed: {exc}")
        return None


def build_session_cache_key(session_id: str, user: Optional[Dict[str, Any]]) -> str:
    """
    Build the backend session cache key.

    Authenticated users get a user-scoped cache namespace.
    Anonymous users keep their existing session ID behavior.
    """
    base_session_id = (session_id or "anonymous").strip() or "anonymous"
    user_id = (user or {}).get("id")
    if user_id:
        return f"user:{user_id}:{base_session_id}"
    return base_session_id
