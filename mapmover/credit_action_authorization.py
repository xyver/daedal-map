"""Verify a short-lived first-party account-credit action authorization."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from typing import Any

from fastapi import Request

from mapmover.api_query_commercial import commercial_access_internal_token


CREDIT_ACTION_HEADER = "x-daedalmap-credit-authorization"
TOKEN_VERSION = 1
MAX_TOKEN_AGE_SECONDS = 120


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def verified_credit_action(
    request: Request,
    *,
    capability_id: str,
    quote_id: str,
    request_id: str,
    user_id: str | None,
    now: int | None = None,
) -> bool:
    token = str(request.headers.get(CREDIT_ACTION_HEADER) or "").strip()
    secret = commercial_access_internal_token()
    if not token or not secret or not user_id:
        return False
    try:
        encoded, supplied_signature = token.split(".", 1)
        expected_signature = base64.urlsafe_b64encode(
            hmac.new(secret.encode("utf-8"), encoded.encode("ascii"), hashlib.sha256).digest()
        ).decode("ascii").rstrip("=")
        if not hmac.compare_digest(supplied_signature, expected_signature):
            return False
        payload: Any = json.loads(_b64url_decode(encoded).decode("utf-8"))
        if not isinstance(payload, dict):
            return False
        current = int(time.time() if now is None else now)
        issued_at = int(payload.get("iat") or 0)
        expires_at = int(payload.get("exp") or 0)
    except (ValueError, TypeError, binascii.Error, json.JSONDecodeError, UnicodeDecodeError):
        return False
    return bool(
        payload.get("v") == TOKEN_VERSION
        and hmac.compare_digest(str(payload.get("sub") or ""), user_id)
        and hmac.compare_digest(str(payload.get("cap") or ""), capability_id)
        and hmac.compare_digest(str(payload.get("quote") or ""), quote_id)
        and hmac.compare_digest(str(payload.get("request") or ""), request_id)
        and issued_at <= current <= expires_at
        and 0 <= expires_at - issued_at <= MAX_TOKEN_AGE_SECONDS
    )
