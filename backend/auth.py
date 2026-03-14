from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import Request, Response

import config

from backend.api import ApiError


SESSION_COOKIE_NAME = "aiagents_session"


def _urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _urlsafe_b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}")


def get_admin_auth_material() -> str:
    return (getattr(config, "ADMIN_PASSWORD_HASH", "") or config.ADMIN_PASSWORD or "").strip()


def get_admin_auth_fingerprint() -> str:
    material = get_admin_auth_material()
    if not material:
        return ""
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def get_admin_auth_secret() -> bytes:
    material = get_admin_auth_material()
    return hashlib.sha256(f"aiagents-admin-auth|{material}".encode("utf-8")).digest()


def encode_admin_auth_token(issued_at: int) -> str:
    ttl = max(getattr(config, "ADMIN_SESSION_TTL_SECONDS", 28800), 60)
    payload = {
        "iat": int(issued_at),
        "exp": int(issued_at) + ttl,
        "fp": get_admin_auth_fingerprint(),
    }
    payload_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    payload_part = _urlsafe_b64encode(payload_text.encode("utf-8"))
    signature = hmac.new(
        get_admin_auth_secret(),
        payload_part.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{payload_part}.{_urlsafe_b64encode(signature)}"


def decode_admin_auth_token(token: str) -> Optional[dict[str, Any]]:
    if not token or "." not in token:
        return None
    try:
        payload_part, signature_part = token.split(".", 1)
        expected_signature = hmac.new(
            get_admin_auth_secret(),
            payload_part.encode("ascii"),
            hashlib.sha256,
        ).digest()
        actual_signature = _urlsafe_b64decode(signature_part)
        if not hmac.compare_digest(actual_signature, expected_signature):
            return None

        payload = json.loads(_urlsafe_b64decode(payload_part).decode("utf-8"))
        issued_at = int(payload.get("iat", 0))
        expires_at = int(payload.get("exp", 0))
        fingerprint = str(payload.get("fp", ""))
        now_ts = int(time.time())
        if issued_at <= 0 or expires_at <= now_ts:
            return None
        if fingerprint != get_admin_auth_fingerprint():
            return None
        return {"iat": issued_at, "exp": expires_at, "fp": fingerprint}
    except Exception:
        return None


def verify_admin_password(input_password: str) -> bool:
    pwd = input_password or ""
    hash_value = (getattr(config, "ADMIN_PASSWORD_HASH", "") or "").strip()
    if hash_value:
        try:
            algo, iter_text, salt_hex, digest_hex = hash_value.split("$", 3)
            if algo != "pbkdf2_sha256":
                return False
            iterations = int(iter_text)
            salt = bytes.fromhex(salt_hex)
            expected = bytes.fromhex(digest_hex)
            computed = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt, iterations)
            return hmac.compare_digest(computed, expected)
        except Exception:
            return False

    plain = (config.ADMIN_PASSWORD or "").strip()
    return bool(plain) and hmac.compare_digest(pwd, plain)


@dataclass
class AuthState:
    failed_attempts: int = 0
    lock_until: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def get_status(self) -> dict[str, int]:
        with self.lock:
            return {
                "failed_attempts": self.failed_attempts,
                "lock_until": self.lock_until,
            }

    def record_success(self) -> None:
        with self.lock:
            self.failed_attempts = 0
            self.lock_until = 0

    def record_failure(self) -> dict[str, int]:
        with self.lock:
            now_ts = int(time.time())
            if now_ts >= self.lock_until:
                self.lock_until = 0
            self.failed_attempts += 1
            if self.failed_attempts >= max(getattr(config, "LOGIN_MAX_ATTEMPTS", 5), 1):
                self.lock_until = now_ts + max(getattr(config, "LOGIN_LOCKOUT_SECONDS", 300), 1)
                self.failed_attempts = 0
            return {
                "failed_attempts": self.failed_attempts,
                "lock_until": self.lock_until,
            }


auth_state = AuthState()


def issue_session_cookie(response: Response, token: str) -> None:
    ttl = max(getattr(config, "ADMIN_SESSION_TTL_SECONDS", 28800), 60)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=ttl,
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")


def get_session_payload_from_request(request: Request) -> Optional[dict[str, Any]]:
    token = request.cookies.get(SESSION_COOKIE_NAME, "")
    return decode_admin_auth_token(token)


def build_session_key(payload: dict[str, Any]) -> str:
    fingerprint = str(payload.get("fp") or "anon")
    issued_at = int(payload.get("iat") or 0)
    return f"api-session-{fingerprint}-{issued_at}"


def require_session(request: Request) -> dict[str, Any]:
    payload = get_session_payload_from_request(request)
    if not payload:
        raise ApiError(401, "未登录或会话已失效", error_code="unauthorized")
    return payload

