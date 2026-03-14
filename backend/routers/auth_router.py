from __future__ import annotations

import time

from fastapi import APIRouter, Request, Response

from backend.api import ApiError, success_payload
from backend.auth import (
    auth_state,
    build_session_key,
    clear_session_cookie,
    decode_admin_auth_token,
    encode_admin_auth_token,
    get_session_payload_from_request,
    issue_session_cookie,
    verify_admin_password,
)
from backend.dto import LoginRequest


router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.get("/session")
def get_session(request: Request) -> dict:
    payload = get_session_payload_from_request(request)
    if not payload:
        return success_payload(
            {
                "authenticated": False,
                "session": None,
                "lock": auth_state.get_status(),
            }
        )
    return success_payload(
        {
            "authenticated": True,
            "session": {
                "issued_at": payload["iat"],
                "expires_at": payload["exp"],
                "fingerprint": payload["fp"],
                "session_key": build_session_key(payload),
            },
            "lock": auth_state.get_status(),
        }
    )


@router.post("/login")
def login(payload: LoginRequest, response: Response) -> dict:
    now_ts = int(time.time())
    lock_status = auth_state.get_status()
    if now_ts < int(lock_status.get("lock_until", 0)):
        raise ApiError(
            429,
            f"尝试次数过多，请 {int(lock_status['lock_until']) - now_ts} 秒后重试",
            error_code="login_locked",
            details=lock_status,
        )

    if not verify_admin_password(payload.password):
        updated = auth_state.record_failure()
        raise ApiError(
            401,
            "密码错误，请重试",
            error_code="invalid_credentials",
            details=updated,
        )

    auth_state.record_success()
    issued_at = int(time.time())
    token = encode_admin_auth_token(issued_at)
    session_payload = decode_admin_auth_token(token) or {"iat": issued_at, "exp": issued_at, "fp": ""}
    issue_session_cookie(response, token)
    return success_payload(
        {
            "authenticated": True,
            "issued_at": issued_at,
            "expires_at": session_payload["exp"],
            "fingerprint": session_payload["fp"],
            "session_key": build_session_key(session_payload),
        },
        message="登录成功",
    )


@router.post("/logout")
def logout(response: Response) -> dict:
    clear_session_cookie(response)
    return success_payload({"authenticated": False}, message="已退出登录")
