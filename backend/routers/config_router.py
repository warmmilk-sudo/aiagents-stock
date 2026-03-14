from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import ConfigUpdateRequest
from backend import services


router = APIRouter(prefix="/api/config", tags=["config"])


@router.get("")
def get_config(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_config_payload())


@router.put("")
def update_config(request: Request, payload: ConfigUpdateRequest) -> dict:
    require_session(request)
    success, message = services.save_config_values(payload.values)
    if not success:
        raise ApiError(400, message, error_code="invalid_config")
    return success_payload(services.get_config_payload(), message=message)


@router.post("/test-webhook")
def test_webhook(request: Request) -> dict:
    require_session(request)
    success, message = services.test_webhook()
    if not success:
        raise ApiError(400, message, error_code="webhook_test_failed")
    return success_payload({"ok": True}, message=message)
