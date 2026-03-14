from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import build_session_key, require_session
from backend import services


router = APIRouter(prefix="/api/tasks", tags=["tasks"])


@router.get("/latest")
def get_latest_task(request: Request) -> dict:
    payload = require_session(request)
    return success_payload(services.get_latest_task_for_session(build_session_key(payload)))


@router.get("/active")
def get_active_task(request: Request) -> dict:
    payload = require_session(request)
    return success_payload(services.get_active_task_for_session(build_session_key(payload)))


@router.get("/{task_id}")
def get_task(request: Request, task_id: str) -> dict:
    payload = require_session(request)
    task = services.get_task_for_session(build_session_key(payload), task_id)
    if not task:
        raise ApiError(404, "未找到任务", error_code="task_not_found")
    return success_payload(task)
