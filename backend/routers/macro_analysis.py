from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import MacroAnalysisTaskRequest


router = APIRouter(prefix="/api/strategies/macro-analysis", tags=["macro-analysis"])


@router.post("/tasks")
def submit_macro_analysis_task(request: Request, payload: MacroAnalysisTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_macro_analysis_task(
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="macro_analysis_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="宏观分析任务已提交")


@router.get("/tasks/latest")
def get_latest_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.MACRO_ANALYSIS_TASK_TYPE))


@router.get("/tasks/active")
def get_active_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.MACRO_ANALYSIS_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.MACRO_ANALYSIS_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到宏观分析任务", error_code="macro_analysis_task_not_found")
    return success_payload(task)
