from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import MacroCycleTaskRequest


router = APIRouter(prefix="/api/strategies/macro-cycle", tags=["macro-cycle"])


@router.post("/tasks")
def submit_macro_cycle_task(request: Request, payload: MacroCycleTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_macro_cycle_task(
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="macro_cycle_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="宏观周期分析任务已提交")


@router.get("/tasks/latest")
def get_latest_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.MACRO_CYCLE_TASK_TYPE))


@router.get("/tasks/active")
def get_active_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.MACRO_CYCLE_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.MACRO_CYCLE_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到宏观周期任务", error_code="macro_cycle_task_not_found")
    return success_payload(task)


@router.get("/history")
def list_history(request: Request, limit: int = 20) -> dict:
    require_session(request)
    return success_payload(services.list_macro_cycle_reports(limit=limit))


@router.get("/history/{report_id}")
def get_history_report(request: Request, report_id: int) -> dict:
    require_session(request)
    report = services.get_macro_cycle_report(report_id)
    if not report:
        raise ApiError(404, "未找到宏观周期历史报告", error_code="macro_cycle_report_not_found")
    return success_payload(report)


@router.delete("/history/{report_id}")
def delete_history_report(request: Request, report_id: int) -> dict:
    require_session(request)
    if not services.delete_macro_cycle_report(report_id):
        raise ApiError(404, "未找到宏观周期历史报告", error_code="macro_cycle_report_not_found")
    return success_payload({"report_id": report_id}, message="宏观周期历史报告已删除")
