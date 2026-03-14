from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import LonghubangBatchTaskRequest, LonghubangTaskRequest


router = APIRouter(prefix="/api/strategies/longhubang", tags=["longhubang"])


@router.post("/tasks")
def submit_longhubang_task(request: Request, payload: LonghubangTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_longhubang_task(
            date_value=payload.date,
            days=payload.days,
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="longhubang_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="龙虎榜分析任务已提交")


@router.get("/tasks/latest")
def get_latest_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.LONGHUBANG_TASK_TYPE))


@router.get("/tasks/active")
def get_active_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.LONGHUBANG_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.LONGHUBANG_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到龙虎榜分析任务", error_code="longhubang_task_not_found")
    return success_payload(task)


@router.post("/batch-tasks")
def submit_batch_task(request: Request, payload: LonghubangBatchTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_longhubang_batch_task(
            symbols=payload.symbols,
            analysis_mode=payload.analysis_mode,
            max_workers=payload.max_workers,
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="longhubang_batch_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="龙虎榜 TOP 批量分析任务已提交")


@router.get("/batch-tasks/latest")
def get_latest_batch_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.LONGHUBANG_BATCH_TASK_TYPE))


@router.get("/batch-tasks/active")
def get_active_batch_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.LONGHUBANG_BATCH_TASK_TYPE))


@router.get("/batch-tasks/{task_id}")
def get_batch_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.LONGHUBANG_BATCH_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到龙虎榜批量分析任务", error_code="longhubang_batch_task_not_found")
    return success_payload(task)


@router.get("/history")
def list_history(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_longhubang_reports(limit=limit))


@router.get("/history/{report_id}")
def get_history_report(request: Request, report_id: int) -> dict:
    require_session(request)
    report = services.get_longhubang_report(report_id)
    if not report:
        raise ApiError(404, "未找到龙虎榜历史报告", error_code="longhubang_report_not_found")
    return success_payload(report)


@router.delete("/history/{report_id}")
def delete_history_report(request: Request, report_id: int) -> dict:
    require_session(request)
    if not services.delete_longhubang_report(report_id):
        raise ApiError(404, "未找到龙虎榜历史报告", error_code="longhubang_report_not_found")
    return success_payload({"report_id": report_id}, message="龙虎榜历史报告已删除")


@router.get("/statistics")
def get_statistics(request: Request, days: int = 30) -> dict:
    require_session(request)
    return success_payload(services.get_longhubang_statistics(window_days=days))
