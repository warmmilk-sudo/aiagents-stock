from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import MainForceBatchTaskRequest, MainForceSelectionTaskRequest


router = APIRouter(prefix="/api/selectors/main-force", tags=["main-force"])


@router.post("/tasks")
def submit_selection_task(request: Request, payload: MainForceSelectionTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_main_force_selection_task(
            days_ago=payload.days_ago,
            start_date=payload.start_date,
            final_n=payload.final_n,
            max_change=payload.max_change,
            min_cap=payload.min_cap,
            max_cap=payload.max_cap,
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="main_force_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="主力选股任务已提交")


@router.get("/tasks/latest")
def get_latest_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.MAIN_FORCE_SELECTION_TASK_TYPE))


@router.get("/tasks/active")
def get_active_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.MAIN_FORCE_SELECTION_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_selection_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.MAIN_FORCE_SELECTION_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到主力选股任务", error_code="main_force_task_not_found")
    return success_payload(task)


@router.post("/batch-tasks")
def submit_batch_task(request: Request, payload: MainForceBatchTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_main_force_batch_task(
            symbols=payload.symbols,
            analysis_mode=payload.analysis_mode,
            max_workers=payload.max_workers,
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="main_force_batch_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="主力 TOP 批量分析任务已提交")


@router.get("/batch-tasks/latest")
def get_latest_batch_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.MAIN_FORCE_BATCH_TASK_TYPE))


@router.get("/batch-tasks/active")
def get_active_batch_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.MAIN_FORCE_BATCH_TASK_TYPE))


@router.get("/batch-tasks/{task_id}")
def get_batch_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.MAIN_FORCE_BATCH_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到主力批量分析任务", error_code="main_force_batch_task_not_found")
    return success_payload(task)


@router.get("/history")
def list_history(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_main_force_history(limit=limit))


@router.get("/history/{record_id}")
def get_history_record(request: Request, record_id: int) -> dict:
    require_session(request)
    record = services.get_main_force_history_record(record_id)
    if not record:
        raise ApiError(404, "未找到主力批量分析历史", error_code="main_force_history_not_found")
    return success_payload(record)


@router.delete("/history/{record_id}")
def delete_history_record(request: Request, record_id: int) -> dict:
    require_session(request)
    if not services.delete_main_force_history_record(record_id):
        raise ApiError(404, "未找到主力批量分析历史", error_code="main_force_history_not_found")
    return success_payload({"record_id": record_id}, message="主力批量分析历史已删除")
