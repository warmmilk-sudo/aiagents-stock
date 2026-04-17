from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import (
    SmartSelectionImportRequest,
    SmartSelectionRunRequest,
    SmartSelectionSchedulerRequest,
)


router = APIRouter(prefix="/api/smart-selection", tags=["smart-selection"])


@router.get("/overview")
def get_overview(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_smart_selection_overview())


@router.post("/runs")
def submit_run(request: Request, payload: SmartSelectionRunRequest | None = None) -> dict:
    require_session(request)
    try:
        run_id = services.submit_smart_selection_run(
            trigger_source=payload.trigger_source if payload else "manual",
            lightweight_model=payload.lightweight_model if payload else None,
            reasoning_model=payload.reasoning_model if payload else None,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="smart_selection_run_submit_failed") from exc
    return success_payload({"run_id": run_id}, message="智能选股任务已提交")


@router.get("/runs/latest")
def get_latest_run(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_smart_selection_run())


@router.get("/runs/{run_id}")
def get_run(request: Request, run_id: str) -> dict:
    require_session(request)
    run = services.get_smart_selection_run(run_id)
    if not run:
        raise ApiError(404, "未找到智能选股运行记录", error_code="smart_selection_run_not_found")
    return success_payload(run)


@router.post("/import")
def import_run_selection(request: Request, payload: SmartSelectionImportRequest) -> dict:
    require_session(request)
    try:
        result = services.import_smart_selection_run(
            run_id=payload.run_id,
            symbols=payload.symbols,
            replace_existing_focus=payload.replace_existing_focus,
        )
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="smart_selection_import_failed") from exc
    return success_payload(result, message="智能选股结果已导入关注备选")


@router.get("/watch-pool")
def list_watch_pool(request: Request, active_only: bool = True) -> dict:
    require_session(request)
    return success_payload(services.list_smart_selection_watch_pool(active_only=active_only))


@router.post("/watch-pool/cleanup")
def cleanup_watch_pool(request: Request) -> dict:
    require_session(request)
    return success_payload(services.cleanup_smart_selection_watch_pool(), message="观察池已清理")


@router.get("/scheduler")
def get_scheduler(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_smart_selection_scheduler_status())


@router.put("/scheduler")
def update_scheduler(request: Request, payload: SmartSelectionSchedulerRequest) -> dict:
    require_session(request)
    return success_payload(
        services.update_smart_selection_scheduler(
            enabled=payload.enabled,
            schedule_time=payload.schedule_time,
            max_workers=payload.max_workers,
        ),
        message="智能选股调度配置已更新",
    )


@router.post("/scheduler/run-once")
def run_scheduler_once(request: Request) -> dict:
    require_session(request)
    return success_payload(services.run_smart_selection_scheduler_once(), message="已提交一次智能选股任务")
