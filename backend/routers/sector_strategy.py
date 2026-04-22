from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi_cache.decorator import cache

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import (
    SectorStrategyLifecycleConfigRequest,
    SectorStrategySchedulerRequest,
    SectorStrategyTaskRequest,
)


router = APIRouter(prefix="/api/strategies/sector-strategy", tags=["sector-strategy"])


@router.post("/tasks")
def submit_strategy_task(request: Request, payload: SectorStrategyTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_sector_strategy_task(
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="sector_strategy_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="智策分析任务已提交")


@router.get("/tasks/latest")
def get_latest_strategy_task(
    request: Request,
    full: bool = False,
    include_raw_reports: bool = False,
) -> dict:
    require_session(request)
    return success_payload(
        services.get_latest_sector_strategy_task(
            full=full,
            include_raw_reports=include_raw_reports,
        )
    )


@router.get("/tasks/active")
def get_active_strategy_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.SECTOR_STRATEGY_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_strategy_task(
    request: Request,
    task_id: str,
    full: bool = False,
    include_raw_reports: bool = False,
) -> dict:
    require_session(request)
    task = services.get_sector_strategy_task(
        task_id,
        full=full,
        include_raw_reports=include_raw_reports,
    )
    if not task:
        raise ApiError(404, "未找到智策分析任务", error_code="sector_strategy_task_not_found")
    return success_payload(task)


@router.get("/lifecycle/rebuild/tasks/latest")
def get_latest_rebuild_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.SECTOR_STRATEGY_LIFECYCLE_REBUILD_TASK_TYPE))


@router.get("/lifecycle/rebuild/tasks/active")
def get_active_rebuild_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.SECTOR_STRATEGY_LIFECYCLE_REBUILD_TASK_TYPE))


@router.get("/lifecycle/rebuild/tasks/{task_id}")
def get_rebuild_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.SECTOR_STRATEGY_LIFECYCLE_REBUILD_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到生命周期重建任务", error_code="sector_strategy_lifecycle_rebuild_task_not_found")
    return success_payload(task)


@router.get("/history")
@cache(expire=15, namespace="sector-strategy")
def list_history(
    request: Request,
    _session: dict = Depends(require_session),
    limit: int = 20,
) -> dict:
    return success_payload(services.list_sector_strategy_reports(limit=limit))


@router.get("/latest-report")
@cache(expire=15, namespace="sector-strategy")
def get_latest_report(
    request: Request,
    _session: dict = Depends(require_session),
) -> dict:
    return success_payload(services.get_latest_sector_strategy_report_overview())


@router.get("/history/{report_id}")
@cache(expire=15, namespace="sector-strategy")
def get_history_report(
    request: Request,
    report_id: int,
    _session: dict = Depends(require_session),
    include_raw_reports: bool = False,
) -> dict:
    report = services.get_sector_strategy_report(report_id, include_raw_reports=include_raw_reports)
    if not report:
        raise ApiError(404, "未找到智策历史报告", error_code="sector_strategy_report_not_found")
    return success_payload(report)


@router.get("/lifecycle/latest")
@cache(expire=15, namespace="sector-strategy")
def get_latest_lifecycle(
    request: Request,
    _session: dict = Depends(require_session),
) -> dict:
    return success_payload(services.get_sector_strategy_latest_lifecycle())


@router.get("/lifecycle")
def list_lifecycle(request: Request, days: int = 20) -> dict:
    require_session(request)
    return success_payload(services.list_sector_strategy_lifecycle(days=days))


@router.get("/lifecycle-config")
def get_lifecycle_config(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_sector_strategy_lifecycle_config())


@router.put("/lifecycle-config")
def update_lifecycle_config(request: Request, payload: SectorStrategyLifecycleConfigRequest) -> dict:
    require_session(request)
    raise ApiError(403, "生命周期阈值已固定在代码配置中，不支持在线修改", error_code="sector_strategy_lifecycle_config_read_only")


@router.post("/lifecycle/rebuild")
def rebuild_lifecycle(request: Request) -> dict:
    require_session(request)
    task = services.submit_sector_strategy_lifecycle_rebuild_task(reason="manual")
    return success_payload(
        {"task_id": task.get("task_id"), "reused": bool(task.get("reused"))},
        message="已存在进行中的生命周期重建任务，已复用" if task.get("reused") else "生命周期重建任务已提交",
    )


@router.get("/heat-daily/latest")
def get_latest_daily_heat(request: Request, limit: int = 30) -> dict:
    require_session(request)
    return success_payload(services.get_sector_strategy_latest_heat_daily(limit=limit))


@router.delete("/history/{report_id}")
def delete_history_report(request: Request, report_id: int) -> dict:
    require_session(request)
    if not services.delete_sector_strategy_report(report_id):
        raise ApiError(404, "未找到智策历史报告", error_code="sector_strategy_report_not_found")
    return success_payload({"report_id": report_id}, message="智策历史报告已删除")


@router.get("/scheduler")
def get_scheduler_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_sector_strategy_scheduler_status())


@router.put("/scheduler")
def update_scheduler(request: Request, payload: SectorStrategySchedulerRequest) -> dict:
    require_session(request)
    try:
        data = services.update_sector_strategy_scheduler(
            schedule_time=payload.schedule_time,
            enabled=payload.enabled,
        )
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="sector_strategy_scheduler_update_failed") from exc
    return success_payload(data, message="智策定时任务配置已更新")


@router.post("/scheduler/run-once")
def run_scheduler_once(request: Request) -> dict:
    require_session(request)
    try:
        data = services.run_sector_strategy_scheduler_once()
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="sector_strategy_scheduler_run_failed") from exc
    return success_payload(data, message="已提交一次智策后台分析")


@router.post("/scheduler/test-email")
def test_email(request: Request) -> dict:
    require_session(request)
    success, message = services.test_email_notification()
    if not success:
        raise ApiError(400, message, error_code="sector_strategy_test_email_failed")
    return success_payload({"ok": True}, message=message)
