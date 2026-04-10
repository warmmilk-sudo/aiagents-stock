from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import build_session_key, require_session
from backend.dto import (
    PendingActionResolveRequest,
    SmartMonitorAccountRiskConfigRequest,
    SmartMonitorAnalyzeRequest,
    SmartMonitorConfigRequest,
    SmartMonitorRuntimeConfigRequest,
    SmartMonitorTaskRequest,
)
from backend import services


router = APIRouter(prefix="/api/smart-monitor", tags=["smart-monitor"])


@router.get("/tasks")
def list_tasks(
    request: Request,
    enabled_only: bool = False,
    account_name: Optional[str] = None,
    has_position: Optional[bool] = None,
) -> dict:
    require_session(request)
    return success_payload(
        services.list_smart_monitor_tasks(
            enabled_only=enabled_only,
            account_name=account_name,
            has_position=has_position,
        )
    )


@router.get("/runtime-config")
def get_runtime_config(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_smart_monitor_runtime_config())


@router.get("/config")
def get_config(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_smart_monitor_config())


@router.put("/config")
def update_config(request: Request, payload: SmartMonitorConfigRequest) -> dict:
    require_session(request)
    return success_payload(
        services.update_smart_monitor_config(payload.model_dump()),
        message="盯盘配置已更新",
    )


@router.get("/account-configs")
def list_account_configs(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_smart_monitor_account_configs())


@router.put("/account-configs")
def update_account_config(request: Request, payload: SmartMonitorAccountRiskConfigRequest) -> dict:
    require_session(request)
    return success_payload(
        services.update_smart_monitor_account_config(payload.model_dump()),
        message="盯盘配置已更新",
    )


@router.put("/runtime-config")
def update_runtime_config(request: Request, payload: SmartMonitorRuntimeConfigRequest) -> dict:
    require_session(request)
    return success_payload(
        services.update_smart_monitor_runtime_config(payload.model_dump()),
        message="智能盯盘运行配置已更新",
    )


@router.post("/tasks")
def create_task(request: Request, payload: SmartMonitorTaskRequest) -> dict:
    require_session(request)
    task_id = services.upsert_smart_monitor_task(payload.model_dump(exclude_none=True))
    return success_payload({"task_id": task_id}, message="智能盯盘任务已保存")


@router.patch("/tasks/{task_id}")
def update_task(request: Request, task_id: int, payload: SmartMonitorTaskRequest) -> dict:
    require_session(request)
    if not services.update_smart_monitor_task(task_id, payload.model_dump(exclude_none=True)):
        raise ApiError(400, "更新智能盯盘任务失败", error_code="smart_monitor_update_failed")
    return success_payload({"task_id": task_id}, message="智能盯盘任务已更新")


@router.delete("/tasks/{task_id}")
def delete_task(request: Request, task_id: int) -> dict:
    require_session(request)
    if not services.delete_smart_monitor_task(task_id):
        raise ApiError(404, "未找到智能盯盘任务", error_code="smart_monitor_task_not_found")
    return success_payload({"task_id": task_id}, message="智能盯盘任务已删除")


@router.post("/tasks/{task_id}/enable")
def toggle_task(request: Request, task_id: int, enabled: bool) -> dict:
    require_session(request)
    if not services.set_smart_monitor_task_enabled(task_id, enabled):
        raise ApiError(400, "更新任务启用状态失败", error_code="smart_monitor_enable_failed")
    return success_payload({"task_id": task_id, "enabled": enabled}, message="任务状态已更新")


@router.post("/tasks/enable-all")
def toggle_all_tasks(request: Request, enabled: bool) -> dict:
    require_session(request)
    changed = services.set_all_smart_monitor_tasks_enabled(enabled)
    return success_payload({"changed": changed, "enabled": enabled}, message="批量任务状态已更新")


@router.post("/tasks/sync-baselines")
def sync_task_baselines(
    request: Request,
    enabled_only: bool = False,
    account_name: Optional[str] = None,
    has_position: Optional[bool] = None,
) -> dict:
    require_session(request)
    return success_payload(
        services.sync_smart_monitor_analysis_baselines(
            enabled_only=enabled_only,
            account_name=account_name,
            has_position=has_position,
        ),
        message="已强制同步智能盯盘分析基线",
    )


@router.post("/tasks/refresh-baselines")
def refresh_task_baselines(
    request: Request,
    enabled_only: bool = False,
    account_name: Optional[str] = None,
    has_position: Optional[bool] = None,
) -> dict:
    session = require_session(request)
    try:
        task_id = services.submit_smart_monitor_baseline_refresh_task(
            session_key=build_session_key(session),
            enabled_only=enabled_only,
            account_name=account_name,
            has_position=has_position,
        )
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="smart_monitor_refresh_baselines_failed") from exc
    return success_payload({"task_id": task_id}, message="已提交盯盘基线更新任务")


@router.post("/tasks/run-once")
def run_all_tasks_once(
    request: Request,
    enabled_only: bool = True,
    account_name: Optional[str] = None,
    has_position: Optional[bool] = None,
) -> dict:
    require_session(request)
    return success_payload(
        services.run_smart_monitor_tasks_once(
            enabled_only=enabled_only,
            account_name=account_name,
            has_position=has_position,
        ),
        message="已触发一次智能盯盘批量执行",
    )


@router.post("/tasks/{task_id}/run-once")
def run_task_once(request: Request, task_id: int) -> dict:
    require_session(request)
    try:
        result = services.run_smart_monitor_task_once(task_id)
    except ValueError as exc:
        raise ApiError(404, str(exc), error_code="smart_monitor_task_not_found") from exc
    if not result.get("success"):
        raise ApiError(400, "单任务盘中决策失败", error_code="smart_monitor_run_once_failed", details=result)
    return success_payload(result, message="已触发一次智能盯盘任务执行")


@router.post("/analyze")
def run_manual_analysis(request: Request, payload: SmartMonitorAnalyzeRequest) -> dict:
    require_session(request)
    return success_payload(
        services.run_manual_smart_monitor_analysis(payload.model_dump(exclude_none=True)),
        message="盯盘分析已执行",
    )


@router.get("/decisions")
def list_decisions(request: Request, limit: int = 100) -> dict:
    require_session(request)
    return success_payload(services.list_smart_monitor_decisions(limit=limit))


@router.get("/decisions/summary")
def get_decision_summary(request: Request, limit: int = 120) -> dict:
    require_session(request)
    return success_payload(services.get_smart_monitor_decision_summary(limit=limit))


@router.get("/trades")
def list_trade_records(request: Request, limit: int = 100) -> dict:
    require_session(request)
    return success_payload(services.list_smart_monitor_trade_records(limit=limit))


@router.get("/pending-actions")
def list_pending(
    request: Request,
    status: Optional[str] = "pending",
    account_name: Optional[str] = None,
    asset_id: Optional[int] = None,
    limit: int = 100,
) -> dict:
    require_session(request)
    return success_payload(
        services.list_pending_actions(
            status=status,
            account_name=account_name,
            asset_id=asset_id,
            limit=limit,
        )
    )


@router.post("/pending-actions/{action_id}/resolve")
def resolve_pending(request: Request, action_id: int, payload: PendingActionResolveRequest) -> dict:
    require_session(request)
    if not services.resolve_pending_action(action_id, payload.status, payload.resolution_note):
        raise ApiError(400, "处理待办动作失败", error_code="pending_action_resolve_failed")
    return success_payload({"action_id": action_id}, message="待办动作已处理")
