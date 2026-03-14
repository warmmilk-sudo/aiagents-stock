from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import (
    PendingActionResolveRequest,
    SmartMonitorAnalyzeRequest,
    SmartMonitorTaskRequest,
)
from backend import services


router = APIRouter(prefix="/api/smart-monitor", tags=["smart-monitor"])


@router.get("/tasks")
def list_tasks(request: Request, enabled_only: bool = False) -> dict:
    require_session(request)
    return success_payload(services.list_smart_monitor_tasks(enabled_only=enabled_only))


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
