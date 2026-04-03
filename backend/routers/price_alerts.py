from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import PriceAlertCreateRequest, PriceAlertUpdateRequest
from backend import services


router = APIRouter(prefix="/api/price-alerts", tags=["price-alerts"])


@router.get("")
def list_price_alerts(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_price_alerts())


@router.post("")
def create_price_alert(request: Request, payload: PriceAlertCreateRequest) -> dict:
    require_session(request)
    alert_id = services.create_price_alert(payload.model_dump())
    return success_payload({"alert_id": alert_id}, message="价格预警已创建")


@router.patch("/{alert_id}")
def update_price_alert(request: Request, alert_id: int, payload: PriceAlertUpdateRequest) -> dict:
    require_session(request)
    if not services.update_price_alert(alert_id, payload.model_dump(exclude_none=True)):
        raise ApiError(400, "更新价格预警失败", error_code="price_alert_update_failed")
    return success_payload({"alert_id": alert_id}, message="价格预警已更新")


@router.delete("/{alert_id}")
def delete_price_alert(request: Request, alert_id: int) -> dict:
    require_session(request)
    if not services.delete_price_alert(alert_id):
        raise ApiError(404, "未找到价格预警", error_code="price_alert_not_found")
    return success_payload({"alert_id": alert_id}, message="价格预警已删除")


@router.post("/{alert_id}/notification")
def toggle_notification(request: Request, alert_id: int, enabled: bool) -> dict:
    require_session(request)
    if not services.toggle_price_alert_notification(alert_id, enabled):
        raise ApiError(400, "更新通知状态失败", error_code="price_alert_toggle_failed")
    return success_payload({"alert_id": alert_id, "enabled": enabled}, message="通知状态已更新")


@router.get("/notifications")
def list_notifications(request: Request, limit: int = 30, task_scope: str | None = None) -> dict:
    require_session(request)
    return success_payload(services.list_price_alert_notifications(limit=limit, task_scope=task_scope))


@router.post("/notifications/{event_id}/read")
def mark_notification_read(request: Request, event_id: int) -> dict:
    require_session(request)
    services.mark_monitor_notification_read(event_id)
    return success_payload({"event_id": event_id}, message="通知已标记为已读")


@router.post("/notifications/{event_id}/ignore")
def ignore_notification(request: Request, event_id: int) -> dict:
    require_session(request)
    services.ignore_monitor_notification(event_id)
    return success_payload({"event_id": event_id}, message="通知已忽略")
