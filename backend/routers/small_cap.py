from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import LowPriceBullAlertResolveRequest, LowPriceBullMonitorConfigRequest, LowPriceBullMonitorStockCreateRequest, SmallCapSelectionTaskRequest


router = APIRouter(prefix="/api/selectors/small-cap", tags=["small-cap"])


@router.post("/tasks")
def submit_selection_task(request: Request, payload: SmallCapSelectionTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_small_cap_selection_task(
            top_n=payload.top_n,
            max_market_cap_yi=payload.max_market_cap_yi,
            min_revenue_growth=payload.min_revenue_growth,
            min_profit_growth=payload.min_profit_growth,
            sort_by=payload.sort_by,
            exclude_st=payload.exclude_st,
            exclude_kcb=payload.exclude_kcb,
            exclude_cyb=payload.exclude_cyb,
            only_hs_a=payload.only_hs_a,
            filter_summary=payload.filter_summary,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="small_cap_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="小市值选股任务已提交")


@router.get("/tasks/latest")
def get_latest_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.SMALL_CAP_TASK_TYPE))


@router.get("/tasks/active")
def get_active_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.SMALL_CAP_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_selection_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.SMALL_CAP_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到小市值任务", error_code="small_cap_task_not_found")
    return success_payload(task)


@router.post("/notify")
def notify_selection_result(request: Request, payload: dict) -> dict:
    require_session(request)
    success, message = services.send_small_cap_notification(payload.get("stocks") or [], payload.get("filter_summary") or "")
    if not success:
        raise ApiError(400, message, error_code="small_cap_notify_failed")
    return success_payload({"ok": True}, message=message)


@router.get("/monitor/status")
def get_monitor_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_small_cap_monitor_status())


@router.put("/monitor/config")
def update_monitor_config(request: Request, payload: LowPriceBullMonitorConfigRequest) -> dict:
    require_session(request)
    return success_payload(services.update_small_cap_scan_interval(payload.scan_interval), message="监控配置已更新")


@router.post("/monitor/start")
def start_monitor(request: Request) -> dict:
    require_session(request)
    try:
        data = services.start_small_cap_monitor()
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="small_cap_monitor_start_failed") from exc
    return success_payload(data, message="监控服务已启动")


@router.post("/monitor/stop")
def stop_monitor(request: Request) -> dict:
    require_session(request)
    try:
        data = services.stop_small_cap_monitor()
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="small_cap_monitor_stop_failed") from exc
    return success_payload(data, message="监控服务已停止")


@router.get("/monitor/stocks")
def list_monitor_stocks(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_small_cap_monitored_stocks())


@router.post("/monitor/stocks")
def add_monitor_stock(request: Request, payload: LowPriceBullMonitorStockCreateRequest) -> dict:
    require_session(request)
    success, message = services.add_small_cap_monitored_stock(
        stock_code=payload.stock_code,
        stock_name=payload.stock_name,
        buy_price=payload.buy_price,
        buy_date=payload.buy_date,
    )
    if not success:
        raise ApiError(400, message, error_code="small_cap_monitor_add_failed")
    return success_payload({"stock_code": payload.stock_code}, message=message)


@router.delete("/monitor/stocks/{stock_code}")
def remove_monitor_stock(request: Request, stock_code: str) -> dict:
    require_session(request)
    success, message = services.remove_small_cap_monitored_stock(stock_code)
    if not success:
        raise ApiError(400, message, error_code="small_cap_monitor_remove_failed")
    return success_payload({"stock_code": stock_code}, message=message)


@router.get("/monitor/alerts")
def list_pending_alerts(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_small_cap_alerts(history=False))


@router.get("/monitor/alerts/history")
def list_alert_history(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_small_cap_alerts(history=True, limit=limit))


@router.post("/monitor/alerts/{alert_id}/resolve")
def resolve_alert(request: Request, alert_id: int, payload: LowPriceBullAlertResolveRequest) -> dict:
    require_session(request)
    success, message = services.resolve_small_cap_alert(alert_id, payload.status)
    if not success:
        raise ApiError(404, message, error_code="small_cap_alert_not_found")
    return success_payload({"alert_id": alert_id, "status": payload.status}, message=message)


@router.post("/monitor/alerts/cleanup")
def cleanup_alert_history(request: Request, days: int = 30) -> dict:
    require_session(request)
    return success_payload(services.cleanup_small_cap_alerts(days=days), message="旧提醒记录已清理")
