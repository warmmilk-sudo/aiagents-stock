from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import (
    LowPriceBullAlertResolveRequest,
    LowPriceBullMonitorConfigRequest,
    LowPriceBullMonitorStockCreateRequest,
    LowPriceBullSelectionTaskRequest,
    LowPriceBullSimulationRequest,
)


router = APIRouter(prefix="/api/selectors/low-price-bull", tags=["low-price-bull"])


@router.post("/tasks")
def submit_selection_task(request: Request, payload: LowPriceBullSelectionTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_low_price_bull_selection_task(
            top_n=payload.top_n,
            max_price=payload.max_price,
            min_profit_growth=payload.min_profit_growth,
            min_turnover_yi=payload.min_turnover_yi,
            max_turnover_yi=payload.max_turnover_yi,
            min_market_cap_yi=payload.min_market_cap_yi,
            max_market_cap_yi=payload.max_market_cap_yi,
            sort_by=payload.sort_by,
            exclude_st=payload.exclude_st,
            exclude_kcb=payload.exclude_kcb,
            exclude_cyb=payload.exclude_cyb,
            only_hs_a=payload.only_hs_a,
            filter_summary=payload.filter_summary,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="low_price_bull_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="低价擒牛选股任务已提交")


@router.get("/tasks/latest")
def get_latest_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.LOW_PRICE_BULL_TASK_TYPE))


@router.get("/tasks/active")
def get_active_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.LOW_PRICE_BULL_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_selection_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.LOW_PRICE_BULL_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到低价擒牛任务", error_code="low_price_bull_task_not_found")
    return success_payload(task)


@router.post("/simulation")
def run_simulation(request: Request, payload: LowPriceBullSimulationRequest) -> dict:
    require_session(request)
    return success_payload(services.simulate_low_price_bull_strategy(payload.stocks))


@router.get("/monitor/status")
def get_monitor_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_low_price_bull_status())


@router.put("/monitor/config")
def update_monitor_config(request: Request, payload: LowPriceBullMonitorConfigRequest) -> dict:
    require_session(request)
    return success_payload(
        services.update_low_price_bull_scan_interval(payload.scan_interval),
        message="监控配置已更新",
    )


@router.post("/monitor/start")
def start_monitor(request: Request) -> dict:
    require_session(request)
    try:
        data = services.start_low_price_bull_monitor()
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="low_price_bull_start_failed") from exc
    return success_payload(data, message="监控服务已启动")


@router.post("/monitor/stop")
def stop_monitor(request: Request) -> dict:
    require_session(request)
    try:
        data = services.stop_low_price_bull_monitor()
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="low_price_bull_stop_failed") from exc
    return success_payload(data, message="监控服务已停止")


@router.get("/monitor/stocks")
def list_monitor_stocks(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_low_price_bull_monitored_stocks())


@router.post("/monitor/stocks")
def add_monitor_stock(request: Request, payload: LowPriceBullMonitorStockCreateRequest) -> dict:
    require_session(request)
    success, message = services.add_low_price_bull_monitored_stock(
        stock_code=payload.stock_code,
        stock_name=payload.stock_name,
        buy_price=payload.buy_price,
        buy_date=payload.buy_date,
    )
    if not success:
        raise ApiError(400, message, error_code="low_price_bull_monitor_add_failed")
    return success_payload({"stock_code": payload.stock_code}, message=message)


@router.delete("/monitor/stocks/{stock_code}")
def remove_monitor_stock(request: Request, stock_code: str) -> dict:
    require_session(request)
    success, message = services.remove_low_price_bull_monitored_stock(stock_code)
    if not success:
        raise ApiError(400, message, error_code="low_price_bull_monitor_remove_failed")
    return success_payload({"stock_code": stock_code}, message=message)


@router.get("/monitor/alerts")
def list_pending_alerts(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_low_price_bull_alerts(history=False))


@router.get("/monitor/alerts/history")
def list_alert_history(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_low_price_bull_alerts(history=True, limit=limit))


@router.post("/monitor/alerts/{alert_id}/resolve")
def resolve_alert(request: Request, alert_id: int, payload: LowPriceBullAlertResolveRequest) -> dict:
    require_session(request)
    success, message = services.resolve_low_price_bull_alert(alert_id, payload.status)
    if not success:
        raise ApiError(404, message, error_code="low_price_bull_alert_not_found")
    return success_payload({"alert_id": alert_id, "status": payload.status}, message=message)


@router.post("/monitor/alerts/cleanup")
def cleanup_alert_history(request: Request, days: int = 30) -> dict:
    require_session(request)
    return success_payload(
        services.cleanup_low_price_bull_alerts(days=days),
        message="旧提醒记录已清理",
    )
