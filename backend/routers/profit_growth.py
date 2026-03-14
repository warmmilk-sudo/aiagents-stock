from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import ProfitGrowthMonitorStockCreateRequest, ProfitGrowthSelectionTaskRequest


router = APIRouter(prefix="/api/selectors/profit-growth", tags=["profit-growth"])


@router.post("/tasks")
def submit_selection_task(request: Request, payload: ProfitGrowthSelectionTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_profit_growth_selection_task(
            top_n=payload.top_n,
            min_profit_growth=payload.min_profit_growth,
            min_turnover_yi=payload.min_turnover_yi,
            max_turnover_yi=payload.max_turnover_yi,
            sort_by=payload.sort_by,
            exclude_st=payload.exclude_st,
            exclude_kcb=payload.exclude_kcb,
            exclude_cyb=payload.exclude_cyb,
            filter_summary=payload.filter_summary,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="profit_growth_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="净利增长选股任务已提交")


@router.get("/tasks/latest")
def get_latest_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.PROFIT_GROWTH_TASK_TYPE))


@router.get("/tasks/active")
def get_active_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.PROFIT_GROWTH_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_selection_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.PROFIT_GROWTH_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到净利增长任务", error_code="profit_growth_task_not_found")
    return success_payload(task)


@router.post("/notify")
def notify_selection_result(request: Request, payload: dict) -> dict:
    require_session(request)
    success, message = services.send_profit_growth_notification(payload.get("stocks") or [], payload.get("filter_summary") or "")
    if not success:
        raise ApiError(400, message, error_code="profit_growth_notify_failed")
    return success_payload({"ok": True}, message=message)


@router.get("/monitor/status")
def get_monitor_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_profit_growth_monitor_status())


@router.get("/monitor/stocks")
def list_monitor_stocks(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_profit_growth_monitored_stocks())


@router.post("/monitor/stocks")
def add_monitor_stock(request: Request, payload: ProfitGrowthMonitorStockCreateRequest) -> dict:
    require_session(request)
    success, message = services.add_profit_growth_monitored_stock(
        stock_code=payload.stock_code,
        stock_name=payload.stock_name,
        buy_price=payload.buy_price,
        buy_date=payload.buy_date,
    )
    if not success:
        raise ApiError(400, message, error_code="profit_growth_monitor_add_failed")
    return success_payload({"stock_code": payload.stock_code}, message=message)


@router.delete("/monitor/stocks/{stock_code}")
def remove_monitor_stock(request: Request, stock_code: str, reason: str = "手动移除") -> dict:
    require_session(request)
    success, message = services.remove_profit_growth_monitored_stock(stock_code, reason)
    if not success:
        raise ApiError(400, message, error_code="profit_growth_monitor_remove_failed")
    return success_payload({"stock_code": stock_code}, message=message)


@router.get("/monitor/alerts")
def list_pending_alerts(request: Request) -> dict:
    require_session(request)
    return success_payload(services.list_profit_growth_alerts(history=False))


@router.get("/monitor/alerts/history")
def list_alert_history(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_profit_growth_alerts(history=True, limit=limit))


@router.get("/monitor/removed")
def list_removed_stocks(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_profit_growth_removed_stocks(limit=limit))
