from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import ValueStockSelectionTaskRequest, ValueStockSimulationRequest


router = APIRouter(prefix="/api/selectors/value-stock", tags=["value-stock"])


@router.post("/tasks")
def submit_selection_task(request: Request, payload: ValueStockSelectionTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_value_stock_selection_task(
            top_n=payload.top_n,
            max_pe=payload.max_pe,
            max_pb=payload.max_pb,
            min_dividend_yield=payload.min_dividend_yield,
            max_debt_ratio=payload.max_debt_ratio,
            min_float_cap_yi=payload.min_float_cap_yi,
            max_float_cap_yi=payload.max_float_cap_yi,
            sort_by=payload.sort_by,
            exclude_st=payload.exclude_st,
            exclude_kcb=payload.exclude_kcb,
            exclude_cyb=payload.exclude_cyb,
            filter_summary=payload.filter_summary,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="value_stock_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="低估值选股任务已提交")


@router.get("/tasks/latest")
def get_latest_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.VALUE_STOCK_TASK_TYPE))


@router.get("/tasks/active")
def get_active_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.VALUE_STOCK_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_selection_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.VALUE_STOCK_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到低估值任务", error_code="value_stock_task_not_found")
    return success_payload(task)


@router.post("/simulation")
def run_simulation(request: Request, payload: ValueStockSimulationRequest) -> dict:
    require_session(request)
    return success_payload(services.simulate_value_stock_strategy(payload.stocks))
