from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

import config
from backend.api import ApiError, success_payload
from backend.auth import build_session_key, require_session
from backend.dto import (
    PortfolioAnalysisTaskRequest,
    PortfolioSchedulerConfigRequest,
    PortfolioStockCreateRequest,
    PortfolioStockUpdateRequest,
    TradeRecordCreateRequest,
)
from backend import services


router = APIRouter(prefix="/api/portfolio", tags=["portfolio"])


@router.get("/stocks")
def list_stocks(request: Request, account_name: Optional[str] = None) -> dict:
    require_session(request)
    return success_payload(services.list_portfolio_stocks(account_name=account_name))


@router.post("/stocks")
def create_stock(request: Request, payload: PortfolioStockCreateRequest) -> dict:
    require_session(request)
    success, message, stock_id, warnings = services.create_portfolio_stock(payload.model_dump())
    if not success:
        raise ApiError(400, message, error_code="portfolio_create_failed")
    return success_payload(
        {
            "stock_id": stock_id,
            "warnings": warnings,
        },
        message=message,
    )


@router.patch("/stocks/{stock_id}")
def update_stock(request: Request, stock_id: int, payload: PortfolioStockUpdateRequest) -> dict:
    require_session(request)
    success, message = services.update_portfolio_stock(stock_id, payload.model_dump(exclude_none=True))
    if not success:
        raise ApiError(400, message, error_code="portfolio_update_failed")
    return success_payload({"stock_id": stock_id}, message=message)


@router.delete("/stocks/{stock_id}")
def delete_stock(request: Request, stock_id: int) -> dict:
    require_session(request)
    success, message = services.delete_portfolio_stock(stock_id)
    if not success:
        raise ApiError(400, message, error_code="portfolio_delete_failed")
    return success_payload({"stock_id": stock_id}, message=message)


@router.post("/stocks/{stock_id}/trades")
def record_trade(request: Request, stock_id: int, payload: TradeRecordCreateRequest) -> dict:
    require_session(request)
    success, message, updated_stock = services.record_portfolio_trade(stock_id, payload.model_dump())
    if not success:
        raise ApiError(400, message, error_code="portfolio_trade_failed")
    return success_payload(
        {
            "stock_id": stock_id,
            "updated_stock": updated_stock,
        },
        message=message,
    )


@router.get("/trades")
def list_trades(
    request: Request,
    account_name: Optional[str] = None,
    limit: Optional[int] = 120,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
) -> dict:
    require_session(request)
    return success_payload(
        services.list_portfolio_trade_records(
            account_name=account_name,
            limit=limit,
            page=page,
            page_size=page_size,
        )
    )


@router.get("/risk")
def get_risk(request: Request, account_name: Optional[str] = None) -> dict:
    require_session(request)
    return success_payload(services.get_portfolio_risk(account_name=account_name))


@router.get("/stocks/{stock_id}/history")
def list_stock_history(request: Request, stock_id: int, limit: int = 10) -> dict:
    require_session(request)
    return success_payload(services.list_portfolio_analysis_history(stock_id, limit=limit))


@router.post("/analysis/tasks")
def submit_portfolio_analysis_task(request: Request, payload: PortfolioAnalysisTaskRequest) -> dict:
    session = require_session(request)
    try:
        task_id = services.submit_portfolio_analysis_task(
            session_key=build_session_key(session),
            account_name=payload.account_name,
            period=payload.period or getattr(config, "DATA_PERIOD", "1y"),
            batch_mode=payload.batch_mode,
            max_workers=payload.max_workers,
            analysts=services.build_analyst_config(payload.analysts),
        )
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="portfolio_analysis_invalid_request") from exc
    return success_payload({"task_id": task_id}, message="持仓分析任务已提交")


@router.get("/scheduler")
def get_scheduler_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_portfolio_scheduler_status())


@router.put("/scheduler")
def update_scheduler(request: Request, payload: PortfolioSchedulerConfigRequest) -> dict:
    require_session(request)
    return success_payload(
        services.update_portfolio_scheduler(payload.model_dump(exclude_none=True)),
        message="定时分析配置已更新",
    )


@router.post("/scheduler/start")
def start_scheduler(request: Request) -> dict:
    require_session(request)
    return success_payload(services.start_portfolio_scheduler(), message="定时分析已启动")


@router.post("/scheduler/stop")
def stop_scheduler(request: Request) -> dict:
    require_session(request)
    return success_payload(services.stop_portfolio_scheduler(), message="定时分析已停止")


@router.get("/scheduler/tasks/active")
def get_scheduler_active_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_portfolio_scheduler_active_task())


@router.get("/scheduler/tasks/latest")
def get_scheduler_latest_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_portfolio_scheduler_latest_task())


@router.get("/scheduler/tasks/{task_id}")
def get_scheduler_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_portfolio_scheduler_task(task_id)
    if not task:
        raise ApiError(404, "未找到持仓分析任务", error_code="portfolio_scheduler_task_not_found")
    return success_payload(task)


@router.post("/scheduler/run-once")
def run_scheduler_once(request: Request) -> dict:
    require_session(request)
    result = services.run_portfolio_scheduler_once()
    if not result["success"]:
        raise ApiError(
            400,
            str(result.get("message") or "立即执行失败"),
            error_code="portfolio_scheduler_run_failed",
            details=result,
        )
    return success_payload(result, message="已触发一次持仓分析")
