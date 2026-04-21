from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi_cache.decorator import cache

from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend import services


router = APIRouter(prefix="/api/analysis-history", tags=["analysis-history"])


@router.get("/stocks")
@cache(expire=60, namespace="analysis-history")
def list_history_grouped(
    request: Request,
    _session: dict = Depends(require_session),
    portfolio_state: str = "全部",
    account_name: Optional[str] = None,
    search_term: str = "",
) -> dict:
    """Return analysis history grouped by stock (one entry per symbol)."""
    return success_payload(
        services.list_analysis_history_grouped(
            portfolio_state=portfolio_state,
            account_name=account_name,
            search_term=search_term,
        )
    )


@router.get("/stocks/{symbol}")
@cache(expire=60, namespace="analysis-history")
def list_history_by_symbol(
    request: Request,
    symbol: str,
    _session: dict = Depends(require_session),
    portfolio_state: str = "全部",
    account_name: Optional[str] = None,
) -> dict:
    """Return all analysis records for a specific stock symbol."""
    return success_payload(
        services.list_analysis_history_by_symbol(
            symbol=symbol,
            portfolio_state=portfolio_state,
            account_name=account_name,
        )
    )


@router.get("")
@cache(expire=60, namespace="analysis-history")
def list_history(
    request: Request,
    _session: dict = Depends(require_session),
    portfolio_state: str = "全部",
    account_name: Optional[str] = None,
    search_term: str = "",
) -> dict:
    return success_payload(
        services.list_analysis_history(
            portfolio_state=portfolio_state,
            account_name=account_name,
            search_term=search_term,
        )
    )


@router.get("/{record_id}")
@cache(expire=60, namespace="analysis-history")
def get_history_detail(
    request: Request,
    record_id: int,
    _session: dict = Depends(require_session),
) -> dict:
    record = services.get_analysis_record_detail(record_id)
    if not record:
        raise ApiError(404, "未找到分析记录", error_code="analysis_record_not_found")
    return success_payload(record)


@router.delete("/{record_id}")
def delete_history_record(request: Request, record_id: int) -> dict:
    require_session(request)
    if not services.delete_analysis_record(record_id):
        raise ApiError(404, "未找到分析记录", error_code="analysis_record_not_found")
    return success_payload({"deleted": True}, message="分析记录已删除")
