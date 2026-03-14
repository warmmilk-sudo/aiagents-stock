from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import FollowupStatusRequest
from backend import services


router = APIRouter(prefix="/api/followup-assets", tags=["followup-assets"])


@router.get("")
def list_followup_assets(request: Request, status_filter: str = "全部", search_term: str = "") -> dict:
    require_session(request)
    return success_payload(
        services.list_followup_assets(status_filter=status_filter, search_term=search_term)
    )


@router.post("/{asset_id}/watchlist")
def promote_to_watchlist(request: Request, asset_id: int) -> dict:
    require_session(request)
    success, message = services.promote_followup_to_watchlist(asset_id)
    if not success:
        raise ApiError(400, message, error_code="followup_promote_failed")
    return success_payload({"asset_id": asset_id}, message=message)


@router.post("/{asset_id}/research")
def demote_to_research(request: Request, asset_id: int, payload: FollowupStatusRequest) -> dict:
    require_session(request)
    success = services.demote_followup_to_research(asset_id, note=payload.note)
    if not success:
        raise ApiError(400, "移回看过失败", error_code="followup_demote_failed")
    return success_payload({"asset_id": asset_id}, message="已移回看过")
