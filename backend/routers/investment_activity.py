from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import success_payload
from backend.auth import require_session
from backend import services


router = APIRouter(prefix="/api/investment-activity", tags=["investment-activity"])


@router.get("/snapshot")
def get_snapshot(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_activity_snapshot())
