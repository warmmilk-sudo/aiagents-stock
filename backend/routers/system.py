from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import success_payload
from backend.auth import require_session
from backend import services


router = APIRouter(prefix="/api/system", tags=["system"])


@router.get("/status")
def get_system_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_system_status())


@router.post("/monitor-service/start")
def start_monitor_service(request: Request) -> dict:
    require_session(request)
    return success_payload(services.start_monitor_runtime(), message="智能盯盘服务已启动")


@router.post("/monitor-service/stop")
def stop_monitor_service(request: Request) -> dict:
    require_session(request)
    return success_payload(services.stop_monitor_runtime(), message="智能盯盘服务已停止")
