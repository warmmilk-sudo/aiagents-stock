from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import DatabaseCleanupRequest, DatabaseRestoreRequest


router = APIRouter(prefix="/api/system/database", tags=["database-admin"])


@router.get("")
def get_database_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_database_admin_status())


@router.post("/backup")
def create_backup(request: Request) -> dict:
    require_session(request)
    return success_payload(services.create_database_backup(), message="数据库备份已创建")


@router.post("/restore")
def restore_backup(request: Request, payload: DatabaseRestoreRequest) -> dict:
    require_session(request)
    try:
        data = services.restore_database_backup(payload.backup_name)
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="database_backup_not_found") from exc
    return success_payload(data, message="数据库已从备份恢复")


@router.post("/cleanup")
def cleanup_history(request: Request, payload: DatabaseCleanupRequest) -> dict:
    require_session(request)
    return success_payload(services.cleanup_database_history(payload.days), message="历史数据清理完成")
