from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


logger = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        error_code: str = "api_error",
        details: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.error_code = error_code
        self.details = details


def success_payload(data: Any = None, message: str = "ok") -> dict[str, Any]:
    return {
        "success": True,
        "message": message,
        "data": data,
    }


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "success": False,
                "error_code": exc.error_code,
                "message": exc.message,
                "details": exc.details,
            },
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(_request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled backend error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error_code": "internal_error",
                "message": str(exc) or "服务器内部错误",
                "details": None,
            },
        )

