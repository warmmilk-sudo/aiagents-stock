from __future__ import annotations

import logging
import json
import math
import numbers
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


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, set):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        numeric = float(value)
        if math.isfinite(numeric):
            if isinstance(value, numbers.Integral):
                return int(value)
            return numeric
        return None
    return value


class SafeJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        safe_content = _sanitize_json_value(content)
        return json.dumps(
            safe_content,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")


def success_payload(data: Any = None, message: str = "ok") -> dict[str, Any]:
    return {
        "success": True,
        "message": message,
        "data": data,
    }


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return SafeJSONResponse(
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
        return SafeJSONResponse(
            status_code=500,
            content={
                "success": False,
                "error_code": "internal_error",
                "message": str(exc) or "服务器内部错误",
                "details": None,
            },
        )
