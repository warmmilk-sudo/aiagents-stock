from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import build_session_key, require_session
from backend.dto import AnalysisTaskRequest
from backend import services


router = APIRouter(prefix="/api/analysis", tags=["analysis"])


@router.post("/tasks")
def submit_analysis_task(request: Request, payload: AnalysisTaskRequest) -> dict:
    session = require_session(request)
    symbols = payload.symbols or services.parse_stock_list(payload.stock_input)
    try:
        task_id = services.submit_research_analysis_task(
            session_key=build_session_key(session),
            symbols=symbols,
            period=payload.period,
            batch_mode=payload.batch_mode,
            max_workers=payload.max_workers,
            analysts=services.build_analyst_config(payload.analysts),
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="invalid_analysis_request") from exc
    return success_payload({"task_id": task_id}, message="分析任务已提交")
