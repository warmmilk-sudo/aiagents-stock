from __future__ import annotations

from fastapi import APIRouter, Request, Response

from backend import services
from backend.auth import require_session
from backend.dto import LonghubangExportRequest, MacroCycleExportRequest, MainForceExportRequest, NewsFlowExportRequest, SectorStrategyExportRequest


router = APIRouter(prefix="/api/exports", tags=["exports"])


def _build_file_response(data: bytes, filename: str, media_type: str) -> Response:
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
    }
    return Response(content=data, media_type=media_type, headers=headers)


@router.post("/main-force/markdown")
def export_main_force_markdown(request: Request, payload: MainForceExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_main_force_markdown(
        payload.result,
        payload.context_snapshot,
    )
    return _build_file_response(data, filename, media_type)


@router.post("/main-force/pdf")
def export_main_force_pdf(request: Request, payload: MainForceExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_main_force_pdf(
        payload.result,
        payload.context_snapshot,
    )
    return _build_file_response(data, filename, media_type)


@router.post("/sector-strategy/markdown")
def export_sector_strategy_markdown(request: Request, payload: SectorStrategyExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_sector_strategy_markdown(payload.result)
    return _build_file_response(data, filename, media_type)


@router.post("/sector-strategy/pdf")
def export_sector_strategy_pdf(request: Request, payload: SectorStrategyExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_sector_strategy_pdf(payload.result)
    return _build_file_response(data, filename, media_type)


@router.post("/longhubang/markdown")
def export_longhubang_markdown(request: Request, payload: LonghubangExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_longhubang_markdown(payload.result)
    return _build_file_response(data, filename, media_type)


@router.post("/longhubang/pdf")
def export_longhubang_pdf(request: Request, payload: LonghubangExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_longhubang_pdf(payload.result)
    return _build_file_response(data, filename, media_type)


@router.post("/macro-cycle/markdown")
def export_macro_cycle_markdown(request: Request, payload: MacroCycleExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_macro_cycle_markdown(payload.result)
    return _build_file_response(data, filename, media_type)


@router.post("/macro-cycle/pdf")
def export_macro_cycle_pdf(request: Request, payload: MacroCycleExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_macro_cycle_pdf(payload.result)
    return _build_file_response(data, filename, media_type)


@router.post("/news-flow/pdf")
def export_news_flow_pdf(request: Request, payload: NewsFlowExportRequest) -> Response:
    require_session(request)
    data, filename, media_type = services.export_news_flow_pdf(payload.result)
    return _build_file_response(data, filename, media_type)
