from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import ResearchHubAssetUpdateRequest, ResearchHubQuickAnalyzeRequest, ResearchHubSelectionTaskRequest
import research_hub_service


router = APIRouter(prefix="/api/watchlist-hub", tags=["research-hub"])


@router.get("/overview")
def get_overview(request: Request) -> dict:
    require_session(request)
    return success_payload(research_hub_service.get_hub_overview())


@router.get("/assets")
def list_assets(request: Request, pool: str = "", search_term: str = "") -> dict:
    require_session(request)
    return success_payload(research_hub_service.list_hub_assets(pool=pool or None, search_term=search_term))


@router.get("/assets/{asset_id}")
def get_asset_detail(request: Request, asset_id: int) -> dict:
    require_session(request)
    item = research_hub_service.get_hub_asset_detail(asset_id)
    if not item:
        raise ApiError(404, "未找到标的", error_code="research_hub_asset_not_found")
    return success_payload(item)


@router.get("/assets/{asset_id}/timeline")
def get_asset_timeline(request: Request, asset_id: int) -> dict:
    require_session(request)
    return success_payload(research_hub_service.get_hub_asset_timeline(asset_id))


@router.patch("/assets/{asset_id}")
def update_asset(request: Request, asset_id: int, payload: ResearchHubAssetUpdateRequest) -> dict:
    require_session(request)
    try:
        item = research_hub_service.update_hub_asset(asset_id, **payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="research_hub_asset_update_failed") from exc
    if not item:
        raise ApiError(404, "未找到标的", error_code="research_hub_asset_not_found")
    return success_payload(item, message="标的状态已更新")


@router.delete("/assets/{asset_id}")
def delete_asset(request: Request, asset_id: int) -> dict:
    require_session(request)
    try:
        deleted = research_hub_service.delete_hub_asset(asset_id)
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="research_hub_asset_delete_failed") from exc
    if not deleted:
        raise ApiError(404, "未找到标的", error_code="research_hub_asset_not_found")
    return success_payload({"deleted": True, "asset_id": asset_id}, message="研究池卡片已删除")


@router.post("/quick-analyze")
def quick_analyze(request: Request, payload: ResearchHubQuickAnalyzeRequest) -> dict:
    require_session(request)
    try:
        result = research_hub_service.quick_analyze_and_add_to_research(payload.symbol)
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="research_hub_quick_analyze_failed") from exc
    return success_payload(result, message="快速分析完成，已加入研究池")


@router.get("/sector-strategy/latest")
def get_sector_strategy_latest(request: Request) -> dict:
    require_session(request)
    return success_payload(research_hub_service.get_recent_sector_strategy_report())


@router.post("/selection/run")
def run_selection(request: Request, payload: ResearchHubSelectionTaskRequest | None = None) -> dict:
    require_session(request)
    try:
        task_id = research_hub_service.submit_selection_run(
            lightweight_model=payload.lightweight_model if payload else None,
            reasoning_model=payload.reasoning_model if payload else None,
        )
    except (ValueError, RuntimeError, TimeoutError) as exc:
        raise ApiError(400, str(exc), error_code="research_hub_selection_start_failed") from exc
    return success_payload({"task_id": task_id}, message="已提交智能选股任务")


@router.get("/selection/tasks/latest")
def get_latest_selection_task(request: Request) -> dict:
    require_session(request)
    return success_payload(research_hub_service.get_selection_task_status())


@router.get("/selection/tasks/{task_id}")
def get_selection_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = research_hub_service.get_selection_task_status(task_id)
    if not task:
        raise ApiError(404, "未找到智能选股任务", error_code="research_hub_selection_task_not_found")
    return success_payload(task)


@router.post("/funnel/run")
def run_funnel(request: Request, payload: ResearchHubSelectionTaskRequest | None = None) -> dict:
    require_session(request)
    try:
        task_id = research_hub_service.submit_funnel_run(
            lightweight_model=payload.lightweight_model if payload else None,
            reasoning_model=payload.reasoning_model if payload else None,
        )
    except (ValueError, RuntimeError, TimeoutError) as exc:
        raise ApiError(400, str(exc), error_code="research_hub_funnel_start_failed") from exc
    return success_payload({"task_id": task_id}, message="已提交智能选股任务")


@router.get("/funnel/tasks/latest")
def get_latest_funnel_task(request: Request) -> dict:
    require_session(request)
    return success_payload(research_hub_service.get_selection_task_status())


@router.get("/funnel/tasks/{task_id}")
def get_funnel_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = research_hub_service.get_selection_task_status(task_id)
    if not task:
        raise ApiError(404, "未找到智能选股任务", error_code="research_hub_funnel_task_not_found")
    return success_payload(task)
