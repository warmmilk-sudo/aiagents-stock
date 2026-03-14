from __future__ import annotations

from fastapi import APIRouter, Request

from backend import services
from backend.api import ApiError, success_payload
from backend.auth import require_session
from backend.dto import NewsFlowAlertConfigRequest, NewsFlowQuickAnalysisRequest, NewsFlowSchedulerConfigRequest, NewsFlowTaskRequest


router = APIRouter(prefix="/api/strategies/news-flow", tags=["news-flow"])


@router.post("/tasks")
def submit_news_flow_task(request: Request, payload: NewsFlowTaskRequest) -> dict:
    require_session(request)
    try:
        task_id = services.submit_news_flow_task(
            category=payload.category,
            lightweight_model=payload.lightweight_model,
            reasoning_model=payload.reasoning_model,
        )
    except (ValueError, RuntimeError) as exc:
        raise ApiError(400, str(exc), error_code="news_flow_submit_failed") from exc
    return success_payload({"task_id": task_id}, message="新闻流量分析任务已提交")


@router.get("/tasks/latest")
def get_latest_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_latest_ui_task(services.NEWS_FLOW_TASK_TYPE))


@router.get("/tasks/active")
def get_active_task(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_active_ui_task(services.NEWS_FLOW_TASK_TYPE))


@router.get("/tasks/{task_id}")
def get_task(request: Request, task_id: str) -> dict:
    require_session(request)
    task = services.get_ui_task(services.NEWS_FLOW_TASK_TYPE, task_id)
    if not task:
        raise ApiError(404, "未找到新闻流量任务", error_code="news_flow_task_not_found")
    return success_payload(task)


@router.post("/quick-analysis")
def run_quick_analysis(request: Request, payload: NewsFlowQuickAnalysisRequest) -> dict:
    require_session(request)
    try:
        data = services.run_news_flow_quick_analysis(category=payload.category)
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="news_flow_quick_analysis_failed") from exc
    return success_payload(data, message="热点同步已完成")


@router.post("/alerts/check")
def run_alert_check(request: Request) -> dict:
    require_session(request)
    try:
        data = services.run_news_flow_alert_check()
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="news_flow_alert_check_failed") from exc
    return success_payload(data, message="预警检查已完成")


@router.get("/dashboard")
def get_dashboard(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_news_flow_dashboard())


@router.get("/trend")
def get_trend(request: Request, days: int = 7) -> dict:
    require_session(request)
    return success_payload(services.get_news_flow_trend(days=days))


@router.get("/history")
def list_history(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_news_flow_history(limit=limit))


@router.get("/history/{snapshot_id}")
def get_history_detail(request: Request, snapshot_id: int) -> dict:
    require_session(request)
    detail = services.get_news_flow_snapshot_detail(snapshot_id)
    if not detail:
        raise ApiError(404, "未找到新闻流量历史记录", error_code="news_flow_history_not_found")
    return success_payload(detail)


@router.get("/alerts")
def list_alerts(request: Request, days: int = 7, alert_type: str | None = None) -> dict:
    require_session(request)
    return success_payload(services.list_news_flow_alerts(days=days, alert_type=alert_type))


@router.get("/ai-history")
def list_ai_history(request: Request, limit: int = 20) -> dict:
    require_session(request)
    return success_payload(services.list_news_flow_ai_history(limit=limit))


@router.get("/sentiment-history")
def list_sentiment_history(request: Request, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.list_news_flow_sentiment_history(limit=limit))


@router.get("/daily-statistics")
def list_daily_statistics(request: Request, days: int = 7) -> dict:
    require_session(request)
    return success_payload(services.list_news_flow_daily_statistics(days=days))


@router.get("/search-stock-news")
def search_stock_news(request: Request, keyword: str, limit: int = 50) -> dict:
    require_session(request)
    return success_payload(services.search_news_flow_stock_news(keyword, limit=limit))


@router.get("/alert-config")
def get_alert_config(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_news_flow_alert_configs())


@router.put("/alert-config")
def update_alert_config(request: Request, payload: NewsFlowAlertConfigRequest) -> dict:
    require_session(request)
    return success_payload(services.update_news_flow_alert_configs(payload.values), message="预警配置已保存")


@router.get("/scheduler")
def get_scheduler_status(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_news_flow_scheduler_status())


@router.put("/scheduler")
def update_scheduler_config(request: Request, payload: NewsFlowSchedulerConfigRequest) -> dict:
    require_session(request)
    return success_payload(
        services.update_news_flow_scheduler_config(
            task_enabled=payload.task_enabled,
            task_intervals=payload.task_intervals,
        ),
        message="定时任务配置已更新",
    )


@router.post("/scheduler/start")
def start_scheduler(request: Request) -> dict:
    require_session(request)
    return success_payload(services.start_news_flow_scheduler(), message="新闻流量调度器已启动")


@router.post("/scheduler/stop")
def stop_scheduler(request: Request) -> dict:
    require_session(request)
    return success_payload(services.stop_news_flow_scheduler(), message="新闻流量调度器已停止")


@router.post("/scheduler/run-now")
def run_scheduler_task(request: Request, task_type: str) -> dict:
    require_session(request)
    try:
        data = services.run_news_flow_scheduler_task(task_type)
    except ValueError as exc:
        raise ApiError(400, str(exc), error_code="news_flow_scheduler_task_failed") from exc
    return success_payload(data, message="新闻流量任务已触发")


@router.get("/scheduler/logs")
def list_scheduler_logs(request: Request, days: int = 7, task_type: str | None = None) -> dict:
    require_session(request)
    return success_payload(services.list_news_flow_scheduler_logs(days=days, task_type=task_type))


@router.get("/platforms")
def list_supported_platforms(request: Request) -> dict:
    require_session(request)
    return success_payload(services.get_news_flow_supported_platforms())
