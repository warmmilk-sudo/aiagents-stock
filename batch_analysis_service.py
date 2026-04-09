from __future__ import annotations

import concurrent.futures
import logging
import sqlite3
import time
from typing import Callable, Dict, Optional

from ai_agents import StockAnalysisAgents
from asset_service import asset_service
from asset_repository import STATUS_PORTFOLIO, asset_repository
from database import db
from stock_data import StockDataFetcher
from stock_data_cache import strip_cache_meta


logger = logging.getLogger(__name__)
EXPECTED_OPTIONAL_ERRORS = (
    ImportError,
    ValueError,
    TypeError,
    RuntimeError,
    OSError,
    TimeoutError,
    ConnectionError,
)


def _get_stock_data(symbol: str, period: str):
    fetcher = StockDataFetcher()
    stock_info = fetcher.get_stock_info(symbol)
    realtime_quote = fetcher.get_realtime_quote(symbol)
    stock_data = fetcher.get_stock_data(symbol, period)

    if isinstance(stock_data, dict) and "error" in stock_data:
        return stock_info, None, None

    if isinstance(stock_info, dict) and "error" not in stock_info:
        stock_info = dict(stock_info)
        if isinstance(realtime_quote, dict) and realtime_quote:
            current_price = realtime_quote.get("current_price", realtime_quote.get("price"))
            try:
                current_price_value = float(current_price)
            except (TypeError, ValueError):
                current_price_value = 0.0
            if current_price_value > 0:
                stock_info["current_price"] = current_price_value
                stock_info.pop("change_percent", None)
                stock_info.pop("realtime_data_source", None)
                if realtime_quote.get("change_percent") not in (None, ""):
                    stock_info["change_percent"] = realtime_quote.get("change_percent")
                if realtime_quote.get("data_source"):
                    stock_info["realtime_data_source"] = realtime_quote.get("data_source")
            else:
                stock_info.pop("current_price", None)
                stock_info.pop("change_percent", None)
                stock_info.pop("realtime_data_source", None)
        else:
            stock_info.pop("current_price", None)
            stock_info.pop("change_percent", None)
            stock_info.pop("realtime_data_source", None)

    stock_data_with_indicators = fetcher.calculate_technical_indicators(stock_data)
    indicators = fetcher.get_latest_indicators(stock_data_with_indicators, symbol=symbol)
    return stock_info, stock_data_with_indicators, indicators


def _resolve_position_state(symbol: str, has_position: Optional[bool] = None) -> bool:
    if has_position is not None:
        return bool(has_position)
    try:
        assets = asset_repository.list_assets(
            status=STATUS_PORTFOLIO,
            symbol=symbol,
            include_deleted=False,
        )
        return bool(assets)
    except Exception as exc:
        logger.warning("[%s] failed resolving holding state: %s", symbol, exc)
        return False


def _fetch_optional_data(symbol: str, source_name: str, fetch_func: Callable[[], object], strip_meta: bool = False):
    """Fetch optional data source and downgrade on expected source/runtime failures."""
    try:
        data = fetch_func()
        return strip_cache_meta(data) if strip_meta else data
    except EXPECTED_OPTIONAL_ERRORS as exc:
        logger.warning("[%s] optional source '%s' downgraded: %s", symbol, source_name, exc)
        return None


def _collect_optional_context_data(symbol: str, stock_data, enabled_analysts_config: Dict[str, bool]) -> dict[str, object]:
    fetcher = StockDataFetcher()
    is_chinese_stock = fetcher._is_chinese_stock(symbol)
    tasks: dict[str, tuple[str, Callable[[], object], bool]] = {}

    if enabled_analysts_config.get("fundamental", True):
        tasks["financial_data"] = (
            "financial_data",
            lambda: StockDataFetcher().get_financial_data(symbol),
            True,
        )
        if is_chinese_stock:
            tasks["quarterly_data"] = (
                "quarterly_report_data",
                lambda: __import__("quarterly_report_data").QuarterlyReportDataFetcher().get_quarterly_reports(symbol),
                True,
            )

    if enabled_analysts_config.get("fund_flow", True) and is_chinese_stock:
        tasks["fund_flow_data"] = (
            "fund_flow_data",
            lambda: __import__("fund_flow_data").FundFlowDataFetcher().get_fund_flow_data(symbol),
            False,
        )

    if enabled_analysts_config.get("sentiment", False) and is_chinese_stock:
        tasks["sentiment_data"] = (
            "market_sentiment_data",
            lambda: __import__("market_sentiment_data").MarketSentimentDataFetcher().get_market_sentiment_data(symbol, stock_data),
            False,
        )

    if enabled_analysts_config.get("news", False) and is_chinese_stock:
        tasks["news_data"] = (
            "stock_research_news_data",
            lambda: __import__("stock_research_news_data").StockResearchNewsDataFetcher().get_stock_news(symbol),
            False,
        )

    if enabled_analysts_config.get("risk", True) and is_chinese_stock:
        tasks["risk_data"] = (
            "risk_data",
            lambda: StockDataFetcher().get_risk_data(symbol),
            False,
        )

    results = {
        "financial_data": None,
        "quarterly_data": None,
        "fund_flow_data": None,
        "sentiment_data": None,
        "news_data": None,
        "risk_data": None,
    }
    if not tasks:
        return results

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        future_map = {
            executor.submit(_fetch_optional_data, symbol, source_name, fetch_func, strip_meta): result_key
            for result_key, (source_name, fetch_func, strip_meta) in tasks.items()
        }
        for future in concurrent.futures.as_completed(future_map):
            result_key = future_map[future]
            results[result_key] = future.result()
    return results


def _sync_managed_monitors_for_symbol(symbol: str) -> None:
    try:
        sync_result = asset_service.sync_managed_monitors_for_symbol(symbol)
        logger.info(
            "[%s] synced managed monitor baselines after research analysis: ai=%s alert=%s removed=%s",
            symbol,
            sync_result.get("ai_tasks_upserted", 0),
            sync_result.get("price_alerts_upserted", 0),
            sync_result.get("removed", 0),
        )
    except Exception as exc:
        logger.warning("[%s] failed syncing managed monitor baselines: %s", symbol, exc)


def _report_stage_progress(
    progress_callback: Optional[Callable[[int, int, str], None]],
    current: int,
    total: int,
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(current, total, message)


def analyze_single_stock_for_batch(
    symbol,
    period,
    enabled_analysts_config: Optional[Dict] = None,
    selected_model=None,
    selected_lightweight_model=None,
    selected_reasoning_model=None,
    save_to_global_history: bool = True,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    has_position: Optional[bool] = None,
):
    """Analyze one stock for batch mode via the shared backend analysis pipeline."""
    try:
        overall_started_at = time.perf_counter()
        forced_model = selected_model
        if selected_lightweight_model or selected_reasoning_model:
            forced_model = None

        if enabled_analysts_config is None:
            enabled_analysts_config = {
                "technical": True,
                "fundamental": True,
                "fund_flow": True,
                "risk": True,
                "sentiment": False,
                "news": False,
            }
        elif not any(bool(enabled) for enabled in enabled_analysts_config.values()):
            return {"symbol": symbol, "error": "请至少选择一位分析师参与分析", "success": False}

        _report_stage_progress(progress_callback, 5, 100, f"正在获取 {symbol} 的分析数据...")
        stock_info, stock_data, indicators = _get_stock_data(symbol, period)
        if "error" in stock_info:
            return {"symbol": symbol, "error": stock_info["error"], "success": False}
        if stock_data is None:
            return {"symbol": symbol, "error": "无法获取股票历史数据", "success": False}

        stock_info = strip_cache_meta(stock_info)
        stock_data = strip_cache_meta(stock_data)
        resolved_has_position = _resolve_position_state(symbol, has_position=has_position)
        stock_info["has_position"] = resolved_has_position
        stock_info["position_status"] = "已持仓" if resolved_has_position else "未持仓"
        context_started_at = time.perf_counter()
        context_data = _collect_optional_context_data(symbol, stock_data, enabled_analysts_config)
        logger.info("[%s] optional context data prepared in %.2fs", symbol, time.perf_counter() - context_started_at)

        agents = StockAnalysisAgents(
            model=forced_model,
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )

        _report_stage_progress(progress_callback, 25, 100, f"AI 分析师团队正在分析 {symbol}...")
        analysts_started_at = time.perf_counter()
        agents_results = agents.run_multi_agent_analysis(
            stock_info,
            stock_data,
            indicators,
            context_data["financial_data"],
            context_data["fund_flow_data"],
            context_data["sentiment_data"],
            context_data["news_data"],
            context_data["quarterly_data"],
            context_data["risk_data"],
            enabled_analysts=enabled_analysts_config,
        )
        logger.info("[%s] analyst stage completed in %.2fs", symbol, time.perf_counter() - analysts_started_at)

        _report_stage_progress(progress_callback, 75, 100, f"AI 团队正在讨论 {symbol} 的综合结论...")
        discussion_started_at = time.perf_counter()
        discussion_result = agents.conduct_team_discussion(agents_results, stock_info, indicators)
        logger.info("[%s] team discussion completed in %.2fs", symbol, time.perf_counter() - discussion_started_at)

        _report_stage_progress(progress_callback, 90, 100, f"正在生成 {symbol} 的最终决策...")
        decision_started_at = time.perf_counter()
        final_decision = agents.make_final_decision(discussion_result, stock_info, indicators)
        logger.info("[%s] final decision completed in %.2fs", symbol, time.perf_counter() - decision_started_at)

        saved_to_db = False
        record_id = None
        db_error = None
        if save_to_global_history:
            try:
                record_id = db.save_analysis(
                    symbol=stock_info.get("symbol", ""),
                    stock_name=stock_info.get("name", ""),
                    period=period,
                    stock_info=stock_info,
                    agents_results=agents_results,
                    discussion_result=discussion_result,
                    final_decision=final_decision,
                )
                saved_to_db = True
                logger.info("%s saved to global history (record_id=%s)", symbol, record_id)
                _sync_managed_monitors_for_symbol(symbol)
            except (sqlite3.DatabaseError, OSError, RuntimeError, TypeError, ValueError) as exc:
                db_error = str(exc)
                logger.warning("%s failed saving to global history: %s", symbol, db_error)
            except Exception:
                logger.exception("%s unexpected error while saving to global history", symbol)
                raise

        logger.info("[%s] full analysis pipeline completed in %.2fs", symbol, time.perf_counter() - overall_started_at)

        return {
            "symbol": symbol,
            "success": True,
            "stock_info": stock_info,
            "indicators": indicators,
            "agents_results": agents_results,
            "discussion_result": discussion_result,
            "final_decision": final_decision,
            "record_id": record_id,
            "saved_to_db": saved_to_db,
            "db_error": db_error,
        }
    except Exception as exc:
        logger.exception("%s batch analysis failed", symbol)
        return {"symbol": symbol, "error": str(exc), "success": False}
