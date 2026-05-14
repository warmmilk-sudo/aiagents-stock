from __future__ import annotations

import concurrent.futures
import logging
import sqlite3
import time
from typing import Any, Callable, Dict, Optional

from ai_agents import StockAnalysisAgents
from asset_service import asset_service
from asset_repository import STATUS_PORTFOLIO, asset_repository
from database import db
from investment_db_utils import DEFAULT_ACCOUNT_NAME, normalize_account_name
from portfolio_db import portfolio_db
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
        return stock_info, None, {"error": stock_data["error"]}

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
                for field in (
                    "volume",
                    "amount",
                    "high",
                    "low",
                    "open",
                    "pre_close",
                    "turnover_rate",
                    "volume_ratio",
                    "order_book",
                ):
                    if realtime_quote.get(field) not in (None, ""):
                        stock_info[field] = realtime_quote.get(field)
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
    if isinstance(stock_info, dict) and isinstance(indicators, dict):
        volume_ratio = indicators.get("volume_ratio")
        if stock_info.get("volume_ratio") in (None, "", "N/A") and volume_ratio not in (None, "", "N/A"):
            try:
                volume_ratio = float(volume_ratio)
            except (TypeError, ValueError):
                pass
            stock_info["volume_ratio"] = volume_ratio
            stock_info["volume_ratio_source"] = "history_volume_ma5"
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


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value) if value not in (None, "") else 0
    except (TypeError, ValueError):
        return 0


def _resolve_position_asset(
    *,
    symbol: str,
    account_name: Optional[str],
    asset_id: Optional[int] = None,
    portfolio_stock_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    for candidate_id in (portfolio_stock_id, asset_id):
        if candidate_id in (None, ""):
            continue
        try:
            asset = asset_repository.get_asset(int(candidate_id))
        except Exception as exc:
            logger.warning("[%s] failed loading holding asset %s: %s", symbol, candidate_id, exc)
            asset = None
        if asset:
            return asset

    normalized_account = normalize_account_name(account_name) or DEFAULT_ACCOUNT_NAME
    try:
        asset = asset_repository.get_asset_by_symbol(symbol, normalized_account)
    except Exception as exc:
        logger.warning("[%s] failed loading holding asset by symbol: %s", symbol, exc)
        asset = None
    if asset:
        return asset

    try:
        assets = asset_repository.list_assets(
            status=STATUS_PORTFOLIO,
            symbol=symbol,
            include_deleted=False,
        )
    except Exception as exc:
        logger.warning("[%s] failed listing holding assets: %s", symbol, exc)
        return None
    return assets[0] if assets else None


def _resolve_position_context(
    *,
    symbol: str,
    has_position: bool,
    account_name: Optional[str] = None,
    asset_id: Optional[int] = None,
    portfolio_stock_id: Optional[int] = None,
    current_price: Any = None,
) -> Dict[str, Any]:
    if not has_position:
        return {}

    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return {}

    asset = _resolve_position_asset(
        symbol=normalized_symbol,
        account_name=account_name,
        asset_id=asset_id,
        portfolio_stock_id=portfolio_stock_id,
    )
    if not asset:
        return {}

    quantity = _safe_int(asset.get("quantity"))
    cost_price = _safe_float(asset.get("cost_price"))
    if quantity <= 0:
        return {}

    normalized_account = normalize_account_name(asset.get("account_name") or account_name) or DEFAULT_ACCOUNT_NAME
    resolved_current_price = _safe_float(current_price) or cost_price
    market_value = max(0.0, resolved_current_price * quantity)
    cost_value = max(0.0, cost_price * quantity)
    profit_loss = market_value - cost_value
    profit_loss_pct = (profit_loss / cost_value * 100.0) if cost_value > 0 else 0.0

    account_total_market_value = 0.0
    found_current_asset = False
    try:
        account_assets = asset_repository.list_assets(
            status=STATUS_PORTFOLIO,
            include_deleted=False,
        )
    except Exception as exc:
        logger.warning("[%s] failed listing account holdings for position ratio: %s", normalized_symbol, exc)
        account_assets = []

    asset_identity = asset.get("id")
    for holding in account_assets:
        if normalize_account_name(holding.get("account_name")) != normalized_account:
            continue
        holding_quantity = _safe_int(holding.get("quantity"))
        if holding_quantity <= 0:
            continue
        holding_cost_price = _safe_float(holding.get("cost_price"))
        same_asset = (
            (asset_identity is not None and holding.get("id") == asset_identity)
            or str(holding.get("symbol") or holding.get("code") or "").strip().upper() == normalized_symbol
        )
        holding_price = resolved_current_price if same_asset and resolved_current_price > 0 else holding_cost_price
        account_total_market_value += max(0.0, holding_price * holding_quantity)
        found_current_asset = found_current_asset or same_asset

    if not found_current_asset:
        account_total_market_value += market_value

    try:
        configured_total_assets = _safe_float(portfolio_db.get_account_total_assets(normalized_account, 0.0))
    except Exception as exc:
        logger.warning("[%s] failed loading account total assets: %s", normalized_symbol, exc)
        configured_total_assets = 0.0

    effective_total_assets = configured_total_assets if configured_total_assets > 0 else max(
        account_total_market_value,
        market_value,
    )
    position_pct = (market_value / effective_total_assets * 100.0) if effective_total_assets > 0 else 0.0
    position_weight_pct = (
        market_value / account_total_market_value * 100.0
        if account_total_market_value > 0
        else position_pct
    )
    total_position_pct = (
        account_total_market_value / effective_total_assets * 100.0
        if effective_total_assets > 0
        else 0.0
    )

    return {
        "asset_id": asset.get("id"),
        "portfolio_stock_id": asset.get("id") if asset.get("status") == STATUS_PORTFOLIO else portfolio_stock_id,
        "account_name": normalized_account,
        "quantity": quantity,
        "cost_price": round(cost_price, 4),
        "current_price": round(resolved_current_price, 4),
        "market_value": round(market_value, 2),
        "cost_value": round(cost_value, 2),
        "profit_loss": round(profit_loss, 2),
        "profit_loss_pct": round(profit_loss_pct, 2),
        "position_pct": round(position_pct, 2),
        "position_weight_pct": round(position_weight_pct, 2),
        "total_position_pct": round(total_position_pct, 2),
        "account_total_assets": round(effective_total_assets, 2),
        "configured_total_assets": round(configured_total_assets, 2),
        "account_total_market_value": round(account_total_market_value, 2),
        "total_assets_configured": configured_total_assets > 0,
    }


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


def _load_latest_strategy_context(
    *,
    symbol: str,
    has_position: bool,
    account_name: Optional[str] = None,
    asset_id: Optional[int] = None,
    portfolio_stock_id: Optional[int] = None,
) -> Optional[Dict]:
    if not has_position:
        return None
    try:
        return db.repository.get_latest_strategy_context(
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            symbol=symbol,
            account_name=account_name,
        )
    except Exception as exc:
        logger.warning("[%s] failed loading latest strategy context: %s", symbol, exc)
        return None


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
    account_name: Optional[str] = None,
    asset_id: Optional[int] = None,
    portfolio_stock_id: Optional[int] = None,
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
            detail = indicators.get("error") if isinstance(indicators, dict) else ""
            message = detail or "无法获取股票历史数据"
            return {"symbol": symbol, "error": message, "success": False}

        stock_info = strip_cache_meta(stock_info)
        stock_data = strip_cache_meta(stock_data)
        resolved_has_position = _resolve_position_state(symbol, has_position=has_position)
        stock_info["has_position"] = resolved_has_position
        stock_info["position_status"] = "已持仓" if resolved_has_position else "未持仓"
        position_context = _resolve_position_context(
            symbol=symbol,
            has_position=resolved_has_position,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            current_price=stock_info.get("current_price"),
        )
        if position_context:
            stock_info["position_context"] = position_context
            stock_info["position_cost"] = position_context.get("cost_price")
            stock_info["position_quantity"] = position_context.get("quantity")
            stock_info["current_position_pct"] = position_context.get("position_pct")
            stock_info["position_market_value"] = position_context.get("market_value")
            stock_info["account_total_assets"] = position_context.get("account_total_assets")
            account_name = position_context.get("account_name") or account_name
            asset_id = asset_id or position_context.get("asset_id")
            portfolio_stock_id = portfolio_stock_id or position_context.get("portfolio_stock_id")
        existing_strategy_context = _load_latest_strategy_context(
            symbol=symbol,
            has_position=resolved_has_position,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        existing_swing_type = str((existing_strategy_context or {}).get("swing_type") or "").strip()
        is_initial_holding_analysis = bool(resolved_has_position and not existing_swing_type)
        context_started_at = time.perf_counter()
        context_data = _collect_optional_context_data(symbol, stock_data, enabled_analysts_config)
        logger.info("[%s] optional context data prepared in %.2fs", symbol, time.perf_counter() - context_started_at)

        # --- Memory module: load historical context for this stock ---
        memory_context = None
        try:
            from agent_memory_service import agent_memory_service
            memory_context = agent_memory_service.assemble_memory_context(
                stock_code=symbol,
                current_summary=stock_info.get("name", "") + " " + symbol,
                stock_name=stock_info.get("name", ""),
            )
            if memory_context and any(memory_context.get(k) for k in ("long_term_profile", "working_memories", "recalled_facts")):
                logger.info("[%s] Memory context loaded: profile=%s, working=%d, recalled=%d",
                            symbol,
                            bool(memory_context.get("long_term_profile")),
                            len(memory_context.get("working_memories", [])),
                            len(memory_context.get("recalled_facts", [])))
            else:
                memory_context = None
        except Exception as mem_exc:
            logger.warning("[%s] Memory context loading skipped: %s", symbol, mem_exc)
            memory_context = None

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
        discussion_result = agents.conduct_team_discussion(
            agents_results,
            stock_info,
            indicators,
            memory_context=memory_context,
            strategy_context=existing_strategy_context,
            is_initial_holding_analysis=is_initial_holding_analysis,
        )
        logger.info("[%s] team discussion completed in %.2fs", symbol, time.perf_counter() - discussion_started_at)

        _report_stage_progress(progress_callback, 90, 100, f"正在生成 {symbol} 的最终决策...")
        decision_started_at = time.perf_counter()
        final_decision = agents.make_final_decision(
            discussion_result,
            stock_info,
            indicators,
            strategy_context=existing_strategy_context,
            is_initial_holding_analysis=is_initial_holding_analysis,
            memory_context=memory_context,
        )
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

                # --- Memory daemon: fire event for background processing ---
                try:
                    from agent_memory_daemon import publish_analysis_completed, start_memory_daemon
                    start_memory_daemon()
                    publish_analysis_completed(
                        stock_code=symbol,
                        stock_name=stock_info.get("name", ""),
                        analysis_date=time.strftime("%Y-%m-%d %H:%M:%S"),
                        rating=str((final_decision or {}).get("rating", "")),
                        summary=str((final_decision or {}).get("operation_advice", "")),
                        discussion_summary=str(discussion_result or "")[:4000],
                        final_decision=final_decision,
                        source_analysis_id=record_id,
                    )
                except Exception as mem_evt_exc:
                    logger.warning("[%s] Memory event publish skipped: %s", symbol, mem_evt_exc)

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
            "recalled_fact_ids": (memory_context or {}).get("recalled_fact_ids", []),
        }
    except Exception as exc:
        logger.exception("%s batch analysis failed", symbol)
        return {"symbol": symbol, "error": str(exc), "success": False}
