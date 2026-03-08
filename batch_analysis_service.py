from __future__ import annotations

import logging
import sqlite3
from typing import Callable, Dict, Optional

from ai_agents import StockAnalysisAgents
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
    stock_data = fetcher.get_stock_data(symbol, period)

    if isinstance(stock_data, dict) and "error" in stock_data:
        return stock_info, None, None

    stock_data_with_indicators = fetcher.calculate_technical_indicators(stock_data)
    indicators = fetcher.get_latest_indicators(stock_data_with_indicators)
    return stock_info, stock_data_with_indicators, indicators


def _fetch_optional_data(symbol: str, source_name: str, fetch_func: Callable[[], object], strip_meta: bool = False):
    """Fetch optional data source and downgrade on expected source/runtime failures."""
    try:
        data = fetch_func()
        return strip_cache_meta(data) if strip_meta else data
    except EXPECTED_OPTIONAL_ERRORS as exc:
        logger.warning("[%s] optional source '%s' downgraded: %s", symbol, source_name, exc)
        return None


def analyze_single_stock_for_batch(
    symbol,
    period,
    enabled_analysts_config: Optional[Dict] = None,
    selected_model=None,
    selected_lightweight_model=None,
    selected_reasoning_model=None,
    save_to_global_history: bool = True,
):
    """Analyze one stock for batch mode; extracted from app.py to avoid cyclic imports."""
    try:
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

        stock_info, stock_data, indicators = _get_stock_data(symbol, period)
        if "error" in stock_info:
            return {"symbol": symbol, "error": stock_info["error"], "success": False}
        if stock_data is None:
            return {"symbol": symbol, "error": "无法获取股票历史数据", "success": False}

        stock_info = strip_cache_meta(stock_info)
        stock_data = strip_cache_meta(stock_data)

        fetcher = StockDataFetcher()
        financial_data = strip_cache_meta(fetcher.get_financial_data(symbol))

        quarterly_data = None
        if enabled_analysts_config.get("fundamental", True) and fetcher._is_chinese_stock(symbol):
            quarterly_data = _fetch_optional_data(
                symbol,
                "quarterly_report_data",
                lambda: __import__("quarterly_report_data")
                .QuarterlyReportDataFetcher()
                .get_quarterly_reports(symbol),
                strip_meta=True,
            )

        fund_flow_data = None
        if enabled_analysts_config.get("fund_flow", True) and fetcher._is_chinese_stock(symbol):
            fund_flow_data = _fetch_optional_data(
                symbol,
                "fund_flow_akshare",
                lambda: __import__("fund_flow_akshare").FundFlowAkshareDataFetcher().get_fund_flow_data(symbol),
            )

        sentiment_data = None
        if enabled_analysts_config.get("sentiment", False) and fetcher._is_chinese_stock(symbol):
            sentiment_data = _fetch_optional_data(
                symbol,
                "market_sentiment_data",
                lambda: __import__("market_sentiment_data")
                .MarketSentimentDataFetcher()
                .get_market_sentiment_data(symbol, stock_data),
            )

        news_data = None
        if enabled_analysts_config.get("news", False) and fetcher._is_chinese_stock(symbol):
            news_data = _fetch_optional_data(
                symbol,
                "qstock_news_data",
                lambda: __import__("qstock_news_data").QStockNewsDataFetcher().get_stock_news(symbol),
            )

        risk_data = None
        if enabled_analysts_config.get("risk", True) and fetcher._is_chinese_stock(symbol):
            risk_data = _fetch_optional_data(
                symbol,
                "risk_data",
                lambda: fetcher.get_risk_data(symbol),
            )

        agents = StockAnalysisAgents(
            model=forced_model,
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )

        agents_results = agents.run_multi_agent_analysis(
            stock_info,
            stock_data,
            indicators,
            financial_data,
            fund_flow_data,
            sentiment_data,
            news_data,
            quarterly_data,
            risk_data,
            enabled_analysts=enabled_analysts_config,
        )
        discussion_result = agents.conduct_team_discussion(agents_results, stock_info)
        final_decision = agents.make_final_decision(discussion_result, stock_info, indicators)

        saved_to_db = False
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
            except (sqlite3.DatabaseError, OSError, RuntimeError, TypeError, ValueError) as exc:
                db_error = str(exc)
                logger.warning("%s failed saving to global history: %s", symbol, db_error)
            except Exception:
                logger.exception("%s unexpected error while saving to global history", symbol)
                raise

        return {
            "symbol": symbol,
            "success": True,
            "stock_info": stock_info,
            "indicators": indicators,
            "agents_results": agents_results,
            "discussion_result": discussion_result,
            "final_decision": final_decision,
            "saved_to_db": saved_to_db,
            "db_error": db_error,
        }
    except Exception as exc:
        logger.exception("%s batch analysis failed", symbol)
        return {"symbol": symbol, "error": str(exc), "success": False}
