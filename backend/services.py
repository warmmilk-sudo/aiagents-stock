from __future__ import annotations

import concurrent.futures
import logging
import time
from datetime import date, datetime, timedelta
from functools import lru_cache
from types import SimpleNamespace
from typing import Any, Optional

import config
from analysis_history_service import analysis_history_service
from analysis_repository import analysis_repository
from asset_service import STATUS_RESEARCH, STATUS_WATCHLIST, asset_service
from batch_analysis_service import analyze_single_stock_for_batch
from config_manager import config_manager
from database_admin import database_admin
from investment_action_utils import build_analysis_action_payload
from investment_db_utils import DEFAULT_ACCOUNT_NAME
from low_price_bull_monitor import low_price_bull_monitor
from low_price_bull_selector import LowPriceBullSelector
from low_price_bull_service import low_price_bull_service
from low_price_bull_strategy import LowPriceBullStrategy
from main_force_pdf_generator import MainForcePDFGenerator, generate_main_force_markdown_report
from model_config import get_lightweight_model_options, get_reasoning_model_options
from monitor_db import monitor_db
from monitor_service import monitor_service
from monitoring_repository import MonitoringRepository
from main_force_analysis import MainForceAnalyzer
from main_force_batch_db import batch_db as main_force_batch_db
from notification_service import notification_service
from portfolio_analysis_tasks import portfolio_analysis_task_manager
from portfolio_manager import portfolio_manager
from portfolio_scheduler import portfolio_scheduler
from sector_strategy_pdf import SectorStrategyPDFGenerator
from sector_strategy_data import SectorStrategyDataFetcher
from sector_strategy_db import SectorStrategyDatabase
from sector_strategy_engine import SectorStrategyEngine
from sector_strategy_normalization import (
    build_sector_strategy_summary,
    normalize_sector_strategy_export_payload,
    normalize_sector_strategy_result,
)
from sector_strategy_scheduler import sector_strategy_scheduler
from smart_monitor_db import SmartMonitorDB
from stock_data import StockDataFetcher
from strategy_markdown_reports import (
    generate_longhubang_markdown_report,
    generate_sector_markdown_report,
)
from time_utils import local_now_str
from ui_analysis_task_utils import (
    get_active_ui_analysis_task,
    get_latest_ui_analysis_task,
    start_ui_analysis_task,
)

try:
    import pandas as pd
except Exception:  # pragma: no cover - pandas is expected but keep service import resilient
    pd = None


logger = logging.getLogger(__name__)

smart_monitor_db = SmartMonitorDB()
monitoring_repository = MonitoringRepository()
sector_strategy_db = SectorStrategyDatabase()
MAIN_FORCE_SELECTION_TASK_TYPE = "main_force_selection"
MAIN_FORCE_BATCH_TASK_TYPE = "main_force_batch_analysis"
SECTOR_STRATEGY_TASK_TYPE = "sector_strategy_analysis"
LONGHUBANG_TASK_TYPE = "longhubang_analysis"
LONGHUBANG_BATCH_TASK_TYPE = "longhubang_batch_analysis"
LOW_PRICE_BULL_TASK_TYPE = "low_price_bull_selection"
SMALL_CAP_TASK_TYPE = "small_cap_selection"
PROFIT_GROWTH_TASK_TYPE = "profit_growth_selection"
VALUE_STOCK_TASK_TYPE = "value_stock_selection"
MACRO_CYCLE_TASK_TYPE = "macro_cycle_analysis"
NEWS_FLOW_TASK_TYPE = "news_flow_analysis"
PORTFOLIO_SCHEDULER_TASK_TYPE = "batch"
SMART_MONITOR_INTRADAY_INTERVAL_KEY = "smart_monitor_intraday_decision_interval_minutes"
SMART_MONITOR_REALTIME_INTERVAL_KEY = "smart_monitor_realtime_monitor_interval_minutes"


def _clamp_int(value: Any, minimum: int, maximum: int, default: int) -> int:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        numeric = default
    return max(minimum, min(maximum, numeric))


def _default_intraday_decision_interval_minutes() -> int:
    return _clamp_int(getattr(config, "SMART_MONITOR_AI_INTERVAL_MINUTES", 60), 10, 120, 60)


def _default_realtime_monitor_interval_minutes() -> int:
    return _clamp_int(getattr(config, "SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES", 3), 1, 10, 3)


def ensure_runtime_started() -> None:
    monitor_service.ensure_scheduler_state()


def parse_stock_list(stock_input: str) -> list[str]:
    if not stock_input or not stock_input.strip():
        return []
    normalized_input = (
        stock_input.replace("，", ",")
        .replace("；", ",")
        .replace(";", ",")
        .replace("、", ",")
    )
    lines = normalized_input.strip().split("\n")
    stock_list: list[str] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if "," in line:
            stock_list.extend([code.strip() for code in line.split(",") if code.strip()])
        elif " " in line:
            stock_list.extend([code.strip() for code in line.split() if code.strip()])
        else:
            stock_list.append(line)
    seen: set[str] = set()
    unique_list: list[str] = []
    for code in stock_list:
        normalized = str(code or "").strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_list.append(normalized)
    return unique_list


def build_analyst_config(payload: Any) -> dict[str, bool]:
    if hasattr(payload, "model_dump"):
        raw = payload.model_dump()
    elif isinstance(payload, dict):
        raw = payload
    else:
        raw = {}
    return {
        "technical": bool(raw.get("technical", True)),
        "fundamental": bool(raw.get("fundamental", True)),
        "fund_flow": bool(raw.get("fund_flow", True)),
        "risk": bool(raw.get("risk", True)),
        "sentiment": bool(raw.get("sentiment", False)),
        "news": bool(raw.get("news", False)),
    }


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump())
    if pd is not None and isinstance(value, pd.DataFrame):
        return _json_safe(value.to_dict(orient="records"))
    if pd is not None and isinstance(value, pd.Series):
        return _json_safe(value.to_dict())
    try:
        if pd is not None and pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if hasattr(value, "tolist"):
        try:
            return _json_safe(value.tolist())
        except Exception:
            pass
    return str(value)


def _dataframe_from_records(records: Any):
    if pd is None:
        return records
    if isinstance(records, pd.DataFrame):
        return records
    if isinstance(records, list):
        return pd.DataFrame(records)
    return pd.DataFrame([])


def build_task_summary(task: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not task:
        return None
    return {
        "id": task.get("id"),
        "session_id": task.get("session_id"),
        "task_type": task.get("task_type"),
        "label": task.get("label"),
        "status": task.get("status"),
        "message": task.get("message"),
        "current": task.get("current"),
        "total": task.get("total"),
        "progress": task.get("progress"),
        "result": _json_safe(task.get("result")),
        "error": task.get("error"),
        "traceback": task.get("traceback"),
        "metadata": _json_safe(task.get("metadata") or {}),
        "created_at": task.get("created_at"),
        "started_at": task.get("started_at"),
        "finished_at": task.get("finished_at"),
    }


def submit_research_analysis_task(
    *,
    session_key: str,
    symbols: list[str],
    period: str,
    batch_mode: str,
    max_workers: int,
    analysts: dict[str, bool],
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    normalized_symbols = [str(item or "").strip().upper() for item in symbols if str(item or "").strip()]
    if not normalized_symbols:
        raise ValueError("请输入有效的股票代码")
    if not any(analysts.values()):
        raise ValueError("请至少选择一位分析师参与分析")
    if not getattr(config, "DEEPSEEK_API_KEY", ""):
        raise ValueError("请先配置 DeepSeek API Key")

    existing_task = portfolio_analysis_task_manager.get_active_task(session_key)
    if existing_task:
        raise ValueError("当前已有分析任务在执行或排队中，请稍后再试")

    if len(normalized_symbols) == 1:
        symbol = normalized_symbols[0]

        def runner(_task_id: str, report_progress) -> dict[str, Any]:
            report_progress(current=0, total=3, message=f"正在准备 {symbol} 的分析任务...")
            report_progress(current=1, total=3, message=f"AI 分析师团队正在分析 {symbol}...")
            result = analyze_single_stock_for_batch(
                symbol=symbol,
                period=period,
                enabled_analysts_config=analysts,
                selected_lightweight_model=lightweight_model,
                selected_reasoning_model=reasoning_model,
                save_to_global_history=True,
            )
            if not result.get("success"):
                raise RuntimeError(result.get("error") or f"{symbol} 分析失败")
            report_progress(current=3, total=3, message=f"{symbol} 分析完成，正在同步结果...")
            return {
                "mode": "single",
                "symbol": symbol,
                "period": period,
                "stock_info": result.get("stock_info"),
                "indicators": result.get("indicators"),
                "agents_results": result.get("agents_results"),
                "discussion_result": result.get("discussion_result"),
                "final_decision": result.get("final_decision"),
                "record_id": result.get("record_id"),
                "saved_to_db": bool(result.get("saved_to_db")),
                "db_error": result.get("db_error"),
            }

        return portfolio_analysis_task_manager.start_task(
            session_key,
            task_type="home_stock_analysis",
            label=f"深度分析 {symbol}",
            runner=runner,
            metadata={"mode": "single", "symbol": symbol, "period": period, "max_workers": 1},
        )

    total = len(normalized_symbols)
    worker_count = _clamp_int(max_workers, 1, 5, 3)

    def batch_runner(_task_id: str, report_progress) -> dict[str, Any]:
        results_by_symbol: dict[str, dict[str, Any]] = {}
        report_progress(current=0, total=total, message=f"准备分析 {total} 只股票...")

        def analyze_one(symbol: str) -> dict[str, Any]:
            return analyze_single_stock_for_batch(
                symbol=symbol,
                period=period,
                enabled_analysts_config=analysts,
                selected_lightweight_model=lightweight_model,
                selected_reasoning_model=reasoning_model,
                save_to_global_history=True,
            )

        if batch_mode == "多线程并行":
            with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_symbol = {
                    executor.submit(analyze_one, symbol): symbol
                    for symbol in normalized_symbols
                }
                for completed_count, future in enumerate(concurrent.futures.as_completed(future_to_symbol), start=1):
                    symbol = future_to_symbol[future]
                    try:
                        results_by_symbol[symbol] = future.result(timeout=300)
                    except concurrent.futures.TimeoutError:
                        results_by_symbol[symbol] = {
                            "symbol": symbol,
                            "error": "分析超时（5分钟）",
                            "success": False,
                        }
                    except Exception as exc:
                        results_by_symbol[symbol] = {
                            "symbol": symbol,
                            "error": str(exc),
                            "success": False,
                        }
                    current_result = results_by_symbol[symbol]
                    report_progress(
                        current=completed_count,
                        total=total,
                        message=f"[{completed_count}/{total}] {symbol} {'分析完成' if current_result.get('success') else '分析失败'}",
                    )
        else:
            for index, symbol in enumerate(normalized_symbols, start=1):
                results_by_symbol[symbol] = analyze_one(symbol)
                current_result = results_by_symbol[symbol]
                report_progress(
                    current=index,
                    total=total,
                    message=f"[{index}/{total}] {symbol} {'分析完成' if current_result.get('success') else '分析失败'}",
                )

        ordered_results = [results_by_symbol[symbol] for symbol in normalized_symbols if symbol in results_by_symbol]
        success_count = sum(1 for item in ordered_results if item.get("success"))
        saved_count = sum(1 for item in ordered_results if item.get("saved_to_db"))
        return {
            "mode": "batch",
            "results": ordered_results,
            "batch_mode": batch_mode,
            "max_workers": worker_count if batch_mode == "多线程并行" else 1,
            "success_count": success_count,
            "failed_count": total - success_count,
            "saved_count": saved_count,
            "period": period,
        }

    return portfolio_analysis_task_manager.start_task(
        session_key,
        task_type="home_stock_analysis",
        label=f"批量深度分析 {total} 只股票",
        runner=batch_runner,
        metadata={
            "mode": "batch",
            "total": total,
            "batch_mode": batch_mode,
            "max_workers": worker_count if batch_mode == "多线程并行" else 1,
        },
    )


def _selected_analyst_keys(analysts: dict[str, bool]) -> list[str]:
    return [key for key, enabled in (analysts or {}).items() if enabled]


def submit_portfolio_analysis_task(
    *,
    session_key: str,
    account_name: Optional[str],
    period: str,
    batch_mode: str,
    max_workers: int,
    analysts: dict[str, bool],
) -> str:
    normalized_account = str(account_name or "").strip()
    if normalized_account in {"", "全部账户"}:
        normalized_account = ""
    if not any(analysts.values()):
        raise ValueError("请至少选择一位分析师参与分析")
    if not getattr(config, "DEEPSEEK_API_KEY", ""):
        raise ValueError("请先配置 DeepSeek API Key")

    existing_task = portfolio_analysis_task_manager.get_active_task(session_key)
    if existing_task:
        raise ValueError("当前已有分析任务在执行或排队中，请稍后再试")

    selected_analysts = _selected_analyst_keys(analysts)
    worker_count = _clamp_int(max_workers, 1, 5, 3)
    normalized_batch_mode = "多线程并行" if batch_mode == "多线程并行" else "顺序分析"
    portfolio_mode = "parallel" if normalized_batch_mode == "多线程并行" else "sequential"
    account_label = normalized_account or "全部账户"
    stock_count = portfolio_manager.get_stock_count(normalized_account or None)
    if stock_count <= 0:
        raise ValueError(f"{account_label} 当前没有可分析的持仓股")

    def batch_runner(_task_id: str, report_progress) -> dict[str, Any]:
        saved_ids: list[int] = []
        sync_totals = {"added": 0, "updated": 0, "failed": 0, "total": 0}
        report_progress(
            current=0,
            total=0,
            step_status="analyzing",
            message=f"正在准备 {account_label} 的持仓分析任务...",
        )

        def progress_callback(current, total, code, status):
            status_text = {
                "success": "分析完成",
                "failed": "分析失败",
                "error": "分析异常",
            }.get(status, "正在分析")
            report_progress(
                current=int(current or 0),
                total=int(total or 0),
                step_code=code,
                step_status=status,
                message=f"{account_label} | {code} {status_text}",
            )

        def result_callback(code: str, single_result: dict[str, Any]):
            persistence_result = portfolio_manager.persist_single_analysis_result(
                code,
                single_result,
                sync_realtime_monitor=True,
                analysis_source="portfolio_batch_analysis",
                analysis_period=period,
                account_name=normalized_account or None,
            )
            saved_ids.extend(persistence_result.get("saved_ids", []))
            sync_result = persistence_result.get("sync_result") or {}
            for key in sync_totals:
                sync_totals[key] += int(sync_result.get(key, 0) or 0)

        analysis_results = portfolio_manager.batch_analyze_portfolio(
            mode=portfolio_mode,
            period=period,
            selected_agents=selected_analysts,
            max_workers=worker_count,
            progress_callback=progress_callback,
            result_callback=result_callback,
            account_name=normalized_account or None,
        )
        if not analysis_results.get("success"):
            raise RuntimeError(str(analysis_results.get("error") or "持仓分析失败"))

        success_rows = [
            {
                "code": item.get("code"),
                "success": True,
                "account_name": normalized_account or None,
            }
            for item in analysis_results.get("results", [])
        ]
        failed_rows = [
            {
                "code": item.get("code"),
                "success": False,
                "error": item.get("error"),
                "account_name": normalized_account or None,
            }
            for item in analysis_results.get("failed_stocks", [])
        ]
        total = int(analysis_results.get("total") or (len(success_rows) + len(failed_rows)))
        success_count = int(analysis_results.get("succeeded") or len(success_rows))
        failed_count = int(analysis_results.get("failed") or len(failed_rows))
        report_progress(
            current=total,
            total=total or 1,
            step_status="success",
            message=f"{account_label} 持仓分析完成：成功 {success_count}，失败 {failed_count}，已写入 {len(saved_ids)} 条历史",
        )
        return {
            "mode": "batch",
            "account_name": normalized_account or None,
            "results": success_rows + failed_rows,
            "batch_mode": normalized_batch_mode,
            "max_workers": worker_count if normalized_batch_mode == "多线程并行" else 1,
            "success_count": success_count,
            "failed_count": failed_count,
            "saved_count": len(saved_ids),
            "period": period,
            "persistence_result": {
                "saved_ids": list(saved_ids),
                "sync_result": sync_totals,
            },
        }

    label = f"{account_label}持仓批量分析" if normalized_account else "全部账户持仓批量分析"
    return portfolio_analysis_task_manager.start_task(
        session_key,
        task_type="portfolio_holdings_analysis",
        label=label,
        runner=batch_runner,
        metadata={
            "mode": "batch",
            "account_name": normalized_account or None,
            "stock_count": stock_count,
            "batch_mode": normalized_batch_mode,
            "max_workers": worker_count if normalized_batch_mode == "多线程并行" else 1,
            "selected_agents": selected_analysts,
        },
    )


def get_latest_task_for_session(session_key: str) -> Optional[dict[str, Any]]:
    portfolio_analysis_task_manager.prune_session_tasks(session_key)
    return build_task_summary(portfolio_analysis_task_manager.get_latest_task(session_key))


def get_active_task_for_session(session_key: str) -> Optional[dict[str, Any]]:
    portfolio_analysis_task_manager.prune_session_tasks(session_key)
    return build_task_summary(portfolio_analysis_task_manager.get_active_task(session_key))


def get_task_for_session(session_key: str, task_id: str) -> Optional[dict[str, Any]]:
    task = portfolio_analysis_task_manager.get_task(task_id)
    if not task or task.get("session_id") != session_key:
        return None
    return build_task_summary(task)


def get_latest_ui_task(task_type: str) -> Optional[dict[str, Any]]:
    return build_task_summary(get_latest_ui_analysis_task(task_type))


def get_active_ui_task(task_type: str) -> Optional[dict[str, Any]]:
    return build_task_summary(get_active_ui_analysis_task(task_type))


def get_ui_task(task_type: str, task_id: str) -> Optional[dict[str, Any]]:
    task = portfolio_analysis_task_manager.get_task(task_id)
    if not task or task.get("task_type") != task_type:
        return None
    return build_task_summary(task)


def _parse_main_force_start_date(raw_value: Optional[str]) -> Optional[str]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            parsed = datetime.strptime(value, fmt)
            return f"{parsed.year}年{parsed.month}月{parsed.day}日"
        except ValueError:
            continue
    return value


def submit_main_force_selection_task(
    *,
    days_ago: Optional[int],
    start_date: Optional[str],
    final_n: int,
    max_change: float,
    min_cap: float,
    max_cap: float,
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    if not getattr(config, "DEEPSEEK_API_KEY", ""):
        raise ValueError("请先配置 DeepSeek API Key")
    parsed_start_date = _parse_main_force_start_date(start_date)
    normalized_days_ago = None if parsed_start_date else int(days_ago or 90)

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=0, total=3, message="正在准备主力资金数据...")
        analyzer = MainForceAnalyzer(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        report_progress(current=1, total=3, message="正在执行主力筛选与 AI 分析...")
        result = analyzer.run_full_analysis(
            start_date=parsed_start_date,
            days_ago=normalized_days_ago,
            final_n=int(final_n),
            max_range_change=float(max_change),
            min_market_cap=float(min_cap),
            max_market_cap=float(max_cap),
        )
        if not result.get("success"):
            raise RuntimeError(result.get("error") or "主力选股分析失败")

        context_snapshot = {
            "raw_stocks": _json_safe(getattr(analyzer, "raw_stocks", None)),
            "fund_flow_analysis": getattr(analyzer, "fund_flow_analysis", ""),
            "industry_analysis": getattr(analyzer, "industry_analysis", ""),
            "fundamental_analysis": getattr(analyzer, "fundamental_analysis", ""),
        }
        recommendation_count = len(result.get("final_recommendations") or [])
        report_progress(
            current=3,
            total=3,
            message=f"主力选股完成，共筛选出 {recommendation_count} 只优质标的。",
        )
        return {
            "result": _json_safe(result),
            "context_snapshot": context_snapshot,
            "message": f"分析完成，共筛选出 {recommendation_count} 只优质标的。",
        }

    return start_ui_analysis_task(
        task_type=MAIN_FORCE_SELECTION_TASK_TYPE,
        label="主力选股分析",
        runner=runner,
        metadata={
            "days_ago": normalized_days_ago,
            "start_date": parsed_start_date,
            "final_n": int(final_n),
            "max_change": float(max_change),
            "min_cap": float(min_cap),
            "max_cap": float(max_cap),
        },
    )


def submit_main_force_batch_task(
    *,
    symbols: list[str],
    analysis_mode: str,
    max_workers: int,
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    normalized_symbols = [str(item or "").strip().upper() for item in symbols if str(item or "").strip()]
    if not normalized_symbols:
        raise ValueError("请先提供需要批量分析的股票代码")
    if not getattr(config, "DEEPSEEK_API_KEY", ""):
        raise ValueError("请先配置 DeepSeek API Key")

    normalized_mode = "parallel" if analysis_mode == "parallel" else "sequential"
    worker_count = max(1, min(int(max_workers or 1), 5))
    enabled_analysts_config = {
        "technical": True,
        "fundamental": True,
        "fund_flow": True,
        "risk": True,
        "sentiment": False,
        "news": False,
    }

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        start_ts = time.time()
        results_by_symbol: dict[str, dict[str, Any]] = {}
        total = len(normalized_symbols)
        report_progress(current=0, total=total, message=f"准备分析 {total} 只股票...")

        def analyze_one(symbol: str) -> dict[str, Any]:
            return analyze_single_stock_for_batch(
                symbol=symbol,
                period="1y",
                enabled_analysts_config=enabled_analysts_config,
                selected_lightweight_model=lightweight_model,
                selected_reasoning_model=reasoning_model,
                save_to_global_history=False,
            )

        if normalized_mode == "parallel":
            with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_symbol = {
                    executor.submit(analyze_one, symbol): symbol for symbol in normalized_symbols
                }
                for completed_count, future in enumerate(concurrent.futures.as_completed(future_to_symbol), start=1):
                    symbol = future_to_symbol[future]
                    try:
                        results_by_symbol[symbol] = future.result(timeout=300)
                    except concurrent.futures.TimeoutError:
                        results_by_symbol[symbol] = {"symbol": symbol, "success": False, "error": "分析超时（5分钟）"}
                    except Exception as exc:
                        results_by_symbol[symbol] = {"symbol": symbol, "success": False, "error": str(exc)}
                    current_result = results_by_symbol[symbol]
                    report_progress(
                        current=completed_count,
                        total=total,
                        message=f"[{completed_count}/{total}] {symbol} {'分析完成' if current_result.get('success') else '分析失败'}",
                    )
        else:
            for index, symbol in enumerate(normalized_symbols, start=1):
                results_by_symbol[symbol] = analyze_one(symbol)
                current_result = results_by_symbol[symbol]
                report_progress(
                    current=index,
                    total=total,
                    message=f"[{index}/{total}] {symbol} {'分析完成' if current_result.get('success') else '分析失败'}",
                )

        ordered_results = [results_by_symbol[symbol] for symbol in normalized_symbols if symbol in results_by_symbol]
        success_count = sum(1 for item in ordered_results if item.get("success"))
        failed_count = total - success_count
        elapsed_time = time.time() - start_ts
        save_success = False
        save_error = None
        record_id = None
        try:
            record_id = main_force_batch_db.save_batch_analysis(
                batch_count=total,
                analysis_mode=normalized_mode,
                success_count=success_count,
                failed_count=failed_count,
                total_time=elapsed_time,
                results=ordered_results,
            )
            save_success = True
        except Exception as exc:
            save_error = str(exc)

        return {
            "results": _json_safe(ordered_results),
            "total": total,
            "success": success_count,
            "failed": failed_count,
            "elapsed_time": elapsed_time,
            "analysis_mode": normalized_mode,
            "analysis_date": local_now_str(),
            "saved_to_history": save_success,
            "save_error": save_error,
            "history_record_id": record_id,
        }

    return start_ui_analysis_task(
        task_type=MAIN_FORCE_BATCH_TASK_TYPE,
        label=f"主力 TOP 批量分析 {len(normalized_symbols)} 只股票",
        runner=runner,
        metadata={
            "symbols": normalized_symbols,
            "analysis_mode": normalized_mode,
            "max_workers": worker_count,
        },
    )


def _build_main_force_batch_results(record: dict[str, Any]) -> dict[str, Any]:
    results = list(record.get("results") or [])
    batch_count = int(record.get("batch_count") or len(results))
    success_count = int(record.get("success_count") or sum(1 for item in results if item.get("success")))
    failed_count = int(record.get("failed_count") or max(batch_count - success_count, 0))
    return {
        "results": _json_safe(results),
        "total": batch_count,
        "success": success_count,
        "failed": failed_count,
        "elapsed_time": float(record.get("total_time") or 0),
        "analysis_mode": record.get("analysis_mode") or "unknown",
        "analysis_date": record.get("analysis_date") or "",
        "saved_to_history": True,
        "history_record_id": record.get("id"),
    }


def _summarize_main_force_batch_record(record: dict[str, Any]) -> str:
    successful_results = [item for item in (record.get("results") or []) if item.get("success")]
    highlighted: list[str] = []
    for item in successful_results[:3]:
        stock_info = item.get("stock_info") or {}
        final_decision = item.get("final_decision") or {}
        symbol = item.get("symbol") or stock_info.get("symbol") or "-"
        name = stock_info.get("name") or stock_info.get("股票名称") or symbol
        rating = final_decision.get("rating") or final_decision.get("investment_rating")
        highlighted.append(f"{name}({symbol}){f' {rating}' if rating else ''}")
    summary_parts = [
        f"成功 {record.get('success_count', 0)}/{record.get('batch_count', 0)} 只",
        f"模式 {record.get('analysis_mode') or '-'}",
    ]
    if highlighted:
        summary_parts.append("重点: " + "、".join(highlighted))
    return " | ".join(summary_parts)


def list_main_force_history(limit: int = 50) -> dict[str, Any]:
    records = main_force_batch_db.get_all_history(limit=limit)
    return {
        "stats": _json_safe(main_force_batch_db.get_statistics()),
        "records": [
            {
                **_json_safe(record),
                "summary": _summarize_main_force_batch_record(record),
                "batch_results": _build_main_force_batch_results(record),
            }
            for record in records
        ],
    }


def get_main_force_history_record(record_id: int) -> Optional[dict[str, Any]]:
    record = main_force_batch_db.get_record_by_id(record_id)
    if not record:
        return None
    return {
        **_json_safe(record),
        "summary": _summarize_main_force_batch_record(record),
        "batch_results": _build_main_force_batch_results(record),
    }


def delete_main_force_history_record(record_id: int) -> bool:
    return bool(main_force_batch_db.delete_record(record_id))


def submit_sector_strategy_task(
    *,
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    if not getattr(config, "DEEPSEEK_API_KEY", ""):
        raise ValueError("请先配置 DeepSeek API Key")

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=0, total=3, message="正在获取市场数据...")
        fetcher = SectorStrategyDataFetcher()
        data = fetcher.get_cached_data_with_fallback()
        if not data.get("success"):
            raise RuntimeError(data.get("error") or "数据获取失败")

        report_progress(current=1, total=3, message="市场数据获取完成，正在执行 AI 分析...")
        engine = SectorStrategyEngine(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        result = engine.run_comprehensive_analysis(data)
        if data.get("from_cache") or data.get("cache_warning"):
            result["cache_meta"] = {
                "from_cache": bool(data.get("from_cache")),
                "cache_warning": data.get("cache_warning", ""),
                "data_timestamp": data.get("timestamp"),
            }
        if not result.get("success"):
            raise RuntimeError(result.get("error") or "智策分析失败")

        report_progress(current=3, total=3, message="智策分析完成，正在同步结果...")
        data_summary = {
            "from_cache": bool(data.get("from_cache")),
            "cache_warning": data.get("cache_warning", ""),
            "data_timestamp": data.get("timestamp"),
            "market_overview": data.get("market_overview", {}),
            "sectors": data.get("sectors", {}) or {},
            "concepts": data.get("concepts", {}) or {},
        }
        return {
            "result": _json_safe(result),
            "report_view": _json_safe(normalize_sector_strategy_result(result, data_summary=data_summary)),
            "data_summary": _json_safe(data_summary),
            "message": "智策分析完成。",
        }

    return start_ui_analysis_task(
        task_type=SECTOR_STRATEGY_TASK_TYPE,
        label="智策分析",
        runner=runner,
        metadata={},
    )


def _extract_sector_strategy_summary(source: dict[str, Any]) -> dict[str, Any]:
    payload = source or {}
    if (
        isinstance(payload.get("analysis_content_parsed"), dict)
        or isinstance(payload.get("final_predictions"), dict)
        or (isinstance(payload.get("summary"), dict) and "predictions" in payload)
    ):
        return build_sector_strategy_summary(normalize_sector_strategy_result(source))
    return {
        "headline": payload.get("summary") or "智策板块分析报告",
        "market_view": payload.get("summary") or "暂无",
        "key_opportunity": "暂无",
        "major_risk": "暂无",
        "strategy": "暂无",
        "bullish": [],
        "neutral": [],
        "bearish": [],
        "risk_level": payload.get("risk_level") or "中等",
        "market_outlook": payload.get("market_outlook") or "中性",
        "confidence_score": int(payload.get("confidence_score") or 0),
    }


def list_sector_strategy_reports(limit: int = 20) -> list[dict[str, Any]]:
    reports_df = sector_strategy_db.get_analysis_reports(limit=limit)
    if pd is not None and isinstance(reports_df, pd.DataFrame):
        reports = reports_df.to_dict(orient="records")
    else:
        reports = list(reports_df or [])
    return [
        {
            **_json_safe(report),
            "summary_data": _extract_sector_strategy_summary(report),
        }
        for report in reports
    ]


def get_sector_strategy_report(report_id: int) -> Optional[dict[str, Any]]:
    report = sector_strategy_db.get_analysis_report(report_id)
    if not report:
        return None
    report_view = normalize_sector_strategy_result(report)
    return {
        **_json_safe(report),
        "summary_data": _extract_sector_strategy_summary(report_view),
        "report_view": _json_safe(report_view),
    }


def delete_sector_strategy_report(report_id: int) -> bool:
    return bool(sector_strategy_db.delete_analysis_report(report_id))


def get_sector_strategy_scheduler_status() -> dict[str, Any]:
    notification_service.__init__()
    config_payload = notification_service.config
    return {
        **_json_safe(sector_strategy_scheduler.get_status()),
        "email_config": {
            "enabled": bool(config_payload.get("email_enabled")),
            "smtp_server": config_payload.get("smtp_server") or "",
            "email_from": config_payload.get("email_from") or "",
            "email_to": config_payload.get("email_to") or "",
            "password_configured": bool(config_payload.get("email_password")),
            "configured": bool(
                config_payload.get("email_enabled")
                and config_payload.get("smtp_server")
                and config_payload.get("email_from")
                and config_payload.get("email_password")
                and config_payload.get("email_to")
            ),
        },
    }


def update_sector_strategy_scheduler(*, schedule_time: str, enabled: bool) -> dict[str, Any]:
    normalized_time = str(schedule_time or "").strip() or "09:00"
    if enabled:
        if sector_strategy_scheduler.get_status().get("running"):
            sector_strategy_scheduler.stop()
        if not sector_strategy_scheduler.start(normalized_time):
            raise ValueError("启动智策定时任务失败")
    elif sector_strategy_scheduler.get_status().get("running"):
        if not sector_strategy_scheduler.stop():
            raise ValueError("停止智策定时任务失败")
    return get_sector_strategy_scheduler_status()


def run_sector_strategy_scheduler_once() -> dict[str, Any]:
    if not sector_strategy_scheduler.manual_run():
        raise ValueError("后台任务提交失败，请稍后重试")
    latest_task = get_latest_ui_task(SECTOR_STRATEGY_TASK_TYPE)
    return {
        "submitted": True,
        "task": latest_task,
    }


def test_email_notification() -> tuple[bool, str]:
    notification_service.__init__()
    return notification_service.send_test_email()


def _create_longhubang_engine(
    *,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
):
    from longhubang_engine import LonghubangEngine

    return LonghubangEngine(
        lightweight_model=lightweight_model,
        reasoning_model=reasoning_model,
    )


@lru_cache(maxsize=1)
def _create_longhubang_database():
    from longhubang_db import LonghubangDatabase

    return LonghubangDatabase()


def _normalize_longhubang_result(
    payload: Optional[dict[str, Any]],
    *,
    recommended_stocks: Optional[list[dict[str, Any]]] = None,
    timestamp: Optional[str] = None,
) -> dict[str, Any]:
    source = payload or {}
    return {
        "success": bool(source.get("success", True)),
        "timestamp": timestamp or source.get("timestamp") or local_now_str(),
        "data_info": _json_safe(source.get("data_info") or {}),
        "agents_analysis": _json_safe(source.get("agents_analysis") or {}),
        "scoring_ranking": _json_safe(source.get("scoring_ranking") or []),
        "final_report": _json_safe(source.get("final_report") or {}),
        "recommended_stocks": _json_safe(
            recommended_stocks if recommended_stocks is not None else source.get("recommended_stocks") or []
        ),
        "error": source.get("error"),
        "report_id": source.get("report_id"),
    }


def _extract_longhubang_report_summary(report: Optional[dict[str, Any]]) -> dict[str, Any]:
    payload = report or {}
    parsed = payload.get("analysis_content_parsed")
    if not isinstance(parsed, dict):
        parsed = {}
    recommended = payload.get("recommended_stocks")
    if not isinstance(recommended, list):
        recommended = []
    data_info = parsed.get("data_info", {}) if isinstance(parsed, dict) else {}
    summary = data_info.get("summary", {}) if isinstance(data_info, dict) else {}
    hot_concepts = summary.get("hot_concepts", {}) if isinstance(summary, dict) else {}
    top_stocks = summary.get("top_stocks", []) if isinstance(summary, dict) else []
    concept_names = []
    if isinstance(hot_concepts, dict):
        concept_names = [str(name) for name in list(hot_concepts.keys())[:3]]
    stock_names = [
        str(item.get("name"))
        for item in top_stocks[:3]
        if isinstance(item, dict) and item.get("name")
    ]
    return {
        "recommended_count": len(recommended),
        "total_records": data_info.get("total_records", 0),
        "total_stocks": data_info.get("total_stocks", 0),
        "total_youzi": data_info.get("total_youzi", 0),
        "top_concepts": concept_names,
        "top_stocks": stock_names,
    }


def submit_longhubang_task(
    *,
    date_value: Optional[str],
    days: int,
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    if not getattr(config, "DEEPSEEK_API_KEY", ""):
        raise ValueError("请先配置 DeepSeek API Key")
    normalized_date = str(date_value or "").strip() or None
    normalized_days = max(1, min(int(days or 1), 10))

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=0, total=3, message="正在初始化龙虎榜分析引擎...")
        engine = _create_longhubang_engine(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        report_progress(current=1, total=3, message="正在获取龙虎榜数据并执行多智能体分析...")
        result = engine.run_comprehensive_analysis(date=normalized_date, days=normalized_days)
        if not result.get("success"):
            raise RuntimeError(result.get("error") or "龙虎榜分析失败")
        report_progress(current=3, total=3, message="龙虎榜分析完成，正在同步结果...")
        return {
            "result": _json_safe(result),
            "message": "龙虎榜分析完成。",
        }

    return start_ui_analysis_task(
        task_type=LONGHUBANG_TASK_TYPE,
        label="龙虎榜分析",
        runner=runner,
        metadata={
            "date": normalized_date,
            "days": normalized_days,
            "mode": "指定日期" if normalized_date else "最近N天",
        },
    )


def submit_longhubang_batch_task(
    *,
    symbols: list[str],
    analysis_mode: str,
    max_workers: int,
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    normalized_symbols = [str(item or "").strip().upper() for item in symbols if str(item or "").strip()]
    if not normalized_symbols:
        raise ValueError("请先提供需要批量分析的股票代码")
    if not getattr(config, "DEEPSEEK_API_KEY", ""):
        raise ValueError("请先配置 DeepSeek API Key")

    normalized_mode = "parallel" if analysis_mode == "parallel" else "sequential"
    worker_count = max(1, min(int(max_workers or 1), 5))
    enabled_analysts_config = {
        "technical": True,
        "fundamental": True,
        "fund_flow": True,
        "risk": True,
        "sentiment": False,
        "news": False,
    }

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        start_ts = time.time()
        results_by_symbol: dict[str, dict[str, Any]] = {}
        total = len(normalized_symbols)
        report_progress(current=0, total=total, message=f"准备分析 {total} 只龙虎榜股票...")

        def analyze_one(symbol: str) -> dict[str, Any]:
            return analyze_single_stock_for_batch(
                symbol=symbol,
                period="1y",
                enabled_analysts_config=enabled_analysts_config,
                selected_lightweight_model=lightweight_model,
                selected_reasoning_model=reasoning_model,
                save_to_global_history=True,
            )

        if normalized_mode == "parallel":
            with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_symbol = {
                    executor.submit(analyze_one, symbol): symbol for symbol in normalized_symbols
                }
                for completed_count, future in enumerate(concurrent.futures.as_completed(future_to_symbol), start=1):
                    symbol = future_to_symbol[future]
                    try:
                        results_by_symbol[symbol] = future.result(timeout=300)
                    except concurrent.futures.TimeoutError:
                        results_by_symbol[symbol] = {"symbol": symbol, "success": False, "error": "分析超时（5分钟）"}
                    except Exception as exc:
                        results_by_symbol[symbol] = {"symbol": symbol, "success": False, "error": str(exc)}
                    current_result = results_by_symbol[symbol]
                    report_progress(
                        current=completed_count,
                        total=total,
                        message=f"[{completed_count}/{total}] {symbol} {'分析完成' if current_result.get('success') else '分析失败'}",
                    )
        else:
            for index, symbol in enumerate(normalized_symbols, start=1):
                results_by_symbol[symbol] = analyze_one(symbol)
                current_result = results_by_symbol[symbol]
                report_progress(
                    current=index,
                    total=total,
                    message=f"[{index}/{total}] {symbol} {'分析完成' if current_result.get('success') else '分析失败'}",
                )

        ordered_results = [results_by_symbol[symbol] for symbol in normalized_symbols if symbol in results_by_symbol]
        success_count = sum(1 for item in ordered_results if item.get("success"))
        elapsed_time = time.time() - start_ts
        return {
            "results": _json_safe(ordered_results),
            "total": total,
            "success": success_count,
            "failed": total - success_count,
            "elapsed_time": elapsed_time,
            "analysis_mode": normalized_mode,
            "analysis_date": local_now_str(),
        }

    return start_ui_analysis_task(
        task_type=LONGHUBANG_BATCH_TASK_TYPE,
        label=f"龙虎榜 TOP 批量分析 {len(normalized_symbols)} 只股票",
        runner=runner,
        metadata={
            "symbols": normalized_symbols,
            "analysis_mode": normalized_mode,
            "max_workers": worker_count,
        },
    )


def list_longhubang_reports(limit: int = 50) -> list[dict[str, Any]]:
    database = _create_longhubang_database()
    reports_df = database.get_analysis_reports(limit=limit)
    if pd is not None and isinstance(reports_df, pd.DataFrame):
        reports = reports_df.to_dict(orient="records")
    else:
        reports = list(reports_df or [])
    return [_json_safe(report) for report in reports]


def get_longhubang_report(report_id: int) -> Optional[dict[str, Any]]:
    database = _create_longhubang_database()
    report = database.get_analysis_report(report_id)
    if not report:
        return None
    parsed = report.get("analysis_content_parsed")
    recommended = report.get("recommended_stocks")
    result_payload = _normalize_longhubang_result(
        parsed if isinstance(parsed, dict) else None,
        recommended_stocks=recommended if isinstance(recommended, list) else None,
        timestamp=report.get("analysis_date"),
    )
    return {
        **_json_safe(report),
        "summary_data": _extract_longhubang_report_summary(report),
        "result_payload": result_payload,
    }


def delete_longhubang_report(report_id: int) -> bool:
    database = _create_longhubang_database()
    return bool(database.delete_analysis_report(report_id))


def get_longhubang_statistics(window_days: int = 30) -> dict[str, Any]:
    database = _create_longhubang_database()
    normalized_days = max(1, int(window_days or 30))
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=normalized_days)).strftime("%Y-%m-%d")
    return {
        "stats": _json_safe(database.get_statistics()),
        "window_days": normalized_days,
        "top_youzi": _json_safe(database.get_top_youzi(start_date, end_date, limit=20)),
        "top_stocks": _json_safe(database.get_top_stocks(start_date, end_date, limit=20)),
    }


def _build_main_force_export_context(context_snapshot: Optional[dict[str, Any]]) -> SimpleNamespace:
    snapshot = context_snapshot or {}
    raw_stocks = snapshot.get("raw_stocks")
    return SimpleNamespace(
        raw_stocks=_dataframe_from_records(raw_stocks),
        fund_flow_analysis=snapshot.get("fund_flow_analysis", ""),
        industry_analysis=snapshot.get("industry_analysis", ""),
        fundamental_analysis=snapshot.get("fundamental_analysis", ""),
    )


def export_main_force_markdown(
    result: dict[str, Any],
    context_snapshot: Optional[dict[str, Any]] = None,
) -> tuple[bytes, str, str]:
    analyzer = _build_main_force_export_context(context_snapshot)
    normalized_result = dict(result or {})
    normalized_result.setdefault("total_fetched", normalized_result.get("total_stocks", 0))
    normalized_result.setdefault("filtered_count", normalized_result.get("filtered_stocks", 0))
    markdown = generate_main_force_markdown_report(analyzer, normalized_result)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return markdown.encode("utf-8"), f"主力选股分析报告_{timestamp}.md", "text/markdown; charset=utf-8"


def export_main_force_pdf(
    result: dict[str, Any],
    context_snapshot: Optional[dict[str, Any]] = None,
) -> tuple[bytes, str, str]:
    analyzer = _build_main_force_export_context(context_snapshot)
    normalized_result = dict(result or {})
    normalized_result.setdefault("total_fetched", normalized_result.get("total_stocks", 0))
    normalized_result.setdefault("filtered_count", normalized_result.get("filtered_stocks", 0))
    generator = MainForcePDFGenerator()
    pdf_path = generator.generate_pdf(analyzer, normalized_result)
    with open(pdf_path, "rb") as file_obj:
        data = file_obj.read()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return data, f"主力选股分析报告_{timestamp}.pdf", "application/pdf"


def export_sector_strategy_markdown(result: dict[str, Any]) -> tuple[bytes, str, str]:
    markdown = generate_sector_markdown_report(normalize_sector_strategy_export_payload(result or {}))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return markdown.encode("utf-8"), f"智策报告_{timestamp}.md", "text/markdown; charset=utf-8"


def export_sector_strategy_pdf(result: dict[str, Any]) -> tuple[bytes, str, str]:
    generator = SectorStrategyPDFGenerator()
    pdf_path = generator.generate_pdf(normalize_sector_strategy_export_payload(result or {}))
    with open(pdf_path, "rb") as file_obj:
        data = file_obj.read()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return data, f"智策报告_{timestamp}.pdf", "application/pdf"


def export_longhubang_markdown(result: dict[str, Any]) -> tuple[bytes, str, str]:
    markdown = generate_longhubang_markdown_report(result or {})
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return markdown.encode("utf-8"), f"智瞰龙虎报告_{timestamp}.md", "text/markdown; charset=utf-8"


def export_longhubang_pdf(result: dict[str, Any]) -> tuple[bytes, str, str]:
    from longhubang_pdf import LonghubangPDFGenerator

    generator = LonghubangPDFGenerator()
    pdf_path = generator.generate_pdf(result or {})
    with open(pdf_path, "rb") as file_obj:
        data = file_obj.read()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return data, f"智瞰龙虎报告_{timestamp}.pdf", "application/pdf"


def _send_low_price_bull_notification(records: list[dict[str, Any]], top_n: int, filter_summary: str) -> None:
    try:
        notification_service.__init__()
        webhook_config = notification_service.get_webhook_config_status()
        if not webhook_config.get("enabled") or not webhook_config.get("configured"):
            return
        if notification_service.config.get("webhook_type") != "dingtalk":
            return
        import requests

        keyword = notification_service.config.get("webhook_keyword", "aiagents通知")
        message_text = f"### {keyword} - 低价擒牛选股完成\n\n"
        if filter_summary:
            message_text += f"**筛选条件**: {filter_summary}\n\n"
        message_text += f"**筛选数量**: {len(records)} 只\n\n"
        message_text += "**精选股票**:\n\n"
        for index, row in enumerate(records[:top_n], start=1):
            code = row.get("股票代码", "")
            if isinstance(code, str) and "." in code:
                code = code.split(".")[0]
            name = row.get("股票简称", "")
            message_text += f"{index}. **{code} {name}**\n"
            price = row.get("股价", row.get("最新价"))
            if price not in (None, "", "N/A"):
                message_text += f"   - 股价: {price}\n"
            growth = row.get("净利润增长率", row.get("净利润同比增长率"))
            if growth not in (None, "", "N/A"):
                message_text += f"   - 净利增长: {growth}%\n"
            turnover = row.get("成交额")
            if turnover not in (None, "", "N/A"):
                message_text += f"   - 成交额: {turnover}\n"
            industry = row.get("所属行业", row.get("所属同花顺行业"))
            if industry not in (None, "", "N/A"):
                message_text += f"   - 所属行业: {industry}\n"
            message_text += "\n"
        message_text += f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message_text += "_此消息由AI股票分析系统自动发送_"
        requests.post(
            notification_service.config["webhook_url"],
            json={
                "msgtype": "markdown",
                "markdown": {
                    "title": keyword,
                    "text": message_text,
                },
            },
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
    except Exception:
        logger.exception("Failed to send low price bull webhook notification")


def submit_low_price_bull_selection_task(
    *,
    top_n: int,
    max_price: float,
    min_profit_growth: float,
    min_turnover_yi: float,
    max_turnover_yi: float,
    min_market_cap_yi: float,
    max_market_cap_yi: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
    only_hs_a: bool,
    filter_summary: str,
) -> str:
    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=0, total=2, message="正在拉取低价擒牛候选数据...")
        selector = LowPriceBullSelector()
        success, stocks_df, message = selector.get_low_price_stocks(
            top_n=int(top_n),
            max_price=float(max_price),
            min_profit_growth=float(min_profit_growth),
            min_turnover_yi=float(min_turnover_yi) or None,
            max_turnover_yi=float(max_turnover_yi) or None,
            min_market_cap_yi=float(min_market_cap_yi) or None,
            max_market_cap_yi=float(max_market_cap_yi) or None,
            sort_by=sort_by,
            exclude_st=bool(exclude_st),
            exclude_kcb=bool(exclude_kcb),
            exclude_cyb=bool(exclude_cyb),
            only_hs_a=bool(only_hs_a),
        )
        if not success or stocks_df is None:
            raise RuntimeError(message or "低价擒牛选股失败")
        records = _json_safe(stocks_df)
        _send_low_price_bull_notification(records if isinstance(records, list) else [], int(top_n), filter_summary)
        report_progress(current=2, total=2, message="低价擒牛选股完成，正在同步结果...")
        return {
            "stocks": records,
            "message": message,
            "filter_summary": filter_summary,
            "selected_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    return start_ui_analysis_task(
        task_type=LOW_PRICE_BULL_TASK_TYPE,
        label="低价擒牛选股",
        runner=runner,
        metadata={"top_n": int(top_n), "filter_summary": filter_summary},
    )


def get_low_price_bull_status() -> dict[str, Any]:
    return _json_safe(low_price_bull_service.get_status())


def update_low_price_bull_scan_interval(scan_interval: int) -> dict[str, Any]:
    low_price_bull_service.scan_interval = max(10, int(scan_interval))
    return get_low_price_bull_status()


def start_low_price_bull_monitor() -> dict[str, Any]:
    if not low_price_bull_service.start():
        raise ValueError("监控服务启动失败")
    return get_low_price_bull_status()


def stop_low_price_bull_monitor() -> dict[str, Any]:
    if not low_price_bull_service.stop():
        raise ValueError("监控服务停止失败")
    return get_low_price_bull_status()


def list_low_price_bull_monitored_stocks() -> list[dict[str, Any]]:
    return _json_safe(low_price_bull_monitor.get_monitored_stocks())


def add_low_price_bull_monitored_stock(
    *,
    stock_code: str,
    stock_name: str,
    buy_price: float,
    buy_date: Optional[str] = None,
) -> tuple[bool, str]:
    return low_price_bull_monitor.add_stock(
        stock_code=str(stock_code or "").strip().upper(),
        stock_name=str(stock_name or "").strip(),
        buy_price=float(buy_price or 0),
        buy_date=buy_date or None,
    )


def remove_low_price_bull_monitored_stock(stock_code: str, reason: str = "手动移除") -> tuple[bool, str]:
    return low_price_bull_monitor.remove_stock(str(stock_code or "").strip().upper(), reason)


def list_low_price_bull_alerts(*, history: bool = False, limit: int = 50) -> list[dict[str, Any]]:
    if history:
        return _json_safe(low_price_bull_monitor.get_history_alerts(limit=limit))
    return _json_safe(low_price_bull_monitor.get_pending_alerts())


def resolve_low_price_bull_alert(alert_id: int, status: str) -> tuple[bool, str]:
    alerts = low_price_bull_monitor.get_pending_alerts()
    alert = next((item for item in alerts if int(item.get("id") or 0) == int(alert_id)), None)
    if not alert:
        return False, "未找到待处理提醒"
    low_price_bull_monitor.mark_alert_sent(int(alert_id))
    if status == "done":
        low_price_bull_monitor.remove_stock(str(alert.get("stock_code") or "").strip().upper(), "已处理提醒")
        return True, "已标记为已处理并移出监控列表"
    return True, "已忽略提醒（股票保留在监控列表）"


def cleanup_low_price_bull_alerts(days: int = 30) -> dict[str, Any]:
    low_price_bull_monitor.clear_old_alerts(days=max(1, int(days)))
    return {"cleaned": True, "days": max(1, int(days))}


def simulate_low_price_bull_strategy(stocks: list[dict[str, Any]]) -> dict[str, Any]:
    strategy = LowPriceBullStrategy(initial_capital=1000000.0)
    buy_results: list[dict[str, Any]] = []
    current_date = datetime.now().strftime("%Y-%m-%d")
    for row in stocks[: strategy.max_daily_buy]:
        code = str(row.get("股票代码") or "").split(".")[0]
        name = str(row.get("股票简称") or row.get("股票名称") or "N/A")
        price = row.get("股价", row.get("最新价", 0))
        try:
            price_value = float(price)
        except (TypeError, ValueError):
            price_value = 0.0
        if not code or price_value <= 0:
            continue
        success, message, trade = strategy.buy(code, name, price_value, current_date)
        buy_results.append(
            {
                "success": success,
                "message": message,
                "trade": _json_safe(trade),
            }
        )
    return {
        "buy_results": buy_results,
        "positions": _json_safe(strategy.get_positions()),
        "summary": _json_safe(strategy.get_portfolio_summary()),
        "trade_history": _json_safe(strategy.get_trade_history()),
    }


def _send_selector_notification(
    *,
    strategy_name: str,
    records: list[dict[str, Any]],
    filter_summary: str = "",
) -> tuple[bool, str]:
    try:
        notification_service.__init__()
        webhook_config = notification_service.get_webhook_config_status()
        if not webhook_config.get("enabled"):
            return False, "Webhook通知未启用，请在系统配置中启用。"
        if not webhook_config.get("configured"):
            return False, "Webhook通知未配置完整。"
        if notification_service.config.get("webhook_type") != "dingtalk":
            return False, "当前仅支持钉钉Webhook通知。"

        import requests

        keyword = notification_service.config.get("webhook_keyword", "aiagents通知")
        message_text = f"### {keyword} - {strategy_name}选股完成\n\n"
        if filter_summary:
            message_text += f"**筛选策略**: {filter_summary}\n\n"
        message_text += f"**筛选数量**: {len(records)} 只\n\n"
        message_text += "**精选股票**:\n\n"
        for index, row in enumerate(records[:20], start=1):
            code = str(row.get("股票代码") or row.get("code") or "").strip()
            if "." in code:
                code = code.split(".")[0]
            name = str(row.get("股票简称") or row.get("股票名称") or row.get("name") or "").strip()
            message_text += f"{index}. {code} {name}\n\n"
        message_text += f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message_text += "_此消息由AI股票分析系统自动发送_"
        response = requests.post(
            notification_service.config["webhook_url"],
            json={
                "msgtype": "markdown",
                "markdown": {
                    "title": keyword,
                    "text": message_text,
                },
            },
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code != 200:
            return False, f"钉钉通知发送失败: HTTP {response.status_code}"
        return True, "钉钉通知发送成功。"
    except Exception as exc:
        logger.exception("Failed to send selector notification")
        return False, f"发送通知失败: {exc}"


def send_small_cap_notification(records: list[dict[str, Any]], filter_summary: str = "") -> tuple[bool, str]:
    return _send_selector_notification(strategy_name="小市值策略", records=records, filter_summary=filter_summary)


def send_profit_growth_notification(records: list[dict[str, Any]], filter_summary: str = "") -> tuple[bool, str]:
    return _send_selector_notification(strategy_name="净利增长策略", records=records, filter_summary=filter_summary)


def submit_small_cap_selection_task(
    *,
    top_n: int,
    max_market_cap_yi: float,
    min_revenue_growth: float,
    min_profit_growth: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
    only_hs_a: bool,
    filter_summary: str,
) -> str:
    from small_cap_selector import small_cap_selector

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=0, total=2, message="正在拉取小市值候选数据...")
        success, stocks_df, message = small_cap_selector.get_small_cap_stocks(
            top_n=int(top_n),
            max_market_cap_yi=float(max_market_cap_yi),
            min_revenue_growth=float(min_revenue_growth),
            min_profit_growth=float(min_profit_growth),
            sort_by=sort_by,
            exclude_st=bool(exclude_st),
            exclude_kcb=bool(exclude_kcb),
            exclude_cyb=bool(exclude_cyb),
            only_hs_a=bool(only_hs_a),
        )
        if not success or stocks_df is None:
            raise RuntimeError(message or "小市值选股失败")
        report_progress(current=2, total=2, message="小市值选股完成，正在同步结果...")
        return {
            "stocks": _json_safe(stocks_df),
            "message": message,
            "filter_summary": filter_summary,
            "selected_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    return start_ui_analysis_task(
        task_type=SMALL_CAP_TASK_TYPE,
        label="小市值选股",
        runner=runner,
        metadata={"top_n": int(top_n), "filter_summary": filter_summary},
    )


def get_small_cap_monitor_status() -> dict[str, Any]:
    return get_low_price_bull_status()


def update_small_cap_scan_interval(scan_interval: int) -> dict[str, Any]:
    return update_low_price_bull_scan_interval(scan_interval)


def start_small_cap_monitor() -> dict[str, Any]:
    return start_low_price_bull_monitor()


def stop_small_cap_monitor() -> dict[str, Any]:
    return stop_low_price_bull_monitor()


def list_small_cap_monitored_stocks() -> list[dict[str, Any]]:
    return list_low_price_bull_monitored_stocks()


def add_small_cap_monitored_stock(
    *,
    stock_code: str,
    stock_name: str,
    buy_price: float,
    buy_date: Optional[str] = None,
) -> tuple[bool, str]:
    return add_low_price_bull_monitored_stock(
        stock_code=stock_code,
        stock_name=stock_name,
        buy_price=buy_price,
        buy_date=buy_date,
    )


def remove_small_cap_monitored_stock(stock_code: str, reason: str = "手动移除") -> tuple[bool, str]:
    return remove_low_price_bull_monitored_stock(stock_code, reason)


def list_small_cap_alerts(*, history: bool = False, limit: int = 50) -> list[dict[str, Any]]:
    return list_low_price_bull_alerts(history=history, limit=limit)


def resolve_small_cap_alert(alert_id: int, status: str) -> tuple[bool, str]:
    return resolve_low_price_bull_alert(alert_id, status)


def cleanup_small_cap_alerts(days: int = 30) -> dict[str, Any]:
    return cleanup_low_price_bull_alerts(days)


def submit_profit_growth_selection_task(
    *,
    top_n: int,
    min_profit_growth: float,
    min_turnover_yi: float,
    max_turnover_yi: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
    filter_summary: str,
) -> str:
    from profit_growth_selector import profit_growth_selector

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=0, total=2, message="正在拉取净利增长候选数据...")
        success, stocks_df, message = profit_growth_selector.get_profit_growth_stocks(
            top_n=int(top_n),
            min_profit_growth=float(min_profit_growth),
            min_turnover_yi=float(min_turnover_yi) or None,
            max_turnover_yi=float(max_turnover_yi) or None,
            sort_by=sort_by,
            exclude_st=bool(exclude_st),
            exclude_kcb=bool(exclude_kcb),
            exclude_cyb=bool(exclude_cyb),
        )
        if not success or stocks_df is None:
            raise RuntimeError(message or "净利增长选股失败")
        report_progress(current=2, total=2, message="净利增长选股完成，正在同步结果...")
        return {
            "stocks": _json_safe(stocks_df),
            "message": message,
            "filter_summary": filter_summary,
            "selected_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    return start_ui_analysis_task(
        task_type=PROFIT_GROWTH_TASK_TYPE,
        label="净利增长选股",
        runner=runner,
        metadata={"top_n": int(top_n), "filter_summary": filter_summary},
    )


def _get_profit_growth_monitor():
    from profit_growth_monitor import profit_growth_monitor

    return profit_growth_monitor


def get_profit_growth_monitor_status() -> dict[str, Any]:
    monitor = _get_profit_growth_monitor()
    monitored = monitor.get_monitoring_stocks()
    pending_alerts = monitor.get_unprocessed_alerts()
    removed = monitor.get_removed_stocks(limit=50)
    return {
        "monitored_count": len(monitored),
        "pending_alerts": len(pending_alerts),
        "removed_count": len(removed),
    }


def list_profit_growth_monitored_stocks() -> list[dict[str, Any]]:
    return _json_safe(_get_profit_growth_monitor().get_monitoring_stocks())


def add_profit_growth_monitored_stock(
    *,
    stock_code: str,
    stock_name: str,
    buy_price: float,
    buy_date: Optional[str] = None,
) -> tuple[bool, str]:
    return _get_profit_growth_monitor().add_stock(
        stock_code=str(stock_code or "").strip().upper(),
        stock_name=str(stock_name or "").strip(),
        buy_price=float(buy_price or 0),
        buy_date=buy_date or None,
    )


def remove_profit_growth_monitored_stock(stock_code: str, reason: str = "手动移除") -> tuple[bool, str]:
    return _get_profit_growth_monitor().remove_stock(str(stock_code or "").strip().upper(), reason)


def list_profit_growth_alerts(*, history: bool = False, limit: int = 50) -> list[dict[str, Any]]:
    monitor = _get_profit_growth_monitor()
    if history:
        return _json_safe(monitor.get_all_alerts(limit=limit))
    return _json_safe(monitor.get_unprocessed_alerts())


def list_profit_growth_removed_stocks(limit: int = 50) -> list[dict[str, Any]]:
    return _json_safe(_get_profit_growth_monitor().get_removed_stocks(limit=limit))


def submit_value_stock_selection_task(
    *,
    top_n: int,
    max_pe: float,
    max_pb: float,
    min_dividend_yield: float,
    max_debt_ratio: float,
    min_float_cap_yi: float,
    max_float_cap_yi: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
    filter_summary: str,
) -> str:
    from value_stock_selector import ValueStockSelector

    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=0, total=2, message="正在拉取低估值候选数据...")
        selector = ValueStockSelector()
        success, stocks_df, message = selector.get_value_stocks(
            top_n=int(top_n),
            max_pe=float(max_pe),
            max_pb=float(max_pb),
            min_dividend_yield=float(min_dividend_yield),
            max_debt_ratio=float(max_debt_ratio),
            min_float_cap_yi=float(min_float_cap_yi) or None,
            max_float_cap_yi=float(max_float_cap_yi) or None,
            sort_by=sort_by,
            exclude_st=bool(exclude_st),
            exclude_kcb=bool(exclude_kcb),
            exclude_cyb=bool(exclude_cyb),
        )
        if not success or stocks_df is None:
            raise RuntimeError(message or "低估值选股失败")
        report_progress(current=2, total=2, message="低估值选股完成，正在同步结果...")
        return {
            "stocks": _json_safe(stocks_df),
            "message": message,
            "filter_summary": filter_summary,
            "selected_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    return start_ui_analysis_task(
        task_type=VALUE_STOCK_TASK_TYPE,
        label="低估值选股",
        runner=runner,
        metadata={"top_n": int(top_n), "filter_summary": filter_summary},
    )


def simulate_value_stock_strategy(stocks: list[dict[str, Any]]) -> dict[str, Any]:
    from value_stock_strategy import ValueStockStrategy

    strategy = ValueStockStrategy(initial_capital=1000000.0)
    buy_results: list[dict[str, Any]] = []
    sell_checks: list[dict[str, Any]] = []
    current_date = datetime.now().strftime("%Y-%m-%d")
    for row in stocks[: strategy.max_daily_buy]:
        code = str(row.get("股票代码") or "").split(".")[0]
        name = str(row.get("股票简称") or row.get("股票名称") or "N/A")
        price = row.get("最新价", row.get("股价", 0))
        try:
            price_value = float(price)
        except (TypeError, ValueError):
            price_value = 0.0
        if not code or price_value <= 0:
            continue
        success, message, trade = strategy.buy(code, name, price_value, current_date)
        buy_results.append({"success": success, "message": message, "trade": _json_safe(trade)})

    for code, position in list(strategy.positions.items()):
        should_sell, reason, rsi = strategy.should_sell(code, current_date=current_date)
        sell_checks.append(
            {
                "code": code,
                "name": position.get("name"),
                "should_sell": should_sell,
                "reason": reason,
                "rsi": rsi,
            }
        )

    return {
        "buy_results": buy_results,
        "sell_checks": sell_checks,
        "positions": _json_safe(strategy.get_positions()),
        "summary": _json_safe(strategy.get_portfolio_summary()),
        "trade_history": _json_safe(strategy.get_trade_history()),
    }


def _create_macro_cycle_database():
    from macro_cycle_db import macro_cycle_db

    return macro_cycle_db


def submit_macro_cycle_task(
    *,
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        from macro_cycle_engine import MacroCycleEngine

        def progress_callback(progress_pct, text):
            raw = float(progress_pct or 0)
            if 0 <= raw <= 1:
                raw *= 100
            pct_value = int(max(0, min(100, raw)))
            report_progress(current=pct_value, total=100, message=text or "宏观周期分析进行中...")

        engine = MacroCycleEngine(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        result = engine.run_full_analysis(progress_callback=progress_callback)
        if not result.get("success"):
            raise RuntimeError(result.get("error") or "宏观周期分析失败")
        report_progress(current=100, total=100, message="宏观周期分析完成，正在同步结果...")
        return {
            "result": _json_safe(result),
            "message": "宏观周期分析完成。",
        }

    return start_ui_analysis_task(
        task_type=MACRO_CYCLE_TASK_TYPE,
        label="宏观周期分析",
        runner=runner,
        metadata={},
    )


def list_macro_cycle_reports(limit: int = 20) -> list[dict[str, Any]]:
    reports_df = _create_macro_cycle_database().get_historical_reports(limit=limit)
    if pd is not None and isinstance(reports_df, pd.DataFrame):
        return _json_safe(reports_df.to_dict(orient="records"))
    return _json_safe(reports_df or [])


def get_macro_cycle_report(report_id: int) -> Optional[dict[str, Any]]:
    return _json_safe(_create_macro_cycle_database().get_report_detail(report_id))


def delete_macro_cycle_report(report_id: int) -> bool:
    return bool(_create_macro_cycle_database().delete_report(report_id))


def export_macro_cycle_markdown(result: dict[str, Any]) -> tuple[bytes, str, str]:
    from macro_cycle_pdf import generate_macro_cycle_markdown

    markdown = generate_macro_cycle_markdown(result or {})
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return markdown.encode("utf-8"), f"宏观周期报告_{timestamp}.md", "text/markdown; charset=utf-8"


def export_macro_cycle_pdf(result: dict[str, Any]) -> tuple[bytes, str, str]:
    from macro_cycle_pdf import MacroCyclePDFGenerator

    generator = MacroCyclePDFGenerator()
    pdf_path = generator.generate_pdf(result or {})
    with open(pdf_path, "rb") as file_obj:
        data = file_obj.read()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return data, f"宏观周期报告_{timestamp}.pdf", "application/pdf"


def _get_news_flow_engine():
    from news_flow_engine import news_flow_engine

    return news_flow_engine


def _create_news_flow_database():
    from news_flow_db import news_flow_db

    return news_flow_db


def _get_news_flow_scheduler():
    from news_flow_scheduler import news_flow_scheduler

    return news_flow_scheduler


def _get_news_flow_fetcher():
    from news_flow_data import NewsFlowDataFetcher

    return NewsFlowDataFetcher()


def submit_news_flow_task(
    *,
    category: Optional[str],
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
) -> str:
    def runner(_task_id: str, report_progress) -> dict[str, Any]:
        report_progress(current=10, total=100, message="正在获取多平台新闻数据...")
        result = _get_news_flow_engine().run_full_analysis(
            category=category,
            include_ai=True,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        if not result.get("success"):
            raise RuntimeError(result.get("error") or "新闻流量分析失败")
        report_progress(current=100, total=100, message="新闻流量分析完成，正在同步结果...")
        return {
            "result": _json_safe(result),
            "message": f"AI分析完成，耗时 {result.get('duration', 0):.1f} 秒。",
        }

    return start_ui_analysis_task(
        task_type=NEWS_FLOW_TASK_TYPE,
        label="新闻流量分析",
        runner=runner,
        metadata={"category": category or "all"},
    )


def run_news_flow_quick_analysis(category: Optional[str] = None) -> dict[str, Any]:
    result = _get_news_flow_engine().run_quick_analysis(category=category)
    if not result.get("success"):
        raise ValueError(result.get("error") or "热点同步失败")
    return _json_safe(result)


def run_news_flow_alert_check() -> dict[str, Any]:
    result = _get_news_flow_engine().run_alert_check()
    if not result.get("success"):
        raise ValueError(result.get("error") or "预警检查失败")
    return _json_safe(result)


def get_news_flow_dashboard() -> dict[str, Any]:
    return _json_safe(_get_news_flow_engine().get_dashboard_data())


def get_news_flow_trend(days: int = 7) -> dict[str, Any]:
    return _json_safe(_get_news_flow_engine().get_flow_trend(days=max(1, int(days))))


def list_news_flow_history(limit: int = 50) -> list[dict[str, Any]]:
    return _json_safe(_create_news_flow_database().get_history_snapshots(limit=limit))


def get_news_flow_snapshot_detail(snapshot_id: int) -> Optional[dict[str, Any]]:
    detail = _create_news_flow_database().get_snapshot_detail(snapshot_id)
    return _json_safe(detail) if detail else None


def list_news_flow_alerts(days: int = 7, alert_type: Optional[str] = None) -> list[dict[str, Any]]:
    return _json_safe(_create_news_flow_database().get_alerts(days=max(1, int(days)), alert_type=alert_type))


def list_news_flow_ai_history(limit: int = 20) -> list[dict[str, Any]]:
    return _json_safe(_create_news_flow_database().get_ai_analysis_history(limit=limit))


def list_news_flow_sentiment_history(limit: int = 50) -> list[dict[str, Any]]:
    return _json_safe(_create_news_flow_database().get_sentiment_history(limit=limit))


def list_news_flow_daily_statistics(days: int = 7) -> list[dict[str, Any]]:
    return _json_safe(_create_news_flow_database().get_daily_statistics(days=max(1, int(days))))


def search_news_flow_stock_news(keyword: str, limit: int = 50) -> list[dict[str, Any]]:
    return _json_safe(_create_news_flow_database().search_stock_news(keyword, limit=limit))


def get_news_flow_alert_configs() -> dict[str, str]:
    return _json_safe(_create_news_flow_database().get_all_alert_configs())


def update_news_flow_alert_configs(values: dict[str, str]) -> dict[str, str]:
    database = _create_news_flow_database()
    for key, value in values.items():
        database.set_alert_config(str(key), str(value))
    return _json_safe(database.get_all_alert_configs())


def get_news_flow_scheduler_status() -> dict[str, Any]:
    return _json_safe(_get_news_flow_scheduler().get_status())


def update_news_flow_scheduler_config(
    *,
    task_enabled: dict[str, bool],
    task_intervals: dict[str, int],
) -> dict[str, Any]:
    scheduler = _get_news_flow_scheduler()
    scheduler.update_task_config(task_enabled, task_intervals)
    return _json_safe(scheduler.get_status())


def start_news_flow_scheduler() -> dict[str, Any]:
    scheduler = _get_news_flow_scheduler()
    scheduler.start()
    return _json_safe(scheduler.get_status())


def stop_news_flow_scheduler() -> dict[str, Any]:
    scheduler = _get_news_flow_scheduler()
    scheduler.stop()
    return _json_safe(scheduler.get_status())


def run_news_flow_scheduler_task(task_type: str) -> dict[str, Any]:
    scheduler = _get_news_flow_scheduler()
    task_map = {
        "sync_hotspots": scheduler.run_sync_now,
        "generate_alerts": scheduler.run_alerts_now,
        "deep_analysis": scheduler.run_analysis_now,
    }
    handler = task_map.get(task_type)
    if not handler:
        raise ValueError("不支持的新闻流量任务类型")
    return _json_safe(handler())


def list_news_flow_scheduler_logs(days: int = 7, task_type: Optional[str] = None) -> list[dict[str, Any]]:
    return _json_safe(_get_news_flow_scheduler().get_task_logs(days=max(1, int(days)), task_type=task_type))


def get_news_flow_supported_platforms() -> list[dict[str, Any]]:
    return _json_safe(_get_news_flow_fetcher().get_platform_list())


def export_news_flow_pdf(result: dict[str, Any]) -> tuple[bytes, str, str]:
    from news_flow_pdf import NewsFlowPDFGenerator

    generator = NewsFlowPDFGenerator()
    pdf_path = generator.generate_report(result or {})
    if not pdf_path:
        raise ValueError("新闻流量 PDF 生成失败")
    with open(pdf_path, "rb") as file_obj:
        data = file_obj.read()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return data, f"新闻流量分析报告_{timestamp}.pdf", "application/pdf"


def build_action_payload_for_record(record: Optional[dict[str, Any]], analysis_source: str) -> Optional[dict[str, Any]]:
    if not record:
        return None
    symbol = str(record.get("symbol") or "").strip()
    if not symbol:
        return None
    return build_analysis_action_payload(
        symbol=symbol,
        stock_name=record.get("stock_name") or record.get("name") or symbol,
        final_decision=record.get("final_decision") or {},
        origin_analysis_id=record.get("id"),
        summary=record.get("summary"),
        account_name=record.get("account_name") or DEFAULT_ACCOUNT_NAME,
        analysis_scope=record.get("analysis_scope") or "research",
        analysis_source=analysis_source,
    )


def list_analysis_history(
    *,
    portfolio_state: str = "全部",
    account_name: Optional[str] = None,
    search_term: str = "",
) -> list[dict[str, Any]]:
    records = analysis_history_service.list_records(
        portfolio_state=portfolio_state,
        account_name=account_name,
        search_term=search_term,
    )
    result: list[dict[str, Any]] = []
    for record in records:
        item = dict(record)
        item["action_payload"] = build_action_payload_for_record(item, item.get("analysis_source") or "history")
        result.append(item)
    return result


def get_analysis_record_detail(record_id: int) -> Optional[dict[str, Any]]:
    record = analysis_history_service.get_record(record_id)
    if not record:
        return None
    item = dict(record)
    item["action_payload"] = build_action_payload_for_record(item, item.get("analysis_source") or "history")
    return item


def delete_analysis_record(record_id: int) -> bool:
    return analysis_repository.delete_record(record_id)


def list_followup_assets(*, status_filter: str = "全部", search_term: str = "") -> list[dict[str, Any]]:
    status_map = {
        "全部": (STATUS_WATCHLIST, STATUS_RESEARCH),
        "仅关注": (STATUS_WATCHLIST,),
        "仅看过": (STATUS_RESEARCH,),
    }
    items = asset_service.list_followup_assets(
        statuses=status_map.get(status_filter, (STATUS_WATCHLIST, STATUS_RESEARCH)),
        search_term=search_term,
        limit=60,
    )
    result: list[dict[str, Any]] = []
    for item in items:
        normalized = dict(item)
        normalized["action_payload"] = build_action_payload_for_record(
            {
                "id": item.get("latest_analysis_id"),
                "symbol": item.get("symbol"),
                "stock_name": item.get("name"),
                "summary": item.get("latest_analysis_summary"),
                "account_name": item.get("account_name"),
                "analysis_scope": item.get("latest_analysis_scope") or "research",
                "analysis_source": item.get("latest_analysis_source") or "followup",
                "final_decision": item.get("strategy_context", {}).get("final_decision")
                or item.get("strategy_context", {}),
            },
            item.get("latest_analysis_source") or "followup",
        )
        result.append(normalized)
    return result


def promote_followup_to_watchlist(asset_id: int) -> tuple[bool, str]:
    asset = asset_service.asset_repository.get_asset(asset_id)
    if not asset:
        return False, "未找到对应标的"
    success, message, _ = asset_service.promote_to_watchlist(
        symbol=asset.get("symbol") or asset.get("code"),
        stock_name=asset.get("name") or asset.get("symbol") or asset.get("code"),
        account_name=asset.get("account_name") or DEFAULT_ACCOUNT_NAME,
        note=asset.get("note") or "",
        origin_analysis_id=asset.get("origin_analysis_id"),
        monitor_enabled=bool(asset.get("monitor_enabled", True)),
    )
    return success, message


def demote_followup_to_research(asset_id: int, note: str = "") -> bool:
    return asset_service.remove_from_watchlist(asset_id, note=note)


def get_system_status() -> dict[str, Any]:
    ensure_runtime_started()
    scheduler = monitor_service.get_scheduler()
    return {
        "api_key_configured": bool(getattr(config, "DEEPSEEK_API_KEY", "")),
        "record_count": analysis_history_service.count_records(),
        "followup_count": len(asset_service.list_followup_assets(limit=None)),
        "models": {
            "lightweight": getattr(config, "LIGHTWEIGHT_MODEL_NAME", ""),
            "reasoning": getattr(config, "REASONING_MODEL_NAME", ""),
            "lightweight_options": list(get_lightweight_model_options().keys()),
            "reasoning_options": list(get_reasoning_model_options().keys()),
        },
        "monitor_service": monitor_service.get_status(),
        "monitor_scheduler": scheduler.get_status() if scheduler else None,
        "portfolio_scheduler": {
            **portfolio_scheduler.get_status(),
            "schedule_times": portfolio_scheduler.get_schedule_times(),
        },
        "site_filing": {
            "number": getattr(config, "ICP_NUMBER", ""),
            "link": getattr(config, "ICP_LINK", ""),
        },
    }


def get_stock_info(symbol: str) -> dict[str, Any]:
    fetcher = StockDataFetcher()
    return fetcher.get_stock_info(symbol, max_age_seconds=30, allow_stale_on_failure=True, cache_first=True)


def list_portfolio_stocks(account_name: Optional[str] = None) -> list[dict[str, Any]]:
    all_stocks = portfolio_manager.get_all_latest_analysis()
    if account_name and account_name != "全部账户":
        all_stocks = [
            item for item in all_stocks
            if item.get("account_name", DEFAULT_ACCOUNT_NAME) == account_name
        ]
    trade_summary_map = portfolio_manager.get_trade_summary_map(
        [stock.get("id") for stock in all_stocks if stock.get("id")]
    )
    result = []
    for stock in all_stocks:
        item = dict(stock)
        item.update(trade_summary_map.get(stock.get("id"), {}))
        result.append(item)
    return result


def create_portfolio_stock(payload: dict[str, Any]) -> tuple[bool, str, Optional[int], list[str]]:
    success, message, stock_id = portfolio_manager.add_stock(
        code=str(payload.get("code") or "").strip().upper(),
        name=payload.get("name"),
        cost_price=payload.get("cost_price"),
        quantity=payload.get("quantity"),
        note=payload.get("note") or "",
        auto_monitor=bool(payload.get("auto_monitor", True)),
        account_name=str(payload.get("account_name") or DEFAULT_ACCOUNT_NAME).strip() or DEFAULT_ACCOUNT_NAME,
        origin_analysis_id=payload.get("origin_analysis_id"),
    )
    warnings: list[str] = []
    if success and stock_id and payload.get("buy_date") and payload.get("cost_price") and payload.get("quantity"):
        trade_success, trade_message = portfolio_manager.seed_initial_trade(
            stock_id,
            trade_date=payload.get("buy_date"),
            note=payload.get("note") or "",
        )
        if not trade_success:
            warnings.append(trade_message)
    return success, message, stock_id, warnings


def update_portfolio_stock(stock_id: int, payload: dict[str, Any]) -> tuple[bool, str]:
    updates = {
        key: value
        for key, value in payload.items()
        if key in {"code", "name", "cost_price", "quantity", "note", "auto_monitor", "account_name"} and value is not None
    }
    return portfolio_manager.update_stock(stock_id, **updates)


def delete_portfolio_stock(stock_id: int) -> tuple[bool, str]:
    return portfolio_manager.delete_stock(stock_id)


def record_portfolio_trade(stock_id: int, payload: dict[str, Any]) -> tuple[bool, str, Optional[dict[str, Any]]]:
    return portfolio_manager.record_trade(
        stock_id=stock_id,
        trade_type=payload.get("trade_type"),
        quantity=int(payload.get("quantity") or 0),
        price=float(payload.get("price") or 0),
        trade_date=payload.get("trade_date"),
        note=payload.get("note") or "",
    )


def list_portfolio_trade_records(
    account_name: Optional[str] = None,
    limit: Optional[int] = 120,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
) -> Any:
    if page is not None or page_size is not None:
        resolved_page = max(1, int(page or 1))
        resolved_page_size = _clamp_int(page_size, 1, 100, 20)
        return portfolio_manager.get_trade_records_paginated(
            account_name=account_name,
            page=resolved_page,
            page_size=resolved_page_size,
        )
    return portfolio_manager.get_trade_records(account_name=account_name, limit=int(limit or 120))


def get_portfolio_risk(account_name: Optional[str] = None) -> dict[str, Any]:
    return portfolio_manager.calculate_portfolio_risk(account_name=account_name)

def list_portfolio_analysis_history(stock_id: int, limit: int = 10) -> list[dict[str, Any]]:
    return portfolio_manager.get_analysis_history(stock_id, limit=limit)


def get_portfolio_scheduler_status() -> dict[str, Any]:
    return {
        **portfolio_scheduler.get_status(),
        "schedule_times": portfolio_scheduler.get_schedule_times(),
    }


def get_portfolio_scheduler_latest_task() -> Optional[dict[str, Any]]:
    return get_latest_ui_task(PORTFOLIO_SCHEDULER_TASK_TYPE)


def get_portfolio_scheduler_active_task() -> Optional[dict[str, Any]]:
    return get_active_ui_task(PORTFOLIO_SCHEDULER_TASK_TYPE)


def get_portfolio_scheduler_task(task_id: str) -> Optional[dict[str, Any]]:
    return get_ui_task(PORTFOLIO_SCHEDULER_TASK_TYPE, task_id)


def update_portfolio_scheduler(payload: dict[str, Any]) -> dict[str, Any]:
    schedule_times = payload.get("schedule_times") or []
    if schedule_times:
        portfolio_scheduler.set_schedule_times(schedule_times)
    portfolio_scheduler.update_config(
        analysis_mode=payload.get("analysis_mode"),
        max_workers=payload.get("max_workers"),
        auto_sync_monitor=payload.get("auto_sync_monitor"),
        send_notification=payload.get("send_notification"),
        selected_agents=payload.get("selected_agents"),
        account_configs=payload.get("account_configs"),
    )
    return get_portfolio_scheduler_status()


def list_price_alerts() -> list[dict[str, Any]]:
    return monitor_db.get_monitored_stocks()


def create_price_alert(payload: dict[str, Any]) -> int:
    runtime_config = get_smart_monitor_runtime_config()
    return monitor_db.add_monitored_stock(
        symbol=str(payload.get("symbol") or "").strip().upper(),
        name=payload.get("name") or payload.get("symbol"),
        rating=payload.get("rating") or "买入",
        entry_range={"min": float(payload.get("entry_min") or 0), "max": float(payload.get("entry_max") or 0)},
        take_profit=payload.get("take_profit"),
        stop_loss=payload.get("stop_loss"),
        check_interval=int(
            payload.get("check_interval")
            or runtime_config["realtime_monitor_interval_minutes"]
        ),
        notification_enabled=bool(payload.get("notification_enabled", True)),
        trading_hours_only=bool(payload.get("trading_hours_only", True)),
        account_name=str(payload.get("account_name") or DEFAULT_ACCOUNT_NAME).strip() or DEFAULT_ACCOUNT_NAME,
        origin_analysis_id=payload.get("origin_analysis_id"),
    )


def update_price_alert(stock_id: int, payload: dict[str, Any]) -> bool:
    runtime_config = get_smart_monitor_runtime_config()
    return bool(
        monitor_db.update_monitored_stock(
            stock_id,
            rating=payload.get("rating") or "买入",
            entry_range={"min": float(payload.get("entry_min") or 0), "max": float(payload.get("entry_max") or 0)},
            take_profit=payload.get("take_profit"),
            stop_loss=payload.get("stop_loss"),
            check_interval=int(
                payload.get("check_interval")
                or runtime_config["realtime_monitor_interval_minutes"]
            ),
            notification_enabled=bool(payload.get("notification_enabled", True)),
            trading_hours_only=payload.get("trading_hours_only"),
            managed_by_portfolio=payload.get("managed_by_portfolio"),
        )
    )


def list_price_alert_notifications(limit: int = 30) -> list[dict[str, Any]]:
    return monitor_db.get_all_recent_notifications(limit=limit)


def mark_monitor_notification_read(event_id: int) -> None:
    monitor_db.repository.mark_notification_read(event_id)


def ignore_monitor_notification(event_id: int) -> None:
    monitor_db.ignore_notification(event_id)


def get_smart_monitor_runtime_config() -> dict[str, int]:
    intraday_value = monitoring_repository.get_metadata(SMART_MONITOR_INTRADAY_INTERVAL_KEY)
    realtime_value = monitoring_repository.get_metadata(SMART_MONITOR_REALTIME_INTERVAL_KEY)
    return {
        "intraday_decision_interval_minutes": _clamp_int(
            intraday_value,
            10,
            120,
            _default_intraday_decision_interval_minutes(),
        ),
        "realtime_monitor_interval_minutes": _clamp_int(
            realtime_value,
            1,
            10,
            _default_realtime_monitor_interval_minutes(),
        ),
    }


def update_smart_monitor_runtime_config(payload: dict[str, Any]) -> dict[str, int]:
    intraday_minutes = _clamp_int(
        payload.get("intraday_decision_interval_minutes"),
        10,
        120,
        _default_intraday_decision_interval_minutes(),
    )
    realtime_minutes = _clamp_int(
        payload.get("realtime_monitor_interval_minutes"),
        1,
        10,
        _default_realtime_monitor_interval_minutes(),
    )
    monitoring_repository.set_metadata(
        SMART_MONITOR_INTRADAY_INTERVAL_KEY,
        str(intraday_minutes),
    )
    monitoring_repository.set_metadata(
        SMART_MONITOR_REALTIME_INTERVAL_KEY,
        str(realtime_minutes),
    )
    monitoring_repository.bulk_set_interval_minutes("ai_task", intraday_minutes)
    monitoring_repository.bulk_set_interval_minutes("price_alert", realtime_minutes)
    return {
        "intraday_decision_interval_minutes": intraday_minutes,
        "realtime_monitor_interval_minutes": realtime_minutes,
    }


def list_smart_monitor_tasks(
    enabled_only: bool = False,
    account_name: Optional[str] = None,
    has_position: Optional[bool] = None,
) -> list[dict[str, Any]]:
    return smart_monitor_db.get_monitor_tasks(
        enabled_only=enabled_only,
        account_name=account_name,
        has_position=has_position,
    )


def upsert_smart_monitor_task(payload: dict[str, Any]) -> int:
    runtime_config = get_smart_monitor_runtime_config()
    normalized_payload = dict(payload)
    normalized_payload.setdefault(
        "check_interval",
        int(runtime_config["intraday_decision_interval_minutes"]) * 60,
    )
    return smart_monitor_db.upsert_monitor_task(normalized_payload)


def update_smart_monitor_task(task_id: int, payload: dict[str, Any]) -> bool:
    item = smart_monitor_db.monitoring_repository.get_item(task_id)
    if not item:
        return False
    task_payload = dict(payload)
    runtime_config = get_smart_monitor_runtime_config()
    task_payload.setdefault(
        "check_interval",
        int(runtime_config["intraday_decision_interval_minutes"]) * 60,
    )
    task_payload.setdefault("account_name", item.get("account_name") or DEFAULT_ACCOUNT_NAME)
    task_payload.setdefault("asset_id", item.get("asset_id"))
    task_payload.setdefault("portfolio_stock_id", item.get("portfolio_stock_id"))
    task_payload.setdefault("origin_analysis_id", item.get("origin_analysis_id"))
    return bool(smart_monitor_db.update_monitor_task(item.get("symbol") or "", task_payload))


def run_manual_smart_monitor_analysis(payload: dict[str, Any]) -> dict[str, Any]:
    engine = monitor_service.orchestrator.engine
    return engine.analyze_stock(
        stock_code=str(payload.get("stock_code") or "").strip().upper(),
        notify=bool(payload.get("notify", False)),
        trading_hours_only=bool(payload.get("trading_hours_only", True)),
        account_name=str(payload.get("account_name") or DEFAULT_ACCOUNT_NAME).strip() or DEFAULT_ACCOUNT_NAME,
        asset_id=payload.get("asset_id"),
        portfolio_stock_id=payload.get("portfolio_stock_id"),
    )


def run_smart_monitor_tasks_once(
    *,
    enabled_only: bool = True,
    account_name: Optional[str] = None,
    has_position: Optional[bool] = None,
) -> dict[str, Any]:
    tasks = smart_monitor_db.get_monitor_tasks(
        enabled_only=enabled_only,
        account_name=account_name,
        has_position=has_position,
    )
    orchestrator = monitor_service.orchestrator
    processed_alert_ids: set[int] = set()
    summary = {
        "task_total": 0,
        "task_success": 0,
        "task_failed": 0,
        "price_alert_total": 0,
        "price_alert_success": 0,
        "price_alert_failed": 0,
        "account_name": account_name or "",
        "has_position": has_position,
        "enabled_only": enabled_only,
    }

    for task in tasks:
        task_id = int(task.get("id") or 0)
        if task_id <= 0:
            continue
        summary["task_total"] += 1
        if orchestrator.run_item_once(task_id):
            summary["task_success"] += 1
        else:
            summary["task_failed"] += 1

        alert_item = smart_monitor_db.monitoring_repository.get_item_by_symbol(
            task.get("stock_code") or "",
            monitor_type="price_alert",
            managed_only=True if task.get("managed_by_portfolio") else None,
            account_name=task.get("account_name"),
            asset_id=task.get("asset_id"),
            portfolio_stock_id=task.get("portfolio_stock_id"),
        )
        alert_id = int((alert_item or {}).get("id") or 0)
        if alert_id <= 0 or alert_id in processed_alert_ids:
            continue
        processed_alert_ids.add(alert_id)
        summary["price_alert_total"] += 1
        if orchestrator.run_item_once(alert_id):
            summary["price_alert_success"] += 1
        else:
            summary["price_alert_failed"] += 1

    scheduler = monitor_service.get_scheduler()
    summary["service_status"] = monitor_service.get_status()
    summary["scheduler_status"] = scheduler.get_status() if scheduler else None
    return summary


def get_activity_snapshot() -> dict[str, Any]:
    return {
        "service_status": monitor_service.get_status(),
        "price_alert_notifications": monitor_db.get_all_recent_notifications(limit=50),
        "recent_events": monitor_service.get_recent_events(limit=50),
        "ai_decisions": smart_monitor_db.get_ai_decisions(limit=50),
        "trade_records": smart_monitor_db.get_trade_records(limit=50),
        "pending_actions": smart_monitor_db.get_pending_actions(limit=50),
        "registry_items": monitor_service.get_registry_items(enabled_only=False),
    }


def save_config_values(values: dict[str, str]) -> tuple[bool, str]:
    valid, message = config_manager.validate_config(values)
    if not valid:
        return False, message
    if not config_manager.write_env(values):
        return False, "保存配置失败"
    config_manager.reload_config()
    notification_service.__init__()
    ensure_runtime_started()
    return True, "配置已保存"


def get_config_payload() -> dict[str, Any]:
    return {
        "config": config_manager.get_config_info(),
        "webhook_status": notification_service.get_webhook_config_status(),
    }


def test_webhook() -> tuple[bool, str]:
    return notification_service.send_test_webhook()


def get_database_admin_status() -> dict[str, Any]:
    return database_admin.get_status()


def create_database_backup() -> dict[str, Any]:
    return database_admin.create_backup()


def restore_database_backup(backup_name: str) -> dict[str, Any]:
    return database_admin.restore_backup(backup_name)


def cleanup_database_history(days: int) -> dict[str, Any]:
    return database_admin.cleanup_history(days)


def delete_price_alert(stock_id: int) -> bool:
    return bool(monitor_db.remove_monitored_stock(stock_id))


def toggle_price_alert_notification(stock_id: int, enabled: bool) -> bool:
    return bool(monitor_db.toggle_notification(stock_id, enabled))


def delete_smart_monitor_task(task_id: int) -> bool:
    return bool(smart_monitor_db.delete_monitor_task(task_id))


def set_smart_monitor_task_enabled(task_id: int, enabled: bool) -> bool:
    return bool(smart_monitor_db.set_monitor_task_enabled(task_id, enabled))


def set_all_smart_monitor_tasks_enabled(enabled: bool) -> int:
    return int(smart_monitor_db.set_all_monitor_tasks_enabled(enabled))


def list_smart_monitor_decisions(limit: int = 100) -> list[dict[str, Any]]:
    return smart_monitor_db.get_ai_decisions(limit=limit)


def list_smart_monitor_trade_records(limit: int = 100) -> list[dict[str, Any]]:
    return smart_monitor_db.get_trade_records(limit=limit)


def list_pending_actions(
    *,
    status: Optional[str] = "pending",
    account_name: Optional[str] = None,
    asset_id: Optional[int] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return smart_monitor_db.get_pending_actions(
        status=status,
        account_name=account_name,
        asset_id=asset_id,
        limit=limit,
    )


def resolve_pending_action(action_id: int, status: str, resolution_note: str = "") -> bool:
    return bool(
        smart_monitor_db.resolve_pending_action(
            action_id,
            status=status,
            resolution_note=resolution_note,
        )
    )


def start_monitor_runtime() -> dict[str, Any]:
    scheduler = monitor_service.get_scheduler()
    if scheduler is not None:
        scheduler.update_config(enabled=True)
        monitor_service.ensure_scheduler_state()
    else:
        monitor_service.start_monitoring()
    return {
        "monitor_service": monitor_service.get_status(),
        "monitor_scheduler": scheduler.get_status() if scheduler else None,
    }


def stop_monitor_runtime() -> dict[str, Any]:
    scheduler = monitor_service.get_scheduler()
    if scheduler is not None:
        scheduler.update_config(enabled=False)
        if scheduler.running:
            scheduler.stop_scheduler()
    monitor_service.stop_monitoring()
    return {
        "monitor_service": monitor_service.get_status(),
        "monitor_scheduler": scheduler.get_status() if scheduler else None,
    }


def start_portfolio_scheduler() -> dict[str, Any]:
    portfolio_scheduler.start_scheduler()
    return get_portfolio_scheduler_status()


def stop_portfolio_scheduler() -> dict[str, Any]:
    portfolio_scheduler.stop_scheduler()
    return get_portfolio_scheduler_status()


def run_portfolio_scheduler_once() -> dict[str, Any]:
    task_id = portfolio_scheduler.run_once()
    active_task = get_portfolio_scheduler_active_task()
    if task_id:
        return {
            "success": True,
            "task_id": task_id,
            "task": get_portfolio_scheduler_task(task_id),
            "status": get_portfolio_scheduler_status(),
        }

    if active_task:
        return {
            "success": False,
            "message": "当前已有持仓分析任务正在执行或排队，请等待当前任务完成。",
            "task": active_task,
            "status": get_portfolio_scheduler_status(),
        }

    if portfolio_manager.get_stock_count() <= 0:
        return {
            "success": False,
            "message": "当前没有可执行分析的持仓股票。",
            "status": get_portfolio_scheduler_status(),
        }

    return {
        "success": False,
        "message": "立即执行失败",
        "status": get_portfolio_scheduler_status(),
    }
