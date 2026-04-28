from __future__ import annotations

import concurrent.futures
import json
import re
import threading
import uuid
from datetime import datetime, time
from typing import Any, Dict, List, Optional

import schedule

import research_hub_service
from asset_repository import STATUS_FOCUS, STATUS_RESEARCH, asset_repository
from investment_db_utils import connect_sqlite, get_metadata, set_metadata
from sector_strategy_db import SectorStrategyDatabase
from selector_filter_utils import find_matching_column, normalize_stock_code, parse_numeric_value
from time_utils import local_now, parse_display_timestamp


SMART_SELECTION_SCHEDULER_ENABLED_KEY = "smart_selection_scheduler_enabled"
SMART_SELECTION_SCHEDULER_TIME_KEY = "smart_selection_scheduler_time"
SMART_SELECTION_MAX_WORKERS_KEY = "smart_selection_max_workers"

DEFAULT_SCHEDULE_TIME = "14:30"
DEFAULT_MAX_WORKERS = 6
FINAL_SELECTION_LIMIT = 10
SECTOR_WATCH_LIMIT = 3
HOT_LIFECYCLE_STAGES = {"startup", "explosive", "decay"}
STRICT_EXECUTION_TRIGGER_SOURCES = {"scheduled"}
RISK_REVIEW_PARALLELISM_CAP = 4
EXTERNAL_DISCOVERY_LIMIT = 8
EXTERNAL_DISCOVERY_LIMIT_PER_SECTOR = 4
EXTERNAL_DISCOVERY_MIN_MATCHED_CANDIDATES = 5
EXTERNAL_DISCOVERY_MIN_STARTUP_CANDIDATES = 2
EXTERNAL_DISCOVERY_MIN_EXPLOSIVE_CANDIDATES = 3
EXTERNAL_DISCOVERY_MIN_FINAL_SELECTED = 3
NEWS_FLOW_CONTEXT_MAX_AGE_HOURS = 12
EXCLUDED_SELECTION_SECTOR_KEYWORDS = (
    "昨日连扳",
    "昨日连板",
    "昨日涨停",
    "昨日首板",
    "季报预减",
    "季报预亏",
    "一季报预减",
    "中报预减",
    "年报预减",
    "业绩预减",
    "业绩预亏",
)
NEWS_RISK_KEYWORDS = ("减持", "监管函", "立案", "处罚", "问询", "预减", "预亏", "亏损", "ST")
AGENT_COMPOSITE_SCORE_SCALE_BY_STATE = {
    "momentum": 2.04,
    "momo": 2.04,
    "range": 1.96,
    "ice": 1.94,
    "retreat": 1.80,
}
EXECUTION_GATE_POLICY_BY_STATE = {
    "momentum": {
        "strict_threshold": 56.0,
        "manual_threshold": 50.0,
        "strict_floor": 40.0,
        "manual_floor": 35.0,
        "strict_distribution_max": 78.0,
        "manual_distribution_max": 82.0,
        "sector_limit": 2,
    },
    "momo": {
        "strict_threshold": 56.0,
        "manual_threshold": 50.0,
        "strict_floor": 40.0,
        "manual_floor": 35.0,
        "strict_distribution_max": 78.0,
        "manual_distribution_max": 82.0,
        "sector_limit": 2,
    },
    "range": {
        "strict_threshold": 54.0,
        "manual_threshold": 50.0,
        "strict_floor": 42.0,
        "manual_floor": 38.0,
        "strict_distribution_max": 72.0,
        "manual_distribution_max": 76.0,
        "sector_limit": 2,
    },
    "ice": {
        "strict_threshold": 58.0,
        "manual_threshold": 55.0,
        "strict_floor": 45.0,
        "manual_floor": 42.0,
        "strict_distribution_max": 65.0,
        "manual_distribution_max": 70.0,
        "sector_limit": 1,
    },
    "retreat": {
        "strict_threshold": 62.0,
        "manual_threshold": 58.0,
        "strict_floor": 48.0,
        "manual_floor": 45.0,
        "strict_distribution_max": 60.0,
        "manual_distribution_max": 65.0,
        "sector_limit": 1,
    },
}
DEFAULT_EXECUTION_GATE_POLICY = EXECUTION_GATE_POLICY_BY_STATE["range"]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_json_loads(value: Any, default: Any):
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _clamp_score(value: Any, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, _safe_float(value, minimum)))


def _score_range(value: Any, *, low: float, high: float, default: float = 50.0) -> float:
    if value in (None, ""):
        return default
    numeric_value = _safe_float(value, default)
    if high <= low:
        return default
    return _clamp_score((numeric_value - low) / (high - low) * 100.0)


def _safe_text(value: Any) -> str:
    try:
        if value != value:
            return ""
    except Exception:
        pass
    return str(value or "").strip()


def _normalize_trigger_source(value: Any) -> str:
    return str(value or "manual").strip().lower() or "manual"


def _is_ordered_subsequence(shorter: str, longer: str) -> bool:
    if not shorter or not longer or len(shorter) > len(longer):
        return False
    position = 0
    for char in shorter:
        position = longer.find(char, position)
        if position < 0:
            return False
        position += 1
    return True


def _is_excluded_selection_sector(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    normalized = research_hub_service._normalize_sector_text(text)
    for keyword in EXCLUDED_SELECTION_SECTOR_KEYWORDS:
        keyword_normalized = research_hub_service._normalize_sector_text(keyword)
        if keyword in text or (keyword_normalized and keyword_normalized in normalized):
            return True
    return False


class SmartSelectionService:
    def __init__(self, db_path: str = "investment.db") -> None:
        self.db_path = db_path
        self._db_lock = threading.RLock()
        self._execution_lock = threading.Lock()
        self._threads: dict[str, threading.Thread] = {}
        self.sector_strategy_db = SectorStrategyDatabase()
        self._init_database()

    def _connect(self):
        return connect_sqlite(self.db_path)

    def _init_database(self) -> None:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS smart_selection_runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    message TEXT,
                    current INTEGER NOT NULL DEFAULT 0,
                    total INTEGER NOT NULL DEFAULT 100,
                    error TEXT,
                    trigger_source TEXT,
                    lightweight_model TEXT,
                    reasoning_model TEXT,
                    sector_report_id INTEGER,
                    sector_report_reused INTEGER NOT NULL DEFAULT 0,
                    result_summary_json TEXT,
                    warnings_json TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS smart_selection_run_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    bucket TEXT NOT NULL,
                    symbol TEXT,
                    name TEXT,
                    primary_sector TEXT,
                    lifecycle_stage TEXT,
                    defense_line_type TEXT,
                    score REAL,
                    heat_score REAL,
                    rank_order INTEGER NOT NULL DEFAULT 0,
                    reason TEXT,
                    snapshot_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES smart_selection_runs(run_id)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_smart_selection_run_items_run_bucket
                ON smart_selection_run_items(run_id, bucket, rank_order)
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS smart_selection_watch_pool (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL UNIQUE,
                    name TEXT,
                    source_run_id TEXT,
                    source_sector TEXT,
                    lifecycle_stage TEXT,
                    defense_line_type TEXT,
                    trajectory_json TEXT,
                    reason TEXT,
                    last_seen_at TEXT,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_smart_selection_watch_pool_active_seen
                ON smart_selection_watch_pool(active, datetime(last_seen_at) DESC, datetime(updated_at) DESC)
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS smart_selection_sector_heat_daily (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    sector_report_id INTEGER,
                    board_date TEXT NOT NULL,
                    sector_name TEXT NOT NULL,
                    normalized_sector_name TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT '',
                    heat_score REAL NOT NULL DEFAULT 0,
                    rank_order INTEGER NOT NULL DEFAULT 0,
                    lifecycle_stage TEXT,
                    defense_line_type TEXT,
                    delta_1 REAL,
                    delta_2 REAL,
                    trajectory_json TEXT,
                    action_hint TEXT,
                    selection_veto INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(board_date, normalized_sector_name, source_type)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_smart_selection_sector_heat_daily_date
                ON smart_selection_sector_heat_daily(board_date, rank_order)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_smart_selection_sector_heat_daily_sector
                ON smart_selection_sector_heat_daily(normalized_sector_name, board_date DESC)
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _get_metadata(self, key: str) -> Optional[str]:
        conn = self._connect()
        try:
            return get_metadata(conn, key)
        finally:
            conn.close()

    def _set_metadata(self, key: str, value: str) -> None:
        conn = self._connect()
        try:
            set_metadata(conn, key, value)
            conn.commit()
        finally:
            conn.close()

    def get_scheduler_config(self) -> dict[str, Any]:
        enabled_raw = self._get_metadata(SMART_SELECTION_SCHEDULER_ENABLED_KEY)
        time_raw = self._get_metadata(SMART_SELECTION_SCHEDULER_TIME_KEY)
        max_workers_raw = self._get_metadata(SMART_SELECTION_MAX_WORKERS_KEY)
        return {
            "enabled": str(enabled_raw or "0").strip() == "1",
            "schedule_time": str(time_raw or DEFAULT_SCHEDULE_TIME).strip() or DEFAULT_SCHEDULE_TIME,
            "max_workers": max(1, _safe_int(max_workers_raw, DEFAULT_MAX_WORKERS)),
        }

    def update_scheduler_config(self, *, enabled: bool, schedule_time: str, max_workers: Optional[int] = None) -> dict[str, Any]:
        self._set_metadata(SMART_SELECTION_SCHEDULER_ENABLED_KEY, "1" if enabled else "0")
        self._set_metadata(SMART_SELECTION_SCHEDULER_TIME_KEY, str(schedule_time or DEFAULT_SCHEDULE_TIME).strip() or DEFAULT_SCHEDULE_TIME)
        if max_workers is not None:
            self._set_metadata(SMART_SELECTION_MAX_WORKERS_KEY, str(max(1, int(max_workers))))
        config = self.get_scheduler_config()
        smart_selection_scheduler.apply_runtime_config(config)
        return smart_selection_scheduler.get_status()

    def _insert_run(self, *, trigger_source: str, lightweight_model: Optional[str], reasoning_model: Optional[str]) -> str:
        run_id = uuid.uuid4().hex
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO smart_selection_runs (
                    run_id, status, message, trigger_source, lightweight_model, reasoning_model
                )
                VALUES (?, 'queued', ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "等待智能选股任务开始执行",
                    str(trigger_source or "manual"),
                    lightweight_model,
                    reasoning_model,
                ),
            )
            conn.commit()
            return run_id
        finally:
            conn.close()

    def _update_run(self, run_id: str, **updates: Any) -> None:
        if not updates:
            return
        with self._db_lock:
            conn = self._connect()
            try:
                fields = []
                values = []
                for key, value in updates.items():
                    if key in {"result_summary_json", "warnings_json"} and isinstance(value, (dict, list)):
                        value = json.dumps(value, ensure_ascii=False)
                    fields.append(f"{key} = ?")
                    values.append(value)
                fields.append("updated_at = ?")
                values.append(_now_text())
                values.append(run_id)
                conn.execute(
                    f"UPDATE smart_selection_runs SET {', '.join(fields)} WHERE run_id = ?",
                    tuple(values),
                )
                conn.commit()
            finally:
                conn.close()

    def _replace_run_items(self, run_id: str, bucket: str, items: list[dict[str, Any]]) -> None:
        with self._db_lock:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM smart_selection_run_items WHERE run_id = ? AND bucket = ?", (run_id, bucket))
                for index, item in enumerate(items, 1):
                    snapshot = dict(item)
                    cursor.execute(
                        """
                        INSERT INTO smart_selection_run_items (
                            run_id, bucket, symbol, name, primary_sector, lifecycle_stage,
                            defense_line_type, score, heat_score, rank_order, reason, snapshot_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            bucket,
                            item.get("symbol"),
                            item.get("name"),
                            item.get("primary_sector"),
                            item.get("lifecycle_stage"),
                            item.get("defense_line_type"),
                            _safe_float(item.get("score"), 0.0),
                            _safe_float(item.get("heat_score"), 0.0),
                            index,
                            item.get("reason"),
                            json.dumps(snapshot, ensure_ascii=False),
                        ),
                    )
                conn.commit()
            finally:
                conn.close()

    def _load_run_items(self, run_id: str) -> dict[str, list[dict[str, Any]]]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT bucket, snapshot_json
                FROM smart_selection_run_items
                WHERE run_id = ?
                ORDER BY bucket ASC, rank_order ASC, id ASC
                """,
                (run_id,),
            )
            grouped: dict[str, list[dict[str, Any]]] = {}
            for bucket, snapshot_json in cursor.fetchall():
                grouped.setdefault(bucket, []).append(_safe_json_loads(snapshot_json, {}))
            return grouped
        finally:
            conn.close()

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM smart_selection_runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            payload = dict(zip(columns, row))
        finally:
            conn.close()

        payload["sector_report_reused"] = bool(payload.get("sector_report_reused", 0))
        payload["result_summary"] = _safe_json_loads(payload.pop("result_summary_json", None), {})
        payload["warnings"] = _safe_json_loads(payload.pop("warnings_json", None), [])
        items = self._load_run_items(run_id)
        payload["result"] = {
            **(payload.get("result_summary") or {}),
            "matched_candidates": items.get("matched_candidates", []),
            "observed_startup_candidates": items.get("observed_startup_candidates", []),
            "observe_candidates": items.get("observe_candidates", []),
            "external_discovery_candidates": items.get("external_discovery_candidates", []),
            "observed_decay_candidates": items.get("observed_decay_candidates", []),
            "ranked_action_candidates": items.get("ranked_action_candidates", []),
            "excluded_by_execution_gate": items.get("excluded_by_execution_gate", []),
            "final_selected": items.get("final_selected", []),
            "excluded_by_lifecycle_veto": items.get("excluded_by_lifecycle_veto", []),
            "excluded_by_risk_veto": items.get("excluded_by_risk_veto", []),
        }
        return payload

    def get_latest_run(self) -> Optional[dict[str, Any]]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT run_id
                FROM smart_selection_runs
                ORDER BY datetime(created_at) DESC, rowid DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self.get_run(row[0])
        finally:
            conn.close()

    def get_active_run(self) -> Optional[dict[str, Any]]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT run_id
                FROM smart_selection_runs
                WHERE status IN ('queued', 'running')
                ORDER BY datetime(created_at) DESC, rowid DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self.get_run(row[0])
        finally:
            conn.close()

    def submit_run(
        self,
        *,
        trigger_source: str = "manual",
        lightweight_model: Optional[str] = None,
        reasoning_model: Optional[str] = None,
    ) -> str:
        active = self.get_active_run()
        if active:
            return str(active["run_id"])

        run_id = self._insert_run(
            trigger_source=trigger_source,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        worker = threading.Thread(
            target=self._execute_run,
            args=(run_id, trigger_source, lightweight_model, reasoning_model),
            daemon=True,
            name=f"smart-selection-{run_id[:8]}",
        )
        self._threads[run_id] = worker
        worker.start()
        return run_id

    def _build_score_fusion(
        self,
        *,
        agent_composite_score: float,
        execution_composite_score: float,
        metrics: dict[str, Any],
    ) -> dict[str, Any]:
        market_state = str(metrics.get("market_state") or "")
        score_scale = AGENT_COMPOSITE_SCORE_SCALE_BY_STATE.get(market_state, 2.0)
        normalized_agent_score = _clamp_score(agent_composite_score / max(_safe_float(score_scale, 2.0), 1.0))
        execution_weight_by_state = {
            "momentum": 0.44,
            "momo": 0.44,
            "range": 0.38,
            "ice": 0.30,
            "retreat": 0.34,
        }
        execution_weight = execution_weight_by_state.get(market_state, 0.38)
        agent_weight = 1.0 - execution_weight
        readiness_adjustment = 0.0
        if bool(metrics.get("tail_session")):
            readiness_adjustment += 4.0
        else:
            readiness_adjustment -= 5.0
        realtime_freshness = metrics.get("realtime_freshness") if isinstance(metrics.get("realtime_freshness"), dict) else {}
        if realtime_freshness.get("intraday_decision_ready") is True:
            readiness_adjustment += 3.0
        else:
            readiness_adjustment -= 4.0
        fusion_score = round(
            normalized_agent_score * agent_weight
            + _clamp_score(execution_composite_score) * execution_weight
            + readiness_adjustment,
            2,
        )
        return {
            "raw_agent_composite_score": round(agent_composite_score, 2),
            "agent_composite_score": round(normalized_agent_score, 2),
            "agent_score_scale": round(score_scale, 2),
            "execution_composite_score": round(_clamp_score(execution_composite_score), 2),
            "score_fusion_weights": {
                "agent": round(agent_weight, 2),
                "execution": round(execution_weight, 2),
            },
            "readiness_adjustment": round(readiness_adjustment, 2),
            "fusion_score": fusion_score,
        }

    def _sync_news_flow_context(self, warnings: list[str]) -> dict[str, Any]:
        try:
            from news_flow_engine import news_flow_engine
        except Exception as exc:
            warnings.append(f"新闻情绪同步不可用: {exc}")
            return {"success": False, "error": str(exc)}

        try:
            result = news_flow_engine.run_quick_analysis(category="finance")
        except Exception as exc:
            warnings.append(f"新闻情绪同步失败，改用最近快照: {exc}")
            return {"success": False, "error": str(exc)}

        if not isinstance(result, dict):
            warnings.append("新闻情绪同步返回异常，改用最近快照")
            return {"success": False, "error": "invalid_result"}
        if not result.get("success"):
            error_text = str(result.get("error") or "未知错误")
            warnings.append(f"新闻情绪同步失败，改用最近快照: {error_text}")
            return result
        if result.get("data_warning"):
            warnings.append(str(result.get("data_warning")))
        return result

    def _sync_longhubang_data(self, warnings: list[str], now: Optional[datetime] = None) -> dict[str, Any]:
        current = now or local_now()
        if current.weekday() >= 5:
            return {
                "attempted": False,
                "reason": "非交易日，龙虎榜不主动更新",
                "available_after": "17:30",
            }
        if current.time() < time(17, 30):
            return {
                "attempted": False,
                "reason": "未到 17:30，龙虎榜当日数据通常尚未发布",
                "available_after": "17:30",
            }

        trade_date = current.strftime("%Y-%m-%d")
        try:
            from longhubang_data import LonghubangDataFetcher
            from longhubang_db import LonghubangDatabase
        except Exception as exc:
            warnings.append(f"龙虎榜同步不可用: {exc}")
            return {"attempted": True, "success": False, "date": trade_date, "error": str(exc)}

        try:
            raw_result = LonghubangDataFetcher().get_longhubang_data(trade_date)
            data_list = raw_result.get("data", []) if isinstance(raw_result, dict) else []
            if not data_list:
                warnings.append(f"龙虎榜 {trade_date} 未获取到当日数据，继续使用本地近 3 日记录")
                return {
                    "attempted": True,
                    "success": False,
                    "date": trade_date,
                    "records": 0,
                    "saved": 0,
                    "reason": "empty",
                }
            saved_count = LonghubangDatabase().save_longhubang_data(data_list)
            return {
                "attempted": True,
                "success": True,
                "date": trade_date,
                "records": len(data_list),
                "saved": int(saved_count or 0),
            }
        except Exception as exc:
            warnings.append(f"龙虎榜同步失败，继续使用本地近 3 日记录: {exc}")
            return {"attempted": True, "success": False, "date": trade_date, "error": str(exc)}

    def _load_news_flow_context(self, warnings: list[str]) -> dict[str, Any]:
        try:
            from news_flow_db import news_flow_db
        except Exception as exc:
            warnings.append(f"新闻流上下文不可用: {exc}")
            return {"available": False, "fresh": False, "status": "unavailable", "reason": str(exc)}

        try:
            snapshot = news_flow_db.get_latest_snapshot()
        except Exception as exc:
            warnings.append(f"新闻流快照读取失败: {exc}")
            return {"available": False, "fresh": False, "status": "unavailable", "reason": str(exc)}
        if not snapshot:
            return {"available": False, "fresh": False, "status": "missing", "reason": "暂无新闻流快照"}

        try:
            detail = news_flow_db.get_snapshot_detail(int(snapshot.get("id") or 0)) if snapshot.get("id") else {}
        except Exception as exc:
            warnings.append(f"新闻流详情读取失败: {exc}")
            detail = {}

        snapshot_time = snapshot.get("fetch_time") or snapshot.get("created_at")
        parsed_time = parse_display_timestamp(snapshot_time)
        age_hours = None
        fresh = False
        if parsed_time is not None:
            age_hours = max(0.0, (local_now() - parsed_time).total_seconds() / 3600.0)
            fresh = age_hours <= NEWS_FLOW_CONTEXT_MAX_AGE_HOURS

        sentiment = detail.get("sentiment") if isinstance(detail, dict) else None
        if not sentiment:
            try:
                sentiment = news_flow_db.get_latest_sentiment()
            except Exception:
                sentiment = None
        try:
            sentiment_history = news_flow_db.get_sentiment_history(limit=3)
        except Exception:
            sentiment_history = []

        return {
            "available": True,
            "fresh": fresh,
            "status": "fresh" if fresh else "stale",
            "age_hours": round(age_hours, 2) if age_hours is not None else None,
            "snapshot": snapshot,
            "sentiment": sentiment if isinstance(sentiment, dict) else {},
            "sentiment_history": sentiment_history if isinstance(sentiment_history, list) else [],
            "hot_topics": detail.get("hot_topics") if isinstance(detail, dict) and isinstance(detail.get("hot_topics"), list) else [],
            "stock_news": detail.get("stock_news") if isinstance(detail, dict) and isinstance(detail.get("stock_news"), list) else [],
        }

    @staticmethod
    def _text_matches_any_theme(text: Any, themes: list[str]) -> bool:
        normalized_text = research_hub_service._normalize_sector_text(text)
        if not normalized_text:
            return False
        for theme in themes:
            normalized_theme = research_hub_service._normalize_sector_text(theme)
            if not normalized_theme:
                continue
            if normalized_theme in normalized_text or normalized_text in normalized_theme:
                return True
            shorter, longer = sorted((normalized_theme, normalized_text), key=len)
            if len(shorter) >= 2 and _is_ordered_subsequence(shorter, longer):
                return True
        return False

    def _build_news_sentiment_confirmation(
        self,
        candidate: dict[str, Any],
        lifecycle_item: dict[str, Any],
        news_context: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        news_context = news_context if isinstance(news_context, dict) else {}
        if not news_context.get("available"):
            return {
                "news_confirmation_score": 48.0,
                "sentiment_confirmation_score": 50.0,
                "context_confirmation_score": 48.5,
                "news_context_status": str(news_context.get("status") or "missing"),
                "news_context_fresh": False,
                "news_risk_flag": False,
                "news_positive_hits": [],
                "news_negative_hits": [],
                "topic_hits": [],
                "sentiment_note": str(news_context.get("reason") or "暂无新闻流上下文"),
            }

        theme_candidates = [
            candidate.get("name"),
            candidate.get("symbol"),
            candidate.get("primary_sector"),
            candidate.get("canonical_sector"),
            lifecycle_item.get("sector_name"),
        ]
        for match in candidate.get("matched_sectors") or []:
            if isinstance(match, dict):
                theme_candidates.extend([match.get("sector"), match.get("canonical_sector")])
        themes = [str(item).strip() for item in theme_candidates if str(item or "").strip()]

        hot_topics = [item for item in news_context.get("hot_topics") or [] if isinstance(item, dict)]
        stock_news = [item for item in news_context.get("stock_news") or [] if isinstance(item, dict)]
        topic_hits: list[dict[str, Any]] = []
        for topic in hot_topics[:30]:
            topic_text = topic.get("topic")
            if not self._text_matches_any_theme(topic_text, themes):
                continue
            topic_hits.append(
                {
                    "topic": topic_text,
                    "heat": _safe_float(topic.get("heat"), 0.0),
                    "count": _safe_int(topic.get("count"), 0),
                }
            )
        topic_hits.sort(key=lambda item: (_safe_float(item.get("heat"), 0.0), _safe_int(item.get("count"), 0)), reverse=True)

        positive_hits: list[dict[str, Any]] = []
        negative_hits: list[dict[str, Any]] = []
        for news in stock_news[:80]:
            text = " ".join(
                [
                    str(news.get("title") or ""),
                    str(news.get("content") or ""),
                    " ".join(str(item or "") for item in news.get("matched_keywords") or []),
                ]
            )
            if not self._text_matches_any_theme(text, themes):
                continue
            hit_payload = {
                "title": _safe_text(news.get("title"))[:120],
                "score": _safe_float(news.get("score"), 0.0),
                "cross_platform": _safe_int(news.get("cross_platform"), 1),
            }
            if any(keyword in text for keyword in NEWS_RISK_KEYWORDS):
                negative_hits.append(hit_payload)
            else:
                positive_hits.append(hit_payload)
        positive_hits.sort(key=lambda item: (_safe_int(item.get("cross_platform"), 1), _safe_float(item.get("score"), 0.0)), reverse=True)
        negative_hits.sort(key=lambda item: (_safe_int(item.get("cross_platform"), 1), _safe_float(item.get("score"), 0.0)), reverse=True)

        sentiment = news_context.get("sentiment") if isinstance(news_context.get("sentiment"), dict) else {}
        sentiment_index = _safe_float(sentiment.get("sentiment_index"), 50.0)
        flow_stage = str(sentiment.get("flow_stage") or "").strip()
        flow_level = str((news_context.get("snapshot") or {}).get("flow_level") or "").strip()
        total_score = _safe_float((news_context.get("snapshot") or {}).get("total_score"), 50.0)

        if sentiment_index < 25:
            sentiment_score = 30.0
        elif sentiment_index < 40:
            sentiment_score = 48.0
        elif sentiment_index <= 75:
            sentiment_score = 68.0
        elif sentiment_index <= 85:
            sentiment_score = 62.0
        else:
            sentiment_score = 46.0

        if any(label in flow_stage for label in ("启动", "加速")):
            sentiment_score += 8.0
        elif any(label in flow_stage for label in ("一致", "高潮")) and sentiment_index >= 80:
            sentiment_score -= 6.0
        elif any(label in flow_stage for label in ("退潮", "衰退")):
            sentiment_score -= 12.0
        sentiment_score = round(_clamp_score(sentiment_score), 2)

        matched_topic_heat = max((_safe_float(item.get("heat"), 0.0) for item in topic_hits), default=0.0)
        topic_score = 45.0 + min(35.0, matched_topic_heat * 0.35) if topic_hits else 42.0
        if flow_level in {"高", "极高"} or total_score >= 75:
            topic_score += 4.0
        if not bool(news_context.get("fresh")):
            topic_score -= 6.0

        news_score = 50.0
        if positive_hits:
            best_positive = max(_safe_float(item.get("score"), 0.0) for item in positive_hits)
            news_score += min(25.0, 8.0 + best_positive * 0.25)
        elif topic_hits:
            news_score += 8.0
        else:
            news_score -= 4.0
        if negative_hits:
            news_score -= min(40.0, 18.0 + len(negative_hits) * 6.0)
        if not bool(news_context.get("fresh")):
            news_score -= 4.0
        news_score = round(_clamp_score(news_score), 2)
        topic_score = round(_clamp_score(topic_score), 2)
        context_score = round(_clamp_score(news_score * 0.42 + topic_score * 0.33 + sentiment_score * 0.25), 2)

        return {
            "news_confirmation_score": news_score,
            "sentiment_confirmation_score": sentiment_score,
            "topic_confirmation_score": topic_score,
            "context_confirmation_score": context_score,
            "news_context_status": str(news_context.get("status") or "fresh"),
            "news_context_fresh": bool(news_context.get("fresh")),
            "news_context_age_hours": news_context.get("age_hours"),
            "news_risk_flag": bool(negative_hits),
            "news_positive_hits": positive_hits[:3],
            "news_negative_hits": negative_hits[:3],
            "topic_hits": topic_hits[:3],
            "sentiment_note": f"情绪{sentiment_index:.0f}，阶段{flow_stage or '未知'}，流量{flow_level or '未知'}",
        }

    @staticmethod
    def _summarize_news_flow_context(news_context: dict[str, Any]) -> dict[str, Any]:
        news_context = news_context if isinstance(news_context, dict) else {}
        sentiment = news_context.get("sentiment") if isinstance(news_context.get("sentiment"), dict) else {}
        snapshot = news_context.get("snapshot") if isinstance(news_context.get("snapshot"), dict) else {}
        return {
            "available": bool(news_context.get("available")),
            "fresh": bool(news_context.get("fresh")),
            "status": str(news_context.get("status") or ""),
            "age_hours": news_context.get("age_hours"),
            "sentiment_index": sentiment.get("sentiment_index"),
            "sentiment_class": sentiment.get("sentiment_class"),
            "flow_stage": sentiment.get("flow_stage"),
            "flow_level": snapshot.get("flow_level"),
            "total_score": snapshot.get("total_score"),
            "hot_topic_count": len(news_context.get("hot_topics") or []),
            "stock_news_count": len(news_context.get("stock_news") or []),
        }

    def _build_candidate_metrics(
        self,
        candidate: dict[str, Any],
        lifecycle_item: dict[str, Any],
        news_context: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        metrics = candidate.get("technical_metrics") or {}
        anticipation_score = round((_safe_float(candidate.get("heat_score")) * 0.55) + (_safe_float(metrics.get("trend_score")) * 0.25) + (_safe_float(metrics.get("chip_score")) * 0.2), 2)
        washout_score = round((_safe_float(metrics.get("reversal_score")) * 0.5) + (_safe_float(metrics.get("mean_reversion_score")) * 0.5), 2)
        volume_contraction_days = _safe_int(metrics.get("volume_contraction_days"))
        shrinkage_score = round(min(100.0, _safe_float(metrics.get("volume_score")) * 0.35 + volume_contraction_days * 16 + max(0.0, 12 - abs(_safe_float(metrics.get("bias_pct")))) * 2.2), 2)
        relative_strength_score = round((_safe_float(metrics.get("order_flow_score")) * 0.45) + (_safe_float(metrics.get("chip_score")) * 0.25) + (_safe_float(metrics.get("trend_score")) * 0.3), 2)
        tail_confirmation_score = round((_safe_float(metrics.get("intraday_score")) * 0.75) + (10 if str(metrics.get("intraday_bias") or "") in {"trend_continuation", "pullback_support"} else 0), 2)
        fund_confirmation_score = round(
            _safe_float(metrics.get("order_flow_score"), 50.0) * 0.50
            + _score_range(metrics.get("main_net_pct"), low=-20.0, high=20.0, default=50.0) * 0.25
            + _score_range(metrics.get("order_book_imbalance"), low=-0.35, high=0.35, default=50.0) * 0.15
            + _safe_float(metrics.get("volume_score"), 50.0) * 0.10,
            2,
        )
        context_confirmation = self._build_news_sentiment_confirmation(candidate, lifecycle_item, news_context)
        context_confirmation_score = _safe_float(context_confirmation.get("context_confirmation_score"), 48.0)
        distribution_penalty = round(_safe_float(metrics.get("distribution_risk")), 2)
        execution_composite_score = round(
            anticipation_score * 0.14
            + washout_score * 0.14
            + shrinkage_score * 0.16
            + relative_strength_score * 0.18
            + tail_confirmation_score * 0.20
            + fund_confirmation_score * 0.12
            + context_confirmation_score * 0.06
            - distribution_penalty * 0.2,
            2,
        )
        fusion_payload = self._build_score_fusion(
            agent_composite_score=_safe_float(candidate.get("composite_score"), 0.0),
            execution_composite_score=execution_composite_score,
            metrics=metrics,
        )
        return {
            "anticipation_score": anticipation_score,
            "washout_score": washout_score,
            "shrinkage_score": shrinkage_score,
            "relative_strength_score": relative_strength_score,
            "tail_confirmation_score": tail_confirmation_score,
            "fund_confirmation_score": fund_confirmation_score,
            "news_confirmation_score": context_confirmation.get("news_confirmation_score"),
            "sentiment_confirmation_score": context_confirmation.get("sentiment_confirmation_score"),
            "topic_confirmation_score": context_confirmation.get("topic_confirmation_score"),
            "context_confirmation_score": context_confirmation_score,
            "gate_score": round(
                shrinkage_score * 0.25
                + relative_strength_score * 0.25
                + tail_confirmation_score * 0.25
                + fund_confirmation_score * 0.15
                + context_confirmation_score * 0.10,
                2,
            ),
            "distribution_penalty": distribution_penalty,
            "execution_composite_score": execution_composite_score,
            "raw_agent_composite_score": fusion_payload.get("raw_agent_composite_score"),
            "agent_composite_score": fusion_payload.get("agent_composite_score"),
            "agent_score_scale": fusion_payload.get("agent_score_scale"),
            "score_fusion_weights": fusion_payload.get("score_fusion_weights"),
            "readiness_adjustment": fusion_payload.get("readiness_adjustment"),
            "composite_score": fusion_payload.get("fusion_score"),
            "volume_contraction_days": volume_contraction_days,
            "tail_session": bool(metrics.get("tail_session")),
            "latest_minute_time": str(metrics.get("latest_minute_time") or ""),
            "latest_trade_time": str(metrics.get("latest_trade_time") or ""),
            "realtime_freshness": metrics.get("realtime_freshness") if isinstance(metrics.get("realtime_freshness"), dict) else {},
            "lifecycle_stage": lifecycle_item.get("lifecycle_stage") or "neutral",
            "defense_line_type": lifecycle_item.get("defense_line_type") or "NONE",
            "selection_veto": bool(lifecycle_item.get("selection_veto")),
            "trajectory": lifecycle_item.get("trajectory") or [],
            "delta_1": lifecycle_item.get("delta_1"),
            "delta_2": lifecycle_item.get("delta_2"),
            "action_hint": lifecycle_item.get("action_hint") or "",
            "market_state": str(metrics.get("market_state") or ""),
            "market_state_label": str(metrics.get("market_state_label") or ""),
            "match_score": round(_safe_float(candidate.get("match_score"), 0.0), 2),
            "news_context_status": context_confirmation.get("news_context_status"),
            "news_context_fresh": bool(context_confirmation.get("news_context_fresh")),
            "news_context_age_hours": context_confirmation.get("news_context_age_hours"),
            "news_risk_flag": bool(context_confirmation.get("news_risk_flag")),
            "news_positive_hits": context_confirmation.get("news_positive_hits") or [],
            "news_negative_hits": context_confirmation.get("news_negative_hits") or [],
            "topic_hits": context_confirmation.get("topic_hits") or [],
            "sentiment_note": context_confirmation.get("sentiment_note") or "",
        }

    def _get_execution_gate_policy(self, item: dict[str, Any], *, strict_execution_mode: bool) -> dict[str, Any]:
        market_state = str(item.get("market_state") or "").strip()
        policy = dict(EXECUTION_GATE_POLICY_BY_STATE.get(market_state, DEFAULT_EXECUTION_GATE_POLICY))
        policy["execution_gate_threshold"] = (
            policy["strict_threshold"] if strict_execution_mode else policy["manual_threshold"]
        )
        policy["min_component_floor"] = policy["strict_floor"] if strict_execution_mode else policy["manual_floor"]
        policy["distribution_risk_max"] = (
            policy["strict_distribution_max"] if strict_execution_mode else policy["manual_distribution_max"]
        )
        return policy

    def _resolve_run_session(self, trigger_source: str, now: Optional[datetime] = None) -> dict[str, Any]:
        current = now or local_now()
        current_time = current.time()
        is_trading_day = current.weekday() < 5
        base = {
            "trigger_source": _normalize_trigger_source(trigger_source),
            "timestamp": current.strftime("%Y-%m-%d %H:%M:%S"),
            "time": current.strftime("%H:%M"),
            "is_trading_day": is_trading_day,
            "can_trade_now": False,
            "allow_final_selection": False,
            "requires_tail": True,
            "requires_freshness": True,
            "post_close_review": False,
        }

        if _normalize_trigger_source(trigger_source) in STRICT_EXECUTION_TRIGGER_SOURCES:
            return {
                **base,
                "mode": "strict_tail_execution",
                "label": "定时尾盘执行",
                "can_trade_now": is_trading_day and time(14, 30) <= current_time <= time(15, 0),
                "allow_final_selection": True,
                "recommendation": "定时任务按严格尾盘执行口径筛选",
            }

        if not is_trading_day:
            return {
                **base,
                "mode": "non_trading_review",
                "label": "休市复盘",
                "requires_tail": False,
                "requires_freshness": False,
                "post_close_review": True,
                "allow_final_selection": True,
                "recommendation": "休市手动触发只生成复盘候选，不能视为即时交易信号",
            }
        if current_time < time(9, 30):
            return {
                **base,
                "mode": "pre_market_review",
                "label": "开盘前",
                "requires_tail": False,
                "requires_freshness": False,
                "recommendation": "开盘前未形成当日盘面确认，仅输出观察和补池线索",
            }
        if time(9, 30) <= current_time < time(11, 30):
            return {
                **base,
                "mode": "morning_preview",
                "label": "上午盘",
                "can_trade_now": True,
                "requires_tail": False,
                "requires_freshness": False,
                "recommendation": "上午盘手动触发不进入执行清单，仅做盘中预选观察",
            }
        if time(11, 30) <= current_time < time(13, 0):
            return {
                **base,
                "mode": "lunch_review",
                "label": "午间休市",
                "requires_tail": False,
                "requires_freshness": False,
                "recommendation": "午间休市可复核上午强弱，但未到尾盘，不进入执行清单",
            }
        if time(13, 0) <= current_time < time(14, 30):
            return {
                **base,
                "mode": "afternoon_preview",
                "label": "下午盘非尾盘",
                "can_trade_now": True,
                "requires_tail": False,
                "requires_freshness": False,
                "recommendation": "尚未到 14:30 尾盘确认窗口，仅输出观察级候选",
            }
        if time(14, 30) <= current_time <= time(15, 0):
            return {
                **base,
                "mode": "tail_execution",
                "label": "尾盘",
                "can_trade_now": True,
                "allow_final_selection": True,
                "recommendation": "当前处于尾盘窗口，可按执行门槛生成最终清单",
            }
        return {
            **base,
            "mode": "post_close_review",
            "label": "盘后复盘",
            "requires_tail": False,
            "requires_freshness": False,
            "post_close_review": True,
            "allow_final_selection": True,
            "recommendation": "收盘后只能生成次日候选，次日开盘前需要重新确认",
        }

    @staticmethod
    def _has_tail_or_close_snapshot(item: dict[str, Any]) -> bool:
        if bool(item.get("tail_session")):
            return True
        for key in ("latest_minute_time", "latest_trade_time"):
            text = str(item.get(key) or "").strip()
            match = re.search(r"(\d{1,2}:\d{2})", text)
            if match and match.group(1) >= "14:30":
                return True
        return False

    def _evaluate_execution_gate(
        self,
        item: dict[str, Any],
        *,
        run_session: dict[str, Any],
        strict_execution_mode: bool,
    ) -> dict[str, Any]:
        gate_policy = self._get_execution_gate_policy(item, strict_execution_mode=strict_execution_mode)
        threshold = _safe_float(gate_policy.get("execution_gate_threshold"), 50.0)
        min_floor = _safe_float(gate_policy.get("min_component_floor"), 40.0)
        distribution_max = _safe_float(gate_policy.get("distribution_risk_max"), 72.0)
        distribution_hard_max = distribution_max + (4.0 if strict_execution_mode else 5.0)
        observe_threshold = max(38.0, threshold - 10.0)

        realtime_freshness = item.get("realtime_freshness") if isinstance(item.get("realtime_freshness"), dict) else {}
        gate_score = _safe_float(item.get("gate_score"), 0.0)
        distribution_risk = _safe_float(item.get("distribution_penalty"), 0.0)
        hard_blocks: list[dict[str, str]] = []
        soft_notes: list[dict[str, Any]] = []
        penalty = 0.0

        def add_hard(block_type: str, reason: str) -> None:
            hard_blocks.append({"type": block_type, "reason": reason})

        def add_soft(note_type: str, reason: str, value: float) -> None:
            nonlocal penalty
            penalty += max(0.0, value)
            soft_notes.append({"type": note_type, "reason": reason, "penalty": round(max(0.0, value), 2)})

        if not bool(run_session.get("allow_final_selection")):
            add_hard("session_time", f"{run_session.get('label') or '非执行时段'}，未到尾盘执行窗口，降级观察")

        post_close_review = bool(run_session.get("post_close_review"))
        if post_close_review:
            review_ready = realtime_freshness.get("intraday_review_ready") is True
            has_tail_or_close_snapshot = (
                realtime_freshness.get("has_tail_or_close_snapshot") is True
                or self._has_tail_or_close_snapshot(item)
            )
            if not (review_ready or has_tail_or_close_snapshot):
                add_hard("post_close_snapshot", "盘后缺少尾盘/收盘快照，降级观察")
        elif bool(run_session.get("requires_tail", True)) and not bool(item.get("tail_session")):
            add_hard("tail_session", "未到尾盘执行时段，降级观察")

        freshness_status = str(realtime_freshness.get("overall_status") or "").strip()
        minute_quality = realtime_freshness.get("minute_quality") if isinstance(realtime_freshness.get("minute_quality"), dict) else {}
        minute_quality_status = str(minute_quality.get("status") or "").strip()
        if bool(run_session.get("requires_freshness", True)):
            if realtime_freshness.get("intraday_decision_ready") is True:
                pass
            elif freshness_status in {"degraded", "review_ready"}:
                add_soft("realtime_freshness", "分时新鲜度一般，保留但降低执行分", 4.0)
            elif freshness_status in {"stale", "unavailable", ""}:
                add_hard("realtime_freshness", "分时新鲜度不可验证，降级观察")
            else:
                add_soft("realtime_freshness", "分时状态未完全确认，降低执行分", 3.0)
        elif post_close_review and realtime_freshness.get("intraday_review_ready") is not True:
            add_soft("realtime_review", "盘后分时复盘口径未完全确认，降低次日候选优先级", 3.0)

        if minute_quality_status == "poor":
            add_hard("minute_quality", "分时覆盖质量较差，降级观察")
        elif minute_quality_status == "fair":
            add_soft("minute_quality", "分时覆盖存在少量缺口", 2.0)

        component_fields = [
            ("shrinkage_score", "缩量承接"),
            ("relative_strength_score", "相对强度"),
            ("tail_confirmation_score", "尾盘确认"),
            ("fund_confirmation_score", "资金确认"),
            ("context_confirmation_score", "新闻情绪确认"),
        ]
        component_deficits: list[str] = []
        for key, label in component_fields:
            score = _safe_float(item.get(key), 0.0)
            if score < min_floor:
                deficit = min_floor - score
                component_deficits.append(f"{label}低于{min_floor:.0f}")
                add_soft(key, f"{label}不足", min(8.0, deficit * 0.35))

        if distribution_risk > distribution_hard_max:
            add_hard(
                "distribution_risk",
                f"派发风险过高，风险分 {distribution_risk:.1f} > {distribution_hard_max:.0f}",
            )
        elif distribution_risk > distribution_max:
            add_soft(
                "distribution_risk",
                f"派发风险偏高，风险分 {distribution_risk:.1f} > {distribution_max:.0f}",
                min(10.0, (distribution_risk - distribution_max) * 0.8),
            )

        news_context_status = str(item.get("news_context_status") or "").strip()
        if bool(item.get("news_risk_flag")):
            add_hard("news_risk", "新闻/公告流出现负面风险词，降级观察")
        elif news_context_status in {"missing", "unavailable"} and strict_execution_mode:
            add_soft("news_context", "严格尾盘缺少新闻情绪上下文", 2.5)
        elif news_context_status == "stale":
            add_soft("news_context", "新闻情绪上下文已过期", 2.0)

        adjusted_score = round(max(0.0, gate_score - penalty), 2)
        if hard_blocks:
            decision = "observe"
        elif adjusted_score >= threshold:
            decision = "execute"
        elif adjusted_score >= observe_threshold or _safe_float(item.get("match_score"), 0.0) >= 0.9:
            decision = "observe"
        else:
            decision = "observe"
            add_hard("gate_score", f"调整后执行分 {adjusted_score:.2f} < {threshold:.0f}")

        primary_reason = ""
        primary_type = ""
        if hard_blocks:
            primary_type = hard_blocks[0]["type"]
            primary_reason = hard_blocks[0]["reason"]
        elif decision != "execute":
            primary_type = "gate_score"
            primary_reason = f"调整后执行分 {adjusted_score:.2f} < {threshold:.0f}，降级观察"

        return {
            "decision": decision,
            "primary_type": primary_type,
            "primary_reason": primary_reason,
            "hard_blocks": hard_blocks,
            "soft_notes": soft_notes,
            "component_deficits": component_deficits,
            "base_gate_score": round(gate_score, 2),
            "adjusted_gate_score": adjusted_score,
            "soft_penalty": round(penalty, 2),
            "threshold": round(threshold, 2),
            "observe_threshold": round(observe_threshold, 2),
            "min_component_floor": round(min_floor, 2),
            "distribution_risk_max": round(distribution_max, 2),
            "distribution_risk_hard_max": round(distribution_hard_max, 2),
            "sector_limit": _safe_int(gate_policy.get("sector_limit"), 2),
        }

    def _format_result_item(self, candidate: dict[str, Any], lifecycle_item: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        return {
            "asset_id": candidate.get("asset_id"),
            "symbol": candidate.get("symbol"),
            "name": candidate.get("name") or candidate.get("symbol"),
            "primary_sector": candidate.get("primary_sector"),
            "canonical_sector": candidate.get("canonical_sector") or candidate.get("primary_sector"),
            "matched_sectors": candidate.get("matched_sectors") or [],
            "match_score": metrics.get("match_score"),
            "score": metrics.get("composite_score"),
            "heat_score": candidate.get("heat_score"),
            "tech_score": candidate.get("tech_score"),
            "raw_agent_composite_score": metrics.get("raw_agent_composite_score"),
            "agent_composite_score": metrics.get("agent_composite_score"),
            "agent_score_scale": metrics.get("agent_score_scale"),
            "execution_composite_score": metrics.get("execution_composite_score"),
            "score_fusion_weights": metrics.get("score_fusion_weights"),
            "readiness_adjustment": metrics.get("readiness_adjustment"),
            "reason": candidate.get("reason"),
            "lifecycle_stage": metrics.get("lifecycle_stage"),
            "defense_line_type": metrics.get("defense_line_type"),
            "selection_veto": bool(metrics.get("selection_veto")),
            "trajectory": metrics.get("trajectory"),
            "delta_1": metrics.get("delta_1"),
            "delta_2": metrics.get("delta_2"),
            "action_hint": metrics.get("action_hint"),
            "anticipation_score": metrics.get("anticipation_score"),
            "washout_score": metrics.get("washout_score"),
            "shrinkage_score": metrics.get("shrinkage_score"),
            "relative_strength_score": metrics.get("relative_strength_score"),
            "tail_confirmation_score": metrics.get("tail_confirmation_score"),
            "fund_confirmation_score": metrics.get("fund_confirmation_score"),
            "news_confirmation_score": metrics.get("news_confirmation_score"),
            "sentiment_confirmation_score": metrics.get("sentiment_confirmation_score"),
            "topic_confirmation_score": metrics.get("topic_confirmation_score"),
            "context_confirmation_score": metrics.get("context_confirmation_score"),
            "gate_score": metrics.get("gate_score"),
            "distribution_penalty": metrics.get("distribution_penalty"),
            "market_cap": candidate.get("market_cap"),
            "tail_session": bool(metrics.get("tail_session")),
            "latest_minute_time": metrics.get("latest_minute_time"),
            "latest_trade_time": metrics.get("latest_trade_time"),
            "realtime_freshness": metrics.get("realtime_freshness") if isinstance(metrics.get("realtime_freshness"), dict) else {},
            "market_state": metrics.get("market_state"),
            "market_state_label": metrics.get("market_state_label"),
            "news_context_status": metrics.get("news_context_status"),
            "news_context_fresh": bool(metrics.get("news_context_fresh")),
            "news_context_age_hours": metrics.get("news_context_age_hours"),
            "news_risk_flag": bool(metrics.get("news_risk_flag")),
            "news_positive_hits": metrics.get("news_positive_hits") or [],
            "news_negative_hits": metrics.get("news_negative_hits") or [],
            "topic_hits": metrics.get("topic_hits") or [],
            "sentiment_note": metrics.get("sentiment_note") or "",
        }

    def _find_lifecycle_match(
        self,
        sector_name: Any,
        lifecycle_by_name: dict[str, dict[str, Any]],
    ) -> tuple[str, Optional[dict[str, Any]]]:
        normalized_name = research_hub_service._normalize_sector_text(sector_name)
        if not normalized_name:
            return "", None
        exact_match = lifecycle_by_name.get(normalized_name)
        if exact_match:
            return normalized_name, exact_match

        fallback_candidates: list[tuple[float, str, dict[str, Any]]] = []
        for lifecycle_key, lifecycle_item in lifecycle_by_name.items():
            if not lifecycle_key:
                continue
            alignment_score = research_hub_service._score_sector_name_alignment(sector_name, lifecycle_item.get("sector_name") or lifecycle_key)
            if alignment_score >= 0.62:
                fallback_candidates.append(
                    (
                        alignment_score,
                        _safe_float(lifecycle_item.get("heat_score"), 0.0),
                        lifecycle_key,
                        lifecycle_item,
                    )
                )
        if not fallback_candidates:
            return normalized_name, None
        fallback_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        _, _, matched_key, lifecycle_item = fallback_candidates[0]
        return matched_key, lifecycle_item

    def _build_selection_sector_snapshot(
        self,
        *,
        extracted_sectors: list[dict[str, Any]],
        lifecycle_snapshot: list[dict[str, Any]],
        warnings: list[str],
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        lifecycle_by_name = {
            research_hub_service._normalize_sector_text(item.get("sector_name")): item
            for item in lifecycle_snapshot
            if item.get("sector_name")
        }

        selection_sector_store: dict[str, dict[str, Any]] = {}
        selected_lifecycle_by_name: dict[str, dict[str, Any]] = {}
        for hot_item in extracted_sectors:
            raw_sector = hot_item.get("sector")
            if _is_excluded_selection_sector(raw_sector):
                warnings.append(f"已忽略非交易主线概念: {raw_sector}")
                continue
            canonical_sector = research_hub_service._canonicalize_sector_name(hot_item.get("sector"))
            if _is_excluded_selection_sector(canonical_sector):
                warnings.append(f"已忽略非交易主线概念: {canonical_sector}")
                continue
            alias_name = research_hub_service._normalize_sector_text(canonical_sector)
            if not alias_name:
                continue
            matched_name, lifecycle_item = self._find_lifecycle_match(canonical_sector, lifecycle_by_name)
            if not lifecycle_item:
                continue
            if _is_excluded_selection_sector(lifecycle_item.get("sector_name")):
                warnings.append(f"已忽略非交易主线概念: {lifecycle_item.get('sector_name')}")
                continue
            lifecycle_stage = str(lifecycle_item.get("lifecycle_stage") or "")
            if lifecycle_stage not in HOT_LIFECYCLE_STAGES:
                continue
            matched_sector_name = matched_name or alias_name
            payload = {
                "sector": canonical_sector or lifecycle_item.get("sector_name"),
                "canonical_sector": canonical_sector or lifecycle_item.get("sector_name"),
                "lifecycle_sector": lifecycle_item.get("sector_name") or hot_item.get("sector"),
                "heat_score": max(
                    _safe_float(hot_item.get("heat_score"), 0.0),
                    _safe_float(lifecycle_item.get("heat_score"), 0.0),
                ),
                "lifecycle_stage": lifecycle_stage,
                "defense_line_type": lifecycle_item.get("defense_line_type"),
                "selection_veto": lifecycle_item.get("selection_veto"),
                "trajectory": lifecycle_item.get("trajectory"),
                "delta_1": lifecycle_item.get("delta_1"),
                "delta_2": lifecycle_item.get("delta_2"),
                "action_hint": lifecycle_item.get("action_hint"),
                "source": hot_item.get("source"),
                "aliases": hot_item.get("aliases") or [],
            }
            existing = selection_sector_store.get(matched_sector_name)
            if existing is None or _safe_float(payload.get("heat_score"), 0.0) > _safe_float(existing.get("heat_score"), 0.0):
                selection_sector_store[matched_sector_name] = payload
            selected_lifecycle_by_name[matched_sector_name] = lifecycle_item
            selected_lifecycle_by_name[alias_name] = lifecycle_item
            selected_lifecycle_by_name[research_hub_service._normalize_sector_text(lifecycle_item.get("sector_name"))] = lifecycle_item

        selection_sectors = sorted(
            selection_sector_store.values(),
            key=lambda item: _safe_float(item.get("heat_score"), 0.0),
            reverse=True,
        )

        if extracted_sectors and not selection_sectors:
            warnings.append("主线热点与生命周期候选未形成有效交集，智能选股降级为空结果")
        elif not extracted_sectors:
            warnings.append("未能从智策板块报告提取到有效主线热点，智能选股降级为空结果")
        return selection_sectors, selected_lifecycle_by_name

    def _upsert_watch_pool(self, run_id: str, items: list[dict[str, Any]]) -> None:
        if not items:
            self._cleanup_watch_pool()
            return
        with self._db_lock:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                now_text = _now_text()
                for item in items:
                    cursor.execute(
                        """
                        INSERT INTO smart_selection_watch_pool (
                            symbol, name, source_run_id, source_sector, lifecycle_stage,
                            defense_line_type, trajectory_json, reason, last_seen_at, active, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                        ON CONFLICT(symbol) DO UPDATE SET
                            name = excluded.name,
                            source_run_id = excluded.source_run_id,
                            source_sector = excluded.source_sector,
                            lifecycle_stage = excluded.lifecycle_stage,
                            defense_line_type = excluded.defense_line_type,
                            trajectory_json = excluded.trajectory_json,
                            reason = excluded.reason,
                            last_seen_at = excluded.last_seen_at,
                            active = 1,
                            updated_at = excluded.updated_at
                        """,
                        (
                            item.get("symbol"),
                            item.get("name"),
                            run_id,
                            item.get("primary_sector"),
                            item.get("lifecycle_stage"),
                            item.get("defense_line_type"),
                            json.dumps(item.get("trajectory") or [], ensure_ascii=False),
                            item.get("reason"),
                            now_text,
                            now_text,
                            now_text,
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        self._cleanup_watch_pool()

    def _cleanup_watch_pool(self) -> int:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT run_id
                FROM smart_selection_runs
                WHERE status = 'success'
                ORDER BY datetime(finished_at) DESC, datetime(created_at) DESC
                LIMIT 3
                """
            )
            recent_run_ids = [row[0] for row in cursor.fetchall()]
            if not recent_run_ids:
                return 0
            placeholders = ",".join("?" for _ in recent_run_ids)
            cursor.execute(
                f"""
                UPDATE smart_selection_watch_pool
                SET active = 0, updated_at = ?
                WHERE active = 1
                  AND COALESCE(source_run_id, '') != ''
                  AND source_run_id NOT IN ({placeholders})
                """,
                tuple([_now_text(), *recent_run_ids]),
            )
            changed = int(cursor.rowcount or 0)
            conn.commit()
            return changed
        finally:
            conn.close()

    @staticmethod
    def _resolve_board_date(report: dict[str, Any]) -> str:
        for value in (
            report.get("board_date"),
            report.get("data_date_range"),
            report.get("analysis_date"),
            report.get("created_at"),
        ):
            match = re.search(r"(\d{4}-\d{2}-\d{2})", str(value or ""))
            if match:
                return match.group(1)
        return str(report.get("analysis_date") or report.get("created_at") or "")[:10]

    def _save_sector_heat_daily_snapshot(
        self,
        *,
        run_id: str,
        sector_report_id: int,
        report: dict[str, Any],
        lifecycle_snapshot: list[dict[str, Any]],
    ) -> int:
        board_date = self._resolve_board_date(report)
        if not board_date:
            return 0

        daily_panel = self.sector_strategy_db.get_daily_heat_panel(board_date=board_date, limit=500)
        panel_items = daily_panel.get("items") if isinstance(daily_panel, dict) else []
        lifecycle_by_name = {
            str(
                item.get("normalized_sector_name")
                or research_hub_service._normalize_sector_text(item.get("sector_name"))
                or ""
            ): item
            for item in lifecycle_snapshot
            if str(
                item.get("normalized_sector_name")
                or research_hub_service._normalize_sector_text(item.get("sector_name"))
                or ""
            )
        }

        rows: list[dict[str, Any]] = []
        if isinstance(panel_items, list) and panel_items:
            for panel_item in panel_items:
                if not isinstance(panel_item, dict):
                    continue
                sector_name = str(panel_item.get("sector_name") or panel_item.get("sector") or "").strip()
                normalized_name = str(
                    panel_item.get("normalized_sector_name")
                    or research_hub_service._normalize_sector_text(sector_name)
                    or ""
                ).strip()
                if not sector_name or not normalized_name:
                    continue
                lifecycle_item = lifecycle_by_name.get(normalized_name, {})
                rows.append(
                    {
                        "sector_name": sector_name,
                        "normalized_sector_name": normalized_name,
                        "source_type": str(panel_item.get("source_type") or lifecycle_item.get("source_type") or "").strip(),
                        "heat_score": _safe_float(panel_item.get("heat_score"), 0.0),
                        "rank_order": _safe_int(panel_item.get("rank_order"), len(rows) + 1),
                        "lifecycle_stage": lifecycle_item.get("lifecycle_stage"),
                        "defense_line_type": lifecycle_item.get("defense_line_type"),
                        "delta_1": lifecycle_item.get("delta_1"),
                        "delta_2": lifecycle_item.get("delta_2"),
                        "trajectory": lifecycle_item.get("trajectory") or [],
                        "action_hint": lifecycle_item.get("action_hint") or "",
                        "selection_veto": bool(lifecycle_item.get("selection_veto")),
                    }
                )

        if not rows:
            ranked_lifecycle_items = sorted(
                [item for item in lifecycle_snapshot if isinstance(item, dict)],
                key=lambda item: _safe_float(item.get("heat_score"), 0.0),
                reverse=True,
            )
            for index, item in enumerate(ranked_lifecycle_items, 1):
                sector_name = str(item.get("sector_name") or "").strip()
                normalized_name = str(
                    item.get("normalized_sector_name")
                    or research_hub_service._normalize_sector_text(sector_name)
                    or ""
                ).strip()
                if not sector_name or not normalized_name:
                    continue
                rows.append(
                    {
                        "sector_name": sector_name,
                        "normalized_sector_name": normalized_name,
                        "source_type": str(item.get("source_type") or "").strip(),
                        "heat_score": _safe_float(item.get("heat_score"), 0.0),
                        "rank_order": index,
                        "lifecycle_stage": item.get("lifecycle_stage"),
                        "defense_line_type": item.get("defense_line_type"),
                        "delta_1": item.get("delta_1"),
                        "delta_2": item.get("delta_2"),
                        "trajectory": item.get("trajectory") or [],
                        "action_hint": item.get("action_hint") or "",
                        "selection_veto": bool(item.get("selection_veto")),
                    }
                )

        if not rows:
            return 0

        with self._db_lock:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                now_text = _now_text()
                for row in rows:
                    cursor.execute(
                        """
                        INSERT INTO smart_selection_sector_heat_daily (
                            run_id, sector_report_id, board_date, sector_name, normalized_sector_name,
                            source_type, heat_score, rank_order, lifecycle_stage, defense_line_type,
                            delta_1, delta_2, trajectory_json, action_hint, selection_veto,
                            created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(board_date, normalized_sector_name, source_type) DO UPDATE SET
                            run_id = excluded.run_id,
                            sector_report_id = excluded.sector_report_id,
                            sector_name = excluded.sector_name,
                            heat_score = excluded.heat_score,
                            rank_order = excluded.rank_order,
                            lifecycle_stage = excluded.lifecycle_stage,
                            defense_line_type = excluded.defense_line_type,
                            delta_1 = excluded.delta_1,
                            delta_2 = excluded.delta_2,
                            trajectory_json = excluded.trajectory_json,
                            action_hint = excluded.action_hint,
                            selection_veto = excluded.selection_veto,
                            updated_at = excluded.updated_at
                        """,
                        (
                            run_id,
                            int(sector_report_id or 0),
                            board_date,
                            row.get("sector_name"),
                            row.get("normalized_sector_name"),
                            str(row.get("source_type") or ""),
                            _safe_float(row.get("heat_score"), 0.0),
                            _safe_int(row.get("rank_order"), 0),
                            row.get("lifecycle_stage"),
                            row.get("defense_line_type"),
                            _safe_float(row.get("delta_1"), 0.0) if row.get("delta_1") is not None else None,
                            _safe_float(row.get("delta_2"), 0.0) if row.get("delta_2") is not None else None,
                            json.dumps(row.get("trajectory") or [], ensure_ascii=False),
                            row.get("action_hint"),
                            1 if row.get("selection_veto") else 0,
                            now_text,
                            now_text,
                        ),
                    )
                conn.commit()
                return len(rows)
            finally:
                conn.close()

    def backfill_sector_heat_daily_from_history(self) -> dict[str, Any]:
        reports_df = self.sector_strategy_db.get_analysis_reports(limit=1000000)
        if hasattr(reports_df, "to_dict"):
            reports = reports_df.to_dict(orient="records")
        else:
            reports = list(reports_df or [])
        ordered_reports = sorted(
            [report for report in reports if isinstance(report, dict) and int(report.get("id") or 0)],
            key=lambda report: (
                str(report.get("analysis_date") or report.get("created_at") or ""),
                int(report.get("id") or 0),
            ),
        )

        processed_reports = 0
        saved_rows = 0
        board_dates: set[str] = set()
        for report_row in ordered_reports:
            report_id = int(report_row.get("id") or 0)
            if not report_id:
                continue
            report = self.sector_strategy_db.get_analysis_report(report_id)
            if not isinstance(report, dict):
                continue
            lifecycle_snapshot = report.get("lifecycle_items") if isinstance(report.get("lifecycle_items"), list) else []
            saved_count = self._save_sector_heat_daily_snapshot(
                run_id=f"history-backfill-{report_id}",
                sector_report_id=report_id,
                report=report,
                lifecycle_snapshot=lifecycle_snapshot,
            )
            processed_reports += 1
            saved_rows += int(saved_count or 0)
            board_date = self._resolve_board_date(report)
            if board_date:
                board_dates.add(board_date)

        return {
            "processed_reports": processed_reports,
            "saved_rows": saved_rows,
            "board_dates": len(board_dates),
        }

    def list_watch_pool(self, active_only: bool = True) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            sql = """
                SELECT *
                FROM smart_selection_watch_pool
            """
            params: list[Any] = []
            if active_only:
                sql += " WHERE active = 1"
            sql += " ORDER BY datetime(last_seen_at) DESC, datetime(updated_at) DESC, id DESC"
            cursor.execute(sql, tuple(params))
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            items = []
            for row in cursor.fetchall():
                item = dict(zip(columns, row))
                item["active"] = bool(item.get("active", 0))
                item["trajectory"] = _safe_json_loads(item.pop("trajectory_json", None), [])
                items.append(item)
            return items
        finally:
            conn.close()

    @staticmethod
    def _wencai_result_to_dataframe(result: Any):
        try:
            import pandas as pd

            if isinstance(result, pd.DataFrame):
                return result
            if isinstance(result, dict):
                table_data = result.get("tableV1")
                if isinstance(table_data, pd.DataFrame):
                    return table_data
                if isinstance(table_data, list):
                    return pd.DataFrame(table_data)
                if "data" in result and isinstance(result.get("data"), list):
                    return pd.DataFrame(result.get("data"))
                return pd.DataFrame([result])
            if isinstance(result, list):
                return pd.DataFrame([item for item in result if isinstance(item, dict)])
        except Exception:
            return None
        return None

    @staticmethod
    def _first_row_value(row: Any, exact_candidates: list[str], fuzzy_keywords: list[str] | None = None) -> Any:
        row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row or {})
        for key in exact_candidates:
            if key in row_dict and row_dict.get(key) not in (None, ""):
                return row_dict.get(key)
        for key, value in row_dict.items():
            key_text = str(key or "")
            if fuzzy_keywords and any(keyword in key_text for keyword in fuzzy_keywords) and value not in (None, ""):
                return value
        return None

    def _build_external_discovery_shortages(
        self,
        *,
        matched_candidates: list[dict[str, Any]],
        observed_startup_candidates: list[dict[str, Any]],
        ranked_action_candidates: list[dict[str, Any]],
        final_selected: list[dict[str, Any]],
    ) -> list[str]:
        shortages: list[str] = []
        if len(matched_candidates) < EXTERNAL_DISCOVERY_MIN_MATCHED_CANDIDATES:
            shortages.append(f"研究池主线匹配不足 {EXTERNAL_DISCOVERY_MIN_MATCHED_CANDIDATES} 只")
        if len(observed_startup_candidates) < EXTERNAL_DISCOVERY_MIN_STARTUP_CANDIDATES:
            shortages.append(f"启动期观察不足 {EXTERNAL_DISCOVERY_MIN_STARTUP_CANDIDATES} 只")
        if len(ranked_action_candidates) < EXTERNAL_DISCOVERY_MIN_EXPLOSIVE_CANDIDATES:
            shortages.append(f"爆发期候选不足 {EXTERNAL_DISCOVERY_MIN_EXPLOSIVE_CANDIDATES} 只")
        if len(final_selected) < EXTERNAL_DISCOVERY_MIN_FINAL_SELECTED:
            shortages.append(f"最终执行不足 {EXTERNAL_DISCOVERY_MIN_FINAL_SELECTED} 只")
        return shortages

    def _query_external_sector_candidates(
        self,
        *,
        sector: str,
        lifecycle_stage: str,
        limit: int,
        warnings: list[str],
    ) -> list[dict[str, Any]]:
        sector_text = _safe_text(sector)
        if not sector_text:
            return []
        if _is_excluded_selection_sector(sector_text):
            warnings.append(f"已忽略研究池外非交易主线概念: {sector_text}")
            return []
        try:
            from pywencai_runtime import setup_pywencai_runtime_env

            setup_pywencai_runtime_env()
            import pywencai
        except Exception as exc:
            warnings.append(f"研究池外发现不可用: {exc}")
            return []

        query = (
            f"{sector_text} 板块 龙头股 今日涨幅 成交额 换手率 量比 主力净流入 总市值 "
            "非ST 非科创板 股票"
        )
        try:
            raw_result = pywencai.get(query=query, loop=True)
        except Exception as exc:
            warnings.append(f"{sector_text} 研究池外发现失败: {exc}")
            return []

        df = self._wencai_result_to_dataframe(raw_result)
        if df is None or getattr(df, "empty", True):
            return []

        code_col = find_matching_column(df, ("股票代码", "证券代码", "代码", "股票代码链接"))
        name_col = find_matching_column(df, ("股票简称", "证券简称", "名称", "股票名称"))
        if not code_col:
            return []

        discovered: list[dict[str, Any]] = []
        for _, row in df.head(max(limit * 3, limit)).iterrows():
            symbol = normalize_stock_code(row.get(code_col))
            if not research_hub_service._is_a_share_symbol(symbol):
                continue
            name = _safe_text(row.get(name_col)) if name_col else symbol
            if "ST" in name.upper():
                continue

            change_pct = parse_numeric_value(
                self._first_row_value(row, ["涨跌幅", "涨跌幅:前复权", "今日涨跌幅"], ["涨跌幅", "涨幅"])
            )
            amount = parse_numeric_value(
                self._first_row_value(row, ["成交额", "成交额(元)", "成交额(万)", "成交额(亿)"], ["成交额"])
            )
            turnover = parse_numeric_value(self._first_row_value(row, ["换手率", "换手率(%)"], ["换手"]))
            volume_ratio = parse_numeric_value(self._first_row_value(row, ["量比"], ["量比"]))
            main_net = parse_numeric_value(self._first_row_value(row, ["主力净流入", "主力净流入-净额"], ["主力净流入"]))
            market_cap = parse_numeric_value(self._first_row_value(row, ["总市值", "总市值(元)", "总市值(亿)"], ["总市值", "市值"]))
            rank_value = parse_numeric_value(self._first_row_value(row, ["排名", "个股热度排名"], ["排名"]))

            amount_yi = _safe_float(amount, 0.0)
            if amount_yi > 1000000:
                amount_yi = amount_yi / 100000000
            elif amount_yi > 10000:
                amount_yi = amount_yi / 10000
            market_cap_yi = _safe_float(market_cap, 0.0)
            if market_cap_yi > 1000000:
                market_cap_yi = market_cap_yi / 100000000

            leader_score = 0.0
            leader_score += min(max(_safe_float(change_pct, 0.0), -5.0), 12.0) * 2.2
            leader_score += min(amount_yi, 120.0) * 0.25
            leader_score += min(max(_safe_float(turnover, 0.0), 0.0), 25.0) * 1.0
            leader_score += min(max(_safe_float(volume_ratio, 0.0), 0.0), 5.0) * 4.0
            if main_net is not None and _safe_float(main_net, 0.0) > 0:
                leader_score += 8.0
            if rank_value:
                leader_score += max(0.0, 12.0 - min(rank_value, 12.0))
            leader_score = round(_clamp_score(45.0 + leader_score * 0.45), 2)

            lifecycle_label = "爆发期" if lifecycle_stage == self.sector_strategy_db.LIFECYCLE_STAGE_EXPLOSIVE else "启动期"
            discovered.append(
                {
                    "symbol": symbol,
                    "name": name or symbol,
                    "primary_sector": sector_text,
                    "canonical_sector": sector_text,
                    "matched_sectors": [{"sector": sector_text, "canonical_sector": sector_text, "match_score": 1.0}],
                    "match_score": 1.0,
                    "score": leader_score,
                    "leader_score": leader_score,
                    "heat_score": None,
                    "lifecycle_stage": lifecycle_stage,
                    "source_type": "external_wencai",
                    "external_discovery": True,
                    "external_discovery_query": query,
                    "change_pct": round(_safe_float(change_pct, 0.0), 2) if change_pct is not None else None,
                    "amount_yi": round(amount_yi, 2) if amount_yi else None,
                    "turnover_rate": round(_safe_float(turnover, 0.0), 2) if turnover is not None else None,
                    "volume_ratio": round(_safe_float(volume_ratio, 0.0), 2) if volume_ratio is not None else None,
                    "main_net": main_net,
                    "market_cap": round(market_cap_yi, 2) if market_cap_yi else None,
                    "reason": (
                        f"研究池外发现：{sector_text} {lifecycle_label}补池候选；"
                        f"涨跌幅 {round(_safe_float(change_pct, 0.0), 2) if change_pct is not None else '-'}%；"
                        f"成交额 {round(amount_yi, 2) if amount_yi else '-'} 亿；"
                        f"换手 {round(_safe_float(turnover, 0.0), 2) if turnover is not None else '-'}%"
                    ),
                }
            )
            if len(discovered) >= limit:
                break
        return discovered

    def _discover_external_candidates(
        self,
        *,
        selection_sectors: list[dict[str, Any]],
        existing_symbols: set[str],
        shortages: list[str],
        warnings: list[str],
    ) -> list[dict[str, Any]]:
        if not shortages:
            return []

        discovered_by_symbol: dict[str, dict[str, Any]] = {}
        eligible_sectors = [
            item
            for item in selection_sectors
            if str(item.get("lifecycle_stage") or "") in {
                self.sector_strategy_db.LIFECYCLE_STAGE_STARTUP,
                self.sector_strategy_db.LIFECYCLE_STAGE_EXPLOSIVE,
            }
            and not bool(item.get("selection_veto"))
        ]
        for sector_item in eligible_sectors[:3]:
            sector_name = _safe_text(sector_item.get("canonical_sector") or sector_item.get("sector") or sector_item.get("lifecycle_sector"))
            lifecycle_stage = _safe_text(sector_item.get("lifecycle_stage"))
            for item in self._query_external_sector_candidates(
                sector=sector_name,
                lifecycle_stage=lifecycle_stage,
                limit=EXTERNAL_DISCOVERY_LIMIT_PER_SECTOR,
                warnings=warnings,
            ):
                symbol = _safe_text(item.get("symbol"))
                if not symbol or symbol in existing_symbols:
                    continue
                lifecycle_label = "爆发期" if lifecycle_stage == self.sector_strategy_db.LIFECYCLE_STAGE_EXPLOSIVE else "启动期"
                item["external_discovery_reason"] = "；".join(shortages[:3])
                item["external_discovery"] = True
                item["reason"] = f"{item.get('reason') or ''} | 未在研究池中，仅作为{lifecycle_label}补池线索".strip(" |")
                previous = discovered_by_symbol.get(symbol)
                if previous is None or _safe_float(item.get("score"), 0.0) > _safe_float(previous.get("score"), 0.0):
                    discovered_by_symbol[symbol] = item

        discovered = sorted(
            discovered_by_symbol.values(),
            key=lambda item: (
                1 if item.get("lifecycle_stage") == self.sector_strategy_db.LIFECYCLE_STAGE_EXPLOSIVE else 0,
                _safe_float(item.get("score"), 0.0),
                _safe_float(item.get("amount_yi"), 0.0),
            ),
            reverse=True,
        )[:EXTERNAL_DISCOVERY_LIMIT]
        if discovered:
            warnings.append(f"研究池匹配不足，已补充 {len(discovered)} 只研究池外候选，仅作补池线索")
        return discovered

    def _run_pipeline(
        self,
        run_id: str,
        *,
        trigger_source: str = "manual",
        lightweight_model: Optional[str] = None,
        reasoning_model: Optional[str] = None,
    ) -> dict[str, Any]:
        warnings: list[str] = []
        normalized_trigger_source = _normalize_trigger_source(trigger_source)
        strict_execution_mode = normalized_trigger_source in STRICT_EXECUTION_TRIGGER_SOURCES
        run_session = self._resolve_run_session(normalized_trigger_source)
        if normalized_trigger_source == "manual":
            warnings.append(f"手动触发时段：{run_session.get('label')}，{run_session.get('recommendation')}")

        def report_progress(current: int, message: str) -> None:
            self._update_run(run_id, current=int(current), total=100, message=message)

        report_progress(5, "检查智策板块报告...")
        sector_info = research_hub_service.ensure_recent_sector_strategy_report(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        warnings.extend([str(item).strip() for item in sector_info.get("warnings") or [] if str(item).strip()])
        report = sector_info.get("report") or {}
        market_context = research_hub_service._build_selection_market_context(report)
        report_progress(12, "同步新闻情绪数据...")
        news_sync_result = self._sync_news_flow_context(warnings)
        news_flow_context = self._load_news_flow_context(warnings)
        news_flow_context_summary = self._summarize_news_flow_context(news_flow_context)
        news_flow_context_summary["sync_success"] = bool(isinstance(news_sync_result, dict) and news_sync_result.get("success"))
        news_flow_context_summary["sync_snapshot_id"] = (
            news_sync_result.get("snapshot_id") if isinstance(news_sync_result, dict) else None
        )
        news_flow_context_summary["sync_duration"] = (
            news_sync_result.get("duration") if isinstance(news_sync_result, dict) else None
        )
        if not news_flow_context_summary.get("available"):
            warnings.append("新闻情绪上下文缺失，尾盘确认按中性降级")
        elif not news_flow_context_summary.get("fresh"):
            warnings.append("新闻情绪上下文超过 12 小时，尾盘确认按过期降级")
        report_id = int(sector_info.get("report_id") or 0)
        lifecycle_snapshot = self.sector_strategy_db.get_lifecycle_items_for_analysis(report_id) if report_id else []
        saved_sector_heat_count = self._save_sector_heat_daily_snapshot(
            run_id=run_id,
            sector_report_id=report_id,
            report=report if isinstance(report, dict) else {},
            lifecycle_snapshot=lifecycle_snapshot,
        )
        if not lifecycle_snapshot:
            warnings.append("最新智策报告缺少生命周期数据，智能选股降级为空结果")
            lifecycle_summary = self.sector_strategy_db.build_lifecycle_summary([])
            return {
                "sector_strategy_report_id": report_id,
                "sector_strategy_reused": bool(sector_info.get("reused")),
                "run_session": run_session,
                "news_flow_context": news_flow_context_summary,
                "saved_sector_heat_count": saved_sector_heat_count,
                "lifecycle_summary": lifecycle_summary,
                "matched_candidates": [],
                "observed_startup_candidates": [],
                "observe_candidates": [],
                "external_discovery_candidates": [],
                "observed_decay_candidates": [],
                "ranked_action_candidates": [],
                "final_selected": [],
                "excluded_by_lifecycle_veto": [],
                "excluded_by_risk_veto": [],
                "match_diagnostics": {},
                "warnings": warnings,
            }

        report_progress(20, "提取主线热点并映射生命周期...")
        extracted_sectors = research_hub_service._extract_selection_sectors(
            report,
            warnings,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        selection_sectors, lifecycle_by_name = self._build_selection_sector_snapshot(
            extracted_sectors=extracted_sectors,
            lifecycle_snapshot=lifecycle_snapshot,
            warnings=warnings,
        )
        if not selection_sectors:
            lifecycle_summary = self.sector_strategy_db.build_lifecycle_summary(lifecycle_snapshot)
            return {
                "sector_strategy_report_id": report_id,
                "sector_strategy_reused": bool(sector_info.get("reused")),
                "market_context": market_context,
                "run_session": run_session,
                "news_flow_context": news_flow_context_summary,
                "extracted_sectors": extracted_sectors,
                "selection_sectors": [],
                "saved_sector_heat_count": saved_sector_heat_count,
                "lifecycle_summary": lifecycle_summary,
                "matched_candidates": [],
                "observed_startup_candidates": [],
                "observe_candidates": [],
                "external_discovery_candidates": [],
                "observed_decay_candidates": [],
                "ranked_action_candidates": [],
                "final_selected": [],
                "excluded_by_lifecycle_veto": [],
                "excluded_by_risk_veto": [],
                "match_diagnostics": {},
                "warnings": list(dict.fromkeys(warnings)),
            }

        research_assets = [
            asset
            for asset in asset_repository.list_assets(status=STATUS_RESEARCH, include_deleted=False)
            if research_hub_service._is_a_share_symbol(asset.get("symbol"))
        ]

        startup_by_sector: dict[str, list[dict[str, Any]]] = {}
        decay_candidates: list[dict[str, Any]] = []
        explosive_candidates: list[dict[str, Any]] = []
        matched_candidates: list[dict[str, Any]] = []
        vetoed_candidates: list[dict[str, Any]] = []
        max_workers = self.get_scheduler_config().get("max_workers", DEFAULT_MAX_WORKERS)
        unmatched_by_sector = 0

        report_progress(45, "计算候选个股信号...")
        warnings_lock = threading.Lock()

        def process_asset(asset: dict[str, Any]) -> Optional[tuple[str, dict[str, Any], str]]:
            local_warnings: list[str] = []
            context = research_hub_service._collect_asset_match_context(asset, local_warnings)
            context["market_context"] = market_context
            candidate = research_hub_service._score_selection_candidate(asset, context, selection_sectors)
            if local_warnings:
                with warnings_lock:
                    warnings.extend(local_warnings)
            if not candidate:
                return None
            requested_sector = candidate.get("canonical_sector") or candidate.get("primary_sector")
            matched_key, lifecycle_item = self._find_lifecycle_match(requested_sector, lifecycle_by_name)
            if not lifecycle_item:
                return ("unmatched", {"symbol": asset.get("symbol"), "requested_sector": requested_sector}, "other")
            metrics = self._build_candidate_metrics(candidate, lifecycle_item, news_flow_context)
            result_item = self._format_result_item(candidate, lifecycle_item, metrics)
            result_item["primary_sector"] = lifecycle_item.get("sector_name") or result_item.get("primary_sector")
            primary_sector = research_hub_service._normalize_sector_text(result_item.get("primary_sector"))

            if metrics["selection_veto"]:
                result_item["reason"] = f"{result_item['reason']} | 生命周期衰退，一票否决"
                return ("veto", result_item, primary_sector or "other")

            if metrics["lifecycle_stage"] == self.sector_strategy_db.LIFECYCLE_STAGE_STARTUP:
                return ("startup", result_item, primary_sector or "other")

            if metrics["lifecycle_stage"] == self.sector_strategy_db.LIFECYCLE_STAGE_DECAY:
                if metrics["selection_veto"]:
                    result_item["reason"] = f"{result_item['reason']} | 生命周期衰退，一票否决"
                    return ("veto", result_item, primary_sector or "other")
                result_item["reason"] = f"{result_item['reason']} | 生命周期衰退，保留观察不进入执行名单"
                return ("decay", result_item, primary_sector or "other")

            if metrics["lifecycle_stage"] != self.sector_strategy_db.LIFECYCLE_STAGE_EXPLOSIVE:
                return None

            return ("explosive", result_item, primary_sector or "other")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, int(max_workers or DEFAULT_MAX_WORKERS))) as executor:
            for outcome in executor.map(process_asset, research_assets):
                if not outcome:
                    continue
                bucket, result_item, primary_sector = outcome
                if bucket == "unmatched":
                    unmatched_by_sector += 1
                elif bucket == "veto":
                    vetoed_candidates.append(result_item)
                elif bucket == "startup":
                    matched_candidates.append(result_item)
                    startup_by_sector.setdefault(primary_sector, []).append(result_item)
                elif bucket == "decay":
                    matched_candidates.append(result_item)
                    decay_candidates.append(result_item)
                elif bucket == "explosive":
                    matched_candidates.append(result_item)
                    explosive_candidates.append(result_item)

        observed_startup_candidates: list[dict[str, Any]] = []
        for sector_name, items in startup_by_sector.items():
            ranked = sorted(
                items,
                key=lambda item: (
                    _safe_float(item.get("score"), 0.0),
                    _safe_float(item.get("market_cap"), 0.0),
                ),
                reverse=True,
            )[:SECTOR_WATCH_LIMIT]
            observed_startup_candidates.extend(ranked)
        observed_startup_candidates.sort(key=lambda item: (_safe_float(item.get("score")), _safe_float(item.get("market_cap"))), reverse=True)
        observed_decay_candidates = sorted(
            decay_candidates,
            key=lambda item: (
                _safe_float(item.get("score"), 0.0),
                _safe_float(item.get("market_cap"), 0.0),
            ),
            reverse=True,
        )
        matched_candidates.sort(
            key=lambda item: (
                _safe_float(item.get("match_score"), 0.0),
                _safe_float(item.get("score"), 0.0),
                _safe_float(item.get("heat_score"), 0.0),
            ),
            reverse=True,
        )

        report_progress(72, "筛选尾盘执行候选...")
        ranked_action_candidates = sorted(explosive_candidates, key=lambda item: _safe_float(item.get("score")), reverse=True)

        final_selected: list[dict[str, Any]] = []
        observe_candidates: list[dict[str, Any]] = list(observed_startup_candidates)
        risk_vetoed_candidates: list[dict[str, Any]] = []
        execution_gated_candidates: list[dict[str, Any]] = []
        sector_counts: dict[str, int] = {}
        report_progress(70, "检查龙虎榜数据...")
        longhubang_sync_result = self._sync_longhubang_data(warnings)
        longhubang_map = research_hub_service._group_recent_longhubang_by_symbol(days=3, warnings=warnings)
        filtered_by_session_time = 0
        filtered_by_tail = 0
        filtered_by_freshness = 0
        filtered_by_distribution = 0
        pre_risk_candidates: list[dict[str, Any]] = []

        def build_execution_gate_item(item: dict[str, Any], gate_reason: str, gate_type: str) -> dict[str, Any]:
            gated_item = dict(item)
            gated_item["execution_gate_reason"] = gate_reason
            gated_item["execution_gate_type"] = gate_type
            gated_item["reason"] = f"{item.get('reason') or ''} | {gate_reason}".strip(" |")
            return gated_item

        def append_observe_candidate(item: dict[str, Any], observe_reason: str, observe_type: str) -> None:
            observe_item = dict(item)
            observe_item["observe_reason"] = observe_reason
            observe_item["observe_type"] = observe_type
            observe_item["reason"] = f"{item.get('reason') or ''} | {observe_reason}".strip(" |")
            existing_symbols = {str(candidate.get("symbol") or "") for candidate in observe_candidates}
            if str(observe_item.get("symbol") or "") not in existing_symbols:
                observe_candidates.append(observe_item)

        for item in ranked_action_candidates:
            gate_result = self._evaluate_execution_gate(
                item,
                run_session=run_session,
                strict_execution_mode=strict_execution_mode,
            )
            item["execution_ready"] = bool(gate_result["decision"] == "execute")
            item["execution_mode"] = str(run_session.get("mode") or ("strict" if strict_execution_mode else "manual_explore"))
            item["run_session"] = run_session
            item["execution_gate_result"] = gate_result
            item["adjusted_gate_score"] = gate_result["adjusted_gate_score"]
            item["gate_soft_penalty"] = gate_result["soft_penalty"]
            item["execution_gate_policy"] = {
                "threshold": gate_result["threshold"],
                "observe_threshold": gate_result["observe_threshold"],
                "min_component_floor": gate_result["min_component_floor"],
                "distribution_risk_max": gate_result["distribution_risk_max"],
                "distribution_risk_hard_max": gate_result["distribution_risk_hard_max"],
                "sector_limit": gate_result["sector_limit"],
            }
            item["gate_score"] = gate_result["adjusted_gate_score"]
            item["base_gate_score"] = gate_result["base_gate_score"]
            if gate_result["soft_notes"]:
                item["execution_gate_notes"] = gate_result["soft_notes"]
                note_text = "；".join(str(note.get("reason") or "") for note in gate_result["soft_notes"][:3] if note.get("reason"))
                if note_text:
                    item["reason"] = f"{item.get('reason') or ''} | 门槛提示: {note_text}".strip(" |")

            hard_types = {block.get("type") for block in gate_result["hard_blocks"]}
            if "session_time" in hard_types:
                filtered_by_session_time += 1
            if "tail_session" in hard_types or "post_close_snapshot" in hard_types:
                filtered_by_tail += 1
            if "realtime_freshness" in hard_types or "minute_quality" in hard_types:
                filtered_by_freshness += 1
            if "distribution_risk" in hard_types:
                filtered_by_distribution += 1

            if gate_result["decision"] != "execute":
                gate_reason = gate_result["primary_reason"] or "执行门槛不足，降级观察"
                gate_type = gate_result["primary_type"] or "gate_score"
                gated_item = build_execution_gate_item(item, gate_reason, gate_type)
                execution_gated_candidates.append(gated_item)
                append_observe_candidate(gated_item, gate_reason, gate_type)
                continue

            if bool(run_session.get("post_close_review")):
                item["execution_ready"] = False
                item["reason"] = f"{item.get('reason') or ''} | 盘后复盘候选，不能当日执行，次日开盘前需重新确认".strip(" |")
            pre_risk_candidates.append(item)

        risk_results: dict[str, dict[str, Any]] = {}

        def evaluate_item_risk(item: dict[str, Any]) -> tuple[str, dict[str, Any], list[str]]:
            symbol = str(item.get("symbol") or "")
            local_warnings: list[str] = []
            client = research_hub_service.LLMClient(
                lightweight_model=lightweight_model,
                reasoning_model=reasoning_model,
            )
            risk_result = research_hub_service._evaluate_risk_for_symbol(
                symbol,
                str(item.get("name") or item.get("symbol") or ""),
                longhubang_map,
                local_warnings,
                risk_client=client,
            )
            return symbol, risk_result, local_warnings

        if pre_risk_candidates:
            risk_worker_count = min(
                max(1, int(max_workers or DEFAULT_MAX_WORKERS)),
                len(pre_risk_candidates),
                RISK_REVIEW_PARALLELISM_CAP,
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=risk_worker_count) as executor:
                future_map = {
                    executor.submit(evaluate_item_risk, item): str(item.get("symbol") or "")
                    for item in pre_risk_candidates
                }
                for future in concurrent.futures.as_completed(future_map):
                    symbol = future_map[future]
                    try:
                        resolved_symbol, risk_result, local_warnings = future.result()
                    except Exception as exc:
                        warnings.append(f"{symbol} 个股风控异常，已降级放行: {exc}")
                        risk_results[symbol] = {
                            "vetoed": False,
                            "risk_notes": [f"风控降级: {exc}"],
                            "risk_level": "medium",
                        }
                        continue
                    if local_warnings:
                        warnings.extend(local_warnings)
                    risk_results[resolved_symbol] = risk_result if isinstance(risk_result, dict) else {}

        for item in pre_risk_candidates:
            if len(final_selected) >= FINAL_SELECTION_LIMIT:
                break
            risk_result = risk_results.get(str(item.get("symbol") or ""), {})
            risk_notes = [str(note).strip() for note in risk_result.get("risk_notes") or [] if str(note).strip()]
            if risk_notes:
                item["risk_notes"] = risk_notes
                item["risk_level"] = str(risk_result.get("risk_level") or "")
                item["reason"] = f"{item.get('reason') or ''} | 风控提示: {'；'.join(risk_notes[:2])}".strip(" |")
            if risk_result.get("vetoed"):
                item["selection_veto"] = True
                item["reason"] = f"{item.get('reason') or ''} | 个股风控否决".strip(" |")
                risk_vetoed_candidates.append(item)
                continue
            bucket = research_hub_service._normalize_sector_text(item.get("primary_sector")) or "other"
            sector_limit = max(1, _safe_int((item.get("execution_gate_policy") or {}).get("sector_limit"), 2))
            if sector_counts.get(bucket, 0) >= sector_limit:
                continue
            sector_counts[bucket] = sector_counts.get(bucket, 0) + 1
            final_selected.append(item)

        if filtered_by_session_time:
            warnings.append(f"{filtered_by_session_time} 只爆发期候选因手动触发时段为{run_session.get('label')}降级为观察名单")
        if filtered_by_tail:
            warnings.append(f"{filtered_by_tail} 只爆发期候选因未到尾盘时段降级为观察名单")
        if filtered_by_freshness:
            warnings.append(f"{filtered_by_freshness} 只爆发期候选因分时新鲜度不足降级为观察名单")
        if filtered_by_distribution:
            warnings.append(f"{filtered_by_distribution} 只爆发期候选因派发风险过高降级为观察名单")
        if not final_selected and observe_candidates:
            warnings.append("已有主线匹配标的，但当前仅达到观察级，未进入执行级名单")

        all_existing_symbols = {
            _safe_text(asset.get("symbol"))
            for asset in asset_repository.list_assets(include_deleted=False)
            if _safe_text(asset.get("symbol"))
        }
        discovery_shortages = self._build_external_discovery_shortages(
            matched_candidates=matched_candidates,
            observed_startup_candidates=observed_startup_candidates,
            ranked_action_candidates=ranked_action_candidates,
            final_selected=final_selected,
        )
        external_discovery_candidates = self._discover_external_candidates(
            selection_sectors=selection_sectors,
            existing_symbols=all_existing_symbols,
            shortages=discovery_shortages,
            warnings=warnings,
        )

        match_diagnostics = {
            "research_asset_count": len(research_assets),
            "selection_sector_count": len(selection_sectors),
            "matched_candidate_count": len(matched_candidates),
            "startup_observe_count": len(observed_startup_candidates),
            "observe_candidate_count": len(observe_candidates),
            "external_discovery_count": len(external_discovery_candidates),
            "ranked_action_count": len(ranked_action_candidates),
            "final_selected_count": len(final_selected),
            "lifecycle_veto_count": len(vetoed_candidates),
            "risk_veto_count": len(risk_vetoed_candidates),
            "execution_gated_count": len(execution_gated_candidates),
            "session_time_gated_count": filtered_by_session_time,
            "distribution_gated_count": filtered_by_distribution,
            "unmatched_after_sector_alignment": unmatched_by_sector,
        }

        observe_candidates.sort(
            key=lambda item: (
                _safe_float(item.get("match_score"), 0.0),
                _safe_float(item.get("gate_score"), 0.0),
                _safe_float(item.get("score"), 0.0),
            ),
            reverse=True,
        )
        self._replace_run_items(run_id, "observed_startup_candidates", observed_startup_candidates)
        self._replace_run_items(run_id, "matched_candidates", matched_candidates)
        self._replace_run_items(run_id, "observe_candidates", observe_candidates)
        self._replace_run_items(run_id, "external_discovery_candidates", external_discovery_candidates)
        self._replace_run_items(run_id, "observed_decay_candidates", observed_decay_candidates)
        self._replace_run_items(run_id, "ranked_action_candidates", ranked_action_candidates)
        self._replace_run_items(run_id, "excluded_by_execution_gate", execution_gated_candidates)
        self._replace_run_items(run_id, "final_selected", final_selected)
        self._replace_run_items(run_id, "excluded_by_lifecycle_veto", vetoed_candidates)
        self._replace_run_items(run_id, "excluded_by_risk_veto", risk_vetoed_candidates)
        self._upsert_watch_pool(
            run_id,
            [item for item in observed_startup_candidates if _safe_float(item.get("match_score"), 0.0) >= 0.8],
        )

        lifecycle_summary = self.sector_strategy_db.build_lifecycle_summary(lifecycle_snapshot)
        report_progress(100, "智能选股完成")
        return {
            "sector_strategy_report_id": report_id,
            "sector_strategy_reused": bool(sector_info.get("reused")),
            "run_session": run_session,
            "news_flow_context": news_flow_context_summary,
            "market_context": market_context,
            "extracted_sectors": extracted_sectors,
            "selection_sectors": selection_sectors,
            "saved_sector_heat_count": saved_sector_heat_count,
            "longhubang_sync": longhubang_sync_result,
            "lifecycle_summary": lifecycle_summary,
            "matched_candidates": matched_candidates,
            "observed_startup_candidates": observed_startup_candidates,
            "observe_candidates": observe_candidates,
            "external_discovery_candidates": external_discovery_candidates,
            "observed_decay_candidates": observed_decay_candidates,
            "ranked_action_candidates": ranked_action_candidates,
            "excluded_by_execution_gate": execution_gated_candidates,
            "final_selected": final_selected,
            "excluded_by_lifecycle_veto": vetoed_candidates,
            "excluded_by_risk_veto": risk_vetoed_candidates,
            "match_diagnostics": match_diagnostics,
            "trigger_source": normalized_trigger_source,
            "strict_execution_mode": strict_execution_mode,
            "warnings": list(dict.fromkeys(warnings)),
            "watch_pool_size": len(self.list_watch_pool(active_only=True)),
        }

    def _execute_run(
        self,
        run_id: str,
        trigger_source: str,
        lightweight_model: Optional[str],
        reasoning_model: Optional[str],
    ) -> None:
        if not self._execution_lock.acquire(blocking=False):
            self._update_run(run_id, status="failed", error="已有智能选股任务正在执行", finished_at=_now_text(), message="已有任务正在执行")
            return
        try:
            self._update_run(run_id, status="running", started_at=_now_text(), message="智能选股任务开始执行", current=0, total=100)
            result = self._run_pipeline(
                run_id,
                trigger_source=trigger_source,
                lightweight_model=lightweight_model,
                reasoning_model=reasoning_model,
            )
            self._update_run(
                run_id,
                status="success",
                finished_at=_now_text(),
                message="智能选股完成",
                current=100,
                total=100,
                sector_report_id=result.get("sector_strategy_report_id"),
                sector_report_reused=1 if result.get("sector_strategy_reused") else 0,
                result_summary_json={
                    "lifecycle_summary": result.get("lifecycle_summary") or {},
                    "watch_pool_size": result.get("watch_pool_size", 0),
                    "saved_sector_heat_count": result.get("saved_sector_heat_count", 0),
                    "match_diagnostics": result.get("match_diagnostics") or {},
                    "run_session": result.get("run_session") or {},
                    "news_flow_context": result.get("news_flow_context") or {},
                    "longhubang_sync": result.get("longhubang_sync") or {},
                },
                warnings_json=result.get("warnings") or [],
            )
        except Exception as exc:
            self._update_run(
                run_id,
                status="failed",
                error=str(exc),
                finished_at=_now_text(),
                message="智能选股执行失败",
                warnings_json=[],
            )
        finally:
            self._execution_lock.release()
            self._threads.pop(run_id, None)
            if trigger_source == "scheduled":
                smart_selection_scheduler.last_run_time = _now_text()

    def import_run_selection(self, *, run_id: str, symbols: list[str], replace_existing_focus: bool = True) -> dict[str, Any]:
        run = self.get_run(run_id)
        if not run:
            raise ValueError("未找到智能选股运行记录")
        final_selected = run.get("result", {}).get("final_selected") or []
        final_lookup = {
            str(item.get("symbol") or "").strip(): item
            for item in final_selected
            if isinstance(item, dict) and str(item.get("symbol") or "").strip()
        }
        if not final_lookup:
            raise ValueError("当前运行结果没有可导入的最终入选股票")
        target_symbols = [symbol for symbol in dict.fromkeys([str(item).strip().upper() for item in (symbols or []) if str(item).strip()]) if symbol in final_lookup]
        if not target_symbols:
            target_symbols = list(final_lookup.keys())
        if not target_symbols:
            raise ValueError("没有可导入的股票")

        demoted_symbols: list[str] = []
        imported_symbols: list[str] = []
        if replace_existing_focus:
            focus_assets = asset_repository.list_assets(status=STATUS_FOCUS, include_deleted=False)
            for asset in focus_assets:
                asset_repository.transition_asset_status(
                    int(asset["id"]),
                    STATUS_RESEARCH,
                    note="智能选股导入覆盖，回退到研究池",
                    pool_reason="智能选股导入覆盖，回退到研究池",
                    pool_reason_source="smart_selection_import",
                )
                asset_repository.update_asset(int(asset["id"]), manual_pin=False)
                demoted_symbols.append(str(asset.get("symbol") or ""))

        for symbol in target_symbols:
            item = final_lookup[symbol]
            asset_id = asset_repository.promote_to_watchlist(
                symbol=symbol,
                name=item.get("name") or symbol,
                note=item.get("reason") or "智能选股导入关注备选",
                origin_analysis_id=None,
                monitor_enabled=True,
            )
            asset_repository.update_asset(
                asset_id,
                manual_pin=False,
                pool_reason=item.get("reason") or "智能选股导入关注备选",
                pool_reason_source="smart_selection",
                last_funnel_score=item.get("score"),
                last_funnel_snapshot_json={
                    "run_id": run_id,
                    "primary_sector": item.get("primary_sector"),
                    "lifecycle_stage": item.get("lifecycle_stage"),
                    "defense_line_type": item.get("defense_line_type"),
                    "trajectory": item.get("trajectory") or [],
                    "delta_1": item.get("delta_1"),
                    "delta_2": item.get("delta_2"),
                },
            )
            imported_symbols.append(symbol)

        return {
            "run_id": run_id,
            "imported_symbols": imported_symbols,
            "demoted_symbols": demoted_symbols,
            "imported_count": len(imported_symbols),
        }

    def get_overview(self) -> dict[str, Any]:
        latest_run = self.get_latest_run()
        return {
            "latest_run": latest_run,
            "watch_pool_count": len(self.list_watch_pool(active_only=True)),
            "lifecycle": self.sector_strategy_db.get_latest_lifecycle_snapshot(),
            "daily_heat_panel": self.sector_strategy_db.get_daily_heat_panel(limit=20),
            "scheduler": smart_selection_scheduler.get_status(),
        }


class SmartSelectionScheduler:
    def __init__(self, service: SmartSelectionService) -> None:
        self.service = service
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.enabled = False
        self.schedule_time = DEFAULT_SCHEDULE_TIME
        self.last_run_time: Optional[str] = None

    def apply_runtime_config(self, config: dict[str, Any]) -> None:
        self.enabled = bool(config.get("enabled"))
        self.schedule_time = str(config.get("schedule_time") or DEFAULT_SCHEDULE_TIME).strip() or DEFAULT_SCHEDULE_TIME
        if self.enabled:
            self.start(self.schedule_time)
        else:
            self.stop()

    def start(self, schedule_time: Optional[str] = None) -> bool:
        schedule_time = str(schedule_time or self.schedule_time or DEFAULT_SCHEDULE_TIME).strip() or DEFAULT_SCHEDULE_TIME
        self.enabled = True
        self.schedule_time = schedule_time
        jobs_to_remove = [job for job in schedule.jobs if "smart_selection_scheduler" in job.tags]
        for job in jobs_to_remove:
            schedule.cancel_job(job)
        job = schedule.every().day.at(schedule_time).do(self._run_scheduled_safe)
        job.tag("smart_selection_scheduler")
        if self.running:
            return True
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True, name="smart-selection-scheduler")
        self.thread.start()
        return True

    def stop(self) -> bool:
        self.enabled = False
        if not self.running:
            jobs_to_remove = [job for job in schedule.jobs if "smart_selection_scheduler" in job.tags]
            for job in jobs_to_remove:
                schedule.cancel_job(job)
            return True
        self.running = False
        jobs_to_remove = [job for job in schedule.jobs if "smart_selection_scheduler" in job.tags]
        for job in jobs_to_remove:
            schedule.cancel_job(job)
        return True

    def _loop(self) -> None:
        while self.running:
            try:
                schedule.run_pending()
            except Exception:
                pass
            threading.Event().wait(30)

    def _run_scheduled_safe(self):
        self.service.submit_run(trigger_source="scheduled")

    def manual_run(self) -> str:
        return self.service.submit_run(trigger_source="manual")

    def get_status(self) -> dict[str, Any]:
        next_run_time = None
        jobs = schedule.get_jobs("smart_selection_scheduler")
        if jobs:
            next_run_time = jobs[0].next_run.strftime("%Y-%m-%d %H:%M:%S") if jobs[0].next_run else None
        config = self.service.get_scheduler_config()
        return {
            "running": self.running,
            "enabled": self.enabled,
            "schedule_time": self.schedule_time,
            "max_workers": max(1, _safe_int(config.get("max_workers"), DEFAULT_MAX_WORKERS)),
            "next_run_time": next_run_time,
            "last_run_time": self.last_run_time,
        }


smart_selection_service = SmartSelectionService()
smart_selection_scheduler = SmartSelectionScheduler(smart_selection_service)
