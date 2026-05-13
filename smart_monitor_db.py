import json
import logging
import os
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from analysis_repository import AnalysisRepository
from asset_repository import STATUS_PORTFOLIO, STATUS_WATCHLIST, AssetRepository
from asset_service import AssetService, asset_service
from investment_db_utils import (
    DEFAULT_ACCOUNT_NAME,
    connect_sqlite,
    get_metadata,
    normalize_account_name,
    resolve_investment_db_path,
    run_with_monitoring_write_lock,
    set_metadata,
)
from monitoring_repository import MonitoringRepository
from portfolio_db import PortfolioDB
from time_utils import local_now_str


class SmartMonitorDB:
    """Repository-backed smart monitor facade."""

    ACCOUNT_NORMALIZATION_KEY = "smart_monitor_history_account_normalization_v1"
    NOTIFICATION_CLEANUP_MIGRATION_KEY = "smart_monitor_notification_cleanup_v1"
    TASK_ENABLE_SYNC_MIGRATION_KEY = "smart_monitor_task_enable_sync_v1"
    VALID_NOTIFICATION_STATUSES = {"pending", "sent", "failed"}

    def __init__(self, db_file: str = "smart_monitor.db"):
        self.seed_db_file = db_file
        self.db_file = resolve_investment_db_path(db_file)
        self.legacy_db_file = db_file if os.path.abspath(db_file) != os.path.abspath(self.db_file) else "smart_monitor.db"
        self.logger = logging.getLogger(__name__)
        self.monitoring_repository = MonitoringRepository(self.db_file)
        self.portfolio_db = PortfolioDB(self.db_file)
        self.analysis_repository = AnalysisRepository(self.db_file, legacy_analysis_db_path=self.db_file)
        self.asset_repository = AssetRepository(self.db_file)
        self.asset_service = AssetService(
            asset_store=self.asset_repository,
            analysis_store=self.analysis_repository,
            monitoring_store=self.monitoring_repository,
        )
        from investment_lifecycle_service import InvestmentLifecycleService
        from monitor_db import StockMonitorDatabase

        self.lifecycle_service = InvestmentLifecycleService(
            portfolio_store=self.portfolio_db,
            realtime_monitor_store=StockMonitorDatabase(self.db_file),
            analysis_store=self.analysis_repository,
            monitoring_store=self.monitoring_repository,
            asset_service=self.asset_service,
        )
        self._init_database()
        self.monitoring_repository.migrate_legacy_smart_db(self.legacy_db_file)
        self._migrate_legacy_history_db(self.legacy_db_file)
        self._normalize_account_names()
        self._repair_ai_decision_history()
        self._cleanup_notification_history()
        self._reconcile_task_enable_projection()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_file)

    def _init_database(self):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                account_name TEXT,
                asset_id INTEGER,
                portfolio_stock_id INTEGER,
                origin_analysis_id INTEGER,
                decision_time TEXT NOT NULL,
                trading_session TEXT,
                action TEXT NOT NULL,
                action_detail TEXT,
                action_ratio_pct REAL,
                trade_intent TEXT,
                current_position_pct REAL,
                target_position_pct REAL,
                position_delta_pct REAL,
                confidence INTEGER,
                reasoning TEXT,
                position_size_pct REAL,
                stop_loss_pct REAL,
                take_profit_pct REAL,
                risk_level TEXT,
                key_price_levels TEXT,
                monitor_levels TEXT,
                decision_context TEXT,
                market_data TEXT,
                account_info TEXT,
                execution_mode TEXT DEFAULT 'manual_only',
                action_status TEXT DEFAULT 'suggested',
                executed INTEGER DEFAULT 0,
                execution_result TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                notify_type TEXT NOT NULL,
                notify_target TEXT,
                subject TEXT,
                content TEXT,
                status TEXT DEFAULT 'pending',
                error_msg TEXT,
                sent_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_decision_evaluations (
                decision_id INTEGER PRIMARY KEY,
                stock_code TEXT,
                account_name TEXT,
                action TEXT,
                decision_time TEXT,
                decision_quality_score REAL,
                quality_flags_json TEXT,
                veto_reason TEXT,
                baseline_relation TEXT,
                data_freshness_state TEXT,
                feedback_status TEXT DEFAULT '',
                feedback_note TEXT,
                actual_action_id INTEGER,
                sample_count INTEGER DEFAULT 0,
                latest_sample_id INTEGER,
                max_upside_pct REAL,
                max_downside_pct REAL,
                take_profit_hit INTEGER DEFAULT 0,
                stop_loss_hit INTEGER DEFAULT 0,
                outcome_label TEXT,
                evaluation_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._ensure_column(cursor, "ai_decisions", "account_name", "TEXT")
        self._ensure_column(cursor, "ai_decisions", "portfolio_stock_id", "INTEGER")
        self._ensure_column(cursor, "ai_decisions", "origin_analysis_id", "INTEGER")
        self._ensure_column(cursor, "ai_decisions", "asset_id", "INTEGER")
        self._ensure_column(cursor, "ai_decisions", "execution_mode", "TEXT DEFAULT 'manual_only'")
        self._ensure_column(cursor, "ai_decisions", "action_status", "TEXT DEFAULT 'suggested'")
        self._ensure_column(cursor, "ai_decisions", "monitor_levels", "TEXT")
        self._ensure_column(cursor, "ai_decisions", "decision_context", "TEXT")
        self._ensure_column(cursor, "ai_decisions", "action_detail", "TEXT")
        self._ensure_column(cursor, "ai_decisions", "action_ratio_pct", "REAL")
        self._ensure_column(cursor, "ai_decisions", "trade_intent", "TEXT")
        self._ensure_column(cursor, "ai_decisions", "current_position_pct", "REAL")
        self._ensure_column(cursor, "ai_decisions", "target_position_pct", "REAL")
        self._ensure_column(cursor, "ai_decisions", "position_delta_pct", "REAL")
        self._ensure_column(cursor, "monitor_decision_evaluations", "decision_quality_score", "REAL")
        self._ensure_column(cursor, "monitor_decision_evaluations", "quality_flags_json", "TEXT")
        self._ensure_column(cursor, "monitor_decision_evaluations", "veto_reason", "TEXT")
        self._ensure_column(cursor, "monitor_decision_evaluations", "baseline_relation", "TEXT")
        self._ensure_column(cursor, "monitor_decision_evaluations", "data_freshness_state", "TEXT")
        self._ensure_column(cursor, "monitor_decision_evaluations", "feedback_status", "TEXT DEFAULT ''")
        self._ensure_column(cursor, "monitor_decision_evaluations", "feedback_note", "TEXT")
        self._ensure_column(cursor, "monitor_decision_evaluations", "actual_action_id", "INTEGER")
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monitor_decision_evaluations_stock_time
            ON monitor_decision_evaluations(stock_code, datetime(decision_time) DESC, decision_id DESC)
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _ensure_column(cursor, table: str, column: str, definition: str) -> None:
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @staticmethod
    def _normalize_account_name_value(
        account_name: Optional[object],
        *,
        keep_none: bool = False,
    ) -> Optional[str]:
        return normalize_account_name(account_name, keep_none=keep_none)

    @staticmethod
    def _serialize_json_field(value, default):
        if value in (None, ""):
            return json.dumps(default, ensure_ascii=False)
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False)

    @staticmethod
    def _safe_json_loads(value: Any, default: Any = None) -> Any:
        if value in (None, ""):
            return default
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return default

    @staticmethod
    def _extract_first_number(value, allow_zero: bool = False) -> Optional[float]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            numeric = float(value)
            return numeric if allow_zero or numeric != 0 else None
        text = str(value)
        digits: List[str] = []
        current: List[str] = []
        for char in text:
            if char.isdigit() or char in {".", "-"}:
                current.append(char)
                continue
            if current:
                digits.append("".join(current))
                current = []
        if current:
            digits.append("".join(current))
        for candidate in digits:
            try:
                numeric = float(candidate)
            except ValueError:
                continue
            if allow_zero or numeric != 0:
                return numeric
        return None

    @staticmethod
    def _format_entry_range_text(entry_min: Optional[float], entry_max: Optional[float]) -> str:
        if entry_min is None and entry_max is None:
            return "N/A"
        if entry_min is None:
            return f"{float(entry_max):.3f}"
        if entry_max is None:
            return f"{float(entry_min):.3f}"
        return f"{float(entry_min):.3f}-{float(entry_max):.3f}"

    @staticmethod
    def _map_action_to_rating(action: object, fallback: str = "持有") -> str:
        normalized = str(action or "").strip().upper()
        if normalized == "BUY":
            return "买入"
        if normalized == "SELL":
            return "卖出"
        return fallback or "持有"

    @classmethod
    def _build_unified_decision_view(cls, decision: Dict) -> Dict[str, object]:
        monitor_levels = decision.get("monitor_levels") if isinstance(decision.get("monitor_levels"), dict) else {}
        entry_min = cls._extract_first_number(monitor_levels.get("entry_min"), allow_zero=True)
        entry_max = cls._extract_first_number(monitor_levels.get("entry_max"), allow_zero=True)
        take_profit = cls._extract_first_number(monitor_levels.get("take_profit"), allow_zero=True)
        stop_loss = cls._extract_first_number(monitor_levels.get("stop_loss"), allow_zero=True)
        rating = str(decision.get("rating") or "").strip() or cls._map_action_to_rating(
            decision.get("action"),
            fallback="持有",
        )
        confidence_level = decision.get("confidence_level")
        if confidence_level in (None, ""):
            confidence_level = decision.get("confidence")
        advice = str(
            decision.get("advice")
            or decision.get("reasoning")
            or ""
        ).strip()
        return {
            "rating": rating,
            "confidence_level": confidence_level,
            "entry_range": decision.get("entry_range") or cls._format_entry_range_text(entry_min, entry_max),
            "take_profit": decision.get("take_profit") if decision.get("take_profit") not in (None, "") else take_profit,
            "stop_loss": decision.get("stop_loss") if decision.get("stop_loss") not in (None, "") else stop_loss,
            "advice": advice,
        }

    @staticmethod
    def _extract_decision_context_snapshot(payload: Dict) -> Dict:
        explicit_context = payload.get("decision_context")
        merged_context: Dict[str, object] = dict(explicit_context) if isinstance(explicit_context, dict) and explicit_context else {}

        market_data = payload.get("market_data")
        if not isinstance(market_data, dict):
            return merged_context
        intraday_context = market_data.get("intraday_context")
        if not isinstance(intraday_context, dict) or not intraday_context:
            return merged_context

        snapshot = {
            "swing_execution_mode": payload.get("swing_execution_mode"),
            "intraday_bias": intraday_context.get("intraday_bias"),
            "intraday_bias_text": intraday_context.get("intraday_bias_text"),
            "intraday_signal_labels": intraday_context.get("intraday_signal_labels") if isinstance(intraday_context.get("intraday_signal_labels"), list) else [],
            "intraday_observations": intraday_context.get("intraday_observations") if isinstance(intraday_context.get("intraday_observations"), list) else [],
            "price_position_pct": intraday_context.get("price_position_pct"),
            "last_5m_change_pct": intraday_context.get("last_5m_change_pct"),
            "last_15m_change_pct": intraday_context.get("last_15m_change_pct"),
            "last_30m_change_pct": intraday_context.get("last_30m_change_pct"),
            "volume_acceleration_ratio": intraday_context.get("volume_acceleration_ratio"),
            "intraday_vwap": intraday_context.get("intraday_vwap"),
            "latest_trade_time": intraday_context.get("latest_trade_time"),
        }
        for key, value in snapshot.items():
            if key in merged_context:
                continue
            if value in (None, [], {}, ""):
                continue
            merged_context[key] = value
        return merged_context

    @staticmethod
    def _query_asset_binding_by_id(cursor: sqlite3.Cursor, asset_id: int) -> Optional[Dict]:
        cursor.execute(
            """
            SELECT id, account_name
            FROM assets
            WHERE id = ? AND deleted_at IS NULL
            LIMIT 1
            """,
            (asset_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "asset_id": int(row["id"]),
            "account_name": row["account_name"] or DEFAULT_ACCOUNT_NAME,
        }

    @staticmethod
    def _query_ai_task_binding(cursor: sqlite3.Cursor, stock_code: str) -> Optional[Dict]:
        cursor.execute(
            """
            SELECT asset_id, account_name
            FROM monitoring_items
            WHERE monitor_type = 'ai_task'
              AND symbol = ?
              AND asset_id IS NOT NULL
            ORDER BY enabled DESC, managed_by_portfolio DESC, datetime(updated_at) DESC, id DESC
            """,
            (stock_code,),
        )
        rows = cursor.fetchall()
        bindings = {
            (int(row["asset_id"]), row["account_name"] or DEFAULT_ACCOUNT_NAME)
            for row in rows
            if row["asset_id"] is not None
        }
        if len(bindings) != 1:
            return None
        asset_id, account_name = next(iter(bindings))
        return {"asset_id": asset_id, "account_name": account_name}

    @staticmethod
    def _query_unique_asset_binding_by_symbol(cursor: sqlite3.Cursor, stock_code: str) -> Optional[Dict]:
        cursor.execute(
            """
            SELECT id, account_name
            FROM assets
            WHERE symbol = ? AND deleted_at IS NULL
            ORDER BY
                CASE status
                    WHEN 'holding' THEN 1
                    WHEN 'portfolio' THEN 1
                    WHEN 'focus' THEN 2
                    WHEN 'watchlist' THEN 2
                    ELSE 3
                END,
                id DESC
            """,
            (stock_code,),
        )
        rows = cursor.fetchall()
        bindings = {
            (int(row["id"]), row["account_name"] or DEFAULT_ACCOUNT_NAME)
            for row in rows
        }
        if len(bindings) != 1:
            return None
        asset_id, account_name = next(iter(bindings))
        return {"asset_id": asset_id, "account_name": account_name}

    def _resolve_ai_decision_binding(self, cursor: sqlite3.Cursor, decision_data: Dict) -> Optional[Dict]:
        asset_id = decision_data.get("asset_id")
        if asset_id is not None:
            binding = self._query_asset_binding_by_id(cursor, int(asset_id))
            if binding:
                return binding

        portfolio_stock_id = decision_data.get("portfolio_stock_id")
        if portfolio_stock_id is not None:
            binding = self._query_asset_binding_by_id(cursor, int(portfolio_stock_id))
            if binding:
                return binding

        stock_code = str(decision_data.get("stock_code") or "").strip()
        if not stock_code:
            return None

        binding = self._query_ai_task_binding(cursor, stock_code)
        if binding:
            return binding

        return self._query_unique_asset_binding_by_symbol(cursor, stock_code)

    def _prepare_ai_decision_payload(
        self,
        cursor: sqlite3.Cursor,
        decision_data: Dict,
        *,
        default_account: bool,
        default_action_status: Optional[str] = None,
    ) -> Dict:
        payload = dict(decision_data)
        normalized_account_name = self._normalize_account_name_value(
            payload.get("account_name"),
            keep_none=not default_account,
        )
        if normalized_account_name is not None:
            payload["account_name"] = normalized_account_name
        binding = self._resolve_ai_decision_binding(cursor, payload)
        if binding:
            payload["asset_id"] = payload.get("asset_id") or binding["asset_id"]
            payload["account_name"] = payload.get("account_name") or binding["account_name"]
        if default_account and not payload.get("account_name"):
            payload["account_name"] = DEFAULT_ACCOUNT_NAME
        if isinstance(payload.get("account_info"), dict):
            account_info = dict(payload["account_info"])
            normalized_info_account = self._normalize_account_name_value(
                account_info.get("account_name"),
                keep_none=True,
            )
            if normalized_info_account:
                account_info["account_name"] = normalized_info_account
            elif "account_name" in account_info:
                account_info.pop("account_name", None)
            payload["account_info"] = account_info
        payload["stock_name"] = self._resolve_stock_name_for_write(
            payload.get("stock_code") or "",
            stock_name=payload.get("stock_name"),
            account_name=payload.get("account_name"),
            asset_id=payload.get("asset_id"),
            portfolio_stock_id=payload.get("portfolio_stock_id"),
        )
        payload["decision_context"] = self._extract_decision_context_snapshot(payload)

        action = str(payload.get("action") or "").upper()
        payload["execution_mode"] = payload.get("execution_mode") or "manual_only"
        payload["action_status"] = payload.get("action_status") or default_action_status or (
            "pending" if action in {"BUY", "SELL"} else "suggested"
        )
        return payload

    def _insert_ai_decision(
        self,
        cursor: sqlite3.Cursor,
        decision_data: Dict,
        *,
        default_account: bool,
        default_action_status: Optional[str] = None,
    ) -> int:
        payload = self._prepare_ai_decision_payload(
            cursor,
            decision_data,
            default_account=default_account,
            default_action_status=default_action_status,
        )
        cursor.execute(
            """
            INSERT INTO ai_decisions (
                stock_code, stock_name, account_name, asset_id, portfolio_stock_id, origin_analysis_id,
                decision_time, trading_session, action, action_detail, action_ratio_pct, trade_intent, current_position_pct, target_position_pct, position_delta_pct,
                confidence, reasoning, position_size_pct,
                stop_loss_pct, take_profit_pct, risk_level, key_price_levels, monitor_levels, decision_context, market_data,
                account_info, execution_mode, action_status, executed, execution_result, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.get("stock_code"),
                payload.get("stock_name"),
                payload.get("account_name"),
                payload.get("asset_id"),
                payload.get("portfolio_stock_id"),
                payload.get("origin_analysis_id"),
                payload.get("decision_time", local_now_str()),
                payload.get("trading_session"),
                payload.get("action"),
                payload.get("action_detail"),
                payload.get("action_ratio_pct"),
                payload.get("trade_intent"),
                payload.get("current_position_pct"),
                payload.get("target_position_pct"),
                payload.get("position_delta_pct"),
                payload.get("confidence"),
                payload.get("reasoning"),
                payload.get("position_size_pct"),
                payload.get("stop_loss_pct"),
                payload.get("take_profit_pct"),
                payload.get("risk_level"),
                self._serialize_json_field(payload.get("key_price_levels"), {}),
                self._serialize_json_field(payload.get("monitor_levels"), {}),
                self._serialize_json_field(payload.get("decision_context"), {}),
                self._serialize_json_field(payload.get("market_data"), {}),
                self._serialize_json_field(payload.get("account_info"), {}),
                payload.get("execution_mode", "manual_only"),
                payload.get("action_status", "suggested"),
                int(payload.get("executed", 0) or 0),
                payload.get("execution_result"),
                payload.get("created_at") or local_now_str(),
            ),
        )
        decision_id = int(cursor.lastrowid)
        self._upsert_decision_evaluation_from_payload(cursor, decision_id, payload)
        return decision_id

    def _upsert_decision_evaluation_from_payload(
        self,
        cursor: sqlite3.Cursor,
        decision_id: int,
        payload: Dict[str, Any],
    ) -> None:
        decision_context = payload.get("decision_context") if isinstance(payload.get("decision_context"), dict) else {}
        audit = decision_context.get("decision_audit") if isinstance(decision_context.get("decision_audit"), dict) else {}
        score = decision_context.get("decision_quality_score", audit.get("decision_quality_score"))
        quality_flags = decision_context.get("quality_flags") or audit.get("quality_flags") or []
        veto_reason = decision_context.get("veto_reason") or audit.get("veto_reason") or ""
        baseline_relation = decision_context.get("baseline_relation") or audit.get("baseline_relation") or ""
        data_freshness_state = decision_context.get("data_freshness_state") or audit.get("data_freshness_state") or ""
        now_text = local_now_str()
        cursor.execute(
            """
            INSERT INTO monitor_decision_evaluations (
                decision_id, stock_code, account_name, action, decision_time,
                decision_quality_score, quality_flags_json, veto_reason, baseline_relation,
                data_freshness_state, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(decision_id) DO UPDATE SET
                stock_code = excluded.stock_code,
                account_name = excluded.account_name,
                action = excluded.action,
                decision_time = excluded.decision_time,
                decision_quality_score = excluded.decision_quality_score,
                quality_flags_json = excluded.quality_flags_json,
                veto_reason = excluded.veto_reason,
                baseline_relation = excluded.baseline_relation,
                data_freshness_state = excluded.data_freshness_state,
                updated_at = excluded.updated_at
            """,
            (
                decision_id,
                payload.get("stock_code"),
                payload.get("account_name"),
                payload.get("action"),
                payload.get("decision_time", now_text),
                score,
                json.dumps(quality_flags if isinstance(quality_flags, list) else [], ensure_ascii=False),
                veto_reason,
                baseline_relation,
                data_freshness_state,
                now_text,
                now_text,
            ),
        )

    def _update_decision_evaluation_outcome(
        self,
        cursor: sqlite3.Cursor,
        decision_id: int,
        outcome: Dict[str, Any],
    ) -> None:
        if not isinstance(outcome, dict):
            return
        cursor.execute(
            """
            UPDATE monitor_decision_evaluations
            SET sample_count = ?,
                latest_sample_id = ?,
                max_upside_pct = ?,
                max_downside_pct = ?,
                take_profit_hit = ?,
                stop_loss_hit = ?,
                outcome_label = ?,
                evaluation_json = ?,
                updated_at = ?
            WHERE decision_id = ?
            """,
            (
                int(outcome.get("sample_count") or 0),
                outcome.get("latest_sample_id"),
                outcome.get("max_upside_pct"),
                outcome.get("max_downside_pct"),
                1 if outcome.get("take_profit_hit") else 0,
                1 if outcome.get("stop_loss_hit") else 0,
                outcome.get("outcome_label"),
                json.dumps(outcome, ensure_ascii=False),
                local_now_str(),
                decision_id,
            ),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO monitor_decision_evaluations (
                    decision_id, sample_count, latest_sample_id, max_upside_pct, max_downside_pct,
                    take_profit_hit, stop_loss_hit, outcome_label, evaluation_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    int(outcome.get("sample_count") or 0),
                    outcome.get("latest_sample_id"),
                    outcome.get("max_upside_pct"),
                    outcome.get("max_downside_pct"),
                    1 if outcome.get("take_profit_hit") else 0,
                    1 if outcome.get("stop_loss_hit") else 0,
                    outcome.get("outcome_label"),
                    json.dumps(outcome, ensure_ascii=False),
                    local_now_str(),
                    local_now_str(),
                ),
            )

    def _update_ai_decision_fields(self, cursor: sqlite3.Cursor, decision_id: int, updates: Dict[str, object]) -> bool:
        if not updates:
            return False
        fields = [f"{key} = ?" for key in updates]
        values = list(updates.values())
        values.append(decision_id)
        cursor.execute(
            f"UPDATE ai_decisions SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        return cursor.rowcount > 0

    @classmethod
    def _decision_market_price(cls, decision: Dict[str, Any]) -> Optional[float]:
        market_data = cls._safe_json_loads(decision.get("market_data"), {})
        if not isinstance(market_data, dict):
            market_data = decision.get("market_data") if isinstance(decision.get("market_data"), dict) else {}
        value = market_data.get("current_price")
        try:
            numeric = float(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None
        return numeric if numeric and numeric > 0 else None

    @classmethod
    def _decision_monitor_levels(cls, decision: Dict[str, Any]) -> Dict[str, Any]:
        levels = cls._safe_json_loads(decision.get("monitor_levels"), {})
        return levels if isinstance(levels, dict) else {}

    @staticmethod
    def _parse_decision_datetime(value: object) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _round_pct(value: float) -> float:
        return round(float(value), 2)

    @classmethod
    def _classify_outcome_label(
        cls,
        *,
        action: str,
        max_upside_pct: float,
        max_drawdown_pct: float,
        latest_return_pct: float,
        take_profit_hit: bool,
        stop_loss_hit: bool,
    ) -> str:
        normalized_action = str(action or "").upper()
        if normalized_action == "BUY":
            if stop_loss_hit or max_drawdown_pct <= -3.0:
                return "risk_realized"
            if take_profit_hit or max_upside_pct >= 3.0 or latest_return_pct >= 1.0:
                return "favorable_follow_through"
            return "neutral"
        if normalized_action == "SELL":
            if max_drawdown_pct <= -3.0:
                return "risk_avoided"
            if max_upside_pct >= 5.0:
                return "early_exit_risk"
            return "neutral"
        if max_upside_pct >= 5.0 and not stop_loss_hit:
            return "missed_upside"
        if max_drawdown_pct <= -5.0:
            return "avoided_drawdown"
        return "neutral"

    @classmethod
    def _same_decision_binding(cls, left: Dict[str, Any], right: Dict[str, Any]) -> bool:
        if str(left.get("stock_code") or "").strip().upper() != str(right.get("stock_code") or "").strip().upper():
            return False
        left_account = normalize_account_name(left.get("account_name")) or DEFAULT_ACCOUNT_NAME
        right_account = normalize_account_name(right.get("account_name")) or DEFAULT_ACCOUNT_NAME
        if left_account != right_account:
            return False
        for key in ("asset_id", "portfolio_stock_id"):
            left_id = left.get(key)
            right_id = right.get(key)
            if left_id is not None and right_id is not None and int(left_id) != int(right_id):
                return False
        return True

    @classmethod
    def _build_outcome_snapshot(
        cls,
        decision: Dict[str, Any],
        future_decisions: List[Dict[str, Any]],
        *,
        horizon_days: int,
    ) -> Optional[Dict[str, Any]]:
        decision_price = cls._decision_market_price(decision)
        if decision_price is None:
            return None

        samples: List[Dict[str, Any]] = []
        for future in future_decisions:
            price = cls._decision_market_price(future)
            if price is None:
                continue
            samples.append({
                "id": int(future["id"]),
                "decision_time": future.get("decision_time"),
                "price": price,
            })
        if not samples:
            return None

        prices = [float(sample["price"]) for sample in samples]
        latest_sample = samples[-1]
        max_price = max(prices)
        min_price = min(prices)
        latest_price = float(latest_sample["price"])
        max_upside_pct = (max_price - decision_price) / decision_price * 100
        max_drawdown_pct = (min_price - decision_price) / decision_price * 100
        latest_return_pct = (latest_price - decision_price) / decision_price * 100

        monitor_levels = cls._decision_monitor_levels(decision)
        take_profit = cls._extract_first_number(monitor_levels.get("take_profit"), allow_zero=True)
        stop_loss = cls._extract_first_number(monitor_levels.get("stop_loss"), allow_zero=True)
        take_profit_hit = bool(take_profit is not None and max_price >= take_profit)
        stop_loss_hit = bool(stop_loss is not None and min_price <= stop_loss)
        action = str(decision.get("action") or "").upper()
        outcome_label = cls._classify_outcome_label(
            action=action,
            max_upside_pct=max_upside_pct,
            max_drawdown_pct=max_drawdown_pct,
            latest_return_pct=latest_return_pct,
            take_profit_hit=take_profit_hit,
            stop_loss_hit=stop_loss_hit,
        )

        return {
            "version": "intraday_outcome_v1",
            "decision_id": int(decision["id"]),
            "evaluated_at": local_now_str(),
            "horizon_days": int(horizon_days),
            "sample_count": len(samples),
            "latest_sample_id": int(latest_sample["id"]),
            "latest_sample_time": latest_sample.get("decision_time"),
            "decision_price": round(decision_price, 4),
            "latest_price": round(latest_price, 4),
            "max_forward_price": round(max_price, 4),
            "min_forward_price": round(min_price, 4),
            "max_upside_pct": cls._round_pct(max_upside_pct),
            "max_drawdown_pct": cls._round_pct(max_drawdown_pct),
            "latest_return_pct": cls._round_pct(latest_return_pct),
            "take_profit_hit": take_profit_hit,
            "stop_loss_hit": stop_loss_hit,
            "outcome_label": outcome_label,
            "action": action,
        }

    def _dedupe_ai_decisions(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute("SELECT * FROM ai_decisions ORDER BY id ASC")
        rows = [dict(row) for row in cursor.fetchall()]
        key_fields = [
            "stock_code",
            "stock_name",
            "account_name",
            "asset_id",
            "portfolio_stock_id",
            "origin_analysis_id",
            "decision_time",
            "trading_session",
            "action",
            "action_detail",
            "action_ratio_pct",
            "trade_intent",
            "current_position_pct",
            "target_position_pct",
            "position_delta_pct",
            "confidence",
            "reasoning",
            "position_size_pct",
            "stop_loss_pct",
            "take_profit_pct",
            "risk_level",
            "key_price_levels",
            "monitor_levels",
            "decision_context",
            "market_data",
            "account_info",
            "execution_mode",
            "executed",
            "execution_result",
            "created_at",
        ]
        seen = {}
        duplicate_ids = []
        for row in rows:
            key = tuple(row.get(field) for field in key_fields)
            if key in seen:
                duplicate_ids.append(int(row["id"]))
            else:
                seen[key] = int(row["id"])
        if not duplicate_ids:
            return 0
        placeholders = ", ".join("?" for _ in duplicate_ids)
        cursor.execute(f"DELETE FROM ai_decisions WHERE id IN ({placeholders})", tuple(duplicate_ids))
        return len(duplicate_ids)

    def _repair_ai_decision_history(self) -> Dict[str, int]:
        conn = self._connect()
        cursor = conn.cursor()
        repaired_accounts = 0
        repaired_assets = 0
        removed_duplicates = 0
        try:
            cursor.execute("SELECT * FROM ai_decisions ORDER BY id ASC")
            decisions = [dict(row) for row in cursor.fetchall()]
            for decision in decisions:
                prepared = self._prepare_ai_decision_payload(
                    cursor,
                    decision,
                    default_account=False,
                    default_action_status="suggested",
                )
                updates: Dict[str, object] = {}
                if not decision.get("account_name") and prepared.get("account_name"):
                    updates["account_name"] = prepared["account_name"]
                if decision.get("asset_id") is None and prepared.get("asset_id") is not None:
                    updates["asset_id"] = prepared["asset_id"]
                existing_decision_context = decision.get("decision_context")
                prepared_decision_context = self._serialize_json_field(prepared.get("decision_context"), {})
                if (existing_decision_context in (None, "", "{}")) and prepared_decision_context not in (None, "", "{}"):
                    updates["decision_context"] = prepared_decision_context
                if updates and self._update_ai_decision_fields(cursor, int(decision["id"]), updates):
                    repaired_accounts += 1 if "account_name" in updates else 0
                    repaired_assets += 1 if "asset_id" in updates else 0

            removed_duplicates = self._dedupe_ai_decisions(cursor)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        if repaired_accounts or repaired_assets or removed_duplicates:
            self.logger.info(
                "AI决策历史已修复: account_name=%s asset_id=%s duplicates=%s",
                repaired_accounts,
                repaired_assets,
                removed_duplicates,
            )
        return {
            "account_name": repaired_accounts,
            "asset_id": repaired_assets,
            "duplicates": removed_duplicates,
        }

    def _cleanup_notification_history(self) -> Dict[str, int]:
        conn = self._connect()
        cleanup_key = self.NOTIFICATION_CLEANUP_MIGRATION_KEY
        if get_metadata(conn, cleanup_key):
            conn.close()
            return {"invalid": 0, "duplicates": 0, "status": 0}

        cursor = conn.cursor()
        removed_invalid = 0
        removed_duplicates = 0
        repaired_status = 0
        seen = set()
        duplicate_ids: List[int] = []
        invalid_ids: List[int] = []
        status_updates: List[tuple[str, int]] = []
        try:
            cursor.execute("SELECT * FROM notifications ORDER BY datetime(created_at) DESC, id DESC")
            for row in cursor.fetchall():
                notification = dict(row)
                notification_id = int(notification["id"])
                notify_type = str(notification.get("notify_type") or "").strip().lower()
                subject = str(notification.get("subject") or "").strip()
                content = str(notification.get("content") or "").strip()
                status = str(notification.get("status") or "").strip().lower() or "pending"
                if not notify_type or not (subject or content):
                    invalid_ids.append(notification_id)
                    continue
                normalized_status = status if status in self.VALID_NOTIFICATION_STATUSES else "pending"
                if normalized_status != status:
                    status_updates.append((normalized_status, notification_id))
                dedupe_key = (
                    str(notification.get("stock_code") or "").strip().upper(),
                    notify_type,
                    str(notification.get("notify_target") or "").strip(),
                    subject,
                    content,
                    normalized_status,
                    str(notification.get("error_msg") or "").strip(),
                    str(notification.get("sent_at") or "").strip(),
                    str(notification.get("created_at") or "").strip(),
                )
                if dedupe_key in seen:
                    duplicate_ids.append(notification_id)
                    continue
                seen.add(dedupe_key)

            if invalid_ids:
                placeholders = ", ".join("?" for _ in invalid_ids)
                cursor.execute(f"DELETE FROM notifications WHERE id IN ({placeholders})", tuple(invalid_ids))
                removed_invalid = int(cursor.rowcount or 0)
            if duplicate_ids:
                placeholders = ", ".join("?" for _ in duplicate_ids)
                cursor.execute(f"DELETE FROM notifications WHERE id IN ({placeholders})", tuple(duplicate_ids))
                removed_duplicates = int(cursor.rowcount or 0)
            for normalized_status, notification_id in status_updates:
                cursor.execute(
                    "UPDATE notifications SET status = ? WHERE id = ?",
                    (normalized_status, notification_id),
                )
                repaired_status += 1 if cursor.rowcount > 0 else 0

            set_metadata(
                conn,
                cleanup_key,
                json.dumps(
                    {
                        "invalid": removed_invalid,
                        "duplicates": removed_duplicates,
                        "status": repaired_status,
                        "updated_at": local_now_str(),
                    },
                    ensure_ascii=False,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {
            "invalid": removed_invalid,
            "duplicates": removed_duplicates,
            "status": repaired_status,
        }

    def _reconcile_task_enable_projection(self) -> Dict[str, int]:
        conn = self._connect()
        cleanup_key = self.TASK_ENABLE_SYNC_MIGRATION_KEY
        if get_metadata(conn, cleanup_key):
            conn.close()
            return {"assets": 0, "alerts": 0}
        conn.close()

        synchronized_assets = 0
        synchronized_alerts = 0
        processed_asset_ids = set()
        for item in self.monitoring_repository.list_items(monitor_type="ai_task", enabled_only=False):
            asset_id = item.get("asset_id")
            if asset_id is None:
                continue
            asset_id = int(asset_id)
            if asset_id in processed_asset_ids:
                continue
            processed_asset_ids.add(asset_id)

            asset = self.asset_repository.get_asset(asset_id)
            if not asset:
                continue

            target_enabled = bool(item.get("enabled", True))
            if bool(asset.get("monitor_enabled", True)) != target_enabled:
                self.asset_repository.update_asset(asset_id, monitor_enabled=target_enabled)
                synchronized_assets += 1

            sync_result = self.asset_service.sync_managed_monitors(asset_id)
            synchronized_alerts += int(sync_result.get("price_alerts_upserted", 0))

        conn = self._connect()
        try:
            set_metadata(
                conn,
                cleanup_key,
                json.dumps(
                    {
                        "assets": synchronized_assets,
                        "alerts": synchronized_alerts,
                        "updated_at": local_now_str(),
                    },
                    ensure_ascii=False,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        return {"assets": synchronized_assets, "alerts": synchronized_alerts}

    def _migrate_legacy_history_db(self, legacy_db_path: str) -> int:
        if not legacy_db_path or not os.path.exists(legacy_db_path):
            return 0
        if os.path.abspath(legacy_db_path) == os.path.abspath(self.db_file):
            return 0

        conn = self._connect()
        key = f"migrated_smart_history::{os.path.abspath(legacy_db_path)}"
        if get_metadata(conn, key):
            conn.close()
            return 0
        conn.close()

        legacy_conn = sqlite3.connect(legacy_db_path)
        legacy_conn.row_factory = sqlite3.Row
        legacy_cursor = legacy_conn.cursor()
        migrated = 0

        conn = self._connect()
        cursor = conn.cursor()
        try:
            legacy_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row["name"] for row in legacy_cursor.fetchall()}

            if "ai_decisions" in tables:
                legacy_cursor.execute("SELECT * FROM ai_decisions ORDER BY id ASC")
                for row in legacy_cursor.fetchall():
                    decision = dict(row)
                    self._insert_ai_decision(
                        cursor,
                        decision,
                        default_account=False,
                        default_action_status="suggested",
                    )
                    migrated += 1

            if "notifications" in tables:
                legacy_cursor.execute("SELECT * FROM notifications ORDER BY id ASC")
                for row in legacy_cursor.fetchall():
                    notification = dict(row)
                    cursor.execute(
                        """
                        INSERT INTO notifications (
                            stock_code, notify_type, notify_target, subject,
                            content, status, error_msg, sent_at, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            notification.get("stock_code"),
                            notification.get("notify_type"),
                            notification.get("notify_target"),
                            notification.get("subject"),
                            notification.get("content"),
                            notification.get("status", "pending"),
                            notification.get("error_msg"),
                            notification.get("sent_at"),
                            notification.get("created_at") or local_now_str(),
                        ),
                    )

            set_metadata(cursor.connection, key, str(migrated))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
            legacy_conn.close()
        return migrated

    def _normalize_account_names(self) -> int:
        conn = self._connect()
        try:
            if get_metadata(conn, self.ACCOUNT_NORMALIZATION_KEY):
                return 0
            cursor = conn.cursor()
            cursor.execute("SELECT id, account_name, account_info FROM ai_decisions ORDER BY id ASC")
            updated = 0
            for row in cursor.fetchall():
                normalized_account_name = self._normalize_account_name_value(row["account_name"])
                current_account_name = str(row["account_name"] or "").strip()
                account_info = self._serialize_json_field(row["account_info"], {})
                parsed_account_info = json.loads(account_info) if account_info else {}
                if not isinstance(parsed_account_info, dict):
                    parsed_account_info = {}
                normalized_info_account = self._normalize_account_name_value(
                    parsed_account_info.get("account_name"),
                    keep_none=True,
                )
                if normalized_info_account:
                    parsed_account_info["account_name"] = normalized_info_account
                elif "account_name" in parsed_account_info:
                    parsed_account_info.pop("account_name", None)
                if (
                    normalized_account_name == current_account_name
                    and json.dumps(parsed_account_info, ensure_ascii=False) == account_info
                ):
                    continue
                cursor.execute(
                    """
                    UPDATE ai_decisions
                    SET account_name = ?, account_info = ?
                    WHERE id = ?
                    """,
                    (
                        normalized_account_name,
                        json.dumps(parsed_account_info, ensure_ascii=False),
                        int(row["id"]),
                    ),
                )
                updated += 1 if cursor.rowcount > 0 else 0
            set_metadata(conn, self.ACCOUNT_NORMALIZATION_KEY, str(updated))
            conn.commit()
            return updated
        finally:
            conn.close()

    @staticmethod
    def _seconds_to_interval_minutes(check_interval: Optional[int]) -> int:
        seconds = int(check_interval or 300)
        return max(1, (seconds + 59) // 60)

    def _default_check_interval_seconds(self) -> int:
        raw = self.monitoring_repository.get_metadata("smart_monitor_intraday_decision_interval_minutes")
        try:
            if raw is not None:
                return max(10, min(120, int(raw))) * 60
        except (TypeError, ValueError):
            pass
        return 3600

    def _task_config_from_data(self, task_data: Dict) -> Dict:
        account_name = self._normalize_account_name_value(task_data.get("account_name")) or DEFAULT_ACCOUNT_NAME
        account_risk = self.monitoring_repository.resolve_account_risk_profile(account_name, task_data)
        strategy_context = task_data.get("strategy_context")
        return {
            "task_name": task_data.get("task_name"),
            "position_size_pct": account_risk["position_size_pct"],
            "total_position_pct": account_risk["total_position_pct"],
            "stop_loss_pct": account_risk["stop_loss_pct"],
            "take_profit_pct": account_risk["take_profit_pct"],
            "notify_email": task_data.get("notify_email"),
            "notify_webhook": task_data.get("notify_webhook"),
            "position_date": task_data.get("position_date"),
            "strategy_context": strategy_context if isinstance(strategy_context, dict) else {},
        }

    @staticmethod
    def _normalize_stock_name_text(value: object) -> str:
        return str(value or "").strip()

    @classmethod
    def _is_valid_stock_name(cls, name: object, code: object) -> bool:
        normalized_name = cls._normalize_stock_name_text(name)
        normalized_code = str(code or "").strip().upper()
        if not normalized_name:
            return False
        invalid_names = {
            "",
            "N/A",
            "NA",
            "未知",
            "股票",
            f"股票{normalized_code}",
            f"港股{normalized_code}",
            f"美股{normalized_code}",
        }
        return normalized_name not in invalid_names and normalized_name.upper() != normalized_code

    def _resolve_stock_display_name(
        self,
        stock_code: str,
        *,
        stock_name: Optional[object] = None,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> str:
        normalized_code = str(stock_code or "").strip()
        candidates = [stock_name]

        asset = None
        target_asset_id = asset_id if asset_id is not None else portfolio_stock_id
        if target_asset_id is not None:
            asset = self.asset_repository.get_asset(int(target_asset_id))
        if asset is None and normalized_code:
            asset = self.asset_repository.get_asset_by_symbol(normalized_code, account_name or DEFAULT_ACCOUNT_NAME)
        if asset:
            candidates.extend([asset.get("name"), asset.get("symbol"), asset.get("code")])

        latest_context = self.analysis_repository.get_latest_strategy_context(
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            symbol=normalized_code or None,
            account_name=account_name,
        )
        if isinstance(latest_context, dict):
            candidates.extend([latest_context.get("stock_name"), latest_context.get("symbol")])

        for candidate in candidates:
            if self._is_valid_stock_name(candidate, normalized_code):
                return self._normalize_stock_name_text(candidate)
        return normalized_code

    def _resolve_stock_name_for_write(
        self,
        stock_code: str,
        *,
        stock_name: Optional[object] = None,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> str:
        normalized_code = str(stock_code or "").strip()
        candidates = [stock_name]

        asset = None
        target_asset_id = asset_id if asset_id is not None else portfolio_stock_id
        if target_asset_id is not None:
            asset = self.asset_repository.get_asset(int(target_asset_id))
        if asset is None and normalized_code:
            asset = self.asset_repository.get_asset_by_symbol(normalized_code, account_name or DEFAULT_ACCOUNT_NAME)
        if asset:
            candidates.extend([asset.get("name"), asset.get("symbol"), asset.get("code")])

        latest_context = self.analysis_repository.get_latest_strategy_context(
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            symbol=normalized_code or None,
            account_name=account_name,
        )
        if isinstance(latest_context, dict):
            candidates.extend([latest_context.get("stock_name"), latest_context.get("symbol")])

        for candidate in candidates:
            if self._is_valid_stock_name(candidate, normalized_code):
                return self._normalize_stock_name_text(candidate)

        if normalized_code:
            try:
                from data_source_manager import data_source_manager

                basic_info = data_source_manager.get_stock_basic_info(normalized_code)
                if isinstance(basic_info, dict):
                    candidates.extend(
                        [
                            basic_info.get("name"),
                            basic_info.get("股票名称"),
                            basic_info.get("股票简称"),
                        ]
                    )
            except Exception as exc:
                self.logger.debug("股票名称写入解析失败 (%s): %s", normalized_code, exc)

        for candidate in candidates:
            if self._is_valid_stock_name(candidate, normalized_code):
                return self._normalize_stock_name_text(candidate)
        return normalized_code

    def _item_to_task(self, item: Dict) -> Dict:
        config = item.get("config") or {}
        account_name = self._normalize_account_name_value(item.get("account_name")) or DEFAULT_ACCOUNT_NAME
        account_risk = self.monitoring_repository.resolve_account_risk_profile(account_name, config)
        interval_minutes = int(item.get("interval_minutes") or 1)
        asset = self.asset_repository.get_asset(item.get("asset_id")) if item.get("asset_id") else None
        has_position = bool(asset and asset.get("status") == STATUS_PORTFOLIO and (asset.get("quantity") or 0) > 0)
        monitor_mode = config.get("monitor_mode") or ("exit" if has_position else "entry")
        config_strategy_context = config.get("strategy_context") if isinstance(config.get("strategy_context"), dict) else {}
        latest_strategy_context = self.analysis_repository.get_latest_strategy_context(
            asset_id=item.get("asset_id"),
            portfolio_stock_id=item.get("portfolio_stock_id"),
            symbol=item.get("symbol"),
            account_name=account_name,
        ) or {}
        strategy_context = latest_strategy_context or config_strategy_context or {}
        return {
            "id": item["id"],
            "task_name": config.get("task_name") or f"{item['symbol']} AI监控任务",
            "stock_code": item["symbol"],
            "stock_name": self._resolve_stock_display_name(
                item.get("symbol") or "",
                stock_name=item.get("name"),
                account_name=account_name,
                asset_id=item.get("asset_id"),
                portfolio_stock_id=item.get("portfolio_stock_id"),
            ),
            "enabled": 1 if item.get("enabled", True) else 0,
            "check_interval": interval_minutes * 60,
            "trading_hours_only": 1 if item.get("trading_hours_only", True) else 0,
            "position_size_pct": account_risk["position_size_pct"],
            "total_position_pct": account_risk["total_position_pct"],
            "stop_loss_pct": account_risk["stop_loss_pct"],
            "take_profit_pct": account_risk["take_profit_pct"],
            "notify_email": config.get("notify_email"),
            "notify_webhook": config.get("notify_webhook"),
            "has_position": 1 if has_position else 0,
            "monitor_mode": monitor_mode,
            "strategy_track_label": "防御模式" if monitor_mode == "exit" else "狩猎模式",
            "position_cost": asset.get("cost_price") if asset else 0,
            "position_quantity": asset.get("quantity") if asset else 0,
            "position_date": config.get("position_date"),
            "managed_by_portfolio": 1 if item.get("managed_by_portfolio", False) else 0,
            "account_name": account_name,
            "asset_id": item.get("asset_id"),
            "asset_status": asset.get("status") if asset else None,
            "portfolio_stock_id": item.get("portfolio_stock_id"),
            "origin_analysis_id": strategy_context.get("origin_analysis_id") or item.get("origin_analysis_id"),
            "strategy_context": strategy_context,
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
        }

    def add_monitor_task(self, task_data: Dict) -> int:
        stock_code = task_data.get("stock_code")
        if not stock_code:
            raise ValueError("stock_code 不能为空")
        account_name = self._normalize_account_name_value(task_data.get("account_name")) or DEFAULT_ACCOUNT_NAME
        asset_id = task_data.get("asset_id")
        resolved_stock_name = self._resolve_stock_name_for_write(
            stock_code,
            stock_name=task_data.get("stock_name"),
            account_name=account_name,
            asset_id=asset_id,
        )
        if asset_id is None:
            _, _, asset_id = self.asset_service.promote_to_watchlist(
                symbol=stock_code,
                stock_name=resolved_stock_name,
                account_name=account_name,
                note="",
                origin_analysis_id=task_data.get("origin_analysis_id"),
            )
        return self.monitoring_repository.create_item(
            {
                "symbol": stock_code,
                "name": resolved_stock_name,
                "monitor_type": "ai_task",
                "source": "portfolio" if task_data.get("managed_by_portfolio") else "ai_monitor",
                "enabled": bool(task_data.get("enabled", 1)),
                "interval_minutes": self._seconds_to_interval_minutes(
                    task_data.get("check_interval", self._default_check_interval_seconds())
                ),
                "trading_hours_only": bool(task_data.get("trading_hours_only", 1)),
                "notification_enabled": True,
                "managed_by_portfolio": bool(task_data.get("managed_by_portfolio", 0)),
                "account_name": account_name,
                "asset_id": asset_id,
                "portfolio_stock_id": task_data.get("portfolio_stock_id"),
                "origin_analysis_id": task_data.get("origin_analysis_id"),
                "config": self._task_config_from_data(task_data),
            }
        )

    def get_monitor_tasks(
        self,
        enabled_only: bool = True,
        account_name: Optional[str] = None,
        has_position: Optional[bool] = None,
    ) -> List[Dict]:
        items = self.monitoring_repository.list_items(
            monitor_type="ai_task",
            enabled_only=enabled_only,
            account_name=account_name,
        )
        tasks = [self._item_to_task(item) for item in items]
        if has_position is not None:
            tasks = [t for t in tasks if t["has_position"] == (1 if has_position else 0)]
        return tasks

    def update_monitor_task(self, stock_code: str, task_data: Dict):
        account_name = self._normalize_account_name_value(task_data.get("account_name"), keep_none=True)
        asset_id = task_data.get("asset_id")
        portfolio_stock_id = task_data.get("portfolio_stock_id")
        item = self.monitoring_repository.get_item_by_symbol(
            stock_code,
            monitor_type="ai_task",
            account_name=account_name or DEFAULT_ACCOUNT_NAME,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        if not item:
            item = self.monitoring_repository.get_item_by_symbol(stock_code, monitor_type="ai_task")
        if not item:
            return False

        target_asset_id = asset_id if asset_id is not None else item.get("asset_id")
        updates: Dict[str, object] = {}
        config = dict(item.get("config") or {})
        config_updates = self._task_config_from_data(task_data)

        if "stock_name" in task_data:
            updates["name"] = task_data.get("stock_name")
        if "enabled" in task_data:
            updates["enabled"] = bool(task_data.get("enabled"))
        if "check_interval" in task_data:
            updates["interval_minutes"] = self._seconds_to_interval_minutes(task_data.get("check_interval"))
        if "trading_hours_only" in task_data:
            updates["trading_hours_only"] = bool(task_data.get("trading_hours_only"))
        if "managed_by_portfolio" in task_data:
            managed = bool(task_data.get("managed_by_portfolio"))
            updates["managed_by_portfolio"] = managed
            updates["source"] = "portfolio" if managed else "ai_monitor"
        if "account_name" in task_data:
            updates["account_name"] = self._normalize_account_name_value(task_data.get("account_name")) or DEFAULT_ACCOUNT_NAME
        if "asset_id" in task_data:
            updates["asset_id"] = task_data.get("asset_id")
        if "portfolio_stock_id" in task_data:
            updates["portfolio_stock_id"] = task_data.get("portfolio_stock_id")
        if "origin_analysis_id" in task_data:
            updates["origin_analysis_id"] = task_data.get("origin_analysis_id")

        tracked_keys = {
            "task_name",
            "position_size_pct",
            "total_position_pct",
            "stop_loss_pct",
            "take_profit_pct",
            "notify_email",
            "notify_webhook",
            "position_date",
            "strategy_context",
        }
        if any(key in task_data for key in tracked_keys):
            for key in tracked_keys:
                if key in task_data:
                    config[key] = config_updates[key]
            updates["config"] = config

        if not updates:
            return False

        if "enabled" in task_data and target_asset_id is not None:
            self.asset_repository.update_asset(target_asset_id, monitor_enabled=bool(task_data.get("enabled")))

        changed = self.monitoring_repository.update_item(item["id"], updates)
        if target_asset_id is not None:
            self.asset_service.sync_managed_monitors(int(target_asset_id))
        return changed

    def _get_linked_price_alert_item(self, task_item: Dict) -> Optional[Dict]:
        return self.monitoring_repository.get_item_by_symbol(
            task_item.get("symbol"),
            monitor_type="price_alert",
            managed_only=True if task_item.get("managed_by_portfolio") else None,
            account_name=task_item.get("account_name"),
            asset_id=task_item.get("asset_id"),
            portfolio_stock_id=task_item.get("portfolio_stock_id"),
        )

    def set_monitor_task_enabled(self, task_id: int, enabled: bool) -> bool:
        item = self.monitoring_repository.get_item(task_id)
        if not item or item.get("monitor_type") != "ai_task":
            return False

        target_enabled = bool(enabled)
        linked_alert = self._get_linked_price_alert_item(item)
        asset = self.asset_repository.get_asset(int(item["asset_id"])) if item.get("asset_id") is not None else None
        needs_change = (
            bool(item.get("enabled", True)) != target_enabled
            or linked_alert is None
            or (linked_alert is not None and bool(linked_alert.get("enabled", True)) != target_enabled)
            or (asset is not None and bool(asset.get("monitor_enabled", True)) != target_enabled)
        )
        if not needs_change:
            return False

        if asset is not None:
            self.asset_service.set_monitoring_enabled(int(asset["id"]), target_enabled)
            return True

        self.monitoring_repository.update_item(item["id"], {"enabled": target_enabled})
        if linked_alert is not None:
            self.monitoring_repository.update_item(linked_alert["id"], {"enabled": target_enabled})
        return True

    def set_all_monitor_tasks_enabled(self, enabled: bool) -> int:
        changed_count = 0
        target_enabled = bool(enabled)
        for item in self.monitoring_repository.list_items(monitor_type="ai_task", enabled_only=False):
            if self.set_monitor_task_enabled(int(item["id"]), target_enabled):
                changed_count += 1
        return changed_count

    def get_monitor_task_by_code(
        self,
        stock_code: str,
        managed_only: Optional[bool] = None,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> Optional[Dict]:
        item = self.monitoring_repository.get_item_by_symbol(
            stock_code,
            monitor_type="ai_task",
            managed_only=managed_only,
            account_name=account_name if account_name is not None else None,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        if not item and account_name is None and asset_id is None and portfolio_stock_id is None:
            item = self.monitoring_repository.get_item_by_symbol(
                stock_code,
                monitor_type="ai_task",
                managed_only=managed_only,
                account_name=DEFAULT_ACCOUNT_NAME,
            )
        return self._item_to_task(item) if item else None

    def upsert_monitor_task(self, task_data: Dict) -> int:
        stock_code = task_data.get("stock_code")
        if not stock_code:
            raise ValueError("stock_code 不能为空")
        managed_sync = bool(task_data.get("managed_by_portfolio"))
        account_name = self._normalize_account_name_value(task_data.get("account_name")) or DEFAULT_ACCOUNT_NAME
        asset_id = task_data.get("asset_id")
        target_enabled = bool(task_data.get("enabled", 1))
        resolved_stock_name = self._resolve_stock_name_for_write(
            stock_code,
            stock_name=task_data.get("stock_name"),
            account_name=account_name,
            asset_id=asset_id,
        )
        if asset_id is None:
            _, _, asset_id = self.asset_service.promote_to_watchlist(
                symbol=stock_code,
                stock_name=resolved_stock_name,
                account_name=account_name,
                note="",
                origin_analysis_id=task_data.get("origin_analysis_id"),
                monitor_enabled=target_enabled,
            )
        else:
            asset = self.asset_repository.get_asset(int(asset_id))
            if asset and self._is_valid_stock_name(resolved_stock_name, stock_code) and not self._is_valid_stock_name(asset.get("name"), stock_code):
                self.asset_repository.update_asset(int(asset_id), name=resolved_stock_name)
        if "enabled" in task_data:
            self.asset_repository.update_asset(int(asset_id), monitor_enabled=target_enabled)
        existing = self.monitoring_repository.get_item_by_symbol(
            stock_code,
            monitor_type="ai_task",
            account_name=account_name,
            asset_id=asset_id,
        )
        if managed_sync and existing and not existing.get("managed_by_portfolio"):
            self.logger.info(f"跳过持仓同步任务 {stock_code}，同账户下手工任务已存在")
            return int(existing["id"])

        task_id = self.monitoring_repository.upsert_item(
            {
                "symbol": stock_code,
                "name": resolved_stock_name,
                "monitor_type": "ai_task",
                "source": "portfolio" if managed_sync else "ai_monitor",
                "enabled": target_enabled,
                "interval_minutes": self._seconds_to_interval_minutes(
                    task_data.get("check_interval", self._default_check_interval_seconds())
                ),
                "trading_hours_only": bool(task_data.get("trading_hours_only", 1)),
                "notification_enabled": True,
                "managed_by_portfolio": managed_sync,
                "account_name": account_name,
                "asset_id": asset_id,
                "portfolio_stock_id": task_data.get("portfolio_stock_id"),
                "origin_analysis_id": task_data.get("origin_analysis_id"),
                "config": self._task_config_from_data(task_data),
            }
        )
        if asset_id is not None:
            self.asset_service.sync_managed_monitors(int(asset_id))
        strategy_context = task_data.get("strategy_context")
        if isinstance(strategy_context, dict) and strategy_context:
            self.monitoring_repository.update_item(
                int(task_id),
                {"config": self._task_config_from_data(task_data)},
            )
        return task_id

    def delete_monitor_task(self, task_id: int):
        item = self.monitoring_repository.get_item(task_id)
        if not item or item.get("monitor_type") != "ai_task":
            return False

        linked_alert = self._get_linked_price_alert_item(item)
        target_asset_id = item.get("asset_id")
        if target_asset_id is not None:
            asset = self.asset_repository.get_asset(int(target_asset_id))
            if asset and bool(asset.get("monitor_enabled", True)):
                self.asset_repository.update_asset(int(target_asset_id), monitor_enabled=False)

        deleted = self.monitoring_repository.delete_item(task_id)
        if linked_alert is not None:
            deleted = self.monitoring_repository.delete_item(int(linked_alert["id"])) or deleted
        return deleted

    def delete_monitor_task_by_code(
        self,
        stock_code: str,
        managed_only: bool = False,
        account_name: Optional[str] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> bool:
        tasks = self.monitoring_repository.list_items(
            monitor_type="ai_task",
            symbol=stock_code,
            managed_by_portfolio=True if managed_only else None,
            account_name=account_name,
            portfolio_stock_id=portfolio_stock_id,
            enabled_only=False,
        )
        deleted = False
        for item in tasks:
            deleted = self.delete_monitor_task(int(item["id"])) or deleted
        return deleted

    def save_ai_decision(self, decision_data: Dict) -> int:
        def _save() -> int:
            conn = self._connect()
            cursor = conn.cursor()
            try:
                record_id = self._insert_ai_decision(cursor, decision_data, default_account=True)
                conn.commit()
                return record_id
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_save)

    def backfill_ai_decision_outcomes(
        self,
        *,
        stock_code: Optional[str] = None,
        account_name: Optional[str] = None,
        limit: int = 160,
        horizon_days: int = 21,
    ) -> Dict[str, Any]:
        normalized_stock = str(stock_code or "").strip().upper()
        normalized_account = self._normalize_account_name_value(account_name, keep_none=True)
        max_rows = max(20, int(limit or 160))
        horizon = max(1, int(horizon_days or 21))

        def _backfill() -> Dict[str, Any]:
            conn = self._connect()
            cursor = conn.cursor()
            try:
                where_parts = []
                params: List[Any] = []
                if normalized_stock:
                    where_parts.append("stock_code = ?")
                    params.append(normalized_stock)
                if normalized_account:
                    where_parts.append("account_name = ?")
                    params.append(normalized_account)
                where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
                cursor.execute(
                    f"""
                    SELECT *
                    FROM ai_decisions
                    {where_sql}
                    ORDER BY datetime(decision_time) DESC, id DESC
                    LIMIT ?
                    """,
                    tuple([*params, max_rows]),
                )
                rows = [dict(row) for row in cursor.fetchall()]
                rows.sort(key=lambda item: (str(item.get("decision_time") or ""), int(item.get("id") or 0)))

                updated: List[Dict[str, Any]] = []
                for index, decision in enumerate(rows):
                    decision_time = self._parse_decision_datetime(decision.get("decision_time"))
                    if decision_time is None or self._decision_market_price(decision) is None:
                        continue

                    future_rows: List[Dict[str, Any]] = []
                    for candidate in rows[index + 1:]:
                        if not self._same_decision_binding(decision, candidate):
                            continue
                        candidate_time = self._parse_decision_datetime(candidate.get("decision_time"))
                        if candidate_time is None or candidate_time <= decision_time:
                            continue
                        if (candidate_time - decision_time).days > horizon:
                            continue
                        future_rows.append(candidate)

                    outcome = self._build_outcome_snapshot(
                        decision,
                        future_rows,
                        horizon_days=horizon,
                    )
                    if not outcome:
                        continue

                    decision_context = self._safe_json_loads(decision.get("decision_context"), {})
                    decision_context = decision_context if isinstance(decision_context, dict) else {}
                    existing = decision_context.get("outcome_snapshot")
                    if (
                        isinstance(existing, dict)
                        and existing.get("version") == outcome["version"]
                        and existing.get("latest_sample_id") == outcome["latest_sample_id"]
                        and existing.get("sample_count") == outcome["sample_count"]
                    ):
                        continue

                    decision_context["outcome_snapshot"] = outcome
                    if self._update_ai_decision_fields(
                        cursor,
                        int(decision["id"]),
                        {"decision_context": json.dumps(decision_context, ensure_ascii=False)},
                    ):
                        self._update_decision_evaluation_outcome(cursor, int(decision["id"]), outcome)
                        updated.append({
                            "decision_id": int(decision["id"]),
                            "stock_code": decision.get("stock_code"),
                            "stock_name": decision.get("stock_name"),
                            "decision_time": decision.get("decision_time"),
                            "action": decision.get("action"),
                            "decision_context": decision_context,
                            "outcome_snapshot": outcome,
                        })

                conn.commit()
                return {
                    "updated": len(updated),
                    "updated_outcomes": updated,
                    "limit": max_rows,
                    "horizon_days": horizon,
                }
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_backfill)

    def get_latest_ai_decision_for_context(
        self,
        *,
        stock_code: str,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> Optional[Dict]:
        decisions = self.get_ai_decisions(stock_code=stock_code, limit=20)
        normalized_account = self._normalize_account_name_value(account_name) or DEFAULT_ACCOUNT_NAME
        target_asset_id = int(asset_id) if asset_id is not None else None
        target_portfolio_stock_id = int(portfolio_stock_id) if portfolio_stock_id is not None else None
        for decision in decisions:
            decision_account_name = (
                self._normalize_account_name_value(decision.get("account_name"), keep_none=True)
                or self._normalize_account_name_value(
                    (decision.get("account_info") or {}).get("account_name"),
                    keep_none=True,
                )
                or DEFAULT_ACCOUNT_NAME
            )
            decision_asset_id = decision.get("asset_id")
            decision_portfolio_stock_id = decision.get("portfolio_stock_id")
            if decision_account_name != normalized_account:
                continue
            if target_asset_id is not None and decision_asset_id is not None and int(decision_asset_id) != target_asset_id:
                continue
            if (
                target_portfolio_stock_id is not None
                and decision_portfolio_stock_id is not None
                and int(decision_portfolio_stock_id) != target_portfolio_stock_id
            ):
                continue
            return decision
        return None

    def save_ai_decision_if_changed(self, decision_data: Dict) -> tuple[int, bool]:
        stock_code = str(decision_data.get("stock_code") or "").strip()
        if not stock_code:
            return self.save_ai_decision(decision_data), True
        latest = self.get_latest_ai_decision_for_context(
            stock_code=stock_code,
            account_name=decision_data.get("account_name"),
            asset_id=decision_data.get("asset_id"),
            portfolio_stock_id=decision_data.get("portfolio_stock_id"),
        )
        latest_action = str((latest or {}).get("action") or "").upper()
        current_action = str(decision_data.get("action") or "").upper()
        latest_action_detail = str((latest or {}).get("action_detail") or "").strip()
        current_action_detail = str(decision_data.get("action_detail") or "").strip()
        latest_swing_execution_mode = str((latest or {}).get("swing_execution_mode") or "").strip()
        current_swing_execution_mode = str(decision_data.get("swing_execution_mode") or "").strip()
        latest_action_ratio_pct = (latest or {}).get("action_ratio_pct")
        current_action_ratio_pct = decision_data.get("action_ratio_pct")
        if (
            latest
            and latest_action
            and latest_action == current_action
            and latest_action_detail == current_action_detail
            and latest_swing_execution_mode == current_swing_execution_mode
            and latest_action_ratio_pct == current_action_ratio_pct
        ):
            return self.save_ai_decision(decision_data), False
        return self.save_ai_decision(decision_data), True

    def _get_evaluations_by_decision_ids(self, decision_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        normalized_ids = [int(item) for item in decision_ids if int(item or 0) > 0]
        if not normalized_ids:
            return {}
        placeholders = ",".join("?" for _ in normalized_ids)
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT *
            FROM monitor_decision_evaluations
            WHERE decision_id IN ({placeholders})
            """,
            tuple(normalized_ids),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        evaluations: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            row["quality_flags"] = self._safe_json_loads(row.pop("quality_flags_json", None), [])
            row["evaluation"] = self._safe_json_loads(row.pop("evaluation_json", None), {})
            evaluations[int(row["decision_id"])] = row
        return evaluations

    def get_ai_decisions(self, stock_code: str = None, limit: int = 100) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        if stock_code:
            cursor.execute(
                """
                SELECT * FROM ai_decisions
                WHERE stock_code = ?
                ORDER BY datetime(decision_time) DESC, id DESC
                LIMIT ?
                """,
                (stock_code, limit),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM ai_decisions
                ORDER BY datetime(decision_time) DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = cursor.fetchall()
        conn.close()
        evaluation_by_id = self._get_evaluations_by_decision_ids([int(row["id"]) for row in rows])
        decisions = []
        for row in rows:
            decision = dict(row)
            decision["key_price_levels"] = json.loads(decision["key_price_levels"]) if decision.get("key_price_levels") else {}
            decision["monitor_levels"] = json.loads(decision["monitor_levels"]) if decision.get("monitor_levels") else {}
            decision["decision_context"] = json.loads(decision["decision_context"]) if decision.get("decision_context") else {}
            decision["market_data"] = json.loads(decision["market_data"]) if decision.get("market_data") else {}
            decision["account_info"] = json.loads(decision["account_info"]) if decision.get("account_info") else {}
            decision["account_name"] = self._normalize_account_name_value(decision.get("account_name")) or DEFAULT_ACCOUNT_NAME
            if isinstance(decision.get("account_info"), dict):
                normalized_info_account = self._normalize_account_name_value(
                    decision["account_info"].get("account_name"),
                    keep_none=True,
                )
                if normalized_info_account:
                    decision["account_info"]["account_name"] = normalized_info_account
            decision["stock_name"] = self._resolve_stock_display_name(
                decision.get("stock_code") or "",
                stock_name=decision.get("stock_name"),
                account_name=decision.get("account_name"),
                asset_id=decision.get("asset_id"),
                portfolio_stock_id=decision.get("portfolio_stock_id"),
            )
            decision_context = decision.get("decision_context") if isinstance(decision.get("decision_context"), dict) else {}
            if decision_context:
                decision["swing_execution_mode"] = decision_context.get("swing_execution_mode")
                decision["previous_swing_execution_mode"] = decision_context.get("previous_swing_execution_mode")
                decision["intraday_bias"] = decision_context.get("intraday_bias")
                decision["intraday_bias_text"] = decision_context.get("intraday_bias_text")
                decision["intraday_signal_labels"] = decision_context.get("intraday_signal_labels") or []
                decision["intraday_observations"] = decision_context.get("intraday_observations") or []
                decision["previous_action"] = decision_context.get("previous_action")
                decision["previous_action_detail"] = decision_context.get("previous_action_detail")
                decision["previous_action_ratio_pct"] = decision_context.get("previous_action_ratio_pct")
                decision["decision_changed"] = decision_context.get("decision_changed")
                decision["action_changed"] = decision_context.get("action_changed")
                decision["thresholds_changed"] = decision_context.get("thresholds_changed")
                decision["delta_summary"] = decision_context.get("delta_summary")
                decision["new_intraday_signal_labels"] = decision_context.get("new_intraday_signal_labels") or []
                decision["baseline_relation"] = decision_context.get("baseline_relation")
                decision["matched_baseline_conditions"] = decision_context.get("matched_baseline_conditions") or []
                decision["unmet_baseline_conditions"] = decision_context.get("unmet_baseline_conditions") or []
                decision["baseline_conflict_score"] = decision_context.get("baseline_conflict_score")
                decision["memory_evidence_ids"] = decision_context.get("memory_evidence_ids") or []
                decision["deviation_reason"] = decision_context.get("deviation_reason")
                decision["decision_state"] = decision_context.get("decision_state")
                decision["outcome_snapshot"] = decision_context.get("outcome_snapshot") if isinstance(decision_context.get("outcome_snapshot"), dict) else {}
                decision["decision_quality_score"] = decision_context.get("decision_quality_score")
                decision["quality_flags"] = decision_context.get("quality_flags") or []
                decision["veto_reason"] = decision_context.get("veto_reason")
                decision["data_freshness_state"] = decision_context.get("data_freshness_state")
                decision["baseline_quality_snapshot"] = decision_context.get("baseline_quality_snapshot") if isinstance(decision_context.get("baseline_quality_snapshot"), dict) else {}
                decision["decision_audit"] = decision_context.get("decision_audit") if isinstance(decision_context.get("decision_audit"), dict) else {}
                decision["auto_baseline_reanalysis"] = (
                    decision_context.get("auto_baseline_reanalysis")
                    if isinstance(decision_context.get("auto_baseline_reanalysis"), dict)
                    else {}
                )
            evaluation = evaluation_by_id.get(int(decision["id"]))
            if evaluation:
                decision["evaluation"] = evaluation
                if decision.get("decision_quality_score") in (None, ""):
                    decision["decision_quality_score"] = evaluation.get("decision_quality_score")
                if not decision.get("quality_flags"):
                    decision["quality_flags"] = evaluation.get("quality_flags") or []
                if not decision.get("veto_reason"):
                    decision["veto_reason"] = evaluation.get("veto_reason")
            else:
                decision["evaluation"] = {}
            unified_view = self._build_unified_decision_view(decision)
            decision.update(unified_view)
            decision["final_decision"] = unified_view
            decisions.append(decision)
        return decisions

    def get_ai_decision_intraday_summary(self, limit: int = 120) -> Dict:
        summary_limit = max(1, int(limit or 120))
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, stock_code, stock_name, decision_time, action, decision_context
            FROM ai_decisions
            ORDER BY datetime(decision_time) DESC, id DESC
            LIMIT ?
            """,
            (summary_limit,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        evaluation_by_id = self._get_evaluations_by_decision_ids([int(row["id"]) for row in rows])

        action_counts: Counter[str] = Counter()
        bias_counts: Counter[str] = Counter()
        bias_action_counts: dict[str, Counter[str]] = defaultdict(Counter)
        signal_label_counts: Counter[str] = Counter()
        baseline_relation_counts: Counter[str] = Counter()
        decision_state_counts: Counter[str] = Counter()
        outcome_label_counts: Counter[str] = Counter()
        stock_relation_counts: dict[str, Counter[str]] = defaultdict(Counter)
        stock_names: Dict[str, str] = {}
        with_intraday_context = 0
        with_baseline_relation = 0
        conflict_score_sum = 0.0
        conflict_score_count = 0
        outcome_count = 0
        quality_score_sum = 0.0
        quality_score_count = 0
        veto_reason_counts: Counter[str] = Counter()
        feedback_counts: Counter[str] = Counter()
        evaluated_action_counts: dict[str, Counter[str]] = defaultdict(Counter)
        latest_decision_time = rows[0].get("decision_time") if rows else None

        for row in rows:
            action = str(row.get("action") or "").upper() or "UNKNOWN"
            action_counts[action] += 1
            evaluation = evaluation_by_id.get(int(row.get("id") or 0), {})
            if evaluation:
                try:
                    quality_score = float(evaluation.get("decision_quality_score"))
                except (TypeError, ValueError):
                    quality_score = None
                if quality_score is not None:
                    quality_score_sum += quality_score
                    quality_score_count += 1
                veto_reason = str(evaluation.get("veto_reason") or "").strip()
                if veto_reason:
                    veto_reason_counts[veto_reason] += 1
                feedback_status = str(evaluation.get("feedback_status") or "").strip()
                if feedback_status:
                    feedback_counts[feedback_status] += 1
                outcome_label = str(evaluation.get("outcome_label") or "").strip()
                if outcome_label:
                    evaluated_action_counts[action][outcome_label] += 1

            raw_context = row.get("decision_context")
            try:
                decision_context = json.loads(raw_context) if raw_context else {}
            except json.JSONDecodeError:
                decision_context = {}
            if not isinstance(decision_context, dict) or not decision_context:
                continue

            with_intraday_context += 1
            baseline_relation = str(decision_context.get("baseline_relation") or "").strip() or "unknown"
            if baseline_relation != "unknown":
                with_baseline_relation += 1
                baseline_relation_counts[baseline_relation] += 1
                stock_code = str(row.get("stock_code") or "").strip().upper()
                if stock_code:
                    stock_relation_counts[stock_code][baseline_relation] += 1
                    stock_names[stock_code] = str(row.get("stock_name") or stock_code).strip() or stock_code
            decision_state = str(decision_context.get("decision_state") or "").strip()
            if decision_state:
                decision_state_counts[decision_state] += 1
            try:
                conflict_score = float(decision_context.get("baseline_conflict_score"))
            except (TypeError, ValueError):
                conflict_score = None
            if conflict_score is not None:
                conflict_score_sum += conflict_score
                conflict_score_count += 1
            outcome_snapshot = decision_context.get("outcome_snapshot")
            if isinstance(outcome_snapshot, dict):
                outcome_count += 1
                outcome_label = str(outcome_snapshot.get("outcome_label") or "unknown").strip() or "unknown"
                outcome_label_counts[outcome_label] += 1

            intraday_bias = str(decision_context.get("intraday_bias") or "unclassified").strip() or "unclassified"
            bias_counts[intraday_bias] += 1
            bias_action_counts[intraday_bias][action] += 1

            signal_labels = decision_context.get("intraday_signal_labels")
            if isinstance(signal_labels, list):
                for label in signal_labels:
                    normalized_label = str(label or "").strip()
                    if normalized_label:
                        signal_label_counts[normalized_label] += 1

        def _sorted_count_items(counts: Counter[str], *, key_name: str) -> list[Dict[str, object]]:
            return [
                {key_name: key, "count": int(count)}
                for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
            ]

        def _sorted_action_breakdown(counts: Counter[str]) -> list[Dict[str, object]]:
            return [
                {"action": action, "count": int(count)}
                for action, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
            ]

        bias_rows = []
        for bias, count in sorted(bias_counts.items(), key=lambda item: (-item[1], item[0])):
            bias_rows.append(
                {
                    "intraday_bias": bias,
                    "count": int(count),
                    "action_counts": _sorted_action_breakdown(bias_action_counts[bias]),
                }
            )

        total = len(rows)
        coverage_pct = round((with_intraday_context / total * 100) if total else 0.0, 1)
        baseline_total = sum(baseline_relation_counts.values())
        followed_count = baseline_relation_counts.get("followed", 0)
        partially_deviated_count = baseline_relation_counts.get("partially_deviated", 0)
        invalidated_count = baseline_relation_counts.get("invalidated", 0)
        upgraded_count = baseline_relation_counts.get("upgrade_requested", 0)

        def _pct(count: int, denominator: int) -> float:
            return round((count / denominator * 100) if denominator else 0.0, 1)

        action_outcome_rows = []
        for action, counts in sorted(evaluated_action_counts.items(), key=lambda item: item[0]):
            total_count = sum(counts.values())
            favorable = counts.get("favorable_follow_through", 0) + counts.get("take_profit_hit", 0)
            action_outcome_rows.append({
                "action": action,
                "total": int(total_count),
                "favorable_pct": _pct(favorable, total_count),
                "outcome_label_counts": _sorted_count_items(counts, key_name="outcome_label"),
            })

        stock_rows: List[Dict[str, object]] = []
        for stock_code, counts in stock_relation_counts.items():
            stock_total = sum(counts.values())
            if not stock_total:
                continue
            stock_rows.append({
                "stock_code": stock_code,
                "stock_name": stock_names.get(stock_code, stock_code),
                "total": int(stock_total),
                "followed_pct": _pct(counts.get("followed", 0), stock_total),
                "deviation_pct": _pct(counts.get("partially_deviated", 0), stock_total),
                "invalidated_pct": _pct(counts.get("invalidated", 0), stock_total),
            })
        stock_rows.sort(key=lambda item: (-int(item["total"]), str(item["stock_code"])))

        return {
            "limit": summary_limit,
            "total": total,
            "with_intraday_context": with_intraday_context,
            "coverage_pct": coverage_pct,
            "latest_decision_time": latest_decision_time,
            "action_counts": _sorted_count_items(action_counts, key_name="action"),
            "intraday_bias_counts": bias_rows,
            "signal_label_counts": _sorted_count_items(signal_label_counts, key_name="label")[:8],
            "decision_quality": {
                "scored_count": int(quality_score_count),
                "avg_score": round(quality_score_sum / quality_score_count, 1) if quality_score_count else 0.0,
                "veto_reason_counts": _sorted_count_items(veto_reason_counts, key_name="veto_reason")[:8],
                "feedback_counts": _sorted_count_items(feedback_counts, key_name="feedback_status"),
                "action_outcomes": action_outcome_rows,
                "manual_acceptance_pct": _pct(
                    feedback_counts.get("accepted", 0) + feedback_counts.get("done", 0),
                    sum(feedback_counts.values()),
                ),
            },
            "baseline_compliance": {
                "total": int(baseline_total),
                "with_baseline_relation": int(with_baseline_relation),
                "followed_pct": _pct(followed_count, baseline_total),
                "partial_deviation_pct": _pct(partially_deviated_count, baseline_total),
                "invalidated_pct": _pct(invalidated_count, baseline_total),
                "upgrade_requested_pct": _pct(upgraded_count, baseline_total),
                "avg_conflict_score": round(conflict_score_sum / conflict_score_count, 1) if conflict_score_count else 0.0,
                "relation_counts": _sorted_count_items(baseline_relation_counts, key_name="baseline_relation"),
                "decision_state_counts": _sorted_count_items(decision_state_counts, key_name="decision_state"),
                "outcome_count": int(outcome_count),
                "outcome_label_counts": _sorted_count_items(outcome_label_counts, key_name="outcome_label"),
                "stock_rows": stock_rows[:10],
            },
        }

    def update_decision_execution(self, decision_id: int, executed: bool, result: str):
        def _update() -> None:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE ai_decisions
                SET executed = ?, execution_result = ?, action_status = ?
                WHERE id = ?
                """,
                (1 if executed else 0, result, "accepted" if executed else "suggested", decision_id),
            )
            cursor.execute(
                """
                UPDATE monitor_decision_evaluations
                SET feedback_status = CASE WHEN ? = 1 THEN 'accepted' ELSE feedback_status END,
                    feedback_note = CASE WHEN ? != '' THEN ? ELSE feedback_note END,
                    updated_at = ?
                WHERE decision_id = ?
                """,
                (1 if executed else 0, result or "", result or "", local_now_str(), decision_id),
            )
            conn.commit()
            conn.close()

        run_with_monitoring_write_lock(_update)

    def record_decision_feedback(
        self,
        decision_id: int,
        *,
        status: str,
        note: str = "",
        actual_action_id: Optional[int] = None,
    ) -> bool:
        normalized_status = str(status or "").strip().lower()
        if normalized_status not in {"accepted", "rejected", "ignored", "wrong", "done"}:
            return False

        def _update() -> bool:
            conn = self._connect()
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT * FROM ai_decisions WHERE id = ?", (decision_id,))
                decision = dict(cursor.fetchone() or {})
                if decision:
                    self._upsert_decision_evaluation_from_payload(
                        cursor,
                        decision_id,
                        {
                            **decision,
                            "decision_context": self._safe_json_loads(decision.get("decision_context"), {}),
                        },
                    )
                cursor.execute(
                    """
                    UPDATE monitor_decision_evaluations
                    SET feedback_status = ?,
                        feedback_note = ?,
                        actual_action_id = COALESCE(?, actual_action_id),
                        updated_at = ?
                    WHERE decision_id = ?
                    """,
                    (
                        normalized_status,
                        note,
                        actual_action_id,
                        local_now_str(),
                        decision_id,
                    ),
                )
                changed = cursor.rowcount > 0
                if normalized_status in {"accepted", "done"}:
                    cursor.execute(
                        """
                        UPDATE ai_decisions
                        SET action_status = ?, executed = ?
                        WHERE id = ?
                        """,
                        ("accepted", 1, decision_id),
                    )
                elif normalized_status in {"rejected", "ignored", "wrong"}:
                    cursor.execute(
                        """
                        UPDATE ai_decisions
                        SET action_status = ?
                        WHERE id = ?
                        """,
                        (normalized_status, decision_id),
                    )
                conn.commit()
                return changed
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_update)

    def save_trade_record(self, trade_data: Dict) -> int:
        pending_action = None
        if trade_data.get("pending_action_id"):
            pending_action = self.asset_repository.get_pending_action(int(trade_data.get("pending_action_id")))
        result = self.asset_service.record_manual_trade(
            asset_id=int(trade_data.get("asset_id") or trade_data.get("portfolio_stock_id") or 0),
            trade_type=str(trade_data.get("trade_type", "")).lower(),
            quantity=int(trade_data.get("quantity") or 0),
            price=float(trade_data.get("price") or 0),
            trade_date=trade_data.get("trade_date"),
            note=trade_data.get("note") or "",
            trade_source=trade_data.get("trade_source", "manual"),
            pending_action_id=trade_data.get("pending_action_id"),
        )
        trade_id = int((result[2] or {}).get("id") or 0) if result[0] else 0
        decision_id = (pending_action or {}).get("origin_decision_id") or trade_data.get("origin_decision_id")
        if trade_id and decision_id:
            self.record_decision_feedback(
                int(decision_id),
                status="accepted",
                note=trade_data.get("note") or "已登记人工成交",
                actual_action_id=trade_id,
            )
        return trade_id

    def get_trade_records(self, stock_code: str = None, limit: int = 100) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        sql = [
            """
            SELECT
                t.id,
                s.symbol AS stock_code,
                s.name AS stock_name,
                UPPER(t.trade_type) AS trade_type,
                t.quantity,
                t.price,
                t.price * t.quantity AS amount,
                NULL AS order_id,
                NULL AS order_status,
                NULL AS ai_decision_id,
                t.trade_date AS trade_time,
                0 AS commission,
                0 AS tax,
                0 AS profit_loss,
                t.trade_source
            FROM asset_trade_history t
            INNER JOIN assets s
                ON s.id = t.asset_id
            WHERE 1 = 1
            """
        ]
        params: List[object] = []
        if stock_code:
            sql.append("AND s.symbol = ?")
            params.append(stock_code)
        sql.append("ORDER BY t.trade_date DESC, t.id DESC LIMIT ?")
        params.append(limit)
        cursor.execute(" ".join(sql), tuple(params))
        rows = cursor.fetchall()
        conn.close()
        if rows:
            return [dict(row) for row in rows]
        return []

    def save_position(self, position_data: Dict):
        success, _, stock_id = self.asset_service.promote_to_portfolio(
            symbol=position_data.get("stock_code"),
            stock_name=position_data.get("stock_name") or position_data.get("stock_code"),
            account_name=self._normalize_account_name_value(position_data.get("account_name")) or DEFAULT_ACCOUNT_NAME,
            cost_price=position_data.get("cost_price"),
            quantity=position_data.get("quantity"),
            note=position_data.get("note") or "",
            monitor_enabled=True,
            origin_analysis_id=position_data.get("origin_analysis_id"),
        )
        return stock_id if success else 0

    def get_positions(self) -> List[Dict]:
        positions = []
        for stock in self.asset_repository.list_assets(status=STATUS_PORTFOLIO):
            positions.append(
                {
                    "stock_code": stock["code"],
                    "stock_name": stock.get("name") or stock["code"],
                    "quantity": stock.get("quantity"),
                    "cost_price": stock.get("cost_price"),
                    "current_price": None,
                    "profit_loss": None,
                    "profit_loss_pct": None,
                    "holding_days": None,
                    "buy_date": None,
                    "status": "holding",
                    "account_name": self._normalize_account_name_value(stock.get("account_name")) or DEFAULT_ACCOUNT_NAME,
                }
            )
        if positions:
            return positions
        return []

    def close_position(self, stock_code: str, account_name: str = DEFAULT_ACCOUNT_NAME):
        stock = self.asset_repository.get_asset_by_symbol(stock_code, account_name)
        if not stock:
            return False
        return self.asset_service.clear_position_to_watchlist(
            stock["id"],
            note="手动清仓",
            last_trade_at=local_now_str(),
        )

    def save_notification(self, notify_data: Dict) -> int:
        def _save() -> int:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO notifications
                (stock_code, notify_type, notify_target, subject, content, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    notify_data.get("stock_code"),
                    notify_data.get("notify_type"),
                    notify_data.get("notify_target"),
                    notify_data.get("subject"),
                    notify_data.get("content"),
                    notify_data.get("status", "pending"),
                ),
            )
            notify_id = int(cursor.lastrowid)
            conn.commit()
            conn.close()
            return notify_id

        return run_with_monitoring_write_lock(_save)

    def update_notification_status(self, notify_id: int, status: str, error_msg: str = None):
        def _update() -> None:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE notifications
                SET status = ?, error_msg = ?, sent_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (status, error_msg, notify_id),
            )
            conn.commit()
            conn.close()

        run_with_monitoring_write_lock(_update)

    def create_pending_action(self, *, asset_id: int, action_type: str, origin_decision_id: Optional[int] = None, payload: Optional[Dict] = None) -> int:
        return self.asset_repository.create_pending_action(
            asset_id=asset_id,
            action_type=action_type,
            origin_decision_id=origin_decision_id,
            payload=payload or {},
        )

    def get_pending_actions(
        self,
        *,
        status: Optional[str] = "pending",
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict]:
        return self.asset_repository.list_pending_actions(
            status=status,
            account_name=account_name,
            asset_id=asset_id,
            limit=limit,
        )

    def resolve_pending_action(self, action_id: int, *, status: str, resolution_note: str = "") -> bool:
        pending_action = self.asset_repository.get_pending_action(action_id)
        normalized_status = str(status or "").strip().lower()
        queue_status = {
            "done": "accepted",
            "ignored": "rejected",
            "wrong": "rejected",
        }.get(normalized_status, normalized_status)
        changed = self.asset_repository.update_pending_action(
            action_id,
            status=queue_status,
            resolution_note=resolution_note,
        )
        if changed and pending_action and pending_action.get("origin_decision_id"):
            feedback_status = {
                "done": "done",
                "accepted": "accepted",
                "rejected": "rejected",
                "ignored": "ignored",
                "wrong": "wrong",
            }.get(normalized_status, queue_status)
            self.record_decision_feedback(
                int(pending_action["origin_decision_id"]),
                status=feedback_status,
                note=resolution_note,
                actual_action_id=action_id if feedback_status in {"accepted", "done"} else None,
            )
        return changed
