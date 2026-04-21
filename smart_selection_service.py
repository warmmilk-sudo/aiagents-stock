from __future__ import annotations

import concurrent.futures
import json
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import schedule

import research_hub_service
from asset_repository import STATUS_FOCUS, STATUS_RESEARCH, asset_repository
from investment_db_utils import connect_sqlite, get_metadata, set_metadata
from sector_strategy_db import SectorStrategyDatabase


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
            "observed_startup_candidates": items.get("observed_startup_candidates", []),
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
            agent_composite_score * agent_weight
            + execution_composite_score * execution_weight
            + readiness_adjustment,
            2,
        )
        return {
            "agent_composite_score": round(agent_composite_score, 2),
            "execution_composite_score": round(execution_composite_score, 2),
            "score_fusion_weights": {
                "agent": round(agent_weight, 2),
                "execution": round(execution_weight, 2),
            },
            "readiness_adjustment": round(readiness_adjustment, 2),
            "fusion_score": fusion_score,
        }

    def _build_candidate_metrics(self, candidate: dict[str, Any], lifecycle_item: dict[str, Any]) -> dict[str, Any]:
        metrics = candidate.get("technical_metrics") or {}
        anticipation_score = round((_safe_float(candidate.get("heat_score")) * 0.55) + (_safe_float(metrics.get("trend_score")) * 0.25) + (_safe_float(metrics.get("chip_score")) * 0.2), 2)
        washout_score = round((_safe_float(metrics.get("reversal_score")) * 0.5) + (_safe_float(metrics.get("mean_reversion_score")) * 0.5), 2)
        volume_contraction_days = _safe_int(metrics.get("volume_contraction_days"))
        shrinkage_score = round(min(100.0, _safe_float(metrics.get("volume_score")) * 0.35 + volume_contraction_days * 16 + max(0.0, 12 - abs(_safe_float(metrics.get("bias_pct")))) * 2.2), 2)
        relative_strength_score = round((_safe_float(metrics.get("order_flow_score")) * 0.45) + (_safe_float(metrics.get("chip_score")) * 0.25) + (_safe_float(metrics.get("trend_score")) * 0.3), 2)
        tail_confirmation_score = round((_safe_float(metrics.get("intraday_score")) * 0.75) + (10 if str(metrics.get("intraday_bias") or "") in {"trend_continuation", "pullback_support"} else 0), 2)
        distribution_penalty = round(_safe_float(metrics.get("distribution_risk")), 2)
        execution_composite_score = round(
            anticipation_score * 0.18
            + washout_score * 0.2
            + shrinkage_score * 0.18
            + relative_strength_score * 0.2
            + tail_confirmation_score * 0.24
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
            "distribution_penalty": distribution_penalty,
            "execution_composite_score": execution_composite_score,
            "agent_composite_score": fusion_payload.get("agent_composite_score"),
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
        }

    def _format_result_item(self, candidate: dict[str, Any], lifecycle_item: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        return {
            "asset_id": candidate.get("asset_id"),
            "symbol": candidate.get("symbol"),
            "name": candidate.get("name") or candidate.get("symbol"),
            "primary_sector": candidate.get("primary_sector"),
            "score": metrics.get("composite_score"),
            "heat_score": candidate.get("heat_score"),
            "tech_score": candidate.get("tech_score"),
            "agent_composite_score": metrics.get("agent_composite_score"),
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
            "distribution_penalty": metrics.get("distribution_penalty"),
            "market_cap": candidate.get("market_cap"),
            "tail_session": bool(metrics.get("tail_session")),
            "latest_minute_time": metrics.get("latest_minute_time"),
            "latest_trade_time": metrics.get("latest_trade_time"),
            "realtime_freshness": metrics.get("realtime_freshness") if isinstance(metrics.get("realtime_freshness"), dict) else {},
            "market_state": metrics.get("market_state"),
            "market_state_label": metrics.get("market_state_label"),
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
            if (
                normalized_name in lifecycle_key
                or lifecycle_key in normalized_name
                or _is_ordered_subsequence(normalized_name, lifecycle_key)
                or _is_ordered_subsequence(lifecycle_key, normalized_name)
            ):
                fallback_candidates.append(
                    (
                        _safe_float(lifecycle_item.get("heat_score"), 0.0),
                        lifecycle_key,
                        lifecycle_item,
                    )
                )
        if not fallback_candidates:
            return normalized_name, None
        fallback_candidates.sort(key=lambda item: item[0], reverse=True)
        _, matched_key, lifecycle_item = fallback_candidates[0]
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
            alias_name = research_hub_service._normalize_sector_text(hot_item.get("sector"))
            if not alias_name:
                continue
            matched_name, lifecycle_item = self._find_lifecycle_match(hot_item.get("sector"), lifecycle_by_name)
            if not lifecycle_item:
                continue
            lifecycle_stage = str(lifecycle_item.get("lifecycle_stage") or "")
            if lifecycle_stage not in HOT_LIFECYCLE_STAGES:
                continue
            matched_sector_name = matched_name or alias_name
            payload = {
                "sector": hot_item.get("sector") or lifecycle_item.get("sector_name"),
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
            }
            existing = selection_sector_store.get(matched_sector_name)
            if existing is None or _safe_float(payload.get("heat_score"), 0.0) > _safe_float(existing.get("heat_score"), 0.0):
                selection_sector_store[matched_sector_name] = payload
            selected_lifecycle_by_name[matched_sector_name] = lifecycle_item
            selected_lifecycle_by_name[alias_name] = lifecycle_item

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
        report_id = int(sector_info.get("report_id") or 0)
        lifecycle_snapshot = self.sector_strategy_db.get_lifecycle_items_for_analysis(report_id) if report_id else []
        if not lifecycle_snapshot:
            warnings.append("最新智策报告缺少生命周期数据，智能选股降级为空结果")
            lifecycle_summary = self.sector_strategy_db.build_lifecycle_summary([])
            return {
                "sector_strategy_report_id": report_id,
                "sector_strategy_reused": bool(sector_info.get("reused")),
                "lifecycle_summary": lifecycle_summary,
                "observed_startup_candidates": [],
                "observed_decay_candidates": [],
                "ranked_action_candidates": [],
                "final_selected": [],
                "excluded_by_lifecycle_veto": [],
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
                "extracted_sectors": extracted_sectors,
                "selection_sectors": [],
                "lifecycle_summary": lifecycle_summary,
                "observed_startup_candidates": [],
                "observed_decay_candidates": [],
                "ranked_action_candidates": [],
                "final_selected": [],
                "excluded_by_lifecycle_veto": [],
                "excluded_by_risk_veto": [],
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
        vetoed_candidates: list[dict[str, Any]] = []
        max_workers = self.get_scheduler_config().get("max_workers", DEFAULT_MAX_WORKERS)

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
            primary_sector = research_hub_service._normalize_sector_text(candidate.get("primary_sector"))
            lifecycle_item = lifecycle_by_name.get(primary_sector)
            if not lifecycle_item:
                return None
            metrics = self._build_candidate_metrics(candidate, lifecycle_item)
            result_item = self._format_result_item(candidate, lifecycle_item, metrics)

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
                if bucket == "veto":
                    vetoed_candidates.append(result_item)
                elif bucket == "startup":
                    startup_by_sector.setdefault(primary_sector, []).append(result_item)
                elif bucket == "decay":
                    decay_candidates.append(result_item)
                elif bucket == "explosive":
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

        report_progress(72, "筛选尾盘执行候选...")
        ranked_action_candidates = sorted(explosive_candidates, key=lambda item: _safe_float(item.get("score")), reverse=True)

        final_selected: list[dict[str, Any]] = []
        risk_vetoed_candidates: list[dict[str, Any]] = []
        execution_gated_candidates: list[dict[str, Any]] = []
        sector_counts: dict[str, int] = {}
        longhubang_map = research_hub_service._group_recent_longhubang_by_symbol(days=3, warnings=warnings)
        filtered_by_tail = 0
        filtered_by_freshness = 0
        manual_relaxed_candidates = 0
        pre_risk_candidates: list[dict[str, Any]] = []
        min_shrinkage = 55.0 if strict_execution_mode else 50.0
        min_relative_strength = 55.0 if strict_execution_mode else 50.0
        min_tail_confirmation = 55.0 if strict_execution_mode else 45.0

        def build_execution_gate_item(item: dict[str, Any], gate_reason: str, gate_type: str) -> dict[str, Any]:
            gated_item = dict(item)
            gated_item["execution_gate_reason"] = gate_reason
            gated_item["execution_gate_type"] = gate_type
            gated_item["reason"] = f"{item.get('reason') or ''} | {gate_reason}".strip(" |")
            return gated_item

        for item in ranked_action_candidates:
            tail_ready = bool(item.get("tail_session"))
            realtime_freshness = item.get("realtime_freshness") if isinstance(item.get("realtime_freshness"), dict) else {}
            freshness_ready = realtime_freshness.get("intraday_decision_ready") is True
            item["execution_ready"] = bool(tail_ready and freshness_ready)
            item["execution_mode"] = "strict" if strict_execution_mode else "manual_explore"
            if strict_execution_mode and not tail_ready:
                filtered_by_tail += 1
                execution_gated_candidates.append(build_execution_gate_item(item, "未到尾盘执行时段", "tail_session"))
                continue
            if strict_execution_mode and not freshness_ready:
                filtered_by_freshness += 1
                execution_gated_candidates.append(build_execution_gate_item(item, "分时新鲜度不足", "realtime_freshness"))
                continue
            if not strict_execution_mode and not item["execution_ready"]:
                manual_relaxed_candidates += 1
                suffix = "手工触发盘中探索模式，先保留候选不做尾盘一刀切"
                item["reason"] = f"{item.get('reason') or ''} | {suffix}".strip(" |")
            if _safe_float(item.get("shrinkage_score")) < min_shrinkage:
                execution_gated_candidates.append(
                    build_execution_gate_item(item, f"缩量分不足 {min_shrinkage:.0f}", "shrinkage_score")
                )
                continue
            if _safe_float(item.get("relative_strength_score")) < min_relative_strength:
                execution_gated_candidates.append(
                    build_execution_gate_item(item, f"相对强度不足 {min_relative_strength:.0f}", "relative_strength_score")
                )
                continue
            if _safe_float(item.get("tail_confirmation_score")) < min_tail_confirmation:
                execution_gated_candidates.append(
                    build_execution_gate_item(item, f"尾盘确认不足 {min_tail_confirmation:.0f}", "tail_confirmation_score")
                )
                continue
            pre_risk_candidates.append(item)

        risk_results: dict[str, dict[str, Any]] = {}

        def evaluate_item_risk(item: dict[str, Any]) -> tuple[str, dict[str, Any], list[str]]:
            symbol = str(item.get("symbol") or "")
            local_warnings: list[str] = []
            client = research_hub_service.DeepSeekClient(
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
            if sector_counts.get(bucket, 0) >= 2:
                continue
            sector_counts[bucket] = sector_counts.get(bucket, 0) + 1
            final_selected.append(item)

        if filtered_by_tail:
            warnings.append(f"{filtered_by_tail} 只爆发期候选因未到尾盘时段而仅保留观察，不进入执行名单")
        if filtered_by_freshness:
            warnings.append(f"{filtered_by_freshness} 只爆发期候选因分时新鲜度不足而未进入执行名单")
        if manual_relaxed_candidates and not strict_execution_mode:
            warnings.append(f"{manual_relaxed_candidates} 只爆发期候选按手工盘中探索模式放宽尾盘时段与分时新鲜度限制")

        self._replace_run_items(run_id, "observed_startup_candidates", observed_startup_candidates)
        self._replace_run_items(run_id, "observed_decay_candidates", observed_decay_candidates)
        self._replace_run_items(run_id, "ranked_action_candidates", ranked_action_candidates)
        self._replace_run_items(run_id, "excluded_by_execution_gate", execution_gated_candidates)
        self._replace_run_items(run_id, "final_selected", final_selected)
        self._replace_run_items(run_id, "excluded_by_lifecycle_veto", vetoed_candidates)
        self._replace_run_items(run_id, "excluded_by_risk_veto", risk_vetoed_candidates)
        self._upsert_watch_pool(run_id, observed_startup_candidates)

        lifecycle_summary = self.sector_strategy_db.build_lifecycle_summary(lifecycle_snapshot)
        report_progress(100, "智能选股完成")
        return {
            "sector_strategy_report_id": report_id,
            "sector_strategy_reused": bool(sector_info.get("reused")),
            "market_context": market_context,
            "extracted_sectors": extracted_sectors,
            "selection_sectors": selection_sectors,
            "lifecycle_summary": lifecycle_summary,
            "observed_startup_candidates": observed_startup_candidates,
            "observed_decay_candidates": observed_decay_candidates,
            "ranked_action_candidates": ranked_action_candidates,
            "excluded_by_execution_gate": execution_gated_candidates,
            "final_selected": final_selected,
            "excluded_by_lifecycle_veto": vetoed_candidates,
            "excluded_by_risk_veto": risk_vetoed_candidates,
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
        final_lookup = {str(item.get("symbol") or "").strip(): item for item in final_selected if str(item.get("symbol") or "").strip()}
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
