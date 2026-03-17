import json
import math
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

import config
from investment_db_utils import (
    DEFAULT_ACCOUNT_NAME,
    SQLITE_BUSY_TIMEOUT_MILLISECONDS,
    SQLITE_TIMEOUT_SECONDS,
    configure_sqlite_connection,
    normalize_account_name,
    resolve_investment_db_path,
    run_with_monitoring_write_lock,
)
from time_utils import local_now_str


def resolve_monitoring_db_path(seed_db_path: str) -> str:
    """Backwards-compatible alias pointing all monitor facades to investment.db."""
    return resolve_investment_db_path(seed_db_path)


class MonitoringRepository:
    """Canonical monitoring storage for AI tasks and price alerts."""

    CONNECTION_TIMEOUT_SECONDS = SQLITE_TIMEOUT_SECONDS
    BUSY_TIMEOUT_MILLISECONDS = SQLITE_BUSY_TIMEOUT_MILLISECONDS
    INDEX_MIGRATION_KEY = "monitoring_index_migration_v1"
    MANAGED_BINDING_REPAIR_KEY = "monitoring_managed_binding_repair_v1"
    CONFIG_CLEANUP_MIGRATION_KEY = "monitoring_config_cleanup_v1"
    DIRTY_DATA_CLEANUP_KEY = "monitoring_dirty_data_cleanup_v1"
    ACCOUNT_NORMALIZATION_KEY = "monitoring_account_normalization_v1"
    ACCOUNT_RISK_PROFILE_KEY_PREFIX = "smart_monitor_account_risk_profile::"
    SHARED_RISK_PROFILE_KEY = "smart_monitor_shared_risk_profile"

    @staticmethod
    def _clamp_int(value: object, minimum: int, maximum: int, default: int) -> int:
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            numeric = default
        return max(minimum, min(maximum, numeric))

    @staticmethod
    def _normalize_account_name_value(account_name: Optional[str]) -> str:
        return str(normalize_account_name(account_name))

    @classmethod
    def _normalize_item_account_name(
        cls,
        account_name: Optional[object],
        *,
        monitor_type: Optional[str] = None,
        keep_none: bool = False,
    ) -> Optional[str]:
        normalized_monitor_type = str(monitor_type or "").strip().lower()
        resolved_keep_none = keep_none and normalized_monitor_type != "ai_task"
        return normalize_account_name(account_name, keep_none=resolved_keep_none)

    @staticmethod
    def _default_account_risk_profile() -> Dict[str, int]:
        return dict(config.get_smart_monitor_risk_defaults())

    @classmethod
    def _normalize_account_risk_profile(
        cls,
        payload: Optional[Dict],
        *,
        defaults: Optional[Dict[str, int]] = None,
    ) -> Dict[str, int]:
        baseline = dict(defaults or cls._default_account_risk_profile())
        source = payload or {}
        return {
            "position_size_pct": cls._clamp_int(
                source.get("position_size_pct"),
                0,
                100,
                baseline["position_size_pct"],
            ),
            "total_position_pct": cls._clamp_int(
                source.get("total_position_pct"),
                0,
                100,
                baseline["total_position_pct"],
            ),
            "stop_loss_pct": cls._clamp_int(
                source.get("stop_loss_pct"),
                0,
                100,
                baseline["stop_loss_pct"],
            ),
            "take_profit_pct": cls._clamp_int(
                source.get("take_profit_pct"),
                0,
                100,
                baseline["take_profit_pct"],
            ),
        }

    @classmethod
    def _account_risk_profile_key(cls, account_name: Optional[str]) -> str:
        return f"{cls.ACCOUNT_RISK_PROFILE_KEY_PREFIX}{cls._normalize_account_name_value(account_name)}"
    VALID_MONITOR_TYPES = {"ai_task", "price_alert"}
    VALID_SOURCES = {"manual", "portfolio", "ai_monitor", "legacy_conflict"}
    DEPRECATED_CONFIG_KEYS = {
        "auto_trade",
        "qmt_account_id",
        "quant_enabled",
        "quant_config",
    }

    def __init__(self, db_path: str = "investment.db"):
        self.seed_db_path = db_path
        self.db_path = resolve_investment_db_path(db_path)
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        self._init_database()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=self.CONNECTION_TIMEOUT_SECONDS,
        )
        return configure_sqlite_connection(conn)

    def _init_database(self) -> None:
        conn = self._connect()
        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS monitoring_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    name TEXT NOT NULL,
                    monitor_type TEXT NOT NULL,
                    source TEXT DEFAULT 'manual',
                    enabled INTEGER DEFAULT 1,
                    interval_minutes INTEGER DEFAULT 30,
                    trading_hours_only INTEGER DEFAULT 1,
                    notification_enabled INTEGER DEFAULT 1,
                    managed_by_portfolio INTEGER DEFAULT 0,
                    account_name TEXT,
                    asset_id INTEGER,
                    portfolio_stock_id INTEGER,
                    origin_analysis_id INTEGER,
                    current_price REAL,
                    last_checked TEXT,
                    last_status TEXT,
                    last_message TEXT,
                    config_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS migration_conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_db TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    symbol TEXT,
                    conflict_type TEXT NOT NULL,
                    payload_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS monitoring_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitoring_item_id INTEGER,
                    symbol TEXT NOT NULL,
                    name TEXT,
                    monitor_type TEXT,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details_json TEXT,
                    notification_pending INTEGER DEFAULT 0,
                    sent INTEGER DEFAULT 0,
                    is_read INTEGER DEFAULT 0,
                    read_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (monitoring_item_id) REFERENCES monitoring_items (id)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS monitoring_price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    monitoring_item_id INTEGER NOT NULL,
                    price REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (monitoring_item_id) REFERENCES monitoring_items (id)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS monitoring_metadata (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._ensure_column(cursor, "monitoring_items", "account_name", "TEXT")
            self._ensure_column(cursor, "monitoring_items", "asset_id", "INTEGER")
            self._ensure_column(cursor, "monitoring_items", "portfolio_stock_id", "INTEGER")
            self._ensure_column(cursor, "monitoring_items", "origin_analysis_id", "INTEGER")
            self._ensure_column(cursor, "monitoring_events", "is_read", "INTEGER DEFAULT 0")
            self._ensure_column(cursor, "monitoring_events", "read_at", "TEXT")
            self._cleanup_dirty_monitoring_data_if_needed(cursor)
            self._migrate_indexes_if_needed(cursor)
            self._ensure_indexes(cursor)
            self._repair_managed_bindings_if_needed(cursor)
            self._repair_portfolio_state_drift(cursor)
            self._cleanup_deprecated_config_keys_if_needed(cursor)
            self._normalize_account_data_if_needed(cursor)
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _ensure_column(cursor, table: str, column: str, definition: str) -> None:
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table,),
        )
        return cursor.fetchone() is not None

    @staticmethod
    def _get_metadata_from_cursor(cursor: sqlite3.Cursor, key: str) -> Optional[str]:
        cursor.execute("SELECT meta_value FROM monitoring_metadata WHERE meta_key = ?", (key,))
        row = cursor.fetchone()
        return row[0] if row else None

    @staticmethod
    def _set_metadata_on_cursor(cursor: sqlite3.Cursor, key: str, value: str) -> None:
        cursor.execute(
            """
            INSERT INTO monitoring_metadata (meta_key, meta_value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(meta_key) DO UPDATE SET
                meta_value = excluded.meta_value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value),
        )

    def _migrate_indexes_if_needed(self, cursor: sqlite3.Cursor) -> None:
        if self._get_metadata_from_cursor(cursor, self.INDEX_MIGRATION_KEY):
            return

        cursor.execute(
            "SELECT name, COALESCE(sql, '') AS sql FROM sqlite_master WHERE type='index' AND tbl_name='monitoring_items'"
        )
        index_sql_map = {row[0]: row[1] for row in cursor.fetchall()}

        legacy_indexes = {
            "idx_monitoring_ai_task_symbol",
            "idx_monitoring_managed_alert_symbol",
        }
        for index_name in legacy_indexes:
            if index_name in index_sql_map:
                cursor.execute(f"DROP INDEX IF EXISTS {index_name}")

        asset_index_sql = (index_sql_map.get("idx_monitoring_asset_type") or "").upper()
        if asset_index_sql and (
            "UNIQUE" not in asset_index_sql or "ASSET_ID IS NOT NULL" not in asset_index_sql
        ):
            cursor.execute("DROP INDEX IF EXISTS idx_monitoring_asset_type")

        self._set_metadata_on_cursor(
            cursor,
            self.INDEX_MIGRATION_KEY,
            local_now_str(),
        )

    def _ensure_indexes(self, cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_asset_type
            ON monitoring_items(asset_id, monitor_type)
            WHERE asset_id IS NOT NULL
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_ai_task_account_symbol
            ON monitoring_items(account_name, symbol)
            WHERE monitor_type = 'ai_task'
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_managed_alert_position
            ON monitoring_items(portfolio_stock_id, monitor_type)
            WHERE monitor_type = 'price_alert'
              AND managed_by_portfolio = 1
              AND portfolio_stock_id IS NOT NULL
            """
        )

    def _repair_managed_bindings_if_needed(self, cursor: sqlite3.Cursor) -> None:
        repaired = self._repair_managed_bindings(cursor)
        if repaired:
            self._set_metadata_on_cursor(
                cursor,
                self.MANAGED_BINDING_REPAIR_KEY,
                json.dumps(
                    {
                        "repaired": repaired,
                        "updated_at": local_now_str(),
                    },
                    ensure_ascii=False,
                ),
            )

    def _repair_managed_bindings(self, cursor: sqlite3.Cursor) -> int:
        connection = cursor.connection
        if not self._table_exists(connection, "monitoring_items"):
            return 0

        cursor.execute(
            """
            SELECT id, symbol, account_name, asset_id, portfolio_stock_id
            FROM monitoring_items
            WHERE managed_by_portfolio = 1
              AND monitor_type IN ('ai_task', 'price_alert')
              AND (
                    asset_id IS NULL
                 OR account_name IS NULL
                 OR TRIM(account_name) = ''
              )
            ORDER BY id ASC
            """
        )
        rows = [dict(row) for row in cursor.fetchall()]
        repaired = 0

        for row in rows:
            resolved = self._resolve_asset_binding_for_managed_item(cursor, row)
            if not resolved:
                continue

            updates: List[str] = []
            params: List[object] = []
            current_account = str(row.get("account_name") or "").strip()
            resolved_account = str(resolved.get("account_name") or "").strip()
            resolved_asset_id = resolved.get("asset_id")

            if row.get("asset_id") is None and resolved_asset_id is not None:
                updates.append("asset_id = ?")
                params.append(resolved_asset_id)

            if not current_account and resolved_account:
                updates.append("account_name = ?")
                params.append(resolved_account)

            if not updates:
                continue

            params.append(row["id"])
            try:
                cursor.execute(
                    f"UPDATE monitoring_items SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    tuple(params),
                )
            except sqlite3.IntegrityError:
                continue
            if cursor.rowcount > 0:
                repaired += 1

        return repaired

    def _repair_portfolio_state_drift(self, cursor: sqlite3.Cursor) -> int:
        connection = cursor.connection
        if not self._table_exists(connection, "monitoring_items") or not self._table_exists(connection, "assets"):
            return 0

        cursor.execute(
            """
            SELECT
                mi.id,
                mi.monitor_type,
                mi.source,
                mi.managed_by_portfolio,
                mi.portfolio_stock_id
            FROM monitoring_items mi
            INNER JOIN assets a
                ON a.id = mi.asset_id
            WHERE mi.asset_id IS NOT NULL
              AND a.deleted_at IS NULL
              AND a.status <> 'portfolio'
              AND (
                    mi.managed_by_portfolio = 1
                 OR mi.portfolio_stock_id IS NOT NULL
                 OR mi.source = 'portfolio'
              )
            ORDER BY mi.id ASC
            """
        )
        rows = [dict(row) for row in cursor.fetchall()]
        repaired = 0

        for row in rows:
            normalized_source = "ai_monitor" if row.get("monitor_type") == "ai_task" else "manual"
            try:
                cursor.execute(
                    """
                    UPDATE monitoring_items
                    SET managed_by_portfolio = 0,
                        portfolio_stock_id = NULL,
                        source = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (normalized_source, row["id"]),
                )
            except sqlite3.IntegrityError:
                continue
            if cursor.rowcount > 0:
                repaired += 1

        return repaired

    def _resolve_asset_binding_for_managed_item(self, cursor: sqlite3.Cursor, item: Dict) -> Optional[Dict]:
        symbol = item.get("symbol")
        account_name = self._normalize_item_account_name(
            item.get("account_name"),
            monitor_type=item.get("monitor_type"),
            keep_none=True,
        )
        asset_id = item.get("asset_id")
        portfolio_stock_id = item.get("portfolio_stock_id")

        if asset_id is not None:
            bound = self._query_asset_by_id(cursor, int(asset_id))
            if bound:
                return bound

        if portfolio_stock_id is not None:
            bound = self._query_asset_by_id(cursor, int(portfolio_stock_id))
            if bound:
                return bound

        if symbol and account_name:
            bound = self._query_asset_by_symbol(cursor, symbol, account_name)
            if bound:
                return bound

        if symbol and not account_name:
            bound = self._query_unique_asset_by_symbol(cursor, symbol)
            if bound:
                return bound

        return None

    def _query_asset_by_id(self, cursor: sqlite3.Cursor, asset_id: int) -> Optional[Dict]:
        if not self._table_exists(cursor.connection, "assets"):
            return None
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
            "asset_id": int(row[0]),
            "account_name": row[1] or DEFAULT_ACCOUNT_NAME,
        }

    def _query_asset_by_symbol(
        self,
        cursor: sqlite3.Cursor,
        symbol: str,
        account_name: str,
    ) -> Optional[Dict]:
        if not self._table_exists(cursor.connection, "assets"):
            return None
        cursor.execute(
            """
            SELECT id, account_name
            FROM assets
            WHERE symbol = ?
              AND account_name = ?
              AND deleted_at IS NULL
            ORDER BY
                CASE status
                    WHEN 'portfolio' THEN 1
                    WHEN 'watchlist' THEN 2
                    ELSE 3
                END,
                id DESC
            LIMIT 1
            """,
            (symbol, account_name),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "asset_id": int(row[0]),
            "account_name": row[1] or account_name,
        }

    def _query_unique_asset_by_symbol(self, cursor: sqlite3.Cursor, symbol: str) -> Optional[Dict]:
        if not self._table_exists(cursor.connection, "assets"):
            return None
        cursor.execute(
            """
            SELECT id, account_name
            FROM assets
            WHERE symbol = ?
              AND deleted_at IS NULL
            ORDER BY id ASC
            """,
            (symbol,),
        )
        rows = cursor.fetchall()
        if len(rows) != 1:
            return None
        row = rows[0]
        return {
            "asset_id": int(row[0]),
            "account_name": row[1] or DEFAULT_ACCOUNT_NAME,
        }

    @staticmethod
    def _normalize_interval_minutes(value: Optional[int]) -> int:
        try:
            interval = int(value or 0)
        except (TypeError, ValueError):
            interval = 0
        return max(1, interval)

    @staticmethod
    def _safe_json_loads(raw_value, default):
        if raw_value in (None, ""):
            return default
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            return json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            return default

    @classmethod
    def _sanitize_monitor_config(cls, config: Optional[Dict]) -> Dict:
        sanitized = dict(config or {})
        for key in cls.DEPRECATED_CONFIG_KEYS:
            sanitized.pop(key, None)
        return sanitized

    @staticmethod
    def _normalize_optional_text(value: Optional[object]) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    @classmethod
    def _normalize_source_for_row(cls, row: Dict) -> str:
        monitor_type = str(row.get("monitor_type") or "").strip().lower()
        managed_by_portfolio = bool(row.get("managed_by_portfolio"))
        if managed_by_portfolio:
            return "portfolio"
        if monitor_type == "ai_task":
            return "ai_monitor"
        return "manual"

    @staticmethod
    def _parse_sortable_timestamp(raw_value: Optional[object]) -> datetime:
        text = str(raw_value or "").strip()
        if not text:
            return datetime.min
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return datetime.min

    def _row_to_item(self, row: sqlite3.Row) -> Dict:
        data = dict(row)
        data["enabled"] = bool(data.get("enabled", 1))
        data["trading_hours_only"] = bool(data.get("trading_hours_only", 1))
        data["notification_enabled"] = bool(data.get("notification_enabled", 1))
        data["managed_by_portfolio"] = bool(data.get("managed_by_portfolio", 0))
        data["config"] = self._sanitize_monitor_config(self._safe_json_loads(data.get("config_json"), {}))
        return data

    def _cleanup_deprecated_config_keys_if_needed(self, cursor: sqlite3.Cursor) -> None:
        if self._get_metadata_from_cursor(cursor, self.CONFIG_CLEANUP_MIGRATION_KEY):
            return

        cursor.execute("SELECT id, config_json FROM monitoring_items")
        changed = 0
        for row in cursor.fetchall():
            item_id = int(row["id"])
            config = self._safe_json_loads(row["config_json"], {})
            sanitized = self._sanitize_monitor_config(config)
            if sanitized == config:
                continue
            cursor.execute(
                "UPDATE monitoring_items SET config_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (json.dumps(sanitized, ensure_ascii=False), item_id),
            )
            if cursor.rowcount > 0:
                changed += 1

        self._set_metadata_on_cursor(
            cursor,
            self.CONFIG_CLEANUP_MIGRATION_KEY,
            json.dumps(
                {
                    "changed": changed,
                    "updated_at": local_now_str(),
                },
                ensure_ascii=False,
            ),
        )

    def _cleanup_dirty_monitoring_data_if_needed(self, cursor: sqlite3.Cursor) -> None:
        if self._get_metadata_from_cursor(cursor, self.DIRTY_DATA_CLEANUP_KEY):
            return

        summary = self._cleanup_dirty_monitoring_data(cursor)
        self._set_metadata_on_cursor(
            cursor,
            self.DIRTY_DATA_CLEANUP_KEY,
            json.dumps(
                {
                    **summary,
                    "updated_at": local_now_str(),
                },
                ensure_ascii=False,
            ),
        )

    def _cleanup_dirty_monitoring_data(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        summary = {
            "invalid_items": 0,
            "normalized_items": 0,
            "orphan_asset_items": 0,
            "deduplicated_items": 0,
            "rewired_events": 0,
            "rewired_price_history": 0,
            "orphan_events": 0,
            "orphan_price_history": 0,
            "invalid_events": 0,
            "repaired_events": 0,
        }

        summary["invalid_items"] += self._delete_invalid_monitoring_items(cursor)
        normalized_count, rewired_event_count, rewired_price_count = self._dedupe_monitoring_items(cursor)
        summary["deduplicated_items"] += normalized_count
        summary["rewired_events"] += rewired_event_count
        summary["rewired_price_history"] += rewired_price_count
        summary["normalized_items"] += self._normalize_monitoring_items(cursor)
        summary["orphan_asset_items"] += self._cleanup_orphan_asset_bindings(cursor)
        summary["invalid_events"] += self._delete_invalid_monitoring_events(cursor)
        summary["orphan_events"] += self._delete_orphan_monitoring_events(cursor)
        summary["orphan_price_history"] += self._delete_orphan_price_history(cursor)
        summary["repaired_events"] += self._repair_monitoring_event_payloads(cursor)
        return summary

    def _delete_item_graph(self, cursor: sqlite3.Cursor, item_id: int) -> int:
        deleted_rows = 0
        cursor.execute("DELETE FROM monitoring_price_history WHERE monitoring_item_id = ?", (item_id,))
        deleted_rows += int(cursor.rowcount or 0)
        cursor.execute("DELETE FROM monitoring_events WHERE monitoring_item_id = ?", (item_id,))
        deleted_rows += int(cursor.rowcount or 0)
        cursor.execute("DELETE FROM monitoring_items WHERE id = ?", (item_id,))
        deleted_rows += int(cursor.rowcount or 0)
        return deleted_rows

    def _delete_invalid_monitoring_items(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute("SELECT id, symbol, monitor_type FROM monitoring_items ORDER BY id ASC")
        invalid_item_ids: List[int] = []
        for row in cursor.fetchall():
            symbol = str(row["symbol"] or "").strip()
            monitor_type = str(row["monitor_type"] or "").strip().lower()
            if not symbol or monitor_type not in self.VALID_MONITOR_TYPES:
                invalid_item_ids.append(int(row["id"]))
        removed = 0
        for item_id in invalid_item_ids:
            removed += self._delete_item_graph(cursor, item_id)
        return removed

    def _normalize_monitoring_items(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute("SELECT * FROM monitoring_items ORDER BY id ASC")
        changed = 0
        for row in cursor.fetchall():
            item = dict(row)
            item_id = int(item["id"])
            symbol = str(item.get("symbol") or "").strip().upper()
            monitor_type = str(item.get("monitor_type") or "").strip().lower()
            name = str(item.get("name") or "").strip() or symbol
            account_name = self._normalize_item_account_name(
                item.get("account_name"),
                monitor_type=monitor_type,
                keep_none=True,
            )
            source = self._normalize_optional_text(item.get("source"))
            normalized_source = source if source in self.VALID_SOURCES else self._normalize_source_for_row(item)
            interval_minutes = self._normalize_interval_minutes(item.get("interval_minutes"))
            config = self._sanitize_monitor_config(self._safe_json_loads(item.get("config_json"), {}))
            config_json = json.dumps(config, ensure_ascii=False)
            updates: List[str] = []
            params: List[object] = []

            if symbol != item.get("symbol"):
                updates.append("symbol = ?")
                params.append(symbol)
            if name != item.get("name"):
                updates.append("name = ?")
                params.append(name)
            if monitor_type != item.get("monitor_type"):
                updates.append("monitor_type = ?")
                params.append(monitor_type)
            if account_name != self._normalize_optional_text(item.get("account_name")):
                updates.append("account_name = ?")
                params.append(account_name)
            if normalized_source != item.get("source"):
                updates.append("source = ?")
                params.append(normalized_source)
            if interval_minutes != int(item.get("interval_minutes") or 0):
                updates.append("interval_minutes = ?")
                params.append(interval_minutes)
            if config_json != (item.get("config_json") or "{}"):
                updates.append("config_json = ?")
                params.append(config_json)

            if not updates:
                continue

            params.append(item_id)
            cursor.execute(
                f"UPDATE monitoring_items SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                tuple(params),
            )
            changed += 1 if cursor.rowcount > 0 else 0
        return changed

    def _cleanup_orphan_asset_bindings(self, cursor: sqlite3.Cursor) -> int:
        if not self._table_exists(cursor.connection, "assets"):
            return 0

        cursor.execute(
            """
            SELECT mi.*
            FROM monitoring_items mi
            LEFT JOIN assets a
                ON a.id = mi.asset_id
            WHERE mi.asset_id IS NOT NULL
              AND (a.id IS NULL OR a.deleted_at IS NOT NULL)
            ORDER BY mi.id ASC
            """
        )
        rows = [dict(row) for row in cursor.fetchall()]
        changed = 0
        for row in rows:
            item_id = int(row["id"])
            if row.get("managed_by_portfolio") or row.get("portfolio_stock_id") is not None or row.get("source") == "portfolio":
                changed += self._delete_item_graph(cursor, item_id)
                continue

            normalized_source = self._normalize_source_for_row(row)
            account_name = self._normalize_item_account_name(
                row.get("account_name"),
                monitor_type=row.get("monitor_type"),
                keep_none=True,
            )

            cursor.execute(
                """
                UPDATE monitoring_items
                SET asset_id = NULL,
                    portfolio_stock_id = NULL,
                    managed_by_portfolio = 0,
                    account_name = ?,
                    source = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (account_name, normalized_source, item_id),
            )
            changed += 1 if cursor.rowcount > 0 else 0
        return changed

    @classmethod
    def _duplicate_priority(cls, row: Dict) -> tuple:
        return (
            1 if row.get("enabled") else 0,
            1 if row.get("managed_by_portfolio") else 0,
            1 if row.get("asset_id") is not None else 0,
            1 if row.get("portfolio_stock_id") is not None else 0,
            cls._parse_sortable_timestamp(row.get("updated_at")),
            cls._parse_sortable_timestamp(row.get("created_at")),
            int(row.get("id") or 0),
        )

    @classmethod
    def _build_deduplication_keys(cls, row: Dict) -> List[tuple]:
        keys: List[tuple] = []
        monitor_type = str(row.get("monitor_type") or "").strip().lower()
        symbol = str(row.get("symbol") or "").strip().upper()
        account_name = cls._normalize_item_account_name(
            row.get("account_name"),
            monitor_type=monitor_type,
            keep_none=True,
        )
        asset_id = row.get("asset_id")
        portfolio_stock_id = row.get("portfolio_stock_id")

        if monitor_type == "ai_task":
            keys.append(("ai_task_symbol", cls._normalize_account_name_value(account_name), symbol))
        if asset_id is not None:
            keys.append(("asset_binding", int(asset_id), monitor_type))
        if monitor_type == "price_alert" and row.get("managed_by_portfolio") and portfolio_stock_id is not None:
            keys.append(("managed_position", int(portfolio_stock_id), monitor_type))
        return keys

    def _dedupe_monitoring_items(self, cursor: sqlite3.Cursor) -> tuple[int, int, int]:
        cursor.execute("SELECT * FROM monitoring_items ORDER BY id ASC")
        rows = [dict(row) for row in cursor.fetchall()]
        if not rows:
            return 0, 0, 0

        ordered_rows = sorted(rows, key=self._duplicate_priority, reverse=True)
        seen_keys: Dict[tuple, int] = {}
        keep_rows: Dict[int, Dict] = {}
        duplicate_pairs: List[tuple[int, int]] = []

        for row in ordered_rows:
            dedupe_keys = self._build_deduplication_keys(row)
            duplicate_keep_id = next((seen_keys[key] for key in dedupe_keys if key in seen_keys), None)
            if duplicate_keep_id is not None:
                duplicate_pairs.append((duplicate_keep_id, int(row["id"])))
                continue
            keep_rows[int(row["id"])] = dict(row)
            for key in dedupe_keys:
                seen_keys[key] = int(row["id"])

        if not duplicate_pairs:
            return 0, 0, 0

        deduped_count = 0
        rewired_events = 0
        rewired_price_history = 0
        for keep_id, drop_id in duplicate_pairs:
            keep_row = keep_rows.get(keep_id)
            drop_row = next((row for row in rows if int(row["id"]) == drop_id), None)
            if not keep_row or not drop_row:
                continue

            keep_config = self._sanitize_monitor_config(self._safe_json_loads(keep_row.get("config_json"), {}))
            drop_config = self._sanitize_monitor_config(self._safe_json_loads(drop_row.get("config_json"), {}))
            merged_config = dict(drop_config)
            merged_config.update(keep_config)
            normalized_account_name = self._normalize_item_account_name(
                keep_row.get("account_name"),
                monitor_type=keep_row.get("monitor_type"),
                keep_none=True,
            )

            cursor.execute(
                """
                UPDATE monitoring_items
                SET name = ?,
                    source = ?,
                    enabled = ?,
                    interval_minutes = ?,
                    trading_hours_only = ?,
                    notification_enabled = ?,
                    managed_by_portfolio = ?,
                    account_name = ?,
                    asset_id = ?,
                    portfolio_stock_id = ?,
                    origin_analysis_id = ?,
                    current_price = ?,
                    last_checked = ?,
                    last_status = ?,
                    last_message = ?,
                    config_json = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    str(keep_row.get("name") or drop_row.get("name") or keep_row.get("symbol") or "").strip() or str(keep_row.get("symbol") or "").strip(),
                    self._normalize_optional_text(keep_row.get("source")) or self._normalize_source_for_row(keep_row),
                    1 if keep_row.get("enabled", drop_row.get("enabled", 1)) else 0,
                    self._normalize_interval_minutes(keep_row.get("interval_minutes") or drop_row.get("interval_minutes")),
                    1 if keep_row.get("trading_hours_only", drop_row.get("trading_hours_only", 1)) else 0,
                    1 if keep_row.get("notification_enabled", drop_row.get("notification_enabled", 1)) else 0,
                    1 if keep_row.get("managed_by_portfolio", drop_row.get("managed_by_portfolio", 0)) else 0,
                    normalized_account_name,
                    keep_row.get("asset_id") if keep_row.get("asset_id") is not None else drop_row.get("asset_id"),
                    keep_row.get("portfolio_stock_id") if keep_row.get("portfolio_stock_id") is not None else drop_row.get("portfolio_stock_id"),
                    keep_row.get("origin_analysis_id") if keep_row.get("origin_analysis_id") is not None else drop_row.get("origin_analysis_id"),
                    keep_row.get("current_price") if keep_row.get("current_price") is not None else drop_row.get("current_price"),
                    keep_row.get("last_checked") or drop_row.get("last_checked"),
                    keep_row.get("last_status") or drop_row.get("last_status"),
                    keep_row.get("last_message") or drop_row.get("last_message"),
                    json.dumps(merged_config, ensure_ascii=False),
                    keep_id,
                ),
            )
            if cursor.rowcount > 0:
                keep_row.update(
                    {
                        "name": str(keep_row.get("name") or drop_row.get("name") or keep_row.get("symbol") or "").strip() or str(keep_row.get("symbol") or "").strip(),
                        "source": self._normalize_optional_text(keep_row.get("source")) or self._normalize_source_for_row(keep_row),
                        "account_name": normalized_account_name,
                        "asset_id": keep_row.get("asset_id") if keep_row.get("asset_id") is not None else drop_row.get("asset_id"),
                        "portfolio_stock_id": keep_row.get("portfolio_stock_id") if keep_row.get("portfolio_stock_id") is not None else drop_row.get("portfolio_stock_id"),
                        "origin_analysis_id": keep_row.get("origin_analysis_id") if keep_row.get("origin_analysis_id") is not None else drop_row.get("origin_analysis_id"),
                        "config_json": json.dumps(merged_config, ensure_ascii=False),
                    }
                )

            cursor.execute(
                """
                UPDATE monitoring_events
                SET monitoring_item_id = ?,
                    symbol = ?,
                    name = ?,
                    monitor_type = ?
                WHERE monitoring_item_id = ?
                """,
                (
                    keep_id,
                    keep_row.get("symbol"),
                    keep_row.get("name"),
                    keep_row.get("monitor_type"),
                    drop_id,
                ),
            )
            rewired_events += int(cursor.rowcount or 0)

            cursor.execute(
                "UPDATE monitoring_price_history SET monitoring_item_id = ? WHERE monitoring_item_id = ?",
                (keep_id, drop_id),
            )
            rewired_price_history += int(cursor.rowcount or 0)

            cursor.execute("DELETE FROM monitoring_items WHERE id = ?", (drop_id,))
            deduped_count += 1 if cursor.rowcount > 0 else 0
        return deduped_count, rewired_events, rewired_price_history

    def _delete_invalid_monitoring_events(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute(
            """
            DELETE FROM monitoring_events
            WHERE monitoring_item_id IS NULL
              AND TRIM(COALESCE(symbol, '')) = ''
            """
        )
        return int(cursor.rowcount or 0)

    def _delete_orphan_monitoring_events(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute(
            """
            DELETE FROM monitoring_events
            WHERE monitoring_item_id IS NOT NULL
              AND monitoring_item_id NOT IN (SELECT id FROM monitoring_items)
            """
        )
        return int(cursor.rowcount or 0)

    def _delete_orphan_price_history(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute(
            """
            DELETE FROM monitoring_price_history
            WHERE monitoring_item_id NOT IN (SELECT id FROM monitoring_items)
            """
        )
        return int(cursor.rowcount or 0)

    def _repair_monitoring_event_payloads(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute(
            """
            SELECT e.id, e.symbol, e.name, e.monitor_type, i.symbol AS item_symbol, i.name AS item_name, i.monitor_type AS item_monitor_type
            FROM monitoring_events e
            INNER JOIN monitoring_items i
                ON i.id = e.monitoring_item_id
            ORDER BY e.id ASC
            """
        )
        changed = 0
        for row in cursor.fetchall():
            event = dict(row)
            symbol = str(event.get("item_symbol") or "").strip().upper()
            name = str(event.get("item_name") or "").strip() or symbol
            monitor_type = str(event.get("item_monitor_type") or "").strip().lower() or None
            if (
                symbol == str(event.get("symbol") or "").strip()
                and name == str(event.get("name") or "").strip()
                and monitor_type == event.get("monitor_type")
            ):
                continue
            cursor.execute(
                """
                UPDATE monitoring_events
                SET symbol = ?, name = ?, monitor_type = ?
                WHERE id = ?
                """,
                (symbol, name, monitor_type, int(event["id"])),
            )
            changed += 1 if cursor.rowcount > 0 else 0
        return changed

    def _normalize_account_risk_profile_metadata(self, cursor: sqlite3.Cursor) -> int:
        cursor.execute(
            """
            SELECT meta_key, meta_value, updated_at
            FROM monitoring_metadata
            WHERE meta_key LIKE ?
            ORDER BY meta_key ASC
            """,
            (f"{self.ACCOUNT_RISK_PROFILE_KEY_PREFIX}%",),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        grouped_rows: Dict[str, List[Dict[str, str]]] = {}
        for row in rows:
            raw_account_name = str(row["meta_key"]).removeprefix(self.ACCOUNT_RISK_PROFILE_KEY_PREFIX).strip()
            canonical_key = self._account_risk_profile_key(raw_account_name)
            grouped_rows.setdefault(canonical_key, []).append(row)

        changed = 0
        for canonical_key, group_rows in grouped_rows.items():
            canonical_row = next((row for row in group_rows if row["meta_key"] == canonical_key), None)
            if canonical_row is None:
                chosen_row = max(
                    group_rows,
                    key=lambda row: (
                        self._parse_sortable_timestamp(row.get("updated_at")),
                        str(row.get("meta_key") or ""),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO monitoring_metadata (meta_key, meta_value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(meta_key) DO UPDATE SET
                        meta_value = excluded.meta_value,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (canonical_key, chosen_row.get("meta_value")),
                )
                changed += 1 if cursor.rowcount > 0 else 0
            for row in group_rows:
                if row["meta_key"] == canonical_key:
                    continue
                cursor.execute(
                    "DELETE FROM monitoring_metadata WHERE meta_key = ?",
                    (row["meta_key"],),
                )
                changed += 1 if cursor.rowcount > 0 else 0
        return changed

    def _normalize_account_data_if_needed(self, cursor: sqlite3.Cursor) -> None:
        if self._get_metadata_from_cursor(cursor, self.ACCOUNT_NORMALIZATION_KEY):
            return

        normalized_items = self._normalize_monitoring_items(cursor)
        deduplicated_items, rewired_events, rewired_price_history = self._dedupe_monitoring_items(cursor)
        normalized_items += self._normalize_monitoring_items(cursor)
        risk_profile_updates = self._normalize_account_risk_profile_metadata(cursor)
        self._set_metadata_on_cursor(
            cursor,
            self.ACCOUNT_NORMALIZATION_KEY,
            json.dumps(
                {
                    "normalized_items": normalized_items,
                    "deduplicated_items": deduplicated_items,
                    "rewired_events": rewired_events,
                    "rewired_price_history": rewired_price_history,
                    "risk_profile_updates": risk_profile_updates,
                    "updated_at": local_now_str(),
                },
                ensure_ascii=False,
            ),
        )

    def _set_metadata(self, key: str, value: str) -> None:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO monitoring_metadata (meta_key, meta_value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(meta_key) DO UPDATE SET
                    meta_value = excluded.meta_value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (key, value),
            )
            conn.commit()
        finally:
            conn.close()

    def _get_metadata(self, key: str) -> Optional[str]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT meta_value FROM monitoring_metadata WHERE meta_key = ?", (key,))
            row = cursor.fetchone()
            return row["meta_value"] if row else None
        finally:
            conn.close()

    def _list_metadata(self, prefix: Optional[str] = None) -> Dict[str, str]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if prefix:
                cursor.execute(
                    "SELECT meta_key, meta_value FROM monitoring_metadata WHERE meta_key LIKE ? ORDER BY meta_key ASC",
                    (f"{prefix}%",),
                )
            else:
                cursor.execute("SELECT meta_key, meta_value FROM monitoring_metadata ORDER BY meta_key ASC")
            return {
                str(row["meta_key"]): str(row["meta_value"])
                for row in cursor.fetchall()
            }
        finally:
            conn.close()

    def get_metadata(self, key: str) -> Optional[str]:
        return self._get_metadata(key)

    def set_metadata(self, key: str, value: str) -> None:
        self._set_metadata(key, value)

    def list_metadata(self, prefix: Optional[str] = None) -> Dict[str, str]:
        return self._list_metadata(prefix=prefix)

    def list_account_risk_profile_names(self) -> List[str]:
        names: List[str] = []
        entries = self._list_metadata(prefix=self.ACCOUNT_RISK_PROFILE_KEY_PREFIX)
        for key in entries:
            account_name = self._normalize_account_name_value(
                key.removeprefix(self.ACCOUNT_RISK_PROFILE_KEY_PREFIX).strip()
            )
            if account_name and account_name not in names:
                names.append(account_name)
        return names

    def _ensure_shared_risk_profile_exists(self) -> None:
        if self._get_metadata(self.SHARED_RISK_PROFILE_KEY) is not None:
            return

        conn = self._connect()
        try:
            cursor = conn.cursor()
            if self._get_metadata_from_cursor(cursor, self.SHARED_RISK_PROFILE_KEY) is not None:
                return

            default_key = self._account_risk_profile_key(DEFAULT_ACCOUNT_NAME)
            cursor.execute(
                """
                SELECT meta_key, meta_value
                FROM monitoring_metadata
                WHERE meta_key LIKE ?
                ORDER BY
                    CASE WHEN meta_key = ? THEN 0 ELSE 1 END,
                    datetime(updated_at) DESC,
                    meta_key ASC
                """,
                (f"{self.ACCOUNT_RISK_PROFILE_KEY_PREFIX}%", default_key),
            )
            rows = [dict(row) for row in cursor.fetchall()]
            if not rows:
                return

            chosen_profile: Dict = {}
            for row in rows:
                try:
                    parsed = json.loads(row.get("meta_value") or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if isinstance(parsed, dict):
                    chosen_profile = parsed
                    break

            self._set_metadata_on_cursor(
                cursor,
                self.SHARED_RISK_PROFILE_KEY,
                json.dumps(
                    self._normalize_account_risk_profile(chosen_profile),
                    ensure_ascii=False,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_shared_risk_profile(self) -> Dict[str, object]:
        self._ensure_shared_risk_profile_exists()
        raw = self._get_metadata(self.SHARED_RISK_PROFILE_KEY)
        parsed: Dict = {}
        source = "global_default"
        if raw:
            try:
                candidate = json.loads(raw)
                if isinstance(candidate, dict):
                    parsed = candidate
                    source = "shared_custom"
            except (TypeError, ValueError, json.JSONDecodeError):
                parsed = {}
        values = self._normalize_account_risk_profile(parsed)
        return {
            "source": source,
            **values,
        }

    def resolve_shared_risk_profile(self, overrides: Optional[Dict] = None) -> Dict[str, object]:
        shared_profile = self.get_shared_risk_profile()
        merged = self._normalize_account_risk_profile(
            overrides,
            defaults={
                "position_size_pct": int(shared_profile["position_size_pct"]),
                "total_position_pct": int(shared_profile["total_position_pct"]),
                "stop_loss_pct": int(shared_profile["stop_loss_pct"]),
                "take_profit_pct": int(shared_profile["take_profit_pct"]),
            },
        )
        return {
            "source": str(shared_profile["source"]),
            **merged,
        }

    def set_shared_risk_profile(self, profile: Dict) -> Dict[str, object]:
        normalized_profile = self._normalize_account_risk_profile(profile)
        self._set_metadata(
            self.SHARED_RISK_PROFILE_KEY,
            json.dumps(normalized_profile, ensure_ascii=False),
        )
        return self.get_shared_risk_profile()

    def get_account_risk_profile(self, account_name: Optional[str]) -> Dict[str, object]:
        normalized_account = self._normalize_account_name_value(account_name)
        shared_profile = self.get_shared_risk_profile()
        return {
            "account_name": normalized_account,
            "source": shared_profile["source"],
            "position_size_pct": int(shared_profile["position_size_pct"]),
            "total_position_pct": int(shared_profile["total_position_pct"]),
            "stop_loss_pct": int(shared_profile["stop_loss_pct"]),
            "take_profit_pct": int(shared_profile["take_profit_pct"]),
        }

    def resolve_account_risk_profile(self, account_name: Optional[str], overrides: Optional[Dict] = None) -> Dict[str, object]:
        account_profile = self.get_account_risk_profile(account_name)
        merged = self.resolve_shared_risk_profile(overrides)
        return {
            "account_name": str(account_profile["account_name"]),
            "source": str(merged["source"]),
            "position_size_pct": int(merged["position_size_pct"]),
            "total_position_pct": int(merged["total_position_pct"]),
            "stop_loss_pct": int(merged["stop_loss_pct"]),
            "take_profit_pct": int(merged["take_profit_pct"]),
        }

    def set_account_risk_profile(self, account_name: Optional[str], profile: Dict) -> Dict[str, object]:
        normalized_account = self._normalize_account_name_value(account_name)
        shared_profile = self.set_shared_risk_profile(profile)
        return {
            "account_name": normalized_account,
            "source": shared_profile["source"],
            "position_size_pct": int(shared_profile["position_size_pct"]),
            "total_position_pct": int(shared_profile["total_position_pct"]),
            "stop_loss_pct": int(shared_profile["stop_loss_pct"]),
            "take_profit_pct": int(shared_profile["take_profit_pct"]),
        }

    def bulk_set_interval_minutes(self, monitor_type: str, interval_minutes: int) -> int:
        normalized_interval = max(1, int(interval_minutes or 1))

        def _update() -> int:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE monitoring_items
                    SET interval_minutes = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE monitor_type = ?
                    """,
                    (normalized_interval, monitor_type),
                )
                changed = int(cursor.rowcount or 0)
                conn.commit()
                return changed
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_update)

    def _ensure_asset_binding(self, item_data: Dict) -> Dict:
        if item_data.get("asset_id") or not item_data.get("symbol"):
            return item_data
        monitor_type = item_data.get("monitor_type")
        managed_by_portfolio = bool(item_data.get("managed_by_portfolio", False))
        if monitor_type not in {"ai_task", "price_alert"}:
            return item_data
        if monitor_type == "price_alert" and not managed_by_portfolio:
            return item_data

        from asset_repository import AssetRepository, STATUS_PORTFOLIO, STATUS_RESEARCH

        account_name = self._normalize_item_account_name(
            item_data.get("account_name"),
            monitor_type=monitor_type,
        ) or DEFAULT_ACCOUNT_NAME
        item_data["account_name"] = account_name
        asset_repo = AssetRepository(self.db_path)
        asset = asset_repo.get_asset_by_symbol(item_data["symbol"], account_name)
        if asset is None:
            asset_id = asset_repo.promote_to_watchlist(
                symbol=item_data["symbol"],
                name=item_data.get("name") or item_data["symbol"],
                account_name=account_name,
                note="由监控注册表创建",
                origin_analysis_id=item_data.get("origin_analysis_id"),
                monitor_enabled=bool(item_data.get("enabled", True)),
            )
            asset = asset_repo.get_asset(asset_id)
        elif asset.get("status") == STATUS_RESEARCH:
            asset_id = asset_repo.promote_to_watchlist(
                symbol=item_data["symbol"],
                name=item_data.get("name") or asset.get("name") or item_data["symbol"],
                account_name=account_name,
                note=asset.get("note") or "由监控注册表升级为盯盘",
                origin_analysis_id=item_data.get("origin_analysis_id") or asset.get("origin_analysis_id"),
                monitor_enabled=bool(item_data.get("enabled", True)),
            )
            asset = asset_repo.get_asset(asset_id)

        if asset:
            item_data["account_name"] = asset.get("account_name") or account_name
            item_data["asset_id"] = asset.get("id")
            item_data["portfolio_stock_id"] = asset.get("id") if asset.get("status") == STATUS_PORTFOLIO else None
        return item_data

    def _find_existing_item_for_upsert(self, item_data: Dict) -> Optional[Dict]:
        monitor_type = item_data["monitor_type"]
        symbol = item_data["symbol"]
        managed = bool(item_data.get("managed_by_portfolio", False))
        account_name = self._normalize_item_account_name(
            item_data.get("account_name"),
            monitor_type=monitor_type,
            keep_none=True,
        )
        asset_id = item_data.get("asset_id")
        portfolio_stock_id = item_data.get("portfolio_stock_id")

        if asset_id is not None:
            # `asset_id + monitor_type` is the canonical uniqueness boundary.
            # Managed state, account, and symbol can legitimately drift during
            # lifecycle transitions (for example, full sell -> watchlist).
            by_asset = self.list_items(
                monitor_type=monitor_type,
                asset_id=asset_id,
            )
            if by_asset:
                return by_asset[0]

        if monitor_type == "ai_task":
            target_account = self._normalize_account_name_value(account_name)
            by_account_symbol = self.get_item_by_symbol(
                symbol,
                monitor_type="ai_task",
                account_name=target_account,
            )
            if by_account_symbol:
                return by_account_symbol
            if portfolio_stock_id is not None:
                by_position = self.get_item_by_symbol(
                    symbol,
                    monitor_type="ai_task",
                    portfolio_stock_id=portfolio_stock_id,
                )
                if by_position:
                    return by_position

        if monitor_type == "price_alert" and managed:
            if portfolio_stock_id is not None:
                by_position = self.get_item_by_symbol(
                    symbol,
                    monitor_type="price_alert",
                    managed_only=True,
                    account_name=account_name if account_name is not None else None,
                    portfolio_stock_id=portfolio_stock_id,
                )
                if by_position:
                    return by_position
                by_position_loose = self.get_item_by_symbol(
                    symbol,
                    monitor_type="price_alert",
                    managed_only=True,
                    portfolio_stock_id=portfolio_stock_id,
                )
                if by_position_loose:
                    return by_position_loose
            if account_name:
                by_account = self.get_item_by_symbol(
                    symbol,
                    monitor_type="price_alert",
                    managed_only=True,
                    account_name=account_name,
                )
                if by_account:
                    return by_account

        return None

    def _recover_from_integrity_conflict(self, item_data: Dict) -> Optional[int]:
        existing = self._find_existing_item_for_upsert(item_data)
        if not existing:
            return None
        monitor_type = item_data.get("monitor_type")
        merged_config = dict(existing.get("config") or {})
        merged_config.update(item_data.get("config") or {})
        self.update_item(
            existing["id"],
            {
                "name": item_data.get("name", existing.get("name")),
                "source": item_data.get("source", existing.get("source", "manual")),
                "enabled": item_data.get("enabled", existing.get("enabled", True)),
                "interval_minutes": item_data.get("interval_minutes", existing.get("interval_minutes", 30)),
                "trading_hours_only": item_data.get("trading_hours_only", existing.get("trading_hours_only", True)),
                "notification_enabled": item_data.get("notification_enabled", existing.get("notification_enabled", True)),
                "managed_by_portfolio": item_data.get("managed_by_portfolio", existing.get("managed_by_portfolio", False)),
                "account_name": (
                    self._normalize_account_name_value(item_data.get("account_name") or existing.get("account_name"))
                    if monitor_type == "ai_task"
                    else self._normalize_item_account_name(
                        item_data.get("account_name", existing.get("account_name")),
                        monitor_type=monitor_type,
                        keep_none=True,
                    )
                ),
                "asset_id": item_data.get("asset_id", existing.get("asset_id")),
                "portfolio_stock_id": item_data.get("portfolio_stock_id", existing.get("portfolio_stock_id")),
                "origin_analysis_id": item_data.get("origin_analysis_id", existing.get("origin_analysis_id")),
                "current_price": item_data.get("current_price", existing.get("current_price")),
                "last_checked": item_data.get("last_checked", existing.get("last_checked")),
                "last_status": item_data.get("last_status", existing.get("last_status")),
                "last_message": item_data.get("last_message", existing.get("last_message")),
                "config": merged_config,
            },
        )
        return int(existing["id"])

    def create_item(self, item_data: Dict) -> int:
        def _create() -> int:
            bound_item = self._ensure_asset_binding(dict(item_data))
            config = self._sanitize_monitor_config(bound_item.get("config") or {})
            monitor_type = bound_item.get("monitor_type")
            account_name = self._normalize_item_account_name(
                bound_item.get("account_name"),
                monitor_type=monitor_type,
                keep_none=True,
            )
            if monitor_type == "ai_task":
                account_name = self._normalize_account_name_value(account_name)
                bound_item["account_name"] = account_name
            else:
                bound_item["account_name"] = account_name
            integrity_error = None
            item_id = None
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO monitoring_items (
                        symbol, name, monitor_type, source, enabled, interval_minutes,
                        trading_hours_only, notification_enabled, managed_by_portfolio,
                        account_name, asset_id, portfolio_stock_id, origin_analysis_id,
                        current_price, last_checked, last_status, last_message, config_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        bound_item["symbol"],
                        bound_item.get("name") or bound_item["symbol"],
                        bound_item["monitor_type"],
                        bound_item.get("source", "manual"),
                        1 if bound_item.get("enabled", True) else 0,
                        self._normalize_interval_minutes(bound_item.get("interval_minutes", 30)),
                        1 if bound_item.get("trading_hours_only", True) else 0,
                        1 if bound_item.get("notification_enabled", True) else 0,
                        1 if bound_item.get("managed_by_portfolio", False) else 0,
                        account_name,
                        bound_item.get("asset_id"),
                        bound_item.get("portfolio_stock_id"),
                        bound_item.get("origin_analysis_id"),
                        bound_item.get("current_price"),
                        bound_item.get("last_checked"),
                        bound_item.get("last_status"),
                        bound_item.get("last_message"),
                        json.dumps(config, ensure_ascii=False),
                    ),
                )
                item_id = int(cursor.lastrowid)
                conn.commit()
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                integrity_error = exc
            finally:
                conn.close()

            if integrity_error is None:
                return int(item_id)

            if "UNIQUE constraint failed" not in str(integrity_error):
                raise integrity_error

            recovered_id = self._recover_from_integrity_conflict(bound_item)
            if recovered_id is not None:
                return recovered_id
            raise integrity_error

        return run_with_monitoring_write_lock(_create)

    def update_item(self, item_id: int, updates: Dict) -> bool:
        if not updates:
            return False

        fields: List[str] = []
        values: List[object] = []
        existing_item = None
        if "account_name" in updates and "monitor_type" not in updates:
            existing_item = self.get_item(item_id)
        scalar_fields = {
            "symbol",
            "name",
            "monitor_type",
            "source",
            "enabled",
            "interval_minutes",
            "trading_hours_only",
            "notification_enabled",
            "managed_by_portfolio",
            "account_name",
            "asset_id",
            "portfolio_stock_id",
            "origin_analysis_id",
            "current_price",
            "last_checked",
            "last_status",
            "last_message",
        }

        for key in scalar_fields:
            if key not in updates:
                continue
            value = updates[key]
            if key in {"enabled", "trading_hours_only", "notification_enabled", "managed_by_portfolio"}:
                value = 1 if value else 0
            if key == "interval_minutes":
                value = self._normalize_interval_minutes(value)
            if key == "account_name":
                value = self._normalize_item_account_name(
                    value,
                    monitor_type=updates.get("monitor_type") or (existing_item or {}).get("monitor_type"),
                    keep_none=True,
                )
            fields.append(f"{key} = ?")
            values.append(value)

        if "config" in updates:
            fields.append("config_json = ?")
            values.append(
                json.dumps(self._sanitize_monitor_config(updates["config"] or {}), ensure_ascii=False)
            )

        if not fields:
            return False

        fields.append("updated_at = CURRENT_TIMESTAMP")
        values.append(item_id)
        def _update() -> bool:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    f"UPDATE monitoring_items SET {', '.join(fields)} WHERE id = ?",
                    tuple(values),
                )
                changed = cursor.rowcount > 0
                conn.commit()
                return changed
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_update)

    def get_item(self, item_id: int) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM monitoring_items WHERE id = ?", (item_id,))
        row = cursor.fetchone()
        conn.close()
        return self._row_to_item(row) if row else None

    def get_item_by_symbol(
        self,
        symbol: str,
        monitor_type: Optional[str] = None,
        managed_only: Optional[bool] = None,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> Optional[Dict]:
        items = self.list_items(
            monitor_type=monitor_type,
            symbol=symbol,
            managed_by_portfolio=managed_only,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        return items[0] if items else None

    def list_items(
        self,
        monitor_type: Optional[str] = None,
        source: Optional[str] = None,
        managed_by_portfolio: Optional[bool] = None,
        enabled_only: bool = False,
        symbol: Optional[str] = None,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()

        clauses = []
        params: List[object] = []
        if monitor_type:
            clauses.append("monitor_type = ?")
            params.append(monitor_type)
        if source:
            clauses.append("source = ?")
            params.append(source)
        if managed_by_portfolio is not None:
            clauses.append("managed_by_portfolio = ?")
            params.append(1 if managed_by_portfolio else 0)
        if enabled_only:
            clauses.append("enabled = 1")
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if account_name is not None:
            clauses.append("account_name = ?")
            params.append(
                self._normalize_item_account_name(
                    account_name,
                    monitor_type=monitor_type,
                    keep_none=True,
                )
            )
        if asset_id is not None:
            clauses.append("asset_id = ?")
            params.append(asset_id)
        if portfolio_stock_id is not None:
            clauses.append("portfolio_stock_id = ?")
            params.append(portfolio_stock_id)

        sql = "SELECT * FROM monitoring_items"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY datetime(created_at) DESC, id DESC"

        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_item(row) for row in rows]

    def upsert_item(self, item_data: Dict) -> int:
        item_data = self._ensure_asset_binding(dict(item_data))
        monitor_type = item_data["monitor_type"]
        managed = bool(item_data.get("managed_by_portfolio", False))
        account_name = self._normalize_item_account_name(
            item_data.get("account_name"),
            monitor_type=monitor_type,
            keep_none=True,
        )
        item_data["account_name"] = account_name
        existing = self._find_existing_item_for_upsert(item_data)

        if not existing:
            return self.create_item(item_data)

        merged_config = self._sanitize_monitor_config(existing.get("config") or {})
        merged_config.update(self._sanitize_monitor_config(item_data.get("config") or {}))
        merged_config = self._sanitize_monitor_config(merged_config)
        updates = {
            "name": item_data.get("name", existing["name"]),
            "source": item_data.get("source", existing.get("source", "manual")),
            "enabled": item_data.get("enabled", existing["enabled"]),
            "interval_minutes": item_data.get("interval_minutes", existing["interval_minutes"]),
            "trading_hours_only": item_data.get("trading_hours_only", existing["trading_hours_only"]),
            "notification_enabled": item_data.get("notification_enabled", existing["notification_enabled"]),
            "managed_by_portfolio": managed,
            "account_name": (
                self._normalize_account_name_value(account_name)
                if monitor_type == "ai_task"
                else self._normalize_item_account_name(
                    item_data.get("account_name", existing.get("account_name")),
                    monitor_type=monitor_type,
                    keep_none=True,
                )
            ),
            "asset_id": item_data.get("asset_id", existing.get("asset_id")),
            "portfolio_stock_id": item_data.get("portfolio_stock_id", existing.get("portfolio_stock_id")),
            "origin_analysis_id": item_data.get("origin_analysis_id", existing.get("origin_analysis_id")),
            "current_price": item_data.get("current_price", existing.get("current_price")),
            "last_checked": item_data.get("last_checked", existing.get("last_checked")),
            "last_status": item_data.get("last_status", existing.get("last_status")),
            "last_message": item_data.get("last_message", existing.get("last_message")),
            "config": merged_config,
        }
        self.update_item(existing["id"], updates)
        return int(existing["id"])

    def delete_item(self, item_id: int) -> bool:
        def _delete() -> bool:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM monitoring_price_history WHERE monitoring_item_id = ?", (item_id,))
                cursor.execute("DELETE FROM monitoring_events WHERE monitoring_item_id = ?", (item_id,))
                cursor.execute("DELETE FROM monitoring_items WHERE id = ?", (item_id,))
                deleted = cursor.rowcount > 0
                conn.commit()
                return deleted
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_delete)

    def delete_by_symbol(
        self,
        symbol: str,
        monitor_type: Optional[str] = None,
        managed_only: bool = False,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> bool:
        items = self.list_items(
            monitor_type=monitor_type,
            symbol=symbol,
            managed_by_portfolio=True if managed_only else None,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        deleted = False
        for item in items:
            deleted = self.delete_item(item["id"]) or deleted
        return deleted

    def set_notification_enabled(self, item_id: int, enabled: bool) -> bool:
        return self.update_item(item_id, {"notification_enabled": enabled})

    def update_runtime(
        self,
        item_id: int,
        *,
        current_price: Optional[float] = None,
        last_checked: Optional[str] = None,
        last_status: Optional[str] = None,
        last_message: Optional[str] = None,
    ) -> bool:
        def _update_runtime() -> bool:
            item = self.get_item(item_id)
            if not item:
                return False

            effective_checked = last_checked or local_now_str()
            updated = self.update_item(
                item_id,
                {
                    "current_price": current_price if current_price is not None else item.get("current_price"),
                    "last_checked": effective_checked,
                    "last_status": last_status if last_status is not None else item.get("last_status"),
                    "last_message": last_message if last_message is not None else item.get("last_message"),
                },
            )
            if updated and current_price is not None:
                conn = self._connect()
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        INSERT INTO monitoring_price_history (monitoring_item_id, price)
                        VALUES (?, ?)
                        """,
                        (item_id, current_price),
                    )
                    conn.commit()
                finally:
                    conn.close()
            return updated

        return run_with_monitoring_write_lock(_update_runtime)

    def get_due_items(self, now: Optional[datetime] = None, service_running: bool = True) -> List[Dict]:
        if not service_running:
            return []

        current_time = now or datetime.now()
        due_items: List[Dict] = []
        for item in self.list_items(enabled_only=True):
            last_checked_raw = item.get("last_checked")
            if not last_checked_raw:
                due_items.append(item)
                continue
            try:
                last_checked = datetime.fromisoformat(str(last_checked_raw))
            except ValueError:
                due_items.append(item)
                continue
            next_due = last_checked + timedelta(minutes=self._normalize_interval_minutes(item.get("interval_minutes")))
            if current_time >= next_due:
                due_items.append(item)
        return due_items

    def record_event(
        self,
        *,
        item_id: Optional[int],
        event_type: str,
        message: str,
        notification_pending: bool = False,
        sent: bool = False,
        details: Optional[Dict] = None,
        created_at: Optional[str] = None,
        suppress_if_latest_same_type: bool = False,
    ) -> int:
        def _record() -> int:
            item = self.get_item(item_id) if item_id else None
            conn = self._connect()
            try:
                cursor = conn.cursor()
                if suppress_if_latest_same_type and item_id:
                    cursor.execute(
                        """
                        SELECT id, event_type
                        FROM monitoring_events
                        WHERE monitoring_item_id = ?
                        ORDER BY datetime(created_at) DESC, id DESC
                        LIMIT 1
                        """,
                        (item_id,),
                    )
                    latest = cursor.fetchone()
                    if latest and latest["event_type"] == event_type:
                        return int(latest["id"])
                cursor.execute(
                    """
                    INSERT INTO monitoring_events (
                        monitoring_item_id, symbol, name, monitor_type, event_type,
                        message, details_json, notification_pending, sent, is_read, read_at, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item_id,
                        item.get("symbol") if item else "",
                        item.get("name") if item else None,
                        item.get("monitor_type") if item else None,
                        event_type,
                        message,
                        json.dumps(details or {}, ensure_ascii=False),
                        1 if notification_pending else 0,
                        1 if sent else 0,
                        0,
                        None,
                        created_at or local_now_str(),
                    ),
                )
                event_id = int(cursor.lastrowid)
                conn.commit()
                return event_id
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_record)

    def get_pending_notifications(self) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, monitoring_item_id, symbol, name, event_type, message, details_json, created_at
            FROM monitoring_events
            WHERE notification_pending = 1 AND sent = 0
            ORDER BY datetime(created_at) ASC, id ASC
            """
        )
        rows = cursor.fetchall()
        conn.close()
        notifications: List[Dict] = []
        for row in rows:
            details = self._safe_json_loads(row["details_json"], {})
            payload = details if isinstance(details, dict) else {}
            payload.update(
                {
                    "id": row["id"],
                    "stock_id": row["monitoring_item_id"],
                    "symbol": row["symbol"],
                    "name": row["name"] or row["symbol"],
                    "event_type": row["event_type"],
                    "type": row["event_type"],
                    "message": row["message"],
                    "triggered_at": row["created_at"],
                }
            )
            notifications.append(payload)
        return notifications

    def get_recent_events(self, limit: int = 20) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM monitoring_events
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_all_recent_notifications(self, limit: int = 10) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                e.id,
                e.monitoring_item_id,
                e.symbol,
                e.name,
                e.monitor_type,
                e.event_type,
                e.message,
                e.details_json,
                e.created_at,
                e.sent,
                e.is_read,
                e.read_at,
                COALESCE(mi.account_name, '') AS account_name
            FROM monitoring_events e
            LEFT JOIN monitoring_items mi
                ON mi.id = e.monitoring_item_id
            WHERE e.notification_pending = 1
            ORDER BY datetime(e.created_at) DESC, e.id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        conn.close()
        notifications: List[Dict] = []
        for row in rows:
            details = self._safe_json_loads(row["details_json"], {})
            payload = details if isinstance(details, dict) else {}
            payload.update(
                {
                    "id": row["id"],
                    "stock_id": row["monitoring_item_id"],
                    "symbol": row["symbol"],
                    "name": row["name"] or row["symbol"],
                    "monitor_type": row["monitor_type"],
                    "account_name": row["account_name"] or DEFAULT_ACCOUNT_NAME,
                    "event_type": row["event_type"],
                    "type": row["event_type"],
                    "message": row["message"],
                    "triggered_at": row["created_at"],
                    "sent": bool(row["sent"]),
                    "is_read": bool(row["is_read"]),
                    "read_at": row["read_at"],
                }
            )
            notifications.append(payload)
        return notifications

    def mark_notification_sent(self, event_id: int) -> None:
        def _mark() -> None:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute("UPDATE monitoring_events SET sent = 1 WHERE id = ?", (event_id,))
                conn.commit()
            finally:
                conn.close()

        run_with_monitoring_write_lock(_mark)

    def mark_notification_read(self, event_id: int) -> None:
        def _mark_read() -> None:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE monitoring_events
                    SET is_read = 1,
                        read_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (event_id,),
                )
                conn.commit()
            finally:
                conn.close()

        run_with_monitoring_write_lock(_mark_read)

    def ignore_notification(self, event_id: int) -> None:
        def _ignore() -> None:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE monitoring_events
                    SET notification_pending = 0,
                        is_read = 1,
                        read_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (event_id,),
                )
                conn.commit()
            finally:
                conn.close()

        run_with_monitoring_write_lock(_ignore)

    def mark_all_notifications_sent(self) -> int:
        def _mark_all() -> int:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute("UPDATE monitoring_events SET sent = 1 WHERE notification_pending = 1 AND sent = 0")
                changed = cursor.rowcount
                conn.commit()
                return changed
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_mark_all)

    def clear_all_notifications(self) -> int:
        def _clear() -> int:
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM monitoring_events WHERE notification_pending = 1")
                changed = cursor.rowcount
                conn.commit()
                return changed
            finally:
                conn.close()

        return run_with_monitoring_write_lock(_clear)

    def has_latest_notification_type(self, item_id: int, event_type: str) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT event_type
            FROM monitoring_events
            WHERE monitoring_item_id = ?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT 1
            """,
            (item_id,),
        )
        latest = cursor.fetchone()
        conn.close()
        return bool(latest and latest["event_type"] == event_type)

    def has_recent_notification(self, item_id: int, event_type: str, minutes: int = 60) -> bool:
        return self.has_latest_notification_type(item_id, event_type)

    @staticmethod
    def _build_smart_migration_key(db_path: str) -> str:
        return f"migrated_smart::{os.path.abspath(db_path)}"

    @staticmethod
    def _build_stock_migration_key(db_path: str) -> str:
        return f"migrated_stock::{os.path.abspath(db_path)}"

    def record_migration_conflict(
        self,
        *,
        source_db: str,
        source_table: str,
        source_key: str,
        symbol: Optional[str],
        conflict_type: str,
        payload: Optional[Dict] = None,
    ) -> int:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO migration_conflicts (
                    source_db, source_table, source_key, symbol, conflict_type, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_db,
                    source_table,
                    source_key,
                    symbol,
                    conflict_type,
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )
            conflict_id = int(cursor.lastrowid)
            conn.commit()
            return conflict_id
        finally:
            conn.close()

    def _resolve_portfolio_binding(self, symbol: str) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        try:
            if self._table_exists(cursor.connection, "assets"):
                cursor.execute(
                    """
                    SELECT id, account_name, symbol
                    FROM assets
                    WHERE symbol = ?
                      AND status = 'portfolio'
                      AND deleted_at IS NULL
                    ORDER BY id ASC
                    """,
                    (symbol,),
                )
                rows = cursor.fetchall()
            else:
                cursor.execute(
                    """
                    SELECT id, account_name, code, position_status
                    FROM portfolio_stocks
                    WHERE code = ?
                      AND COALESCE(position_status, 'active') = 'active'
                    ORDER BY id ASC
                    """,
                    (symbol,),
                )
                rows = cursor.fetchall()
        except sqlite3.OperationalError:
            rows = []
        finally:
            conn.close()
        if len(rows) == 1:
            return {
                "asset_id": rows[0]["id"],
                "portfolio_stock_id": rows[0]["id"],
                "account_name": rows[0]["account_name"] or DEFAULT_ACCOUNT_NAME,
            }
        return None

    def migrate_legacy_smart_db(self, legacy_db_path: str) -> int:
        if not legacy_db_path or not os.path.exists(legacy_db_path):
            return 0
        if os.path.abspath(legacy_db_path) == os.path.abspath(self.db_path):
            return 0

        key = self._build_smart_migration_key(legacy_db_path)
        if self._get_metadata(key):
            return 0

        conn = sqlite3.connect(legacy_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='monitor_tasks'"
        )
        if not cursor.fetchone():
            conn.close()
            self._set_metadata(key, "missing")
            return 0

        cursor.execute("SELECT * FROM monitor_tasks ORDER BY id ASC")
        migrated = 0
        for row in cursor.fetchall():
            task = dict(row)
            binding = self._resolve_portfolio_binding(task["stock_code"])
            account_name = DEFAULT_ACCOUNT_NAME
            asset_id = None
            portfolio_stock_id = None
            enabled = bool(task.get("enabled", 1))
            source = "portfolio" if task.get("managed_by_portfolio") else "manual"

            if task.get("managed_by_portfolio") or task.get("has_position"):
                if binding:
                    account_name = binding["account_name"]
                    asset_id = binding["asset_id"]
                    portfolio_stock_id = binding["portfolio_stock_id"]
                else:
                    enabled = False
                    source = "legacy_conflict"
                    self.record_migration_conflict(
                        source_db=os.path.abspath(legacy_db_path),
                        source_table="monitor_tasks",
                        source_key=str(task.get("id")),
                        symbol=task.get("stock_code"),
                        conflict_type="portfolio_binding_ambiguous",
                        payload=task,
                    )

            account_risk = self.resolve_account_risk_profile(account_name, task)
            config = {
                "task_name": task.get("task_name"),
                "position_size_pct": account_risk["position_size_pct"],
                "total_position_pct": account_risk["total_position_pct"],
                "stop_loss_pct": account_risk["stop_loss_pct"],
                "take_profit_pct": account_risk["take_profit_pct"],
                "notify_email": task.get("notify_email"),
                "notify_webhook": task.get("notify_webhook"),
                "has_position": bool(task.get("has_position", 0)),
                "position_cost": task.get("position_cost", 0),
                "position_quantity": task.get("position_quantity", 0),
                "position_date": task.get("position_date"),
            }
            self.upsert_item(
                {
                    "symbol": task["stock_code"],
                    "name": task.get("stock_name") or task.get("task_name") or task["stock_code"],
                    "monitor_type": "ai_task",
                    "source": source,
                    "enabled": enabled,
                    "interval_minutes": max(1, int(math.ceil((task.get("check_interval") or 60) / 60))),
                    "trading_hours_only": bool(task.get("trading_hours_only", 1)),
                    "notification_enabled": True,
                    "managed_by_portfolio": bool(task.get("managed_by_portfolio", 0)),
                    "account_name": account_name,
                    "asset_id": asset_id,
                    "portfolio_stock_id": portfolio_stock_id,
                    "config": config,
                    "last_checked": task.get("updated_at"),
                }
            )
            migrated += 1

        conn.close()
        self._set_metadata(key, str(migrated))
        return migrated

    def migrate_legacy_stock_db(self, legacy_db_path: str) -> int:
        if not legacy_db_path or not os.path.exists(legacy_db_path):
            return 0
        if os.path.abspath(legacy_db_path) == os.path.abspath(self.db_path):
            return 0

        key = self._build_stock_migration_key(legacy_db_path)
        if self._get_metadata(key):
            return 0

        conn = sqlite3.connect(legacy_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='monitored_stocks'"
        )
        if not cursor.fetchone():
            conn.close()
            self._set_metadata(key, "missing")
            return 0

        cursor.execute("SELECT * FROM monitored_stocks ORDER BY id ASC")
        migrated = 0
        for row in cursor.fetchall():
            stock = dict(row)
            binding = self._resolve_portfolio_binding(stock["symbol"]) if stock.get("managed_by_portfolio") else None
            account_name = binding["account_name"] if binding else None
            asset_id = binding["asset_id"] if binding else None
            portfolio_stock_id = binding["portfolio_stock_id"] if binding else None
            enabled = True
            source = "portfolio" if stock.get("managed_by_portfolio") else "manual"
            if stock.get("managed_by_portfolio") and not binding:
                enabled = False
                source = "legacy_conflict"
                self.record_migration_conflict(
                    source_db=os.path.abspath(legacy_db_path),
                    source_table="monitored_stocks",
                    source_key=str(stock.get("id")),
                    symbol=stock.get("symbol"),
                    conflict_type="portfolio_binding_ambiguous",
                    payload=stock,
                )
            config = {
                "rating": stock.get("rating", "持有"),
                "entry_range": self._safe_json_loads(stock.get("entry_range"), {}),
                "take_profit": stock.get("take_profit"),
                "stop_loss": stock.get("stop_loss"),
            }
            item_data = {
                "symbol": stock["symbol"],
                "name": stock.get("name") or stock["symbol"],
                "monitor_type": "price_alert",
                "source": source,
                "enabled": enabled,
                "interval_minutes": self._normalize_interval_minutes(stock.get("check_interval", 30)),
                "trading_hours_only": bool(stock.get("trading_hours_only", 1)),
                "notification_enabled": bool(stock.get("notification_enabled", 1)),
                "managed_by_portfolio": bool(stock.get("managed_by_portfolio", 0)),
                "account_name": account_name,
                "asset_id": asset_id,
                "portfolio_stock_id": portfolio_stock_id,
                "current_price": stock.get("current_price"),
                "last_checked": stock.get("last_checked"),
                "config": config,
            }
            if item_data["managed_by_portfolio"]:
                self.upsert_item(item_data)
            else:
                self.create_item(item_data)
            migrated += 1

        conn.close()
        self._set_metadata(key, str(migrated))
        return migrated
