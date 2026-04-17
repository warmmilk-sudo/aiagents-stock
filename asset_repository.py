from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from investment_db_utils import (
    DEFAULT_ACCOUNT_NAME,
    connect_sqlite,
    get_metadata,
    normalize_account_name,
    resolve_investment_db_path,
    set_metadata,
)


STATUS_RESEARCH = "research"
STATUS_FOCUS = "focus"
STATUS_HOLDING = "holding"
STATUS_WATCHLIST = STATUS_FOCUS
STATUS_PORTFOLIO = STATUS_HOLDING
ASSET_STATUSES = {STATUS_RESEARCH, STATUS_FOCUS, STATUS_HOLDING}
STATUS_PRIORITY = {
    STATUS_RESEARCH: 0,
    STATUS_FOCUS: 1,
    STATUS_HOLDING: 2,
}


class AssetRepository:
    """Canonical storage for the investment lifecycle domain."""

    ACCOUNT_NORMALIZATION_KEY = "assets_account_normalization_v1"
    LIFECYCLE_SCHEMA_KEY = "assets_lifecycle_schema_v2"
    LIFECYCLE_FOREIGN_KEY_REPAIR_KEY = "assets_lifecycle_foreign_key_repair_v1"
    LIFECYCLE_DATA_BACKFILL_KEY = "assets_lifecycle_data_backfill_v2"

    def __init__(self, db_path: str = "investment.db"):
        self.seed_db_path = db_path
        self.db_path = resolve_investment_db_path(db_path)
        self._init_database()
        self._migrate_assets_schema_to_lifecycle()
        self._repair_lifecycle_foreign_keys()
        self._migrate_existing_canonical_tables()
        self._normalize_account_names_if_needed()
        self._cleanup_to_single_account_mode()
        self.backfill_lifecycle_data_from_legacy()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _init_database(self) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL DEFAULT 'zfy',
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK(status IN ('research', 'focus', 'holding')),
                cost_price REAL,
                quantity INTEGER,
                note TEXT,
                monitor_enabled INTEGER NOT NULL DEFAULT 1,
                origin_analysis_id INTEGER,
                manual_pin INTEGER NOT NULL DEFAULT 0,
                pool_reason TEXT,
                pool_reason_source TEXT,
                last_funnel_score REAL,
                last_funnel_snapshot_json TEXT,
                last_exit_reason TEXT,
                last_exit_at TEXT,
                sector_tags_json TEXT,
                last_trade_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_active_symbol
            ON assets(symbol)
            WHERE deleted_at IS NULL
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_assets_status_updated
            ON assets(status, datetime(updated_at) DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS asset_trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                trade_date TEXT NOT NULL,
                trade_type TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                note TEXT,
                trade_source TEXT DEFAULT 'manual',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_asset_trade_asset_date
            ON asset_trade_history(asset_id, trade_date DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS asset_action_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                origin_decision_id INTEGER,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'accepted', 'rejected', 'expired')),
                payload_json TEXT NOT NULL DEFAULT '{}',
                resolution_note TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_asset_action_asset_status
            ON asset_action_queue(asset_id, status, datetime(created_at) DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS asset_position_cycles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open'
                    CHECK(status IN ('open', 'closed')),
                opened_at TEXT NOT NULL,
                opened_trade_date TEXT,
                opened_trade_id INTEGER,
                closed_at TEXT,
                closed_trade_date TEXT,
                closed_trade_id INTEGER,
                baseline_source TEXT,
                baseline_analysis_id INTEGER,
                baseline_decision_id INTEGER,
                swing_type TEXT,
                swing_type_reason TEXT,
                baseline_snapshot_json TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_asset_position_cycles_asset_status
            ON asset_position_cycles(asset_id, status, datetime(opened_at) DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_position_cycles_one_open
            ON asset_position_cycles(asset_id)
            WHERE status = 'open'
            """
        )
        conn.commit()
        conn.close()

    def _migrate_assets_schema_to_lifecycle(self) -> None:
        conn = self._connect()
        try:
            if get_metadata(conn, self.LIFECYCLE_SCHEMA_KEY):
                return
            cursor = conn.cursor()
            cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'assets'"
            )
            row = cursor.fetchone()
            current_sql = str(row["sql"] or "") if row else ""
            schema_outdated = any(
                token in current_sql.lower()
                for token in ("watchlist", "portfolio")
            ) or "manual_pin" not in current_sql
            if not schema_outdated:
                set_metadata(conn, self.LIFECYCLE_SCHEMA_KEY, datetime.now().isoformat())
                conn.commit()
                return

            cursor.execute("PRAGMA foreign_keys = OFF")
            cursor.execute("ALTER TABLE assets RENAME TO assets_lifecycle_legacy")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_name TEXT NOT NULL DEFAULT 'zfy',
                    symbol TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK(status IN ('research', 'focus', 'holding')),
                    cost_price REAL,
                    quantity INTEGER,
                    note TEXT,
                    monitor_enabled INTEGER NOT NULL DEFAULT 1,
                    origin_analysis_id INTEGER,
                    manual_pin INTEGER NOT NULL DEFAULT 0,
                    pool_reason TEXT,
                    pool_reason_source TEXT,
                    last_funnel_score REAL,
                    last_funnel_snapshot_json TEXT,
                    last_exit_reason TEXT,
                    last_exit_at TEXT,
                    sector_tags_json TEXT,
                    last_trade_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    deleted_at TEXT
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO assets (
                    id, account_name, symbol, name, status, cost_price, quantity, note,
                    monitor_enabled, origin_analysis_id, manual_pin, pool_reason,
                    pool_reason_source, last_funnel_score, last_funnel_snapshot_json,
                    last_exit_reason, last_exit_at, sector_tags_json, last_trade_at,
                    created_at, updated_at, deleted_at
                )
                SELECT
                    id,
                    COALESCE(NULLIF(TRIM(account_name), ''), ?),
                    symbol,
                    name,
                    CASE
                        WHEN status = 'watchlist' THEN 'focus'
                        WHEN status = 'portfolio' THEN 'holding'
                        ELSE 'research'
                    END,
                    cost_price,
                    quantity,
                    note,
                    COALESCE(monitor_enabled, 1),
                    origin_analysis_id,
                    0,
                    note,
                    CASE
                        WHEN status = 'watchlist' THEN 'legacy_watchlist'
                        WHEN status = 'portfolio' THEN 'legacy_portfolio'
                        ELSE 'legacy_research'
                    END,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    last_trade_at,
                    COALESCE(created_at, CURRENT_TIMESTAMP),
                    COALESCE(updated_at, CURRENT_TIMESTAMP),
                    deleted_at
                FROM assets_lifecycle_legacy
                """,
                (DEFAULT_ACCOUNT_NAME,),
            )
            cursor.execute("DROP TABLE assets_lifecycle_legacy")
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_active_symbol
                ON assets(symbol)
                WHERE deleted_at IS NULL
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_assets_status_updated
                ON assets(status, datetime(updated_at) DESC, id DESC)
                """
            )
            cursor.execute("PRAGMA foreign_keys = ON")
            set_metadata(conn, self.LIFECYCLE_SCHEMA_KEY, datetime.now().isoformat())
            conn.commit()
        finally:
            conn.close()

    def _repair_asset_child_table_foreign_key(
        self,
        cursor: sqlite3.Cursor,
        *,
        table_name: str,
        create_sql: str,
        copy_columns: tuple[str, ...],
    ) -> bool:
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
        row = cursor.fetchone()
        current_sql = str(row["sql"] or "") if row else ""
        if "assets_lifecycle_legacy" not in current_sql:
            return False

        backup_table = f"{table_name}_foreign_key_legacy"
        cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
        cursor.execute(f"ALTER TABLE {table_name} RENAME TO {backup_table}")
        cursor.execute(create_sql)
        columns_sql = ", ".join(copy_columns)
        cursor.execute(
            f"""
            INSERT INTO {table_name} ({columns_sql})
            SELECT {columns_sql}
            FROM {backup_table}
            """
        )
        cursor.execute(f"DROP TABLE {backup_table}")
        return True

    def _repair_lifecycle_foreign_keys(self) -> None:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = OFF")
            repaired = 0
            if self._repair_asset_child_table_foreign_key(
                cursor,
                table_name="asset_trade_history",
                create_sql="""
                    CREATE TABLE asset_trade_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        asset_id INTEGER NOT NULL,
                        trade_date TEXT NOT NULL,
                        trade_type TEXT NOT NULL,
                        price REAL NOT NULL,
                        quantity INTEGER NOT NULL,
                        note TEXT,
                        trade_source TEXT DEFAULT 'manual',
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (asset_id) REFERENCES assets(id)
                    )
                """,
                copy_columns=(
                    "id",
                    "asset_id",
                    "trade_date",
                    "trade_type",
                    "price",
                    "quantity",
                    "note",
                    "trade_source",
                    "created_at",
                ),
            ):
                repaired += 1
            if self._repair_asset_child_table_foreign_key(
                cursor,
                table_name="asset_action_queue",
                create_sql="""
                    CREATE TABLE asset_action_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        asset_id INTEGER NOT NULL,
                        action_type TEXT NOT NULL,
                        origin_decision_id INTEGER,
                        status TEXT NOT NULL DEFAULT 'pending'
                            CHECK(status IN ('pending', 'accepted', 'rejected', 'expired')),
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        resolution_note TEXT,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (asset_id) REFERENCES assets(id)
                    )
                """,
                copy_columns=(
                    "id",
                    "asset_id",
                    "action_type",
                    "origin_decision_id",
                    "status",
                    "payload_json",
                    "resolution_note",
                    "created_at",
                    "updated_at",
                ),
            ):
                repaired += 1
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_asset_trade_asset_date
                ON asset_trade_history(asset_id, trade_date DESC, id DESC)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_asset_action_asset_status
                ON asset_action_queue(asset_id, status, datetime(created_at) DESC, id DESC)
                """
            )
            set_metadata(
                conn,
                self.LIFECYCLE_FOREIGN_KEY_REPAIR_KEY,
                json.dumps(
                    {
                        "repaired_tables": repaired,
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    ensure_ascii=False,
                ),
            )
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
        finally:
            conn.close()

    def _cleanup_to_single_account_mode(self) -> None:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            for table_name in (
                "assets",
                "analysis_records",
                "ai_decisions",
                "monitoring_items",
                "portfolio_daily_snapshots",
                "portfolio_stocks",
            ):
                if not self._table_exists(conn, table_name):
                    continue
                columns = self._table_columns(conn, table_name)
                if "account_name" not in columns:
                    continue
                cursor.execute(
                    f"""
                    UPDATE {table_name}
                    SET account_name = ?
                    WHERE COALESCE(TRIM(account_name), '') != ?
                    """,
                    (DEFAULT_ACCOUNT_NAME, DEFAULT_ACCOUNT_NAME),
                )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _safe_json_loads(raw_value: Any, default: Any):
        if raw_value in (None, ""):
            return default
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            return json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            return default

    @staticmethod
    def _bool_to_int(value: Any) -> int:
        return 1 if bool(value) else 0

    @staticmethod
    def _normalize_status(status: Optional[str], *, fallback: str = STATUS_RESEARCH) -> str:
        normalized = str(status or "").strip().lower()
        alias_map = {
            "watchlist": STATUS_FOCUS,
            "portfolio": STATUS_HOLDING,
            "focus": STATUS_FOCUS,
            "holding": STATUS_HOLDING,
        }
        normalized = alias_map.get(normalized, normalized)
        if normalized not in ASSET_STATUSES:
            return fallback
        return normalized

    @staticmethod
    def _normalize_account_name(account_name: Optional[str]) -> str:
        return str(normalize_account_name(account_name))

    @staticmethod
    def _normalize_symbol(symbol: Optional[str]) -> str:
        return str(symbol or "").strip().upper()

    @staticmethod
    def _normalize_trade_type(trade_type: Any) -> str:
        normalized = str(trade_type or "").strip().lower()
        if normalized in {"buy", "加仓", "买入", "建仓"}:
            return "buy"
        if normalized in {"sell", "减仓", "卖出"}:
            return "sell"
        if normalized in {"clear", "liquidate", "清仓", "清仓并降级"}:
            return "clear"
        return ""

    @staticmethod
    def _normalize_trade_date_text(value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("交易日期不能为空")

        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass

        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"无法识别交易日期: {raw}") from exc

    @staticmethod
    def _normalize_flat_status(status: Any) -> str:
        normalized = str(status or "").strip().lower()
        normalized = {"watchlist": STATUS_FOCUS, "focus": STATUS_FOCUS}.get(normalized, normalized)
        return normalized if normalized in {STATUS_FOCUS, STATUS_RESEARCH} else STATUS_FOCUS

    @staticmethod
    def _parse_sortable_timestamp(raw_value: Optional[object]) -> datetime:
        text = str(raw_value or "").strip()
        if not text:
            return datetime.min
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return datetime.min

    def _row_to_asset(self, row: sqlite3.Row) -> Dict:
        asset = dict(row)
        asset["monitor_enabled"] = bool(asset.get("monitor_enabled", 1))
        asset["manual_pin"] = bool(asset.get("manual_pin", 0))
        asset["sector_tags"] = self._safe_json_loads(asset.get("sector_tags_json"), [])
        asset["last_funnel_snapshot"] = self._safe_json_loads(asset.get("last_funnel_snapshot_json"), {})
        asset["code"] = asset.get("symbol")
        asset["auto_monitor"] = bool(asset.get("monitor_enabled", 1))
        asset["position_status"] = "active" if asset.get("status") == STATUS_HOLDING else asset.get("status")
        return asset

    @staticmethod
    def _row_to_trade(row: sqlite3.Row) -> Dict:
        return dict(row)

    def _row_to_action(self, row: sqlite3.Row) -> Dict:
        action = dict(row)
        action["payload"] = self._safe_json_loads(action.pop("payload_json", None), {})
        return action

    def _row_to_position_cycle(self, row: sqlite3.Row) -> Dict:
        cycle = dict(row)
        cycle["baseline_snapshot"] = self._safe_json_loads(cycle.pop("baseline_snapshot_json", None), {})
        return cycle

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def _table_columns(self, conn: sqlite3.Connection, table_name: str) -> set[str]:
        if not self._table_exists(conn, table_name):
            return set()
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return {str(row["name"]) for row in cursor.fetchall()}

    def _asset_merge_priority(self, asset: Dict) -> tuple:
        return (
            STATUS_PRIORITY[self._normalize_status(asset.get("status"))],
            1 if bool(asset.get("monitor_enabled", 1)) else 0,
            self._parse_sortable_timestamp(asset.get("updated_at")),
            self._parse_sortable_timestamp(asset.get("created_at")),
            int(asset.get("id") or 0),
        )

    def _merge_asset_rows(self, rows: List[Dict], normalized_account_name: str) -> Dict[str, Any]:
        ordered_rows = sorted(rows, key=self._asset_merge_priority, reverse=True)
        portfolio_row = next(
            (
                row for row in ordered_rows
                if self._normalize_status(row.get("status")) == STATUS_PORTFOLIO
            ),
            ordered_rows[0],
        )
        merged_status = self._normalize_status(ordered_rows[0].get("status"))
        merged_name = next(
            (str(row.get("name") or "").strip() for row in ordered_rows if str(row.get("name") or "").strip()),
            self._normalize_symbol(ordered_rows[0].get("symbol")),
        )
        merged_note = next(
            (row.get("note") for row in ordered_rows if row.get("note") not in (None, "")),
            None,
        )
        merged_origin_analysis_id = next(
            (
                row.get("origin_analysis_id")
                for row in ordered_rows
                if row.get("origin_analysis_id") not in (None, "")
            ),
            None,
        )
        merged_last_trade_at = next(
            (
                str(row.get("last_trade_at") or "").strip()
                for row in ordered_rows
                if str(row.get("last_trade_at") or "").strip()
            ),
            None,
        )
        return {
            "account_name": normalized_account_name,
            "symbol": self._normalize_symbol(ordered_rows[0].get("symbol")),
            "name": merged_name,
            "status": merged_status,
            "cost_price": portfolio_row.get("cost_price") if merged_status == STATUS_HOLDING else None,
            "quantity": portfolio_row.get("quantity") if merged_status == STATUS_HOLDING else None,
            "note": merged_note,
            "monitor_enabled": self._bool_to_int(any(bool(row.get("monitor_enabled", 1)) for row in ordered_rows)),
            "origin_analysis_id": merged_origin_analysis_id,
            "last_trade_at": merged_last_trade_at,
        }

    def _rewire_table_reference(
        self,
        cursor: sqlite3.Cursor,
        table_name: str,
        column_name: str,
        source_asset_id: int,
        target_asset_id: int,
    ) -> int:
        if not self._table_exists(cursor.connection, table_name):
            return 0
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = {str(row[1]) for row in cursor.fetchall()}
        if column_name not in columns:
            return 0
        cursor.execute(
            f"UPDATE {table_name} SET {column_name} = ? WHERE {column_name} = ?",
            (target_asset_id, source_asset_id),
        )
        return int(cursor.rowcount or 0)

    def _rewire_monitoring_items(
        self,
        cursor: sqlite3.Cursor,
        source_asset_id: int,
        target_asset_id: int,
    ) -> int:
        if not self._table_exists(cursor.connection, "monitoring_items"):
            return 0

        changed = 0
        cursor.execute(
            """
            SELECT *
            FROM monitoring_items
            WHERE asset_id = ?
            ORDER BY id ASC
            """,
            (source_asset_id,),
        )
        source_items = [dict(row) for row in cursor.fetchall()]
        for item in source_items:
            item_id = int(item["id"])
            monitor_type = str(item.get("monitor_type") or "").strip().lower()
            cursor.execute(
                """
                SELECT id
                FROM monitoring_items
                WHERE asset_id = ?
                  AND monitor_type = ?
                  AND id != ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (target_asset_id, monitor_type, item_id),
            )
            existing_row = cursor.fetchone()
            if existing_row:
                target_item_id = int(existing_row["id"])
                if self._table_exists(cursor.connection, "monitoring_events"):
                    cursor.execute(
                        "UPDATE monitoring_events SET monitoring_item_id = ? WHERE monitoring_item_id = ?",
                        (target_item_id, item_id),
                    )
                    changed += int(cursor.rowcount or 0)
                if self._table_exists(cursor.connection, "monitoring_price_history"):
                    cursor.execute(
                        "UPDATE monitoring_price_history SET monitoring_item_id = ? WHERE monitoring_item_id = ?",
                        (target_item_id, item_id),
                    )
                    changed += int(cursor.rowcount or 0)
                cursor.execute("DELETE FROM monitoring_items WHERE id = ?", (item_id,))
                changed += int(cursor.rowcount or 0)
                continue

            cursor.execute(
                """
                UPDATE monitoring_items
                SET asset_id = ?,
                    portfolio_stock_id = CASE WHEN portfolio_stock_id = ? THEN ? ELSE portfolio_stock_id END
                WHERE id = ?
                """,
                (target_asset_id, source_asset_id, target_asset_id, item_id),
            )
            changed += int(cursor.rowcount or 0)

        cursor.execute(
            """
            UPDATE monitoring_items
            SET portfolio_stock_id = ?
            WHERE portfolio_stock_id = ?
            """,
            (target_asset_id, source_asset_id),
        )
        changed += int(cursor.rowcount or 0)
        return changed

    def _rewire_asset_references(self, cursor: sqlite3.Cursor, source_asset_id: int, target_asset_id: int) -> int:
        rewired = 0
        rewired += self._rewire_monitoring_items(cursor, source_asset_id, target_asset_id)
        reference_map = {
            "asset_trade_history": ("asset_id",),
            "asset_action_queue": ("asset_id",),
            "analysis_records": ("asset_id", "portfolio_stock_id"),
            "ai_decisions": ("asset_id", "portfolio_stock_id"),
            "portfolio_trade_history": ("portfolio_stock_id",),
            "portfolio_analysis_history": ("portfolio_stock_id",),
        }
        for table_name, columns in reference_map.items():
            for column_name in columns:
                rewired += self._rewire_table_reference(
                    cursor,
                    table_name,
                    column_name,
                    source_asset_id,
                    target_asset_id,
                )
        return rewired

    def _normalize_account_names_if_needed(self) -> None:
        conn = self._connect()
        try:
            if get_metadata(conn, self.ACCOUNT_NORMALIZATION_KEY):
                return
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM assets
                WHERE deleted_at IS NULL
                ORDER BY id ASC
                """
            )
            rows = [dict(row) for row in cursor.fetchall()]
            groups: Dict[tuple[str, str], List[Dict]] = {}
            for row in rows:
                key = (
                    self._normalize_account_name(row.get("account_name")),
                    self._normalize_symbol(row.get("symbol")),
                )
                groups.setdefault(key, []).append(row)

            updated_assets = 0
            merged_assets = 0
            rewired_references = 0
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for (normalized_account_name, _symbol), group_rows in groups.items():
                ordered_rows = sorted(group_rows, key=self._asset_merge_priority, reverse=True)
                target_row = ordered_rows[0]
                duplicate_rows = ordered_rows[1:]

                for duplicate_row in duplicate_rows:
                    rewired_references += self._rewire_asset_references(
                        cursor,
                        int(duplicate_row["id"]),
                        int(target_row["id"]),
                    )
                    cursor.execute(
                        """
                        UPDATE assets
                        SET deleted_at = ?, updated_at = ?
                        WHERE id = ? AND deleted_at IS NULL
                        """,
                        (now, now, int(duplicate_row["id"])),
                    )
                    merged_assets += 1 if cursor.rowcount > 0 else 0

                merged_row = self._merge_asset_rows(ordered_rows, normalized_account_name)
                fields: List[str] = []
                values: List[Any] = []
                for key in (
                    "account_name",
                    "symbol",
                    "name",
                    "status",
                    "cost_price",
                    "quantity",
                    "note",
                    "monitor_enabled",
                    "origin_analysis_id",
                    "last_trade_at",
                ):
                    if merged_row[key] != target_row.get(key):
                        fields.append(f"{key} = ?")
                        values.append(merged_row[key])
                if not fields:
                    continue
                fields.append("updated_at = ?")
                values.append(now)
                values.append(int(target_row["id"]))
                cursor.execute(
                    f"UPDATE assets SET {', '.join(fields)} WHERE id = ?",
                    tuple(values),
                )
                updated_assets += 1 if cursor.rowcount > 0 else 0

            set_metadata(
                conn,
                self.ACCOUNT_NORMALIZATION_KEY,
                json.dumps(
                    {
                        "updated_assets": updated_assets,
                        "merged_assets": merged_assets,
                        "rewired_references": rewired_references,
                        "updated_at": now,
                    },
                    ensure_ascii=False,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _migrate_existing_canonical_tables(self) -> None:
        conn = self._connect()
        try:
            if get_metadata(conn, "assets_canonical_migration_v1"):
                return
        finally:
            conn.close()

        self._migrate_portfolio_stocks_to_assets()
        self._migrate_monitoring_items_to_assets()
        self._migrate_analysis_records_to_assets()
        self._migrate_trade_history_to_assets()

        conn = self._connect()
        set_metadata(conn, "assets_canonical_migration_v1", datetime.now().isoformat())
        conn.commit()
        conn.close()

    def _upsert_asset_from_migration(
        self,
        *,
        account_name: str,
        symbol: str,
        name: str,
        status: str,
        asset_id: Optional[int] = None,
        cost_price: Optional[float] = None,
        quantity: Optional[int] = None,
        note: Optional[str] = None,
        monitor_enabled: bool = True,
        origin_analysis_id: Optional[int] = None,
        last_trade_at: Optional[str] = None,
        created_at: Optional[str] = None,
        updated_at: Optional[str] = None,
    ) -> None:
        existing = self.get_asset_by_symbol(symbol, account_name)
        target_status = self._normalize_status(status)
        if existing:
            merged_status = (
                target_status
                if STATUS_PRIORITY[target_status] >= STATUS_PRIORITY[self._normalize_status(existing.get("status"))]
                else self._normalize_status(existing.get("status"))
            )
            updates = {
                "status": merged_status,
                "name": name or existing.get("name") or symbol,
                "monitor_enabled": monitor_enabled if monitor_enabled is not None else existing.get("monitor_enabled", True),
                "origin_analysis_id": origin_analysis_id or existing.get("origin_analysis_id"),
                "note": note if note not in (None, "") else existing.get("note"),
                "last_trade_at": last_trade_at or existing.get("last_trade_at"),
            }
            if merged_status == STATUS_HOLDING:
                updates["cost_price"] = cost_price if cost_price not in (None, 0) else existing.get("cost_price")
                updates["quantity"] = quantity if quantity not in (None, 0) else existing.get("quantity")
            self.update_asset(existing["id"], **updates)
            return

        conn = self._connect()
        cursor = conn.cursor()
        columns = [
            "account_name",
            "symbol",
            "name",
            "status",
            "cost_price",
            "quantity",
            "note",
            "monitor_enabled",
            "origin_analysis_id",
            "last_trade_at",
            "created_at",
            "updated_at",
        ]
        values = [
            account_name,
            symbol,
            name or symbol,
            target_status,
            cost_price if target_status == STATUS_HOLDING else None,
            quantity if target_status == STATUS_HOLDING else None,
            note,
            self._bool_to_int(monitor_enabled),
            origin_analysis_id,
            last_trade_at,
            created_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ]
        if asset_id is not None:
            columns.insert(0, "id")
            values.insert(0, asset_id)
        cursor.execute(
            f"""
            INSERT OR IGNORE INTO assets ({", ".join(columns)})
            VALUES ({", ".join("?" for _ in columns)})
            """,
            tuple(values),
        )
        conn.commit()
        conn.close()

    def _extract_sector_tags_from_stock_info(self, raw_value: Any) -> List[str]:
        stock_info = self._safe_json_loads(raw_value, {})
        if not isinstance(stock_info, dict):
            return []

        tags: List[str] = []
        for key in (
            "industry",
            "sector",
            "concept",
            "concepts",
            "sectors",
            "sector_tags",
            "所属行业",
            "所属板块",
            "概念板块",
        ):
            value = stock_info.get(key)
            if isinstance(value, list):
                candidates = value
            elif isinstance(value, str):
                candidates = value.replace("，", ",").replace("、", ",").split(",")
            else:
                candidates = []
            for candidate in candidates:
                text = str(candidate or "").strip()
                if text and text not in tags:
                    tags.append(text)
        return tags[:12]

    def _portfolio_stock_is_active(self, stock: Dict[str, Any]) -> bool:
        status = str(stock.get("position_status") or "active").strip().lower()
        if status in {"closed", "cleared", "deleted", "removed", "inactive"}:
            return False
        try:
            quantity = int(float(stock.get("quantity") or 0))
        except (TypeError, ValueError):
            quantity = 0
        return quantity > 0

    def _upsert_lifecycle_asset(
        self,
        cursor: sqlite3.Cursor,
        *,
        symbol: Any,
        name: Any = "",
        status: str = STATUS_RESEARCH,
        cost_price: Any = None,
        quantity: Any = None,
        note: Any = None,
        monitor_enabled: Any = None,
        origin_analysis_id: Any = None,
        pool_reason: Any = None,
        pool_reason_source: Any = None,
        sector_tags: Optional[List[str]] = None,
        last_trade_at: Any = None,
        created_at: Any = None,
        updated_at: Any = None,
    ) -> tuple[Optional[int], bool, bool]:
        normalized_symbol = self._normalize_symbol(symbol)
        if not normalized_symbol:
            return None, False, False

        target_status = self._normalize_status(status)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            """
            SELECT *
            FROM assets
            WHERE symbol = ? AND deleted_at IS NULL
            ORDER BY id ASC
            LIMIT 1
            """,
            (normalized_symbol,),
        )
        row = cursor.fetchone()
        name_text = str(name or "").strip() or normalized_symbol
        note_text = str(note).strip() if note not in (None, "") else None
        reason_text = str(pool_reason).strip() if pool_reason not in (None, "") else None
        reason_source_text = (
            str(pool_reason_source).strip() if pool_reason_source not in (None, "") else None
        )
        sector_tags_json = (
            json.dumps(sector_tags, ensure_ascii=False)
            if isinstance(sector_tags, list) and sector_tags
            else None
        )

        if row is None:
            cursor.execute(
                """
                INSERT INTO assets (
                    account_name, symbol, name, status, cost_price, quantity, note,
                    monitor_enabled, origin_analysis_id, pool_reason, pool_reason_source,
                    sector_tags_json, last_trade_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    DEFAULT_ACCOUNT_NAME,
                    normalized_symbol,
                    name_text,
                    target_status,
                    cost_price if target_status == STATUS_HOLDING else None,
                    quantity if target_status == STATUS_HOLDING else None,
                    note_text,
                    self._bool_to_int(True if monitor_enabled is None else monitor_enabled),
                    origin_analysis_id,
                    reason_text or note_text,
                    reason_source_text,
                    sector_tags_json,
                    last_trade_at,
                    created_at or now,
                    updated_at or now,
                ),
            )
            return int(cursor.lastrowid), True, False

        existing = dict(row)
        existing_status = self._normalize_status(existing.get("status"))
        merged_status = (
            target_status
            if STATUS_PRIORITY[target_status] >= STATUS_PRIORITY[existing_status]
            else existing_status
        )
        updates: Dict[str, Any] = {
            "account_name": DEFAULT_ACCOUNT_NAME,
            "status": merged_status,
        }

        if name_text and (not existing.get("name") or existing.get("name") == existing.get("symbol")):
            updates["name"] = name_text
        if monitor_enabled is not None:
            updates["monitor_enabled"] = self._bool_to_int(monitor_enabled)
        if origin_analysis_id not in (None, "") and existing.get("origin_analysis_id") in (None, ""):
            updates["origin_analysis_id"] = origin_analysis_id
        if note_text and not existing.get("note"):
            updates["note"] = note_text
        if reason_text and (
            not existing.get("pool_reason")
            or STATUS_PRIORITY[target_status] >= STATUS_PRIORITY[existing_status]
        ):
            updates["pool_reason"] = reason_text
        if reason_source_text and (
            not existing.get("pool_reason_source")
            or STATUS_PRIORITY[target_status] >= STATUS_PRIORITY[existing_status]
        ):
            updates["pool_reason_source"] = reason_source_text
        if sector_tags_json and not existing.get("sector_tags_json"):
            updates["sector_tags_json"] = sector_tags_json
        if last_trade_at:
            updates["last_trade_at"] = last_trade_at
        if merged_status == STATUS_HOLDING:
            if cost_price not in (None, ""):
                updates["cost_price"] = cost_price
            if quantity not in (None, ""):
                updates["quantity"] = quantity

        changed_fields = [
            (key, value)
            for key, value in updates.items()
            if existing.get(key) != value
        ]
        if not changed_fields:
            return int(existing["id"]), False, False

        changed_fields.append(("updated_at", updated_at or now))
        assignments = ", ".join(f"{key} = ?" for key, _ in changed_fields)
        values = [value for _, value in changed_fields]
        values.append(int(existing["id"]))
        cursor.execute(
            f"UPDATE assets SET {assignments} WHERE id = ?",
            tuple(values),
        )
        return int(existing["id"]), False, True

    def _backfill_portfolio_assets(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        if not self._table_exists(cursor.connection, "portfolio_stocks"):
            return {}
        columns = self._table_columns(cursor.connection, "portfolio_stocks")
        if "code" not in columns:
            return {}
        cursor.execute(
            """
            SELECT COUNT(*) AS count
            FROM assets
            WHERE deleted_at IS NULL AND status = ?
            """,
            (STATUS_HOLDING,),
        )
        existing_holding_count = int((cursor.fetchone() or {"count": 0})["count"] or 0)
        if existing_holding_count > 0:
            return {
                "portfolio_holdings": 0,
                "portfolio_created": 0,
                "portfolio_updated": 0,
                "portfolio_skipped_existing_holdings": existing_holding_count,
            }

        cursor.execute("SELECT * FROM portfolio_stocks ORDER BY id ASC")
        created = 0
        updated = 0
        holdings = 0
        for row in cursor.fetchall():
            stock = dict(row)
            if not self._portfolio_stock_is_active(stock):
                continue
            holdings += 1
            last_trade_at = stock.get("last_trade_at")
            if not last_trade_at and self._table_exists(cursor.connection, "portfolio_trade_history"):
                cursor.execute(
                    """
                    SELECT MAX(trade_date) AS latest_trade_date
                    FROM portfolio_trade_history
                    WHERE portfolio_stock_id = ?
                    """,
                    (stock.get("id"),),
                )
                trade_row = cursor.fetchone()
                last_trade_at = trade_row["latest_trade_date"] if trade_row else None
            _, was_created, was_updated = self._upsert_lifecycle_asset(
                cursor,
                symbol=stock.get("code"),
                name=stock.get("name"),
                status=STATUS_HOLDING,
                cost_price=stock.get("cost_price"),
                quantity=stock.get("quantity"),
                note=stock.get("note"),
                monitor_enabled=stock.get("auto_monitor", 1),
                origin_analysis_id=stock.get("origin_analysis_id"),
                pool_reason="由旧持仓账本迁入持仓中",
                pool_reason_source="legacy_portfolio",
                last_trade_at=last_trade_at,
                created_at=stock.get("created_at"),
                updated_at=stock.get("updated_at"),
            )
            created += 1 if was_created else 0
            updated += 1 if was_updated else 0
        return {"portfolio_holdings": holdings, "portfolio_created": created, "portfolio_updated": updated}

    def _get_active_portfolio_symbols(self, cursor: sqlite3.Cursor) -> set[str]:
        if not self._table_exists(cursor.connection, "portfolio_stocks"):
            return set()
        columns = self._table_columns(cursor.connection, "portfolio_stocks")
        if "code" not in columns:
            return set()

        cursor.execute("SELECT * FROM portfolio_stocks ORDER BY id ASC")
        return {
            self._normalize_symbol(dict(row).get("code"))
            for row in cursor.fetchall()
            if self._portfolio_stock_is_active(dict(row))
        }

    def _reconcile_holdings_with_portfolio(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        return {"holding_demoted_not_in_portfolio": 0}

    def _backfill_monitoring_assets(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        if not self._table_exists(cursor.connection, "monitoring_items"):
            return {}
        columns = self._table_columns(cursor.connection, "monitoring_items")
        if "symbol" not in columns:
            return {}

        cursor.execute("SELECT * FROM monitoring_items ORDER BY id ASC")
        created = 0
        updated = 0
        focus_candidates = 0
        seen: set[str] = set()
        for row in cursor.fetchall():
            item = dict(row)
            symbol = self._normalize_symbol(item.get("symbol"))
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            config = self._safe_json_loads(item.get("config_json"), {})
            strategy_context = config.get("strategy_context") if isinstance(config, dict) else {}
            if not isinstance(strategy_context, dict):
                strategy_context = {}
            reason = (
                strategy_context.get("summary")
                or item.get("last_message")
                or "由旧关注和盯盘记录迁入备选关注"
            )
            _, was_created, was_updated = self._upsert_lifecycle_asset(
                cursor,
                symbol=symbol,
                name=item.get("name"),
                status=STATUS_FOCUS,
                monitor_enabled=item.get("enabled", 1),
                origin_analysis_id=item.get("origin_analysis_id") or strategy_context.get("origin_analysis_id"),
                pool_reason=reason,
                pool_reason_source="legacy_monitoring",
                created_at=item.get("created_at"),
                updated_at=item.get("updated_at"),
            )
            focus_candidates += 1
            created += 1 if was_created else 0
            updated += 1 if was_updated else 0
        return {"monitoring_candidates": focus_candidates, "monitoring_created": created, "monitoring_updated": updated}

    def _backfill_analysis_assets(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        if not self._table_exists(cursor.connection, "analysis_records"):
            return {}
        columns = self._table_columns(cursor.connection, "analysis_records")
        if "symbol" not in columns:
            return {}

        cursor.execute(
            """
            SELECT *
            FROM analysis_records
            WHERE COALESCE(TRIM(symbol), '') != ''
            ORDER BY datetime(COALESCE(analysis_date, created_at)) DESC, id DESC
            """
        )
        created = 0
        updated = 0
        symbols = 0
        seen: set[str] = set()
        for row in cursor.fetchall():
            record = dict(row)
            symbol = self._normalize_symbol(record.get("symbol"))
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols += 1
            sector_tags = self._extract_sector_tags_from_stock_info(record.get("stock_info_json"))
            _, was_created, was_updated = self._upsert_lifecycle_asset(
                cursor,
                symbol=symbol,
                name=record.get("stock_name"),
                status=STATUS_RESEARCH,
                note=record.get("summary"),
                origin_analysis_id=record.get("id"),
                pool_reason=record.get("summary") or "由历史分析迁入研究池",
                pool_reason_source="analysis_history",
                sector_tags=sector_tags,
                created_at=record.get("created_at"),
                updated_at=record.get("created_at"),
            )
            created += 1 if was_created else 0
            updated += 1 if was_updated else 0
        return {"analysis_symbols": symbols, "analysis_created": created, "analysis_updated": updated}

    def _rewire_lifecycle_references(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        rewired: Dict[str, int] = {}

        if self._table_exists(cursor.connection, "analysis_records"):
            cursor.execute(
                """
                UPDATE analysis_records
                SET asset_id = (
                    SELECT id FROM assets
                    WHERE assets.symbol = analysis_records.symbol
                      AND assets.deleted_at IS NULL
                    LIMIT 1
                )
                WHERE COALESCE(TRIM(symbol), '') != ''
                  AND EXISTS (
                    SELECT 1 FROM assets
                    WHERE assets.symbol = analysis_records.symbol
                      AND assets.deleted_at IS NULL
                  )
                  AND (
                    asset_id IS NULL
                    OR asset_id != (
                        SELECT id FROM assets
                        WHERE assets.symbol = analysis_records.symbol
                          AND assets.deleted_at IS NULL
                        LIMIT 1
                    )
                  )
                """
            )
            rewired["analysis_records_asset_id"] = int(cursor.rowcount or 0)
            columns = self._table_columns(cursor.connection, "analysis_records")
            if "portfolio_stock_id" in columns:
                cursor.execute(
                    """
                    UPDATE analysis_records
                    SET portfolio_stock_id = asset_id
                    WHERE asset_id IS NOT NULL
                      AND (portfolio_stock_id IS NULL OR portfolio_stock_id != asset_id)
                    """
                )
                rewired["analysis_records_portfolio_stock_id"] = int(cursor.rowcount or 0)

        if self._table_exists(cursor.connection, "monitoring_items"):
            cursor.execute(
                """
                UPDATE monitoring_items
                SET asset_id = (
                    SELECT id FROM assets
                    WHERE assets.symbol = monitoring_items.symbol
                      AND assets.deleted_at IS NULL
                    LIMIT 1
                )
                WHERE COALESCE(TRIM(symbol), '') != ''
                  AND EXISTS (
                    SELECT 1 FROM assets
                    WHERE assets.symbol = monitoring_items.symbol
                      AND assets.deleted_at IS NULL
                  )
                  AND (
                    asset_id IS NULL
                    OR asset_id != (
                        SELECT id FROM assets
                        WHERE assets.symbol = monitoring_items.symbol
                          AND assets.deleted_at IS NULL
                        LIMIT 1
                    )
                  )
                """
            )
            rewired["monitoring_items_asset_id"] = int(cursor.rowcount or 0)
            columns = self._table_columns(cursor.connection, "monitoring_items")
            if "portfolio_stock_id" in columns:
                cursor.execute(
                    """
                    UPDATE monitoring_items
                    SET portfolio_stock_id = asset_id
                    WHERE asset_id IS NOT NULL
                      AND (portfolio_stock_id IS NULL OR portfolio_stock_id != asset_id)
                    """
                )
                rewired["monitoring_items_portfolio_stock_id"] = int(cursor.rowcount or 0)

        if self._table_exists(cursor.connection, "ai_decisions"):
            cursor.execute(
                """
                UPDATE ai_decisions
                SET asset_id = (
                    SELECT id FROM assets
                    WHERE assets.symbol = ai_decisions.stock_code
                      AND assets.deleted_at IS NULL
                    LIMIT 1
                )
                WHERE COALESCE(TRIM(stock_code), '') != ''
                  AND EXISTS (
                    SELECT 1 FROM assets
                    WHERE assets.symbol = ai_decisions.stock_code
                      AND assets.deleted_at IS NULL
                  )
                  AND (
                    asset_id IS NULL
                    OR asset_id != (
                        SELECT id FROM assets
                        WHERE assets.symbol = ai_decisions.stock_code
                          AND assets.deleted_at IS NULL
                        LIMIT 1
                    )
                  )
                """
            )
            rewired["ai_decisions_asset_id"] = int(cursor.rowcount or 0)
            columns = self._table_columns(cursor.connection, "ai_decisions")
            if "portfolio_stock_id" in columns:
                cursor.execute(
                    """
                    UPDATE ai_decisions
                    SET portfolio_stock_id = asset_id
                    WHERE asset_id IS NOT NULL
                      AND (portfolio_stock_id IS NULL OR portfolio_stock_id != asset_id)
                    """
                )
                rewired["ai_decisions_portfolio_stock_id"] = int(cursor.rowcount or 0)

        return rewired

    def _backfill_trade_history(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        if not (
            self._table_exists(cursor.connection, "portfolio_trade_history")
            and self._table_exists(cursor.connection, "portfolio_stocks")
            and self._table_exists(cursor.connection, "asset_trade_history")
        ):
            return {}

        cursor.execute(
            """
            SELECT
                pth.id AS legacy_trade_id,
                pth.trade_date,
                pth.trade_type,
                pth.price,
                pth.quantity,
                pth.note,
                pth.trade_source,
                pth.created_at,
                ps.code AS symbol
            FROM portfolio_trade_history pth
            LEFT JOIN portfolio_stocks ps ON ps.id = pth.portfolio_stock_id
            ORDER BY pth.id ASC
            """
        )
        inserted = 0
        skipped = 0
        for row in cursor.fetchall():
            trade = dict(row)
            symbol = self._normalize_symbol(trade.get("symbol"))
            if not symbol:
                skipped += 1
                continue
            cursor.execute(
                """
                SELECT id
                FROM assets
                WHERE symbol = ? AND deleted_at IS NULL
                LIMIT 1
                """,
                (symbol,),
            )
            asset_row = cursor.fetchone()
            if asset_row is None:
                skipped += 1
                continue
            asset_id = int(asset_row["id"])
            cursor.execute(
                """
                SELECT 1
                FROM asset_trade_history
                WHERE asset_id = ?
                  AND trade_date = ?
                  AND trade_type = ?
                  AND ABS(COALESCE(price, 0) - COALESCE(?, 0)) < 0.000001
                  AND quantity = ?
                  AND COALESCE(note, '') = COALESCE(?, '')
                  AND COALESCE(trade_source, '') = COALESCE(?, '')
                LIMIT 1
                """,
                (
                    asset_id,
                    trade.get("trade_date"),
                    trade.get("trade_type"),
                    trade.get("price"),
                    trade.get("quantity"),
                    trade.get("note"),
                    trade.get("trade_source") or "manual",
                ),
            )
            if cursor.fetchone():
                skipped += 1
                continue
            cursor.execute(
                """
                INSERT INTO asset_trade_history (
                    asset_id, trade_date, trade_type, price, quantity, note, trade_source, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset_id,
                    trade.get("trade_date"),
                    trade.get("trade_type"),
                    trade.get("price"),
                    trade.get("quantity"),
                    trade.get("note"),
                    trade.get("trade_source") or "manual",
                    trade.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            inserted += 1
        return {"trade_history_inserted": inserted, "trade_history_skipped": skipped}

    def backfill_lifecycle_data_from_legacy(self, *, force: bool = False) -> Dict[str, Any]:
        conn = self._connect()
        try:
            if not force:
                existing_report = get_metadata(conn, self.LIFECYCLE_DATA_BACKFILL_KEY)
                if existing_report:
                    return self._safe_json_loads(existing_report, {})

            cursor = conn.cursor()
            report: Dict[str, Any] = {
                "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "force": bool(force),
            }
            for partial in (
                self._backfill_analysis_assets(cursor),
                self._backfill_monitoring_assets(cursor),
                self._backfill_portfolio_assets(cursor),
                self._reconcile_holdings_with_portfolio(cursor),
                self._rewire_lifecycle_references(cursor),
                self._backfill_trade_history(cursor),
            ):
                report.update(partial)

            cursor.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM assets
                WHERE deleted_at IS NULL
                GROUP BY status
                """
            )
            report["asset_status_counts"] = {
                row["status"]: int(row["count"]) for row in cursor.fetchall()
            }
            report["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            set_metadata(
                conn,
                self.LIFECYCLE_DATA_BACKFILL_KEY,
                json.dumps(report, ensure_ascii=False),
            )
            conn.commit()
            return report
        finally:
            conn.close()

    def _migrate_portfolio_stocks_to_assets(self) -> None:
        conn = self._connect()
        try:
            if not self._table_exists(conn, "portfolio_stocks"):
                return
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT *
                FROM portfolio_stocks
                ORDER BY id ASC
                """
            )
            for row in cursor.fetchall():
                stock = dict(row)
                self._upsert_asset_from_migration(
                    account_name=self._normalize_account_name(stock.get("account_name")),
                    symbol=self._normalize_symbol(stock.get("code")),
                    name=stock.get("name") or stock.get("code") or "",
                    status=STATUS_HOLDING if (stock.get("position_status") or "active") == "active" else STATUS_FOCUS,
                    asset_id=stock.get("id"),
                    cost_price=stock.get("cost_price"),
                    quantity=stock.get("quantity"),
                    note=stock.get("note"),
                    monitor_enabled=bool(stock.get("auto_monitor", 1)),
                    origin_analysis_id=stock.get("origin_analysis_id"),
                    last_trade_at=stock.get("last_trade_at"),
                    created_at=str(stock.get("created_at") or datetime.now()),
                    updated_at=str(stock.get("updated_at") or datetime.now()),
                )
        finally:
            conn.close()

    def _migrate_monitoring_items_to_assets(self) -> None:
        conn = self._connect()
        try:
            if not self._table_exists(conn, "monitoring_items"):
                return
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT symbol, name, account_name, config_json, origin_analysis_id
                FROM monitoring_items
                ORDER BY id ASC
                """
            )
            for row in cursor.fetchall():
                item = dict(row)
                symbol = self._normalize_symbol(item.get("symbol"))
                if not symbol:
                    continue
                account_name = self._normalize_account_name(item.get("account_name"))
                config = self._safe_json_loads(item.get("config_json"), {})
                strategy_context = config.get("strategy_context") or {}
                note = strategy_context.get("summary") or None
                self._upsert_asset_from_migration(
                    account_name=account_name,
                    symbol=symbol,
                    name=item.get("name") or symbol,
                    status=STATUS_FOCUS,
                    note=note,
                    origin_analysis_id=item.get("origin_analysis_id") or strategy_context.get("origin_analysis_id"),
                )
        finally:
            conn.close()

    def _migrate_analysis_records_to_assets(self) -> None:
        conn = self._connect()
        try:
            if not self._table_exists(conn, "analysis_records"):
                return
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT symbol, stock_name, account_name, analysis_scope, summary, id
                FROM analysis_records
                ORDER BY id ASC
                """
            )
            for row in cursor.fetchall():
                record = dict(row)
                symbol = self._normalize_symbol(record.get("symbol"))
                if not symbol:
                    continue
                status = STATUS_HOLDING if record.get("analysis_scope") == STATUS_HOLDING else STATUS_RESEARCH
                self._upsert_asset_from_migration(
                    account_name=self._normalize_account_name(record.get("account_name")),
                    symbol=symbol,
                    name=record.get("stock_name") or symbol,
                    status=status,
                    note=record.get("summary") or None,
                    origin_analysis_id=record.get("id"),
                )
        finally:
            conn.close()

    def _migrate_trade_history_to_assets(self) -> None:
        conn = self._connect()
        try:
            if not self._table_exists(conn, "portfolio_trade_history"):
                return
            if get_metadata(conn, "asset_trade_history_migrated_v1"):
                return
        finally:
            conn.close()

        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, portfolio_stock_id, trade_date, trade_type, price, quantity, note, trade_source, created_at
                FROM portfolio_trade_history
                ORDER BY id ASC
                """
            )
            for row in cursor.fetchall():
                trade = dict(row)
                asset_id = trade.get("portfolio_stock_id")
                if asset_id is None:
                    continue
                cursor.execute(
                    "SELECT 1 FROM assets WHERE id = ? LIMIT 1",
                    (asset_id,),
                )
                if cursor.fetchone() is None:
                    continue
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO asset_trade_history (
                        id, asset_id, trade_date, trade_type, price, quantity, note, trade_source, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade.get("id"),
                        asset_id,
                        trade.get("trade_date"),
                        trade.get("trade_type"),
                        trade.get("price"),
                        trade.get("quantity"),
                        trade.get("note"),
                        trade.get("trade_source", "manual"),
                        trade.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
            set_metadata(conn, "asset_trade_history_migrated_v1", datetime.now().isoformat())
            conn.commit()
        finally:
            conn.close()

    def get_asset(self, asset_id: int) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM assets
            WHERE id = ? AND deleted_at IS NULL
            """,
            (asset_id,),
        )
        row = cursor.fetchone()
        conn.close()
        return self._row_to_asset(row) if row else None

    def get_asset_by_symbol(self, symbol: str, account_name: str = DEFAULT_ACCOUNT_NAME) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM assets
            WHERE symbol = ? AND deleted_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (self._normalize_symbol(symbol),),
        )
        row = cursor.fetchone()
        conn.close()
        return self._row_to_asset(row) if row else None

    def list_assets(
        self,
        *,
        status: Optional[str] = None,
        account_name: Optional[str] = None,
        monitor_enabled: Optional[bool] = None,
        include_deleted: bool = False,
        symbol: Optional[str] = None,
    ) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        clauses = []
        params: List[Any] = []
        if not include_deleted:
            clauses.append("deleted_at IS NULL")
        if status:
            clauses.append("status = ?")
            params.append(self._normalize_status(status))
        if monitor_enabled is not None:
            clauses.append("monitor_enabled = ?")
            params.append(self._bool_to_int(monitor_enabled))
        if symbol:
            clauses.append("symbol = ?")
            params.append(self._normalize_symbol(symbol))
        sql = "SELECT * FROM assets"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY datetime(updated_at) DESC, id DESC"
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_asset(row) for row in rows]

    def create_or_update_research_asset(
        self,
        *,
        symbol: str,
        name: str,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        note: str = "",
        origin_analysis_id: Optional[int] = None,
        monitor_enabled: bool = True,
    ) -> int:
        symbol = self._normalize_symbol(symbol)
        existing = self.get_asset_by_symbol(symbol, account_name)
        if existing:
            self.update_asset(
                existing["id"],
                name=name or existing.get("name") or symbol,
                note=note or existing.get("note"),
                monitor_enabled=monitor_enabled if monitor_enabled is not None else existing.get("monitor_enabled", True),
                origin_analysis_id=origin_analysis_id or existing.get("origin_analysis_id"),
                account_name=DEFAULT_ACCOUNT_NAME,
            )
            return int(existing["id"])

        conn = self._connect()
        cursor = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            """
            INSERT INTO assets (
                account_name, symbol, name, status, note, monitor_enabled, origin_analysis_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                DEFAULT_ACCOUNT_NAME,
                symbol,
                name or symbol,
                STATUS_RESEARCH,
                note or None,
                self._bool_to_int(monitor_enabled),
                origin_analysis_id,
                now,
                now,
            ),
        )
        asset_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return asset_id

    def update_asset(self, asset_id: int, **updates) -> bool:
        if not updates:
            return False
        allowed_fields = {
            "account_name",
            "symbol",
            "name",
            "status",
            "cost_price",
            "quantity",
            "note",
            "monitor_enabled",
            "origin_analysis_id",
            "last_trade_at",
            "deleted_at",
            "manual_pin",
            "pool_reason",
            "pool_reason_source",
            "last_funnel_score",
            "last_funnel_snapshot_json",
            "last_exit_reason",
            "last_exit_at",
            "sector_tags_json",
        }
        fields = []
        values: List[Any] = []
        status = self._normalize_status(updates.get("status")) if "status" in updates else None
        for key, value in updates.items():
            if key not in allowed_fields:
                continue
            if key == "account_name":
                value = self._normalize_account_name(value)
            elif key == "symbol":
                value = self._normalize_symbol(value)
            elif key == "status":
                value = status
            elif key == "monitor_enabled":
                value = self._bool_to_int(value)
            elif key == "manual_pin":
                value = self._bool_to_int(value)
            elif key in {"sector_tags_json", "last_funnel_snapshot_json"} and isinstance(value, (list, dict)):
                value = json.dumps(value, ensure_ascii=False)
            fields.append(f"{key} = ?")
            values.append(value)

        if status:
            if status != STATUS_HOLDING:
                if "cost_price" not in updates:
                    fields.append("cost_price = NULL")
                if "quantity" not in updates:
                    fields.append("quantity = NULL")
            else:
                cost_price = updates.get("cost_price")
                quantity = updates.get("quantity")
                if cost_price in (None, "") or int(quantity or 0) <= 0:
                    raise ValueError("portfolio 状态必须同时提供有效的 cost_price 和 quantity")

        if not fields:
            return False

        fields.append("updated_at = ?")
        values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        values.append(asset_id)

        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE assets SET {', '.join(fields)} WHERE id = ? AND deleted_at IS NULL",
            tuple(values),
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def transition_asset_status(
        self,
        asset_id: int,
        target_status: str,
        *,
        cost_price: Optional[float] = None,
        quantity: Optional[int] = None,
        note: Optional[str] = None,
        origin_analysis_id: Optional[int] = None,
        last_trade_at: Optional[str] = None,
        last_exit_reason: Optional[str] = None,
        last_exit_at: Optional[str] = None,
        pool_reason: Optional[str] = None,
        pool_reason_source: Optional[str] = None,
    ) -> bool:
        target_status = self._normalize_status(target_status)
        asset = self.get_asset(asset_id)
        if not asset:
            return False
        update_payload = {
            "status": target_status,
            "note": note if note is not None else asset.get("note"),
            "origin_analysis_id": origin_analysis_id or asset.get("origin_analysis_id"),
            "last_trade_at": last_trade_at or asset.get("last_trade_at"),
            "last_exit_reason": last_exit_reason if last_exit_reason is not None else asset.get("last_exit_reason"),
            "last_exit_at": last_exit_at if last_exit_at is not None else asset.get("last_exit_at"),
            "pool_reason": pool_reason if pool_reason is not None else asset.get("pool_reason"),
            "pool_reason_source": pool_reason_source if pool_reason_source is not None else asset.get("pool_reason_source"),
        }
        if target_status == STATUS_HOLDING:
            update_payload["cost_price"] = cost_price if cost_price is not None else asset.get("cost_price")
            update_payload["quantity"] = quantity if quantity is not None else asset.get("quantity")
        else:
            update_payload["cost_price"] = None
            update_payload["quantity"] = None
        return self.update_asset(asset_id, **update_payload)

    def update_portfolio_fields(
        self,
        asset_id: int,
        *,
        cost_price: Optional[float],
        quantity: Optional[int],
        note: Optional[str] = None,
        last_trade_at: Optional[str] = None,
    ) -> bool:
        effective_quantity = int(quantity or 0)
        if effective_quantity <= 0:
            return self.transition_asset_status(
                asset_id,
                STATUS_RESEARCH,
                note=note,
                last_trade_at=last_trade_at,
            )
        effective_cost = float(cost_price or 0)
        if effective_cost <= 0:
            raise ValueError("portfolio 资产必须提供正数成本价")
        return self.update_asset(
            asset_id,
            status=STATUS_HOLDING,
            cost_price=effective_cost,
            quantity=effective_quantity,
            note=note,
            last_trade_at=last_trade_at,
        )

    def promote_to_watchlist(
        self,
        *,
        symbol: str,
        name: str,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        note: str = "",
        origin_analysis_id: Optional[int] = None,
        monitor_enabled: bool = True,
    ) -> int:
        symbol = self._normalize_symbol(symbol)
        existing = self.get_asset_by_symbol(symbol, account_name)
        if existing:
            if existing.get("status") == STATUS_HOLDING:
                return int(existing["id"])
            self.update_asset(
                existing["id"],
                name=name or existing.get("name") or symbol,
                status=STATUS_FOCUS,
                note=note or existing.get("note"),
                monitor_enabled=monitor_enabled if monitor_enabled is not None else existing.get("monitor_enabled", True),
                origin_analysis_id=origin_analysis_id or existing.get("origin_analysis_id"),
            )
            return int(existing["id"])
        asset_id = self.create_or_update_research_asset(
            symbol=symbol,
            name=name,
            account_name=DEFAULT_ACCOUNT_NAME,
            note=note,
            origin_analysis_id=origin_analysis_id,
            monitor_enabled=monitor_enabled,
        )
        self.transition_asset_status(
            asset_id,
            STATUS_FOCUS,
            note=note,
            origin_analysis_id=origin_analysis_id,
        )
        return asset_id

    def soft_delete_asset(self, asset_id: int) -> bool:
        return self.update_asset(asset_id, deleted_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def add_trade_history(
        self,
        asset_id: int,
        *,
        trade_type: str,
        trade_date: str,
        price: float,
        quantity: int,
        note: str = "",
        trade_source: str = "manual",
    ) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO asset_trade_history (
                asset_id, trade_date, trade_type, price, quantity, note, trade_source, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                trade_date,
                trade_type,
                float(price),
                int(quantity),
                note,
                trade_source,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        trade_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return trade_id

    def get_trade_history(self, asset_id: int, limit: int = 20) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM asset_trade_history
            WHERE asset_id = ?
            ORDER BY trade_date DESC, id DESC
            LIMIT ?
            """,
            (asset_id, limit),
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_trade(row) for row in rows]

    def get_open_position_cycle(self, asset_id: int) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM asset_position_cycles
            WHERE asset_id = ? AND status = 'open'
            ORDER BY datetime(opened_at) DESC, id DESC
            LIMIT 1
            """,
            (asset_id,),
        )
        row = cursor.fetchone()
        conn.close()
        return self._row_to_position_cycle(row) if row else None

    def list_position_cycles(self, asset_id: int, limit: int = 20) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM asset_position_cycles
            WHERE asset_id = ?
            ORDER BY datetime(opened_at) DESC, id DESC
            LIMIT ?
            """,
            (asset_id, max(1, int(limit or 20))),
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_position_cycle(row) for row in rows]

    def open_position_cycle(
        self,
        asset_id: int,
        *,
        opened_at: Optional[str] = None,
        opened_trade_date: Optional[str] = None,
        opened_trade_id: Optional[int] = None,
        baseline_source: Optional[str] = None,
        baseline_analysis_id: Optional[int] = None,
        baseline_decision_id: Optional[int] = None,
        swing_type: Optional[str] = None,
        swing_type_reason: Optional[str] = None,
        baseline_snapshot: Optional[Dict[str, Any]] = None,
        overwrite_baseline: bool = False,
    ) -> int:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        effective_opened_at = str(opened_at or now_text)
        normalized_swing_type = str(swing_type or "").strip()
        normalized_reason = str(swing_type_reason or "").strip()
        serialized_snapshot = (
            json.dumps(baseline_snapshot, ensure_ascii=False)
            if isinstance(baseline_snapshot, dict) and baseline_snapshot
            else None
        )

        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM asset_position_cycles
            WHERE asset_id = ? AND status = 'open'
            ORDER BY datetime(opened_at) DESC, id DESC
            LIMIT 1
            """,
            (asset_id,),
        )
        existing = cursor.fetchone()
        if existing:
            existing_cycle = dict(existing)
            updates: Dict[str, Any] = {}
            if opened_trade_id and existing_cycle.get("opened_trade_id") in (None, ""):
                updates["opened_trade_id"] = opened_trade_id
            if opened_trade_date and not existing_cycle.get("opened_trade_date"):
                updates["opened_trade_date"] = opened_trade_date
            if effective_opened_at and not existing_cycle.get("opened_at"):
                updates["opened_at"] = effective_opened_at
            can_write_baseline = overwrite_baseline or not str(existing_cycle.get("swing_type") or "").strip()
            if can_write_baseline:
                if normalized_swing_type:
                    updates["swing_type"] = normalized_swing_type
                if normalized_reason:
                    updates["swing_type_reason"] = normalized_reason
                if baseline_source:
                    updates["baseline_source"] = baseline_source
                if baseline_analysis_id not in (None, ""):
                    updates["baseline_analysis_id"] = baseline_analysis_id
                if baseline_decision_id not in (None, ""):
                    updates["baseline_decision_id"] = baseline_decision_id
                if serialized_snapshot:
                    updates["baseline_snapshot_json"] = serialized_snapshot
            if updates:
                updates["updated_at"] = now_text
                assignments = ", ".join(f"{column} = ?" for column in updates)
                values = list(updates.values())
                values.append(int(existing_cycle["id"]))
                cursor.execute(
                    f"UPDATE asset_position_cycles SET {assignments} WHERE id = ?",
                    tuple(values),
                )
                conn.commit()
            conn.close()
            return int(existing_cycle["id"])

        cursor.execute(
            """
            INSERT INTO asset_position_cycles (
                asset_id, status, opened_at, opened_trade_date, opened_trade_id,
                baseline_source, baseline_analysis_id, baseline_decision_id,
                swing_type, swing_type_reason, baseline_snapshot_json, created_at, updated_at
            )
            VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                effective_opened_at,
                opened_trade_date,
                opened_trade_id,
                baseline_source,
                baseline_analysis_id,
                baseline_decision_id,
                normalized_swing_type or None,
                normalized_reason or None,
                serialized_snapshot,
                now_text,
                now_text,
            ),
        )
        cycle_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return cycle_id

    def close_open_position_cycle(
        self,
        asset_id: int,
        *,
        closed_at: Optional[str] = None,
        closed_trade_date: Optional[str] = None,
        closed_trade_id: Optional[int] = None,
    ) -> bool:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        effective_closed_at = str(closed_at or now_text)
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE asset_position_cycles
            SET status = 'closed',
                closed_at = ?,
                closed_trade_date = COALESCE(?, closed_trade_date),
                closed_trade_id = COALESCE(?, closed_trade_id),
                updated_at = ?
            WHERE asset_id = ? AND status = 'open'
            """,
            (
                effective_closed_at,
                closed_trade_date,
                closed_trade_id,
                now_text,
                asset_id,
            ),
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def set_open_position_cycle_baseline(
        self,
        asset_id: int,
        *,
        swing_type: Optional[str],
        swing_type_reason: Optional[str] = None,
        holding_period: Optional[str] = None,
        baseline_source: Optional[str] = None,
        baseline_analysis_id: Optional[int] = None,
        baseline_decision_id: Optional[int] = None,
        overwrite: bool = False,
    ) -> bool:
        baseline_snapshot = {}
        if holding_period not in (None, ""):
            baseline_snapshot["holding_period"] = str(holding_period).strip()
        cycle_id = self.open_position_cycle(
            asset_id,
            baseline_source=baseline_source,
            baseline_analysis_id=baseline_analysis_id,
            baseline_decision_id=baseline_decision_id,
            swing_type=swing_type,
            swing_type_reason=swing_type_reason,
            baseline_snapshot=baseline_snapshot or None,
            overwrite_baseline=overwrite,
        )
        return bool(cycle_id)

    def get_trade_summary_map(self, asset_ids: Optional[List[int]] = None) -> Dict[int, Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        sql = [
            """
            SELECT
                asset_id,
                COUNT(*) AS trade_count,
                MAX(trade_date) AS last_trade_date
            FROM asset_trade_history
            """
        ]
        params: List[Any] = []
        if asset_ids:
            placeholders = ",".join("?" for _ in asset_ids)
            sql.append(f"WHERE asset_id IN ({placeholders})")
            params.extend(asset_ids)
        sql.append("GROUP BY asset_id")
        cursor.execute(" ".join(sql), tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return {
            int(row["asset_id"]): {
                "trade_count": int(row["trade_count"] or 0),
                "last_trade_date": row["last_trade_date"],
            }
            for row in rows
        }

    def replace_trade_history(
        self,
        asset_id: int,
        trades: List[Dict],
        *,
        final_status_when_flat: str = STATUS_RESEARCH,
        default_trade_source: str = "manual_fix",
    ) -> Dict:
        asset = self.get_asset(asset_id)
        if not asset:
            raise ValueError(f"未找到资产ID: {asset_id}")
        if trades is None:
            raise ValueError("trades 不能为空")
        if not isinstance(trades, list):
            raise ValueError("trades 必须是数组")

        staged: List[Dict] = []
        for index, trade in enumerate(trades):
            if not isinstance(trade, dict):
                raise ValueError(f"第 {index + 1} 条交易格式错误，必须是对象")

            normalized_type = self._normalize_trade_type(trade.get("trade_type"))
            if not normalized_type:
                raise ValueError(f"第 {index + 1} 条交易类型无效: {trade.get('trade_type')}")

            staged.append(
                {
                    "trade_date": self._normalize_trade_date_text(trade.get("trade_date")),
                    "trade_type": normalized_type,
                    "price": float(trade.get("price") or 0),
                    "quantity": int(trade.get("quantity") or 0),
                    "note": str(trade.get("note") or ""),
                    "trade_source": str(trade.get("trade_source") or default_trade_source).strip() or default_trade_source,
                    "input_order": index,
                }
            )

        staged.sort(key=lambda item: (item["trade_date"], int(item["input_order"])))

        replayed: List[Dict] = []
        running_quantity = 0
        running_cost = 0.0
        for index, trade in enumerate(staged):
            price = float(trade["price"])
            if price <= 0:
                raise ValueError(f"第 {index + 1} 条交易价格必须大于 0")

            trade_type = trade["trade_type"]
            quantity = int(trade["quantity"])
            if trade_type == "clear":
                if running_quantity <= 0:
                    raise ValueError(f"第 {index + 1} 条清仓交易无可用持仓数量")
                quantity = running_quantity
                persisted_type = "sell"
            else:
                if quantity <= 0:
                    raise ValueError(f"第 {index + 1} 条交易数量必须大于 0")
                persisted_type = trade_type

            if persisted_type == "buy":
                new_quantity = running_quantity + quantity
                running_cost = (
                    ((running_cost * running_quantity) + (price * quantity)) / new_quantity
                    if new_quantity > 0
                    else 0.0
                )
                running_quantity = new_quantity
            else:
                if quantity > running_quantity:
                    raise ValueError(
                        f"第 {index + 1} 条卖出数量超过当前持仓: 当前 {running_quantity}，卖出 {quantity}"
                    )
                remaining_quantity = running_quantity - quantity
                if remaining_quantity > 0:
                    # Selling should not reprice the remaining average cost basis.
                    if abs(running_cost) < 1e-12:
                        running_cost = 0.0
                else:
                    running_cost = 0.0
                running_quantity = remaining_quantity

            replayed.append(
                {
                    "trade_date": trade["trade_date"],
                    "trade_type": persisted_type,
                    "price": price,
                    "quantity": quantity,
                    "note": trade["note"],
                    "trade_source": trade["trade_source"],
                }
            )

        normalized_flat_status = self._normalize_flat_status(final_status_when_flat)
        final_status = STATUS_PORTFOLIO if running_quantity > 0 else normalized_flat_status
        final_cost_price = running_cost if running_quantity > 0 else None
        final_quantity = running_quantity if running_quantity > 0 else None
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_trade_at = (
            f"{replayed[-1]['trade_date']} 00:00:00"
            if replayed
            else (asset.get("last_trade_at") or now_text)
        )

        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN")
            cursor.execute("DELETE FROM asset_trade_history WHERE asset_id = ?", (asset_id,))
            for trade in replayed:
                cursor.execute(
                    """
                    INSERT INTO asset_trade_history (
                        asset_id, trade_date, trade_type, price, quantity, note, trade_source, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        asset_id,
                        trade["trade_date"],
                        trade["trade_type"],
                        trade["price"],
                        trade["quantity"],
                        trade["note"],
                        trade["trade_source"],
                        now_text,
                    ),
                )

            cursor.execute(
                """
                UPDATE assets
                SET
                    status = ?,
                    cost_price = ?,
                    quantity = ?,
                    last_trade_at = ?,
                    updated_at = ?
                WHERE id = ? AND deleted_at IS NULL
                """,
                (final_status, final_cost_price, final_quantity, last_trade_at, now_text, asset_id),
            )
            if cursor.rowcount <= 0:
                raise ValueError(f"未找到资产ID: {asset_id}")

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return {
            "asset_id": int(asset_id),
            "trade_count": len(replayed),
            "final_status": final_status,
            "final_quantity": int(final_quantity or 0),
            "final_cost_price": float(final_cost_price or 0.0),
            "last_trade_date": replayed[-1]["trade_date"] if replayed else None,
        }

    def create_pending_action(
        self,
        *,
        asset_id: int,
        action_type: str,
        origin_decision_id: Optional[int] = None,
        payload: Optional[Dict] = None,
        status: str = "pending",
    ) -> int:
        normalized_status = str(status or "pending").strip().lower()
        if normalized_status not in {"pending", "accepted", "rejected", "expired"}:
            normalized_status = "pending"
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO asset_action_queue (
                asset_id, action_type, origin_decision_id, status, payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                str(action_type or "").strip().lower(),
                origin_decision_id,
                normalized_status,
                json.dumps(payload or {}, ensure_ascii=False),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        action_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return action_id

    def list_pending_actions(
        self,
        *,
        status: Optional[str] = None,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        clauses = ["1 = 1", "a.deleted_at IS NULL"]
        params: List[Any] = []
        if status:
            clauses.append("q.status = ?")
            params.append(str(status).strip().lower())
        if account_name is not None:
            clauses.append("a.account_name = ?")
            params.append(self._normalize_account_name(account_name))
        if asset_id is not None:
            clauses.append("q.asset_id = ?")
            params.append(asset_id)
        sql = f"""
            SELECT
                q.*,
                a.account_name,
                a.symbol,
                a.name,
                a.status AS asset_status,
                a.cost_price,
                a.quantity
            FROM asset_action_queue q
            INNER JOIN assets a
                ON a.id = q.asset_id
            WHERE {' AND '.join(clauses)}
            ORDER BY datetime(q.created_at) DESC, q.id DESC
            LIMIT ?
        """
        params.append(limit)
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_action(row) for row in rows]

    def get_pending_action(self, action_id: int) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                q.*,
                a.account_name,
                a.symbol,
                a.name,
                a.status AS asset_status,
                a.cost_price,
                a.quantity
            FROM asset_action_queue q
            INNER JOIN assets a
                ON a.id = q.asset_id
            WHERE q.id = ? AND a.deleted_at IS NULL
            """,
            (action_id,),
        )
        row = cursor.fetchone()
        conn.close()
        return self._row_to_action(row) if row else None

    def update_pending_action(
        self,
        action_id: int,
        *,
        status: str,
        resolution_note: Optional[str] = None,
    ) -> bool:
        normalized_status = str(status or "").strip().lower()
        if normalized_status not in {"pending", "accepted", "rejected", "expired"}:
            return False
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE asset_action_queue
            SET status = ?, resolution_note = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                normalized_status,
                resolution_note,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                action_id,
            ),
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed


asset_repository = AssetRepository()
