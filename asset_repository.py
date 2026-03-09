from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from investment_db_utils import (
    DEFAULT_ACCOUNT_NAME,
    connect_sqlite,
    get_metadata,
    resolve_investment_db_path,
    set_metadata,
)


STATUS_RESEARCH = "research"
STATUS_WATCHLIST = "watchlist"
STATUS_PORTFOLIO = "portfolio"
ASSET_STATUSES = {STATUS_RESEARCH, STATUS_WATCHLIST, STATUS_PORTFOLIO}
STATUS_PRIORITY = {
    STATUS_RESEARCH: 0,
    STATUS_WATCHLIST: 1,
    STATUS_PORTFOLIO: 2,
}


class AssetRepository:
    """Canonical storage for the investment lifecycle domain."""

    def __init__(self, db_path: str = "investment.db"):
        self.seed_db_path = db_path
        self.db_path = resolve_investment_db_path(db_path)
        self._init_database()
        self._migrate_existing_canonical_tables()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _init_database(self) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK(status IN ('research', 'watchlist', 'portfolio')),
                cost_price REAL,
                quantity INTEGER,
                note TEXT,
                monitor_enabled INTEGER NOT NULL DEFAULT 1,
                origin_analysis_id INTEGER,
                last_trade_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_active_account_symbol
            ON assets(account_name, symbol)
            WHERE deleted_at IS NULL
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_assets_status_account
            ON assets(status, account_name, datetime(updated_at) DESC, id DESC)
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
        conn.commit()
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
        if normalized not in ASSET_STATUSES:
            return fallback
        return normalized

    @staticmethod
    def _normalize_account_name(account_name: Optional[str]) -> str:
        normalized = str(account_name or "").strip()
        return normalized or DEFAULT_ACCOUNT_NAME

    @staticmethod
    def _normalize_symbol(symbol: Optional[str]) -> str:
        return str(symbol or "").strip().upper()

    def _row_to_asset(self, row: sqlite3.Row) -> Dict:
        asset = dict(row)
        asset["monitor_enabled"] = bool(asset.get("monitor_enabled", 1))
        asset["code"] = asset.get("symbol")
        asset["auto_monitor"] = bool(asset.get("monitor_enabled", 1))
        asset["position_status"] = "active" if asset.get("status") == STATUS_PORTFOLIO else asset.get("status")
        return asset

    @staticmethod
    def _row_to_trade(row: sqlite3.Row) -> Dict:
        return dict(row)

    def _row_to_action(self, row: sqlite3.Row) -> Dict:
        action = dict(row)
        action["payload"] = self._safe_json_loads(action.pop("payload_json", None), {})
        return action

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None

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
            if merged_status == STATUS_PORTFOLIO:
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
            cost_price if target_status == STATUS_PORTFOLIO else None,
            quantity if target_status == STATUS_PORTFOLIO else None,
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
                    status=STATUS_PORTFOLIO if (stock.get("position_status") or "active") == "active" else STATUS_WATCHLIST,
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
                    status=STATUS_WATCHLIST,
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
                status = STATUS_PORTFOLIO if record.get("analysis_scope") == STATUS_PORTFOLIO else STATUS_RESEARCH
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
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO asset_trade_history (
                        id, asset_id, trade_date, trade_type, price, quantity, note, trade_source, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade.get("id"),
                        trade.get("portfolio_stock_id"),
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
            WHERE symbol = ? AND account_name = ? AND deleted_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (self._normalize_symbol(symbol), self._normalize_account_name(account_name)),
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
        if account_name is not None:
            clauses.append("account_name = ?")
            params.append(self._normalize_account_name(account_name))
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
        account_name = self._normalize_account_name(account_name)
        existing = self.get_asset_by_symbol(symbol, account_name)
        if existing:
            self.update_asset(
                existing["id"],
                name=name or existing.get("name") or symbol,
                note=note or existing.get("note"),
                monitor_enabled=monitor_enabled if monitor_enabled is not None else existing.get("monitor_enabled", True),
                origin_analysis_id=origin_analysis_id or existing.get("origin_analysis_id"),
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
                account_name,
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
            fields.append(f"{key} = ?")
            values.append(value)

        if status:
            if status != STATUS_PORTFOLIO:
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
        }
        if target_status == STATUS_PORTFOLIO:
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
                STATUS_WATCHLIST,
                note=note,
                last_trade_at=last_trade_at,
            )
        effective_cost = float(cost_price or 0)
        if effective_cost <= 0:
            raise ValueError("portfolio 资产必须提供正数成本价")
        return self.update_asset(
            asset_id,
            status=STATUS_PORTFOLIO,
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
        account_name = self._normalize_account_name(account_name)
        existing = self.get_asset_by_symbol(symbol, account_name)
        if existing:
            if existing.get("status") == STATUS_PORTFOLIO:
                return int(existing["id"])
            self.update_asset(
                existing["id"],
                name=name or existing.get("name") or symbol,
                status=STATUS_WATCHLIST,
                note=note or existing.get("note"),
                monitor_enabled=monitor_enabled if monitor_enabled is not None else existing.get("monitor_enabled", True),
                origin_analysis_id=origin_analysis_id or existing.get("origin_analysis_id"),
            )
            return int(existing["id"])
        asset_id = self.create_or_update_research_asset(
            symbol=symbol,
            name=name,
            account_name=account_name,
            note=note,
            origin_analysis_id=origin_analysis_id,
            monitor_enabled=monitor_enabled,
        )
        self.transition_asset_status(
            asset_id,
            STATUS_WATCHLIST,
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
