import json
import math
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional


def resolve_monitoring_db_path(seed_db_path: str) -> str:
    """Resolve the canonical monitoring database path from any legacy DB path."""
    directory = os.path.dirname(seed_db_path) if seed_db_path else ""
    return os.path.join(directory, "monitoring.db") if directory else "monitoring.db"


class MonitoringRepository:
    """Canonical monitoring storage for AI tasks and price alerts."""

    def __init__(self, db_path: str = "monitoring.db"):
        self.db_path = db_path
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        self._init_database()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_database(self) -> None:
        conn = self._connect()
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
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_ai_task_symbol
            ON monitoring_items(symbol)
            WHERE monitor_type = 'ai_task'
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_managed_alert_symbol
            ON monitoring_items(symbol)
            WHERE monitor_type = 'price_alert' AND managed_by_portfolio = 1
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
        conn.commit()
        conn.close()

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

    def _row_to_item(self, row: sqlite3.Row) -> Dict:
        data = dict(row)
        data["enabled"] = bool(data.get("enabled", 1))
        data["trading_hours_only"] = bool(data.get("trading_hours_only", 1))
        data["notification_enabled"] = bool(data.get("notification_enabled", 1))
        data["managed_by_portfolio"] = bool(data.get("managed_by_portfolio", 0))
        data["config"] = self._safe_json_loads(data.get("config_json"), {})
        return data

    def _set_metadata(self, key: str, value: str) -> None:
        conn = self._connect()
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
        conn.close()

    def _get_metadata(self, key: str) -> Optional[str]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT meta_value FROM monitoring_metadata WHERE meta_key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        return row["meta_value"] if row else None

    def create_item(self, item_data: Dict) -> int:
        config = dict(item_data.get("config") or {})
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO monitoring_items (
                symbol, name, monitor_type, source, enabled, interval_minutes,
                trading_hours_only, notification_enabled, managed_by_portfolio,
                current_price, last_checked, last_status, last_message, config_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_data["symbol"],
                item_data.get("name") or item_data["symbol"],
                item_data["monitor_type"],
                item_data.get("source", "manual"),
                1 if item_data.get("enabled", True) else 0,
                self._normalize_interval_minutes(item_data.get("interval_minutes", 30)),
                1 if item_data.get("trading_hours_only", True) else 0,
                1 if item_data.get("notification_enabled", True) else 0,
                1 if item_data.get("managed_by_portfolio", False) else 0,
                item_data.get("current_price"),
                item_data.get("last_checked"),
                item_data.get("last_status"),
                item_data.get("last_message"),
                json.dumps(config, ensure_ascii=False),
            ),
        )
        item_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return item_id

    def update_item(self, item_id: int, updates: Dict) -> bool:
        if not updates:
            return False

        fields: List[str] = []
        values: List[object] = []
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
            fields.append(f"{key} = ?")
            values.append(value)

        if "config" in updates:
            fields.append("config_json = ?")
            values.append(json.dumps(updates["config"] or {}, ensure_ascii=False))

        if not fields:
            return False

        fields.append("updated_at = CURRENT_TIMESTAMP")
        values.append(item_id)
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE monitoring_items SET {', '.join(fields)} WHERE id = ?",
            tuple(values),
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

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
    ) -> Optional[Dict]:
        items = self.list_items(
            monitor_type=monitor_type,
            symbol=symbol,
            managed_by_portfolio=managed_only,
        )
        return items[0] if items else None

    def list_items(
        self,
        monitor_type: Optional[str] = None,
        source: Optional[str] = None,
        managed_by_portfolio: Optional[bool] = None,
        enabled_only: bool = False,
        symbol: Optional[str] = None,
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

        sql = "SELECT * FROM monitoring_items"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY datetime(created_at) DESC, id DESC"

        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_item(row) for row in rows]

    def upsert_item(self, item_data: Dict) -> int:
        monitor_type = item_data["monitor_type"]
        symbol = item_data["symbol"]
        managed = bool(item_data.get("managed_by_portfolio", False))

        existing = None
        if monitor_type == "ai_task":
            existing = self.get_item_by_symbol(symbol, monitor_type="ai_task")
        elif monitor_type == "price_alert" and managed:
            existing = self.get_item_by_symbol(
                symbol,
                monitor_type="price_alert",
                managed_only=True,
            )

        if not existing:
            return self.create_item(item_data)

        merged_config = dict(existing.get("config") or {})
        merged_config.update(item_data.get("config") or {})
        updates = {
            "name": item_data.get("name", existing["name"]),
            "source": item_data.get("source", existing.get("source", "manual")),
            "enabled": item_data.get("enabled", existing["enabled"]),
            "interval_minutes": item_data.get("interval_minutes", existing["interval_minutes"]),
            "trading_hours_only": item_data.get("trading_hours_only", existing["trading_hours_only"]),
            "notification_enabled": item_data.get("notification_enabled", existing["notification_enabled"]),
            "managed_by_portfolio": managed,
            "current_price": item_data.get("current_price", existing.get("current_price")),
            "last_checked": item_data.get("last_checked", existing.get("last_checked")),
            "last_status": item_data.get("last_status", existing.get("last_status")),
            "last_message": item_data.get("last_message", existing.get("last_message")),
            "config": merged_config,
        }
        self.update_item(existing["id"], updates)
        return int(existing["id"])

    def delete_item(self, item_id: int) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM monitoring_price_history WHERE monitoring_item_id = ?", (item_id,))
        cursor.execute("DELETE FROM monitoring_events WHERE monitoring_item_id = ?", (item_id,))
        cursor.execute("DELETE FROM monitoring_items WHERE id = ?", (item_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def delete_by_symbol(
        self,
        symbol: str,
        monitor_type: Optional[str] = None,
        managed_only: bool = False,
    ) -> bool:
        items = self.list_items(
            monitor_type=monitor_type,
            symbol=symbol,
            managed_by_portfolio=True if managed_only else None,
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
        item = self.get_item(item_id)
        if not item:
            return False

        effective_checked = last_checked or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO monitoring_price_history (monitoring_item_id, price)
                VALUES (?, ?)
                """,
                (item_id, current_price),
            )
            conn.commit()
            conn.close()
        return updated

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
        details: Optional[Dict] = None,
        created_at: Optional[str] = None,
    ) -> int:
        item = self.get_item(item_id) if item_id else None
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO monitoring_events (
                monitoring_item_id, symbol, name, monitor_type, event_type,
                message, details_json, notification_pending, sent, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                0,
                created_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        event_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return event_id

    def get_pending_notifications(self) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, monitoring_item_id, symbol, name, event_type, message, created_at
            FROM monitoring_events
            WHERE notification_pending = 1 AND sent = 0
            ORDER BY datetime(created_at) ASC, id ASC
            """
        )
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                "id": row["id"],
                "stock_id": row["monitoring_item_id"],
                "symbol": row["symbol"],
                "name": row["name"] or row["symbol"],
                "type": row["event_type"],
                "message": row["message"],
                "triggered_at": row["created_at"],
            }
            for row in rows
        ]

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
            SELECT id, monitoring_item_id, symbol, name, event_type, message, created_at, sent
            FROM monitoring_events
            WHERE notification_pending = 1
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                "id": row["id"],
                "stock_id": row["monitoring_item_id"],
                "symbol": row["symbol"],
                "name": row["name"] or row["symbol"],
                "type": row["event_type"],
                "message": row["message"],
                "triggered_at": row["created_at"],
                "sent": bool(row["sent"]),
            }
            for row in rows
        ]

    def mark_notification_sent(self, event_id: int) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("UPDATE monitoring_events SET sent = 1 WHERE id = ?", (event_id,))
        conn.commit()
        conn.close()

    def mark_all_notifications_sent(self) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("UPDATE monitoring_events SET sent = 1 WHERE notification_pending = 1 AND sent = 0")
        changed = cursor.rowcount
        conn.commit()
        conn.close()
        return changed

    def clear_all_notifications(self) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM monitoring_events WHERE notification_pending = 1")
        changed = cursor.rowcount
        conn.commit()
        conn.close()
        return changed

    def has_recent_notification(self, item_id: int, event_type: str, minutes: int = 60) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM monitoring_events
            WHERE monitoring_item_id = ? AND event_type = ?
              AND notification_pending = 1
              AND datetime(created_at) > datetime('now', '-' || ? || ' minutes')
            """,
            (item_id, event_type, minutes),
        )
        count = int(cursor.fetchone()[0])
        conn.close()
        return count > 0

    @staticmethod
    def _build_smart_migration_key(db_path: str) -> str:
        return f"migrated_smart::{os.path.abspath(db_path)}"

    @staticmethod
    def _build_stock_migration_key(db_path: str) -> str:
        return f"migrated_stock::{os.path.abspath(db_path)}"

    def migrate_legacy_smart_db(self, legacy_db_path: str) -> int:
        if not legacy_db_path or not os.path.exists(legacy_db_path):
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
            config = {
                "task_name": task.get("task_name"),
                "auto_trade": bool(task.get("auto_trade", 0)),
                "position_size_pct": task.get("position_size_pct", 20),
                "stop_loss_pct": task.get("stop_loss_pct", 5),
                "take_profit_pct": task.get("take_profit_pct", 10),
                "qmt_account_id": task.get("qmt_account_id"),
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
                    "source": "portfolio" if task.get("managed_by_portfolio") else "manual",
                    "enabled": bool(task.get("enabled", 1)),
                    "interval_minutes": max(1, int(math.ceil((task.get("check_interval") or 60) / 60))),
                    "trading_hours_only": bool(task.get("trading_hours_only", 1)),
                    "notification_enabled": True,
                    "managed_by_portfolio": bool(task.get("managed_by_portfolio", 0)),
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
            config = {
                "rating": stock.get("rating", "持有"),
                "entry_range": self._safe_json_loads(stock.get("entry_range"), {}),
                "take_profit": stock.get("take_profit"),
                "stop_loss": stock.get("stop_loss"),
                "quant_enabled": bool(stock.get("quant_enabled", 0)),
                "quant_config": self._safe_json_loads(stock.get("quant_config"), {}),
            }
            item_data = {
                "symbol": stock["symbol"],
                "name": stock.get("name") or stock["symbol"],
                "monitor_type": "price_alert",
                "source": "portfolio" if stock.get("managed_by_portfolio") else "manual",
                "enabled": True,
                "interval_minutes": self._normalize_interval_minutes(stock.get("check_interval", 30)),
                "trading_hours_only": bool(stock.get("trading_hours_only", 1)),
                "notification_enabled": bool(stock.get("notification_enabled", 1)),
                "managed_by_portfolio": bool(stock.get("managed_by_portfolio", 0)),
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

