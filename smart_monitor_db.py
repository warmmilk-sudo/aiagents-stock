import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

from analysis_repository import AnalysisRepository
from asset_repository import STATUS_PORTFOLIO, STATUS_WATCHLIST, AssetRepository
from asset_service import AssetService, asset_service
from investment_db_utils import DEFAULT_ACCOUNT_NAME, get_metadata, resolve_investment_db_path, set_metadata
from monitoring_repository import MonitoringRepository
from portfolio_db import PortfolioDB


class SmartMonitorDB:
    """Repository-backed smart monitor facade."""

    def __init__(self, db_file: str = "smart_monitor.db"):
        self.seed_db_file = db_file
        self.db_file = resolve_investment_db_path(db_file)
        self.legacy_db_file = db_file if os.path.abspath(db_file) != os.path.abspath(self.db_file) else "smart_monitor.db"
        self.logger = logging.getLogger(__name__)
        self.monitoring_repository = MonitoringRepository(self.db_file)
        self.portfolio_db = PortfolioDB(self.db_file)
        self.analysis_repository = AnalysisRepository(self.db_file, legacy_analysis_db_path="")
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

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn

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
                confidence INTEGER,
                reasoning TEXT,
                position_size_pct REAL,
                stop_loss_pct REAL,
                take_profit_pct REAL,
                risk_level TEXT,
                key_price_levels TEXT,
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
        self._ensure_column(cursor, "ai_decisions", "asset_id", "INTEGER")
        self._ensure_column(cursor, "ai_decisions", "execution_mode", "TEXT DEFAULT 'manual_only'")
        self._ensure_column(cursor, "ai_decisions", "action_status", "TEXT DEFAULT 'suggested'")
        conn.commit()
        conn.close()

    @staticmethod
    def _ensure_column(cursor, table: str, column: str, definition: str) -> None:
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
                    cursor.execute(
                        """
                        INSERT INTO ai_decisions (
                            stock_code, stock_name, decision_time, trading_session,
                            action, confidence, reasoning, position_size_pct, stop_loss_pct,
                            take_profit_pct, risk_level, key_price_levels, market_data,
                            account_info, executed, execution_result, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            decision.get("stock_code"),
                            decision.get("stock_name"),
                            decision.get("decision_time") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            decision.get("trading_session"),
                            decision.get("action"),
                            decision.get("confidence"),
                            decision.get("reasoning"),
                            decision.get("position_size_pct"),
                            decision.get("stop_loss_pct"),
                            decision.get("take_profit_pct"),
                            decision.get("risk_level"),
                            decision.get("key_price_levels"),
                            decision.get("market_data"),
                            decision.get("account_info"),
                            decision.get("executed", 0),
                            decision.get("execution_result"),
                            decision.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        ),
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
                            notification.get("created_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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

    @staticmethod
    def _seconds_to_interval_minutes(check_interval: Optional[int]) -> int:
        seconds = int(check_interval or 300)
        return max(1, (seconds + 59) // 60)

    def _task_config_from_data(self, task_data: Dict) -> Dict:
        return {
            "task_name": task_data.get("task_name"),
            "auto_trade": False,
            "position_size_pct": task_data.get("position_size_pct", 20),
            "stop_loss_pct": task_data.get("stop_loss_pct", 5),
            "take_profit_pct": task_data.get("take_profit_pct", 10),
            "qmt_account_id": task_data.get("qmt_account_id"),
            "notify_email": task_data.get("notify_email"),
            "notify_webhook": task_data.get("notify_webhook"),
            "position_date": task_data.get("position_date"),
        }

    def _item_to_task(self, item: Dict) -> Dict:
        config = item.get("config") or {}
        interval_minutes = int(item.get("interval_minutes") or 1)
        asset = self.asset_repository.get_asset(item.get("asset_id")) if item.get("asset_id") else None
        has_position = bool(asset and asset.get("status") == STATUS_PORTFOLIO and (asset.get("quantity") or 0) > 0)
        return {
            "id": item["id"],
            "task_name": config.get("task_name") or f"{item['symbol']} AI监控任务",
            "stock_code": item["symbol"],
            "stock_name": item.get("name"),
            "enabled": 1 if item.get("enabled", True) else 0,
            "check_interval": interval_minutes * 60,
            "auto_trade": 1 if config.get("auto_trade", False) else 0,
            "trading_hours_only": 1 if item.get("trading_hours_only", True) else 0,
            "position_size_pct": config.get("position_size_pct", 20),
            "stop_loss_pct": config.get("stop_loss_pct", 5),
            "take_profit_pct": config.get("take_profit_pct", 10),
            "qmt_account_id": config.get("qmt_account_id"),
            "notify_email": config.get("notify_email"),
            "notify_webhook": config.get("notify_webhook"),
            "has_position": 1 if has_position else 0,
            "position_cost": asset.get("cost_price") if asset else 0,
            "position_quantity": asset.get("quantity") if asset else 0,
            "position_date": config.get("position_date"),
            "managed_by_portfolio": 1 if item.get("managed_by_portfolio", False) else 0,
            "account_name": item.get("account_name") or DEFAULT_ACCOUNT_NAME,
            "asset_id": item.get("asset_id"),
            "asset_status": asset.get("status") if asset else None,
            "portfolio_stock_id": item.get("portfolio_stock_id"),
            "origin_analysis_id": item.get("origin_analysis_id"),
            "strategy_context": self.analysis_repository.get_latest_strategy_context(
                asset_id=item.get("asset_id"),
                symbol=item.get("symbol"),
                account_name=item.get("account_name") or DEFAULT_ACCOUNT_NAME,
            ) or {},
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
        }

    def add_monitor_task(self, task_data: Dict) -> int:
        stock_code = task_data.get("stock_code")
        if not stock_code:
            raise ValueError("stock_code 不能为空")
        account_name = task_data.get("account_name") or DEFAULT_ACCOUNT_NAME
        asset_id = task_data.get("asset_id")
        if asset_id is None:
            _, _, asset_id = self.asset_service.promote_to_watchlist(
                symbol=stock_code,
                stock_name=task_data.get("stock_name") or stock_code,
                account_name=account_name,
                note="",
                origin_analysis_id=task_data.get("origin_analysis_id"),
            )
        return self.monitoring_repository.create_item(
            {
                "symbol": stock_code,
                "name": task_data.get("stock_name"),
                "monitor_type": "ai_task",
                "source": "portfolio" if task_data.get("managed_by_portfolio") else "ai_monitor",
                "enabled": bool(task_data.get("enabled", 1)),
                "interval_minutes": self._seconds_to_interval_minutes(task_data.get("check_interval", 300)),
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

    def get_monitor_tasks(self, enabled_only: bool = True) -> List[Dict]:
        items = self.monitoring_repository.list_items(monitor_type="ai_task", enabled_only=enabled_only)
        return [self._item_to_task(item) for item in items]

    def update_monitor_task(self, stock_code: str, task_data: Dict):
        account_name = task_data.get("account_name")
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
            updates["account_name"] = task_data.get("account_name") or DEFAULT_ACCOUNT_NAME
        if "asset_id" in task_data:
            updates["asset_id"] = task_data.get("asset_id")
        if "portfolio_stock_id" in task_data:
            updates["portfolio_stock_id"] = task_data.get("portfolio_stock_id")
        if "origin_analysis_id" in task_data:
            updates["origin_analysis_id"] = task_data.get("origin_analysis_id")

        tracked_keys = {
            "task_name",
            "position_size_pct",
            "stop_loss_pct",
            "take_profit_pct",
            "qmt_account_id",
            "notify_email",
            "notify_webhook",
            "position_date",
        }
        if any(key in task_data for key in tracked_keys):
            for key in tracked_keys:
                if key in task_data:
                    config[key] = config_updates[key]
            updates["config"] = config

        if not updates:
            return False
        return self.monitoring_repository.update_item(item["id"], updates)

    def set_all_monitor_tasks_enabled(self, enabled: bool) -> int:
        changed_count = 0
        target_enabled = bool(enabled)
        for item in self.monitoring_repository.list_items(monitor_type="ai_task"):
            if bool(item.get("enabled", True)) == target_enabled:
                continue
            if self.monitoring_repository.update_item(item["id"], {"enabled": target_enabled}):
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
        account_name = task_data.get("account_name") or DEFAULT_ACCOUNT_NAME
        asset_id = task_data.get("asset_id")
        if asset_id is None:
            _, _, asset_id = self.asset_service.promote_to_watchlist(
                symbol=stock_code,
                stock_name=task_data.get("stock_name") or stock_code,
                account_name=account_name,
                note="",
                origin_analysis_id=task_data.get("origin_analysis_id"),
            )
        existing = self.monitoring_repository.get_item_by_symbol(
            stock_code,
            monitor_type="ai_task",
            account_name=account_name,
            asset_id=asset_id,
        )
        if managed_sync and existing and not existing.get("managed_by_portfolio"):
            self.logger.info(f"跳过持仓同步任务 {stock_code}，同账户下手工任务已存在")
            return int(existing["id"])

        return self.monitoring_repository.upsert_item(
            {
                "symbol": stock_code,
                "name": task_data.get("stock_name"),
                "monitor_type": "ai_task",
                "source": "portfolio" if managed_sync else "ai_monitor",
                "enabled": bool(task_data.get("enabled", 1)),
                "interval_minutes": self._seconds_to_interval_minutes(task_data.get("check_interval", 300)),
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

    def delete_monitor_task(self, task_id: int):
        return self.monitoring_repository.delete_item(task_id)

    def delete_monitor_task_by_code(
        self,
        stock_code: str,
        managed_only: bool = False,
        account_name: Optional[str] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> bool:
        return self.monitoring_repository.delete_by_symbol(
            stock_code,
            monitor_type="ai_task",
            managed_only=managed_only,
            account_name=account_name,
            portfolio_stock_id=portfolio_stock_id,
        )

    def save_ai_decision(self, decision_data: Dict) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ai_decisions (
                stock_code, stock_name, account_name, asset_id, portfolio_stock_id, origin_analysis_id,
                decision_time, trading_session, action, confidence, reasoning, position_size_pct,
                stop_loss_pct, take_profit_pct, risk_level, key_price_levels, market_data,
                account_info, execution_mode, action_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_data.get("stock_code"),
                decision_data.get("stock_name"),
                decision_data.get("account_name"),
                decision_data.get("asset_id"),
                decision_data.get("portfolio_stock_id"),
                decision_data.get("origin_analysis_id"),
                decision_data.get("decision_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                decision_data.get("trading_session"),
                decision_data.get("action"),
                decision_data.get("confidence"),
                decision_data.get("reasoning"),
                decision_data.get("position_size_pct"),
                decision_data.get("stop_loss_pct"),
                decision_data.get("take_profit_pct"),
                decision_data.get("risk_level"),
                json.dumps(decision_data.get("key_price_levels", {}), ensure_ascii=False),
                json.dumps(decision_data.get("market_data", {}), ensure_ascii=False),
                json.dumps(decision_data.get("account_info", {}), ensure_ascii=False),
                decision_data.get("execution_mode", "manual_only"),
                decision_data.get("action_status", "suggested"),
            ),
        )
        record_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return record_id

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
        decisions = []
        for row in rows:
            decision = dict(row)
            decision["key_price_levels"] = json.loads(decision["key_price_levels"]) if decision.get("key_price_levels") else {}
            decision["market_data"] = json.loads(decision["market_data"]) if decision.get("market_data") else {}
            decision["account_info"] = json.loads(decision["account_info"]) if decision.get("account_info") else {}
            decisions.append(decision)
        return decisions

    def update_decision_execution(self, decision_id: int, executed: bool, result: str):
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
        conn.commit()
        conn.close()

    def save_trade_record(self, trade_data: Dict) -> int:
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
        return int((result[2] or {}).get("id") or 0) if result[0] else 0

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

        if stock_code and self.legacy_db_file and os.path.exists(self.legacy_db_file):
            legacy_conn = sqlite3.connect(self.legacy_db_file)
            legacy_conn.row_factory = sqlite3.Row
            legacy_cursor = legacy_conn.cursor()
            legacy_cursor.execute(
                """
                SELECT * FROM trade_records
                WHERE stock_code = ?
                ORDER BY datetime(trade_time) DESC, id DESC
                LIMIT ?
                """,
                (stock_code, limit),
            )
            legacy_rows = legacy_cursor.fetchall()
            legacy_conn.close()
            return [dict(row) for row in legacy_rows]
        return []

    def save_position(self, position_data: Dict):
        success, _, stock_id = self.asset_service.promote_to_portfolio(
            symbol=position_data.get("stock_code"),
            stock_name=position_data.get("stock_name") or position_data.get("stock_code"),
            account_name=position_data.get("account_name") or DEFAULT_ACCOUNT_NAME,
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
                    "account_name": stock.get("account_name") or DEFAULT_ACCOUNT_NAME,
                }
            )
        if positions:
            return positions

        if self.legacy_db_file and os.path.exists(self.legacy_db_file):
            legacy_conn = sqlite3.connect(self.legacy_db_file)
            legacy_conn.row_factory = sqlite3.Row
            legacy_cursor = legacy_conn.cursor()
            legacy_cursor.execute('SELECT * FROM position_monitor WHERE status = "holding" ORDER BY id DESC')
            rows = legacy_cursor.fetchall()
            legacy_conn.close()
            return [dict(row) for row in rows]
        return []

    def close_position(self, stock_code: str, account_name: str = DEFAULT_ACCOUNT_NAME):
        stock = self.asset_repository.get_asset_by_symbol(stock_code, account_name)
        if not stock:
            return False
        return self.asset_service.clear_position_to_watchlist(
            stock["id"],
            note="手动清仓",
            last_trade_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

    def save_notification(self, notify_data: Dict) -> int:
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

    def update_notification_status(self, notify_id: int, status: str, error_msg: str = None):
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
        return self.asset_repository.update_pending_action(
            action_id,
            status=status,
            resolution_note=resolution_note,
        )
