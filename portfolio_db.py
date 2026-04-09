"""
持仓股票数据库管理模块

提供持仓股票和分析历史的数据库操作接口
"""

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import os

from analysis_repository import AnalysisRepository
from asset_repository import STATUS_PORTFOLIO, STATUS_RESEARCH, STATUS_WATCHLIST, AssetRepository
from investment_db_utils import (
    DEFAULT_ACCOUNT_NAME,
    cleanup_single_account_data,
    connect_sqlite,
    get_metadata,
    normalize_account_name,
    resolve_investment_db_path,
    set_metadata,
)

# 数据库文件路径
DB_PATH = "investment.db"


class PortfolioDB:
    """持仓股票数据库管理类"""

    SINGLE_ACCOUNT_CLEANUP_KEY = "portfolio_single_account_cleanup_v1"
    SNAPSHOT_ACCOUNT_NORMALIZATION_KEY = "portfolio_snapshot_account_normalization_v1"
    ACCOUNT_TOTAL_ASSETS_KEY = "portfolio_account_total_assets_v1"
    
    def __init__(self, db_path: str = DB_PATH):
        """
        初始化数据库连接
        
        Args:
            db_path: 数据库文件路径
        """
        self.seed_db_path = db_path
        self.db_path = resolve_investment_db_path(db_path)
        self.analysis_repository = AnalysisRepository(self.db_path, legacy_analysis_db_path="")
        self.asset_repository = AssetRepository(self.db_path)
        self._init_database()
        self._migrate_legacy_db(db_path)
        if os.path.abspath(self.db_path) == os.path.abspath(resolve_investment_db_path(db_path)):
            self._migrate_legacy_db("portfolio_stocks.db")
        self._cleanup_single_account_data()
        self._normalize_snapshot_account_names()
    
    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        return connect_sqlite(self.db_path)
    
    def _ensure_column(self, cursor, table: str, column: str, definition: str):
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @staticmethod
    def _normalize_account_name_value(
        account_name: Optional[object],
        *,
        allow_aggregate: bool = False,
    ) -> str:
        return str(normalize_account_name(account_name, allow_aggregate=allow_aggregate))

    def _serialize_json(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    def _cleanup_single_account_data(self) -> Dict[str, int]:
        conn = self._get_connection()
        try:
            cleaned = cleanup_single_account_data(
                conn,
                default_account_name=DEFAULT_ACCOUNT_NAME,
                metadata_key=self.SINGLE_ACCOUNT_CLEANUP_KEY,
            )
            conn.commit()
            return cleaned
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _deserialize_json_object(self, raw_value) -> Dict:
        if not raw_value:
            return {}

        if isinstance(raw_value, dict):
            return raw_value

        if isinstance(raw_value, str):
            try:
                parsed = json.loads(raw_value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}

        return {}

    def _deserialize_flexible_value(self, raw_value, default=""):
        if raw_value in (None, ""):
            return default

        if not isinstance(raw_value, str):
            return raw_value

        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            return raw_value

    @staticmethod
    def _normalize_total_assets_value(value: Any) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            numeric = 0.0
        return round(max(0.0, numeric), 2)

    @staticmethod
    def _normalize_stock_name_text(value: Any) -> str:
        return str(value or "").strip()

    @classmethod
    def _is_valid_stock_name(cls, name: Any, code: Any) -> bool:
        normalized_name = cls._normalize_stock_name_text(name)
        normalized_code = str(code or "").strip().upper()
        if not normalized_name:
            return False

        invalid_names = {
            "",
            "N/A",
            "未知",
            f"股票{normalized_code}",
            f"港股{normalized_code}",
            f"美股{normalized_code}",
        }
        return normalized_name not in invalid_names and normalized_name.upper() != normalized_code

    def _resolve_stock_display_name(self, stock: Dict[str, Any], latest_record: Optional[Dict[str, Any]] = None) -> str:
        code = stock.get("code") or stock.get("symbol") or ""
        latest_record = latest_record or {}
        latest_stock_info = latest_record.get("stock_info") if isinstance(latest_record, dict) else {}
        if not isinstance(latest_stock_info, dict):
            latest_stock_info = {}

        candidates = (
            stock.get("name"),
            latest_record.get("stock_name"),
            latest_stock_info.get("name"),
            latest_stock_info.get("股票名称"),
        )
        for candidate in candidates:
            if self._is_valid_stock_name(candidate, code):
                return self._normalize_stock_name_text(candidate)
        return self._normalize_stock_name_text(code)

    def _deserialize_analysis_row(self, row) -> Dict:
        record = dict(row)
        record["stock_info"] = self._deserialize_json_object(record.pop("stock_info_json", None))
        record["agents_results"] = self._deserialize_json_object(record.pop("agents_results_json", None))
        record["final_decision"] = self._deserialize_json_object(record.pop("final_decision_json", None))
        record["discussion_result"] = self._deserialize_flexible_value(record.get("discussion_result"), default="")
        record["has_full_report"] = bool(record.get("has_full_report"))
        return record

    def _deserialize_snapshot_row(self, row) -> Dict:
        record = dict(row)
        holdings = self._deserialize_flexible_value(record.get("holdings_json"), default=[])
        record["holdings"] = holdings if isinstance(holdings, list) else []
        record.pop("holdings_json", None)
        return record

    def _deserialize_trade_row(self, row) -> Dict:
        return dict(row)

    def _normalize_snapshot_account_names(self) -> int:
        conn = self._get_connection()
        try:
            if get_metadata(conn, self.SNAPSHOT_ACCOUNT_NORMALIZATION_KEY):
                return 0
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM portfolio_daily_snapshots ORDER BY snapshot_date ASC, id ASC")
            rows = [dict(row) for row in cursor.fetchall()]
            grouped_rows: Dict[tuple[str, str], List[Dict]] = {}
            for row in rows:
                normalized_account_name = self._normalize_account_name_value(
                    row.get("account_name"),
                    allow_aggregate=True,
                )
                grouped_rows.setdefault(
                    (normalized_account_name, str(row.get("snapshot_date") or "").strip()),
                    [],
                ).append(row)

            normalized_rows = 0
            merged_rows = 0
            for (normalized_account_name, snapshot_date), snapshot_rows in grouped_rows.items():
                ordered_rows = sorted(
                    snapshot_rows,
                    key=lambda item: (
                        str(item.get("updated_at") or ""),
                        int(item.get("id") or 0),
                    ),
                    reverse=True,
                )
                target_row = ordered_rows[0]
                merged_holdings: List[Dict] = []
                total_market_value = 0.0
                total_cost_value = 0.0
                total_pnl = 0.0
                for row in ordered_rows:
                    total_market_value += float(row.get("total_market_value") or 0)
                    total_cost_value += float(row.get("total_cost_value") or 0)
                    total_pnl += float(row.get("total_pnl") or 0)
                    for holding in self._deserialize_flexible_value(row.get("holdings_json"), default=[]):
                        if not isinstance(holding, dict):
                            continue
                        normalized_holding = dict(holding)
                        normalized_holding["account_name"] = normalized_account_name
                        merged_holdings.append(normalized_holding)
                cursor.execute(
                    """
                    UPDATE portfolio_daily_snapshots
                    SET account_name = ?,
                        total_market_value = ?,
                        total_cost_value = ?,
                        total_pnl = ?,
                        holdings_json = ?,
                        data_source = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        normalized_account_name,
                        total_market_value,
                        total_cost_value,
                        total_pnl,
                        self._serialize_json(merged_holdings),
                        target_row.get("data_source") or "manual",
                        datetime.now(),
                        int(target_row["id"]),
                    ),
                )
                normalized_rows += 1 if cursor.rowcount > 0 else 0
                for duplicate_row in ordered_rows[1:]:
                    cursor.execute(
                        "DELETE FROM portfolio_daily_snapshots WHERE id = ?",
                        (int(duplicate_row["id"]),),
                    )
                    merged_rows += 1 if cursor.rowcount > 0 else 0
            set_metadata(
                conn,
                self.SNAPSHOT_ACCOUNT_NORMALIZATION_KEY,
                json.dumps(
                    {
                        "normalized_rows": normalized_rows,
                        "merged_rows": merged_rows,
                        "updated_at": datetime.now().isoformat(),
                    },
                    ensure_ascii=False,
                ),
            )
            conn.commit()
            return normalized_rows + merged_rows
        finally:
            conn.close()

    def _migrate_legacy_db(self, legacy_db_path: str) -> int:
        if not legacy_db_path or not os.path.exists(legacy_db_path):
            return 0
        if os.path.abspath(legacy_db_path) == os.path.abspath(self.db_path):
            return 0

        conn = self._get_connection()
        migration_key = f"migrated_portfolio_state::{os.path.abspath(legacy_db_path)}"
        if get_metadata(conn, migration_key):
            conn.close()
            self.analysis_repository.migrate_legacy_portfolio_db(legacy_db_path)
            return 0
        conn.close()

        legacy_conn = sqlite3.connect(legacy_db_path)
        legacy_conn.row_factory = sqlite3.Row
        legacy_cursor = legacy_conn.cursor()
        migrated_rows = 0

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            legacy_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            legacy_tables = {row["name"] for row in legacy_cursor.fetchall()}

            if "portfolio_stocks" in legacy_tables:
                legacy_cursor.execute("SELECT * FROM portfolio_stocks ORDER BY id ASC")
                for row in legacy_cursor.fetchall():
                    stock = dict(row)
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO portfolio_stocks (
                            id, account_name, code, name, cost_price, quantity, note, auto_monitor,
                            position_status, origin_analysis_id, last_trade_at, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            stock.get("id"),
                            self._normalize_account_name_value(stock.get("account_name")),
                            stock.get("code"),
                            stock.get("name"),
                            stock.get("cost_price"),
                            stock.get("quantity"),
                            stock.get("note"),
                            stock.get("auto_monitor", 1),
                            stock.get("position_status", "active"),
                            stock.get("origin_analysis_id"),
                            stock.get("last_trade_at"),
                            stock.get("created_at") or datetime.now(),
                            stock.get("updated_at") or datetime.now(),
                        ),
                    )
                    migrated_rows += 1

            if "portfolio_trade_history" in legacy_tables:
                legacy_cursor.execute("SELECT * FROM portfolio_trade_history ORDER BY id ASC")
                for row in legacy_cursor.fetchall():
                    trade = dict(row)
                    portfolio_stock_id = trade.get("portfolio_stock_id")
                    if portfolio_stock_id is not None:
                        cursor.execute(
                            "SELECT 1 FROM portfolio_stocks WHERE id = ? LIMIT 1",
                            (portfolio_stock_id,),
                        )
                        if cursor.fetchone() is None:
                            continue
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO portfolio_trade_history (
                            id, portfolio_stock_id, trade_date, trade_type, price, quantity,
                            note, trade_source, created_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            trade.get("id"),
                            portfolio_stock_id,
                            trade.get("trade_date"),
                            trade.get("trade_type"),
                            trade.get("price"),
                            trade.get("quantity"),
                            trade.get("note"),
                            trade.get("trade_source", "manual"),
                            trade.get("created_at") or datetime.now(),
                        ),
                    )

            if "portfolio_daily_snapshots" in legacy_tables:
                legacy_cursor.execute("SELECT * FROM portfolio_daily_snapshots ORDER BY id ASC")
                for row in legacy_cursor.fetchall():
                    snapshot = dict(row)
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO portfolio_daily_snapshots (
                            id, account_name, snapshot_date, total_market_value, total_cost_value,
                            total_pnl, holdings_json, data_source, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot.get("id"),
                            self._normalize_account_name_value(
                                snapshot.get("account_name"),
                                allow_aggregate=True,
                            ),
                            snapshot.get("snapshot_date"),
                            snapshot.get("total_market_value", 0),
                            snapshot.get("total_cost_value", 0),
                            snapshot.get("total_pnl", 0),
                            snapshot.get("holdings_json"),
                            snapshot.get("data_source", "manual"),
                            snapshot.get("created_at") or datetime.now(),
                            snapshot.get("updated_at") or datetime.now(),
                        ),
                    )

            if "portfolio_settings" in legacy_tables:
                legacy_cursor.execute("SELECT * FROM portfolio_settings ORDER BY key ASC")
                for row in legacy_cursor.fetchall():
                    setting = dict(row)
                    cursor.execute(
                        """
                        INSERT INTO portfolio_settings (key, value, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value = excluded.value,
                            updated_at = excluded.updated_at
                        """,
                        (
                            setting.get("key"),
                            setting.get("value"),
                            setting.get("updated_at") or datetime.now(),
                        ),
                    )

            set_metadata(cursor.connection, migration_key, str(migrated_rows))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
            legacy_conn.close()

        self.analysis_repository.migrate_legacy_portfolio_db(legacy_db_path)
        self.asset_repository._migrate_portfolio_stocks_to_assets()
        self.asset_repository._migrate_analysis_records_to_assets()
        self.asset_repository._migrate_trade_history_to_assets()
        return migrated_rows

    def _init_database(self):
        """初始化数据库表结构"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("PRAGMA table_info(portfolio_stocks)")
            columns = {row[1] for row in cursor.fetchall()}
            
            if columns and 'account_name' not in columns:
                print("[INFO] 执行持仓表结构升级：支持多账户，更改唯一约束")
                cursor.execute('ALTER TABLE portfolio_stocks RENAME TO portfolio_stocks_old')
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS portfolio_stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_name TEXT DEFAULT '{DEFAULT_ACCOUNT_NAME}',
                        code TEXT NOT NULL,
                        name TEXT NOT NULL,
                        cost_price REAL,
                        quantity INTEGER,
                        note TEXT,
                        auto_monitor BOOLEAN DEFAULT 1,
                        position_status TEXT DEFAULT 'active',
                        origin_analysis_id INTEGER,
                        last_trade_at TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(code, account_name)
                    )
                ''')
                cursor.execute(f'''
                    INSERT INTO portfolio_stocks (
                        id, account_name, code, name, cost_price, quantity, note, auto_monitor,
                        position_status, origin_analysis_id, last_trade_at, created_at, updated_at
                    )
                    SELECT id, '{DEFAULT_ACCOUNT_NAME}', code, name, cost_price, quantity, note, auto_monitor,
                           'active', NULL, NULL, created_at, updated_at
                    FROM portfolio_stocks_old
                ''')
                cursor.execute('DROP TABLE portfolio_stocks_old')
            elif not columns:
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS portfolio_stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_name TEXT DEFAULT '{DEFAULT_ACCOUNT_NAME}',
                        code TEXT NOT NULL,
                        name TEXT NOT NULL,
                        cost_price REAL,
                        quantity INTEGER,
                        note TEXT,
                        auto_monitor BOOLEAN DEFAULT 1,
                        position_status TEXT DEFAULT 'active',
                        origin_analysis_id INTEGER,
                        last_trade_at TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(code, account_name)
                    )
                ''')
            else:
                self._ensure_column(cursor, "portfolio_stocks", "position_status", "TEXT DEFAULT 'active'")
                self._ensure_column(cursor, "portfolio_stocks", "origin_analysis_id", "INTEGER")
                self._ensure_column(cursor, "portfolio_stocks", "last_trade_at", "TEXT")
            
            # 创建持仓分析历史表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_analysis_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_stock_id INTEGER NOT NULL,
                    analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    rating TEXT,
                    confidence REAL,
                    current_price REAL,
                    target_price REAL,
                    entry_min REAL,
                    entry_max REAL,
                    take_profit REAL,
                    stop_loss REAL,
                    summary TEXT,
                    FOREIGN KEY (portfolio_stock_id) REFERENCES portfolio_stocks(id) ON DELETE CASCADE
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_stock_id INTEGER NOT NULL,
                    trade_date TEXT NOT NULL,
                    trade_type TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    note TEXT,
                    trade_source TEXT DEFAULT 'manual',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (portfolio_stock_id) REFERENCES portfolio_stocks(id) ON DELETE CASCADE
                )
            ''')

            analysis_columns = {
                "stock_info_json": "TEXT",
                "agents_results_json": "TEXT",
                "discussion_result": "TEXT",
                "final_decision_json": "TEXT",
                "analysis_period": "TEXT DEFAULT '1y'",
                "analysis_source": "TEXT DEFAULT 'portfolio_batch_analysis'",
                "has_full_report": "INTEGER DEFAULT 0",
            }
            for column, definition in analysis_columns.items():
                self._ensure_column(cursor, "portfolio_analysis_history", column, definition)
            
            # 创建索引以提升查询性能
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_analysis_stock_id 
                ON portfolio_analysis_history(portfolio_stock_id)
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_analysis_time 
                ON portfolio_analysis_history(analysis_time DESC)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_trade_stock_date
                ON portfolio_trade_history(portfolio_stock_id, trade_date DESC, id DESC)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_trade_type
                ON portfolio_trade_history(trade_type, trade_date DESC)
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_daily_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_name TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    total_market_value REAL DEFAULT 0,
                    total_cost_value REAL DEFAULT 0,
                    total_pnl REAL DEFAULT 0,
                    holdings_json TEXT,
                    data_source TEXT DEFAULT 'manual',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(account_name, snapshot_date)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_date
                ON portfolio_daily_snapshots(snapshot_date DESC)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_account_date
                ON portfolio_daily_snapshots(account_name, snapshot_date DESC)
            ''')
            cursor.execute('DROP INDEX IF EXISTS idx_portfolio_review_reports_created_at')
            cursor.execute('DROP INDEX IF EXISTS idx_portfolio_review_reports_account_period')
            cursor.execute('DROP TABLE IF EXISTS portfolio_review_reports')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                INSERT OR IGNORE INTO portfolio_settings (key, value, updated_at)
                VALUES ('risk_free_rate_annual', '0.015', ?)
            ''', (datetime.now(),))
            
            conn.commit()
            print(f"[OK] 数据库初始化成功: {self.db_path}")
            
        except Exception as e:
            print(f"[ERROR] 数据库初始化失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    # ==================== 持仓股票CRUD操作 ====================
    
    def add_stock(self, code: str, name: str, cost_price: Optional[float] = None,
                  quantity: Optional[int] = None, note: str = "", 
                  auto_monitor: bool = True, account_name: str = DEFAULT_ACCOUNT_NAME) -> int:
        """
        添加持仓股票
        
        Args:
            code: 股票代码
            name: 股票名称
            cost_price: 持仓成本价（可选）
            quantity: 持仓数量（可选）
            note: 备注信息
            auto_monitor: 是否自动同步到监测列表
            account_name: 账户名称
            
        Returns:
            新增股票的ID
            
        Raises:
            sqlite3.IntegrityError: 如果股票代码已存在
        """
        existing = self.asset_repository.get_asset_by_symbol(code, account_name)
        if existing and existing.get("status") == STATUS_PORTFOLIO:
            raise ValueError(f"股票代码 {code} 在账户 {account_name} 中已存在")
        if existing:
            self.asset_repository.update_asset(
                existing["id"],
                name=name,
                status=STATUS_PORTFOLIO,
                cost_price=cost_price,
                quantity=quantity,
                note=note,
                monitor_enabled=auto_monitor,
                last_trade_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            print(f"[OK] 设为持仓成功: {code} {name} (ID: {existing['id']})")
            return int(existing["id"])

        asset_id = self.asset_repository.create_or_update_research_asset(
            symbol=code,
            name=name,
            account_name=account_name,
            note=note,
            monitor_enabled=auto_monitor,
        )
        self.asset_repository.transition_asset_status(
            asset_id,
            STATUS_PORTFOLIO,
            cost_price=cost_price,
            quantity=quantity,
            note=note,
            last_trade_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        print(f"[OK] 添加持仓股票成功: {code} {name} (ID: {asset_id})")
        return asset_id
    
    def update_stock(self, stock_id: int, **kwargs) -> bool:
        """
        更新持仓股票信息
        
        Args:
            stock_id: 股票ID
            **kwargs: 要更新的字段（code, name, cost_price, quantity, note, auto_monitor）
            
        Returns:
            是否更新成功
        """
        # 允许更新的字段
        allowed_fields = [
            'account_name', 'code', 'name', 'cost_price', 'quantity', 'note', 'auto_monitor',
            'position_status', 'origin_analysis_id', 'last_trade_at'
        ]
        update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not update_fields:
            print("[WARN] 没有需要更新的字段")
            return False

        asset = self.asset_repository.get_asset(stock_id)
        if not asset:
            print(f"[WARN] 未找到股票: ID {stock_id}")
            return False

        translated_updates = {}
        if "account_name" in update_fields:
            translated_updates["account_name"] = update_fields["account_name"]
        if "code" in update_fields:
            translated_updates["symbol"] = update_fields["code"]
        if "name" in update_fields:
            translated_updates["name"] = update_fields["name"]
        if "cost_price" in update_fields:
            translated_updates["cost_price"] = update_fields["cost_price"]
        if "quantity" in update_fields:
            translated_updates["quantity"] = update_fields["quantity"]
        if "note" in update_fields:
            translated_updates["note"] = update_fields["note"]
        if "auto_monitor" in update_fields:
            translated_updates["monitor_enabled"] = update_fields["auto_monitor"]
        if "origin_analysis_id" in update_fields:
            translated_updates["origin_analysis_id"] = update_fields["origin_analysis_id"]
        if "last_trade_at" in update_fields:
            translated_updates["last_trade_at"] = update_fields["last_trade_at"]
        if "position_status" in update_fields:
            translated_updates["status"] = STATUS_PORTFOLIO if update_fields["position_status"] == "active" else STATUS_WATCHLIST

        if "status" not in translated_updates:
            if ("quantity" in translated_updates or "cost_price" in translated_updates) and (
                translated_updates.get("quantity") not in (None, 0) and translated_updates.get("cost_price") not in (None, 0)
            ):
                translated_updates["status"] = STATUS_PORTFOLIO
            elif "quantity" in translated_updates and not translated_updates.get("quantity"):
                translated_updates["status"] = STATUS_WATCHLIST

        try:
            changed = self.asset_repository.update_asset(stock_id, **translated_updates)
            if changed:
                print(f"[OK] 更新持仓股票成功: ID {stock_id}")
            else:
                print(f"[WARN] 未找到股票: ID {stock_id}")
            return changed
        except Exception as e:
            print(f"[ERROR] 更新持仓股票失败: {e}")
            raise
    
    def delete_stock(self, stock_id: int) -> bool:
        """
        删除持仓股票（级联删除其所有分析历史）
        
        Args:
            stock_id: 股票ID
            
        Returns:
            是否删除成功
        """
        deleted = self.asset_repository.soft_delete_asset(stock_id)
        if deleted:
            print(f"[OK] 删除持仓股票成功: ID {stock_id}")
        else:
            print(f"[WARN] 未找到股票: ID {stock_id}")
        return deleted
    
    def get_stock(self, stock_id: int) -> Optional[Dict]:
        """
        获取单只持仓股票信息
        
        Args:
            stock_id: 股票ID
            
        Returns:
            股票信息字典，不存在则返回None
        """
        return self.asset_repository.get_asset(stock_id)
    
    def get_stock_by_code(self, code: str, account_name: str = DEFAULT_ACCOUNT_NAME) -> Optional[Dict]:
        """
        根据股票代码获取持仓股票信息
        
        Args:
            code: 股票代码
            
        Returns:
            股票信息字典，不存在则返回None
        """
        return self.asset_repository.get_asset_by_symbol(code, account_name)
    
    def get_stocks_by_code(self, code: str) -> List[Dict]:
        """
        根据股票代码获取该股票在所有账户中的持仓信息
        
        Args:
            code: 股票代码
            
        Returns:
            匹配的股票信息字典列表
        """
        return self.asset_repository.list_assets(status=STATUS_PORTFOLIO, symbol=code)

    def get_all_stocks(
        self,
        auto_monitor_only: bool = False,
        account_name: Optional[str] = None,
    ) -> List[Dict]:
        """
        获取所有持仓股票列表
        
        Args:
            auto_monitor_only: 是否只返回启用自动监测的股票
            
        Returns:
            股票信息字典列表
        """
        return self.asset_repository.list_assets(
            status=STATUS_PORTFOLIO,
            account_name=account_name,
            monitor_enabled=True if auto_monitor_only else None,
        )
    
    def search_stocks(self, keyword: str) -> List[Dict]:
        """
        搜索持仓股票（按代码或名称）
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            匹配的股票信息字典列表
        """
        keyword_lower = str(keyword or "").strip().lower()
        return [
            stock
            for stock in self.asset_repository.list_assets(status=STATUS_PORTFOLIO)
            if keyword_lower in str(stock.get("symbol", "")).lower() or keyword_lower in str(stock.get("name", "")).lower()
        ]
    
    def get_stock_count(self, account_name: Optional[str] = None) -> int:
        """
        获取持仓股票总数
        
        Returns:
            股票数量
        """
        return len(self.asset_repository.list_assets(status=STATUS_PORTFOLIO, account_name=account_name))
    
    # ==================== 分析历史记录操作 ====================
    
    def add_trade_history(
        self,
        stock_id: int,
        trade_type: str,
        trade_date: str,
        price: float,
        quantity: int,
        note: str = "",
        trade_source: str = "manual",
    ) -> int:
        """保存持仓交易流水。"""
        return self.asset_repository.add_trade_history(
            stock_id,
            trade_type=trade_type,
            trade_date=trade_date,
            price=price,
            quantity=quantity,
            note=note,
            trade_source=trade_source,
        )

    def get_trade_history(self, stock_id: int, limit: int = 20) -> List[Dict]:
        """获取指定持仓的交易流水。"""
        return self.asset_repository.get_trade_history(stock_id, limit)

    def get_trade_records(self, account_name: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """获取账户范围内的交易流水。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        sql = [
            """
            SELECT
                t.id,
                a.account_name,
                a.symbol AS stock_code,
                a.name AS stock_name,
                LOWER(t.trade_type) AS trade_type,
                t.quantity,
                t.price,
                t.price * t.quantity AS amount,
                t.note,
                t.trade_source,
                t.trade_date AS trade_time
            FROM asset_trade_history t
            INNER JOIN assets a
                ON a.id = t.asset_id
            WHERE a.deleted_at IS NULL
            """
        ]
        params: List[Any] = []
        if account_name:
            sql.append("AND a.account_name = ?")
            params.append(self._normalize_account_name_value(account_name))
        sql.append("ORDER BY t.trade_date DESC, t.id DESC LIMIT ?")
        params.append(limit)
        cursor.execute(" ".join(sql), tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_trade_records_page(
        self,
        account_name: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:
        """分页获取账户范围内的交易流水。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        clauses = ["a.deleted_at IS NULL"]
        params: List[Any] = []
        if account_name:
            clauses.append("a.account_name = ?")
            params.append(self._normalize_account_name_value(account_name))

        where_clause = " AND ".join(clauses)
        cursor.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM asset_trade_history t
            INNER JOIN assets a
                ON a.id = t.asset_id
            WHERE {where_clause}
            """,
            tuple(params),
        )
        total_row = cursor.fetchone()
        total = int(total_row["total"]) if total_row else 0
        offset = max(0, (max(1, int(page)) - 1) * max(1, int(page_size)))
        query_params = list(params)
        query_params.extend([max(1, int(page_size)), offset])
        cursor.execute(
            f"""
            SELECT
                t.id,
                a.account_name,
                a.symbol AS stock_code,
                a.name AS stock_name,
                LOWER(t.trade_type) AS trade_type,
                t.quantity,
                t.price,
                t.price * t.quantity AS amount,
                t.note,
                t.trade_source,
                t.trade_date AS trade_time
            FROM asset_trade_history t
            INNER JOIN assets a
                ON a.id = t.asset_id
            WHERE {where_clause}
            ORDER BY t.trade_date DESC, t.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(query_params),
        )
        rows = cursor.fetchall()
        conn.close()
        return {
            "items": [dict(row) for row in rows],
            "total": total,
            "page": max(1, int(page)),
            "page_size": max(1, int(page_size)),
        }

    def get_account_trade_history(
        self,
        account_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict]:
        """获取账户交易流水明细（支持日期区间）。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        sql = [
            """
            SELECT
                t.id,
                t.asset_id,
                a.account_name,
                a.symbol AS stock_code,
                a.name AS stock_name,
                LOWER(t.trade_type) AS trade_type,
                t.trade_date,
                t.price,
                t.quantity,
                t.note,
                t.trade_source
            FROM asset_trade_history t
            INNER JOIN assets a
                ON a.id = t.asset_id
            WHERE a.deleted_at IS NULL
            """
        ]
        params: List[Any] = []
        if account_name:
            sql.append("AND a.account_name = ?")
            params.append(self._normalize_account_name_value(account_name))
        if start_date:
            sql.append("AND t.trade_date >= ?")
            params.append(start_date)
        if end_date:
            sql.append("AND t.trade_date <= ?")
            params.append(end_date)
        sql.append("ORDER BY t.trade_date ASC, t.id ASC")
        cursor.execute(" ".join(sql), tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_trade_summary_map(self, stock_ids: Optional[List[int]] = None) -> Dict[int, Dict]:
        """批量获取持仓交易摘要。"""
        return self.asset_repository.get_trade_summary_map(stock_ids)

    def replace_trade_history(
        self,
        stock_id: int,
        trades: List[Dict],
        *,
        final_status_when_flat: str = STATUS_WATCHLIST,
        default_trade_source: str = "manual_fix",
    ) -> Dict:
        """替换单只持仓的交易流水并回算最新持仓状态。"""
        normalized_status = str(final_status_when_flat or "").strip().lower()
        if normalized_status not in {STATUS_WATCHLIST, STATUS_RESEARCH}:
            normalized_status = STATUS_WATCHLIST
        return self.asset_repository.replace_trade_history(
            stock_id,
            trades,
            final_status_when_flat=normalized_status,
            default_trade_source=default_trade_source,
        )

    def save_analysis(self, stock_id: int, rating: str, confidence: float,
                     current_price: float, target_price: Optional[float] = None,
                     entry_min: Optional[float] = None, entry_max: Optional[float] = None,
                     take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
                     summary: str = "", analysis_time: Optional[datetime] = None,
                     stock_info: Optional[Dict] = None,
                     agents_results: Optional[Dict] = None,
                     discussion_result: Optional[str] = None,
                     final_decision: Optional[Dict] = None,
                     analysis_period: str = "1y",
                     analysis_source: str = "portfolio_batch_analysis",
                     has_full_report: Optional[bool] = None) -> int:
        """
        保存分析历史记录
        
        Args:
            stock_id: 持仓股票ID
            rating: 投资评级（买入/持有/卖出）
            confidence: 信心度（0-10）
            current_price: 当前价格
            target_price: 目标价位
            entry_min: 进场区间最小值
            entry_max: 进场区间最大值
            take_profit: 止盈位
            stop_loss: 止损位
            summary: 分析摘要
            analysis_time: 分析时间，默认当前时间
            
        Returns:
            新增分析记录的ID
        """
        stock = self.get_stock(stock_id)
        if not stock:
            raise ValueError(f"未找到持仓股票ID: {stock_id}")

        full_report_flag = has_full_report
        if full_report_flag is None:
            full_report_flag = any(
                self.analysis_repository._payload_has_content(value)
                for value in (agents_results, discussion_result, final_decision)
            )

        analysis_id = self.analysis_repository.save_record(
            symbol=stock["code"],
            stock_name=stock.get("name") or stock["code"],
            account_name=stock.get("account_name", DEFAULT_ACCOUNT_NAME),
            asset_id=stock_id,
            portfolio_stock_id=stock_id,
            analysis_scope="portfolio",
            analysis_source=analysis_source,
            analysis_date=str(analysis_time or datetime.now()),
            period=analysis_period,
            rating=rating,
            confidence=confidence,
            current_price=current_price,
            target_price=target_price,
            entry_min=entry_min,
            entry_max=entry_max,
            take_profit=take_profit,
            stop_loss=stop_loss,
            summary=summary,
            stock_info=stock_info,
            agents_results=agents_results,
            discussion_result=discussion_result,
            final_decision=final_decision,
            has_full_report=full_report_flag,
            asset_status_snapshot=STATUS_PORTFOLIO,
        )
        try:
            from investment_lifecycle_service import InvestmentLifecycleService
            from monitor_db import StockMonitorDatabase

            lifecycle_service = InvestmentLifecycleService(
                portfolio_store=self,
                realtime_monitor_store=StockMonitorDatabase(self.db_path),
                analysis_store=self.analysis_repository,
            )
            lifecycle_service.sync_position(stock_id=stock_id)
        except Exception as exc:
            print(f"[WARN] 战略分析投影同步失败 (stock_id={stock_id}): {exc}")
        print(f"[OK] 保存分析历史成功: 股票ID {stock_id}, 评级 {rating}")
        return analysis_id

    def analysis_exists(self, stock_id: int, analysis_time: str) -> bool:
        """检查指定时间点的分析记录是否已存在。"""
        records = self.analysis_repository.list_records(
            analysis_scope="portfolio",
            asset_id=stock_id,
            portfolio_stock_id=stock_id,
            full_report_only=False,
        )
        return any(str(record.get("analysis_date")) == str(analysis_time) for record in records)
    
    def get_analysis_history(self, stock_id: int, limit: int = 10) -> List[Dict]:
        """
        获取股票的分析历史记录
        
        Args:
            stock_id: 持仓股票ID
            limit: 返回记录数量限制
            
        Returns:
            分析历史记录列表（按时间倒序）
        """
        return self.analysis_repository.list_records(
            analysis_scope="portfolio",
            asset_id=stock_id,
            portfolio_stock_id=stock_id,
            limit=limit,
            full_report_only=True,
        )
    
    def get_latest_analysis_history(self, stock_id: int, limit: int = 10) -> List[Dict]:
        """
        获取股票的最新分析历史记录（按时间倒序）
        
        这是 get_analysis_history 的别名方法，用于保持代码兼容性
        
        Args:
            stock_id: 持仓股票ID
            limit: 返回记录数量限制
            
        Returns:
            分析历史记录列表（按时间倒序）
        """
        return self.get_analysis_history(stock_id, limit)

    def delete_analysis_record(self, analysis_id: int) -> bool:
        """删除单条分析历史记录。"""
        return self.analysis_repository.delete_record(analysis_id)

    def get_latest_analysis(self, stock_id: int) -> Optional[Dict]:
        """
        获取股票的最新一次分析记录
        
        Args:
            stock_id: 持仓股票ID
            
        Returns:
            最新分析记录字典，不存在则返回None
        """
        stock = self.get_stock(stock_id)
        latest = self.analysis_repository.get_latest_linked_record(
            asset_id=stock_id,
            portfolio_stock_id=stock_id,
            symbol=(stock or {}).get("code"),
            account_name=(stock or {}).get("account_name", DEFAULT_ACCOUNT_NAME),
        )
        return latest
    
    def get_rating_changes(self, stock_id: int, days: int = 30) -> List[Tuple[str, str, str]]:
        """
        获取股票在指定天数内的评级变化
        
        Args:
            stock_id: 持仓股票ID
            days: 查询天数
            
        Returns:
            评级变化列表 [(时间, 旧评级, 新评级), ...]
        """
        records = self.analysis_repository.list_records(
            analysis_scope="portfolio",
            asset_id=stock_id,
            portfolio_stock_id=stock_id,
            full_report_only=False,
        )
        changes = []
        for index in range(1, len(records)):
            prev_rating = records[index - 1].get("rating")
            curr_rating = records[index].get("rating")
            if prev_rating != curr_rating:
                changes.append((records[index].get("analysis_date"), prev_rating, curr_rating))
        return changes
    
    def delete_old_analysis(self, days: int = 90) -> int:
        """
        删除超过指定天数的分析历史记录
        
        Args:
            days: 保留天数
            
        Returns:
            删除的记录数量
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                DELETE FROM analysis_records
                WHERE analysis_scope = 'portfolio'
                  AND datetime(analysis_date) < datetime('now', '-' || ? || ' days')
                ''',
                (days,),
            )
            conn.commit()
            return cursor.rowcount
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_latest_analysis_map(
        self,
        stock_ids: List[int],
        stocks: Optional[List[Dict]] = None,
    ) -> Dict[int, Dict]:
        normalized_ids = [int(stock_id) for stock_id in stock_ids if stock_id]
        if not normalized_ids:
            return {}

        latest_map = self.analysis_repository.get_latest_linked_records(normalized_ids)
        result: Dict[int, Dict] = {}
        for stock_id in normalized_ids:
            latest = latest_map.get(stock_id)
            if latest:
                result[stock_id] = latest
        return result

    def get_all_latest_analysis(self, account_name: Optional[str] = None) -> List[Dict]:
        """
        获取所有持仓股票的最新分析记录
        
        Returns:
            包含股票信息和最新分析的字典列表
        """
        stocks = self.get_all_stocks(auto_monitor_only=False, account_name=account_name)
        latest_map = self.get_latest_analysis_map(
            [int(stock["id"]) for stock in stocks if stock.get("id")],
            stocks=stocks,
        )
        result: List[Dict] = []
        for stock in stocks:
            merged = dict(stock)
            latest = latest_map.get(int(stock["id"])) if stock.get("id") else None
            if latest:
                latest_record = dict(latest)
                if latest_record.get("analysis_date") and not latest_record.get("analysis_time"):
                    latest_record["analysis_time"] = latest_record.get("analysis_date")
                merged["analysis_record_id"] = latest_record.get("id")
                merged["analysis_record_account_name"] = latest_record.get("account_name")
                merged.update(latest_record)
                merged["id"] = stock.get("id")
                merged["account_name"] = stock.get("account_name", DEFAULT_ACCOUNT_NAME)
                merged["symbol"] = stock.get("code") or stock.get("symbol")
                merged["code"] = stock.get("code") or stock.get("symbol")
                merged["name"] = self._resolve_stock_display_name(stock, latest_record)
                merged["cost_price"] = stock.get("cost_price")
                merged["quantity"] = stock.get("quantity")
                merged["note"] = stock.get("note")
                merged["auto_monitor"] = stock.get("auto_monitor")
                merged["monitor_enabled"] = stock.get("monitor_enabled")
                merged["position_status"] = stock.get("position_status")
                merged["origin_analysis_id"] = stock.get("origin_analysis_id")
                merged["last_trade_at"] = stock.get("last_trade_at")
                merged["analysis_record_id"] = latest_record.get("id")
                if (
                    stock.get("id")
                    and merged["name"] != self._normalize_stock_name_text(stock.get("name"))
                    and self._is_valid_stock_name(merged["name"], merged.get("code"))
                ):
                    try:
                        self.asset_repository.update_asset(int(stock["id"]), name=merged["name"])
                    except Exception:
                        pass
            else:
                merged["name"] = self._resolve_stock_display_name(stock)
            result.append(merged)
        return result

    # ==================== 快照 / 报告 / 设置 ====================

    def upsert_daily_snapshot(
        self,
        account_name: str,
        snapshot_date: str,
        total_market_value: float,
        total_cost_value: float,
        total_pnl: float,
        holdings: Optional[List[Dict]] = None,
        data_source: str = "manual",
    ) -> int:
        """按日写入或更新持仓快照。"""
        normalized_account_name = self._normalize_account_name_value(account_name, allow_aggregate=True)
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                '''
                INSERT INTO portfolio_daily_snapshots (
                    account_name, snapshot_date, total_market_value, total_cost_value,
                    total_pnl, holdings_json, data_source, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_name, snapshot_date) DO UPDATE SET
                    total_market_value = excluded.total_market_value,
                    total_cost_value = excluded.total_cost_value,
                    total_pnl = excluded.total_pnl,
                    holdings_json = excluded.holdings_json,
                    data_source = excluded.data_source,
                    updated_at = excluded.updated_at
                ''',
                (
                    normalized_account_name,
                    snapshot_date,
                    total_market_value,
                    total_cost_value,
                    total_pnl,
                    self._serialize_json(holdings or []),
                    data_source,
                    datetime.now(),
                    datetime.now(),
                ),
            )
            conn.commit()
            row_id = cursor.lastrowid
            if not row_id:
                cursor.execute(
                    '''
                    SELECT id FROM portfolio_daily_snapshots
                    WHERE account_name = ? AND snapshot_date = ?
                    ''',
                    (normalized_account_name, snapshot_date),
                )
                row = cursor.fetchone()
                row_id = row["id"] if row else 0
            return row_id
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def has_snapshot_for_date(self, account_name: str, snapshot_date: str) -> bool:
        """检查指定账户在某日是否已有快照。"""
        normalized_account_name = self._normalize_account_name_value(account_name, allow_aggregate=True)
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                '''
                SELECT 1
                FROM portfolio_daily_snapshots
                WHERE account_name = ? AND snapshot_date = ?
                LIMIT 1
                ''',
                (normalized_account_name, snapshot_date),
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()

    def get_daily_snapshots(
        self,
        account_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict]:
        """按时间范围查询持仓快照。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            query = ['SELECT * FROM portfolio_daily_snapshots WHERE 1 = 1']
            params: List[Any] = []
            if account_name:
                query.append('AND account_name = ?')
                params.append(self._normalize_account_name_value(account_name, allow_aggregate=True))
            if start_date:
                query.append('AND snapshot_date >= ?')
                params.append(start_date)
            if end_date:
                query.append('AND snapshot_date <= ?')
                params.append(end_date)
            query.append('ORDER BY snapshot_date ASC, account_name ASC')
            cursor.execute(' '.join(query), params)
            rows = cursor.fetchall()
            return [self._deserialize_snapshot_row(row) for row in rows]
        finally:
            conn.close()


    def set_setting(self, key: str, value: Any) -> None:
        """写入持仓域设置。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                '''
                INSERT INTO portfolio_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                ''',
                (key, self._serialize_json(value), datetime.now()),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_setting(self, key: str, default: Any = None) -> Any:
        """读取持仓域设置。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'SELECT value FROM portfolio_settings WHERE key = ? LIMIT 1',
                (key,),
            )
            row = cursor.fetchone()
            if not row:
                return default
            value = self._deserialize_flexible_value(row["value"], default=default)
            return default if value in (None, "") else value
        finally:
            conn.close()

    def get_account_total_assets_settings(self) -> Dict[str, float]:
        raw_value = self.get_setting(self.ACCOUNT_TOTAL_ASSETS_KEY, {})
        parsed = raw_value if isinstance(raw_value, dict) else {}
        return {
            DEFAULT_ACCOUNT_NAME: self._normalize_total_assets_value(parsed.get(DEFAULT_ACCOUNT_NAME)),
        }

    def set_account_total_assets_settings(self, account_assets: Optional[Dict[str, Any]]) -> Dict[str, float]:
        current = self.get_account_total_assets_settings()
        if not isinstance(account_assets, dict):
            return current

        next_value = {
            DEFAULT_ACCOUNT_NAME: self._normalize_total_assets_value(
                account_assets.get(DEFAULT_ACCOUNT_NAME, current.get(DEFAULT_ACCOUNT_NAME, 0.0))
            ),
        }
        self.set_setting(self.ACCOUNT_TOTAL_ASSETS_KEY, next_value)
        return next_value

    def get_account_total_assets(self, account_name: Optional[str], default: float = 0.0) -> float:
        settings = self.get_account_total_assets_settings()
        normalized_account_name = self._normalize_account_name_value(account_name)
        if not normalized_account_name:
            return self._normalize_total_assets_value(default)
        return settings.get(DEFAULT_ACCOUNT_NAME, self._normalize_total_assets_value(default))


# 创建全局数据库实例
portfolio_db = PortfolioDB()


if __name__ == "__main__":
    # 测试代码
    print("=" * 50)
    print("持仓股票数据库测试")
    print("=" * 50)
    
    # 初始化数据库
    db = PortfolioDB("test_portfolio.db")
    
    # 测试添加股票
    try:
        stock_id = db.add_stock("600519", "贵州茅台", 1650.5, 100, "长期持有")
        print(f"\n添加股票ID: {stock_id}")
    except ValueError as e:
        print(f"\n{e}")
    
    # 测试查询所有股票
    print("\n所有持仓股票:")
    stocks = db.get_all_stocks()
    for stock in stocks:
        print(f"  {stock['code']} {stock['name']}")
    
    # 测试保存分析历史
    if stocks:
        stock_id = stocks[0]['id']
        analysis_id = db.save_analysis(
            stock_id, "买入", 8.5, 1700.0, 1850.0,
            1600.0, 1650.0, 1900.0, 1500.0,
            "技术面和基本面均良好"
        )
        print(f"\n保存分析记录ID: {analysis_id}")
        
        # 查询分析历史
        print(f"\n股票 {stocks[0]['code']} 的分析历史:")
        history = db.get_analysis_history(stock_id)
        for h in history:
            print(f"  {h['analysis_time']}: {h['rating']} (信心度: {h['confidence']})")
    
    print("\n[OK] 数据库测试完成")
