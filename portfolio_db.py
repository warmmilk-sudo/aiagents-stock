"""
持仓股票数据库管理模块

提供持仓股票和分析历史的数据库操作接口
"""

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import os

# 数据库文件路径
DB_PATH = "portfolio_stocks.db"


class PortfolioDB:
    """持仓股票数据库管理类"""
    
    def __init__(self, db_path: str = DB_PATH):
        """
        初始化数据库连接
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self._init_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
        return conn
    
    def _ensure_column(self, cursor, table: str, column: str, definition: str):
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _serialize_json(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

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

    def _deserialize_review_report_row(self, row) -> Dict:
        record = dict(row)
        report_json = self._deserialize_flexible_value(record.get("report_json"), default={})
        record["report_json"] = report_json if isinstance(report_json, dict) else {}
        return record

    def _deserialize_trade_row(self, row) -> Dict:
        return dict(row)

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
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS portfolio_stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_name TEXT DEFAULT '默认账户',
                        code TEXT NOT NULL,
                        name TEXT NOT NULL,
                        cost_price REAL,
                        quantity INTEGER,
                        note TEXT,
                        auto_monitor BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(code, account_name)
                    )
                ''')
                cursor.execute('''
                    INSERT INTO portfolio_stocks (id, account_name, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at)
                    SELECT id, '默认账户', code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at
                    FROM portfolio_stocks_old
                ''')
                cursor.execute('DROP TABLE portfolio_stocks_old')
            elif not columns:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS portfolio_stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_name TEXT DEFAULT '默认账户',
                        code TEXT NOT NULL,
                        name TEXT NOT NULL,
                        cost_price REAL,
                        quantity INTEGER,
                        note TEXT,
                        auto_monitor BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(code, account_name)
                    )
                ''')
            
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

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_review_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_name TEXT NOT NULL,
                    period_type TEXT NOT NULL,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    data_mode TEXT DEFAULT 'estimated',
                    report_markdown TEXT NOT NULL,
                    report_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_review_reports_created_at
                ON portfolio_review_reports(created_at DESC)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_portfolio_review_reports_account_period
                ON portfolio_review_reports(account_name, period_type, period_end DESC)
            ''')

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
                  auto_monitor: bool = True, account_name: str = "默认账户") -> int:
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
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO portfolio_stocks 
                (account_name, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (account_name, code, name, cost_price, quantity, note, auto_monitor, 
                  datetime.now(), datetime.now()))
            
            conn.commit()
            stock_id = cursor.lastrowid
            print(f"[OK] 添加持仓股票成功: {code} {name} (ID: {stock_id})")
            return stock_id
            
        except sqlite3.IntegrityError as e:
            print(f"[ERROR] 股票代码在账户 {account_name} 中已存在: {code}")
            raise ValueError(f"股票代码 {code} 在账户 {account_name} 中已存在") from e
        except Exception as e:
            print(f"[ERROR] 添加持仓股票失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def update_stock(self, stock_id: int, **kwargs) -> bool:
        """
        更新持仓股票信息
        
        Args:
            stock_id: 股票ID
            **kwargs: 要更新的字段（code, name, cost_price, quantity, note, auto_monitor）
            
        Returns:
            是否更新成功
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # 允许更新的字段
        allowed_fields = ['account_name', 'code', 'name', 'cost_price', 'quantity', 'note', 'auto_monitor']
        update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not update_fields:
            print("[WARN] 没有需要更新的字段")
            return False
        
        # 添加更新时间
        update_fields['updated_at'] = datetime.now()
        
        # 构建SQL语句
        set_clause = ', '.join([f"{field} = ?" for field in update_fields.keys()])
        values = list(update_fields.values()) + [stock_id]
        
        try:
            cursor.execute(f'''
                UPDATE portfolio_stocks 
                SET {set_clause}
                WHERE id = ?
            ''', values)
            
            conn.commit()
            
            if cursor.rowcount > 0:
                print(f"[OK] 更新持仓股票成功: ID {stock_id}")
                return True
            else:
                print(f"[WARN] 未找到股票: ID {stock_id}")
                return False
                
        except Exception as e:
            print(f"[ERROR] 更新持仓股票失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def delete_stock(self, stock_id: int) -> bool:
        """
        删除持仓股票（级联删除其所有分析历史）
        
        Args:
            stock_id: 股票ID
            
        Returns:
            是否删除成功
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # 由于设置了ON DELETE CASCADE，删除股票会自动删除其分析历史
            cursor.execute('DELETE FROM portfolio_stocks WHERE id = ?', (stock_id,))
            conn.commit()
            
            if cursor.rowcount > 0:
                print(f"[OK] 删除持仓股票成功: ID {stock_id}")
                return True
            else:
                print(f"[WARN] 未找到股票: ID {stock_id}")
                return False
                
        except Exception as e:
            print(f"[ERROR] 删除持仓股票失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_stock(self, stock_id: int) -> Optional[Dict]:
        """
        获取单只持仓股票信息
        
        Args:
            stock_id: 股票ID
            
        Returns:
            股票信息字典，不存在则返回None
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT * FROM portfolio_stocks WHERE id = ?', (stock_id,))
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            return None
            
        finally:
            conn.close()
    
    def get_stock_by_code(self, code: str, account_name: str = "默认账户") -> Optional[Dict]:
        """
        根据股票代码获取持仓股票信息
        
        Args:
            code: 股票代码
            
        Returns:
            股票信息字典，不存在则返回None
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT * FROM portfolio_stocks WHERE code = ? AND account_name = ?', (code, account_name))
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            return None
        finally:
            conn.close()
    
    def get_stocks_by_code(self, code: str) -> List[Dict]:
        """
        根据股票代码获取该股票在所有账户中的持仓信息
        
        Args:
            code: 股票代码
            
        Returns:
            匹配的股票信息字典列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT * FROM portfolio_stocks WHERE code = ?', (code,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_all_stocks(self, auto_monitor_only: bool = False) -> List[Dict]:
        """
        获取所有持仓股票列表
        
        Args:
            auto_monitor_only: 是否只返回启用自动监测的股票
            
        Returns:
            股票信息字典列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if auto_monitor_only:
                cursor.execute('''
                    SELECT * FROM portfolio_stocks 
                    WHERE auto_monitor = 1
                    ORDER BY created_at DESC
                ''')
            else:
                cursor.execute('SELECT * FROM portfolio_stocks ORDER BY created_at DESC')
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        finally:
            conn.close()
    
    def search_stocks(self, keyword: str) -> List[Dict]:
        """
        搜索持仓股票（按代码或名称）
        
        Args:
            keyword: 搜索关键词
            
        Returns:
            匹配的股票信息字典列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            keyword_pattern = f"%{keyword}%"
            cursor.execute('''
                SELECT * FROM portfolio_stocks 
                WHERE code LIKE ? OR name LIKE ?
                ORDER BY created_at DESC
            ''', (keyword_pattern, keyword_pattern))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        finally:
            conn.close()
    
    def get_stock_count(self) -> int:
        """
        获取持仓股票总数
        
        Returns:
            股票数量
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) as count FROM portfolio_stocks')
            result = cursor.fetchone()
            return result['count']
            
        finally:
            conn.close()
    
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
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                '''
                INSERT INTO portfolio_trade_history (
                    portfolio_stock_id, trade_date, trade_type, price, quantity,
                    note, trade_source, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    stock_id,
                    trade_date,
                    trade_type,
                    price,
                    quantity,
                    note,
                    trade_source,
                    datetime.now(),
                ),
            )
            conn.commit()
            return cursor.lastrowid
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_trade_history(self, stock_id: int, limit: int = 20) -> List[Dict]:
        """获取指定持仓的交易流水。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                '''
                SELECT *
                FROM portfolio_trade_history
                WHERE portfolio_stock_id = ?
                ORDER BY trade_date DESC, id DESC
                LIMIT ?
                ''',
                (stock_id, limit),
            )
            rows = cursor.fetchall()
            return [self._deserialize_trade_row(row) for row in rows]
        finally:
            conn.close()

    def get_trade_summary_map(self, stock_ids: Optional[List[int]] = None) -> Dict[int, Dict]:
        """批量获取持仓交易摘要。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            query = [
                '''
                SELECT
                    portfolio_stock_id,
                    COUNT(*) AS trade_count,
                    MIN(CASE WHEN trade_type = 'buy' THEN trade_date END) AS first_buy_date,
                    MAX(trade_date) AS last_trade_date
                FROM portfolio_trade_history
                '''
            ]
            params: List[Any] = []
            if stock_ids:
                placeholders = ','.join('?' for _ in stock_ids)
                query.append(f'WHERE portfolio_stock_id IN ({placeholders})')
                params.extend(stock_ids)
            query.append('GROUP BY portfolio_stock_id')
            cursor.execute(' '.join(query), params)
            rows = cursor.fetchall()
            return {
                int(row["portfolio_stock_id"]): {
                    "trade_count": int(row["trade_count"] or 0),
                    "first_buy_date": row["first_buy_date"],
                    "last_trade_date": row["last_trade_date"],
                }
                for row in rows
            }
        finally:
            conn.close()

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
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            full_report_flag = has_full_report
            if full_report_flag is None:
                full_report_flag = any(
                    value not in (None, "", {}, [])
                    for value in (stock_info, agents_results, discussion_result, final_decision)
                )

            cursor.execute('''
                INSERT INTO portfolio_analysis_history 
                (portfolio_stock_id, analysis_time, rating, confidence, current_price,
                 target_price, entry_min, entry_max, take_profit, stop_loss, summary,
                 stock_info_json, agents_results_json, discussion_result, final_decision_json,
                 analysis_period, analysis_source, has_full_report)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stock_id,
                analysis_time or datetime.now(),
                rating,
                confidence,
                current_price,
                target_price,
                entry_min,
                entry_max,
                take_profit,
                stop_loss,
                summary,
                self._serialize_json(stock_info),
                self._serialize_json(agents_results),
                self._serialize_json(discussion_result),
                self._serialize_json(final_decision),
                analysis_period,
                analysis_source,
                1 if full_report_flag else 0,
            ))
            
            conn.commit()
            analysis_id = cursor.lastrowid
            print(f"[OK] 保存分析历史成功: 股票ID {stock_id}, 评级 {rating}")
            return analysis_id
            
        except Exception as e:
            print(f"[ERROR] 保存分析历史失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def analysis_exists(self, stock_id: int, analysis_time: str) -> bool:
        """检查指定时间点的分析记录是否已存在。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT 1
                FROM portfolio_analysis_history
                WHERE portfolio_stock_id = ?
                AND analysis_time = ?
                LIMIT 1
            ''', (stock_id, analysis_time))
            return cursor.fetchone() is not None
        finally:
            conn.close()
    
    def get_analysis_history(self, stock_id: int, limit: int = 10) -> List[Dict]:
        """
        获取股票的分析历史记录
        
        Args:
            stock_id: 持仓股票ID
            limit: 返回记录数量限制
            
        Returns:
            分析历史记录列表（按时间倒序）
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM portfolio_analysis_history 
                WHERE portfolio_stock_id = ?
                AND COALESCE(has_full_report, 0) = 1
                ORDER BY analysis_time DESC
                LIMIT ?
            ''', (stock_id, limit))
            
            rows = cursor.fetchall()
            return [self._deserialize_analysis_row(row) for row in rows]
            
        finally:
            conn.close()
    
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
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                '''
                DELETE FROM portfolio_analysis_history
                WHERE id = ?
                ''',
                (analysis_id,),
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_latest_analysis(self, stock_id: int) -> Optional[Dict]:
        """
        获取股票的最新一次分析记录
        
        Args:
            stock_id: 持仓股票ID
            
        Returns:
            最新分析记录字典，不存在则返回None
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM portfolio_analysis_history 
                WHERE portfolio_stock_id = ?
                AND COALESCE(has_full_report, 0) = 1
                ORDER BY analysis_time DESC
                LIMIT 1
            ''', (stock_id,))
            
            row = cursor.fetchone()
            return self._deserialize_analysis_row(row) if row else None
            
        finally:
            conn.close()
    
    def get_rating_changes(self, stock_id: int, days: int = 30) -> List[Tuple[str, str, str]]:
        """
        获取股票在指定天数内的评级变化
        
        Args:
            stock_id: 持仓股票ID
            days: 查询天数
            
        Returns:
            评级变化列表 [(时间, 旧评级, 新评级), ...]
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT analysis_time, rating 
                FROM portfolio_analysis_history 
                WHERE portfolio_stock_id = ?
                AND analysis_time >= datetime('now', '-' || ? || ' days')
                ORDER BY analysis_time ASC
            ''', (stock_id, days))
            
            rows = cursor.fetchall()
            
            changes = []
            for i in range(1, len(rows)):
                prev_rating = rows[i-1]['rating']
                curr_rating = rows[i]['rating']
                if prev_rating != curr_rating:
                    changes.append((
                        rows[i]['analysis_time'],
                        prev_rating,
                        curr_rating
                    ))
            
            return changes
            
        finally:
            conn.close()
    
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
            cursor.execute('''
                DELETE FROM portfolio_analysis_history 
                WHERE analysis_time < datetime('now', '-' || ? || ' days')
            ''', (days,))
            
            conn.commit()
            deleted_count = cursor.rowcount
            print(f"[OK] 清理历史分析记录: 删除 {deleted_count} 条记录")
            return deleted_count
            
        except Exception as e:
            print(f"[ERROR] 清理历史分析记录失败: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_all_latest_analysis(self) -> List[Dict]:
        """
        获取所有持仓股票的最新分析记录
        
        Returns:
            包含股票信息和最新分析的字典列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 
                    s.*,
                    h.rating, h.confidence, h.current_price, h.target_price,
                    h.entry_min, h.entry_max, h.take_profit, h.stop_loss,
                    h.analysis_time, h.summary, h.stock_info_json, h.agents_results_json,
                    h.discussion_result, h.final_decision_json, h.analysis_period,
                    h.analysis_source, h.has_full_report
                FROM portfolio_stocks s
                LEFT JOIN (
                    SELECT h1.*
                    FROM portfolio_analysis_history h1
                    INNER JOIN (
                        SELECT portfolio_stock_id, MAX(analysis_time) as max_time
                        FROM portfolio_analysis_history
                        WHERE COALESCE(has_full_report, 0) = 1
                        GROUP BY portfolio_stock_id
                    ) h2
                    ON h1.portfolio_stock_id = h2.portfolio_stock_id 
                    AND h1.analysis_time = h2.max_time
                    WHERE COALESCE(h1.has_full_report, 0) = 1
                ) h ON s.id = h.portfolio_stock_id
                ORDER BY s.created_at DESC
            ''')
            
            rows = cursor.fetchall()
            return [self._deserialize_analysis_row(row) for row in rows]
            
        finally:
            conn.close()

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
                    account_name,
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
                    (account_name, snapshot_date),
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
                (account_name, snapshot_date),
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
                params.append(account_name)
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

    def save_review_report(
        self,
        account_name: str,
        period_type: str,
        period_start: str,
        period_end: str,
        data_mode: str,
        report_markdown: str,
        report_json: Optional[Dict] = None,
    ) -> int:
        """保存周期复盘报告。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                '''
                INSERT INTO portfolio_review_reports (
                    account_name, period_type, period_start, period_end,
                    data_mode, report_markdown, report_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    account_name,
                    period_type,
                    period_start,
                    period_end,
                    data_mode,
                    report_markdown,
                    self._serialize_json(report_json or {}),
                    datetime.now(),
                ),
            )
            conn.commit()
            return cursor.lastrowid
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_review_reports(
        self,
        account_name: Optional[str] = None,
        limit: int = 20,
        period_type: Optional[str] = None,
    ) -> List[Dict]:
        """查询已保存的复盘报告。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            query = ['SELECT * FROM portfolio_review_reports WHERE 1 = 1']
            params: List[Any] = []
            if account_name:
                query.append('AND account_name = ?')
                params.append(account_name)
            if period_type:
                query.append('AND period_type = ?')
                params.append(period_type)
            query.append('ORDER BY created_at DESC LIMIT ?')
            params.append(limit)
            cursor.execute(' '.join(query), params)
            rows = cursor.fetchall()
            return [self._deserialize_review_report_row(row) for row in rows]
        finally:
            conn.close()

    def get_review_report(self, report_id: int) -> Optional[Dict]:
        """根据 ID 获取复盘报告详情。"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'SELECT * FROM portfolio_review_reports WHERE id = ?',
                (report_id,),
            )
            row = cursor.fetchone()
            return self._deserialize_review_report_row(row) if row else None
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
