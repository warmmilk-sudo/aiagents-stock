import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import os

_UNSET = object()

class StockMonitorDatabase:
    """股票监测数据库管理类"""
    
    def __init__(self, db_path: str = "stock_monitor.db"):
        self.db_path = db_path
        # 确保数据库所在目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        self.init_database()
    
    def init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建监测股票表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitored_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                rating TEXT NOT NULL,
                entry_range TEXT NOT NULL,  -- JSON格式: {"min": 10.0, "max": 12.0}
                take_profit REAL,
                stop_loss REAL,
                current_price REAL,
                last_checked TIMESTAMP,
                check_interval INTEGER DEFAULT 30,  -- 分钟
                notification_enabled BOOLEAN DEFAULT TRUE,
                trading_hours_only BOOLEAN DEFAULT TRUE,  -- 仅交易时段监控
                quant_enabled BOOLEAN DEFAULT FALSE,  -- 量化交易开关
                quant_config TEXT,  -- 量化配置JSON
                source_type TEXT DEFAULT 'watch',  -- 来源类型: portfolio|watch
                source_label TEXT DEFAULT '关注',  -- 来源标签: 持仓|关注
                portfolio_stock_id INTEGER,  -- 关联持仓ID
                has_position BOOLEAN DEFAULT FALSE,  -- 是否有持仓
                position_cost REAL,  -- 持仓成本
                position_quantity INTEGER,  -- 持仓数量
                position_updated_at TIMESTAMP,  -- 持仓更新时间
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 检查并添加trading_hours_only字段（兼容已有数据库）
        try:
            cursor.execute("SELECT trading_hours_only FROM monitored_stocks LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE monitored_stocks ADD COLUMN trading_hours_only BOOLEAN DEFAULT TRUE")
            print("✅ 已添加trading_hours_only字段")

        # 迁移持仓来源与持仓字段（兼容已有数据库）
        migrate_columns = [
            ("source_type", "TEXT DEFAULT 'watch'"),
            ("source_label", "TEXT DEFAULT '关注'"),
            ("portfolio_stock_id", "INTEGER"),
            ("has_position", "BOOLEAN DEFAULT FALSE"),
            ("position_cost", "REAL"),
            ("position_quantity", "INTEGER"),
            ("position_updated_at", "TIMESTAMP"),
        ]
        for column_name, column_def in migrate_columns:
            try:
                cursor.execute(f"SELECT {column_name} FROM monitored_stocks LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute(f"ALTER TABLE monitored_stocks ADD COLUMN {column_name} {column_def}")
                print(f"✅ 已添加{column_name}字段")

        # 旧数据默认标记为 watch/关注
        cursor.execute("UPDATE monitored_stocks SET source_type = 'watch' WHERE source_type IS NULL OR TRIM(source_type) = ''")
        cursor.execute("UPDATE monitored_stocks SET source_label = '关注' WHERE source_label IS NULL OR TRIM(source_label) = ''")
        
        # 创建价格历史表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id INTEGER,
                price REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_id) REFERENCES monitored_stocks (id)
            )
        ''')
        
        # 创建提醒记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id INTEGER,
                type TEXT NOT NULL,  -- entry/take_profit/stop_loss
                message TEXT NOT NULL,
                triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (stock_id) REFERENCES monitored_stocks (id)
            )
        ''')
        
        conn.commit()
        conn.close()

    @staticmethod
    def _row_to_stock(row: sqlite3.Row) -> Dict[str, Any]:
        """将数据库行转换为监测股票字典"""
        try:
            entry_range = json.loads(row["entry_range"]) if row["entry_range"] else None
        except (json.JSONDecodeError, TypeError):
            entry_range = None

        try:
            quant_config = json.loads(row["quant_config"]) if row["quant_config"] else None
        except (json.JSONDecodeError, TypeError):
            quant_config = None

        has_position = bool(row["has_position"]) if row["has_position"] is not None else False
        position_cost = row["position_cost"]
        position_quantity = row["position_quantity"]

        if (position_cost in (None, 0) or position_quantity in (None, 0)) and has_position:
            has_position = False

        return {
            "id": row["id"],
            "symbol": row["symbol"],
            "name": row["name"],
            "rating": row["rating"],
            "entry_range": entry_range,
            "take_profit": row["take_profit"],
            "stop_loss": row["stop_loss"],
            "current_price": row["current_price"],
            "last_checked": row["last_checked"],
            "check_interval": row["check_interval"],
            "notification_enabled": bool(row["notification_enabled"]),
            "trading_hours_only": bool(row["trading_hours_only"]) if row["trading_hours_only"] is not None else True,
            "quant_enabled": bool(row["quant_enabled"]) if row["quant_enabled"] is not None else False,
            "quant_config": quant_config,
            "source_type": row["source_type"] or "watch",
            "source_label": row["source_label"] or "关注",
            "portfolio_stock_id": row["portfolio_stock_id"],
            "has_position": has_position,
            "position_cost": position_cost,
            "position_quantity": position_quantity,
            "position_updated_at": row["position_updated_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    
    def add_monitored_stock(self, symbol: str, name: str, rating: str, 
                           entry_range: Dict, take_profit: float, 
                           stop_loss: float, check_interval: int = 30, 
                           notification_enabled: bool = True,
                           trading_hours_only: bool = True,
                           quant_enabled: bool = False,
                           quant_config: Dict = None,
                           source_type: str = "watch",
                           source_label: str = "关注",
                           portfolio_stock_id: Optional[int] = None,
                           has_position: bool = False,
                           position_cost: Optional[float] = None,
                           position_quantity: Optional[int] = None,
                           position_updated_at: Optional[str] = None) -> int:
        """添加监测股票"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        quant_config_json = json.dumps(quant_config) if quant_config else None
        source_type = source_type or "watch"
        source_label = source_label or ("持仓" if source_type == "portfolio" else "关注")
        position_updated_at = position_updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor.execute('''
            INSERT INTO monitored_stocks 
            (symbol, name, rating, entry_range, take_profit, stop_loss, check_interval, 
             notification_enabled, trading_hours_only, quant_enabled, quant_config,
             source_type, source_label, portfolio_stock_id, has_position, position_cost,
             position_quantity, position_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, name, rating, json.dumps(entry_range), take_profit, stop_loss, 
              check_interval, notification_enabled, trading_hours_only, quant_enabled, quant_config_json,
              source_type, source_label, portfolio_stock_id, 1 if has_position else 0,
              position_cost, position_quantity, position_updated_at))
        
        stock_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return stock_id
    
    def get_monitored_stocks(self) -> List[Dict]:
        """获取所有监测股票"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, symbol, name, rating, entry_range, take_profit, stop_loss, 
                   current_price, last_checked, check_interval, notification_enabled,
                   trading_hours_only, quant_enabled, quant_config,
                   source_type, source_label, portfolio_stock_id, has_position,
                   position_cost, position_quantity, position_updated_at,
                   created_at, updated_at
            FROM monitored_stocks
            ORDER BY created_at DESC
        ''')
        
        stocks = [self._row_to_stock(row) for row in cursor.fetchall()]
        conn.close()
        return stocks
    
    def update_stock_price(self, stock_id: int, price: float):
        """更新股票价格"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 更新当前价格
        cursor.execute('''
            UPDATE monitored_stocks 
            SET current_price = ?, last_checked = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (price, stock_id))
        
        # 记录价格历史
        cursor.execute('''
            INSERT INTO price_history (stock_id, price)
            VALUES (?, ?)
        ''', (stock_id, price))
        
        conn.commit()
        conn.close()
    
    def update_last_checked(self, stock_id: int):
        """仅更新最后检查时间（用于获取失败的情况）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE monitored_stocks 
            SET last_checked = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (stock_id,))
        
        conn.commit()
        conn.close()
    
    def has_recent_notification(self, stock_id: int, notification_type: str, minutes: int = 60) -> bool:
        """检查是否在最近X分钟内已有相同类型的通知"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) FROM notifications
            WHERE stock_id = ? AND type = ?
            AND datetime(triggered_at) > datetime('now', '-' || ? || ' minutes')
        ''', (stock_id, notification_type, minutes))
        
        count = cursor.fetchone()[0]
        conn.close()
        
        return count > 0
    
    def add_notification(self, stock_id: int, notification_type: str, message: str):
        """添加提醒记录"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO notifications (stock_id, type, message)
            VALUES (?, ?, ?)
        ''', (stock_id, notification_type, message))
        
        conn.commit()
        conn.close()
    
    def get_pending_notifications(self) -> List[Dict]:
        """获取待发送的提醒"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT n.id, n.stock_id, s.symbol, s.name, n.type, n.message, n.triggered_at
            FROM notifications n
            JOIN monitored_stocks s ON n.stock_id = s.id
            WHERE n.sent = FALSE
            ORDER BY n.triggered_at
        ''')
        
        notifications = []
        for row in cursor.fetchall():
            notifications.append({
                'id': row[0],
                'stock_id': row[1],
                'symbol': row[2],
                'name': row[3],
                'type': row[4],
                'message': row[5],
                'triggered_at': row[6]
            })
        
        conn.close()
        return notifications
    
    def get_all_recent_notifications(self, limit: int = 10) -> List[Dict]:
        """获取最近的所有通知（包括已发送和未发送的）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT n.id, n.stock_id, s.symbol, s.name, n.type, n.message, n.triggered_at, n.sent
            FROM notifications n
            JOIN monitored_stocks s ON n.stock_id = s.id
            ORDER BY n.triggered_at DESC
            LIMIT ?
        ''', (limit,))
        
        notifications = []
        for row in cursor.fetchall():
            notifications.append({
                'id': row[0],
                'stock_id': row[1],
                'symbol': row[2],
                'name': row[3],
                'type': row[4],
                'message': row[5],
                'triggered_at': row[6],
                'sent': bool(row[7])
            })
        
        conn.close()
        return notifications
    
    def mark_notification_sent(self, notification_id: int):
        """标记提醒已发送"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE notifications SET sent = TRUE WHERE id = ?
        ''', (notification_id,))
        
        conn.commit()
        conn.close()
    
    def mark_all_notifications_sent(self):
        """标记所有通知为已读"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('UPDATE notifications SET sent = TRUE WHERE sent = FALSE')
        
        conn.commit()
        conn.close()
        
        return cursor.rowcount
    
    def clear_all_notifications(self):
        """清空所有通知"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM notifications')
        
        conn.commit()
        conn.close()
        
        return cursor.rowcount
    
    def remove_monitored_stock(self, stock_id: int):
        """移除监测股票"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 删除相关记录
            cursor.execute('DELETE FROM price_history WHERE stock_id = ?', (stock_id,))
            cursor.execute('DELETE FROM notifications WHERE stock_id = ?', (stock_id,))
            cursor.execute('DELETE FROM monitored_stocks WHERE id = ?', (stock_id,))
            
            affected_rows = cursor.rowcount
            conn.commit()
            conn.close()
            
            return affected_rows > 0
        except Exception as e:
            print(f"删除股票失败: {e}")
            return False

    def remove_monitored_stock_by_symbol(self, symbol: str) -> bool:
        """按股票代码移除监测股票"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM monitored_stocks WHERE symbol = ?", (symbol,))
            row = cursor.fetchone()
            if not row:
                return False
            stock_id = row[0]
            cursor.execute("DELETE FROM price_history WHERE stock_id = ?", (stock_id,))
            cursor.execute("DELETE FROM notifications WHERE stock_id = ?", (stock_id,))
            cursor.execute("DELETE FROM monitored_stocks WHERE id = ?", (stock_id,))
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()
    
    def update_monitored_stock(self, stock_id: int, rating: str, entry_range: Dict,
                              take_profit: float, stop_loss: float,
                              check_interval: int, notification_enabled: bool,
                              trading_hours_only: Any = _UNSET,
                              quant_enabled: Any = _UNSET,
                              quant_config: Any = _UNSET,
                              source_type: Any = _UNSET,
                              source_label: Any = _UNSET,
                              portfolio_stock_id: Any = _UNSET,
                              has_position: Any = _UNSET,
                              position_cost: Any = _UNSET,
                              position_quantity: Any = _UNSET,
                              position_updated_at: Any = _UNSET):
        """更新监测股票"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        update_fields = [
            "rating = ?",
            "entry_range = ?",
            "take_profit = ?",
            "stop_loss = ?",
            "check_interval = ?",
            "notification_enabled = ?",
        ]
        params = [rating, json.dumps(entry_range), take_profit, stop_loss, check_interval, notification_enabled]

        if trading_hours_only is not _UNSET:
            update_fields.append("trading_hours_only = ?")
            params.append(trading_hours_only)

        if quant_enabled is not _UNSET:
            update_fields.append("quant_enabled = ?")
            params.append(quant_enabled)

        if quant_config is not _UNSET:
            update_fields.append("quant_config = ?")
            params.append(json.dumps(quant_config) if quant_config else None)

        if source_type is not _UNSET:
            update_fields.append("source_type = ?")
            params.append(source_type)

        if source_label is not _UNSET:
            update_fields.append("source_label = ?")
            params.append(source_label)

        if portfolio_stock_id is not _UNSET:
            update_fields.append("portfolio_stock_id = ?")
            params.append(portfolio_stock_id)

        if has_position is not _UNSET:
            update_fields.append("has_position = ?")
            params.append(1 if has_position else 0)

        if position_cost is not _UNSET:
            update_fields.append("position_cost = ?")
            params.append(position_cost)

        if position_quantity is not _UNSET:
            update_fields.append("position_quantity = ?")
            params.append(position_quantity)

        if position_updated_at is not _UNSET:
            update_fields.append("position_updated_at = ?")
            params.append(position_updated_at)

        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        params.append(stock_id)

        cursor.execute(
            f"UPDATE monitored_stocks SET {', '.join(update_fields)} WHERE id = ?",
            tuple(params),
        )

        conn.commit()
        conn.close()
        
        return cursor.rowcount > 0
    
    def toggle_notification(self, stock_id: int, enabled: bool):
        """切换通知状态"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE monitored_stocks 
            SET notification_enabled = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (enabled, stock_id))
        
        conn.commit()
        conn.close()
        
        return cursor.rowcount > 0
    
    def get_stock_by_id(self, stock_id: int) -> Optional[Dict]:
        """根据ID获取股票信息"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, symbol, name, rating, entry_range, take_profit, stop_loss,
                   current_price, last_checked, check_interval, notification_enabled,
                   trading_hours_only, quant_enabled, quant_config,
                   source_type, source_label, portfolio_stock_id, has_position,
                   position_cost, position_quantity, position_updated_at,
                   created_at, updated_at
            FROM monitored_stocks WHERE id = ?
        ''', (stock_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return self._row_to_stock(row)
        return None
    
    def get_monitor_by_code(self, symbol: str) -> Optional[Dict]:
        """
        根据股票代码获取监测信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            监测股票信息字典，不存在则返回None
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM monitored_stocks WHERE symbol = ?
        ''', (symbol,))
        
        row = cursor.fetchone()
        conn.close()
        
        return self._row_to_stock(row) if row else None
    
    def batch_add_or_update_monitors(self, monitors_data: List[Dict]) -> Dict[str, int]:
        """
        批量添加或更新监测股票
        
        Args:
            monitors_data: 监测股票数据列表，每个字典包含：
                - code/symbol: 股票代码
                - name: 股票名称  
                - rating: 投资评级
                - entry_min, entry_max: 进场区间
                - take_profit: 止盈位
                - stop_loss: 止损位
                - check_interval: 检查间隔（可选，默认60秒）
                - notification_enabled: 是否启用通知（可选，默认True）
                
        Returns:
            统计字典 {"added": X, "updated": Y, "failed": Z, "total": N}
        """
        added = 0
        updated = 0
        failed = 0
        
        for data in monitors_data:
            try:
                def _to_float(value):
                    if value is None:
                        return None
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        return None

                # 兼容code和symbol两种字段名
                symbol = data.get('code') or data.get('symbol')
                name = data.get('name', symbol)
                rating = data.get('rating', '持有')
                entry_min = _to_float(data.get('entry_min'))
                entry_max = _to_float(data.get('entry_max'))
                take_profit = _to_float(data.get('take_profit'))
                stop_loss = _to_float(data.get('stop_loss'))
                check_interval = data.get('check_interval')
                notification_enabled = data.get('notification_enabled')
                trading_hours_only = data.get('trading_hours_only')
                needs_review = bool(data.get('needs_review', False))
                source_type = data.get('source_type')
                source_label = data.get('source_label')
                portfolio_stock_id = data.get('portfolio_stock_id')
                has_position = data.get('has_position')
                position_cost = _to_float(data.get('position_cost'))
                position_quantity = data.get('position_quantity')
                position_updated_at = data.get('position_updated_at')
                has_portfolio_stock_id = 'portfolio_stock_id' in data
                has_position_cost = 'position_cost' in data
                has_position_quantity = 'position_quantity' in data
                has_has_position = 'has_position' in data
                
                # 验证必需字段
                if (
                    not symbol
                    or entry_min is None
                    or entry_max is None
                    or take_profit is None
                    or stop_loss is None
                    or entry_max <= entry_min
                    or take_profit <= 0
                    or stop_loss <= 0
                ):
                    print(f"[WARN] {symbol} 参数不完整，跳过")
                    failed += 1
                    continue
                
                # 构建entry_range
                entry_range = {"min": entry_min, "max": entry_max}
                if needs_review:
                    entry_range["needs_review"] = True
                
                # 检查是否已存在
                existing = self.get_monitor_by_code(symbol)
                if existing:
                    if check_interval is None:
                        check_interval = existing.get('check_interval', 60)
                    if notification_enabled is None:
                        notification_enabled = existing.get('notification_enabled', True)
                    if trading_hours_only is None:
                        trading_hours_only = existing.get('trading_hours_only', True)
                    if source_type is None:
                        source_type = existing.get('source_type', 'watch')
                    if source_label is None:
                        source_label = existing.get('source_label') or ('持仓' if source_type == 'portfolio' else '关注')
                    if not has_portfolio_stock_id:
                        portfolio_stock_id = existing.get('portfolio_stock_id')
                    if not has_has_position:
                        has_position = existing.get('has_position', False)
                    if not has_position_cost:
                        position_cost = existing.get('position_cost')
                    if not has_position_quantity:
                        position_quantity = existing.get('position_quantity')
                else:
                    if check_interval is None:
                        check_interval = 60
                    if notification_enabled is None:
                        notification_enabled = True
                    if trading_hours_only is None:
                        trading_hours_only = True
                    if source_type is None:
                        source_type = 'watch'
                    if source_label is None:
                        source_label = '持仓' if source_type == 'portfolio' else '关注'
                    if has_position is None:
                        has_position = bool(position_cost and position_quantity)
                
                if existing:
                    # 更新现有监测
                    self.update_monitored_stock(
                        existing['id'],
                        rating=rating,
                        entry_range=entry_range,
                        take_profit=take_profit,
                        stop_loss=stop_loss,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                        trading_hours_only=trading_hours_only,
                        source_type=source_type,
                        source_label=source_label,
                        portfolio_stock_id=portfolio_stock_id,
                        has_position=has_position,
                        position_cost=position_cost,
                        position_quantity=position_quantity,
                        position_updated_at=position_updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                    updated += 1
                    print(f"[OK] 更新监测: {symbol}")
                else:
                    # 添加新监测
                    self.add_monitored_stock(
                        symbol=symbol,
                        name=name,
                        rating=rating,
                        entry_range=entry_range,
                        take_profit=take_profit,
                        stop_loss=stop_loss,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                        trading_hours_only=trading_hours_only,
                        source_type=source_type,
                        source_label=source_label,
                        portfolio_stock_id=portfolio_stock_id,
                        has_position=has_position,
                        position_cost=position_cost,
                        position_quantity=position_quantity,
                        position_updated_at=position_updated_at
                    )
                    added += 1
                    print(f"[OK] 添加监测: {symbol}")
                    
            except Exception as e:
                symbol_str = data.get('code') or data.get('symbol', 'Unknown')
                print(f"[ERROR] 处理监测失败 ({symbol_str}): {str(e)}")
                failed += 1
        
        result = {
            "added": added,
            "updated": updated,
            "failed": failed,
            "total": added + updated + failed
        }
        
        print(f"\n[OK] 批量同步完成: 新增{added}只, 更新{updated}只, 失败{failed}只")
        return result

# 全局数据库实例
monitor_db = StockMonitorDatabase()
