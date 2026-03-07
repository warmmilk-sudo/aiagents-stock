import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional
import os

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
                managed_by_portfolio BOOLEAN DEFAULT FALSE,  -- 是否由持仓模块托管
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 检查并添加trading_hours_only字段（兼容已有数据库）
        try:
            cursor.execute("SELECT trading_hours_only FROM monitored_stocks LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE monitored_stocks ADD COLUMN trading_hours_only BOOLEAN DEFAULT TRUE")
            print("[OK] 已添加trading_hours_only字段")

        try:
            cursor.execute("SELECT managed_by_portfolio FROM monitored_stocks LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE monitored_stocks ADD COLUMN managed_by_portfolio BOOLEAN DEFAULT FALSE")
            print("[OK] 已添加managed_by_portfolio字段")
        
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
    
    def add_monitored_stock(self, symbol: str, name: str, rating: str, 
                           entry_range: Dict, take_profit: float, 
                           stop_loss: float, check_interval: int = 30, 
                           notification_enabled: bool = True,
                           trading_hours_only: bool = True,
                           quant_enabled: bool = False,
                           quant_config: Dict = None,
                           managed_by_portfolio: bool = False) -> int:
        """添加监测股票"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        quant_config_json = json.dumps(quant_config) if quant_config else None
        
        cursor.execute('''
            INSERT INTO monitored_stocks 
            (symbol, name, rating, entry_range, take_profit, stop_loss, check_interval, 
             notification_enabled, trading_hours_only, quant_enabled, quant_config,
             managed_by_portfolio)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, name, rating, json.dumps(entry_range), take_profit, stop_loss, 
              check_interval, notification_enabled, trading_hours_only, quant_enabled,
              quant_config_json, managed_by_portfolio))
        
        stock_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return stock_id
    
    def get_monitored_stocks(self) -> List[Dict]:
        """获取所有监测股票"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, symbol, name, rating, entry_range, take_profit, stop_loss, 
                   current_price, last_checked, check_interval, notification_enabled,
                   trading_hours_only, quant_enabled, quant_config, managed_by_portfolio,
                   created_at, updated_at
            FROM monitored_stocks
            ORDER BY created_at DESC
        ''')
        
        stocks = []
        for row in cursor.fetchall():
            try:
                quant_config = json.loads(row[13]) if row[13] else None
                entry_range = json.loads(row[4]) if row[4] else None
            except (json.JSONDecodeError, TypeError) as e:
                print(f"警告: 股票 {row[1]} 的JSON解析失败: {e}")
                entry_range = None
                quant_config = None
                
            stocks.append({
                'id': row[0],
                'symbol': row[1],
                'name': row[2],
                'rating': row[3],
                'entry_range': entry_range,
                'take_profit': row[5],
                'stop_loss': row[6],
                'current_price': row[7],
                'last_checked': row[8],
                'check_interval': row[9],
                'notification_enabled': bool(row[10]),
                'trading_hours_only': bool(row[11]) if row[11] is not None else True,
                'quant_enabled': bool(row[12]),
                'quant_config': quant_config,
                'managed_by_portfolio': bool(row[14]) if row[14] is not None else False,
                'created_at': row[15],
                'updated_at': row[16]
            })
        
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
    
    def update_monitored_stock(self, stock_id: int, rating: str, entry_range: Dict, 
                              take_profit: float, stop_loss: float, 
                              check_interval: int, notification_enabled: bool,
                              trading_hours_only: bool = None,
                              quant_enabled: bool = None,
                              quant_config: Dict = None,
                              managed_by_portfolio: Optional[bool] = None):
        """更新监测股票"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if quant_enabled is not None and quant_config is not None:
            quant_config_json = json.dumps(quant_config) if quant_config else None
            trading_hours_sql = ", trading_hours_only = ?" if trading_hours_only is not None else ""
            managed_sql = ", managed_by_portfolio = ?" if managed_by_portfolio is not None else ""
            params = [rating, json.dumps(entry_range), take_profit, stop_loss, 
                      check_interval, notification_enabled, quant_enabled, quant_config_json]
            if trading_hours_only is not None:
                params.append(trading_hours_only)
            if managed_by_portfolio is not None:
                params.append(managed_by_portfolio)
            params.append(stock_id)

            cursor.execute(f'''
                UPDATE monitored_stocks 
                SET rating = ?, entry_range = ?, take_profit = ?, stop_loss = ?, 
                    check_interval = ?, notification_enabled = ?, 
                    quant_enabled = ?, quant_config = ?{trading_hours_sql}{managed_sql},
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', tuple(params))
        else:
            trading_hours_sql = ", trading_hours_only = ?" if trading_hours_only is not None else ""
            managed_sql = ", managed_by_portfolio = ?" if managed_by_portfolio is not None else ""
            params = [rating, json.dumps(entry_range), take_profit, stop_loss, check_interval, notification_enabled]
            if trading_hours_only is not None:
                params.append(trading_hours_only)
            if managed_by_portfolio is not None:
                params.append(managed_by_portfolio)
            params.append(stock_id)

            cursor.execute(f'''
                UPDATE monitored_stocks 
                SET rating = ?, entry_range = ?, take_profit = ?, stop_loss = ?, 
                    check_interval = ?, notification_enabled = ?{trading_hours_sql}{managed_sql}, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', tuple(params))
        
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
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, symbol, name, rating, entry_range, take_profit, stop_loss,
                   current_price, last_checked, check_interval, notification_enabled,
                   trading_hours_only, quant_enabled, quant_config, managed_by_portfolio
            FROM monitored_stocks WHERE id = ?
        ''', (stock_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            try:
                quant_config = json.loads(row[13]) if row[13] else None
                entry_range = json.loads(row[4]) if row[4] else None
            except (json.JSONDecodeError, TypeError) as e:
                print(f"警告: 股票 {row[1]} 的JSON解析失败: {e}")
                entry_range = None
                quant_config = None
                
            return {
                'id': row[0],
                'symbol': row[1],
                'name': row[2],
                'rating': row[3],
                'entry_range': entry_range,
                'take_profit': row[5],
                'stop_loss': row[6],
                'current_price': row[7],
                'last_checked': row[8],
                'check_interval': row[9],
                'notification_enabled': bool(row[10]),
                'trading_hours_only': bool(row[11]) if row[11] is not None else True,
                'quant_enabled': bool(row[12]),
                'quant_config': quant_config,
                'managed_by_portfolio': bool(row[14]) if row[14] is not None else False
            }
        return None

    def get_monitor_by_code(self, symbol: str, managed_only: Optional[bool] = None) -> Optional[Dict]:
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

        sql = 'SELECT * FROM monitored_stocks WHERE symbol = ?'
        params = [symbol]
        if managed_only is True:
            sql += ' AND managed_by_portfolio = 1'
        elif managed_only is False:
            sql += ' AND managed_by_portfolio = 0'

        cursor.execute(sql, tuple(params))
        row = cursor.fetchone()
        conn.close()

        if row:
            try:
                row_dict = dict(row)
                entry_range = json.loads(row_dict['entry_range']) if row_dict['entry_range'] else None
                quant_config = json.loads(row_dict['quant_config']) if row_dict['quant_config'] else None
            except (json.JSONDecodeError, TypeError) as e:
                print(f"警告: 股票 {row['symbol']} 的JSON解析失败: {e}")
                entry_range = None
                quant_config = None
            
            return {
                'id': row['id'],
                'symbol': row['symbol'],
                'name': row['name'],
                'rating': row['rating'],
                'entry_range': entry_range,
                'take_profit': row['take_profit'],
                'stop_loss': row['stop_loss'],
                'current_price': row['current_price'],
                'last_checked': row['last_checked'],
                'check_interval': row['check_interval'],
                'notification_enabled': bool(row['notification_enabled']),
                'trading_hours_only': bool(row['trading_hours_only']) if row['trading_hours_only'] is not None else True,
                'quant_enabled': bool(row['quant_enabled']),
                'quant_config': quant_config,
                'managed_by_portfolio': bool(row['managed_by_portfolio']) if row['managed_by_portfolio'] is not None else False
            }
        return None

    def remove_monitor_by_code(self, symbol: str, managed_only: bool = False) -> bool:
        """按股票代码删除监测项。"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            'SELECT id FROM monitored_stocks WHERE symbol = ?' + (' AND managed_by_portfolio = 1' if managed_only else ''),
            (symbol,),
        )
        rows = cursor.fetchall()
        if not rows:
            conn.close()
            return False

        stock_ids = [row[0] for row in rows]
        placeholders = ','.join('?' for _ in stock_ids)
        cursor.execute(f'DELETE FROM price_history WHERE stock_id IN ({placeholders})', tuple(stock_ids))
        cursor.execute(f'DELETE FROM notifications WHERE stock_id IN ({placeholders})', tuple(stock_ids))
        cursor.execute(
            'DELETE FROM monitored_stocks WHERE symbol = ?' + (' AND managed_by_portfolio = 1' if managed_only else ''),
            (symbol,),
        )
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    
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
                # 兼容code和symbol两种字段名
                symbol = data.get('code') or data.get('symbol')
                name = data.get('name', symbol)
                rating = data.get('rating', '持有')
                entry_min = data.get('entry_min')
                entry_max = data.get('entry_max')
                take_profit = data.get('take_profit')
                stop_loss = data.get('stop_loss')
                check_interval = data.get('check_interval', 60)
                notification_enabled = data.get('notification_enabled', True)
                trading_hours_only = data.get('trading_hours_only', True)
                managed_by_portfolio = data.get('managed_by_portfolio', False)
                
                # 验证必需字段
                if not symbol or not all([entry_min, entry_max, take_profit, stop_loss]):
                    print(f"[WARN] {symbol} 参数不完整，跳过")
                    failed += 1
                    continue
                
                # 构建entry_range
                entry_range = {"min": entry_min, "max": entry_max}
                
                # 检查是否已存在
                existing = self.get_monitor_by_code(
                    symbol,
                    managed_only=True if managed_by_portfolio else None,
                )
                
                if existing:
                    desired_entry_range = {"min": entry_min, "max": entry_max}
                    unchanged = (
                        existing.get('rating') == rating and
                        existing.get('entry_range') == desired_entry_range and
                        existing.get('take_profit') == take_profit and
                        existing.get('stop_loss') == stop_loss and
                        existing.get('check_interval') == check_interval and
                        bool(existing.get('notification_enabled')) == bool(notification_enabled) and
                        bool(existing.get('trading_hours_only', True)) == bool(trading_hours_only) and
                        bool(existing.get('managed_by_portfolio', False)) == bool(managed_by_portfolio)
                    )
                    if unchanged:
                        print(f"[OK] 监测未变化: {symbol}")
                        continue

                    # 更新现有监测
                    self.update_monitored_stock(
                        existing['id'],
                        rating=rating,
                        entry_range=desired_entry_range,
                        take_profit=take_profit,
                        stop_loss=stop_loss,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                        trading_hours_only=trading_hours_only,
                        managed_by_portfolio=managed_by_portfolio
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
                        managed_by_portfolio=managed_by_portfolio
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
