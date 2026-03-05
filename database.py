import sqlite3
import json
from datetime import datetime
import os
from base_db import BaseDatabase
from logger import get_logger

logger = get_logger(__name__)


class StockAnalysisDatabase(BaseDatabase):
    """股票分析数据库"""

    def __init__(self, db_path="stock_analysis.db"):
        super().__init__(db_path)
        self.init_database()

    def init_database(self):
        """初始化数据库表结构"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analysis_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    stock_name TEXT,
                    analysis_date TEXT NOT NULL,
                    period TEXT NOT NULL,
                    stock_info TEXT,
                    agents_results TEXT,
                    discussion_result TEXT,
                    final_decision TEXT,
                    created_at TEXT NOT NULL
                )
            ''')
            conn.commit()

    def save_analysis(self, symbol, stock_name, period, stock_info, agents_results, discussion_result, final_decision):
        """保存分析记录到数据库"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            analysis_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            created_at = datetime.now().isoformat()

            stock_info_json = json.dumps(stock_info, ensure_ascii=False, default=str)
            agents_results_json = json.dumps(agents_results, ensure_ascii=False, default=str)
            discussion_result_json = json.dumps(discussion_result, ensure_ascii=False, default=str)
            final_decision_json = json.dumps(final_decision, ensure_ascii=False, default=str)

            cursor.execute('''
                INSERT INTO analysis_records 
                (symbol, stock_name, analysis_date, period, stock_info, agents_results, discussion_result, final_decision, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, stock_name, analysis_date, period, stock_info_json, agents_results_json, discussion_result_json, final_decision_json, created_at))

            conn.commit()
            return cursor.lastrowid

    @staticmethod
    def _safe_json_loads(data, default=None):
        """安全的 JSON 反序列化，损坏数据不会导致崩溃"""
        if not data:
            return default if default is not None else {}
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"JSON反序列化失败: {e}")
            return default if default is not None else {}

    def get_all_records(self):
        """获取所有分析记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, symbol, stock_name, analysis_date, period, final_decision, created_at
                FROM analysis_records 
                ORDER BY created_at DESC
            ''')

            records = cursor.fetchall()

        result = []
        for record in records:
            final_decision = self._safe_json_loads(record[5])
            rating = final_decision.get('rating', '未知') if isinstance(final_decision, dict) else '未知'

            result.append({
                'id': record[0],
                'symbol': record[1],
                'stock_name': record[2],
                'analysis_date': record[3],
                'period': record[4],
                'rating': rating,
                'created_at': record[6]
            })

        return result

    def get_record_count(self):
        """获取记录总数"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM analysis_records')
            return cursor.fetchone()[0]

    def get_record_by_id(self, record_id):
        """根据ID获取详细分析记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM analysis_records WHERE id = ?', (record_id,))
            record = cursor.fetchone()

        if not record:
            return None

        return {
            'id': record[0],
            'symbol': record[1],
            'stock_name': record[2],
            'analysis_date': record[3],
            'period': record[4],
            'stock_info': self._safe_json_loads(record[5]),
            'agents_results': self._safe_json_loads(record[6]),
            'discussion_result': self._safe_json_loads(record[7]),
            'final_decision': self._safe_json_loads(record[8]),
            'created_at': record[9]
        }

    def delete_record(self, record_id):
        """删除指定记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM analysis_records WHERE id = ?', (record_id,))
            conn.commit()
            return cursor.rowcount > 0


# 全局数据库实例
db = StockAnalysisDatabase()