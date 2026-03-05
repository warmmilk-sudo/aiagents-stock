"""
数据库基类模块
提供统一的 SQLite 连接管理和上下文管理器
"""

import sqlite3
import os
from contextlib import contextmanager
from logger import get_logger

logger = get_logger(__name__)


class BaseDatabase:
    """SQLite 数据库基类，提供统一的连接管理"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        # 确保数据库所在目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

    @contextmanager
    def get_connection(self):
        """
        获取数据库连接的上下文管理器

        用法:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(...)
                conn.commit()

        连接在退出 with 块时自动关闭，异常时自动回滚。
        """
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute_query(self, sql: str, params: tuple = ()):
        """执行查询并返回所有结果"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()

    def execute_write(self, sql: str, params: tuple = ()):
        """执行写入操作并返回 lastrowid"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            return cursor.lastrowid
