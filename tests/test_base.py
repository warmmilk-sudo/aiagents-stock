"""
logger.py 和 base_db.py 的单元测试
"""
import pytest
import sqlite3
from base_db import BaseDatabase
from logger import get_logger


class TestLogger:
    """日志模块测试"""

    def test_get_logger_returns_logger(self):
        """get_logger 应返回 logging.Logger 实例"""
        import logging
        log = get_logger("test_module")
        assert isinstance(log, logging.Logger)
        assert log.name == "test_module"

    def test_get_logger_same_name_returns_same_instance(self):
        """相同名称应返回相同的 logger 实例"""
        log1 = get_logger("same_name")
        log2 = get_logger("same_name")
        assert log1 is log2


class TestBaseDatabase:
    """数据库基类测试"""

    @pytest.fixture(autouse=True)
    def setup_db(self, tmp_db):
        self.db = BaseDatabase(db_path=tmp_db)

    def test_get_connection_context_manager(self):
        """上下文管理器应正确管理连接生命周期"""
        with self.db.get_connection() as conn:
            assert conn is not None
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            conn.commit()

        # 连接退出 with 块后应已关闭（再次打开验证表存在）
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
            assert cursor.fetchone() is not None

    def test_connection_rollback_on_error(self):
        """异常时应自动回滚"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            conn.commit()

        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO test (name) VALUES ('should_rollback')")
                raise ValueError("模拟异常")
        except ValueError:
            pass

        # 验证回滚成功
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM test")
            assert cursor.fetchone()[0] == 0

    def test_execute_query(self):
        """execute_query 应返回查询结果"""
        with self.db.get_connection() as conn:
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
            conn.execute("INSERT INTO test (val) VALUES ('hello')")
            conn.commit()

        results = self.db.execute_query("SELECT val FROM test")
        assert len(results) == 1
        assert results[0][0] == "hello"

    def test_execute_write(self):
        """execute_write 应返回 lastrowid"""
        with self.db.get_connection() as conn:
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
            conn.commit()

        row_id = self.db.execute_write("INSERT INTO test (val) VALUES (?)", ("world",))
        assert row_id == 1
