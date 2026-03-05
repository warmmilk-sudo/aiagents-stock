"""
测试通用 fixtures
"""
import os
import sys
import pytest
import tempfile

# 将项目根目录加入 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def tmp_db(tmp_path):
    """提供一个临时数据库路径"""
    return str(tmp_path / "test.db")


@pytest.fixture
def mock_env(monkeypatch):
    """提供一个干净的环境变量上下文"""
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key-12345")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://api.test.com/v1")
    monkeypatch.setenv("DEFAULT_MODEL_NAME", "test-model")
    monkeypatch.setenv("TUSHARE_TOKEN", "test-tushare-token")
