"""
统一日志配置模块
提供全局日志配置和 get_logger 工厂函数
"""

import os
import logging
from pathlib import Path


# 日志目录
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# 全局日志格式
_LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 从环境变量读取日志级别，默认 INFO
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# 标记是否已完成根日志器初始化
_initialized = False


def _init_root_logger():
    """初始化根日志器（仅执行一次）"""
    global _initialized
    if _initialized:
        return

    root = logging.getLogger()
    root.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))

    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(console_handler)

    # 文件输出
    file_handler = logging.FileHandler(
        LOG_DIR / "app.log", encoding="utf-8", mode="a"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(file_handler)

    _initialized = True


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的日志器

    Args:
        name: 日志器名称，通常使用 __name__

    Returns:
        logging.Logger: 配置好的日志器实例
    """
    _init_root_logger()
    return logging.getLogger(name)
