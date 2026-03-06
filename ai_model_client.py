"""
AI模型客户端导出模块
统一对外暴露 LLMClient 与 AIModelClient 别名
"""

from llm_client import LLMClient

AIModelClient = LLMClient

__all__ = ["LLMClient", "AIModelClient"]
