"""
向后兼容模块 - DeepSeekClient 现已重命名为 LLMClient
请使用 from llm_client import LLMClient
"""
from llm_client import LLMClient

# 向后兼容别名
DeepSeekClient = LLMClient