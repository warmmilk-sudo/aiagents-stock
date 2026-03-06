"""
AI模型路由模块
用于按任务类型选择轻量/长上下文/推理模型
"""

from enum import Enum
from typing import Optional, Tuple, Union

import config


class ModelTier(str, Enum):
    """模型分层"""

    LIGHTWEIGHT = "lightweight"
    LONG_CONTEXT = "long_context"
    REASONING = "reasoning"


_TIER_DEFAULT_MODEL = {
    ModelTier.LIGHTWEIGHT: "deepseek-chat",
    ModelTier.LONG_CONTEXT: "qwen-long",
    ModelTier.REASONING: "deepseek-reasoner",
}


def _parse_tier(tier: Optional[Union[ModelTier, str]]) -> Optional[ModelTier]:
    """解析并标准化模型层级"""
    if tier is None:
        return None

    if isinstance(tier, ModelTier):
        return tier

    raw = str(tier).strip().lower().replace("-", "_")
    if raw in {"lightweight", "light", "fast"}:
        return ModelTier.LIGHTWEIGHT
    if raw in {"long_context", "longcontext", "retrieval", "validation"}:
        return ModelTier.LONG_CONTEXT
    if raw in {"reasoning", "decision", "reasoner"}:
        return ModelTier.REASONING
    return None


def resolve_model_name(
    tier: Optional[Union[ModelTier, str]] = None,
    explicit_model: Optional[str] = None,
) -> str:
    """
    解析最终模型名称
    优先级：explicit_model > tier配置 > DEFAULT_MODEL_NAME > tier内置默认 > 全局默认
    """
    if explicit_model and str(explicit_model).strip():
        return str(explicit_model).strip()

    parsed_tier = _parse_tier(tier)
    if parsed_tier == ModelTier.LIGHTWEIGHT and config.AI_MODEL_LIGHTWEIGHT_NAME.strip():
        return config.AI_MODEL_LIGHTWEIGHT_NAME.strip()
    if parsed_tier == ModelTier.LONG_CONTEXT and config.AI_MODEL_LONG_CONTEXT_NAME.strip():
        return config.AI_MODEL_LONG_CONTEXT_NAME.strip()
    if parsed_tier == ModelTier.REASONING and config.AI_MODEL_REASONING_NAME.strip():
        return config.AI_MODEL_REASONING_NAME.strip()

    if config.DEFAULT_MODEL_NAME.strip():
        return config.DEFAULT_MODEL_NAME.strip()

    if parsed_tier is not None:
        return _TIER_DEFAULT_MODEL[parsed_tier]
    return _TIER_DEFAULT_MODEL[ModelTier.LIGHTWEIGHT]


def resolve_api_credentials() -> Tuple[str, str]:
    """获取AI模型API鉴权配置"""
    api_key = (config.AI_MODEL_API_KEY or "").strip()
    base_url = (config.AI_MODEL_BASE_URL or "").strip() or "https://api.deepseek.com/v1"
    return api_key, base_url


__all__ = ["ModelTier", "resolve_model_name", "resolve_api_credentials"]
