"""
AI模型路由测试
"""

import config
from ai_model_router import ModelTier, resolve_api_credentials, resolve_model_name


def test_resolve_model_name_priority_explicit(monkeypatch):
    monkeypatch.setattr(config, "AI_MODEL_LIGHTWEIGHT_NAME", "light-env")
    monkeypatch.setattr(config, "DEFAULT_MODEL_NAME", "default-env")

    assert resolve_model_name(
        tier=ModelTier.LIGHTWEIGHT,
        explicit_model="explicit-model"
    ) == "explicit-model"


def test_resolve_model_name_uses_tier_env(monkeypatch):
    monkeypatch.setattr(config, "AI_MODEL_LIGHTWEIGHT_NAME", "light-env")
    monkeypatch.setattr(config, "AI_MODEL_LONG_CONTEXT_NAME", "long-env")
    monkeypatch.setattr(config, "AI_MODEL_REASONING_NAME", "reason-env")
    monkeypatch.setattr(config, "DEFAULT_MODEL_NAME", "default-env")

    assert resolve_model_name(ModelTier.LIGHTWEIGHT) == "light-env"
    assert resolve_model_name(ModelTier.LONG_CONTEXT) == "long-env"
    assert resolve_model_name(ModelTier.REASONING) == "reason-env"


def test_resolve_model_name_fallback_default_then_builtin(monkeypatch):
    monkeypatch.setattr(config, "AI_MODEL_LIGHTWEIGHT_NAME", "")
    monkeypatch.setattr(config, "AI_MODEL_LONG_CONTEXT_NAME", "")
    monkeypatch.setattr(config, "AI_MODEL_REASONING_NAME", "")
    monkeypatch.setattr(config, "DEFAULT_MODEL_NAME", "default-env")

    assert resolve_model_name(ModelTier.LONG_CONTEXT) == "default-env"

    monkeypatch.setattr(config, "DEFAULT_MODEL_NAME", "")
    assert resolve_model_name(ModelTier.LONG_CONTEXT) == "qwen-long"
    assert resolve_model_name(None) == "deepseek-chat"


def test_resolve_api_credentials(monkeypatch):
    monkeypatch.setattr(config, "AI_MODEL_API_KEY", "sk-test")
    monkeypatch.setattr(config, "AI_MODEL_BASE_URL", "https://example.com/v1")
    api_key, base_url = resolve_api_credentials()

    assert api_key == "sk-test"
    assert base_url == "https://example.com/v1"

    monkeypatch.setattr(config, "AI_MODEL_BASE_URL", "")
    _, fallback_url = resolve_api_credentials()
    assert fallback_url == "https://api.deepseek.com/v1"
