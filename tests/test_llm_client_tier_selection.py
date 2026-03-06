"""
LLMClient 分层模型选择测试
"""

from types import SimpleNamespace

import config
import llm_client as llm_client_module
from ai_model_router import ModelTier


def _build_fake_openai(captured_calls, captured_init):
    class FakeOpenAI:
        def __init__(self, api_key=None, base_url=None):
            captured_init.append({"api_key": api_key, "base_url": base_url})

            def create(**kwargs):
                captured_calls.append(kwargs)
                return SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))]
                )

            self.chat = SimpleNamespace(
                completions=SimpleNamespace(create=create)
            )

    return FakeOpenAI


def test_call_api_uses_tier_model(monkeypatch):
    captured_calls = []
    captured_init = []

    monkeypatch.setattr(
        llm_client_module.openai,
        "OpenAI",
        _build_fake_openai(captured_calls, captured_init)
    )
    monkeypatch.setattr(config, "AI_MODEL_API_KEY", "sk-unit-test")
    monkeypatch.setattr(config, "AI_MODEL_BASE_URL", "https://unit.example/v1")
    monkeypatch.setattr(config, "AI_MODEL_LONG_CONTEXT_NAME", "long-model")
    monkeypatch.setattr(config, "DEFAULT_MODEL_NAME", "default-model")

    client = llm_client_module.LLMClient(model="custom-default")
    result = client.call_api(
        messages=[{"role": "user", "content": "hello"}],
        model_tier=ModelTier.LONG_CONTEXT,
        max_retries=1
    )

    assert result == "ok"
    assert captured_init[-1]["api_key"] == "sk-unit-test"
    assert captured_init[-1]["base_url"] == "https://unit.example/v1"
    assert captured_calls[-1]["model"] == "long-model"


def test_call_api_explicit_model_overrides_tier(monkeypatch):
    captured_calls = []
    captured_init = []

    monkeypatch.setattr(
        llm_client_module.openai,
        "OpenAI",
        _build_fake_openai(captured_calls, captured_init)
    )
    monkeypatch.setattr(config, "AI_MODEL_REASONING_NAME", "reason-model")

    client = llm_client_module.LLMClient(model="default-model")
    client.call_api(
        messages=[{"role": "user", "content": "hi"}],
        model="explicit-model",
        model_tier=ModelTier.REASONING,
        max_retries=1
    )

    assert captured_calls[-1]["model"] == "explicit-model"


def test_call_api_reasoner_max_tokens_boost(monkeypatch):
    captured_calls = []
    captured_init = []

    monkeypatch.setattr(
        llm_client_module.openai,
        "OpenAI",
        _build_fake_openai(captured_calls, captured_init)
    )
    monkeypatch.setattr(config, "AI_MODEL_REASONING_NAME", "deepseek-reasoner")

    client = llm_client_module.LLMClient(model="default-model")
    client.call_api(
        messages=[{"role": "user", "content": "reason"}],
        model_tier=ModelTier.REASONING,
        max_tokens=1200,
        max_retries=1
    )

    assert captured_calls[-1]["model"] == "deepseek-reasoner"
    assert captured_calls[-1]["max_tokens"] == 8000
