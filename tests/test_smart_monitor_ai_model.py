"""
智能盯盘 AI模型调用测试
"""

import requests

import config
from smart_monitor_ai_model import SmartMonitorAIModel


def test_chat_completion_uses_reasoning_tier_by_default(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(config, "AI_MODEL_API_KEY", "sk-test")
    monkeypatch.setattr(config, "AI_MODEL_BASE_URL", "https://unit.example/v1")
    monkeypatch.setattr(config, "AI_MODEL_REASONING_NAME", "deepseek-reasoner")

    client = SmartMonitorAIModel()
    result = client.chat_completion(
        messages=[{"role": "user", "content": "analyze"}],
        max_tokens=1200
    )

    assert result == {"ok": True}
    assert captured["url"] == "https://unit.example/v1/chat/completions"
    assert captured["json"]["model"] == "deepseek-reasoner"
    assert captured["json"]["max_tokens"] == 8000
