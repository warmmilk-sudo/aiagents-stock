"""
宏观周期引擎测试
"""
import pytest

import macro_cycle_engine as engine_module


class StubDataFetcher:
    def get_all_macro_data(self):
        return {
            "success": True,
            "errors": [],
            "timestamp": "2026-03-06 10:00:00",
            "gdp": {"yearly": []}
        }

    def format_data_for_ai(self, _data):
        return "mock macro data"


class StubAgents:
    def __init__(self, model=None):
        self.model = model

    def kondratieff_wave_agent(self, _text):
        return {"agent_name": "康波", "analysis": "kondratieff"}

    def merrill_lynch_clock_agent(self, _text):
        return {"agent_name": "美林", "analysis": "merrill"}

    def china_policy_agent(self, _text):
        return {"agent_name": "政策", "analysis": "policy"}

    def chief_macro_strategist_agent(self, kondratieff_report, merrill_report, policy_report, macro_data_text):
        assert kondratieff_report == "kondratieff"
        assert merrill_report == "merrill"
        assert policy_report == "policy"
        assert macro_data_text == "mock macro data"
        return {"agent_name": "首席", "analysis": "chief"}


def test_run_full_analysis_no_unicode_encode_error(monkeypatch):
    monkeypatch.setattr(engine_module, "MacroCycleDataFetcher", StubDataFetcher)
    monkeypatch.setattr(engine_module, "MacroCycleAgents", StubAgents)

    engine = engine_module.MacroCycleEngine(model="test-model")
    progress_events = []

    try:
        result = engine.run_full_analysis(
            progress_callback=lambda pct, text: progress_events.append((pct, text))
        )
    except UnicodeEncodeError as exc:
        pytest.fail(f"run_full_analysis 不应抛出 UnicodeEncodeError: {exc}")

    assert result["success"] is True
    assert "kondratieff" in result["agents_analysis"]
    assert "merrill" in result["agents_analysis"]
    assert "policy" in result["agents_analysis"]
    assert "chief" in result["agents_analysis"]
    assert progress_events[-1][0] == 100
