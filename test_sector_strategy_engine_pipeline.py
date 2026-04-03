import importlib
import logging
import sys
import time
import types
import unittest

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)
sys.modules.setdefault(
    "pandas",
    types.SimpleNamespace(
        DataFrame=type("DataFrame", (), {}),
        Series=dict,
        Timestamp=str,
        isna=lambda value: value is None,
    ),
)
sys.modules.setdefault("akshare", types.SimpleNamespace())
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None))

existing_sector_strategy_engine = sys.modules.get("sector_strategy_engine")
if existing_sector_strategy_engine is not None and not getattr(existing_sector_strategy_engine, "__file__", "").endswith("sector_strategy_engine.py"):
    sys.modules.pop("sector_strategy_engine", None)

sector_strategy_engine_module = importlib.import_module("sector_strategy_engine")
SectorStrategyEngine = sector_strategy_engine_module.SectorStrategyEngine


class SectorStrategyEnginePipelineTests(unittest.TestCase):
    def test_run_comprehensive_analysis_parallelizes_agent_stage_and_reports_progress(self):
        engine = SectorStrategyEngine.__new__(SectorStrategyEngine)
        engine.logger = logging.getLogger("test-sector-strategy-engine")
        engine.deepseek_client = None
        engine.database = None
        engine.save_analysis_report = lambda results, data: 99

        def sleep_agent(name):
            def _inner(*args, **kwargs):
                time.sleep(0.2)
                return {"agent_name": name, "analysis": f"{name}分析"}
            return _inner

        engine.agents = types.SimpleNamespace(
            macro_strategist_agent=sleep_agent("宏观策略师"),
            sector_diagnostician_agent=sleep_agent("板块诊断师"),
            fund_flow_analyst_agent=sleep_agent("资金流向分析师"),
            market_sentiment_decoder_agent=sleep_agent("市场情绪解码员"),
        )
        engine._conduct_comprehensive_discussion = lambda agents_results: "综合讨论"
        engine._generate_final_predictions = lambda comprehensive_report, agents_results, raw_data: {"summary": "最终预测"}

        progress_events = []
        started_at = time.perf_counter()
        result = engine.run_comprehensive_analysis(
            {
                "market_overview": {},
                "news": [],
                "sectors": {},
                "concepts": {},
                "sector_fund_flow": {},
                "north_flow": {},
            },
            progress_callback=lambda current, total, message: progress_events.append((current, total, message)),
        )
        elapsed = time.perf_counter() - started_at

        self.assertTrue(result["success"])
        self.assertLess(elapsed, 0.45)
        self.assertEqual(result["report_id"], 99)
        self.assertEqual(
            progress_events,
            [
                (25, 100, "AI 分析师团队正在分析板块与市场..."),
                (75, 100, "AI 团队正在进行综合讨论..."),
                (90, 100, "正在生成智策最终决策..."),
            ],
        )

    def test_conduct_comprehensive_discussion_uses_external_prompt_templates(self):
        captured = {}
        engine = SectorStrategyEngine.__new__(SectorStrategyEngine)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["messages"] = messages
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            return "综合研判报告"

        engine.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = engine._conduct_comprehensive_discussion(
            {
                "macro": {"analysis": "宏观观点"},
                "sector": {"analysis": "板块观点"},
                "fund": {"analysis": "资金观点"},
                "sentiment": {"analysis": "情绪观点"},
            }
        )

        self.assertEqual(result, "综合研判报告")
        self.assertEqual(captured["max_tokens"], 5000)
        self.assertEqual(captured["tier"].value, "reasoning")
        self.assertIn("首席策略官", captured["messages"][0]["content"])
        self.assertIn("【宏观策略师报告】", captured["messages"][1]["content"])
        self.assertIn("【市场情绪解码员报告】", captured["messages"][1]["content"])

    def test_repair_prediction_response_uses_external_prompt_templates(self):
        captured = {}
        engine = SectorStrategyEngine.__new__(SectorStrategyEngine)

        def fake_call_api(messages, temperature=None, max_tokens=None, tier=None):
            captured["messages"] = messages
            captured["temperature"] = temperature
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            return '{"summary":{"market_view":"中性"}}'

        engine.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = engine._repair_prediction_response(
            raw_response="错误输出",
            comprehensive_report="综合结论",
            sectors_str="半导体, AI算力",
        )

        self.assertEqual(result, '{"summary":{"market_view":"中性"}}')
        self.assertEqual(captured["temperature"], 0.1)
        self.assertEqual(captured["max_tokens"], 5000)
        self.assertEqual(captured["tier"].value, "reasoning")
        self.assertIn("JSON 修复助手", captured["messages"][0]["content"])
        self.assertIn("【原始输出】", captured["messages"][1]["content"])
        self.assertIn("错误输出", captured["messages"][1]["content"])


if __name__ == "__main__":
    unittest.main()
