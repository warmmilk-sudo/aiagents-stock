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


if __name__ == "__main__":
    unittest.main()
