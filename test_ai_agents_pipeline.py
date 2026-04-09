import importlib
import sys
import types
import unittest

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)

existing_ai_agents = sys.modules.get("ai_agents")
if existing_ai_agents is not None and not getattr(existing_ai_agents, "__file__", "").endswith("ai_agents.py"):
    sys.modules.pop("ai_agents", None)

ai_agents_module = importlib.import_module("ai_agents")
StockAnalysisAgents = ai_agents_module.StockAnalysisAgents


class StockAnalysisAgentsPipelineTests(unittest.TestCase):
    def test_run_multi_agent_analysis_uses_risk_key(self):
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)
        agent.deepseek_client = None
        agent.technical_analyst_agent = lambda *args, **kwargs: {"analysis": "技术"}
        agent.fundamental_analyst_agent = lambda *args, **kwargs: {"analysis": "基本面"}
        agent.fund_flow_analyst_agent = lambda *args, **kwargs: {"analysis": "资金"}
        agent.risk_management_agent = lambda *args, **kwargs: {"analysis": "风险"}
        agent.market_sentiment_agent = lambda *args, **kwargs: {"analysis": "情绪"}
        agent.news_analyst_agent = lambda *args, **kwargs: {"analysis": "新闻"}

        result = agent.run_multi_agent_analysis(
            stock_info={"symbol": "600519"},
            stock_data=[],
            indicators={},
            enabled_analysts={
                "technical": False,
                "fundamental": False,
                "fund_flow": False,
                "risk": True,
                "sentiment": False,
                "news": False,
            },
        )

        self.assertIn("risk", result)
        self.assertNotIn("risk_management", result)

    def test_conduct_team_discussion_rejects_empty_reports(self):
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)
        agent.deepseek_client = types.SimpleNamespace()

        with self.assertRaisesRegex(RuntimeError, "没有可用于团队讨论的分析师报告"):
            agent.conduct_team_discussion({}, {"symbol": "600519", "name": "贵州茅台"})

    def test_conduct_team_discussion_strips_reasoning_and_keeps_titles(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["prompt"] = messages[1]["content"]
            return "讨论结果"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.conduct_team_discussion(
            {
                "technical": {"analysis": "【推理过程】\n内部推理\n\n正式技术分析正文"},
                "risk": {"analysis": "风险提示正文"},
            },
            {"symbol": "600519", "name": "贵州茅台"},
        )

        self.assertEqual(result, "讨论结果")
        self.assertIn("【技术分析师报告】", captured["prompt"])
        self.assertIn("【风险管理师报告】", captured["prompt"])
        self.assertIn("正式技术分析正文", captured["prompt"])
        self.assertNotIn("内部推理", captured["prompt"])
        self.assertIn("关键筹码摘要", captured["prompt"])

    def test_conduct_team_discussion_includes_chip_summary(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["prompt"] = messages[1]["content"]
            return "讨论结果"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        agent.conduct_team_discussion(
            {"technical": {"analysis": "技术分析正文"}},
            {"symbol": "600519", "name": "贵州茅台"},
            {
                "chip_data_source": "tushare.cyq_chips/cyq_perf",
                "main_chip_peak_price": 1680,
                "cost_band_70": "1600-1720",
                "profit_ratio_estimate": "61.2%",
            },
        )

        self.assertIn("数据源：tushare.cyq_chips/cyq_perf", captured["prompt"])
        self.assertIn("主筹码峰：1680", captured["prompt"])
        self.assertIn("70%成本区：1600-1720", captured["prompt"])
        self.assertIn("获利盘：61.2%", captured["prompt"])

    def test_risk_management_agent_includes_pe_pb_context(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["prompt"] = messages[1]["content"]
            return "风险分析"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.risk_management_agent(
            stock_info={
                "symbol": "600519",
                "name": "贵州茅台",
                "current_price": 1688.0,
                "pe_ratio": 25.6,
                "pb_ratio": 8.4,
                "market_cap": 2100000000000,
                "industry": "白酒",
                "sector": "食品饮料",
            },
            indicators={"rsi": 58.2},
            risk_data=None,
        )

        self.assertEqual(result["analysis"], "风险分析")
        self.assertIn("市盈率(PE)：25.6", captured["prompt"])
        self.assertIn("市净率(PB)：8.4", captured["prompt"])
        self.assertIn("总市值：2100000000000", captured["prompt"])
        self.assertIn("所属行业：白酒", captured["prompt"])
        self.assertIn("所属板块：食品饮料", captured["prompt"])


if __name__ == "__main__":
    unittest.main()
