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


if __name__ == "__main__":
    unittest.main()
