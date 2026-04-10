import sys
import types
import unittest

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)

from model_routing import ModelTier
from sector_strategy_agents import SectorStrategyAgents


class SectorStrategyAgentsTests(unittest.TestCase):
    def test_macro_strategist_agent_uses_external_prompt_templates(self):
        captured = {}
        agent = SectorStrategyAgents.__new__(SectorStrategyAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["messages"] = messages
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            return "宏观分析结果"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.macro_strategist_agent(
            market_data={
                "sh_index": {"close": 3200, "change_pct": 0.5},
                "total_stocks": 5000,
                "up_count": 3000,
                "up_ratio": 60.0,
                "down_count": 2000,
                "limit_up": 80,
                "limit_down": 5,
            },
            news_data=[{"publish_time": "2026-04-03 09:00", "title": "央行降准", "content": "释放长期流动性"}],
            analysis_date="2026-04-09 22:51:31",
        )

        self.assertEqual(result["analysis"], "宏观分析结果")
        self.assertEqual(captured["max_tokens"], 4000)
        self.assertEqual(captured["tier"], ModelTier.REASONING)
        self.assertIn("宏观策略分析师", captured["messages"][0]["content"])
        self.assertIn("分析基准日期", captured["messages"][1]["content"])
        self.assertIn("2026-04-09 22:51:31", captured["messages"][1]["content"])
        self.assertIn("【市场概况】", captured["messages"][1]["content"])
        self.assertIn("【重要财经新闻】", captured["messages"][1]["content"])

    def test_sector_diagnostician_repairs_stale_year_references(self):
        calls = []
        agent = SectorStrategyAgents.__new__(SectorStrategyAgents)

        def fake_call_api(messages, max_tokens=None, tier=None, temperature=None):
            calls.append({
                "messages": messages,
                "max_tokens": max_tokens,
                "tier": tier,
                "temperature": temperature,
            })
            if len(calls) == 1:
                return "板块静态PE约25-30倍，2024年业绩增速预计20%-30%，估值合理。"
            return "板块静态PE约25-30倍。当前输入未提供相关估值/业绩数据，无法做更细的年度量化判断。"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.sector_diagnostician_agent(
            sectors_data={
                "通信线缆及配套": {
                    "change_pct": 4.37,
                    "turnover": 12.27,
                    "top_stock": "汇源通信",
                    "top_stock_change": 10.0,
                    "up_count": 11,
                    "down_count": 1,
                }
            },
            concepts_data={},
            market_data={"sh_index": {"close": 3200, "change_pct": 0.5}, "total_stocks": 5000, "up_count": 3000, "up_ratio": 60.0, "down_count": 2000},
            analysis_date="2026-04-09 22:51:31",
        )

        self.assertEqual(len(calls), 2)
        self.assertIn("分析基准日期", calls[0]["messages"][1]["content"])
        self.assertIn("2026-04-09 22:51:31", calls[0]["messages"][1]["content"])
        self.assertIn("时效性问题", calls[1]["messages"][1]["content"])
        self.assertNotIn("2024年", result["analysis"])
        self.assertIn("当前输入未提供相关估值/业绩数据", result["analysis"])

    def test_macro_agent_repairs_stale_year_references(self):
        calls = []
        agent = SectorStrategyAgents.__new__(SectorStrategyAgents)

        def fake_call_api(messages, max_tokens=None, tier=None, temperature=None):
            calls.append({
                "messages": messages,
                "max_tokens": max_tokens,
                "tier": tier,
                "temperature": temperature,
            })
            if len(calls) == 1:
                return "当前宏观环境延续2024年宽松周期。"
            return "当前输入未提供相关数据，无法做更细的年度量化判断，应仅按当日新闻与市场结构解读。"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.macro_strategist_agent(
            market_data={
                "sh_index": {"close": 3200, "change_pct": 0.5},
                "total_stocks": 5000,
                "up_count": 3000,
                "up_ratio": 60.0,
                "down_count": 2000,
                "limit_up": 80,
                "limit_down": 5,
            },
            news_data=[{"publish_time": "2026-04-09 09:00", "title": "财政政策发力", "content": "提振市场风险偏好"}],
            analysis_date="2026-04-09 22:51:31",
        )

        self.assertEqual(len(calls), 2)
        self.assertIn("宏观策略师", calls[1]["messages"][1]["content"])
        self.assertIn("分析基准日期", calls[1]["messages"][1]["content"])
        self.assertNotIn("2024年", result["analysis"])
        self.assertIn("当前输入未提供相关数据", result["analysis"])


if __name__ == "__main__":
    unittest.main()
