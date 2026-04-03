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
        )

        self.assertEqual(result["analysis"], "宏观分析结果")
        self.assertEqual(captured["max_tokens"], 4000)
        self.assertEqual(captured["tier"], ModelTier.REASONING)
        self.assertIn("宏观策略分析师", captured["messages"][0]["content"])
        self.assertIn("【市场概况】", captured["messages"][1]["content"])
        self.assertIn("【重要财经新闻】", captured["messages"][1]["content"])


if __name__ == "__main__":
    unittest.main()
