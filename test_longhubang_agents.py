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
from longhubang_agents import LonghubangAgents


class LonghubangAgentsTests(unittest.TestCase):
    def test_youzi_behavior_analyst_uses_external_prompt_templates(self):
        captured = {}
        agent = LonghubangAgents.__new__(LonghubangAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["messages"] = messages
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            return "游资分析结果"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.youzi_behavior_analyst(
            "92科比 | 四川黄金(001337) | 买入:1000000 卖出:0 净流入:1000000",
            {
                "total_records": 12,
                "total_stocks": 5,
                "total_youzi": 3,
                "total_buy_amount": 3000000,
                "total_sell_amount": 1000000,
                "total_net_inflow": 2000000,
                "top_youzi": {"92科比": 1000000},
            },
        )

        self.assertEqual(result["analysis"], "游资分析结果")
        self.assertEqual(captured["max_tokens"], 4000)
        self.assertEqual(captured["tier"], ModelTier.REASONING)
        self.assertIn("游资研究专家", captured["messages"][0]["content"])
        self.assertIn("【龙虎榜数据概况】", captured["messages"][1]["content"])
        self.assertIn("92科比", captured["messages"][1]["content"])


if __name__ == "__main__":
    unittest.main()
