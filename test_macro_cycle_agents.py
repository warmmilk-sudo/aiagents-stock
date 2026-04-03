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
from macro_cycle_agents import MacroCycleAgents


class MacroCycleAgentsTests(unittest.TestCase):
    def test_kondratieff_wave_agent_uses_external_prompt_templates(self):
        captured = {}
        agent = MacroCycleAgents.__new__(MacroCycleAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["messages"] = messages
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            return "康波分析结果"

        agent.deepseek_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.kondratieff_wave_agent("GDP增速回落，CPI温和。")

        self.assertEqual(result["analysis"], "康波分析结果")
        self.assertEqual(captured["max_tokens"], 6000)
        self.assertEqual(captured["tier"], ModelTier.REASONING)
        self.assertIn("康德拉季耶夫长波周期研究专家", captured["messages"][0]["content"])
        self.assertIn("以下是当前中国的宏观经济数据", captured["messages"][1]["content"])
        self.assertIn("GDP增速回落", captured["messages"][1]["content"])


if __name__ == "__main__":
    unittest.main()
