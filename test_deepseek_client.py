import types
import unittest

from deepseek_client import DeepSeekClient
from model_routing import ModelTier


class DeepSeekClientTests(unittest.TestCase):
    def test_final_decision_uses_reasoning_tier(self):
        captured = {}
        client = DeepSeekClient.__new__(DeepSeekClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
        ):
            captured["messages"] = messages
            captured["model"] = model
            captured["temperature"] = temperature
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            return '{"rating":"buy","target_price":"10"}'

        client.call_api = types.MethodType(fake_call_api, client)

        result = client.final_decision(
            comprehensive_discussion="Bullish setup with manageable risk.",
            stock_info={"symbol": "000001", "name": "PingAn", "current_price": 10.0},
            indicators={"ma20": 9.8, "bb_upper": 10.8, "bb_lower": 9.2},
        )

        self.assertEqual(captured["tier"], ModelTier.REASONING)
        self.assertEqual(captured["temperature"], 0.3)
        self.assertEqual(captured["max_tokens"], 4000)
        self.assertEqual(len(captured["messages"]), 2)
        self.assertEqual(result["rating"], "buy")


if __name__ == "__main__":
    unittest.main()
