import sys
import types
import unittest
from unittest.mock import MagicMock

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)

from deepseek_client import DeepSeekClient
from model_routing import ModelTier


class DeepSeekClientTests(unittest.TestCase):
    def test_call_api_raises_runtime_error_on_api_failure(self):
        client = DeepSeekClient.__new__(DeepSeekClient)
        client.model = None
        client.lightweight_model = None
        client.reasoning_model = None
        client.client = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=MagicMock(side_effect=ValueError("boom"))
                )
            )
        )

        with self.assertRaisesRegex(RuntimeError, "DeepSeek API调用失败"):
            client.call_api([{"role": "user", "content": "hello"}])

    def test_call_api_omits_reasoning_when_disabled(self):
        client = DeepSeekClient.__new__(DeepSeekClient)
        client.model = None
        client.lightweight_model = None
        client.reasoning_model = None
        message = types.SimpleNamespace(reasoning_content="chain", content='{"ok":true}')
        response = types.SimpleNamespace(choices=[types.SimpleNamespace(message=message)])
        client.client = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=MagicMock(return_value=response)
                )
            )
        )

        result = client.call_api(
            [{"role": "user", "content": "hello"}],
            include_reasoning=False,
        )

        self.assertEqual(result, '{"ok":true}')

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
            include_reasoning=True,
        ):
            captured["messages"] = messages
            captured["model"] = model
            captured["temperature"] = temperature
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            captured["include_reasoning"] = include_reasoning
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
        self.assertFalse(captured["include_reasoning"])
        self.assertEqual(len(captured["messages"]), 2)
        self.assertEqual(result["rating"], "buy")

    def test_final_decision_extracts_json_from_wrapped_text(self):
        client = DeepSeekClient.__new__(DeepSeekClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
        ):
            return '前置说明 {"rating":"买入","target_price":"10","operation_advice":"分批买入"} 后置说明'

        client.call_api = types.MethodType(fake_call_api, client)

        result = client.final_decision(
            comprehensive_discussion="Bullish setup with manageable risk.",
            stock_info={"symbol": "000001", "name": "PingAn", "current_price": 10.0},
            indicators={"ma20": 9.8, "bb_upper": 10.8, "bb_lower": 9.2},
        )

        self.assertEqual(result["rating"], "买入")

    def test_final_decision_raises_on_invalid_json(self):
        client = DeepSeekClient.__new__(DeepSeekClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
        ):
            return "not-json"

        client.call_api = types.MethodType(fake_call_api, client)

        with self.assertRaisesRegex(RuntimeError, "final_decision_invalid_json"):
            client.final_decision(
                comprehensive_discussion="Bullish setup with manageable risk.",
                stock_info={"symbol": "000001", "name": "PingAn", "current_price": 10.0},
                indicators={"ma20": 9.8, "bb_upper": 10.8, "bb_lower": 9.2},
            )


if __name__ == "__main__":
    unittest.main()
