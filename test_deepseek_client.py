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
from prompt_registry import build_messages


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

    def test_call_api_omits_reasoning_by_default(self):
        client = DeepSeekClient.__new__(DeepSeekClient)
        client.model = None
        client.lightweight_model = None
        client.reasoning_model = None
        message = types.SimpleNamespace(reasoning_content="chain", content="最终正文")
        response = types.SimpleNamespace(choices=[types.SimpleNamespace(message=message)])
        client.client = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=MagicMock(return_value=response)
                )
            )
        )

        result = client.call_api([{"role": "user", "content": "hello"}])

        self.assertEqual(result, "最终正文")

    def test_build_messages_renders_external_prompt_templates(self):
        messages = build_messages(
            "stock_analysis/final_decision.system.txt",
            "stock_analysis/final_decision.user.txt",
            symbol="000001",
            name="平安银行",
            current_price="12.34",
            comprehensive_discussion="偏多但需控制回撤。",
            ma20="12.00",
            bb_upper="12.80",
            bb_lower="11.20",
        )

        self.assertEqual(messages[0]["role"], "system")
        self.assertEqual(messages[1]["role"], "user")
        self.assertIn("投资决策专家", messages[0]["content"])
        self.assertIn("股票代码：000001", messages[1]["content"])
        self.assertIn('"rating": "买入/持有/卖出"', messages[1]["content"])

    def test_technical_analysis_uses_external_prompt_template(self):
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
            captured["tier"] = tier
            return "技术分析结果"

        client.call_api = types.MethodType(fake_call_api, client)

        result = client.technical_analysis(
            stock_info={"symbol": "000001", "name": "平安银行", "current_price": 12.34, "change_percent": 1.2},
            stock_data=None,
            indicators={
                "price": 12.34,
                "rsi": 55,
                "volume_ratio": 1.1,
                "chip_data_source": "tushare.cyq_chips/cyq_perf",
                "chip_trade_date": "20260402",
                "chip_peak_shape": "单峰密集",
                "main_chip_peak_price": 12.1,
                "chip_concentration": "高 (56.0%)",
                "average_chip_cost": 12.05,
                "cost_band_70": "11.80-12.40",
                "cost_band_90": "11.30-12.80",
            },
        )

        self.assertEqual(result, "技术分析结果")
        self.assertEqual(captured["tier"], ModelTier.LIGHTWEIGHT)
        self.assertIn("股票技术分析师", captured["messages"][0]["content"])
        self.assertIn("股票代码：000001", captured["messages"][1]["content"])
        self.assertIn("筹码峰结构：", captured["messages"][1]["content"])
        self.assertIn("筹码数据源：tushare.cyq_chips/cyq_perf", captured["messages"][1]["content"])
        self.assertIn("筹码峰形态：单峰密集", captured["messages"][1]["content"])
        self.assertIn("历史行情摘要：暂无可用历史行情。", captured["messages"][1]["content"])

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
            return '{"rating":"buy","target_price":"12","take_profit":"12","stop_loss":"9.2","confidence_level":"8"}'

        client.call_api = types.MethodType(fake_call_api, client)

        result = client.final_decision(
            comprehensive_discussion="Bullish setup with manageable risk.",
            stock_info={"symbol": "000001", "name": "PingAn", "current_price": 10.0},
            indicators={
                "ma20": 9.8,
                "bb_upper": 10.8,
                "bb_lower": 9.2,
                "chip_data_source": "tushare.cyq_chips/cyq_perf",
                "chip_peak_shape": "单峰密集",
                "main_chip_peak_price": 9.9,
                "average_chip_cost": 9.85,
            },
        )

        self.assertEqual(captured["tier"], ModelTier.REASONING)
        self.assertEqual(captured["temperature"], 0.3)
        self.assertEqual(captured["max_tokens"], 4000)
        self.assertFalse(captured["include_reasoning"])
        self.assertEqual(len(captured["messages"]), 2)
        self.assertIn("关键筹码结构", captured["messages"][1]["content"])
        self.assertIn("筹码数据源：tushare.cyq_chips/cyq_perf", captured["messages"][1]["content"])
        self.assertEqual(result["rating"], "买入")
        self.assertEqual(result["confidence_level"], 8.0)

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
            return '前置说明 {"rating":"买入","target_price":"12","take_profit":"12","stop_loss":"9","operation_advice":"分批买入"} 后置说明'

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
