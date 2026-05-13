import sys
import types
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)

import config
from llm_client import LLMClient
from model_routing import ModelTier
from prompt_registry import build_messages


class LLMClientTests(unittest.TestCase):
    def test_call_api_raises_runtime_error_on_api_failure(self):
        client = LLMClient.__new__(LLMClient)
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

        with self.assertRaisesRegex(RuntimeError, "LLM API调用失败"):
            client.call_api([{"role": "user", "content": "hello"}])

    def test_call_api_omits_reasoning_when_disabled(self):
        client = LLMClient.__new__(LLMClient)
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
        client = LLMClient.__new__(LLMClient)
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

    def test_call_api_uses_model_specific_sampling_defaults(self):
        client = LLMClient.__new__(LLMClient)
        client.model = None
        client.lightweight_model = None
        client.reasoning_model = None
        message = types.SimpleNamespace(reasoning_content=None, content="事实回答")
        response = types.SimpleNamespace(choices=[types.SimpleNamespace(message=message)])
        create = MagicMock(return_value=response)
        client.client = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=types.SimpleNamespace(create=create)
            )
        )

        result = client.call_api(
            [{"role": "user", "content": "hello"}],
            model="doubao-2-0-pro",
            sampling_profile="factual",
        )

        self.assertEqual(result, "事实回答")
        expected_temperature, expected_top_p = config.resolve_llm_sampling_params(
            "doubao-2-0-pro",
            profile="factual",
        )
        self.assertEqual(create.call_args.kwargs["temperature"], expected_temperature)
        if expected_top_p < 1.0:
            self.assertEqual(create.call_args.kwargs["top_p"], expected_top_p)
        else:
            self.assertNotIn("top_p", create.call_args.kwargs)

    def test_call_api_retries_transient_failure_then_succeeds(self):
        client = LLMClient.__new__(LLMClient)
        client.model = None
        client.lightweight_model = None
        client.reasoning_model = None
        client.api_retry_count = 2
        client.api_retry_base_delay_seconds = 0.01
        transient_error = RuntimeError("Error code: 500 - {'error': {'message': 'auth_unavailable: no auth available'}}")
        message = types.SimpleNamespace(reasoning_content=None, content="恢复成功")
        response = types.SimpleNamespace(choices=[types.SimpleNamespace(message=message)])
        create = MagicMock(side_effect=[transient_error, response])
        client.client = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=create
                )
            )
        )

        with patch("llm_client.time.sleep", return_value=None) as mocked_sleep:
            result = client.call_api([{"role": "user", "content": "hello"}])

        self.assertEqual(result, "恢复成功")
        self.assertEqual(create.call_count, 2)
        mocked_sleep.assert_called_once()

    def test_call_api_retries_empty_response_then_succeeds(self):
        client = LLMClient.__new__(LLMClient)
        client.model = None
        client.lightweight_model = None
        client.reasoning_model = None
        client.api_retry_count = 2
        client.api_retry_base_delay_seconds = 0.01
        empty_message = types.SimpleNamespace(reasoning_content=None, content="")
        success_message = types.SimpleNamespace(reasoning_content=None, content="团队讨论恢复成功")
        empty_response = types.SimpleNamespace(choices=[types.SimpleNamespace(message=empty_message)])
        success_response = types.SimpleNamespace(choices=[types.SimpleNamespace(message=success_message)])
        create = MagicMock(side_effect=[empty_response, success_response])
        client.client = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=create
                )
            )
        )

        with patch("llm_client.time.sleep", return_value=None) as mocked_sleep:
            result = client.call_api([{"role": "user", "content": "hello"}])

        self.assertEqual(result, "团队讨论恢复成功")
        self.assertEqual(create.call_count, 2)
        mocked_sleep.assert_called_once()

    def test_call_api_does_not_fall_back_when_model_is_not_found(self):
        client = LLMClient.__new__(LLMClient)
        client.model = None
        client.lightweight_model = None
        client.reasoning_model = None
        client.api_retry_count = 0
        client.api_retry_base_delay_seconds = 0.01
        not_found_error = RuntimeError(
            "Error code: 404 - {'error': {'code': 'InvalidEndpointOrModel.NotFound', "
            "'message': 'The model or endpoint doubao-2-0-pro does not exist or you do not have access to it.'}}"
        )
        create = MagicMock(side_effect=not_found_error)
        client.client = types.SimpleNamespace(
            chat=types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=create
                )
            )
        )

        with patch.object(
            client,
            "_get_client_for_model",
            return_value=client.client,
        ), self.assertRaisesRegex(RuntimeError, "LLM API调用失败"):
            client.call_api([{"role": "user", "content": "hello"}], tier=ModelTier.REASONING)

        self.assertEqual(create.call_count, 1)

    def test_build_messages_renders_external_prompt_templates(self):
        messages = build_messages(
            "stock_analysis/final_decision.system.txt",
            "stock_analysis/final_decision.user.txt",
            symbol="000001",
            name="平安银行",
            current_price="12.34",
            position_status="未持仓",
            rating_options="买入/强烈买入/观望",
            comprehensive_discussion="偏多但需控制回撤。",
            ma20="12.00",
            bb_upper="12.80",
            bb_lower="11.20",
        )

        self.assertEqual(messages[0]["role"], "system")
        self.assertEqual(messages[1]["role"], "user")
        self.assertIn("投资决策专家", messages[0]["content"])
        self.assertIn("股票代码：000001", messages[1]["content"])
        self.assertIn("若当前状态为“未持仓”，只能输出：买入 / 强烈买入 / 观望", messages[0]["content"])
        self.assertIn("基线质量硬约束", messages[0]["content"])
        self.assertIn("stop_loss < entry_range下沿 <= entry_range上沿 < take_profit", messages[0]["content"])

    def test_technical_analysis_uses_external_prompt_template(self):
        captured = {}
        client = LLMClient.__new__(LLMClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=None,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
            sampling_profile="default",
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
        client = LLMClient.__new__(LLMClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=None,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
            sampling_profile="default",
        ):
            captured["messages"] = messages
            captured["model"] = model
            captured["temperature"] = temperature
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            captured["include_reasoning"] = include_reasoning
            captured["sampling_profile"] = sampling_profile
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
        self.assertIsNone(captured["temperature"])
        self.assertEqual(captured["sampling_profile"], "factual")
        self.assertEqual(captured["max_tokens"], 4000)
        self.assertFalse(captured["include_reasoning"])
        self.assertEqual(len(captured["messages"]), 2)
        self.assertIn("关键筹码结构", captured["messages"][1]["content"])
        self.assertIn("筹码数据源：tushare.cyq_chips/cyq_perf", captured["messages"][1]["content"])
        self.assertEqual(result["rating"], "买入")
        self.assertEqual(result["confidence_level"], 8.0)

    def test_final_decision_uses_position_rating_options_for_portfolio_stock(self):
        captured = {}
        client = LLMClient.__new__(LLMClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
            sampling_profile="default",
        ):
            captured["messages"] = messages
            return '{"rating":"加仓","target_price":"12","take_profit":"12","stop_loss":"9.2","confidence_level":"8"}'

        client.call_api = types.MethodType(fake_call_api, client)

        result = client.final_decision(
            comprehensive_discussion="Position can be increased on confirmation.",
            stock_info={"symbol": "000001", "name": "PingAn", "current_price": 10.0, "has_position": True},
            indicators={"ma20": 9.8, "bb_upper": 10.8, "bb_lower": 9.2},
        )

        self.assertIn("当前状态：已持仓", captured["messages"][1]["content"])
        self.assertIn("加仓 / 持有 / 减仓 / 卖出", captured["messages"][0]["content"])
        self.assertEqual(result["rating"], "加仓")

    def test_final_decision_includes_existing_holding_swing_baseline_constraints(self):
        captured = {}
        client = LLMClient.__new__(LLMClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
            sampling_profile="default",
        ):
            captured["messages"] = messages
            return '{"rating":"持有","target_price":"12","take_profit":"12","stop_loss":"9.2","confidence_level":"8","swing_type":"标准波段"}'

        client.call_api = types.MethodType(fake_call_api, client)

        result = client.final_decision(
            comprehensive_discussion="继续按原节奏做波段管理。",
            stock_info={"symbol": "000001", "name": "PingAn", "current_price": 10.0, "has_position": True},
            indicators={"ma20": 9.8, "bb_upper": 10.8, "bb_lower": 9.2},
            strategy_context={
                "swing_type": "标准波段",
                "holding_period": "5-15个交易日",
                "strategy_style_summary": "动量突破 / 均值回归",
                "intraday_execution_preference": "优先围绕阶段性主升或反弹主段执行",
            },
        )

        self.assertIn("持仓波段基线约束", captured["messages"][1]["content"])
        self.assertIn("已确认波段类型：标准波段", captured["messages"][1]["content"])
        self.assertIn("默认输出同一 `swing_type`", captured["messages"][0]["content"])
        self.assertEqual(result["rating"], "持有")

    def test_fundamental_analysis_uses_expanded_token_budget_and_business_summary(self):
        captured = {}
        client = LLMClient.__new__(LLMClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
            sampling_profile="default",
        ):
            captured["messages"] = messages
            captured["max_tokens"] = max_tokens
            captured["tier"] = tier
            return "基本面分析结果"

        client.call_api = types.MethodType(fake_call_api, client)

        result = client.fundamental_analysis(
            stock_info={
                "symbol": "002050",
                "name": "三花智控",
                "current_price": 22.5,
                "market_cap": 80000000000,
                "sector": "汽车热管理",
                "industry": "家电零部件",
                "pe_ratio": 28.1,
                "pb_ratio": 5.2,
                "ps_ratio": 3.5,
                "business_summary": "制冷空调电器零部件与汽车热管理系统零部件双主业。",
            },
            financial_data={
                "financial_ratios": {
                    "销售毛利率": "27.1%",
                    "净利润同比增长": "24.3%",
                }
            },
            quarterly_data=None,
        )

        self.assertEqual(result, "基本面分析结果")
        self.assertEqual(captured["tier"], ModelTier.REASONING)
        self.assertGreaterEqual(captured["max_tokens"], 12000)
        self.assertIn("主营业务/业务结构概况", captured["messages"][1]["content"])
        self.assertIn("制冷空调电器零部件与汽车热管理系统零部件双主业。", captured["messages"][1]["content"])

    def test_final_decision_extracts_json_from_wrapped_text(self):
        client = LLMClient.__new__(LLMClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
            sampling_profile="default",
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
        client = LLMClient.__new__(LLMClient)

        def fake_call_api(
            self,
            messages,
            model=None,
            temperature=0.7,
            max_tokens=2000,
            tier=None,
            include_reasoning=True,
            sampling_profile="default",
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
