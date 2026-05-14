import json
import unittest
from unittest.mock import MagicMock, patch

import requests

import config
from investment_action_utils import normalize_strategy_context
from smart_monitor_deepseek import SmartMonitorDeepSeek


class SmartMonitorDeepSeekTests(unittest.TestCase):
    def test_enforce_action_policy_keeps_buy_allowed(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        decision = client._enforce_action_policy({"action": "BUY", "reasoning": "test"}, has_position=True)

        self.assertEqual(decision["action"], "BUY")
        self.assertNotIn("降级为 HOLD", decision["reasoning"])

    def test_enforce_action_policy_downgrades_sell_when_no_position(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        decision = client._enforce_action_policy({"action": "SELL", "reasoning": "test"}, has_position=False)

        self.assertEqual(decision["action"], "HOLD")
        self.assertEqual(decision["action_detail"], "观望")
        self.assertEqual(decision["risk_level"], "high")
        self.assertIn("当前无持仓", decision["reasoning"])

    def test_enforce_action_policy_downgrades_sell_when_t1_blocks_same_day_exit(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        decision = client._enforce_action_policy(
            {"action": "SELL", "action_detail": "减仓", "reasoning": "test"},
            has_position=True,
            can_sell_today=False,
        )

        self.assertEqual(decision["action"], "HOLD")
        self.assertEqual(decision["action_detail"], "持有")
        self.assertEqual(decision["swing_execution_mode"], "watch_hold")
        self.assertEqual(decision["risk_level"], "high")
        self.assertIn("T+1", decision["reasoning"])

    def test_enforce_action_policy_keeps_action_detail_for_sell_variants(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        trim_decision = client._enforce_action_policy(
            {"action": "SELL", "action_detail": "减仓", "reasoning": "趋势仍强但接近止盈位，先锁定部分利润。"},
            has_position=True,
        )
        defensive_trim_decision = client._enforce_action_policy(
            {"action": "SELL", "action_detail": "减仓", "reasoning": "15/30/60分钟走坏，先收缩风险敞口。"},
            has_position=True,
        )
        exit_decision = client._enforce_action_policy(
            {"action": "SELL", "action_detail": "清仓", "reasoning": "test"},
            has_position=True,
        )

        self.assertEqual(trim_decision["action_detail"], "减仓")
        self.assertEqual(trim_decision["swing_execution_mode"], "proactive_trim")
        self.assertEqual(trim_decision["action_ratio_pct"], 25)
        self.assertEqual(defensive_trim_decision["action_detail"], "减仓")
        self.assertEqual(defensive_trim_decision["swing_execution_mode"], "defensive_trim")
        self.assertEqual(defensive_trim_decision["action_ratio_pct"], 35)
        self.assertEqual(exit_decision["action_detail"], "清仓")
        self.assertEqual(exit_decision["action_ratio_pct"], 100)

    def test_attach_execution_targets_for_trim_sell_uses_current_position(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        decision = client._attach_execution_targets(
            {
                "action": "SELL",
                "action_detail": "减仓",
                "action_ratio_pct": 30,
            },
            account_info={"current_position": {"position_pct": 0.4}},
            risk_profile={"position_size_pct": 20, "total_position_pct": 100, "stop_loss_pct": 5, "take_profit_pct": 10},
        )

        self.assertEqual(decision["current_position_pct"], 40.0)
        self.assertEqual(decision["target_position_pct"], 28.0)
        self.assertEqual(decision["position_delta_pct"], -12.0)
        self.assertEqual(decision["trade_intent"], "reduce")

    def test_attach_execution_targets_for_buy_caps_target_position(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        decision = client._attach_execution_targets(
            {
                "action": "BUY",
                "action_detail": "加仓",
                "action_ratio_pct": 15,
            },
            account_info={"current_position": {"position_pct": 0.12}},
            risk_profile={"position_size_pct": 20, "total_position_pct": 100, "stop_loss_pct": 5, "take_profit_pct": 10},
        )

        self.assertEqual(decision["current_position_pct"], 12.0)
        self.assertEqual(decision["target_position_pct"], 20.0)
        self.assertEqual(decision["position_delta_pct"], 8.0)
        self.assertEqual(decision["trade_intent"], "add")

    def test_build_prompt_includes_holding_position_ratio(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        prompt = client._build_a_stock_prompt(
            "600519",
            market_data={
                "name": "贵州茅台",
                "current_price": 12.0,
                "volume": 120000,
            },
            account_info={
                "available_cash": 88000,
                "total_value": 100000,
                "configured_total_assets": 100000,
                "total_market_value": 12000,
                "position_usage_pct": 0.12,
                "positions_count": 1,
                "current_position": {
                    "quantity": 1000,
                    "cost_price": 10.0,
                    "current_price": 12.0,
                    "market_value": 12000,
                    "position_pct": 0.12,
                },
            },
            has_position=True,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃",
                "can_trade": True,
                "beijing_hour": 10,
                "beijing_time": "10:30",
            },
            position_cost=10.0,
            position_quantity=1000,
            position_date="2026-05-10",
        )

        self.assertIn("持仓占总资产: 12.00%", prompt)
        self.assertIn("仓位计算口径: 按配置总资产计算", prompt)

    def test_parse_decision_repairs_json_like_response(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        ai_response = """
```json
{
  action: BUY,
  action_detail: 加仓,
  swing_execution_mode: breakout_add,
  confidence: 0.82,
  reasoning: "量价配合良好，趋势保持上行，可继续观察突破延续。",
  position_size_pct: 20,
  stop_loss_pct: 5.0,
  take_profit_pct: 10.0,
  risk_level: medium,
  key_price_levels: {
    support: 12.34,
    resistance: 13.10,
    stop_loss: 11.72,
  },
  monitor_levels: {
    entry_min: 12.10,
    entry_max: 12.40,
    take_profit: 13.20,
    take_profit_max: 13.80,
    stop_loss: 11.70,
  },
}
```
"""

        decision = client._parse_decision(ai_response, has_position=True)

        self.assertEqual(decision["action"], "BUY")
        self.assertEqual(decision["action_ratio_pct"], 20)
        self.assertEqual(decision["confidence"], 82)
        self.assertEqual(decision["risk_level"], "medium")
        self.assertEqual(decision["swing_execution_mode"], "breakout_add")
        self.assertEqual(decision["monitor_levels"]["entry_min"], 12.1)
        self.assertEqual(decision["monitor_levels"]["take_profit"], 13.2)
        self.assertEqual(decision["monitor_levels"]["take_profit_max"], 13.8)

    def test_parse_decision_normalizes_chinese_action_and_percent_strings(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        ai_response = """
{
  "action": "买入",
  "confidence": "85%",
  "reasoning": "分时量能放大，短线趋势仍然偏强。",
  "risk_level": "中",
  "monitor_levels": {
    "entry_min": "12.10",
    "entry_max": "12.40",
    "take_profit": "13.20",
    "stop_loss": "11.70"
  }
}
"""

        decision = client._parse_decision(ai_response)

        self.assertEqual(decision["action"], "BUY")
        self.assertEqual(decision["action_ratio_pct"], 20)
        self.assertEqual(decision["confidence"], 85)
        self.assertEqual(decision["risk_level"], "medium")
        self.assertEqual(decision["swing_execution_mode"], "pullback_entry")
        self.assertEqual(decision["monitor_levels"]["stop_loss"], 11.7)

    def test_parse_decision_falls_back_to_account_risk_profile(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        ai_response = """
{
  "action": "HOLD",
  "confidence": 78,
  "reasoning": "账户已有基准风控，当前没有新的偏离理由。",
  "risk_level": "中",
  "monitor_levels": {
    "entry_min": 12.10,
    "entry_max": 12.40,
    "take_profit": 13.20,
    "stop_loss": 11.70
  }
}
"""

        decision = client._parse_decision(
            ai_response,
            risk_profile={
                "position_size_pct": 33,
                "total_position_pct": 80,
                "stop_loss_pct": 7,
                "take_profit_pct": 18,
            },
        )

        self.assertEqual(decision["position_size_pct"], 33)
        self.assertEqual(decision["stop_loss_pct"], 7.0)
        self.assertEqual(decision["take_profit_pct"], 18.0)

    def test_parse_decision_preserves_baseline_and_memory_fields(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        ai_response = """
{
  "action": "HOLD",
  "confidence": 73,
  "reasoning": "战略基线仍有效，历史记忆提示谨慎追高，当前30/60分钟确认不足。",
  "risk_level": "中",
  "baseline_relation": "partially_deviated",
  "matched_baseline_conditions": ["价格仍在计划区间"],
  "unmet_baseline_conditions": ["30/60分钟放量确认"],
  "baseline_conflict_score": 58,
  "memory_evidence_ids": [12, "18"],
  "deviation_reason": "历史假突破较多，等待更高一级别确认。",
  "monitor_levels": {
    "entry_min": 12.10,
    "entry_max": 12.40,
    "take_profit": 13.20,
    "stop_loss": 11.70
  }
}
"""

        decision = client._parse_decision(ai_response)

        self.assertEqual(decision["baseline_relation"], "partially_deviated")
        self.assertEqual(decision["baseline_conflict_score"], 58)
        self.assertEqual(decision["memory_evidence_ids"], [12, 18])
        self.assertNotIn("历史记忆", decision["reasoning"])
        self.assertNotIn("历史假突破", decision["deviation_reason"])
        self.assertIn("30/60分钟放量确认", decision["unmet_baseline_conditions"])

    def test_build_prompt_includes_internal_memory_section(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        prompt = client._build_a_stock_prompt(
            "600000",
            market_data={
                "name": "测试银行",
                "current_price": 10.2,
                "change_pct": 1.2,
                "update_time": "2026-05-12 10:30:00",
            },
            account_info={
                "available_cash": 100000,
                "total_value": 100000,
                "total_market_value": 0,
                "position_usage_pct": 0,
                "positions_count": 0,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃",
                "can_trade": True,
                "beijing_hour": 10,
                "beijing_time": "10:30",
            },
            strategy_context={
                "rating": "买入",
                "entry_min": 10.0,
                "entry_max": 10.5,
                "take_profit": 12.0,
                "stop_loss": 9.5,
            },
            memory_context={
                "long_term_profile": "该股历史上多次追高后冲高回落。",
                "working_memories": [
                    {"analysis_date": "2026-05-10", "decision_summary": "等待回踩确认。"}
                ],
                "recalled_facts": [
                    {
                        "id": 7,
                        "timestamp": "2026-05-09",
                        "fact_content": "测试银行历史假突破较多，进场需要30分钟确认。",
                        "category": "risk",
                        "_active_score": 88,
                    }
                ],
            },
        )

        self.assertIn("[INTERNAL_MEMORY_CONTEXT]", prompt)
        self.assertIn("谨慎追高", prompt)
        self.assertIn("#7", prompt)
        self.assertIn("不得在 reasoning", prompt)

    def test_memory_disclosure_is_removed_from_visible_decision_text(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        decision = client._parse_decision(
            """
{
  "action": "HOLD",
  "confidence": 73,
  "reasoning": "战略基线仍有效。历史记忆提示谨慎追高，事实编号#7需要参考。当前30/60分钟确认不足，继续观望。",
  "risk_level": "中",
  "deviation_reason": "历史记忆显示假突破较多，等待更高一级别确认。",
  "memory_evidence_ids": [7],
  "monitor_levels": {
    "entry_min": 12.10,
    "entry_max": 12.40,
    "take_profit": 13.20,
    "stop_loss": 11.70
  }
}
"""
        )

        self.assertNotIn("历史记忆", decision["reasoning"])
        self.assertNotIn("事实编号", decision["reasoning"])
        self.assertIn("当前30/60分钟确认不足", decision["reasoning"])
        self.assertEqual(decision["deviation_reason"], "按战略基线、实时盘面与风控约束综合评估，当前维持既定执行意图。")
        self.assertEqual(decision["memory_evidence_ids"], [7])

    def test_parse_decision_strict_rejects_reasoning_action_conflict(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        ai_response = """
{
  "action": "BUY",
  "action_detail": "加仓",
  "confidence": 81,
  "reasoning": "当前处于基线加仓区间，但尾盘不宜盲目动作，建议维持持有，等待分时转强后再考虑加仓，暂不执行加仓。",
  "risk_level": "中",
  "monitor_levels": {
    "entry_min": 27.4,
    "entry_max": 27.6,
    "take_profit": 30.2,
    "stop_loss": 26.8
  }
}
"""
        with self.assertRaisesRegex(Exception, "reasoning 冲突"):
            client._parse_decision_strict(ai_response, has_position=True)

    def test_analyze_stock_and_decide_retries_when_decision_is_self_conflicting(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        client.get_trading_session = MagicMock(return_value={"session": "上午盘"})
        client._build_reasoning_context = MagicMock(return_value={})
        client._build_prompt_messages = MagicMock(return_value=[{"role": "system", "content": "system"}])
        client._normalize_reasoning_output = MagicMock(side_effect=lambda decision, **_: decision["reasoning"])

        invalid_response = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "action": "BUY",
                                "action_detail": "加仓",
                                "confidence": 82,
                                "reasoning": "建议维持持有，等待转强后再考虑加仓，暂不执行加仓。",
                                "risk_level": "中",
                                "monitor_levels": {
                                    "entry_min": 27.4,
                                    "entry_max": 27.6,
                                    "take_profit": 30.2,
                                    "stop_loss": 26.8,
                                },
                            },
                            ensure_ascii=False,
                        )
                    }
                }
            ]
        }
        valid_response = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "action": "HOLD",
                                "action_detail": "持有",
                                "swing_execution_mode": "watch_hold",
                                "confidence": 76,
                                "reasoning": "当前虽进入基线加仓区，但分时未转强，建议维持持有，等待进一步确认后再评估。",
                                "risk_level": "中",
                                "monitor_levels": {
                                    "entry_min": 27.4,
                                    "entry_max": 27.6,
                                    "take_profit": 30.2,
                                    "stop_loss": 26.8,
                                },
                            },
                            ensure_ascii=False,
                        )
                    }
                }
            ]
        }
        client.chat_completion = MagicMock(side_effect=[invalid_response, valid_response])

        result = client.analyze_stock_and_decide(
            stock_code="600089",
            market_data={"name": "特变电工"},
            account_info={"current_position": {"position_pct": 0.2}},
            has_position=True,
            can_sell_today=True,
            risk_profile={"position_size_pct": 20, "total_position_pct": 100, "stop_loss_pct": 5, "take_profit_pct": 10},
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["decision"]["action"], "HOLD")
        self.assertEqual(result["decision"]["action_detail"], "持有")
        self.assertEqual(client.chat_completion.call_count, 2)

    @patch("smart_monitor_deepseek.requests.post")
    def test_chat_completion_defaults_to_lightweight_model(self, mock_post):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        mock_post.return_value = response

        client = SmartMonitorDeepSeek(api_key="test-key")
        client.lightweight_model = "light-model"
        client.reasoning_model = "heavy-model"

        client.chat_completion(
            messages=[{"role": "user", "content": "test"}],
        )

        self.assertEqual(mock_post.call_args.kwargs["json"]["model"], "light-model")
        self.assertEqual(mock_post.call_args.kwargs["json"]["temperature"], config.LLM_FACTUAL_TEMPERATURE)
        if config.LLM_DEFAULT_TOP_P < 1.0:
            self.assertEqual(mock_post.call_args.kwargs["json"]["top_p"], config.LLM_DEFAULT_TOP_P)
        else:
            self.assertNotIn("top_p", mock_post.call_args.kwargs["json"])

    @patch("smart_monitor_deepseek.requests.post")
    def test_chat_completion_uses_provider_api_model_name_for_mapped_alias(self, mock_post):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        mock_post.return_value = response

        client = SmartMonitorDeepSeek(api_key="test-key")
        with patch.dict(
            config.MODEL_API_NAME_BY_CONFIG_ENV["VOICE_CONFIG"],
            {"doubao-2-0-mini": "doubao-seed-2-0-mini-260425"},
        ):
            client.chat_completion(
                messages=[{"role": "user", "content": "test"}],
                model="doubao-2-0-mini",
            )

        self.assertEqual(
            mock_post.call_args.kwargs["json"]["model"],
            "doubao-seed-2-0-mini-260425",
        )

    @patch("smart_monitor_deepseek.time_module.sleep", return_value=None)
    @patch("smart_monitor_deepseek.requests.post")
    def test_chat_completion_retries_timeout_and_uses_reasoner_budget(self, mock_post, _mock_sleep):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"choices": [{"message": {"content": "{}"}}]}
        mock_post.side_effect = [
            requests.exceptions.ReadTimeout("slow response"),
            response,
        ]

        client = SmartMonitorDeepSeek(api_key="test-key")
        client.http_timeout_seconds = 31
        client.http_retry_count = 1
        client.reasoning_max_tokens = 3200

        result = client.chat_completion(
            messages=[{"role": "user", "content": "test"}],
            model="deepseek-reasoner",
            max_tokens=2000,
        )

        self.assertEqual(result, {"choices": [{"message": {"content": "{}"}}]})
        self.assertEqual(mock_post.call_count, 2)
        for call in mock_post.call_args_list:
            self.assertEqual(call.kwargs["timeout"], (10, 31))
            self.assertEqual(call.kwargs["json"]["max_tokens"], 3200)

    @patch("smart_monitor_deepseek.time_module.sleep", return_value=None)
    @patch("smart_monitor_deepseek.requests.post")
    def test_chat_completion_retries_http_500(self, mock_post, _mock_sleep):
        first_response = MagicMock()
        first_response.status_code = 500
        first_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "500 Server Error",
            response=first_response,
        )
        second_response = MagicMock()
        second_response.raise_for_status.return_value = None
        second_response.json.return_value = {"choices": [{"message": {"content": "{}"}}]}
        mock_post.side_effect = [first_response, second_response]

        client = SmartMonitorDeepSeek(api_key="test-key")
        client.http_timeout_seconds = 31
        client.http_retry_count = 1

        result = client.chat_completion(
            messages=[{"role": "user", "content": "test"}],
            model="gemini-3-flash",
        )

        self.assertEqual(result, {"choices": [{"message": {"content": "{}"}}]})
        self.assertEqual(mock_post.call_count, 2)

    @patch("smart_monitor_deepseek.requests.post")
    def test_chat_completion_keeps_explicit_higher_max_tokens(self, mock_post):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        mock_post.return_value = response

        client = SmartMonitorDeepSeek(api_key="test-key")
        client.reasoning_max_tokens = 3000

        client.chat_completion(
            messages=[{"role": "user", "content": "test"}],
            model="deepseek-reasoner",
            max_tokens=4800,
        )

        self.assertEqual(mock_post.call_args.kwargs["json"]["max_tokens"], 4800)

    def test_build_prompt_uses_intraday_projected_volume_context(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        prompt = client._build_a_stock_prompt(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "data_source": "tdx",
                "update_time": "2026-04-07 10:30:00",
                "current_price": 1650.0,
                "change_pct": 1.8,
                "change_amount": 29.2,
                "high": 1658.0,
                "low": 1628.0,
                "open": 1632.0,
                "pre_close": 1620.8,
                "volume": 120000,
                "amount": 850000000.0,
                "ma5": 1645.0,
                "ma20": 1620.0,
                "ma60": 1580.0,
                "trend": "up",
                "macd_dif": 1.2,
                "macd_dea": 1.0,
                "macd": 0.4,
                "rsi6": 62.0,
                "rsi12": 58.0,
                "rsi24": 55.0,
                "kdj_k": 70.0,
                "kdj_d": 65.0,
                "kdj_j": 80.0,
                "boll_upper": 1680.0,
                "boll_mid": 1638.0,
                "boll_lower": 1596.0,
                "boll_position": "中轨上方",
                "vol_ma5": 240000.0,
                "volume_ratio": 1.35,
                "realtime_freshness": {
                    "asof_time": "2026-04-07 10:30:00",
                    "is_trading_now": True,
                    "intraday_decision_ready": False,
                    "overall_status": "degraded",
                    "summary": "存在同日行情或盘中快照，但新鲜度不足；可参考方向，不宜过度依赖盘中节奏。",
                    "quote": {"timestamp": "2026-04-07 10:30:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "N/A", "status": "unavailable"},
                    "trade": {"timestamp": "N/A", "status": "unavailable"},
                    "minute_quality": {"coverage_ratio": None, "max_gap": 0, "label": "未提供分时质量"},
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
        )

        self.assertIn("当前累计成交量", prompt)
        self.assertIn("交易时段进度", prompt)
        self.assertIn("按当前节奏折算全天成交量", prompt)
        self.assertIn("折算全天成交量/5日均量", prompt)
        self.assertIn("当前盘中决策不使用大盘/板块背景", prompt)
        self.assertIn("[REALTIME_FRESHNESS]", prompt)
        self.assertIn("盘中执行是否可直接依赖实时流: 否", prompt)
        self.assertIn("分时质量: 未提供分时质量", prompt)
        self.assertIn("盘中累计成交量不能直接与历史全天均量比较", prompt)
        self.assertIn("实时量比: 1.35 (放量)", prompt)
        self.assertIn("折算全天成交量/5日均量: 2.00 (放量)", prompt)

    def test_build_prompt_condenses_long_strategy_summary(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        prompt = client._build_a_stock_prompt(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "current_price": 1650.0,
                "volume": 120000,
                "realtime_freshness": {
                    "asof_time": "2026-04-10 10:30:00",
                    "is_trading_now": True,
                    "intraday_decision_ready": True,
                    "overall_status": "ready",
                    "summary": "TDX 分时/逐笔时间戳足够新鲜，且分时覆盖质量可接受，可用于盘中执行判断。",
                    "quote": {"timestamp": "2026-04-10 10:30:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "2026-04-10 10:29:00", "status": "fresh"},
                    "trade": {"timestamp": "2026-04-10 10:29:58", "status": "fresh"},
                    "minute_quality": {"coverage_ratio": 1.0, "max_gap": 0, "label": "分时覆盖完整"},
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
            strategy_context={
                "analysis_date": "2026-04-10 09:30:00",
                "analysis_source": "research",
                "rating": "买入",
                "summary": (
                    "1. 当前不追高，优先等回踩确认。"
                    "2. 若回踩到支撑区并缩量企稳，可分批考虑。"
                    "3. 若放量跌破止损位，盘中不继续硬扛。"
                    "4. 其余细节属于盘后研究展开内容，不需要原样搬到盘中执行。"
                ) * 20,
                "entry_min": 1610.0,
                "entry_max": 1660.0,
                "take_profit": 1720.0,
                "stop_loss": 1570.0,
            },
        )

        self.assertIn("当前不追高，优先等回踩确认", prompt)
        self.assertIn("若回踩到支撑区并缩量企稳，可分批考虑", prompt)
        self.assertNotIn("盘后研究展开内容，不需要原样搬到盘中执行", prompt)
        self.assertLess(len(prompt), 12000)

    def test_build_prompt_messages_uses_template_registry_layout(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        messages = client._build_prompt_messages(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "current_price": 1650.0,
                "change_pct": 1.8,
                "change_amount": 29.2,
                "volume": 120000,
                "ma5": 1645.0,
                "ma20": 1620.0,
                "ma60": 1580.0,
                "trend": "up",
                "macd_dif": 1.2,
                "macd_dea": 1.0,
                "macd": 0.4,
                "rsi6": 62.0,
                "rsi12": 58.0,
                "rsi24": 55.0,
                "kdj_k": 70.0,
                "kdj_d": 65.0,
                "kdj_j": 80.0,
                "boll_upper": 1680.0,
                "boll_mid": 1638.0,
                "boll_lower": 1596.0,
                "boll_position": "中轨上方",
                "vol_ma5": 240000.0,
                "volume_ratio": 1.35,
                "realtime_freshness": {
                    "asof_time": "2026-04-10 10:30:00",
                    "is_trading_now": True,
                    "intraday_decision_ready": True,
                    "overall_status": "ready",
                    "summary": "TDX 分时/逐笔时间戳足够新鲜，且分时覆盖质量可接受，可用于盘中执行判断。",
                    "quote": {"timestamp": "2026-04-10 10:30:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "2026-04-10 10:29:00", "status": "fresh"},
                    "trade": {"timestamp": "2026-04-10 10:29:58", "status": "fresh"},
                    "minute_quality": {"coverage_ratio": 1.0, "max_gap": 0, "label": "分时覆盖完整"},
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
            risk_profile={
                "position_size_pct": 25,
                "total_position_pct": 70,
                "stop_loss_pct": 6,
                "take_profit_pct": 15,
            },
        )

        self.assertEqual(messages[0]["role"], "system")
        self.assertEqual(messages[1]["role"], "user")
        self.assertIn("你是一位资深的A股盘中执行分析专家", messages[0]["content"])
        self.assertIn("当前共享单票仓位上限：25%", messages[0]["content"])
        self.assertIn("当前任务模式：空仓建仓模式", messages[0]["content"])
        self.assertIn("允许动作：BUY / HOLD", messages[0]["content"])
        self.assertIn("禁止动作：SELL、减仓、止盈卖出", messages[0]["content"])
        self.assertIn("若 `strategy_context` 已提供完整的 `entry_min / entry_max / take_profit / stop_loss`，优先把它们作为初始参考", messages[0]["content"])
        self.assertIn("`take_profit` 则允许根据实时强弱更主动地上修或微调", messages[0]["content"])
        self.assertIn("若给出了 `take_profit_max`，必须满足 `take_profit <= take_profit_max`", messages[0]["content"])
        self.assertIn("`stop_loss < entry_min <= entry_max < take_profit`", messages[0]["content"])
        self.assertIn("若高于 `entry_max`，通常应说明是等待回踩还是属于突破追踪，不要机械追价", messages[0]["content"])
        self.assertIn("`50-60`：证据不足，偏观察", messages[0]["content"])
        self.assertIn("若 `action = \"BUY\"`，默认不应低于 `68`", messages[0]["content"])
        self.assertIn("`low`：结构较清晰，风险可控", messages[0]["content"])
        self.assertIn("若 `action = \"BUY\"`，通常不应给出 `high`", messages[0]["content"])
        self.assertIn("若 `risk_level = \"high\"`，`confidence` 不应轻易超过 `85`", messages[0]["content"])
        self.assertIn("1. 先看实时数据是否足够新鲜", messages[0]["content"])
        self.assertIn("当方向转强、时点合适、即时风险可控时，可以考虑 `BUY`", messages[0]["content"])
        self.assertIn("本系统当前只支持两类波段：`微波段（2-5个交易日）`、`标准波段（5-15个交易日）`", messages[0]["content"])
        self.assertIn("若未明确，必须承认“基线波段未明确”，不要自动补成标准波段", messages[0]["content"])
        self.assertIn("不要只依赖单一 `5分钟` 量能脉冲做结论", messages[0]["content"])
        self.assertIn("必须按这个顺序覆盖 5 类信息", messages[0]["content"])
        self.assertIn("不要输出带方括号的小标题", messages[0]["content"])
        self.assertIn("若未明确，必须承认“基线波段未明确”，不要自动补成标准波段", messages[0]["content"])
        self.assertLess(len(messages[0]["content"]), 10000)
        self.assertIn("[TIMER] 当前交易时段", messages[1]["content"])
        self.assertIn("[REALTIME_FRESHNESS] 实时数据新鲜度校验", messages[1]["content"])
        self.assertIn("整体状态: 可直接用于盘中执行", messages[1]["content"])
        self.assertIn("分时覆盖率: 100.0%", messages[1]["content"])
        self.assertIn("分时质量: 分时覆盖完整", messages[1]["content"])
        self.assertIn("股票名称: 贵州茅台", messages[1]["content"])
        self.assertIn("请基于以上数据，给出交易决策（JSON格式）。", messages[1]["content"])

    def test_build_prompt_messages_holding_mode_supports_swing_position_management(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        messages = client._build_prompt_messages(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "update_time": "2026-03-25 10:30:00",
                "current_price": 1650.0,
                "change_pct": 1.8,
                "change_amount": 29.2,
                "volume": 120000,
                "ma5": 1645.0,
                "ma10": 1632.0,
                "ma20": 1620.0,
                "ma60": 1580.0,
                "trend": "up",
                "atr14": 28.5,
                "atr14_pct": 1.73,
                "macd_dif": 1.2,
                "macd_dea": 1.0,
                "macd": 0.4,
                "rsi6": 62.0,
                "rsi12": 58.0,
                "rsi24": 55.0,
                "kdj_k": 70.0,
                "kdj_d": 65.0,
                "kdj_j": 80.0,
                "boll_upper": 1680.0,
                "boll_mid": 1638.0,
                "boll_lower": 1596.0,
                "boll_position": "中轨上方",
                "vol_ma5": 240000.0,
                "volume_ratio": 1.35,
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=True,
            position_cost=1500.0,
            position_quantity=100,
            position_date="2026-03-18",
            can_sell_today=True,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
        )

        self.assertIn("当前任务模式：持仓波段管理模式", messages[0]["content"])
        self.assertIn("允许动作：BUY / SELL / HOLD", messages[0]["content"])
        self.assertIn("禁止动作：做空、日内回转、忽视T+1的卖出", messages[0]["content"])
        self.assertIn("再看是否已出现风险触发：止损、止盈兑现条件成熟、关键破位、放量转弱、基线失效、明确利空", messages[0]["content"])
        self.assertIn("当前基线波段类型：未明确", messages[0]["content"])
        self.assertIn("若未明确，必须承认“基线波段未明确”，不要自动补成标准波段", messages[0]["content"])
        self.assertIn("持仓中的盘中决策不只包含退出，也包含波段持有中的加仓、减仓和止损管理", messages[0]["content"])
        self.assertIn("若基线明确为 `标准波段（5-15个交易日）`，优先把动作理解为 4 类：`回踩确认加仓`、`突破确认加仓`、`主动减仓锁盈`、`防守减仓/清仓`", messages[0]["content"])
        self.assertIn("你必须先基于持仓天数、浮盈亏、量价、均线、ATR、分时结构", messages[0]["content"])
        self.assertIn("`宽幅震荡洗盘`", messages[0]["content"])
        self.assertIn("`趋势突破确认`", messages[0]["content"])
        self.assertIn("`主升加速段`", messages[0]["content"])
        self.assertIn("`筑顶高位派发`", messages[0]["content"])
        self.assertIn("只有当信标明确点亮", messages[0]["content"])
        self.assertIn("`atr_stop_floor` 是系统允许的最宽防守底线", messages[0]["content"])
        self.assertNotIn("第9-15个交易日", messages[0]["content"])
        self.assertNotIn("超过 `15个交易日`", messages[0]["content"])
        self.assertIn("若选择 `BUY` 且当前已有持仓，应将其理解为 `加仓`", messages[0]["content"])
        self.assertIn("止损判断应优先看 `15/30/60分钟` 是否持续走坏、关键位是否失守、承接是否恶化", messages[0]["content"])
        self.assertIn("若价格短暂跌破成本或分时抖动，但 `30/60分钟` 结构未坏、承接仍在、量能未明显失控，可优先 `HOLD`", messages[0]["content"])
        self.assertIn("若触发止损，也要区分“防守减仓”与“直接清仓”", messages[0]["content"])
        self.assertIn("战略基线中的 `take_profit` 主要是初始止盈参考，不等于“价格一到就立即卖出”", messages[0]["content"])
        self.assertIn("若价格接近或触及基线止盈位，但 `60分钟` 结构未走坏、承接未恶化时，不必主动锁盈", messages[0]["content"])
        self.assertIn("`60分钟` 应作为买卖点判断的主锚", messages[0]["content"])
        self.assertIn("再完成 `structure_state` 判定，并说明结构是否支持当前战略基线", messages[0]["content"])
        self.assertIn("一旦进入该状态，退出锚点应转向“是否跌破跟踪均线、结构是否实质破坏、是否出现明确利空”", messages[0]["content"])
        self.assertIn("若趋势重新走强、`60分钟` 结构先行转强", messages[0]["content"])
        self.assertIn("`回踩确认加仓` 更适合用于价格回踩成本区、均线、前支撑或突破后的回踩确认", messages[0]["content"])
        self.assertIn("`突破确认加仓` 更适合用于关键压力位被有效突破后，价格站稳、量能跟随", messages[0]["content"])
        self.assertIn("`主动减仓锁盈` 仅限 `risk_level = \"high\"` 时执行；更适合用于趋势仍在但短线偏离过大、接近动态止盈区、或量价开始出现边际背离的场景", messages[0]["content"])
        self.assertIn("`防守减仓` 更适合用于 `15/30/60分钟` 明显转弱但尚未完全失控", messages[0]["content"])
        self.assertIn("若未进入止盈/止损等执行区且退出证据不足，可继续 `HOLD` 并跟踪阈值变化", messages[0]["content"])
        self.assertIn("持仓日期: 2026-03-18", messages[1]["content"])
        self.assertIn("持仓天数: 第6个交易日（估算）", messages[1]["content"])
        self.assertIn("今日可卖: 是", messages[1]["content"])
        self.assertIn("ATR14: ¥28.50", messages[1]["content"])
        self.assertIn("ATR波动率: +1.73%", messages[1]["content"])
        self.assertIn("系统ATR防守底线:", messages[1]["content"])
        self.assertIn("量化信标: 无", messages[1]["content"])
        self.assertIn("你必须在本轮 JSON 中自行判定 `structure_state`", messages[1]["content"])
        self.assertNotIn("波段阶段:", messages[1]["content"])
        self.assertIn("若基线已明确波段类型，优先按 `未明确`（未明确）视角管理；若未明确，只按持仓成本、结构强弱和风险约束执行，不自动套固定波段模板", messages[1]["content"])
        self.assertIn("优先区分 `回踩确认加仓`、`突破确认加仓`、`主动减仓锁盈`、`防守减仓/清仓` 这几类动作", messages[1]["content"])
        self.assertIn("如果 `15/30/60分钟` 持续走坏、关键位失守、承接恶化，且亏损超过止损线", messages[1]["content"])
        self.assertIn("如果只是短线波动或瞬时下探，但 `30/60分钟` 结构未坏 → 不要轻易把正常回撤当作止损信号", messages[1]["content"])
        self.assertIn("如果风险在扩大但趋势未完全坍塌 → 可先考虑 `防守减仓`，不必一上来就 `清仓`", messages[1]["content"])
        self.assertIn("如果价格接近基线止盈位但 `60分钟` 结构未走坏、承接仍在 → 优先继续持有或上修止盈观察位", messages[1]["content"])
        self.assertIn("先看 `60分钟` 趋势和量能，再用短时量能", messages[1]["content"])
        self.assertIn("如果盈利较多但趋势仍强，只想兑现部分利润 → 优先考虑 `主动减仓锁盈`，而不是直接清仓", messages[1]["content"])
        self.assertIn("若当前是 `标准波段`，且客观条件满足“持仓>=10日 + 利润垫充足 + 未跌破跟踪均线 + 非高位派发”", messages[1]["content"])
        self.assertIn("如果趋势重新走强、`60分钟` 结构先修复，回踩确认有效", messages[1]["content"])
        self.assertIn("如果关键压力位被有效突破并站稳，且 `60分钟` 与短时量能继续共振", messages[1]["content"])
        self.assertIn("不得出现 `take_profit <= stop_loss`", messages[0]["content"])
        self.assertIn("若 `action = \"SELL\"`，默认不应低于 `70`", messages[0]["content"])
        self.assertIn("若 `action = \"SELL\"` 由止损、破位、放量转弱或基线失效主导，通常应为 `medium` 或 `high`", messages[0]["content"])
        self.assertIn("当前有持仓时，若 `action = \"BUY\"`，`action_detail` 应优先写成 `加仓`", messages[0]["content"])
        self.assertIn("波段持仓中的 `加仓`，`action_ratio_pct` 通常应更克制，优先小到中等比例递进", messages[0]["content"])
        self.assertIn("`主动减仓锁盈` 默认更适合 `15-35%` 的小到中等比例", messages[0]["content"])
        self.assertIn("`防守减仓` 默认更适合 `25-50%` 的风险收缩比例", messages[0]["content"])
        self.assertIn("若 `action = \"BUY\"` 且 `action_detail = \"加仓\"`，必须说明这是更接近 `回踩确认加仓` 还是 `突破确认加仓`", messages[0]["content"])
        self.assertIn("`swing_execution_mode` 只能是 `pullback_add` 或 `breakout_add`", messages[0]["content"])
        self.assertIn("\"swing_execution_mode\": \"watch_hold\"", messages[0]["content"])
        self.assertIn("若盈利且趋势仍强，只是做止盈管理，`action = \"SELL\"` 时优先考虑 `减仓`，不要轻易给出 `清仓`", messages[0]["content"])
        self.assertIn("若 `action = \"SELL\"` 且 `action_detail = \"减仓\"`，必须说明“为什么不是继续 `HOLD`”；若属于“主动减仓锁盈”，必须同时确认 `risk_level` 为 `high` 并说明风险来源", messages[0]["content"])
        self.assertIn("若 `action = \"SELL\"` 且 `action_detail = \"清仓\"`，必须同时说明“为什么不是 `HOLD`”以及“为什么不是更温和的 `减仓`”", messages[0]["content"])
        self.assertIn("如果选择 `加仓`，要注意当日新增仓位同样受 `T+1` 限制，不能当日卖出", messages[1]["content"])

    def test_parse_decision_keeps_structure_state_upgrade_and_atr_fields(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        ai_response = """
{
  "action": "SELL",
  "action_detail": "减仓",
  "swing_execution_mode": "proactive_trim",
  "action_ratio_pct": 25,
  "confidence": 79,
  "reasoning": "结构进入主升加速段但已接近动态止盈区，先做小比例锁盈，同时保留趋势仓位。",
  "risk_level": "medium",
  "structure_state": "主升加速段",
  "structure_state_reason": "15/30/60分钟共振向上，量能仍保持扩张。",
  "trend_following_active": true,
  "trend_anchor_type": "MA10",
  "trend_anchor_value": 12.45,
  "atr14": 0.62,
  "atr14_pct": 4.18,
  "atr_stop_floor": 11.70,
  "swing_type_upgrade": true,
  "upgraded_swing_type": "标准波段",
  "upgrade_reason": "创阶段新高且放量突破，微波段已经演化为趋势波段。",
  "feature_beacons": ["stage_new_high", "breakout_20d_high_with_2x_volume"],
  "monitor_levels": {
    "entry_min": 12.10,
    "entry_max": 12.40,
    "take_profit": 13.20,
    "stop_loss": 11.70
  }
}
"""

        decision = client._parse_decision(ai_response, has_position=True)

        self.assertEqual(decision["structure_state"], "主升加速段")
        self.assertTrue(decision["trend_following_active"])
        self.assertEqual(decision["trend_anchor_type"], "MA10")
        self.assertEqual(decision["upgraded_swing_type"], "标准波段")
        self.assertTrue(decision["swing_type_upgrade"])
        self.assertEqual(
            decision["feature_beacons"],
            ["stage_new_high", "breakout_20d_high_with_2x_volume"],
        )

    def test_build_prompt_messages_respects_explicit_micro_swing_baseline(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        messages = client._build_prompt_messages(
            stock_code="300308",
            market_data={
                "name": "中际旭创",
                "update_time": "2026-04-10 10:30:00",
                "current_price": 141.8,
                "change_pct": 3.2,
                "volume": 120000,
                "ma5": 139.2,
                "ma20": 132.0,
                "ma60": 125.0,
                "trend": "up",
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 0.0,
                "position_usage_pct": 0.0,
                "positions_count": 0,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
            strategy_context={
                "analysis_date": "2026-04-10 09:20:00",
                "analysis_source": "research",
                "rating": "买入",
                "summary": "事件驱动明确，放量突破后只做流动性最好的一段，失速就撤，并采用移动止盈策略。",
                "entry_min": 139.5,
                "entry_max": 141.0,
                "take_profit": 148.0,
                "stop_loss": 137.2,
                "holding_period": "2-5个交易日",
                "swing_type": "微波段",
                "swing_type_reason": "政策催化和突破共振，适合快进快出。",
            },
        )

        self.assertIn("当前基线波段类型：微波段", messages[0]["content"])
        self.assertIn("当前基线周期参考：2-5个交易日", messages[0]["content"])
        self.assertIn("当前基线退出方式：移动止盈为主", messages[0]["content"])
        self.assertIn("若基线属于 `微波段`，可以更重视突破延续性、事件驱动时效性和流动性", messages[0]["content"])
        self.assertIn("当前适用波段: 微波段", messages[1]["content"])
        self.assertIn("持有周期: 2-5个交易日", messages[1]["content"])
        self.assertIn("盘中协同重点: 优先验证突破或事件驱动是否继续发酵", messages[1]["content"])
        self.assertIn("波段判断依据: 政策催化和突破共振，适合快进快出。", messages[1]["content"])

    def test_normalize_strategy_context_keeps_swing_type_unset_without_explicit_baseline(self):
        profile = normalize_strategy_context({
            "holding_period": "1-3个月",
            "summary": "宏观政策转向与产业修复共振，适合中线趋势跟随，并采用移动止盈逐步上调离场底线。",
        })

        self.assertEqual(profile["swing_type"], "")
        self.assertEqual(profile["swing_type_code"], "")
        self.assertEqual(profile["swing_horizon_days_text"], "1-3个月")
        self.assertEqual(profile["baseline_exit_style"], "移动止盈为主，随着价格抬升不断上调离场底线")
        self.assertNotIn("标准波段", profile["intraday_execution_preference"])
        self.assertIn("移动止盈", profile["strategy_style_summary"])

    def test_build_prompt_excludes_previous_decision_context(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        messages = client._build_prompt_messages(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "current_price": 1650.0,
                "volume": 120000,
                "realtime_freshness": {
                    "asof_time": "2026-04-10 10:30:00",
                    "is_trading_now": True,
                    "intraday_decision_ready": True,
                    "overall_status": "ready",
                    "summary": "TDX 分时/逐笔时间戳足够新鲜。",
                    "quote": {"timestamp": "2026-04-10 10:30:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "2026-04-10 10:29:00", "status": "fresh"},
                    "trade": {"timestamp": "2026-04-10 10:29:58", "status": "fresh"},
                    "minute_quality": {"coverage_ratio": 1.0, "max_gap": 0, "label": "分时覆盖完整"},
                },
                "intraday_context": {
                    "intraday_bias_text": "价格回到分时均价上方",
                    "last_5m_change_pct": 0.45,
                    "price_position_pct": 72.0,
                    "volume_acceleration_ratio": 1.32,
                    "intraday_signal_labels": ["价格运行在分时均价上方", "量能回升"],
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
        )

        self.assertIn("本轮盘中决策只依据当前盘面、持仓状态、风险约束和战略基线判断，不参考上一轮盘中决策", messages[0]["content"])
        self.assertIn("[INTRADAY_FLOW] TDX分时行为与执行映射（实时参考）", messages[1]["content"])
        self.assertIn("一致性判断:", messages[1]["content"])
        self.assertIn("执行支持:", messages[1]["content"])
        self.assertNotIn("不执行约束:", messages[1]["content"])
        self.assertIn("变化触发器:", messages[1]["content"])
        self.assertNotIn("[PREVIOUS_DECISION]", messages[1]["content"])
        self.assertNotIn("上轮锚点:", messages[1]["content"])

    def test_route_intraday_evidence_prefers_entry_signals_when_flat(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        selected = client._route_intraday_evidence(
            labels=["横盘整理", "高位承接正常", "放量回升", "均价上方震荡", "冲高回落风险"],
            observations=["近15分钟量价配合改善", "分时回踩后修复"],
            has_position=False,
            freshness_status="ready",
            previous_labels=["横盘整理"],
            current_bias_text="价格回到分时均价上方，短线偏强",
        )

        self.assertEqual(selected["primary_evidence"], "放量回升")
        self.assertEqual(selected["delta_evidence"], "均价上方震荡")
        self.assertNotIn("横盘整理", " ".join(selected.values()))

    def test_route_intraday_evidence_prefers_exit_risk_when_holding(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        selected = client._route_intraday_evidence(
            labels=["高位承接正常", "放量转弱", "跌破均价", "横盘整理"],
            observations=["抛压增大", "承接一般"],
            has_position=True,
            freshness_status="ready",
            previous_labels=["高位承接正常"],
            current_bias_text="价格跌回分时均价下方，短线转弱",
        )

        self.assertEqual(selected["primary_evidence"], "放量转弱")
        self.assertEqual(selected["delta_evidence"], "跌破均价")
        self.assertNotIn("横盘整理", " ".join(selected.values()))

    def test_take_profit_hint_keeps_hold_when_60m_structure_stays_intact(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        intraday_context = {
            "intraday_bias_text": "价格运行在分时均价上方，承接优化",
            "price_position_pct": 82.0,
            "last_15m_change_pct": 0.58,
            "last_30m_change_pct": 0.42,
            "last_60m_change_pct": 0.86,
            "volume_acceleration_ratio": 1.18,
            "volume_ratio_15m": 1.16,
            "volume_ratio_30m": 1.08,
            "volume_ratio_60m": 1.02,
            "intraday_signal_labels": ["价格运行在分时均价上方", "高位承接正常"],
        }

        hint = client._derive_take_profit_adjustment_hint(intraday_context, has_position=True)

        self.assertEqual(hint, "若接近基线止盈位但60分钟结构未走坏，可继续持有或上修止盈位，不必主动锁盈")

    def test_take_profit_hint_waits_for_60m_weakening_before_sell_signal(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        intraday_context = {
            "intraday_bias_text": "价格跌回分时均价下方，承接转弱",
            "last_15m_change_pct": -0.36,
            "last_30m_change_pct": -0.42,
            "last_60m_change_pct": -0.72,
            "volume_acceleration_ratio": 0.82,
            "volume_ratio_15m": 0.94,
            "volume_ratio_30m": 0.95,
            "volume_ratio_60m": 0.96,
            "intraday_signal_labels": ["价格跌破分时均价", "承接转弱"],
        }

        hint = client._derive_take_profit_adjustment_hint(intraday_context, has_position=True)

        self.assertEqual(hint, "若接近止盈位且60分钟转弱、风险较高时，再结合15/30分钟与短时量能确认卖点")

    def test_take_profit_trim_resolves_as_proactive_trim_without_volume_trigger(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        swing_mode = client._resolve_swing_execution_mode(
            None,
            action="SELL",
            action_detail="减仓",
            has_position=True,
            reasoning="进入止盈区后先减仓锁定利润，剩余仓位继续跟踪。",
        )

        self.assertEqual(swing_mode, "proactive_trim")

    def test_rank_intraday_evidence_candidates_returns_structured_metadata(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        candidates = client._rank_intraday_evidence_candidates(
            ["放量回升", "横盘整理"],
            kind="label",
            has_position=False,
            freshness_status="ready",
            previous_items=["横盘整理"],
            current_bias_text="价格回到分时均价上方，短线偏强",
        )

        self.assertGreaterEqual(len(candidates), 2)
        top = candidates[0]
        self.assertEqual(top["layer"], "intraday")
        self.assertEqual(top["source_kind"], "label")
        self.assertIn(top["role"], {"support", "constraint", "context"})
        self.assertIn(top["polarity"], {"positive", "negative", "neutral"})
        self.assertIn(top["horizon"], {"intraday"})
        self.assertIn("novelty", top)
        self.assertIn("reliability", top)
        self.assertIn("action_relevance", top)

    def test_cross_layer_summary_uses_strategy_bounds_without_intraday_evidence(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        summary = client._build_cross_layer_evidence_summary(
            has_position=False,
            strategy_context={
                "rating": "买入",
                "summary": "当前不追高，优先等回踩确认，回到计划区间再考虑执行。",
                "entry_min": 141.0,
                "entry_max": 145.0,
                "take_profit": 156.0,
                "stop_loss": 136.0,
            },
            evidence_summary=None,
        )

        self.assertEqual(summary["execution_support"], "计划进场区间：141.0 - 145.0")
        self.assertEqual(summary["execution_constraint"], "N/A")
        self.assertEqual(summary["change_trigger"], "N/A")

    def test_normalize_reasoning_output_preserves_original_reasoning(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        reasoning_context = client._build_reasoning_context(
            has_position=False,
            market_data={
                "intraday_context": {
                    "intraday_bias_text": "价格回到分时均价上方，短线偏强",
                    "last_5m_change_pct": 0.45,
                    "volume_acceleration_ratio": 1.32,
                    "price_position_pct": 72.0,
                    "intraday_signal_labels": ["放量回升", "均价上方震荡"],
                    "intraday_observations": ["近15分钟量价配合改善"],
                },
                "realtime_freshness": {
                    "overall_status": "ready",
                },
            },
            strategy_context={
                "rating": "买入",
                "summary": "当前不追高，优先等回踩确认。",
                "entry_min": 141.0,
                "entry_max": 145.0,
                "take_profit": 156.0,
                "stop_loss": 136.0,
            },
        )

        normalized = client._normalize_reasoning_output(
            {
                "action": "BUY",
                "risk_level": "medium",
                "reasoning": "原始输出写得比较散，但核心观点明确：价格回到分时均价上方，短线偏强，量能也在改善，继续按原计划等待回踩确认，不追高。",
                "monitor_levels": {
                    "entry_min": 141.0,
                    "entry_max": 145.0,
                    "take_profit": 156.0,
                    "stop_loss": 136.0,
                },
            },
            reasoning_context=reasoning_context,
            strategy_context={
                "rating": "买入",
                "summary": "当前不追高，优先等回踩确认。",
                "entry_min": 141.0,
                "entry_max": 145.0,
                "take_profit": 156.0,
                "stop_loss": 136.0,
            },
            has_position=False,
        )

        self.assertIn("原始输出写得比较散", normalized)
        self.assertIn("价格回到分时均价上方", normalized)
        self.assertIn("继续按原计划等待回踩确认", normalized)
        self.assertNotIn("盘中主导证据与战略基线基本一致", normalized)

    def test_normalize_reasoning_output_preserves_original_reasoning_without_strategy(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        normalized = client._normalize_reasoning_output(
            {
                "action": "HOLD",
                "risk_level": "high",
                "reasoning": "当前没有明确战略基线，且实时数据偏旧，只能把盘中波动当辅助参考，暂时继续观望。",
                "monitor_levels": {
                    "entry_min": 12.1,
                    "entry_max": 12.4,
                    "take_profit": 13.2,
                    "stop_loss": 11.7,
                },
            },
            reasoning_context={
                "intraday_context": {},
                "freshness_status": "degraded",
                "has_strategy": False,
                "cross_layer_summary": {},
            },
            strategy_context=None,
            has_position=False,
        )

        self.assertEqual(normalized, "当前没有明确战略基线，且实时数据偏旧，只能把盘中波动当辅助参考，暂时继续观望。")

    def test_normalize_reasoning_output_does_not_replace_with_change_summary(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        normalized = client._normalize_reasoning_output(
            {
                "action": "HOLD",
                "action_detail": "持有",
                "reasoning": "盘中结构未见明确破位，虽然短线波动加大，但不足以推翻继续持有判断。",
                "monitor_levels": {
                    "entry_min": 12.1,
                    "entry_max": 12.4,
                    "take_profit": 13.2,
                    "stop_loss": 11.7,
                },
            },
            reasoning_context={
                "intraday_context": {},
                "freshness_status": "ready",
                "has_strategy": True,
                "cross_layer_summary": {
                    "alignment_summary": "盘中主导证据与战略基线基本一致。",
                    "execution_support": "N/A",
                    "execution_constraint": "价格运行在分时均价下方",
                    "change_trigger": "近5分钟涨跌由+0.37%变为-0.81%",
                },
            },
            strategy_context={
                "entry_min": 12.1,
                "entry_max": 12.4,
                "take_profit": 13.2,
                "stop_loss": 11.7,
            },
            has_position=True,
        )

        self.assertIn("盘中结构未见明确破位", normalized)
        self.assertIn("不足以推翻继续持有判断", normalized)
        self.assertNotIn("近5分钟涨跌由+0.37%变为-0.81%", normalized)

    def test_build_prompt_includes_realtime_freshness_context(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        prompt = client._build_a_stock_prompt(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "current_price": 1650.0,
                "change_pct": 1.8,
                "change_amount": 29.2,
                "volume": 120000,
                "ma5": 1645.0,
                "ma20": 1620.0,
                "ma60": 1580.0,
                "trend": "up",
                "macd_dif": 1.2,
                "macd_dea": 1.0,
                "macd": 0.4,
                "rsi6": 62.0,
                "rsi12": 58.0,
                "rsi24": 55.0,
                "kdj_k": 70.0,
                "kdj_d": 65.0,
                "kdj_j": 80.0,
                "boll_upper": 1680.0,
                "boll_mid": 1638.0,
                "boll_lower": 1596.0,
                "boll_position": "中轨上方",
                "vol_ma5": 240000.0,
                "volume_ratio": 1.35,
                "realtime_freshness": {
                    "asof_time": "2026-04-10 10:30:00",
                    "is_trading_now": True,
                    "intraday_decision_ready": False,
                    "overall_status": "degraded",
                    "summary": "存在同日行情或盘中快照，但新鲜度不足；可参考方向，不宜过度依赖盘中节奏。",
                    "quote": {"timestamp": "2026-04-10 10:30:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "2026-04-10 10:08:00", "status": "stale"},
                    "trade": {"timestamp": "2026-04-10 10:09:12", "status": "stale"},
                    "minute_quality": {"coverage_ratio": 0.83, "max_gap": 6, "label": "分时缺口较多"},
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
        )

        self.assertIn("[REALTIME_FRESHNESS]", prompt)
        self.assertIn("整体状态: 可参考但需适度降权", prompt)
        self.assertIn("TDX 分时最后时间: 2026-04-10 10:08:00 (延迟过久)", prompt)
        self.assertIn("TDX 逐笔最后时间: 2026-04-10 10:09:12 (延迟过久)", prompt)
        self.assertIn("分时质量: 分时缺口较多", prompt)
        self.assertNotIn("[MARKET_CONTEXT]", prompt)
        self.assertNotIn("[SECTOR_CONTEXT]", prompt)

    def test_build_prompt_labels_review_ready_freshness(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        prompt = client._build_a_stock_prompt(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "current_price": 1650.0,
                "change_pct": 1.8,
                "change_amount": 29.2,
                "volume": 120000,
                "realtime_freshness": {
                    "asof_time": "2026-04-10 15:30:00",
                    "is_trading_now": False,
                    "intraday_decision_ready": False,
                    "intraday_review_ready": True,
                    "overall_status": "review_ready",
                    "summary": "存在同日分时/逐笔快照，盘后可用于复盘判断；不应视为盘中实时执行信号。",
                    "quote": {"timestamp": "2026-04-10 15:01:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "2026-04-10 15:00:00", "status": "same_day_snapshot"},
                    "trade": {"timestamp": "2026-04-10 15:00:00", "status": "same_day_snapshot"},
                    "minute_quality": {"coverage_ratio": 0.98, "max_gap": 1, "label": "分时覆盖完整"},
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=True,
            session_info={
                "session": "已收盘",
                "volatility": "medium",
                "recommendation": "适合复盘",
                "beijing_hour": 15,
                "beijing_time": "15:30",
                "can_trade": False,
            },
        )

        self.assertIn("整体状态: 可用于盘后复盘", prompt)
        self.assertNotIn("整体状态: 未知", prompt)

    def test_build_prompt_includes_tdx_intraday_flow_context(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        prompt = client._build_a_stock_prompt(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "data_source": "tdx",
                "update_time": "2026-04-10 10:30:00",
                "current_price": 1450.63,
                "change_pct": -0.67,
                "change_amount": -9.86,
                "high": 1459.14,
                "low": 1441.10,
                "open": 1459.14,
                "pre_close": 1460.49,
                "volume": 22748,
                "amount": 3296746.50,
                "ma5": 1448.0,
                "ma20": 1435.0,
                "ma60": 1402.0,
                "trend": "up",
                "macd_dif": 1.2,
                "macd_dea": 1.0,
                "macd": 0.4,
                "rsi6": 55.0,
                "rsi12": 53.0,
                "rsi24": 50.0,
                "kdj_k": 63.0,
                "kdj_d": 58.0,
                "kdj_j": 73.0,
                "boll_upper": 1472.0,
                "boll_mid": 1444.0,
                "boll_lower": 1416.0,
                "boll_position": "中轨上方",
                "vol_ma5": 240000.0,
                "volume_ratio": 1.10,
                "intraday_context": {
                    "minute_point_count": 121,
                    "filled_minute_point_count": 121,
                    "minute_coverage_ratio": 1.0,
                    "max_minute_gap": 0,
                    "latest_minute_time": "10:27",
                    "intraday_high": 1459.14,
                    "intraday_low": 1441.10,
                    "intraday_range_pct": 1.25,
                    "intraday_vwap": 1449.80,
                    "price_position_pct": 52.94,
                    "last_5m_change_pct": 0.88,
                    "last_15m_change_pct": 1.26,
                    "last_30m_change_pct": 0.42,
                    "last_60m_change_pct": 0.93,
                    "recent_5m_volume": 4200,
                    "previous_5m_volume": 3100,
                    "volume_acceleration_ratio": 1.35,
                    "volume_ratio_15m": 1.18,
                    "volume_ratio_30m": 1.07,
                    "volume_ratio_60m": 1.02,
                    "trade_tick_count": 1800,
                    "latest_trade_time": "2026-04-10T10:28:00+08:00",
                    "avg_trade_volume": 4.8,
                    "largest_trade_volume": 27,
                    "intraday_bias": "trend_continuation",
                    "intraday_bias_text": "高位放量延续，短线趋势偏强",
                    "intraday_signal_labels": ["高位放量延续", "价格运行在分时均价上方"],
                    "intraday_observations": ["近5分钟放量拉升", "当前价格处于日内中位偏上"],
                },
                "realtime_freshness": {
                    "asof_time": "2026-04-10 10:30:00",
                    "is_trading_now": True,
                    "intraday_decision_ready": True,
                    "overall_status": "ready",
                    "summary": "TDX 分时/逐笔时间戳足够新鲜，且分时覆盖质量可接受，可用于盘中执行判断。",
                    "quote": {"timestamp": "2026-04-10 10:30:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "2026-04-10 10:27:00", "status": "fresh"},
                    "trade": {"timestamp": "2026-04-10 10:28:00", "status": "fresh"},
                    "minute_quality": {"coverage_ratio": 1.0, "max_gap": 0, "label": "分时覆盖完整"},
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
        )

        self.assertIn("[INTRADAY_FLOW]", prompt)
        self.assertIn("覆盖率/缺口: 100.0% | 0 分钟", prompt)
        self.assertIn("分时均价: ¥1,449.80", prompt)
        self.assertIn("当前价所处日内位置: 52.94% (处于日内中位)", prompt)
        self.assertIn("近15/30/60分钟涨跌: +1.26% / +0.42% / +0.93%", prompt)
        self.assertIn("近5分钟异动: +0.88% | 量能加速度 1.35 (放量)", prompt)
        self.assertIn("15/30/60分钟量能比: 1.18 / 1.07 / 1.02", prompt)
        self.assertIn("量能结构: 15/30/60分钟量能整体扩张", prompt)
        self.assertIn("15/30/60分钟节奏: 15/30/60分钟整体走强，未见持续走坏", prompt)
        self.assertIn("承接状态: 承接优化，回踩后仍有资金接力", prompt)
        self.assertIn("止盈动态提示: 空仓场景，以入场节奏判断为主", prompt)
        self.assertIn("盘中偏向: 高位放量延续，短线趋势偏强", prompt)
        self.assertIn("执行支持: 价格运行在分时均价上方", prompt)
        self.assertIn("不执行约束: 高位放量延续", prompt)
        self.assertIn("变化触发器: 近5分钟放量拉升", prompt)

    def test_build_prompt_messages_caps_optional_context_to_avoid_warning_threshold(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        messages = client._build_prompt_messages(
            stock_code="300502",
            market_data={
                "name": "新易盛",
                "data_source": "tdx",
                "update_time": "2026-04-10 10:30:00",
                "current_price": 145.63,
                "change_pct": 3.67,
                "change_amount": 5.16,
                "high": 146.14,
                "low": 141.10,
                "open": 142.14,
                "pre_close": 140.47,
                "volume": 227480,
                "amount": 329674650.0,
                "ma5": 144.0,
                "ma20": 138.0,
                "ma60": 126.0,
                "trend": "up",
                "macd_dif": 1.2,
                "macd_dea": 1.0,
                "macd": 0.4,
                "rsi6": 72.0,
                "rsi12": 68.0,
                "rsi24": 61.0,
                "kdj_k": 83.0,
                "kdj_d": 77.0,
                "kdj_j": 95.0,
                "boll_upper": 149.0,
                "boll_mid": 141.0,
                "boll_lower": 133.0,
                "boll_position": "上轨附近",
                "vol_ma5": 180000.0,
                "volume_ratio": 1.55,
                "intraday_context": {
                    "minute_point_count": 120,
                    "filled_minute_point_count": 118,
                    "minute_coverage_ratio": 0.983,
                    "max_minute_gap": 1,
                    "intraday_high": 146.14,
                    "intraday_low": 141.10,
                    "intraday_range_pct": 3.57,
                    "intraday_vwap": 144.18,
                    "price_position_pct": 78.5,
                    "last_5m_change_pct": 0.45,
                    "last_15m_change_pct": 1.2,
                    "last_30m_change_pct": 2.1,
                    "last_60m_change_pct": 3.4,
                    "recent_5m_volume": 15000,
                    "previous_5m_volume": 10000,
                    "volume_acceleration_ratio": 1.5,
                    "volume_ratio_15m": 1.22,
                    "volume_ratio_30m": 1.08,
                    "volume_ratio_60m": 1.03,
                    "trade_tick_count": 240,
                    "latest_trade_time": "2026-04-10 10:29:58",
                    "avg_trade_volume": 500,
                    "largest_trade_volume": 3200,
                    "intraday_bias_text": "价格回到分时均价上方，量价共振，主动买盘占优，短线延续性较强，仍需防止高位回落。",
                    "intraday_signal_labels": ["放量回升", "VWAP上方震荡", "高位承接正常", "算力链共振"],
                    "intraday_observations": ["近15分钟量价配合改善", "分时回踩后修复", "未见明显抢跑抛压", "主动性买单增强"],
                },
                "realtime_freshness": {
                    "asof_time": "2026-04-10 10:30:00",
                    "is_trading_now": True,
                    "intraday_decision_ready": True,
                    "overall_status": "ready",
                    "summary": "TDX 分时/逐笔时间戳足够新鲜，且分时覆盖质量可接受，可用于盘中执行判断。" * 6,
                    "quote": {"timestamp": "2026-04-10 10:30:00", "status": "same_day_service_time"},
                    "minute": {"timestamp": "2026-04-10 10:29:00", "status": "fresh"},
                    "trade": {"timestamp": "2026-04-10 10:29:58", "status": "fresh"},
                    "minute_quality": {"coverage_ratio": 1.0, "max_gap": 0, "label": "分时覆盖完整"},
                },
                "semantic_labels": ["光模块", "AI算力", "高景气", "趋势股", "北美链", "订单兑现", "景气主线", "波动放大"],
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
            strategy_context={
                "analysis_date": "2026-04-10 09:30:00",
                "analysis_source": "research",
                "rating": "买入",
                "summary": (
                    "1. 当前不追高，优先等回踩确认。"
                    "2. 若回踩到支撑区并缩量企稳，可分批考虑。"
                    "3. 若放量跌破止损位，盘中不继续硬扛。"
                    "4. 若拉升过快偏离计划区间，宁可放弃也不追单。"
                ) * 20,
                "entry_min": 141.0,
                "entry_max": 145.0,
                "take_profit": 156.0,
                "stop_loss": 136.0,
            },
        )

        payload_chars = len(json.dumps({
            "model": "gemini-3-flash",
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": 1600,
        }, ensure_ascii=False))

        self.assertLess(payload_chars, 7200)
        self.assertIn("[INTRADAY_FLOW]", messages[1]["content"])
        self.assertIn("[STRATEGY_CONTEXT]", messages[1]["content"])
        self.assertNotIn("[PREVIOUS_DECISION]", messages[1]["content"])

    def test_build_prompt_omits_empty_lines_for_partial_context(self):
        client = SmartMonitorDeepSeek(api_key="test-key")

        prompt = client._build_a_stock_prompt(
            stock_code="600519",
            market_data={
                "name": "贵州茅台",
                "current_price": 1650.0,
                "intraday_context": {
                    "intraday_bias_text": "分时均价附近反复拉锯",
                },
                "realtime_freshness": {
                    "overall_status": "degraded",
                },
            },
            account_info={
                "available_cash": 100000.0,
                "total_value": 300000.0,
                "total_market_value": 200000.0,
                "position_usage_pct": 0.66,
                "positions_count": 3,
            },
            has_position=False,
            session_info={
                "session": "上午盘",
                "volatility": "high",
                "recommendation": "交易活跃，波动较大",
                "beijing_hour": 10,
                "beijing_time": "10:30",
                "can_trade": True,
            },
            strategy_context={
                "rating": "买入",
                "summary": "优先等回踩确认，不追高。",
            },
        )

        self.assertIn("[INTRADAY_FLOW]", prompt)
        self.assertIn("盘中偏向: 分时均价附近反复拉锯", prompt)
        self.assertNotIn("覆盖率/缺口:", prompt)
        self.assertNotIn("近15/30/60分钟涨跌:", prompt)
        self.assertNotIn("15/30/60分钟量能比:", prompt)
        self.assertNotIn("时间/来源:", prompt)
        self.assertNotIn("阈值:", prompt)


if __name__ == "__main__":
    unittest.main()
