import unittest
from unittest.mock import MagicMock, patch

import requests

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
        self.assertEqual(decision["risk_level"], "high")
        self.assertIn("当前无持仓", decision["reasoning"])

    def test_parse_decision_repairs_json_like_response(self):
        client = SmartMonitorDeepSeek(api_key="test-key")
        ai_response = """
```json
{
  action: BUY,
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
    stop_loss: 11.70,
  },
}
```
"""

        decision = client._parse_decision(ai_response)

        self.assertEqual(decision["action"], "BUY")
        self.assertEqual(decision["confidence"], 82)
        self.assertEqual(decision["risk_level"], "medium")
        self.assertEqual(decision["monitor_levels"]["entry_min"], 12.1)
        self.assertEqual(decision["monitor_levels"]["take_profit"], 13.2)

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
        self.assertEqual(decision["confidence"], 85)
        self.assertEqual(decision["risk_level"], "medium")
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
        self.assertIn("当前未注入可验证的实时大盘/板块数据", prompt)
        self.assertIn("盘中累计成交量不能直接与历史全天均量比较", prompt)
        self.assertIn("实时量比: 1.35 (放量)", prompt)
        self.assertIn("折算全天成交量/5日均量: 2.00 (放量)", prompt)

    def test_build_prompt_includes_market_and_sector_context(self):
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
                "market_context": {
                    "market_overview": {
                        "sh_index": {"change_pct": -0.72},
                        "sz_index": {"change_pct": -0.33},
                        "cyb_index": {"change_pct": -0.73},
                        "up_count": 1140,
                        "down_count": 4299,
                        "up_ratio": 20.76,
                        "limit_up": 64,
                        "limit_down": 17,
                    },
                    "limit_stats": {
                        "interpretation": "涨停股远多于跌停股，市场情绪火热",
                        "limit_ratio": "97.3%",
                    },
                    "north_flow": {"north_net_inflow": 123.45},
                },
                "sector_context": {
                    "industry": "白酒",
                    "matched_sector": "白酒",
                    "sector_rank": 12,
                    "sector_total_count": 496,
                    "sector_performance": {
                        "change_pct": 1.26,
                        "top_stock": "贵州茅台",
                        "up_count": 12,
                        "down_count": 3,
                    },
                    "sector_fund_flow": {
                        "main_net_inflow": 456789000.0,
                        "main_net_inflow_pct": 2.18,
                    },
                    "leading_sectors": [
                        {"sector": "通信线缆及配套", "change_pct": 4.37},
                        {"sector": "激光设备", "change_pct": 2.99},
                        {"sector": "印染", "change_pct": 2.45},
                    ],
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

        self.assertIn("[MARKET_CONTEXT]", prompt)
        self.assertIn("上证指数: -0.72%", prompt)
        self.assertIn("情绪解读: 涨停股远多于跌停股，市场情绪火热", prompt)
        self.assertIn("[SECTOR_CONTEXT]", prompt)
        self.assertIn("所属行业: 白酒", prompt)
        self.assertIn("板块排名: 12 / 496", prompt)

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
                    "intraday_high": 1459.14,
                    "intraday_low": 1441.10,
                    "intraday_range_pct": 1.25,
                    "intraday_vwap": 1449.80,
                    "price_position_pct": 52.94,
                    "last_5m_change_pct": 0.88,
                    "last_15m_change_pct": 1.26,
                    "last_30m_change_pct": 0.42,
                    "recent_5m_volume": 4200,
                    "previous_5m_volume": 3100,
                    "volume_acceleration_ratio": 1.35,
                    "trade_tick_count": 1800,
                    "latest_trade_time": "2026-04-10T10:28:00+08:00",
                    "avg_trade_volume": 4.8,
                    "largest_trade_volume": 27,
                    "intraday_bias": "trend_continuation",
                    "intraday_bias_text": "高位放量延续，短线趋势偏强",
                    "intraday_signal_labels": ["高位放量延续", "价格运行在分时均价上方"],
                    "intraday_observations": ["近5分钟放量拉升", "当前价格处于日内中位偏上"],
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
        self.assertIn("分时VWAP: ¥1,449.80", prompt)
        self.assertIn("近5分钟涨跌: +0.88%", prompt)
        self.assertIn("近5分钟量能加速度: 1.35 (放量)", prompt)
        self.assertIn("盘中偏向: 高位放量延续，短线趋势偏强", prompt)
        self.assertIn("盘中标签: 高位放量延续 / 价格运行在分时均价上方", prompt)
        self.assertIn("实时观察: 近5分钟放量拉升 / 当前价格处于日内中位偏上", prompt)


if __name__ == "__main__":
    unittest.main()
