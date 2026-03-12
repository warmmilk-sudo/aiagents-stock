import sys
import types
import unittest


class _DummyContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyStreamlit(types.ModuleType):
    def __init__(self):
        super().__init__("streamlit")
        self.session_state = {}

    def __getattr__(self, name):
        if name == "fragment":
            def _fragment(*args, **kwargs):
                def _decorator(func):
                    return func
                return _decorator
            return _fragment
        if name in {"container", "expander", "form", "spinner"}:
            return lambda *args, **kwargs: _DummyContext()
        if name == "columns":
            return lambda spec, *args, **kwargs: [
                _DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))
            ]
        if name == "tabs":
            return lambda labels, *args, **kwargs: [_DummyContext() for _ in labels]
        if name in {"button", "checkbox", "form_submit_button"}:
            return lambda *args, **kwargs: False
        if name == "toggle":
            return lambda *args, **kwargs: False
        if name in {"selectbox", "radio", "text_input", "text_area", "date_input"}:
            return lambda *args, **kwargs: ""
        if name in {"number_input", "slider"}:
            return lambda *args, **kwargs: 0
        return lambda *args, **kwargs: None


sys.modules.setdefault("streamlit", _DummyStreamlit())

import smart_monitor_ui


class SmartMonitorUIHistorySplitTests(unittest.TestCase):
    def test_select_latest_notification_events_keeps_latest_per_stock(self):
        notifications = [
            {
                "id": 5,
                "account_name": "默认账户",
                "symbol": "600519",
                "event_type": "sell",
                "message": "最新卖出信号",
                "is_read": False,
            },
            {
                "id": 4,
                "account_name": "默认账户",
                "symbol": "600519",
                "event_type": "buy",
                "message": "旧买入信号",
                "is_read": True,
            },
            {
                "id": 3,
                "account_name": "默认账户",
                "symbol": "000001",
                "event_type": "entry",
                "message": "平安银行进入区间",
                "is_read": True,
            },
        ]

        latest = smart_monitor_ui._select_latest_notification_events(notifications)

        self.assertEqual([event["id"] for event in latest], [5, 3])
        self.assertEqual(latest[0]["message"], "最新卖出信号")
        self.assertFalse(latest[0]["is_read"])

    def test_ai_analysis_events_are_grouped_with_ai_decisions(self):
        events = [
            {"event_type": "ai_analysis", "message": "AI决策: HOLD"},
            {"event_type": "threshold_sync", "message": "已更新绑定价格预警的运行时阈值"},
            {"event_type": "hold", "message": "HOLD信号 - 剑桥科技(603083)"},
        ]

        ai_events, monitor_events = smart_monitor_ui._split_history_events(events)

        self.assertEqual([event["event_type"] for event in ai_events], ["ai_analysis"])
        self.assertEqual(
            [event["event_type"] for event in monitor_events],
            ["threshold_sync", "hold"],
        )

    def test_ai_analysis_event_match_is_case_insensitive(self):
        self.assertTrue(
            smart_monitor_ui._is_ai_decision_history_event({"event_type": " AI_ANALYSIS "})
        )
        self.assertFalse(
            smart_monitor_ui._is_ai_decision_history_event({"event_type": "threshold_sync"})
        )

    def test_reasoning_brief_keeps_only_short_summary(self):
        reasoning = (
            "当前为盘后时段，无法进行交易，需等待次日开盘操作。"
            "当前未触发止盈或止损条件，且无明确利空预警信号，建议继续持有。"
        )
        self.assertEqual(
            smart_monitor_ui._format_decision_reasoning_brief(reasoning, max_length=24),
            "未触发止盈或止损条件，建议继续持有",
        )
        self.assertEqual(
            smart_monitor_ui._format_decision_reasoning_brief(
                "根据T+1交易规则，明日可正常卖出该股票，建议执行清仓止损，避免亏损进一步扩大。",
                max_length=20,
            ),
            "明日可正常卖出该股票，建议执行清仓止损",
        )

    def test_monitor_event_message_uses_reason_without_repeating_symbol(self):
        event = {
            "event_type": "entry",
            "symbol": "002230",
            "name": "科大讯飞",
            "message": "股票 002230 (科大讯飞) 价格 53.08 进入进场区间 [51.2-55.0]",
        }
        self.assertEqual(
            smart_monitor_ui._format_monitor_event_message(event),
            "原因：价格 53.08 进入进场区间 [51.2-55.0]",
        )

    def test_monitor_event_message_supports_notification_type_fallback(self):
        event = {
            "type": "entry",
            "symbol": "002230",
            "name": "科大讯飞",
            "message": "股票 002230 (科大讯飞) 价格 53.08 进入进场区间 [51.2-55.0]",
        }
        self.assertEqual(
            smart_monitor_ui._format_monitor_event_message(event),
            "原因：价格 53.08 进入进场区间 [51.2-55.0]",
        )

    def test_build_watchlist_threshold_lines_prefers_analysis_baseline(self):
        task = {
            "strategy_context": {
                "entry_min": 10.1,
                "entry_max": 10.6,
                "take_profit": 11.3,
                "stop_loss": 9.7,
                "analysis_scope": "portfolio",
            }
        }
        alert_item = {
            "config": {
                "runtime_thresholds": {
                    "entry_min": 10.2,
                    "entry_max": 10.7,
                    "take_profit": 11.5,
                    "stop_loss": 9.6,
                },
                "threshold_source": "ai_runtime",
            }
        }

        primary_line, secondary_line = smart_monitor_ui._build_watchlist_threshold_lines(task, alert_item)

        self.assertEqual(primary_line, "预警线(分析基线): 进场 10.1 - 10.6 | 止盈 11.3 | 止损 9.7")
        self.assertEqual(secondary_line, "运行时阈值(盘中分析): 进场 10.2 - 10.7 | 止盈 11.5 | 止损 9.6")

    def test_build_watchlist_threshold_lines_falls_back_to_runtime_levels(self):
        task = {"strategy_context": {}}
        alert_item = {
            "config": {
                "runtime_thresholds": {
                    "entry_min": 51.2,
                    "entry_max": 55.0,
                    "take_profit": 59.0,
                    "stop_loss": 48.5,
                },
                "threshold_source": "ai_runtime",
            }
        }

        primary_line, secondary_line = smart_monitor_ui._build_watchlist_threshold_lines(task, alert_item)

        self.assertEqual(primary_line, "预警线(运行时): 进场 51.2 - 55.0 | 止盈 59.0 | 止损 48.5")
        self.assertIsNone(secondary_line)


if __name__ == "__main__":
    unittest.main()
