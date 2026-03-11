import unittest

import smart_monitor_ui


class SmartMonitorUIHistorySplitTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
