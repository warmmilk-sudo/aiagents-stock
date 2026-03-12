import tempfile
import unittest
from pathlib import Path

from smart_monitor_db import SmartMonitorDB


class SmartMonitorDBTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)
        self.db = SmartMonitorDB(str(self.base / "smart_monitor.db"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_same_action_still_persists_latest_intraday_decision_with_new_time(self):
        first_id, first_changed = self.db.save_ai_decision_if_changed(
            {
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "account_name": "默认账户",
                "decision_time": "2026-03-12 09:30:00",
                "trading_session": "上午盘",
                "action": "HOLD",
                "confidence": 78,
                "reasoning": "第一次盘中分析",
                "risk_level": "中",
                "market_data": {"current_price": 1520.0},
                "account_info": {"account_name": "默认账户"},
                "execution_mode": "manual_only",
                "action_status": "suggested",
            }
        )
        second_id, second_changed = self.db.save_ai_decision_if_changed(
            {
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "account_name": "默认账户",
                "decision_time": "2026-03-12 09:35:00",
                "trading_session": "上午盘",
                "action": "HOLD",
                "confidence": 81,
                "reasoning": "第二次盘中分析",
                "risk_level": "中",
                "market_data": {"current_price": 1521.5},
                "account_info": {"account_name": "默认账户"},
                "execution_mode": "manual_only",
                "action_status": "suggested",
            }
        )

        self.assertTrue(first_changed)
        self.assertFalse(second_changed)
        self.assertNotEqual(first_id, second_id)

        decisions = self.db.get_ai_decisions(stock_code="600519", limit=2)
        self.assertEqual(len(decisions), 2)
        self.assertEqual(decisions[0]["id"], second_id)
        self.assertEqual(decisions[0]["decision_time"], "2026-03-12 09:35:00")
        self.assertEqual(decisions[0]["reasoning"], "第二次盘中分析")
        self.assertEqual(decisions[1]["id"], first_id)
        self.assertEqual(decisions[1]["decision_time"], "2026-03-12 09:30:00")


if __name__ == "__main__":
    unittest.main()
