import tempfile
import unittest
from pathlib import Path

from monitor_db import StockMonitorDatabase
from smart_monitor_db import SmartMonitorDB


class SmartMonitorDBTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)
        self.db = SmartMonitorDB(str(self.base / "smart_monitor.db"))
        self.monitor_db = StockMonitorDatabase(str(self.base / "smart_monitor.db"))

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
                "monitor_levels": {
                    "entry_min": 1498.0,
                    "entry_max": 1506.0,
                    "take_profit": 1588.0,
                    "stop_loss": 1452.0,
                },
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
                "monitor_levels": {
                    "entry_min": 1501.0,
                    "entry_max": 1508.0,
                    "take_profit": 1592.0,
                    "stop_loss": 1458.0,
                },
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
        self.assertEqual(decisions[0]["monitor_levels"]["take_profit"], 1592.0)
        self.assertEqual(decisions[1]["id"], first_id)
        self.assertEqual(decisions[1]["decision_time"], "2026-03-12 09:30:00")
        self.assertEqual(decisions[1]["monitor_levels"]["entry_min"], 1498.0)

    def test_monitor_task_strategy_context_uses_latest_report_timestamp_across_scopes(self):
        success, _, asset_id = self.db.asset_service.promote_to_watchlist(
            symbol="600519",
            stock_name="贵州茅台",
            account_name="默认账户",
        )
        self.assertTrue(success)
        self.assertIsNotNone(asset_id)

        self.db.analysis_repository.save_record(
            symbol="600519",
            stock_name="贵州茅台",
            period="1y",
            stock_info={"symbol": "600519", "name": "贵州茅台", "current_price": 1500.0},
            agents_results={"technical": {"analysis": "较早持仓分析"}},
            discussion_result="较早持仓分析",
            final_decision={
                "rating": "持有",
                "entry_min": 1480.0,
                "entry_max": 1500.0,
                "take_profit": 1580.0,
                "stop_loss": 1450.0,
                "operation_advice": "旧基线",
            },
            account_name="默认账户",
            asset_id=asset_id,
            portfolio_stock_id=asset_id,
            analysis_scope="portfolio",
            analysis_source="portfolio_single_analysis",
            analysis_date="2026-03-12 09:00:00",
            summary="较早持仓分析",
            has_full_report=True,
            asset_status_snapshot="portfolio",
        )

        latest_research_id = self.db.analysis_repository.save_record(
            symbol="600519",
            stock_name="贵州茅台",
            period="1y",
            stock_info={"symbol": "600519", "name": "贵州茅台", "current_price": 1515.0},
            agents_results={"technical": {"analysis": "更新深度分析"}},
            discussion_result="更新深度分析",
            final_decision={
                "rating": "买入",
                "entry_min": 1510.0,
                "entry_max": 1520.0,
                "take_profit": 1620.0,
                "stop_loss": 1470.0,
                "operation_advice": "最新基线",
            },
            account_name="默认账户",
            analysis_scope="research",
            analysis_source="home_single_analysis",
            analysis_date="2026-03-12 10:30:00",
            summary="更新深度分析",
            has_full_report=True,
            asset_status_snapshot="research",
        )

        task_id = self.db.upsert_monitor_task(
            {
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "account_name": "默认账户",
                "asset_id": asset_id,
                "enabled": 1,
                "check_interval": 3600,
                "trading_hours_only": 1,
            }
        )
        self.assertGreater(task_id, 0)

        task = self.db.get_monitor_task_by_code("600519", account_name="默认账户", asset_id=asset_id)
        self.assertIsNotNone(task)
        self.assertEqual(task["origin_analysis_id"], latest_research_id)
        self.assertEqual(task["strategy_context"]["origin_analysis_id"], latest_research_id)
        self.assertEqual(task["strategy_context"]["analysis_scope"], "research")
        self.assertEqual(task["strategy_context"]["summary"], "更新深度分析")

    def test_new_tasks_and_alerts_use_runtime_config_defaults_when_interval_missing(self):
        self.db.monitoring_repository.set_metadata("smart_monitor_intraday_decision_interval_minutes", "30")
        self.db.monitoring_repository.set_metadata("smart_monitor_realtime_monitor_interval_minutes", "2")

        task_id = self.db.upsert_monitor_task(
            {
                "stock_code": "000001",
                "stock_name": "平安银行",
                "account_name": "默认账户",
                "enabled": 1,
                "trading_hours_only": 1,
            }
        )
        alert_id = self.monitor_db.add_monitored_stock(
            symbol="300750",
            name="宁德时代",
            rating="买入",
            entry_range={"min": 200.0, "max": 210.0},
            take_profit=220.0,
            stop_loss=190.0,
            check_interval=None,
        )

        self.assertEqual(self.db.monitoring_repository.get_item(task_id)["interval_minutes"], 30)
        self.assertEqual(self.monitor_db.repository.get_item(alert_id)["interval_minutes"], 2)


if __name__ == "__main__":
    unittest.main()
