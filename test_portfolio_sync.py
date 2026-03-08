import tempfile
import unittest

from monitor_db import StockMonitorDatabase
from portfolio_db import PortfolioDB
from portfolio_manager import PortfolioManager
from smart_monitor_db import SmartMonitorDB


class PortfolioIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        base = self.temp_dir.name

        self.portfolio_db = PortfolioDB(f"{base}/portfolio.db")
        self.realtime_monitor_db = StockMonitorDatabase(f"{base}/monitor.db")
        self.smart_monitor_db = SmartMonitorDB(f"{base}/smart.db")
        self.manager = PortfolioManager(
            portfolio_store=self.portfolio_db,
            realtime_monitor_store=self.realtime_monitor_db,
            smart_monitor_store=self.smart_monitor_db,
        )
        self.manager._resolve_stock_name = lambda code: f"Stock{code}"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _add_stock(self, code: str, auto_monitor: bool = True, cost_price: float = 10.0, quantity: int = 100):
        success, msg, stock_id = self.manager.add_stock(
            code=code,
            name=None,
            cost_price=cost_price,
            quantity=quantity,
            note="test",
            auto_monitor=auto_monitor,
        )
        self.assertTrue(success, msg)
        self.assertIsNotNone(stock_id)
        return stock_id

    def test_reconcile_portfolio_integrations_only_syncs_downstream(self):
        self._add_stock("600519")
        result = self.manager.reconcile_portfolio_integrations()
        self.assertIn("smart_monitor_sync", result)
        self.assertIn("realtime_monitor_sync", result)
        self.assertIn("cleanup", result)
        self.assertNotIn("history_migration", result)
        self.assertNotIn("legacy_backfill", result)

    def test_add_and_update_stock_syncs_to_smart_monitor(self):
        stock_id = self._add_stock("000001", cost_price=10.5, quantity=200)
        snapshots = self.portfolio_db.get_daily_snapshots(account_name="默认账户")
        self.assertTrue(snapshots)
        self.assertEqual(snapshots[-1]["account_name"], "默认账户")

        task = self.smart_monitor_db.get_monitor_task_by_code("000001", managed_only=True)
        self.assertIsNotNone(task)
        self.assertEqual(task["enabled"], 0)
        self.assertEqual(task["has_position"], 1)
        self.assertEqual(task["position_cost"], 10.5)
        self.assertEqual(task["position_quantity"], 200)
        self.assertEqual(task["managed_by_portfolio"], 1)

        success, msg = self.manager.update_stock(stock_id, cost_price=12.3, quantity=500, auto_monitor=True)
        self.assertTrue(success, msg)
        updated_snapshots = self.portfolio_db.get_daily_snapshots(account_name="默认账户")
        self.assertTrue(updated_snapshots[-1]["total_market_value"] >= 12.3 * 500)

        updated_task = self.smart_monitor_db.get_monitor_task_by_code("000001", managed_only=True)
        self.assertIsNotNone(updated_task)
        self.assertEqual(updated_task["position_cost"], 12.3)
        self.assertEqual(updated_task["position_quantity"], 500)

        success, msg = self.manager.update_stock(stock_id, auto_monitor=False)
        self.assertTrue(success, msg)
        self.assertIsNone(self.smart_monitor_db.get_monitor_task_by_code("000001", managed_only=True))

    def test_persist_analysis_results_saves_portfolio_history_and_syncs_realtime_monitor(self):
        stock_id = self._add_stock("300750", cost_price=150.0, quantity=100)

        analysis_results = {
            "success": True,
            "results": [
                {
                    "code": "300750",
                    "result": {
                        "success": True,
                        "stock_info": {"symbol": "300750", "name": "Stock300750", "current_price": 152.6},
                        "final_decision": {
                            "rating": "持有",
                            "confidence_level": 7.2,
                            "entry_range": "148.0-151.0",
                            "take_profit": "168.8元",
                            "stop_loss": "142.3元",
                            "operation_advice": "等待下一次放量突破",
                        },
                    },
                }
            ],
        }

        result = self.manager.persist_analysis_results(analysis_results, sync_realtime_monitor=True)
        self.assertEqual(len(result["saved_ids"]), 1)
        snapshots = self.portfolio_db.get_daily_snapshots(account_name="默认账户")
        self.assertTrue(snapshots)

        history = self.portfolio_db.get_analysis_history(stock_id, limit=10)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["rating"], "持有")

        monitor = self.realtime_monitor_db.get_monitor_by_code("300750", managed_only=True)
        self.assertIsNotNone(monitor)
        self.assertEqual(monitor["entry_range"]["min"], 148.0)
        self.assertEqual(monitor["entry_range"]["max"], 151.0)
        self.assertEqual(monitor["take_profit"], 168.8)
        self.assertEqual(monitor["stop_loss"], 142.3)
        self.assertTrue(monitor["managed_by_portfolio"])

        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="买入",
            confidence=9.0,
            current_price=155.0,
            entry_min=149.0,
            entry_max=152.0,
            take_profit=170.0,
            stop_loss=144.0,
            summary="新分析",
            has_full_report=True,
        )
        sync_result = self.manager.sync_latest_analysis_to_realtime_monitor(["300750"])
        self.assertEqual(sync_result["total"], 1)

        monitors = [
            stock for stock in self.realtime_monitor_db.get_monitored_stocks()
            if stock["symbol"] == "300750" and stock["managed_by_portfolio"]
        ]
        self.assertEqual(len(monitors), 1)
        self.assertEqual(monitors[0]["take_profit"], 170.0)

    def test_batch_analysis_can_persist_history_incrementally(self):
        first_stock_id = self._add_stock("000001", cost_price=10.0, quantity=100)
        second_stock_id = self._add_stock("600519", cost_price=1500.0, quantity=10)

        fake_results = {
            "000001": {
                "success": True,
                "stock_info": {"symbol": "000001", "name": "Stock000001", "current_price": 10.8},
                "final_decision": {
                    "rating": "买入",
                    "confidence_level": 7.6,
                    "entry_range": "10.2-10.5",
                    "take_profit": "11.6元",
                    "stop_loss": "9.8元",
                    "operation_advice": "放量突破后继续持有。",
                },
            },
            "600519": {
                "success": True,
                "stock_info": {"symbol": "600519", "name": "Stock600519", "current_price": 1512.0},
                "final_decision": {
                    "rating": "持有",
                    "confidence_level": 6.9,
                    "entry_range": "1490-1505",
                    "take_profit": "1580元",
                    "stop_loss": "1450元",
                    "operation_advice": "趋势仍在，继续观察量价配合。",
                },
            },
        }

        callback_order = []
        total_history_counts = []

        def fake_analyze_single_stock(code, *args, **kwargs):
            return fake_results[code]

        def result_callback(code, result):
            callback_order.append(code)
            self.manager.persist_single_analysis_result(code, result, sync_realtime_monitor=False)
            total_history = (
                len(self.portfolio_db.get_analysis_history(first_stock_id, limit=10))
                + len(self.portfolio_db.get_analysis_history(second_stock_id, limit=10))
            )
            total_history_counts.append(total_history)

        self.manager.analyze_single_stock = fake_analyze_single_stock

        result = self.manager.batch_analyze_sequential(
            ["000001", "600519"],
            progress_callback=None,
            result_callback=result_callback,
        )

        self.assertTrue(result["success"])
        self.assertEqual(callback_order, ["000001", "600519"])
        self.assertEqual(total_history_counts, [1, 2])
        self.assertEqual(len(self.portfolio_db.get_analysis_history(first_stock_id, limit=10)), 1)
        self.assertEqual(len(self.portfolio_db.get_analysis_history(second_stock_id, limit=10)), 1)

    def test_delete_stock_cascades_managed_integrations(self):
        stock_id = self._add_stock("002594", cost_price=220.0, quantity=300)
        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="买入",
            confidence=8.0,
            current_price=225.0,
            entry_min=218.0,
            entry_max=222.0,
            take_profit=240.0,
            stop_loss=210.0,
            summary="测试清理",
            has_full_report=True,
        )
        self.manager.sync_latest_analysis_to_realtime_monitor(["002594"])

        self.assertIsNotNone(self.smart_monitor_db.get_monitor_task_by_code("002594", managed_only=True))
        self.assertIsNotNone(self.realtime_monitor_db.get_monitor_by_code("002594", managed_only=True))

        success, msg = self.manager.delete_stock(stock_id)
        self.assertTrue(success, msg)
        self.assertIsNone(self.smart_monitor_db.get_monitor_task_by_code("002594", managed_only=True))
        self.assertIsNone(self.realtime_monitor_db.get_monitor_by_code("002594", managed_only=True))


if __name__ == "__main__":
    unittest.main()
