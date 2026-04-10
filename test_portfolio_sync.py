import os
import sqlite3
import tempfile
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault(
    "numpy",
    types.SimpleNamespace(
        array=lambda *args, **kwargs: args[0] if args else None,
        nan=float("nan"),
        isfinite=lambda value: value == value,
        mean=lambda values, *args, **kwargs: sum(values) / len(values) if values else 0,
    ),
)
sys.modules.setdefault(
    "pandas",
    types.SimpleNamespace(
        DataFrame=type("DataFrame", (), {}),
        Series=type("Series", (), {}),
        Timestamp=type("Timestamp", (), {}),
        isna=lambda value: False,
        to_datetime=lambda value, *args, **kwargs: value,
        to_numeric=lambda value, *args, **kwargs: value,
        bdate_range=lambda *args, **kwargs: [],
        date_range=lambda *args, **kwargs: [],
        concat=lambda *args, **kwargs: None,
    ),
)
sys.modules.setdefault(
    "smart_monitor_data",
    types.SimpleNamespace(
        SmartMonitorDataFetcher=type(
            "SmartMonitorDataFetcher",
            (),
            {"__init__": lambda self, *args, **kwargs: None, "get_comprehensive_data": lambda self, *args, **kwargs: {}},
        )
    ),
)
sys.modules.setdefault(
    "smart_monitor_deepseek",
    types.SimpleNamespace(
        SmartMonitorDeepSeek=type(
            "SmartMonitorDeepSeek",
            (),
            {
                "__init__": lambda self, *args, **kwargs: None,
                "http_timeout_seconds": 15,
                "set_model_overrides": lambda self, *args, **kwargs: None,
                "get_trading_session": lambda self: {"session": "上午盘", "can_trade": True, "recommendation": ""},
                "analyze_stock_and_decide": lambda self, **kwargs: {"success": False},
            },
        )
    ),
)

from investment_db_utils import DEFAULT_ACCOUNT_NAME
import smart_monitor_engine as smart_monitor_engine_module
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

    def _add_stock(
        self,
        code: str,
        auto_monitor: bool = True,
        cost_price: float = 10.0,
        quantity: int = 100,
        account_name: str = DEFAULT_ACCOUNT_NAME,
    ):
        success, msg, stock_id = self.manager.add_stock(
            code=code,
            name=None,
            cost_price=cost_price,
            quantity=quantity,
            note="test",
            auto_monitor=auto_monitor,
            account_name=account_name,
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
        snapshots = self.portfolio_db.get_daily_snapshots(account_name=DEFAULT_ACCOUNT_NAME)
        self.assertTrue(snapshots)
        self.assertEqual(snapshots[-1]["account_name"], DEFAULT_ACCOUNT_NAME)

        task = self.smart_monitor_db.get_monitor_task_by_code("000001", managed_only=True)
        self.assertIsNotNone(task)
        self.assertEqual(task["enabled"], 1)
        self.assertEqual(task["has_position"], 1)
        self.assertEqual(task["position_cost"], 10.5)
        self.assertEqual(task["position_quantity"], 200)
        self.assertEqual(task["managed_by_portfolio"], 1)

        success, msg = self.manager.update_stock(stock_id, cost_price=12.3, quantity=500, auto_monitor=True)
        self.assertTrue(success, msg)
        updated_snapshots = self.portfolio_db.get_daily_snapshots(account_name=DEFAULT_ACCOUNT_NAME)
        self.assertTrue(updated_snapshots[-1]["total_market_value"] >= 12.3 * 500)

        updated_task = self.smart_monitor_db.get_monitor_task_by_code("000001", managed_only=True)
        self.assertIsNotNone(updated_task)
        self.assertEqual(updated_task["position_cost"], 12.3)
        self.assertEqual(updated_task["position_quantity"], 500)

        success, msg = self.manager.update_stock(stock_id, auto_monitor=False)
        self.assertTrue(success, msg)
        disabled_task = self.smart_monitor_db.get_monitor_task_by_code("000001", managed_only=True)
        self.assertIsNotNone(disabled_task)
        self.assertEqual(disabled_task["enabled"], 0)
        disabled_alert = self.realtime_monitor_db.get_monitor_by_code(
            "000001",
            account_name=DEFAULT_ACCOUNT_NAME,
            asset_id=stock_id,
        )
        self.assertIsNotNone(disabled_alert)
        self.assertFalse(disabled_alert["enabled"])

    def test_add_stock_survives_snapshot_failure_after_position_created(self):
        with patch.object(self.manager, "capture_daily_snapshot", side_effect=RuntimeError("snapshot broken")):
            success, msg, stock_id = self.manager.add_stock(
                code="688256",
                name=None,
                cost_price=36.5,
                quantity=100,
                note="snapshot-failure",
                auto_monitor=True,
            )

        self.assertTrue(success, msg)
        self.assertIsNotNone(stock_id)
        self.assertIn("快照补写失败", msg)
        stock = self.portfolio_db.get_stock(stock_id)
        self.assertIsNotNone(stock)
        self.assertEqual(stock["code"], "688256")

    def test_add_stock_survives_monitor_sync_failure_after_position_created(self):
        with patch.object(
            self.manager.lifecycle_service.asset_service,
            "sync_managed_monitors",
            side_effect=RuntimeError("monitor sync broken"),
        ):
            success, msg, stock_id = self.manager.add_stock(
                code="688001",
                name=None,
                cost_price=45.0,
                quantity=200,
                note="monitor-failure",
                auto_monitor=True,
            )

        self.assertTrue(success, msg)
        self.assertIsNotNone(stock_id)
        self.assertIn("监测同步失败", msg)
        stock = self.portfolio_db.get_stock(stock_id)
        self.assertIsNotNone(stock)
        self.assertEqual(stock["code"], "688001")

    def test_managed_task_sync_uses_shared_risk_profile(self):
        self.smart_monitor_db.monitoring_repository.set_shared_risk_profile(
            {
                "position_size_pct": 30,
                "total_position_pct": 75,
                "stop_loss_pct": 6,
                "take_profit_pct": 16,
            }
        )

        stock_id = self._add_stock("002594", cost_price=220.0, quantity=100, account_name="账户A")
        task = self.smart_monitor_db.get_monitor_task_by_code("002594", managed_only=True, account_name=DEFAULT_ACCOUNT_NAME)

        self.assertIsNotNone(task)
        self.assertEqual(task["asset_id"], stock_id)
        self.assertEqual(task["position_size_pct"], 30)
        self.assertEqual(task["total_position_pct"], 75)
        self.assertEqual(task["stop_loss_pct"], 6)
        self.assertEqual(task["take_profit_pct"], 16)

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
        snapshots = self.portfolio_db.get_daily_snapshots(account_name=DEFAULT_ACCOUNT_NAME)
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

    def test_persist_analysis_results_refreshes_managed_monitor_baseline(self):
        self._add_stock("600519", cost_price=1500.0, quantity=10)

        analysis_results = {
            "success": True,
            "results": [
                {
                    "code": "600519",
                    "result": {
                        "success": True,
                        "stock_info": {"symbol": "600519", "name": "Stock600519", "current_price": 1520.0},
                        "final_decision": {
                            "rating": "买入",
                            "confidence_level": 8.1,
                            "entry_range": "1510-1530",
                            "take_profit": "1600",
                            "stop_loss": "1488",
                            "operation_advice": "基线已更新，继续观察。",
                        },
                    },
                }
            ],
        }

        with patch.object(
            self.manager.lifecycle_service.asset_service,
            "sync_managed_monitors_for_symbol",
            wraps=self.manager.lifecycle_service.asset_service.sync_managed_monitors_for_symbol,
        ) as mock_sync:
            result = self.manager.persist_analysis_results(analysis_results, sync_realtime_monitor=False)

        self.assertEqual(result["baseline_sync_result"]["ai_tasks_upserted"], 1)
        mock_sync.assert_called_once_with("600519", account_name=None)
        task = self.smart_monitor_db.get_monitor_task_by_code("600519", managed_only=True)
        self.assertIsNotNone(task)
        self.assertIsInstance(task.get("strategy_context"), dict)
        self.assertEqual(task["strategy_context"].get("rating"), "买入")

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

    def test_bulk_toggle_ai_monitor_tasks(self):
        self.smart_monitor_db.upsert_monitor_task(
            {
                "task_name": "茅台任务",
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "enabled": 1,
            }
        )
        self.smart_monitor_db.upsert_monitor_task(
            {
                "task_name": "平安银行任务",
                "stock_code": "000001",
                "stock_name": "平安银行",
                "enabled": 0,
            }
        )

        changed = self.smart_monitor_db.set_all_monitor_tasks_enabled(True)
        self.assertEqual(changed, 1)

        enabled_tasks = self.smart_monitor_db.get_monitor_tasks(enabled_only=False)
        self.assertEqual(len(enabled_tasks), 2)
        self.assertTrue(all(task["enabled"] == 1 for task in enabled_tasks))
        enabled_alerts = {
            stock["symbol"]: stock
            for stock in self.realtime_monitor_db.get_monitored_stocks()
        }
        self.assertIn("600519", enabled_alerts)
        self.assertIn("000001", enabled_alerts)
        self.assertTrue(all(alert["enabled"] for alert in enabled_alerts.values()))

        changed = self.smart_monitor_db.set_all_monitor_tasks_enabled(False)
        self.assertEqual(changed, 2)

        disabled_tasks = self.smart_monitor_db.get_monitor_tasks(enabled_only=False)
        self.assertEqual(len(disabled_tasks), 2)
        self.assertTrue(all(task["enabled"] == 0 for task in disabled_tasks))
        disabled_alerts = self.realtime_monitor_db.get_monitored_stocks()
        self.assertEqual(len(disabled_alerts), 2)
        self.assertTrue(all(not alert["enabled"] for alert in disabled_alerts))

    def test_watchlist_task_sync_creates_placeholder_price_alert(self):
        self.smart_monitor_db.upsert_monitor_task(
            {
                "task_name": "宁德时代任务",
                "stock_code": "300750",
                "stock_name": "宁德时代",
                "enabled": 1,
                "account_name": "默认账户",
            }
        )

        alert = self.realtime_monitor_db.get_monitor_by_code("300750", account_name=DEFAULT_ACCOUNT_NAME)

        self.assertIsNotNone(alert)
        self.assertTrue(alert["enabled"])
        self.assertEqual(alert["check_interval"], 3)
        self.assertIn(alert["threshold_source"], {"pending_ai", "strategy_context", "ai_runtime", "manual"})

    def test_save_ai_decision_resolves_binding_and_persists_extended_columns(self):
        asset_id = self.smart_monitor_db.asset_repository.promote_to_watchlist(
            symbol="600519",
            name="贵州茅台",
            account_name="账户A",
        )
        self.smart_monitor_db.upsert_monitor_task(
            {
                "task_name": "茅台任务",
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "enabled": 1,
                "account_name": "账户A",
                "asset_id": asset_id,
            }
        )

        decision_id = self.smart_monitor_db.save_ai_decision(
            {
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "action": "BUY",
                "confidence": 88,
                "reasoning": "测试决策写入",
                "market_data": {"code": "600519"},
                "account_info": {"cash": 1000},
            }
        )

        decisions = self.smart_monitor_db.get_ai_decisions("600519", limit=5)
        self.assertEqual(decisions[0]["id"], decision_id)
        self.assertEqual(decisions[0]["account_name"], DEFAULT_ACCOUNT_NAME)
        self.assertEqual(decisions[0]["asset_id"], asset_id)
        self.assertEqual(decisions[0]["execution_mode"], "manual_only")
        self.assertEqual(decisions[0]["action_status"], "pending")
        self.assertEqual(decisions[0]["decision_context"], {})

    def test_save_ai_decision_derives_decision_context_from_market_data_intraday_context(self):
        decision_id = self.smart_monitor_db.save_ai_decision(
            {
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "action": "HOLD",
                "confidence": 83,
                "reasoning": "分时高位量能衰减，先观察。",
                "market_data": {
                    "code": "600519",
                    "intraday_context": {
                        "intraday_bias": "high_level_stall",
                        "intraday_bias_text": "价格靠近日内高位，但量能衰减",
                        "intraday_signal_labels": ["价格运行在分时均价上方", "高位量能衰减"],
                        "intraday_observations": ["当前价格接近日内高位"],
                        "price_position_pct": 90.56,
                        "last_5m_change_pct": -0.08,
                    },
                },
                "account_info": {"cash": 1000},
            }
        )

        decisions = self.smart_monitor_db.get_ai_decisions("600519", limit=5)

        self.assertEqual(decisions[0]["id"], decision_id)
        self.assertEqual(decisions[0]["intraday_bias"], "high_level_stall")
        self.assertEqual(decisions[0]["intraday_bias_text"], "价格靠近日内高位，但量能衰减")
        self.assertEqual(decisions[0]["intraday_signal_labels"], ["价格运行在分时均价上方", "高位量能衰减"])
        self.assertEqual(decisions[0]["decision_context"]["price_position_pct"], 90.56)

    def test_smart_monitor_db_cleans_invalid_and_duplicate_notifications(self):
        canonical_path = Path(self.temp_dir.name) / "investment.db"
        conn = sqlite3.connect(canonical_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM notifications")
        cursor.execute("DELETE FROM investment_metadata WHERE meta_key = ?", (SmartMonitorDB.NOTIFICATION_CLEANUP_MIGRATION_KEY,))
        valid_row = (
            "600519",
            "decision",
            None,
            "智能盯盘 - 买入信号",
            "测试通知正文",
            "queued",
            None,
            None,
            "2026-03-11 10:00:00",
        )
        cursor.executemany(
            """
            INSERT INTO notifications (
                stock_code, notify_type, notify_target, subject, content,
                status, error_msg, sent_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                valid_row,
                valid_row,
                ("000001", "", None, "", "", "pending", None, None, "2026-03-11 10:01:00"),
            ],
        )
        conn.commit()
        conn.close()

        repaired_db = SmartMonitorDB(str(Path(self.temp_dir.name) / "smart.db"))
        conn = sqlite3.connect(canonical_path)
        cursor = conn.cursor()
        cursor.execute("SELECT stock_code, notify_type, status FROM notifications ORDER BY id ASC")
        rows = cursor.fetchall()
        conn.close()

        self.assertEqual(rows, [("600519", "decision", "pending")])
        cleanup_meta = repaired_db._connect()
        meta_cursor = cleanup_meta.cursor()
        meta_cursor.execute(
            "SELECT meta_value FROM investment_metadata WHERE meta_key = ?",
            (SmartMonitorDB.NOTIFICATION_CLEANUP_MIGRATION_KEY,),
        )
        meta_value = meta_cursor.fetchone()
        cleanup_meta.close()
        self.assertIsNotNone(meta_value)

    def test_smart_monitor_db_does_not_fallback_to_legacy_position_and_trade_tables(self):
        isolated_dir = Path(self.temp_dir.name) / "legacy_only"
        isolated_dir.mkdir(parents=True, exist_ok=True)
        legacy_path = isolated_dir / "smart.db"
        legacy_conn = sqlite3.connect(legacy_path)
        legacy_cursor = legacy_conn.cursor()
        legacy_cursor.execute(
            """
            CREATE TABLE position_monitor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                quantity INTEGER,
                cost_price REAL,
                status TEXT
            )
            """
        )
        legacy_cursor.execute(
            """
            CREATE TABLE trade_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT,
                stock_name TEXT,
                trade_type TEXT,
                quantity INTEGER,
                price REAL,
                trade_time TEXT
            )
            """
        )
        legacy_cursor.execute(
            """
            INSERT INTO position_monitor (stock_code, stock_name, quantity, cost_price, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("600519", "贵州茅台", 100, 1500.0, "holding"),
        )
        legacy_cursor.execute(
            """
            INSERT INTO trade_records (stock_code, stock_name, trade_type, quantity, price, trade_time)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("600519", "贵州茅台", "BUY", 100, 1500.0, "2026-03-11 10:00:00"),
        )
        legacy_conn.commit()
        legacy_conn.close()

        original_cwd = os.getcwd()
        os.chdir(isolated_dir)
        try:
            repaired_db = SmartMonitorDB(str(legacy_path))
            self.assertEqual(repaired_db.get_positions(), [])
            self.assertEqual(repaired_db.get_trade_records("600519", limit=10), [])
        finally:
            os.chdir(original_cwd)

    def test_smart_monitor_db_repair_dedupes_and_backfills_legacy_ai_decisions(self):
        repair_dir = Path(self.temp_dir.name) / "repair_case"
        repair_dir.mkdir(parents=True, exist_ok=True)
        seed_path = repair_dir / "smart.db"
        canonical_path = repair_dir / "investment.db"

        db = SmartMonitorDB(str(seed_path))
        asset_id = db.asset_repository.promote_to_watchlist(
            symbol="300136",
            name="信维通信",
            account_name="账户B",
        )
        db.upsert_monitor_task(
            {
                "task_name": "信维任务",
                "stock_code": "300136",
                "stock_name": "信维通信",
                "enabled": 1,
                "account_name": "账户B",
                "asset_id": asset_id,
            }
        )

        conn = sqlite3.connect(canonical_path)
        cursor = conn.cursor()
        duplicate_row = (
            "300136",
            "信维通信",
            "2026-03-10 09:30:00",
            "morning",
            "SELL",
            76,
            "重复历史记录",
            20.0,
            5.0,
            10.0,
            "medium",
            "{}",
            "{}",
            "{}",
            0,
            None,
            "2026-03-10 09:30:00",
        )
        cursor.executemany(
            """
            INSERT INTO ai_decisions (
                stock_code, stock_name, decision_time, trading_session, action, confidence,
                reasoning, position_size_pct, stop_loss_pct, take_profit_pct, risk_level,
                key_price_levels, market_data, account_info, executed, execution_result, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [duplicate_row, duplicate_row],
        )
        conn.commit()
        conn.close()

        repaired_db = SmartMonitorDB(str(seed_path))
        decisions = repaired_db.get_ai_decisions("300136", limit=10)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0]["account_name"], DEFAULT_ACCOUNT_NAME)
        self.assertEqual(decisions[0]["asset_id"], asset_id)

    def test_ai_runtime_thresholds_sync_and_clear_on_new_strategy_context(self):
        asset_id = self.smart_monitor_db.asset_repository.promote_to_watchlist(
            symbol="600519",
            name="贵州茅台",
            account_name="默认账户",
        )
        self.smart_monitor_db.upsert_monitor_task(
            {
                "task_name": "茅台任务",
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "enabled": 1,
                "account_name": "默认账户",
                "asset_id": asset_id,
            }
        )

        with patch.object(smart_monitor_engine_module, "SmartMonitorDB", return_value=self.smart_monitor_db), patch.object(
            smart_monitor_engine_module.event_bus,
            "subscribe",
            return_value=None,
        ), patch("monitor_db.monitor_db", self.realtime_monitor_db):
            engine = smart_monitor_engine_module.SmartMonitorEngine(deepseek_api_key="stub")
            synced = engine._sync_runtime_thresholds(
                stock_code="600519",
                stock_name="贵州茅台",
                decision={"action": "BUY", "monitor_levels": {"entry_min": 1498.0, "entry_max": 1506.0, "take_profit": 1588.0, "stop_loss": 1452.0}},
                decision_id=77,
                account_name="默认账户",
                asset_id=asset_id,
                portfolio_stock_id=None,
                strategy_context={},
            )

        self.assertTrue(synced)
        monitor = self.realtime_monitor_db.get_monitor_by_code("600519", account_name=DEFAULT_ACCOUNT_NAME, asset_id=asset_id)
        self.assertIsNotNone(monitor)
        self.assertEqual(monitor["threshold_source"], "ai_runtime")
        self.assertEqual(monitor["entry_range"]["min"], 1498.0)
        self.assertEqual(monitor["take_profit"], 1588.0)
        self.assertEqual(monitor["origin_decision_id"], 77)

        self.smart_monitor_db.asset_service.sync_managed_monitors(asset_id)
        preserved = self.realtime_monitor_db.get_monitor_by_code("600519", account_name=DEFAULT_ACCOUNT_NAME, asset_id=asset_id)
        self.assertEqual(preserved["threshold_source"], "ai_runtime")
        self.assertEqual(preserved["runtime_thresholds"]["stop_loss"], 1452.0)

        new_record_id = self.smart_monitor_db.analysis_repository.save_record(
            symbol="600519",
            stock_name="贵州茅台",
            period="1y",
            account_name="默认账户",
            asset_id=asset_id,
            analysis_scope="portfolio",
            analysis_source="test",
            entry_min=1510.0,
            entry_max=1520.0,
            take_profit=1618.0,
            stop_loss=1480.0,
            final_decision={"rating": "持有"},
            has_full_report=True,
            asset_status_snapshot="watchlist",
        )
        self.assertGreater(new_record_id, 0)

        self.smart_monitor_db.asset_service.sync_managed_monitors(asset_id)
        refreshed = self.realtime_monitor_db.get_monitor_by_code("600519", account_name=DEFAULT_ACCOUNT_NAME, asset_id=asset_id)
        self.assertEqual(refreshed["threshold_source"], "strategy_context")
        self.assertEqual(refreshed["entry_range"]["min"], 1510.0)
        self.assertEqual(refreshed["take_profit"], 1618.0)
        self.assertEqual(refreshed["runtime_thresholds"], {})

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
