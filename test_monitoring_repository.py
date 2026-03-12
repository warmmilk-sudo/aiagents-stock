import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from asset_repository import AssetRepository, STATUS_PORTFOLIO, STATUS_WATCHLIST
from monitoring_repository import MonitoringRepository


class MonitoringRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)
        self.repo = MonitoringRepository(str(self.base / "monitoring.db"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_ai_task_upsert_keeps_single_item_and_merges_config(self):
        first_id = self.repo.upsert_item(
            {
                "symbol": "600519",
                "name": "贵州茅台",
                "monitor_type": "ai_task",
                "interval_minutes": 5,
                "config": {"auto_trade": True, "position_size_pct": 20},
            }
        )
        second_id = self.repo.upsert_item(
            {
                "symbol": "600519",
                "name": "贵州茅台",
                "monitor_type": "ai_task",
                "interval_minutes": 7,
                "config": {"stop_loss_pct": 6},
            }
        )

        items = self.repo.list_items(monitor_type="ai_task")
        self.assertEqual(len(items), 1)
        self.assertEqual(first_id, second_id)
        self.assertEqual(items[0]["interval_minutes"], 7)
        self.assertNotIn("auto_trade", items[0]["config"])
        self.assertEqual(items[0]["config"]["position_size_pct"], 20)
        self.assertEqual(items[0]["config"]["stop_loss_pct"], 6)

    def test_repository_init_cleans_deprecated_config_keys_from_existing_items(self):
        conn = self.repo._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO monitoring_items (
                symbol, name, monitor_type, config_json
            ) VALUES (?, ?, ?, ?)
            """,
            (
                "600000",
                "浦发银行",
                "ai_task",
                json.dumps(
                    {
                        "auto_trade": True,
                        "qmt_account_id": "acct-001",
                        "quant_enabled": True,
                        "quant_config": {"mode": "full"},
                        "position_size_pct": 25,
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        cursor.execute(
            "DELETE FROM monitoring_metadata WHERE meta_key = ?",
            (self.repo.CONFIG_CLEANUP_MIGRATION_KEY,),
        )
        conn.commit()
        conn.close()

        repaired_repo = MonitoringRepository(str(self.base / "monitoring.db"))
        item = repaired_repo.get_item_by_symbol("600000", monitor_type="ai_task")

        self.assertIsNotNone(item)
        self.assertEqual(item["config"]["position_size_pct"], 25)
        self.assertNotIn("auto_trade", item["config"])
        self.assertNotIn("qmt_account_id", item["config"])
        self.assertNotIn("quant_enabled", item["config"])
        self.assertNotIn("quant_config", item["config"])
        cleanup_meta = json.loads(
            repaired_repo._get_metadata(repaired_repo.CONFIG_CLEANUP_MIGRATION_KEY)
        )
        self.assertEqual(cleanup_meta["changed"], 1)

    def test_repository_init_cleans_dirty_items_and_rewires_duplicate_history(self):
        seed_path = self.repo.db_path
        conn = sqlite3.connect(seed_path)
        cursor = conn.cursor()
        cursor.execute("DROP INDEX IF EXISTS idx_monitoring_asset_type")
        cursor.execute("DROP INDEX IF EXISTS idx_monitoring_ai_task_account_symbol")
        cursor.execute("DROP INDEX IF EXISTS idx_monitoring_managed_alert_position")
        cursor.execute("DELETE FROM monitoring_metadata WHERE meta_key = ?", (self.repo.DIRTY_DATA_CLEANUP_KEY,))
        cursor.execute("DELETE FROM monitoring_items")
        cursor.execute("DELETE FROM monitoring_events")
        cursor.execute("DELETE FROM monitoring_price_history")
        cursor.execute(
            """
            INSERT INTO monitoring_items (
                id, symbol, name, monitor_type, source, enabled, interval_minutes,
                trading_hours_only, notification_enabled, managed_by_portfolio,
                account_name, config_json, updated_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "600519",
                "贵州茅台",
                "ai_task",
                "ai_monitor",
                1,
                60,
                1,
                1,
                0,
                "默认账户",
                json.dumps({"position_size_pct": 20}, ensure_ascii=False),
                "2026-03-11 09:31:00",
                "2026-03-11 09:31:00",
            ),
        )
        cursor.execute(
            """
            INSERT INTO monitoring_items (
                id, symbol, name, monitor_type, source, enabled, interval_minutes,
                trading_hours_only, notification_enabled, managed_by_portfolio,
                account_name, last_status, last_message, config_json, updated_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                2,
                "600519",
                "",
                "ai_task",
                "",
                1,
                5,
                1,
                1,
                0,
                "",
                "buy",
                "duplicate_row",
                json.dumps({"notify_email": "ops@example.com"}, ensure_ascii=False),
                "2026-03-11 09:30:00",
                "2026-03-11 09:30:00",
            ),
        )
        cursor.execute(
            """
            INSERT INTO monitoring_items (
                id, symbol, name, monitor_type, source, config_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                3,
                "",
                "坏数据",
                "legacy",
                "manual",
                "{}",
            ),
        )
        cursor.execute(
            """
            INSERT INTO monitoring_events (
                monitoring_item_id, symbol, name, monitor_type, event_type, message, details_json, notification_pending, sent, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                2,
                "OLD",
                "旧任务",
                "ai_task",
                "buy",
                "duplicate_event",
                json.dumps({"source": "duplicate"}, ensure_ascii=False),
                1,
                0,
                "2026-03-11 09:30:30",
            ),
        )
        cursor.execute(
            """
            INSERT INTO monitoring_events (
                monitoring_item_id, symbol, name, monitor_type, event_type, message, details_json, notification_pending, sent, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None,
                "",
                None,
                None,
                "orphan",
                "invalid_blank_event",
                "{}",
                0,
                1,
                "2026-03-11 09:30:31",
            ),
        )
        cursor.execute(
            "INSERT INTO monitoring_price_history (monitoring_item_id, price, created_at) VALUES (?, ?, ?)",
            (2, 1510.0, "2026-03-11 09:30:32"),
        )
        conn.commit()
        conn.close()

        repaired_repo = MonitoringRepository(seed_path)
        items = repaired_repo.list_items(monitor_type="ai_task")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["symbol"], "600519")
        self.assertEqual(items[0]["account_name"], "默认账户")
        self.assertEqual(items[0]["config"]["position_size_pct"], 20)
        self.assertEqual(items[0]["config"]["notify_email"], "ops@example.com")

        events = repaired_repo.get_recent_events(limit=10)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["monitoring_item_id"], items[0]["id"])
        self.assertEqual(events[0]["symbol"], "600519")
        self.assertEqual(events[0]["name"], "贵州茅台")

        conn = repaired_repo._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS total FROM monitoring_price_history WHERE monitoring_item_id = ?", (items[0]["id"],))
        price_history_count = int(cursor.fetchone()["total"])
        cursor.execute("SELECT COUNT(*) AS total FROM monitoring_items WHERE TRIM(COALESCE(symbol, '')) = '' OR monitor_type NOT IN ('ai_task', 'price_alert')")
        invalid_count = int(cursor.fetchone()["total"])
        conn.close()

        self.assertEqual(price_history_count, 1)
        self.assertEqual(invalid_count, 0)
        cleanup_meta = json.loads(repaired_repo._get_metadata(repaired_repo.DIRTY_DATA_CLEANUP_KEY))
        self.assertEqual(cleanup_meta["deduplicated_items"], 1)
        self.assertEqual(cleanup_meta["invalid_events"], 1)

    def test_due_items_respect_runtime_and_service_switch(self):
        item_id = self.repo.create_item(
            {
                "symbol": "300750",
                "name": "宁德时代",
                "monitor_type": "price_alert",
                "interval_minutes": 3,
                "config": {"entry_range": {"min": 200, "max": 210}},
            }
        )
        now = datetime(2026, 3, 8, 10, 0, 0)

        self.assertEqual(self.repo.get_due_items(now=now, service_running=False), [])

        due_items = self.repo.get_due_items(now=now, service_running=True)
        self.assertEqual([item["id"] for item in due_items], [item_id])

        self.repo.update_runtime(
            item_id,
            last_checked=now.strftime("%Y-%m-%d %H:%M:%S"),
            last_status="checked",
        )
        self.assertEqual(self.repo.get_due_items(now=now + timedelta(minutes=2), service_running=True), [])

        due_later = self.repo.get_due_items(now=now + timedelta(minutes=4), service_running=True)
        self.assertEqual([item["id"] for item in due_later], [item_id])

    def test_mark_notification_read_preserves_delivery_state_and_records_timestamp(self):
        item_id = self.repo.create_item(
            {
                "symbol": "600519",
                "name": "贵州茅台",
                "monitor_type": "ai_task",
                "account_name": "主账户",
            }
        )
        event_id = self.repo.record_event(
            item_id=item_id,
            event_type="sell",
            message="触发最新卖出信号",
            notification_pending=True,
            sent=False,
            details={"source": "unit-test"},
            created_at="2026-03-12 10:30:00",
        )

        notifications = self.repo.get_all_recent_notifications(limit=10)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0]["id"], event_id)
        self.assertEqual(notifications[0]["event_type"], "sell")
        self.assertEqual(notifications[0]["type"], "sell")
        self.assertFalse(notifications[0]["sent"])
        self.assertFalse(notifications[0]["is_read"])
        self.assertIsNone(notifications[0]["read_at"])

        self.repo.mark_notification_read(event_id)

        latest = self.repo.get_all_recent_notifications(limit=10)[0]
        self.assertEqual(latest["id"], event_id)
        self.assertTrue(latest["is_read"])
        self.assertTrue(bool(latest["read_at"]))
        self.assertFalse(latest["sent"])

    def test_price_alert_upsert_reuses_existing_asset_record_when_managed_state_changes(self):
        first_id = self.repo.create_item(
            {
                "symbol": "601318",
                "name": "中国平安",
                "monitor_type": "price_alert",
                "managed_by_portfolio": True,
                "account_name": "默认账户",
                "asset_id": 88,
                "portfolio_stock_id": 88,
                "config": {"take_profit": 55.0},
            }
        )

        second_id = self.repo.upsert_item(
            {
                "symbol": "601318",
                "name": "中国平安",
                "monitor_type": "price_alert",
                "managed_by_portfolio": False,
                "account_name": "默认账户",
                "asset_id": 88,
                "portfolio_stock_id": None,
                "config": {"stop_loss": 46.0},
            }
        )

        self.assertEqual(first_id, second_id)
        items = self.repo.list_items(monitor_type="price_alert", asset_id=88)
        self.assertEqual(len(items), 1)
        self.assertFalse(items[0]["managed_by_portfolio"])
        self.assertIsNone(items[0]["portfolio_stock_id"])
        self.assertEqual(items[0]["config"]["take_profit"], 55.0)
        self.assertEqual(items[0]["config"]["stop_loss"], 46.0)

    def test_repository_init_repairs_managed_price_alert_after_asset_leaves_portfolio(self):
        seed_path = str(self.base / "monitoring.db")
        asset_repo = AssetRepository(seed_path)
        asset_id = asset_repo.create_or_update_research_asset(
            symbol="300179",
            name="四方达",
            account_name="zfy",
        )
        asset_repo.update_asset(
            asset_id,
            status=STATUS_PORTFOLIO,
            cost_price=24.176,
            quantity=200,
        )

        self.repo.create_item(
            {
                "symbol": "300179",
                "name": "四方达",
                "monitor_type": "price_alert",
                "source": "portfolio",
                "managed_by_portfolio": True,
                "account_name": "zfy",
                "asset_id": asset_id,
                "portfolio_stock_id": asset_id,
                "config": {"take_profit": 24.8, "stop_loss": 23.0},
            }
        )
        asset_repo.transition_asset_status(asset_id, STATUS_WATCHLIST)

        repaired_repo = MonitoringRepository(seed_path)
        item = repaired_repo.list_items(monitor_type="price_alert", asset_id=asset_id)[0]
        self.assertFalse(item["managed_by_portfolio"])
        self.assertIsNone(item["portfolio_stock_id"])
        self.assertEqual(item["source"], "manual")

    def test_legacy_migration_moves_smart_and_stock_data(self):
        legacy_smart_db = self.base / "legacy_smart.db"
        legacy_stock_db = self.base / "legacy_stock.db"

        self._create_legacy_smart_db(legacy_smart_db)
        self._create_legacy_stock_db(legacy_stock_db)

        migrated_smart = self.repo.migrate_legacy_smart_db(str(legacy_smart_db))
        migrated_stock = self.repo.migrate_legacy_stock_db(str(legacy_stock_db))

        self.assertEqual(migrated_smart, 1)
        self.assertEqual(migrated_stock, 3)

        ai_task = self.repo.get_item_by_symbol("600519", monitor_type="ai_task")
        self.assertIsNotNone(ai_task)
        self.assertEqual(ai_task["interval_minutes"], 2)
        self.assertTrue(ai_task["config"]["has_position"])
        self.assertNotIn("auto_trade", ai_task["config"])
        self.assertNotIn("qmt_account_id", ai_task["config"])
        self.assertFalse(ai_task["enabled"])
        self.assertEqual(ai_task["source"], "legacy_conflict")

        price_alerts = self.repo.list_items(monitor_type="price_alert", symbol="000001")
        self.assertEqual(len(price_alerts), 3)
        managed_items = [item for item in price_alerts if item["managed_by_portfolio"]]
        manual_items = [item for item in price_alerts if not item["managed_by_portfolio"]]
        self.assertEqual(len(managed_items), 1)
        self.assertEqual(len(manual_items), 2)
        self.assertFalse(managed_items[0]["enabled"])
        for item in price_alerts:
            self.assertNotIn("quant_enabled", item["config"])
            self.assertNotIn("quant_config", item["config"])

        conn = self.repo._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS total FROM migration_conflicts")
        conflict_count = int(cursor.fetchone()["total"])
        conn.close()
        self.assertEqual(conflict_count, 2)

    def _create_legacy_smart_db(self, db_path: Path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE monitor_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT,
                stock_code TEXT,
                stock_name TEXT,
                enabled INTEGER,
                check_interval INTEGER,
                auto_trade INTEGER,
                trading_hours_only INTEGER,
                position_size_pct REAL,
                stop_loss_pct REAL,
                take_profit_pct REAL,
                qmt_account_id TEXT,
                notify_email TEXT,
                notify_webhook TEXT,
                has_position INTEGER,
                position_cost REAL,
                position_quantity INTEGER,
                position_date TEXT,
                managed_by_portfolio INTEGER,
                updated_at TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO monitor_tasks (
                task_name, stock_code, stock_name, enabled, check_interval,
                auto_trade, trading_hours_only, position_size_pct, stop_loss_pct,
                take_profit_pct, has_position, position_cost, position_quantity,
                managed_by_portfolio, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "茅台主任务",
                "600519",
                "贵州茅台",
                1,
                90,
                1,
                1,
                20,
                5,
                10,
                1,
                1510.0,
                100,
                0,
                "2026-03-08 09:30:00",
            ),
        )
        conn.commit()
        conn.close()

    def _create_legacy_stock_db(self, db_path: Path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE monitored_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                name TEXT,
                rating TEXT,
                entry_range TEXT,
                take_profit REAL,
                stop_loss REAL,
                current_price REAL,
                check_interval INTEGER,
                notification_enabled INTEGER,
                quant_enabled INTEGER,
                quant_config TEXT,
                trading_hours_only INTEGER,
                managed_by_portfolio INTEGER,
                last_checked TEXT
            )
            """
        )

        rows = [
            (
                "000001",
                "平安银行",
                "买入",
                '{"min": 10.0, "max": 10.5}',
                11.2,
                9.6,
                10.2,
                30,
                1,
                0,
                "{}",
                1,
                0,
                "2026-03-08 09:31:00",
            ),
            (
                "000001",
                "平安银行",
                "持有",
                '{"min": 9.8, "max": 10.2}',
                11.0,
                9.4,
                10.1,
                60,
                1,
                0,
                "{}",
                1,
                0,
                "2026-03-08 09:32:00",
            ),
            (
                "000001",
                "平安银行",
                "买入",
                '{"min": 10.1, "max": 10.6}',
                11.3,
                9.7,
                10.25,
                45,
                1,
                0,
                "{}",
                1,
                1,
                "2026-03-08 09:33:00",
            ),
        ]
        cursor.executemany(
            """
            INSERT INTO monitored_stocks (
                symbol, name, rating, entry_range, take_profit, stop_loss,
                current_price, check_interval, notification_enabled, quant_enabled,
                quant_config, trading_hours_only, managed_by_portfolio, last_checked
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
