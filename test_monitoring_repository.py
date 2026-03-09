import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

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
        self.assertTrue(items[0]["config"]["auto_trade"])
        self.assertEqual(items[0]["config"]["position_size_pct"], 20)
        self.assertEqual(items[0]["config"]["stop_loss_pct"], 6)

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
        self.assertTrue(ai_task["config"]["auto_trade"])
        self.assertTrue(ai_task["config"]["has_position"])
        self.assertFalse(ai_task["enabled"])
        self.assertEqual(ai_task["source"], "legacy_conflict")

        price_alerts = self.repo.list_items(monitor_type="price_alert", symbol="000001")
        self.assertEqual(len(price_alerts), 3)
        managed_items = [item for item in price_alerts if item["managed_by_portfolio"]]
        manual_items = [item for item in price_alerts if not item["managed_by_portfolio"]]
        self.assertEqual(len(managed_items), 1)
        self.assertEqual(len(manual_items), 2)
        self.assertFalse(managed_items[0]["enabled"])

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
