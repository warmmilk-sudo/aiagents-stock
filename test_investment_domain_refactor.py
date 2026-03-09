import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from analysis_repository import AnalysisRepository
from investment_action_utils import build_analysis_action_payload
from investment_lifecycle_service import InvestmentLifecycleService
from monitor_db import StockMonitorDatabase
from monitoring_repository import MonitoringRepository
from portfolio_db import PortfolioDB
from portfolio_manager import PortfolioManager
from smart_monitor_db import SmartMonitorDB


class InvestmentDomainRefactorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_canonical_migration_is_idempotent_across_legacy_sources(self):
        analysis_db = self.base / "stock_analysis.db"
        portfolio_db_path = self.base / "portfolio_stocks.db"
        smart_db = self.base / "smart_monitor.db"
        monitor_db_path = self.base / "monitoring.db"
        canonical_db = self.base / "investment.db"

        self._create_legacy_analysis_db(analysis_db)
        self._create_legacy_portfolio_db(portfolio_db_path)
        self._create_legacy_smart_db(smart_db)
        self._create_legacy_monitor_db(monitor_db_path)

        AnalysisRepository(str(analysis_db))
        PortfolioDB(str(portfolio_db_path))
        SmartMonitorDB(str(smart_db))
        StockMonitorDatabase(str(monitor_db_path))

        self.assertEqual(self._count_rows(canonical_db, "analysis_records"), 2)
        self.assertEqual(self._count_rows(canonical_db, "portfolio_stocks"), 1)
        self.assertEqual(self._count_rows(canonical_db, "monitoring_items"), 2)

        AnalysisRepository(str(analysis_db))
        PortfolioDB(str(portfolio_db_path))
        SmartMonitorDB(str(smart_db))
        StockMonitorDatabase(str(monitor_db_path))

        self.assertEqual(self._count_rows(canonical_db, "analysis_records"), 2)
        self.assertEqual(self._count_rows(canonical_db, "portfolio_stocks"), 1)
        self.assertEqual(self._count_rows(canonical_db, "monitoring_items"), 2)

    def test_multi_account_ai_tasks_are_isolated(self):
        db = SmartMonitorDB(str(self.base / "smart_monitor.db"))

        first_id = db.upsert_monitor_task(
            {
                "task_name": "茅台-A",
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "enabled": 1,
                "account_name": "账户A",
            }
        )
        second_id = db.upsert_monitor_task(
            {
                "task_name": "茅台-B",
                "stock_code": "600519",
                "stock_name": "贵州茅台",
                "enabled": 1,
                "account_name": "账户B",
            }
        )

        self.assertNotEqual(first_id, second_id)
        tasks = db.get_monitor_tasks(enabled_only=False)
        self.assertEqual(len(tasks), 2)
        self.assertEqual(
            {
                (task["account_name"], task["stock_code"])
                for task in tasks
            },
            {("账户A", "600519"), ("账户B", "600519")},
        )

    def test_strategy_context_projection_uses_latest_analysis(self):
        portfolio_db = PortfolioDB(str(self.base / "portfolio.db"))
        realtime_monitor_db = StockMonitorDatabase(str(self.base / "monitor.db"))
        smart_monitor_db = SmartMonitorDB(str(self.base / "smart.db"))
        manager = PortfolioManager(
            portfolio_store=portfolio_db,
            realtime_monitor_store=realtime_monitor_db,
            smart_monitor_store=smart_monitor_db,
        )
        manager._resolve_stock_name = lambda code: f"Stock{code}"

        success, message, stock_id = manager.add_stock(
            code="300750",
            name=None,
            cost_price=150.0,
            quantity=100,
            note="test",
            auto_monitor=True,
        )
        self.assertTrue(success, message)

        latest_id = portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="买入",
            confidence=8.8,
            current_price=152.0,
            entry_min=148.0,
            entry_max=150.0,
            take_profit=168.0,
            stop_loss=142.0,
            summary="最新分析",
            analysis_time=datetime(2026, 3, 9, 15, 0, 0),
            has_full_report=True,
        )
        portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="卖出",
            confidence=4.2,
            current_price=140.0,
            entry_min=139.0,
            entry_max=141.0,
            take_profit=145.0,
            stop_loss=136.0,
            summary="旧分析",
            analysis_time=datetime(2026, 3, 8, 15, 0, 0),
            has_full_report=True,
        )

        task = smart_monitor_db.get_monitor_task_by_code(
            "300750",
            managed_only=True,
            account_name="默认账户",
        )
        alert = realtime_monitor_db.get_monitor_by_code(
            "300750",
            managed_only=True,
            account_name="默认账户",
        )

        self.assertIsNotNone(task)
        self.assertIsNotNone(alert)
        self.assertEqual(task["strategy_context"]["origin_analysis_id"], latest_id)
        self.assertEqual(task["strategy_context"]["rating"], "买入")
        self.assertEqual(task["strategy_context"]["entry_min"], 148.0)
        self.assertEqual(alert["take_profit"], 168.0)
        self.assertEqual(alert["stop_loss"], 142.0)

    def test_apply_monitor_execution_closes_position_and_cleans_managed_items(self):
        portfolio_db = PortfolioDB(str(self.base / "portfolio.db"))
        realtime_monitor_db = StockMonitorDatabase(str(self.base / "monitor.db"))
        lifecycle = InvestmentLifecycleService(
            portfolio_store=portfolio_db,
            realtime_monitor_store=realtime_monitor_db,
            analysis_store=portfolio_db.analysis_repository,
            monitoring_store=realtime_monitor_db.repository,
        )

        buy_result = lifecycle.apply_monitor_execution(
            stock_code="002594",
            stock_name="比亚迪",
            trade_type="buy",
            quantity=200,
            price=220.0,
            account_name="策略账户",
            note="AI买入",
        )
        self.assertTrue(buy_result["success"])

        stock = portfolio_db.get_stock_by_code("002594", "策略账户")
        self.assertIsNotNone(stock)
        self.assertEqual(stock["quantity"], 200)
        self.assertEqual(stock["position_status"], "active")

        portfolio_db.save_analysis(
            stock_id=stock["id"],
            rating="持有",
            confidence=7.5,
            current_price=222.0,
            entry_min=218.0,
            entry_max=221.0,
            take_profit=238.0,
            stop_loss=210.0,
            summary="托管策略",
            has_full_report=True,
        )
        self.assertIsNotNone(
            realtime_monitor_db.get_monitor_by_code(
                "002594",
                managed_only=True,
                account_name="策略账户",
            )
        )

        sell_result = lifecycle.apply_monitor_execution(
            stock_code="002594",
            stock_name="比亚迪",
            trade_type="sell",
            quantity=200,
            price=230.0,
            account_name="策略账户",
            portfolio_stock_id=stock["id"],
            note="AI卖出",
        )
        self.assertTrue(sell_result["success"])

        closed_stock = portfolio_db.get_stock(stock["id"])
        self.assertEqual(closed_stock["position_status"], "closed")
        self.assertIsNone(closed_stock["quantity"])
        self.assertIsNone(
            realtime_monitor_db.get_monitor_by_code(
                "002594",
                managed_only=True,
                account_name="策略账户",
            )
        )
        trade_history = portfolio_db.get_trade_history(stock["id"], limit=10)
        self.assertEqual(len(trade_history), 2)

    def test_conflict_migration_disables_ambiguous_ai_task(self):
        canonical_portfolio = PortfolioDB(str(self.base / "portfolio.db"))
        legacy_smart_db = self.base / "smart_monitor.db"
        repo = MonitoringRepository(str(self.base / "monitoring.db"))

        canonical_portfolio.add_stock("600519", "贵州茅台", 1500.0, 100, "", True, "账户A")
        canonical_portfolio.add_stock("600519", "贵州茅台", 1510.0, 100, "", True, "账户B")
        self._create_legacy_smart_db(legacy_smart_db, managed_by_portfolio=1, has_position=1)

        migrated = repo.migrate_legacy_smart_db(str(legacy_smart_db))
        self.assertEqual(migrated, 1)

        task = repo.get_item_by_symbol(
            "600519",
            monitor_type="ai_task",
            account_name="默认账户",
        )
        self.assertIsNotNone(task)
        self.assertFalse(task["enabled"])
        self.assertEqual(task["source"], "legacy_conflict")
        self.assertEqual(self._count_rows(self.base / "investment.db", "migration_conflicts"), 1)

    def test_analysis_action_payload_parses_strategy_for_prefill(self):
        payload = build_analysis_action_payload(
            symbol="600519",
            stock_name="贵州茅台",
            final_decision={
                "rating": "买入",
                "entry_range": "1480.0 - 1510.0",
                "take_profit": "1650元",
                "stop_loss": "1430元",
                "operation_advice": "等待回踩分批吸纳",
            },
            origin_analysis_id=99,
        )

        self.assertEqual(payload["symbol"], "600519")
        self.assertEqual(payload["origin_analysis_id"], 99)
        self.assertEqual(payload["strategy_context"]["entry_min"], 1480.0)
        self.assertEqual(payload["strategy_context"]["entry_max"], 1510.0)
        self.assertEqual(payload["strategy_context"]["take_profit"], 1650.0)
        self.assertEqual(payload["strategy_context"]["stop_loss"], 1430.0)
        self.assertEqual(payload["default_cost_price"], 1495.0)
        self.assertIn("等待回踩分批吸纳", payload["default_note"])

    def _count_rows(self, db_path: Path, table: str) -> int:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total = int(cursor.fetchone()[0])
        conn.close()
        return total

    def _create_legacy_analysis_db(self, db_path: Path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE analysis_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                stock_name TEXT,
                analysis_date TEXT,
                period TEXT,
                stock_info TEXT,
                agents_results TEXT,
                discussion_result TEXT,
                final_decision TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO analysis_records (
                symbol, stock_name, analysis_date, period,
                stock_info, agents_results, discussion_result, final_decision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "600519",
                "贵州茅台",
                "2026-03-08 10:00:00",
                "1y",
                '{"symbol":"600519","name":"贵州茅台","current_price":1500.0}',
                "{}",
                '"研究记录"',
                '{"rating":"买入","entry_range":"1480-1510","take_profit":"1650元","stop_loss":"1430元"}',
            ),
        )
        conn.commit()
        conn.close()

    def _create_legacy_portfolio_db(self, db_path: Path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE portfolio_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_name TEXT,
                code TEXT,
                name TEXT,
                cost_price REAL,
                quantity INTEGER,
                note TEXT,
                auto_monitor INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE portfolio_analysis_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_stock_id INTEGER,
                analysis_time TEXT,
                rating TEXT,
                confidence REAL,
                current_price REAL,
                target_price REAL,
                entry_min REAL,
                entry_max REAL,
                take_profit REAL,
                stop_loss REAL,
                summary TEXT,
                stock_info_json TEXT,
                agents_results_json TEXT,
                discussion_result TEXT,
                final_decision_json TEXT,
                analysis_period TEXT,
                analysis_source TEXT,
                has_full_report INTEGER
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO portfolio_stocks (
                id, account_name, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "默认账户",
                "300750",
                "宁德时代",
                150.0,
                100,
                "legacy",
                1,
                "2026-03-08 09:00:00",
                "2026-03-08 09:00:00",
            ),
        )
        cursor.execute(
            """
            INSERT INTO portfolio_analysis_history (
                portfolio_stock_id, analysis_time, rating, confidence, current_price, target_price,
                entry_min, entry_max, take_profit, stop_loss, summary, stock_info_json,
                agents_results_json, discussion_result, final_decision_json, analysis_period,
                analysis_source, has_full_report
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "2026-03-08 11:00:00",
                "持有",
                7.0,
                152.0,
                168.0,
                148.0,
                150.0,
                168.0,
                142.0,
                "组合分析",
                '{"symbol":"300750","name":"宁德时代"}',
                "{}",
                '"组合分析"',
                '{"rating":"持有"}',
                "1y",
                "legacy_portfolio_analysis",
                1,
            ),
        )
        conn.commit()
        conn.close()

    def _create_legacy_smart_db(self, db_path: Path, managed_by_portfolio: int = 0, has_position: int = 0):
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
                has_position INTEGER,
                position_cost REAL,
                position_quantity INTEGER,
                managed_by_portfolio INTEGER,
                updated_at TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO monitor_tasks (
                task_name, stock_code, stock_name, enabled, check_interval, auto_trade,
                trading_hours_only, position_size_pct, stop_loss_pct, take_profit_pct,
                has_position, position_cost, position_quantity, managed_by_portfolio, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "AI任务",
                "600519",
                "贵州茅台",
                1,
                120,
                0,
                1,
                20,
                5,
                10,
                has_position,
                1500.0,
                100,
                managed_by_portfolio,
                "2026-03-08 09:30:00",
            ),
        )
        conn.commit()
        conn.close()

    def _create_legacy_monitor_db(self, db_path: Path):
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
        cursor.execute(
            """
            INSERT INTO monitored_stocks (
                symbol, name, rating, entry_range, take_profit, stop_loss, current_price,
                check_interval, notification_enabled, quant_enabled, quant_config,
                trading_hours_only, managed_by_portfolio, last_checked
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
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
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    unittest.main()
