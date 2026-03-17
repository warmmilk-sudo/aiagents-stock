import json
import sys
import tempfile
import time
import unittest
from datetime import date, datetime
from unittest.mock import patch

import pandas as pd

from monitor_db import StockMonitorDatabase
from portfolio_analysis_tasks import PortfolioAnalysisTaskManager
from portfolio_db import PortfolioDB
from portfolio_manager import PortfolioManager
from portfolio_scheduler import PortfolioAnalysisTaskConfig, PortfolioScheduler, schedule as portfolio_schedule
from smart_monitor_db import SmartMonitorDB


def build_price_series(base_price: float, returns: list[float], start: str) -> pd.Series:
    index = pd.bdate_range(start=start, periods=len(returns))
    values = [base_price]
    for daily_return in returns[1:]:
        values.append(values[-1] * (1 + daily_return))
    return pd.Series(values, index=index)


class PortfolioAnalyticsTests(unittest.TestCase):
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

        benchmark_returns = [
            0.0,
            0.010,
            -0.006,
            0.012,
            -0.004,
            0.009,
            0.005,
            -0.007,
            0.011,
            -0.003,
        ] * 5
        stock_returns = [ret * 1.2 + 0.001 for ret in benchmark_returns]
        self.price_series = {
            "000001": build_price_series(10.0, stock_returns, "2026-01-02"),
            "510300": build_price_series(100.0, benchmark_returns, "2026-01-02"),
        }
        self.benchmark_series = self.price_series["510300"]
        self.manager._fetch_price_series = (
            lambda symbol, start_date=None, end_date=None: self._slice_series(
                self.price_series.get(symbol, pd.Series(dtype=float)),
                start_date,
                end_date,
            )
        )
        self.manager._fetch_benchmark_price_series = (
            lambda start_date=None, end_date=None: (
                self._slice_series(self.benchmark_series, start_date, end_date),
                "沪深300",
            )
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _slice_series(self, series: pd.Series, start_date=None, end_date=None) -> pd.Series:
        result = series.copy()
        if start_date:
            result = result[result.index >= pd.Timestamp(start_date)]
        if end_date:
            result = result[result.index <= pd.Timestamp(end_date)]
        return result

    def _add_stock(
        self,
        code: str = "000001",
        cost_price: float = 10.0,
        quantity: int = 100,
        account_name: str = "zfy",
    ) -> int:
        success, msg, stock_id = self.manager.add_stock(
            code=code,
            name=None,
            cost_price=cost_price,
            quantity=quantity,
            note="analytics-test",
            auto_monitor=True,
            account_name=account_name,
        )
        self.assertTrue(success, msg)
        self.assertIsNotNone(stock_id)
        return stock_id

    def _reset_stock_info_migration_metadata(self):
        conn = self.portfolio_db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM investment_metadata WHERE meta_key = ?",
            ("migrated_stock_info_schema::industry_v2",),
        )
        conn.commit()
        conn.close()

    def test_calculate_portfolio_risk_includes_quant_metrics_and_settings(self):
        self._add_stock()
        self.manager.set_risk_free_rate_annual(0.02)

        result = self.manager.calculate_portfolio_risk(account_name="默认账户")

        self.assertEqual(result["status"], "success")
        self.assertIsNotNone(result["annual_volatility"])
        self.assertIsNotNone(result["beta_hs300"])
        self.assertIsNotNone(result["sharpe_ratio"])
        self.assertAlmostEqual(result["risk_free_rate_annual"], 0.02)
        self.assertGreater(result["data_coverage"]["available_days"], 20)
        self.assertEqual(result["benchmark_label"], "沪深300")

    def test_calculate_portfolio_risk_uses_account_total_assets_settings(self):
        self._add_stock(cost_price=10.0, quantity=100, account_name="ly")
        self.manager.set_account_total_assets_settings({"ly": 5000})

        result = self.manager.calculate_portfolio_risk(account_name="ly")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["total_assets"], 5000)
        self.assertEqual(result["available_cash"], 4000)
        self.assertAlmostEqual(result["position_usage_pct"], 0.2)
        self.assertAlmostEqual(result["stock_distribution"][0]["asset_weight"], 0.2)
        self.assertTrue(result["total_assets_configured"])

    def test_save_analysis_normalizes_legacy_industry_alias(self):
        stock_id = self._add_stock()
        repo = self.portfolio_db.analysis_repository
        repo._lookup_basic_info_industry = lambda symbol, industry_cache=None: ""

        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="持有",
            confidence=7.0,
            current_price=10.5,
            summary="alias normalization",
            stock_info={"current_price": 10.5, "所属同花顺行业": "人工智能"},
        )

        latest = self.portfolio_db.get_latest_analysis(stock_id)
        self.assertEqual(latest["stock_info"]["industry"], "人工智能")
        self.assertNotIn("所属同花顺行业", latest["stock_info"])

    def test_stock_info_schema_migration_canonicalizes_legacy_industry_key(self):
        stock_id = self._add_stock()
        repo = self.portfolio_db.analysis_repository
        repo._lookup_basic_info_industry = lambda symbol, industry_cache=None: ""

        analysis_id = self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="持有",
            confidence=7.0,
            current_price=10.5,
            summary="legacy migration",
            stock_info={"current_price": 10.5, "industry": "未知"},
        )

        conn = self.portfolio_db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE analysis_records SET stock_info_json = ? WHERE id = ?",
            (json.dumps({"current_price": 10.5, "所属同花顺行业": "机器人"}, ensure_ascii=False), analysis_id),
        )
        conn.commit()
        conn.close()

        self._reset_stock_info_migration_metadata()
        updated = repo.migrate_stock_info_schema()

        latest = self.portfolio_db.get_latest_analysis(stock_id)
        self.assertEqual(updated, 1)
        self.assertEqual(latest["stock_info"]["industry"], "机器人")
        self.assertNotIn("所属同花顺行业", latest["stock_info"])

    def test_calculate_portfolio_risk_backfills_unknown_industry_from_basic_info(self):
        stock_id = self._add_stock()
        repo = self.portfolio_db.analysis_repository
        repo._lookup_basic_info_industry = lambda symbol, industry_cache=None: ""

        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="持有",
            confidence=7.0,
            current_price=10.5,
            summary="unknown industry",
            stock_info={"current_price": 10.5, "industry": "未知"},
        )

        self.manager._get_basic_stock_info = lambda code: {"industry": "银行"}
        result = self.manager.calculate_portfolio_risk(account_name="默认账户")

        self.assertEqual(result["industry_distribution"][0]["industry"], "银行")

    def test_calculate_portfolio_risk_uses_stock_info_price_when_new_position_has_no_analysis(self):
        self._add_stock(cost_price=10.0, quantity=100, account_name="zfy")
        self.manager._get_realtime_quote = lambda code: {}
        self.manager._get_basic_stock_info = lambda code: {}

        class FakeStockDataFetcher:
            def get_stock_info(self, symbol, **kwargs):
                return {"current_price": 12.34, "industry": "半导体"}

        self.manager._stock_data_fetcher = FakeStockDataFetcher()

        result = self.manager.calculate_portfolio_risk(account_name="zfy")

        self.assertEqual(result["status"], "success")
        self.assertAlmostEqual(result["total_market_value"], 1234.0)
        self.assertAlmostEqual(result["stock_distribution"][0]["market_value"], 1234.0)
        self.assertEqual(result["industry_distribution"][0]["industry"], "半导体")

    def test_return_series_prefers_actual_snapshots_and_calendar_aggregates_daily_changes(self):
        self._add_stock()
        snapshot_date = "2026-01-05"
        self.portfolio_db.upsert_daily_snapshot(
            account_name="默认账户",
            snapshot_date=snapshot_date,
            total_market_value=1500.0,
            total_cost_value=1000.0,
            total_pnl=500.0,
            holdings=[],
            data_source="manual",
        )
        self.portfolio_db.upsert_daily_snapshot(
            account_name="全部账户",
            snapshot_date=snapshot_date,
            total_market_value=1500.0,
            total_cost_value=1000.0,
            total_pnl=500.0,
            holdings=[],
            data_source="manual",
        )

        series = self.manager.build_portfolio_return_series(
            account_name="默认账户",
            start_date="2026-01-02",
            end_date="2026-01-09",
        )

        self.assertEqual(series.loc[pd.Timestamp(snapshot_date), "source"], "actual")
        self.assertEqual(series.loc[pd.Timestamp(snapshot_date), "total_market_value"], 1500.0)

        calendar_result = self.manager.build_pnl_calendar(
            account_name="默认账户",
            view="monthly",
            start_date="2026-01-02",
            end_date="2026-01-31",
        )
        self.assertEqual(calendar_result["status"], "success")
        self.assertTrue(calendar_result["records"])
        self.assertEqual(calendar_result["records"][0]["month_label"], "2026-01")

    def test_seed_initial_trade_creates_opening_record_and_summary(self):
        stock_id = self._add_stock()

        success, msg = self.manager.seed_initial_trade(
            stock_id,
            trade_date="2026-01-06",
            note="initial position",
        )

        self.assertTrue(success, msg)
        history = self.manager.get_trade_history(stock_id, limit=5)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["trade_type"], "buy")
        self.assertEqual(history[0]["trade_date"], "2026-01-06")
        self.assertEqual(history[0]["quantity"], 100)
        summary = self.manager.get_trade_summary_map([stock_id])[stock_id]
        self.assertEqual(summary["trade_count"], 1)
        self.assertEqual(summary["first_buy_date"], "2026-01-06")

    def test_record_trade_buy_updates_weighted_cost_and_snapshot(self):
        stock_id = self._add_stock(cost_price=10.0, quantity=100)

        success, msg, updated_stock = self.manager.record_trade(
            stock_id=stock_id,
            trade_type="buy",
            quantity=100,
            price=12.0,
            trade_date="2026-01-07",
            note="add position",
        )

        self.assertTrue(success, msg)
        self.assertIsNotNone(updated_stock)
        self.assertEqual(updated_stock["quantity"], 200)
        self.assertAlmostEqual(updated_stock["cost_price"], 11.0)

        history = self.manager.get_trade_history(stock_id, limit=5)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["trade_type"], "buy")
        self.assertEqual(history[0]["price"], 12.0)

        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        self.assertTrue(
            self.portfolio_db.has_snapshot_for_date(updated_stock["account_name"], snapshot_date)
        )

    def test_record_trade_sell_recalculates_remaining_cost_and_rejects_invalid_quantity(self):
        stock_id = self._add_stock(cost_price=10.0, quantity=200)

        success, msg, updated_stock = self.manager.record_trade(
            stock_id=stock_id,
            trade_type="sell",
            quantity=100,
            price=13.0,
            trade_date="2026-01-08",
            note="trim position",
        )

        self.assertTrue(success, msg)
        self.assertEqual(updated_stock["quantity"], 100)
        self.assertAlmostEqual(updated_stock["cost_price"], 7.0)

        failed_lot, lot_error_msg, _ = self.manager.record_trade(
            stock_id=stock_id,
            trade_type="sell",
            quantity=50,
            price=13.0,
            trade_date="2026-01-09",
            note="invalid lot",
        )
        self.assertFalse(failed_lot)
        self.assertIn("100的整数倍", lot_error_msg)

        failed, error_msg, _ = self.manager.record_trade(
            stock_id=stock_id,
            trade_type="sell",
            quantity=999,
            price=13.0,
            trade_date="2026-01-10",
            note="invalid oversell",
        )
        self.assertFalse(failed)
        self.assertIn("减仓数量", error_msg)

    def test_apply_trade_corrections_replaces_history_and_rebuilds_position(self):
        stock_id = self._add_stock(cost_price=9.0, quantity=100)
        self.manager.seed_initial_trade(stock_id, trade_date="2026-01-01", note="legacy")
        self.manager.record_trade(
            stock_id=stock_id,
            trade_type="buy",
            quantity=100,
            price=10.0,
            trade_date="2026-01-02",
            note="legacy add",
        )

        summary = self.manager.apply_trade_corrections(
            [
                {
                    "stock_id": stock_id,
                    "trades": [
                        {"trade_date": "2026-02-03", "trade_type": "buy", "price": 10.0, "quantity": 300},
                        {"trade_date": "2026-02-04", "trade_type": "sell", "price": 13.0, "quantity": 100},
                    ],
                }
            ],
            capture_snapshot=False,
            sync_integrations=False,
        )

        self.assertEqual(summary["succeeded"], 1)
        self.assertEqual(summary["failed"], 0)

        updated = self.manager.get_stock(stock_id)
        self.assertIsNotNone(updated)
        self.assertEqual(updated["quantity"], 200)
        self.assertAlmostEqual(updated["cost_price"], 8.5)
        self.assertEqual(updated["position_status"], "active")

        history = self.manager.get_trade_history(stock_id, limit=10)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["trade_type"], "sell")
        self.assertEqual(history[0]["trade_date"], "2026-02-04")
        self.assertEqual(history[0]["quantity"], 100)

    def test_apply_trade_corrections_supports_clear_and_watchlist_fallback(self):
        stock_id = self._add_stock(cost_price=12.0, quantity=200)

        summary = self.manager.apply_trade_corrections(
            [
                {
                    "stock_id": stock_id,
                    "status_when_flat": "watchlist",
                    "trades": [
                        {"trade_date": "2026-03-01", "trade_type": "buy", "price": 12.0, "quantity": 200},
                        {"trade_date": "2026-03-02", "trade_type": "clear", "price": 12.8},
                    ],
                }
            ],
            capture_snapshot=False,
            sync_integrations=False,
        )

        self.assertEqual(summary["succeeded"], 1)
        result = summary["results"][0]["replace_result"]
        self.assertEqual(result["final_status"], "watchlist")
        self.assertEqual(result["final_quantity"], 0)

        updated = self.manager.get_stock(stock_id)
        self.assertIsNotNone(updated)
        self.assertEqual(updated["position_status"], "watchlist")
        self.assertIsNone(updated.get("quantity"))
        self.assertIsNone(updated.get("cost_price"))

        history = self.manager.get_trade_history(stock_id, limit=10)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["trade_type"], "sell")
        self.assertEqual(history[0]["quantity"], 200)


class PortfolioSchedulerConfigTests(unittest.TestCase):
    def tearDown(self):
        portfolio_schedule.clear("portfolio_analysis")

    def test_scheduler_exposes_shared_task_config(self):
        scheduler = PortfolioScheduler()
        config = PortfolioAnalysisTaskConfig(
            analysis_mode="parallel",
            max_workers=4,
            auto_monitor_sync=False,
            notification_enabled=False,
            selected_agents=["technical", "risk"],
        )

        scheduler.set_task_config(config)
        status = scheduler.get_status()
        current = scheduler.get_task_config()

        self.assertEqual(current.analysis_mode, "parallel")
        self.assertEqual(current.max_workers, 4)
        self.assertFalse(current.auto_monitor_sync)
        self.assertFalse(current.notification_enabled)
        self.assertEqual(current.selected_agents, ["technical", "risk"])
        self.assertEqual(status["selected_agents"], ["technical", "risk"])

    def test_scheduler_enqueues_background_task_and_persists_each_result_immediately(self):
        scheduler = PortfolioScheduler()
        scheduler.set_task_config(
            PortfolioAnalysisTaskConfig(
                analysis_mode="sequential",
                max_workers=1,
                auto_monitor_sync=True,
                notification_enabled=False,
                selected_agents=["technical"],
            )
        )

        task_manager = PortfolioAnalysisTaskManager()
        persisted_codes = []

        class FakePortfolioManager:
            def get_all_stocks(self):
                return [
                    {"code": "000001", "account_name": "默认账户"},
                    {"code": "000002", "account_name": "默认账户"},
                ]

            def get_stock_count(self, account_name=None):
                return 2 if account_name in (None, "默认账户") else 0

            def batch_analyze_portfolio(
                self,
                mode="sequential",
                period="1y",
                selected_agents=None,
                max_workers=3,
                progress_callback=None,
                result_callback=None,
                model=None,
                lightweight_model=None,
                reasoning_model=None,
                account_name=None,
            ):
                results = []
                for index, code in enumerate(["000001", "000002"], start=1):
                    single_result = {
                        "success": True,
                        "stock_info": {"symbol": code, "name": f"Stock{code}"},
                        "final_decision": {"rating": "持有"},
                    }
                    if progress_callback:
                        progress_callback(index, 2, code, "success")
                    if result_callback:
                        result_callback(code, single_result)
                    results.append({"code": code, "result": single_result})
                return {
                    "success": True,
                    "mode": mode,
                    "total": 2,
                    "succeeded": 2,
                    "failed": 0,
                    "results": results,
                    "failed_stocks": [],
                    "elapsed_time": 0.1,
                }

            def persist_single_analysis_result(
                self,
                code,
                analysis_result,
                *,
                sync_realtime_monitor=True,
                analysis_source="portfolio_batch_analysis",
                analysis_period="1y",
                account_name=None,
            ):
                persisted_codes.append(code)
                return {
                    "saved_ids": [len(persisted_codes)],
                    "sync_result": {"added": 1, "updated": 0, "failed": 0, "total": 1},
                }

        with patch("portfolio_scheduler.portfolio_analysis_task_manager", task_manager), patch(
            "portfolio_scheduler.portfolio_manager",
            FakePortfolioManager(),
        ):
            task_id = scheduler._scheduled_job()
            self.assertIsNotNone(task_id)

            for _ in range(60):
                task = task_manager.get_task(task_id)
                if task and task.get("status") == "success":
                    break
                time.sleep(0.02)
            else:
                self.fail("scheduled portfolio task did not finish in time")

        task = task_manager.get_task(task_id)
        self.assertEqual(task["status"], "success")
        self.assertEqual(persisted_codes, ["000001", "000002"])
        self.assertEqual(task["result"]["analysis_source"], "portfolio_scheduler")
        self.assertEqual(task["result"]["persistence_result"]["saved_ids"], [1, 2])
        self.assertEqual(task["message"], "定时持仓分析完成：成功 2，失败 0，已写入 2 条历史")

    def test_scheduler_only_runs_enabled_accounts(self):
        scheduler = PortfolioScheduler()
        scheduler.set_task_config(
            PortfolioAnalysisTaskConfig(
                analysis_mode="parallel",
                max_workers=3,
                auto_monitor_sync=False,
                notification_enabled=False,
                selected_agents=["technical"],
            )
        )
        scheduler.set_account_task_configs(
            [
                {"account_name": "ly", "enabled": True},
                {"account_name": "zfy", "enabled": False},
            ]
        )

        task_manager = PortfolioAnalysisTaskManager()
        execution_records = []
        persisted_records = []

        class FakePortfolioManager:
            def get_all_stocks(self):
                return [
                    {"code": "000001", "account_name": "ly"},
                    {"code": "000002", "account_name": "zfy"},
                ]

            def get_stock_count(self, account_name=None):
                if account_name == "ly":
                    return 1
                if account_name == "zfy":
                    return 1
                return 2

            def batch_analyze_portfolio(
                self,
                mode="sequential",
                period="1y",
                selected_agents=None,
                max_workers=3,
                progress_callback=None,
                result_callback=None,
                model=None,
                lightweight_model=None,
                reasoning_model=None,
                account_name=None,
            ):
                code = "000001" if account_name == "ly" else "000002"
                execution_records.append((account_name, mode, max_workers))
                single_result = {
                    "success": True,
                    "stock_info": {"symbol": code, "name": f"Stock{code}"},
                    "final_decision": {"rating": "持有"},
                }
                if progress_callback:
                    progress_callback(1, 1, code, "success")
                if result_callback:
                    result_callback(code, single_result)
                return {
                    "success": True,
                    "mode": mode,
                    "total": 1,
                    "succeeded": 1,
                    "failed": 0,
                    "results": [{"code": code, "result": single_result}],
                    "failed_stocks": [],
                    "elapsed_time": 0.1,
                }

            def persist_single_analysis_result(
                self,
                code,
                analysis_result,
                *,
                sync_realtime_monitor=True,
                analysis_source="portfolio_batch_analysis",
                analysis_period="1y",
                account_name=None,
            ):
                persisted_records.append((account_name, code))
                return {
                    "saved_ids": [len(persisted_records)],
                    "sync_result": None,
                }

        with patch("portfolio_scheduler.portfolio_analysis_task_manager", task_manager), patch(
            "portfolio_scheduler.portfolio_manager",
            FakePortfolioManager(),
        ):
            task_id = scheduler._scheduled_job()
            self.assertIsNotNone(task_id)

            for _ in range(60):
                task = task_manager.get_task(task_id)
                if task and task.get("status") == "success":
                    break
                time.sleep(0.02)
            else:
                self.fail("account-filtered scheduled portfolio task did not finish in time")

        self.assertEqual(execution_records, [("ly", "parallel", 3)])
        self.assertEqual(persisted_records, [("ly", "000001")])

    def test_scheduler_registers_weekday_jobs_only(self):
        scheduler = PortfolioScheduler()
        scheduler.schedule_times = ["09:30", "15:00"]

        class FakePortfolioManager:
            def get_stock_count(self):
                return 1

        with patch("portfolio_scheduler.portfolio_manager", FakePortfolioManager()):
            self.assertTrue(scheduler.start())
            jobs = portfolio_schedule.get_jobs("portfolio_analysis")
            self.assertEqual(len(jobs), 10)
            self.assertTrue(all(getattr(job, "start_day", None) in {"monday", "tuesday", "wednesday", "thursday", "friday"} for job in jobs))

        scheduler.stop()


if __name__ == "__main__":
    unittest.main()
