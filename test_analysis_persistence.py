import tempfile
import unittest

from database import StockAnalysisDatabase
from macro_cycle_db import MacroCycleDatabase
from monitor_db import StockMonitorDatabase
from portfolio_db import PortfolioDB
from portfolio_manager import PortfolioManager
from smart_monitor_db import SmartMonitorDB
from sector_strategy_engine import SectorStrategyEngine


class PortfolioHistoryBackfillTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        base = self.temp_dir.name
        self.portfolio_db = PortfolioDB(f"{base}/portfolio.db")
        self.global_history_db = StockAnalysisDatabase(f"{base}/global.db")
        self.realtime_monitor_db = StockMonitorDatabase(f"{base}/monitor.db")
        self.smart_monitor_db = SmartMonitorDB(f"{base}/smart.db")
        self.manager = PortfolioManager(
            portfolio_store=self.portfolio_db,
            global_history_store=self.global_history_db,
            realtime_monitor_store=self.realtime_monitor_db,
            smart_monitor_store=self.smart_monitor_db,
        )
        self.manager._resolve_stock_name = lambda code: f"Stock{code}"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _add_stock(self, code: str):
        success, msg, stock_id = self.manager.add_stock(
            code=code,
            name=None,
            cost_price=10.0,
            quantity=100,
            note="test",
            auto_monitor=True,
        )
        self.assertTrue(success, msg)
        return stock_id

    def test_summary_sanitization_and_fallback(self):
        clean_summary = self.manager._extract_analysis_summary(
            {
                "rating": "买入",
                "operation_advice": "<think>内部推理</think>建议分批低吸，跌破9.80元止损。",
                "entry_range": "10.00-10.50",
                "take_profit": "11.80元",
                "stop_loss": "9.80元",
            }
        )
        self.assertIn("建议分批低吸", clean_summary)
        self.assertNotIn("<think>", clean_summary)

        fallback_summary = self.manager._extract_analysis_summary(
            {
                "rating": "持有",
                "operation_advice": "【推理过程】我现在需要先逐步分析所有因子，然后再输出。",
                "entry_range": "10.00-10.50",
                "take_profit": "11.80元",
                "stop_loss": "9.80元",
            }
        )
        self.assertIn("评级: 持有", fallback_summary)
        self.assertIn("进场区间", fallback_summary)
        self.assertIn("止盈", fallback_summary)

    def test_legacy_backfill_creates_one_full_report_once(self):
        stock_id = self._add_stock("600519")
        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="持有",
            confidence=6.0,
            current_price=100.0,
            target_price=110.0,
            entry_min=98.0,
            entry_max=101.0,
            take_profit=112.0,
            stop_loss=95.0,
            summary="旧版摘要",
            analysis_source="portfolio_batch_analysis",
            has_full_report=False,
        )

        self.manager.analyze_single_stock = lambda code: {
            "success": True,
            "stock_info": {"symbol": code, "name": f"Stock{code}", "current_price": 101.5},
            "agents_results": {
                "technical": {
                    "agent_name": "技术分析师",
                    "agent_role": "技术",
                    "focus_areas": ["趋势"],
                    "analysis": "趋势改善",
                    "timestamp": "2026-03-07 10:00:00",
                }
            },
            "discussion_result": "团队讨论认为回撤可控。",
            "final_decision": {
                "rating": "买入",
                "confidence_level": 8.2,
                "entry_range": "100.0-102.0",
                "take_profit": "115.0元",
                "stop_loss": "96.0元",
                "operation_advice": "建议分批建仓。",
            },
        }

        first_result = self.manager.backfill_legacy_history_details()
        self.assertEqual(first_result["candidates"], 1)
        self.assertEqual(first_result["created"], 1)
        full_history = self.portfolio_db.get_analysis_history(stock_id, limit=10, include_legacy=False)
        self.assertEqual(len(full_history), 1)
        self.assertEqual(full_history[0]["analysis_source"], "legacy_backfill")
        self.assertTrue(full_history[0]["has_full_report"])
        self.assertEqual(full_history[0]["stock_info"]["symbol"], "600519")

        second_result = self.manager.backfill_legacy_history_details()
        self.assertEqual(second_result["created"], 0)
        self.assertEqual(len(self.portfolio_db.get_analysis_history(stock_id, limit=20, include_legacy=True)), 2)


class MacroCyclePersistenceTests(unittest.TestCase):
    def test_macro_cycle_database_roundtrip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = MacroCycleDatabase(f"{temp_dir}/macro_cycle.db")
            report_id = db.save_analysis_report(
                {
                    "success": True,
                    "timestamp": "2026-03-07 12:00:00",
                    "agents_analysis": {
                        "chief": {"analysis": "当前处于复苏后段，权益资产仍有配置价值。"}
                    },
                },
                "当前处于复苏后段，权益资产仍有配置价值。",
                "当前处于复苏后段，权益资产仍有配置价值。",
            )

            latest = db.get_latest_report()
            self.assertIsNotNone(latest)
            self.assertEqual(latest["id"], report_id)
            self.assertEqual(latest["result_parsed"]["timestamp"], "2026-03-07 12:00:00")

            history = db.get_historical_reports(limit=10)
            self.assertEqual(len(history), 1)

            detail = db.get_report_detail(report_id)
            self.assertEqual(detail["summary"], "当前处于复苏后段，权益资产仍有配置价值。")
            self.assertTrue(db.delete_report(report_id))
            self.assertIsNone(db.get_latest_report())


class SectorStrategySummaryTests(unittest.TestCase):
    def test_generate_report_summary_uses_structured_predictions(self):
        engine = SectorStrategyEngine.__new__(SectorStrategyEngine)
        summary = engine._generate_report_summary(
            {
                "final_predictions": {
                    "summary": {
                        "market_view": "市场风险偏好回升",
                        "key_opportunity": "高景气成长板块有轮动机会",
                    },
                    "long_short": {
                        "bullish": [{"sector": "算力"}, {"sector": "机器人"}],
                        "bearish": [{"sector": "高位题材"}],
                    },
                }
            }
        )

        self.assertIn("市场风险偏好回升", summary)
        self.assertIn("高景气成长板块有轮动机会", summary)
        self.assertIn("看多板块", summary)
        self.assertIn("关注风险板块", summary)


if __name__ == "__main__":
    unittest.main()
