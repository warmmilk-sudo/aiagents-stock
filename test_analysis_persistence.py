import shutil
import sys
import types
import unittest
import uuid
from pathlib import Path

from monitor_db import StockMonitorDatabase
from portfolio_db import PortfolioDB
from portfolio_manager import PortfolioManager
from smart_monitor_db import SmartMonitorDB

sys.modules.setdefault("streamlit", types.SimpleNamespace())

from ui_shared import (
    _normalize_agents_results,
    _normalize_discussion_result,
    _normalize_mapping_input,
    _normalize_text_or_mapping,
)

try:
    from macro_cycle_db import MacroCycleDatabase
except ModuleNotFoundError:
    MacroCycleDatabase = None

try:
    from sector_strategy_engine import SectorStrategyEngine
except ModuleNotFoundError:
    SectorStrategyEngine = None


TEST_TMP_ROOT = Path(".codex_test_tmp")


def make_workspace_temp_dir(prefix: str) -> Path:
    TEST_TMP_ROOT.mkdir(exist_ok=True)
    path = TEST_TMP_ROOT / f"{prefix}{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


class PortfolioHistoryPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = make_workspace_temp_dir("portfolio_history_")
        base = self.temp_dir
        self.portfolio_db = PortfolioDB(str(base / "portfolio.db"))
        self.realtime_monitor_db = StockMonitorDatabase(str(base / "monitor.db"))
        self.smart_monitor_db = SmartMonitorDB(str(base / "smart.db"))
        self.manager = PortfolioManager(
            portfolio_store=self.portfolio_db,
            realtime_monitor_store=self.realtime_monitor_db,
            smart_monitor_store=self.smart_monitor_db,
        )
        self.manager._resolve_stock_name = lambda code: f"Stock{code}"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

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
                "operation_advice": "<think>internal reasoning</think>建议分批低吸，跌破9.80元止损。",
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
        self.assertIn("止盈位", fallback_summary)

    def test_history_queries_only_return_full_reports(self):
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
            summary="不完整记录",
            analysis_source="portfolio_batch_analysis",
            has_full_report=False,
        )
        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="买入",
            confidence=8.2,
            current_price=101.5,
            target_price=115.0,
            entry_min=100.0,
            entry_max=102.0,
            take_profit=115.0,
            stop_loss=96.0,
            summary="建议分批建仓。",
            stock_info={"symbol": "600519", "name": "Stock600519", "current_price": 101.5},
            agents_results={
                "technical": {
                    "agent_name": "技术分析师",
                    "agent_role": "技术面",
                    "focus_areas": ["趋势"],
                    "analysis": "趋势改善",
                    "timestamp": "2026-03-07 10:00:00",
                }
            },
            discussion_result="团队讨论认为回撤可控。",
            final_decision={
                "rating": "买入",
                "confidence_level": 8.2,
                "entry_range": "100.0-102.0",
                "take_profit": "115.0元",
                "stop_loss": "96.0元",
                "operation_advice": "建议分批建仓。",
            },
            analysis_source="portfolio_batch_analysis",
            has_full_report=True,
        )

        full_history = self.portfolio_db.get_analysis_history(stock_id, limit=10)
        self.assertEqual(len(full_history), 1)
        self.assertEqual(full_history[0]["analysis_source"], "portfolio_batch_analysis")
        self.assertTrue(full_history[0]["has_full_report"])
        self.assertEqual(full_history[0]["stock_info"]["symbol"], "600519")
        self.assertNotIn("stock_info_json", full_history[0])
        self.assertNotIn("agents_results_json", full_history[0])
        self.assertNotIn("final_decision_json", full_history[0])

        latest = self.portfolio_db.get_latest_analysis(stock_id)
        self.assertIsNotNone(latest)
        self.assertEqual(latest["summary"], "建议分批建仓。")
        self.assertEqual(latest["final_decision"]["rating"], "买入")
        self.assertEqual(latest["discussion_result"], "团队讨论认为回撤可控。")


class UiSharedNormalizationTests(unittest.TestCase):
    def test_mapping_normalization_accepts_dict_and_json_string(self):
        normalized, invalid = _normalize_mapping_input({"current_price": 123.45})
        self.assertEqual(normalized["current_price"], 123.45)
        self.assertFalse(invalid)

        normalized, invalid = _normalize_mapping_input('{"current_price": 123.45}')
        self.assertEqual(normalized["current_price"], 123.45)
        self.assertFalse(invalid)

    def test_mapping_normalization_rejects_invalid_or_non_object_strings(self):
        normalized, invalid = _normalize_mapping_input("not-json")
        self.assertEqual(normalized, {})
        self.assertTrue(invalid)

        normalized, invalid = _normalize_mapping_input('"text only"')
        self.assertEqual(normalized, {})
        self.assertTrue(invalid)

    def test_agents_and_final_decision_normalization_tolerate_json_strings(self):
        agents_results, invalid = _normalize_agents_results(
            '{"technical": {"agent_name": "技术分析师", "analysis": "趋势改善"}}'
        )
        self.assertFalse(invalid)
        self.assertEqual(agents_results["technical"]["analysis"], "趋势改善")

        final_decision, invalid = _normalize_text_or_mapping(
            '{"rating": "买入", "confidence_level": 8.5}'
        )
        self.assertFalse(invalid)
        self.assertEqual(final_decision["rating"], "买入")

        final_decision, invalid = _normalize_text_or_mapping("plain text decision")
        self.assertFalse(invalid)
        self.assertEqual(final_decision, "plain text decision")

    def test_discussion_normalization_decodes_json_encoded_text(self):
        self.assertEqual(
            _normalize_discussion_result('"团队讨论认为回撤可控。"'),
            "团队讨论认为回撤可控。",
        )
        self.assertEqual(_normalize_discussion_result("plain discussion"), "plain discussion")


@unittest.skipIf(MacroCycleDatabase is None, "macro cycle dependencies unavailable")
class MacroCyclePersistenceTests(unittest.TestCase):
    def test_macro_cycle_database_roundtrip(self):
        temp_dir = make_workspace_temp_dir("macro_cycle_")
        try:
            db = MacroCycleDatabase(str(temp_dir / "macro_cycle.db"))
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
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


@unittest.skipIf(SectorStrategyEngine is None, "sector strategy dependencies unavailable")
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
