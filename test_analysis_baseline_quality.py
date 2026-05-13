import unittest
import tempfile
from pathlib import Path

from analysis_baseline_quality import assess_baseline_quality
from analysis_repository import AnalysisRepository


class AnalysisBaselineQualityTests(unittest.TestCase):
    def test_healthy_research_baseline(self):
        result = assess_baseline_quality(
            {
                "rating": "买入",
                "target_price": "13.5",
                "entry_range": "10.0-10.5",
                "take_profit": "13.0",
                "stop_loss": "9.4",
                "operation_advice": "回踩确认后分批买入。",
                "entry_conditions": ["回踩后缩量企稳"],
                "exit_conditions": ["放量跌破支撑"],
                "hold_conditions": ["维持均线上方"],
                "invalidation_conditions": ["跌破止损位"],
            },
            stock_info={"current_price": 10.2, "realtime_data_source": "tdx"},
        )

        self.assertEqual(result["status"], "healthy")
        self.assertGreaterEqual(result["score"], 75)
        self.assertTrue(result["execution_plan_complete"])

    def test_invalid_price_relationship_needs_review(self):
        result = assess_baseline_quality(
            {
                "rating": "买入",
                "entry_range": "10.0-10.5",
                "take_profit": "9.8",
                "stop_loss": "9.4",
                "entry_conditions": ["回踩后缩量企稳"],
            },
            stock_info={"current_price": 10.2},
        )

        self.assertEqual(result["status"], "needs_review")
        self.assertIn("invalid_price_relationship", result["quality_flags"])

    def test_reduce_or_sell_position_baseline_does_not_require_reward_risk_ratio(self):
        result = assess_baseline_quality(
            {
                "rating": "减仓",
                "entry_range": "111.2-112.8",
                "take_profit": "130",
                "stop_loss": "105",
                "swing_type": "标准波段",
                "entry_conditions": ["回踩后再补仓"],
                "exit_conditions": ["跌破止损位"],
                "hold_conditions": ["未触发离场条件"],
                "invalidation_conditions": ["基本面恶化"],
            },
            stock_info={"current_price": 115.57, "has_position": True, "realtime_data_source": "tdx"},
        )

        self.assertEqual(result["status"], "healthy")
        self.assertNotIn("weak_reward_risk", result["quality_flags"])

    def test_repository_persists_baseline_quality_into_strategy_context(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = AnalysisRepository(str(Path(temp_dir) / "investment.db"))
            record_id = repo.save_record(
                symbol="600519",
                stock_name="贵州茅台",
                period="1y",
                stock_info={"symbol": "600519", "name": "贵州茅台", "current_price": 10.2},
                agents_results={"technical": {"analysis": "ok"}},
                discussion_result="ok",
                final_decision={
                    "rating": "买入",
                    "entry_range": "10.0-10.5",
                    "take_profit": "9.8",
                    "stop_loss": "9.4",
                },
            )

            record = repo.get_record(record_id)
            context = repo.get_latest_strategy_context(symbol="600519")

            self.assertEqual(record["baseline_status"], "needs_review")
            self.assertEqual(context["baseline_status"], "needs_review")
            self.assertIn("invalid_price_relationship", context["baseline_quality"]["quality_flags"])

    def test_repository_rebuilds_missing_baseline_quality_for_legacy_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = AnalysisRepository(str(Path(temp_dir) / "investment.db"))
            record_id = repo.save_record(
                symbol="300001",
                stock_name="测试股份",
                period="1y",
                stock_info={"symbol": "300001", "name": "测试股份", "current_price": 10.2},
                agents_results={"technical": {"analysis": "ok"}},
                discussion_result="ok",
                final_decision={
                    "rating": "买入",
                    "entry_range": "10.0-10.5",
                    "take_profit": "13.0",
                    "stop_loss": "9.4",
                },
            )
            conn = repo._connect()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE analysis_records
                    SET baseline_quality_json = NULL,
                        baseline_status = NULL,
                        baseline_schema_version = NULL
                    WHERE id = ?
                    """,
                    (record_id,),
                )
                conn.commit()
            finally:
                conn.close()

            record = repo.get_record(record_id)
            context = repo.get_latest_strategy_context(symbol="300001")

            self.assertEqual(record["baseline_status"], "incomplete")
            self.assertEqual(record["baseline_quality"]["status"], "incomplete")
            self.assertEqual(context["baseline_status"], "incomplete")
            self.assertEqual(context["baseline_quality"]["status"], "incomplete")

    def test_repository_prefers_final_decision_rating_when_explicit_rating_is_stale(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = AnalysisRepository(str(Path(temp_dir) / "investment.db"))
            record_id = repo.save_record(
                symbol="603179",
                stock_name="新泉股份",
                period="1y",
                stock_info={"symbol": "603179", "name": "新泉股份", "current_price": 71.2, "has_position": True},
                agents_results={"technical": {"analysis": "ok"}},
                discussion_result="ok",
                final_decision={
                    "rating": "加仓",
                    "entry_range": "65.6-70.4",
                    "take_profit": "78",
                    "stop_loss": "67.78",
                    "swing_type": "微波段",
                },
                rating="买入",
                analysis_scope="portfolio",
                asset_status_snapshot="holding",
                has_full_report=True,
            )

            record = repo.get_record(record_id)

            self.assertEqual(record["rating"], "加仓")


if __name__ == "__main__":
    unittest.main()
