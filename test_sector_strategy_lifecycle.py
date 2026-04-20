import os
import tempfile
import unittest
from pathlib import Path

from sector_strategy_db import SectorStrategyDatabase


def build_analysis_payload(startup_score: int, explosive_score: int, decay_score: int) -> dict:
    return {
        "final_predictions": {
            "heat": {
                "hottest": [
                    {"sector": "机器人", "score": explosive_score, "trend": "升温"},
                    {"sector": "高位题材", "score": decay_score, "trend": "降温"},
                ],
                "heating": [
                    {"sector": "算力租赁", "score": startup_score, "trend": "升温"},
                ],
                "cooling": [],
            }
        }
    }


class SectorStrategyLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.db = SectorStrategyDatabase(str(Path(self.temp_dir.name) / "sector_strategy.db"))

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def test_heat_history_classifies_startup_explosive_and_decay(self):
        self.db.save_analysis_report("2026-04-01 数据分析", build_analysis_payload(40, 70, 95), [], "r1")
        self.db.save_analysis_report("2026-04-02 数据分析", build_analysis_payload(55, 90, 95), [], "r2")
        latest_report_id = self.db.save_analysis_report("2026-04-03 数据分析", build_analysis_payload(70, 95, 85), [], "r3")

        items = self.db.get_lifecycle_items_for_analysis(latest_report_id)
        stage_by_sector = {item["sector_name"]: item["lifecycle_stage"] for item in items}
        defense_by_sector = {item["sector_name"]: item["defense_line_type"] for item in items}
        veto_by_sector = {item["sector_name"]: item["selection_veto"] for item in items}

        self.assertEqual(stage_by_sector["算力租赁"], self.db.LIFECYCLE_STAGE_STARTUP)
        self.assertEqual(stage_by_sector["机器人"], self.db.LIFECYCLE_STAGE_EXPLOSIVE)
        self.assertEqual(stage_by_sector["高位题材"], self.db.LIFECYCLE_STAGE_DECAY)
        self.assertEqual(defense_by_sector["算力租赁"], "MA10")
        self.assertEqual(defense_by_sector["机器人"], "MA5")
        self.assertEqual(defense_by_sector["高位题材"], "NONE")
        self.assertTrue(veto_by_sector["高位题材"])

        latest_snapshot = self.db.get_latest_lifecycle_snapshot()
        self.assertTrue(latest_snapshot["available"])
        self.assertEqual(latest_snapshot["summary"]["counts"]["startup"], 1)
        self.assertEqual(latest_snapshot["summary"]["counts"]["explosive"], 1)
        self.assertEqual(latest_snapshot["summary"]["counts"]["decay"], 1)

    def test_rebuild_heat_history_reconstructs_existing_rows(self):
        self.db.save_analysis_report("2026-04-01 数据分析", build_analysis_payload(40, 70, 95), [], "r1")
        self.db.save_analysis_report("2026-04-02 数据分析", build_analysis_payload(55, 90, 95), [], "r2")
        latest_report_id = self.db.save_analysis_report("2026-04-03 数据分析", build_analysis_payload(70, 95, 85), [], "r3")

        conn = self.db.get_connection()
        try:
            conn.execute("DELETE FROM sector_heat_history")
            conn.commit()
        finally:
            conn.close()

        result = self.db.rebuild_heat_history()
        self.assertEqual(result["reports_processed"], 3)
        self.assertEqual(result["heat_rows_rebuilt"], 9)
        self.assertEqual(result["failed_reports"], [])

        items = self.db.get_lifecycle_items_for_analysis(latest_report_id)
        stage_by_sector = {item["sector_name"]: item["lifecycle_stage"] for item in items}
        self.assertEqual(stage_by_sector["算力租赁"], self.db.LIFECYCLE_STAGE_STARTUP)
        self.assertEqual(stage_by_sector["机器人"], self.db.LIFECYCLE_STAGE_EXPLOSIVE)
        self.assertEqual(stage_by_sector["高位题材"], self.db.LIFECYCLE_STAGE_DECAY)

    def test_trajectory_uses_longer_lookback_window(self):
        scores = [25, 30, 35, 42, 48, 55, 61, 69, 74, 79, 83, 88]
        latest_report_id = None
        for index, score in enumerate(scores, 1):
            latest_report_id = self.db.save_analysis_report(
                f"2026-04-{index:02d} 数据分析",
                {
                    "final_predictions": {
                        "heat": {
                            "hottest": [],
                            "heating": [{"sector": "算力租赁", "score": score, "trend": "升温"}],
                            "cooling": [],
                        }
                    }
                },
                [],
                f"r{index}",
            )

        self.assertIsNotNone(latest_report_id)
        items = self.db.get_lifecycle_items_for_analysis(latest_report_id)
        target = next(item for item in items if item["sector_name"] == "算力租赁")
        expected_window = min(len(scores), self.db.LIFECYCLE_LOOKBACK_DAYS)
        self.assertEqual(len(target["trajectory"]), expected_window)
        self.assertEqual(target["trajectory"][0]["score"], scores[-expected_window])
        self.assertEqual(target["trajectory"][-1]["score"], scores[-1])

    def test_same_day_reports_share_latest_daily_panel(self):
        morning_payload = {
            "data_summary": {
                "sectors": {"算力租赁": {"change_pct": 4.0, "turnover": 100.0, "market_cap": 200.0}},
                "concepts": {},
            }
        }
        close_payload = {
            "data_summary": {
                "sectors": {"算力租赁": {"change_pct": 9.0, "turnover": 200.0, "market_cap": 200.0}},
                "concepts": {},
            }
        }

        first_report_id = self.db.save_analysis_report("2026-04-08 数据分析", morning_payload, [], "morning")
        second_report_id = self.db.save_analysis_report("2026-04-08 数据分析", close_payload, [], "close")

        first_items = self.db.get_lifecycle_items_for_analysis(first_report_id)
        second_items = self.db.get_lifecycle_items_for_analysis(second_report_id)
        self.assertEqual(first_items[0]["heat_score"], second_items[0]["heat_score"])
        self.assertEqual(first_items[0]["board_date"], "2026-04-08")
        daily_panel = self.db.get_daily_heat_panel(board_date="2026-04-08", limit=10)
        self.assertTrue(daily_panel["available"])
        self.assertEqual(daily_panel["total_count"], 1)
        self.assertEqual(daily_panel["items"][0]["sector_name"], "算力租赁")

    def test_lifecycle_config_is_code_defined_and_read_only(self):
        config = self.db.get_lifecycle_config()
        self.assertEqual(config, self.db.DEFAULT_LIFECYCLE_CONFIG)

        with self.assertRaisesRegex(ValueError, "不支持在线修改"):
            self.db.update_lifecycle_config(
                {
                    "explosive_current_min": 83,
                    "explosive_avg_10d_min": 68.5,
                    "decay_drawdown_long_min": 16,
                }
            )

        self.assertEqual(self.db.get_lifecycle_config(), self.db.DEFAULT_LIFECYCLE_CONFIG)


if __name__ == "__main__":
    unittest.main()
