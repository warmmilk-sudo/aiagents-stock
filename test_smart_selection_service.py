import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asset_repository import AssetRepository, STATUS_FOCUS, STATUS_RESEARCH
from smart_selection_service import SmartSelectionService


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


class SmartSelectionServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.asset_repo = AssetRepository(str(Path(self.temp_dir.name) / "investment.db"))
        self.service = SmartSelectionService(str(Path(self.temp_dir.name) / "investment.db"))

        self.asset_repo.create_or_update_research_asset(symbol="600001", name="算力一号", note="观察")
        self.asset_repo.update_asset(self.asset_repo.get_asset_by_symbol("600001")["id"], sector_tags_json=["算力租赁"])
        self.asset_repo.create_or_update_research_asset(symbol="600002", name="机器人二号", note="执行")
        self.asset_repo.update_asset(self.asset_repo.get_asset_by_symbol("600002")["id"], sector_tags_json=["机器人"])
        self.asset_repo.create_or_update_research_asset(symbol="600003", name="高位三号", note="衰退")
        self.asset_repo.update_asset(self.asset_repo.get_asset_by_symbol("600003")["id"], sector_tags_json=["高位题材"])
        self.old_focus_id = self.asset_repo.promote_to_watchlist(symbol="600099", name="旧关注", note="旧关注")
        self.asset_repo.update_asset(self.old_focus_id, manual_pin=True)

        self.service.sector_strategy_db.save_analysis_report("2026-04-01 数据分析", build_analysis_payload(40, 70, 95), [], "r1")
        self.service.sector_strategy_db.save_analysis_report("2026-04-02 数据分析", build_analysis_payload(55, 90, 95), [], "r2")
        self.report_id = self.service.sector_strategy_db.save_analysis_report("2026-04-03 数据分析", build_analysis_payload(70, 95, 85), [], "r3")
        self.report = self.service.sector_strategy_db.get_analysis_report(self.report_id)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def _fake_candidate(self, asset, _context, _lifecycle_snapshot):
        symbol = asset["symbol"]
        mapping = {
            "600001": ("算力租赁", 200.0, 78.0, 62.0, 70.0, 58.0, 60.0, 22.0),
            "600002": ("机器人", 150.0, 82.0, 72.0, 80.0, 76.0, 75.0, 18.0),
            "600003": ("高位题材", 180.0, 85.0, 80.0, 78.0, 74.0, 73.0, 15.0),
        }
        primary_sector, market_cap, trend, reversal, mean_reversion, order_flow, intraday, distribution = mapping[symbol]
        return {
            "asset_id": asset["id"],
            "symbol": symbol,
            "name": asset["name"],
            "matched_sectors": [{"sector": primary_sector, "heat_score": 90}],
            "primary_sector": primary_sector,
            "heat_score": 90.0,
            "tech_score": 80.0,
            "composite_score": 160.0,
            "technical_metrics": {
                "trend_score": trend,
                "reversal_score": reversal,
                "mean_reversion_score": mean_reversion,
                "order_flow_score": order_flow,
                "intraday_score": intraday,
                "chip_score": 70.0,
                "volume_score": 60.0,
                "distribution_risk": distribution,
                "volume_contraction_days": 3,
                "intraday_bias": "pullback_support",
                "bias_pct": -3.0,
            },
            "reason": f"{primary_sector} 候选",
            "market_cap": market_cap,
            "asset": asset,
        }

    def test_pipeline_routes_candidates_by_lifecycle(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        with patch("smart_selection_service.asset_repository", self.asset_repo), patch(
            "smart_selection_service.research_hub_service.ensure_recent_sector_strategy_report",
            return_value={"reused": True, "report_id": self.report_id, "report": self.report, "warnings": []},
        ), patch(
            "smart_selection_service.research_hub_service._collect_asset_match_context",
            return_value={},
        ), patch(
            "smart_selection_service.research_hub_service._score_selection_candidate",
            side_effect=self._fake_candidate,
        ):
            result = self.service._run_pipeline(run_id)

        self.assertEqual([item["symbol"] for item in result["observed_startup_candidates"]], ["600001"])
        self.assertEqual([item["symbol"] for item in result["final_selected"]], ["600002"])
        self.assertEqual([item["symbol"] for item in result["excluded_by_lifecycle_veto"]], ["600003"])
        watch_pool = self.service.list_watch_pool()
        self.assertEqual([item["symbol"] for item in watch_pool], ["600001"])

    def test_import_replaces_existing_focus(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        with patch("smart_selection_service.asset_repository", self.asset_repo), patch(
            "smart_selection_service.research_hub_service.ensure_recent_sector_strategy_report",
            return_value={"reused": True, "report_id": self.report_id, "report": self.report, "warnings": []},
        ), patch(
            "smart_selection_service.research_hub_service._collect_asset_match_context",
            return_value={},
        ), patch(
            "smart_selection_service.research_hub_service._score_selection_candidate",
            side_effect=self._fake_candidate,
        ):
            self.service._run_pipeline(run_id)
            result = self.service.import_run_selection(run_id=run_id, symbols=["600002"], replace_existing_focus=True)

        self.assertEqual(result["imported_symbols"], ["600002"])
        self.assertEqual(self.asset_repo.get_asset(self.old_focus_id)["status"], STATUS_RESEARCH)
        self.assertFalse(self.asset_repo.get_asset(self.old_focus_id)["manual_pin"])
        self.assertEqual(self.asset_repo.get_asset_by_symbol("600002")["status"], STATUS_FOCUS)
        self.assertEqual([item["symbol"] for item in self.service.list_watch_pool()], ["600001"])

    def test_scheduler_config_round_trip_includes_max_workers(self):
        with patch("smart_selection_service.smart_selection_scheduler.apply_runtime_config") as apply_runtime_config, patch(
            "smart_selection_service.smart_selection_scheduler.get_status",
            side_effect=lambda: {
                "running": False,
                "enabled": self.service.get_scheduler_config()["enabled"],
                "schedule_time": self.service.get_scheduler_config()["schedule_time"],
                "max_workers": self.service.get_scheduler_config()["max_workers"],
                "next_run_time": None,
                "last_run_time": None,
            },
        ):
            status = self.service.update_scheduler_config(enabled=True, schedule_time="14:35", max_workers=8)

        apply_runtime_config.assert_called_once()
        self.assertTrue(status["enabled"])
        self.assertEqual(status["schedule_time"], "14:35")
        self.assertEqual(status["max_workers"], 8)
        self.assertEqual(self.service.get_scheduler_config()["max_workers"], 8)


if __name__ == "__main__":
    unittest.main()
