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
        self.market_context = {
            "state": "momentum",
            "state_label": "主升浪",
            "profile": {"heat_multiplier": 0.92, "agent_multiplier": 1.12, "weights": {}},
            "signals": ["指数均值 +1.20%", "上涨占比 72.0%", "涨跌停 85/4"],
        }
        self.extracted_sectors = [
            {"sector": "机器人", "heat_score": 95, "source": "heat.hottest"},
            {"sector": "算力租赁", "heat_score": 82, "source": "heat.heating"},
            {"sector": "高位题材", "heat_score": 78, "source": "heat.hottest"},
        ]

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
                "tail_session": True,
                "latest_minute_time": "14:45",
                "latest_trade_time": "14:45",
                "realtime_freshness": {
                    "intraday_decision_ready": True,
                    "overall_status": "ready",
                },
                "bias_pct": -3.0,
            },
            "reason": f"{primary_sector} 候选",
            "market_cap": market_cap,
            "asset": asset,
        }

    def _run_with_common_patches(self, *, score_side_effect=None, risk_side_effect=None):
        return patch("smart_selection_service.asset_repository", self.asset_repo), patch(
            "smart_selection_service.research_hub_service.ensure_recent_sector_strategy_report",
            return_value={"reused": True, "report_id": self.report_id, "report": self.report, "warnings": []},
        ), patch(
            "smart_selection_service.research_hub_service._build_selection_market_context",
            return_value=self.market_context,
        ), patch(
            "smart_selection_service.research_hub_service._extract_selection_sectors",
            return_value=self.extracted_sectors,
        ), patch(
            "smart_selection_service.research_hub_service._collect_asset_match_context",
            return_value={},
        ), patch(
            "smart_selection_service.research_hub_service._score_selection_candidate",
            side_effect=score_side_effect or self._fake_candidate,
        ), patch(
            "smart_selection_service.research_hub_service._group_recent_longhubang_by_symbol",
            return_value={},
        ), patch(
            "smart_selection_service.research_hub_service._evaluate_risk_for_symbol",
            side_effect=risk_side_effect or (lambda *_args, **_kwargs: {"vetoed": False, "risk_notes": [], "risk_level": "low"}),
        )

    def test_pipeline_routes_candidates_by_lifecycle(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        common_patches = self._run_with_common_patches()
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual([item["symbol"] for item in result["observed_startup_candidates"]], ["600001"])
        self.assertEqual([item["symbol"] for item in result["final_selected"]], ["600002"])
        self.assertEqual([item["symbol"] for item in result["excluded_by_lifecycle_veto"]], ["600003"])
        self.assertEqual(result["market_context"]["state"], "momentum")
        watch_pool = self.service.list_watch_pool()
        self.assertEqual([item["symbol"] for item in watch_pool], ["600001"])

    def test_import_replaces_existing_focus(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        common_patches = self._run_with_common_patches()
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
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

    def test_pipeline_injects_market_context_into_candidate_scoring(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        score_calls = []

        def capture_candidate(asset, context, lifecycle_snapshot):
            score_calls.append((asset["symbol"], context, lifecycle_snapshot))
            return self._fake_candidate(asset, context, lifecycle_snapshot)

        common_patches = self._run_with_common_patches(score_side_effect=capture_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            self.service._run_pipeline(run_id)

        self.assertTrue(score_calls)
        self.assertTrue(all(call[1].get("market_context", {}).get("state") == "momentum" for call in score_calls))

    def test_pipeline_requires_tail_session_for_final_selection(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def non_tail_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            candidate["technical_metrics"]["tail_session"] = False
            candidate["technical_metrics"]["latest_minute_time"] = "13:20"
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=non_tail_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual(result["final_selected"], [])
        self.assertTrue(any("未到尾盘时段" in warning for warning in result["warnings"]))

    def test_pipeline_applies_individual_risk_veto(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def risk_result(symbol, *_args, **_kwargs):
            if symbol == "600002":
                return {"vetoed": True, "risk_notes": ["近2天存在股东减持或减持公告"], "risk_level": "high"}
            return {"vetoed": False, "risk_notes": [], "risk_level": "low"}

        common_patches = self._run_with_common_patches(risk_side_effect=risk_result)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual(result["final_selected"], [])
        self.assertEqual([item["symbol"] for item in result["excluded_by_risk_veto"]], ["600002"])
        self.assertIn("个股风控否决", result["excluded_by_risk_veto"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
