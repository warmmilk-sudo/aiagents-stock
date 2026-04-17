import os
import tempfile
import unittest
from pathlib import Path

from analysis_repository import AnalysisRepository
from asset_repository import AssetRepository
from asset_service import AssetService
from monitoring_repository import MonitoringRepository


class PositionCycleStrategyBaselineTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)
        self.original_cwd = os.getcwd()
        os.chdir(self.base)
        self.db_path = self.base / "investment.db"
        self.asset_repo = AssetRepository(str(self.db_path))
        self.analysis_repo = AnalysisRepository(str(self.db_path), legacy_analysis_db_path="")
        self.asset_service = AssetService(
            asset_store=self.asset_repo,
            analysis_store=self.analysis_repo,
            monitoring_store=MonitoringRepository(str(self.db_path)),
        )

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def test_research_level_swing_type_is_not_reused_while_flat_and_cycle_baseline_overrides_when_holding(self):
        asset_id = self.asset_repo.create_or_update_research_asset(
            symbol="600519",
            name="贵州茅台",
            note="研究池",
        )
        self.analysis_repo.save_record(
            symbol="600519",
            stock_name="贵州茅台",
            period="1y",
            account_name="默认账户",
            asset_id=asset_id,
            analysis_scope="research",
            analysis_source="manual_research",
            final_decision={
                "rating": "买入",
                "holding_period": "5-15个交易日",
                "swing_type": "标准波段",
                "swing_type_reason": "旧研究结论认为适合吃趋势。",
                "operation_advice": "等待回踩确认后介入。",
            },
            has_full_report=True,
        )

        flat_context = self.analysis_repo.get_latest_strategy_context(
            asset_id=asset_id,
            symbol="600519",
            account_name="默认账户",
        )
        self.assertEqual(flat_context["swing_type"], "")
        self.assertEqual(flat_context["holding_period"], "")

        pending_action_id = self.asset_repo.create_pending_action(
            asset_id=asset_id,
            action_type="buy",
            origin_decision_id=88,
            payload={
                "entry_strategy_baseline": {
                    "swing_type": "微波段",
                    "swing_type_reason": "当前更偏事件催化+快进快出，止损要更紧。",
                    "holding_period": "2-5个交易日",
                    "baseline_source": "monitor_buy_signal",
                    "baseline_analysis_id": 11,
                    "baseline_decision_id": 88,
                }
            },
        )
        success, _, _ = self.asset_service.record_manual_trade(
            asset_id=asset_id,
            trade_type="buy",
            quantity=100,
            price=1500.0,
            trade_date="2026-04-17",
            pending_action_id=pending_action_id,
        )
        self.assertTrue(success)

        open_cycle = self.asset_repo.get_open_position_cycle(asset_id)
        self.assertIsNotNone(open_cycle)
        self.assertEqual(open_cycle["swing_type"], "微波段")
        self.assertEqual(open_cycle["baseline_decision_id"], 88)

        holding_context = self.analysis_repo.get_latest_strategy_context(
            asset_id=asset_id,
            symbol="600519",
            account_name="默认账户",
        )
        self.assertEqual(holding_context["swing_type"], "微波段")
        self.assertEqual(holding_context["holding_period"], "2-5个交易日")
        self.assertEqual(holding_context["position_cycle_baseline_source"], "monitor_buy_signal")

        clear_success, _, _ = self.asset_service.record_manual_trade(
            asset_id=asset_id,
            trade_type="clear",
            quantity=0,
            price=1512.0,
            trade_date="2026-04-18",
        )
        self.assertTrue(clear_success)
        self.assertIsNone(self.asset_repo.get_open_position_cycle(asset_id))

        cleared_context = self.analysis_repo.get_latest_strategy_context(
            asset_id=asset_id,
            symbol="600519",
            account_name="默认账户",
        )
        self.assertEqual(cleared_context["swing_type"], "")
        self.assertEqual(cleared_context["holding_period"], "")


if __name__ == "__main__":
    unittest.main()
