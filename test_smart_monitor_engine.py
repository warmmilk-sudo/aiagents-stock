import unittest
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.modules.setdefault("pandas", MagicMock())
sys.modules.setdefault("dotenv", MagicMock())
sys.modules.setdefault("tushare", MagicMock())

from asset_repository import STATUS_PORTFOLIO
from smart_monitor_engine import SmartMonitorEngine


class SmartMonitorEngineTests(unittest.TestCase):
    def test_apply_atr_guardrail_clamps_all_stop_loss_fields(self):
        decision = {
            "stop_loss": 9.0,
            "key_price_levels": {"support": 10.5, "resistance": 12.5, "stop_loss": 8.8},
            "monitor_levels": {
                "entry_min": 10.8,
                "entry_max": 11.1,
                "take_profit": 12.4,
                "stop_loss": 8.9,
            },
        }

        updated, clamp_info = SmartMonitorEngine._apply_atr_guardrail(
            decision=decision,
            strategy_context={"swing_type": "微波段"},
            market_data={"current_price": 10.0, "atr14": 0.5},
        )

        self.assertIsNotNone(clamp_info)
        self.assertEqual(clamp_info["atr_stop_floor"], 9.4)
        self.assertEqual(updated["stop_loss"], 9.4)
        self.assertEqual(updated["key_price_levels"]["stop_loss"], 9.4)
        self.assertEqual(updated["monitor_levels"]["stop_loss"], 9.4)

    def test_sync_position_cycle_runtime_state_writes_upgrade_and_runtime_snapshot(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)
        asset_repository = SimpleNamespace(
            get_asset=MagicMock(return_value={"id": 12, "status": STATUS_PORTFOLIO}),
            set_open_position_cycle_baseline=MagicMock(),
        )
        engine.db = SimpleNamespace(asset_repository=asset_repository)

        decision = {
            "reasoning": "创阶段新高且放量突破，短打已经演化为趋势波段。",
            "structure_state": "趋势突破确认",
            "structure_state_reason": "价格创阶段新高并站上关键均线。",
            "swing_type_upgrade": True,
            "upgraded_swing_type": "标准波段",
            "upgrade_reason": "20日新高放量突破。",
            "feature_beacons": ["stage_new_high", "breakout_20d_high_with_2x_volume"],
            "atr14": 0.5,
            "atr14_pct": 4.0,
            "atr_stop_floor": 9.0,
            "trend_anchor_type": "MA10",
            "trend_anchor_value": 10.2,
        }
        strategy_context = {
            "swing_type": "微波段",
            "swing_type_reason": "初始只按短打处理。",
            "holding_period": "2-5个交易日",
            "position_cycle_baseline_source": "analysis_record",
            "position_cycle_baseline_analysis_id": 88,
        }
        market_data = {
            "current_price": 11.0,
            "atr14": 0.5,
            "atr14_pct": 4.0,
            "ma10": 10.2,
            "ma20": 9.8,
            "feature_beacons": ["stage_new_high", "breakout_20d_high_with_2x_volume"],
            "update_time": "2026-04-20 10:30:00",
            "account_position": {"cost_price": 9.5},
        }

        engine._sync_position_cycle_runtime_state(
            asset_id=12,
            decision_id=34,
            decision=decision,
            strategy_context=strategy_context,
            market_data=market_data,
            position_date="2026-04-01",
            has_position=True,
        )

        asset_repository.set_open_position_cycle_baseline.assert_called_once()
        _, kwargs = asset_repository.set_open_position_cycle_baseline.call_args
        self.assertEqual(kwargs["swing_type"], "标准波段")
        self.assertTrue(kwargs["overwrite"])
        self.assertEqual(kwargs["baseline_decision_id"], 34)
        self.assertEqual(kwargs["baseline_snapshot_extra"]["structure_state"], "趋势突破确认")
        self.assertEqual(kwargs["baseline_snapshot_extra"]["trend_anchor_type"], "MA10")
        self.assertIn("stage_new_high", kwargs["baseline_snapshot_extra"]["feature_beacons"])

    def test_reconcile_reasoning_action_consistency_downgrades_buy_to_hold(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)
        engine.llm_client = SimpleNamespace(
            _resolve_action_detail=MagicMock(return_value="持有"),
            _resolve_swing_execution_mode=MagicMock(return_value="watch_hold"),
            _attach_execution_targets=MagicMock(side_effect=lambda decision, **_: {
                **decision,
                "trade_intent": "hold",
                "target_position_pct": 20.0,
                "current_position_pct": 20.0,
                "position_delta_pct": 0.0,
            }),
        )

        decision = {
            "action": "BUY",
            "action_detail": "加仓",
            "action_ratio_pct": 20,
            "swing_execution_mode": "pullback_add",
            "reasoning": (
                "当前价27.53处于基线加仓区间，但尾盘不宜盲目动作。"
                "建议维持持有，等待价格在支撑位企稳或分时转强后再考虑按基线计划加仓，暂不执行加仓。"
            ),
        }

        updated = engine._reconcile_reasoning_action_consistency(
            decision=decision,
            has_position=True,
            account_info={"positions": []},
            risk_profile={"position_size_pct": 20, "total_position_pct": 100},
        )

        self.assertEqual(updated["action"], "HOLD")
        self.assertEqual(updated["action_detail"], "持有")
        self.assertEqual(updated["swing_execution_mode"], "watch_hold")
        self.assertIsNone(updated["action_ratio_pct"])
        self.assertEqual(updated["trade_intent"], "hold")
        self.assertEqual(updated["position_delta_pct"], 0.0)

    def test_plan_guardrails_do_not_auto_buy_when_entry_conditions_exist(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)

        decision = {
            "action": "HOLD",
            "action_detail": "观望",
            "reasoning": "价格进入计划区间，但还需要观察。",
            "action_ratio_pct": None,
        }

        updated = engine._apply_strategy_plan_guardrails(
            decision=decision,
            strategy_context={
                "rating": "买入",
                "entry_min": 10.0,
                "entry_max": 11.0,
                "take_profit": 13.0,
                "stop_loss": 9.5,
                "entry_conditions": ["回踩后缩量企稳"],
            },
            market_data={"current_price": 10.5},
            has_position=False,
        )

        self.assertEqual(updated["action"], "HOLD")
        self.assertEqual(updated["action_detail"], "观望")
        self.assertNotIn("已触发深度分析交易计划", updated["reasoning"])

    def test_plan_guardrails_downgrades_buy_without_condition_confirmation(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)

        decision = {
            "action": "BUY",
            "action_detail": "买入",
            "reasoning": "当前价格进入计划进场区间，可以执行。",
            "action_ratio_pct": 20,
        }

        updated = engine._apply_strategy_plan_guardrails(
            decision=decision,
            strategy_context={
                "rating": "买入",
                "entry_min": 10.0,
                "entry_max": 11.0,
                "take_profit": 13.0,
                "stop_loss": 9.5,
                "entry_conditions": ["回踩后缩量企稳"],
            },
            market_data={"current_price": 10.5},
            has_position=False,
        )

        self.assertEqual(updated["action"], "HOLD")
        self.assertEqual(updated["action_detail"], "观望")
        self.assertIsNone(updated["action_ratio_pct"])
        self.assertIn("结构化进场/加仓条件", updated["reasoning"])

    def test_plan_guardrails_allows_buy_when_condition_confirmed(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)

        decision = {
            "action": "BUY",
            "action_detail": "买入",
            "reasoning": "盘中回踩后缩量企稳，满足进场条件，可以按计划小仓位执行。",
            "action_ratio_pct": 20,
        }

        updated = engine._apply_strategy_plan_guardrails(
            decision=decision,
            strategy_context={
                "rating": "买入",
                "entry_min": 10.0,
                "entry_max": 11.0,
                "take_profit": 13.0,
                "stop_loss": 9.5,
                "entry_conditions": ["回踩后缩量企稳"],
            },
            market_data={"current_price": 10.5},
            has_position=False,
        )

        self.assertEqual(updated["action"], "BUY")
        self.assertEqual(updated["action_detail"], "买入")
        self.assertEqual(updated["action_ratio_pct"], 20)


if __name__ == "__main__":
    unittest.main()
