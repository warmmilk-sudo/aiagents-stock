import unittest
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.modules.setdefault("pandas", MagicMock())
sys.modules.setdefault("dotenv", MagicMock())
sys.modules.setdefault("tushare", MagicMock())

from asset_repository import STATUS_PORTFOLIO
from smart_monitor_decision_auditor import SmartMonitorDecisionAuditor
import smart_monitor_engine as smart_monitor_engine_module
from smart_monitor_engine import (
    SmartMonitorEngine,
    enqueue_single_symbol_baseline_reanalysis,
    evaluate_intraday_baseline_reanalysis_trigger,
)


class SmartMonitorEngineTests(unittest.TestCase):
    def test_build_account_info_recomputes_usage_when_position_is_only_from_snapshot(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)
        engine.db = SimpleNamespace(
            portfolio_db=SimpleNamespace(
                get_all_stocks=MagicMock(return_value=[]),
                get_account_total_assets=MagicMock(return_value=100000),
            )
        )

        account_info = engine._build_account_info(
            account_name="zfy",
            asset=None,
            stock_code="600519",
            asset_id=None,
            portfolio_stock_id=None,
            has_position=True,
            position_cost=10.0,
            position_quantity=1000,
            position_date="2026-05-10",
            current_market_price=12.0,
        )

        self.assertEqual(account_info["total_market_value"], 12000)
        self.assertAlmostEqual(account_info["position_usage_pct"], 0.12)
        self.assertAlmostEqual(account_info["current_position"]["position_pct"], 0.12)
        self.assertEqual(account_info["available_cash"], 88000)

    def test_baseline_reanalysis_trigger_fires_on_limit_up_above_take_profit(self):
        trigger = evaluate_intraday_baseline_reanalysis_trigger(
            decision={"action": "HOLD", "baseline_relation": "followed", "baseline_conflict_score": 10},
            audit_context={"hard_risk_sell": False},
            strategy_context={
                "origin_analysis_id": 91,
                "entry_min": 10.0,
                "entry_max": 10.5,
                "take_profit": 12.0,
                "stop_loss": 9.5,
            },
            market_data={"current_price": 12.1, "feature_beacons": ["limit_up_hit"]},
            has_position=True,
        )

        self.assertIsNotNone(trigger)
        self.assertEqual(trigger["reason_code"], "take_profit_with_strong_extension")
        self.assertIn("limit_up_hit", trigger["strong_beacons"])

    def test_baseline_reanalysis_trigger_fires_when_take_profit_exceeded_by_3pct(self):
        trigger = evaluate_intraday_baseline_reanalysis_trigger(
            decision={"action": "HOLD", "baseline_relation": "followed", "baseline_conflict_score": 10},
            audit_context={},
            strategy_context={
                "entry_min": 10.0,
                "entry_max": 10.5,
                "take_profit": 12.0,
                "stop_loss": 9.5,
            },
            market_data={"current_price": 12.36, "feature_beacons": []},
            has_position=True,
        )

        self.assertIsNotNone(trigger)
        self.assertEqual(trigger["reason_code"], "take_profit_exceeded_by_3pct")

    def test_baseline_reanalysis_trigger_ignores_plain_take_profit_touch(self):
        trigger = evaluate_intraday_baseline_reanalysis_trigger(
            decision={"action": "HOLD", "baseline_relation": "followed", "baseline_conflict_score": 10},
            audit_context={},
            strategy_context={
                "entry_min": 10.0,
                "entry_max": 10.5,
                "take_profit": 12.0,
                "stop_loss": 9.5,
            },
            market_data={"current_price": 12.01, "feature_beacons": []},
            has_position=True,
        )

        self.assertIsNone(trigger)

    def test_baseline_reanalysis_trigger_fires_on_stop_loss_breach(self):
        trigger = evaluate_intraday_baseline_reanalysis_trigger(
            decision={"action": "HOLD", "baseline_relation": "followed", "baseline_conflict_score": 10},
            audit_context={},
            strategy_context={
                "entry_min": 10.0,
                "entry_max": 10.5,
                "take_profit": 12.0,
                "stop_loss": 9.5,
            },
            market_data={"current_price": 9.49},
            has_position=True,
        )

        self.assertIsNotNone(trigger)
        self.assertEqual(trigger["reason_code"], "stop_loss_breached")

    def test_baseline_reanalysis_trigger_respects_conflict_score_threshold(self):
        low_trigger = evaluate_intraday_baseline_reanalysis_trigger(
            decision={"baseline_relation": "partially_deviated", "baseline_conflict_score": 55},
            audit_context={},
            strategy_context={
                "entry_min": 10.0,
                "entry_max": 10.5,
                "take_profit": 12.0,
                "stop_loss": 9.5,
            },
            market_data={"current_price": 10.8},
            has_position=True,
        )
        high_trigger = evaluate_intraday_baseline_reanalysis_trigger(
            decision={"baseline_relation": "partially_deviated", "baseline_conflict_score": 85},
            audit_context={},
            strategy_context={
                "entry_min": 10.0,
                "entry_max": 10.5,
                "take_profit": 12.0,
                "stop_loss": 9.5,
            },
            market_data={"current_price": 10.8},
            has_position=True,
        )

        self.assertIsNone(low_trigger)
        self.assertIsNotNone(high_trigger)
        self.assertEqual(high_trigger["reason_code"], "high_baseline_conflict_score")

    def test_baseline_reanalysis_enqueue_cools_down_same_symbol_once_per_day(self):
        smart_monitor_engine_module._AUTO_BASELINE_REANALYSIS_COOLDOWN_KEYS.clear()
        started_tasks = []

        def fake_start_task(_session_id, **kwargs):
            started_tasks.append(kwargs)
            return "task-auto-1"

        try:
            with patch.object(
                smart_monitor_engine_module.config,
                "has_api_credentials_for_models",
                return_value=True,
            ), patch.object(
                smart_monitor_engine_module.portfolio_analysis_task_manager,
                "get_pending_tasks_any",
                return_value=[],
            ), patch.object(
                smart_monitor_engine_module.portfolio_analysis_task_manager,
                "start_task",
                side_effect=fake_start_task,
            ):
                first = enqueue_single_symbol_baseline_reanalysis(
                    stock_code="600519",
                    stock_name="贵州茅台",
                    account_name="测试账户",
                    has_position=True,
                    asset_id=101,
                    portfolio_stock_id=101,
                    market_data={"realtime_freshness": {"asof_time": "2026-04-20 10:30:00"}},
                    trigger={"reason_code": "stop_loss_breached"},
                    asset_service=None,
                )
                second = enqueue_single_symbol_baseline_reanalysis(
                    stock_code="600519",
                    stock_name="贵州茅台",
                    account_name="测试账户",
                    has_position=True,
                    asset_id=101,
                    portfolio_stock_id=101,
                    market_data={"realtime_freshness": {"asof_time": "2026-04-20 13:30:00"}},
                    trigger={"reason_code": "baseline_invalidated"},
                    asset_service=None,
                )

            self.assertEqual(first["status"], "submitted")
            self.assertEqual(first["task_id"], "task-auto-1")
            self.assertEqual(second["status"], "skipped_duplicate")
            self.assertEqual(len(started_tasks), 1)
        finally:
            smart_monitor_engine_module._AUTO_BASELINE_REANALYSIS_COOLDOWN_KEYS.clear()

    def test_decision_auditor_downgrades_buy_when_realtime_not_ready(self):
        auditor = SmartMonitorDecisionAuditor()
        decision, audit = auditor.audit(
            decision={
                "action": "BUY",
                "action_detail": "建仓",
                "action_ratio_pct": 20,
                "confidence": 82,
                "reasoning": "盘中结构转强。",
                "monitor_levels": {"entry_min": 10.0, "entry_max": 10.8, "take_profit": 12.5, "stop_loss": 9.5},
            },
            strategy_context={
                "baseline_quality": {"status": "healthy", "score": 88},
                "entry_conditions": ["回踩后缩量企稳"],
            },
            market_data={
                "current_price": 10.4,
                "realtime_freshness": {"overall_status": "stale"},
            },
            has_position=False,
            account_info={},
            risk_profile={"position_size_pct": 20, "total_position_pct": 100},
            memory_context={},
            can_sell_today=False,
            session_info={"can_trade": True},
            notify=True,
            trading_hours_only=True,
        )

        self.assertEqual(decision["action"], "HOLD")
        self.assertEqual(decision["original_action"], "BUY")
        self.assertIn("realtime_not_ready", audit["quality_flags"])
        self.assertTrue(audit["veto_reason"])

    def test_decision_auditor_allows_hard_risk_sell_below_score_threshold(self):
        auditor = SmartMonitorDecisionAuditor()
        decision, audit = auditor.audit(
            decision={
                "action": "SELL",
                "action_detail": "清仓",
                "action_ratio_pct": 100,
                "confidence": 65,
                "reasoning": "跌破止损位，基线失效。",
                "monitor_levels": {"entry_min": 10.0, "entry_max": 10.8, "take_profit": 12.5, "stop_loss": 9.5},
                "baseline_relation": "invalidated",
            },
            strategy_context={"baseline_quality": {"status": "incomplete", "score": 70}},
            market_data={
                "current_price": 9.3,
                "realtime_freshness": {"overall_status": "ready"},
            },
            has_position=True,
            account_info={},
            risk_profile={"position_size_pct": 20, "total_position_pct": 100},
            memory_context={},
            can_sell_today=True,
            session_info={"can_trade": True},
            notify=True,
            trading_hours_only=True,
        )

        self.assertEqual(decision["action"], "SELL")
        self.assertTrue(audit["hard_risk_sell"])

    def test_strategy_guardrail_allows_shallow_pullback_dynamic_entry(self):
        engine = object.__new__(SmartMonitorEngine)
        decision = {
            "action": "BUY",
            "action_detail": "建仓",
            "swing_execution_mode": "watch_hold",
            "entry_execution_mode": "shallow_pullback",
            "action_ratio_pct": 10,
            "reasoning": "15/30/60分钟承接恢复，量能配合，适合浅回踩试仓。",
            "monitor_levels": {"entry_min": 100.0, "entry_max": 102.0, "take_profit": 115.0, "stop_loss": 96.0},
        }

        result = engine._apply_strategy_plan_guardrails(
            decision=decision,
            strategy_context={
                "rating": "买入",
                "entry_min": 100.0,
                "entry_max": 102.0,
                "take_profit": 115.0,
                "stop_loss": 96.0,
                "entry_execution_mode": "shallow_pullback",
            },
            market_data={"current_price": 103.0, "atr14": 3.0},
            has_position=False,
        )

        self.assertEqual(result["action"], "BUY")
        self.assertIn("动态容忍范围", result["reasoning"])

    def test_strategy_guardrail_blocks_dynamic_entry_when_deviation_too_large(self):
        engine = object.__new__(SmartMonitorEngine)
        decision = {
            "action": "BUY",
            "action_detail": "建仓",
            "entry_execution_mode": "shallow_pullback",
            "action_ratio_pct": 10,
            "reasoning": "分时转强。",
            "monitor_levels": {"entry_min": 100.0, "entry_max": 102.0, "take_profit": 115.0, "stop_loss": 96.0},
        }

        result = engine._apply_strategy_plan_guardrails(
            decision=decision,
            strategy_context={
                "rating": "买入",
                "entry_min": 100.0,
                "entry_max": 102.0,
                "take_profit": 115.0,
                "stop_loss": 96.0,
                "entry_execution_mode": "shallow_pullback",
            },
            market_data={"current_price": 105.0, "atr14": 3.0},
            has_position=False,
        )

        self.assertEqual(result["action"], "HOLD")
        self.assertIn("已阻断追高", result["reasoning"])

    def test_decision_auditor_does_not_veto_allowed_dynamic_entry(self):
        auditor = SmartMonitorDecisionAuditor()
        decision, audit = auditor.audit(
            decision={
                "action": "BUY",
                "action_detail": "建仓",
                "entry_execution_mode": "shallow_pullback",
                "action_ratio_pct": 10,
                "confidence": 80,
                "reasoning": "15/30/60分钟承接恢复，量能配合，适合浅回踩试仓。",
                "monitor_levels": {"entry_min": 100.0, "entry_max": 102.0, "take_profit": 115.0, "stop_loss": 96.0},
            },
            strategy_context={
                "baseline_quality": {"status": "healthy", "score": 88},
                "rating": "买入",
                "entry_min": 100.0,
                "entry_max": 102.0,
                "take_profit": 115.0,
                "stop_loss": 96.0,
                "entry_execution_mode": "shallow_pullback",
            },
            market_data={
                "current_price": 103.0,
                "atr14": 3.0,
                "realtime_freshness": {"overall_status": "ready"},
            },
            has_position=False,
            account_info={},
            risk_profile={"position_size_pct": 20, "total_position_pct": 100},
            memory_context={},
            can_sell_today=False,
            session_info={"can_trade": True},
            notify=True,
            trading_hours_only=True,
        )

        self.assertEqual(decision["action"], "BUY")
        self.assertEqual(audit["veto_reason"], "")
        self.assertIn("dynamic_entry_outside_range", audit["quality_flags"])

    def test_decision_auditor_ignores_incomplete_baseline_quality(self):
        auditor = SmartMonitorDecisionAuditor()
        decision, audit = auditor.audit(
            decision={
                "action": "BUY",
                "action_detail": "建仓",
                "action_ratio_pct": 10,
                "confidence": 82,
                "reasoning": "盘中结构转强。",
                "monitor_levels": {"entry_min": 10.0, "entry_max": 10.8, "take_profit": 12.5, "stop_loss": 9.5},
                "matched_baseline_conditions": ["回踩后缩量企稳"],
            },
            strategy_context={
                "baseline_quality": {"status": "incomplete", "score": 70},
                "entry_conditions": ["回踩后缩量企稳"],
            },
            market_data={
                "current_price": 10.4,
                "realtime_freshness": {"overall_status": "ready"},
            },
            has_position=False,
            account_info={},
            risk_profile={"position_size_pct": 20, "total_position_pct": 100},
            memory_context={},
            can_sell_today=False,
            session_info={"can_trade": True},
            notify=True,
            trading_hours_only=True,
        )

        self.assertEqual(decision["action"], "BUY")
        self.assertEqual(audit["decision_quality_score"], 100.0)
        self.assertNotIn("baseline_incomplete", audit["quality_flags"])
        self.assertEqual(audit["veto_reason"], "")

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

    def test_baseline_metadata_records_unmet_conditions_and_memory_ids(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)

        decision = {
            "action": "HOLD",
            "action_detail": "观望",
            "reasoning": "当前进入计划区间，但未明确确认这些条件已满足，先降级为HOLD。",
            "baseline_relation": "followed",
        }

        updated = engine._apply_baseline_metadata_guardrails(
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
            memory_context={
                "recalled_facts": [
                    {"id": 31, "fact_content": "历史上多次追高后冲高回落"},
                    {"id": 32, "fact_content": "进场需要30分钟缩量企稳"},
                ]
            },
        )

        self.assertEqual(updated["baseline_relation"], "partially_deviated")
        self.assertGreaterEqual(updated["baseline_conflict_score"], 55)
        self.assertEqual(updated["memory_evidence_ids"], [31, 32])
        self.assertIn("回踩后缩量企稳", updated["unmet_baseline_conditions"])

    def test_invalidated_relation_requires_strong_evidence(self):
        engine = SmartMonitorEngine.__new__(SmartMonitorEngine)

        decision = {
            "action": "SELL",
            "action_detail": "减仓",
            "action_ratio_pct": 30,
            "reasoning": "盘中略有分歧，模型倾向先卖出。",
            "baseline_relation": "invalidated",
        }

        updated = engine._apply_baseline_metadata_guardrails(
            decision=decision,
            strategy_context={
                "rating": "持有",
                "entry_min": 10.0,
                "entry_max": 11.0,
                "take_profit": 13.0,
                "stop_loss": 9.5,
                "invalidation_conditions": ["跌破9.5且30分钟无法收回"],
            },
            market_data={"current_price": 10.2},
            has_position=True,
            memory_context={},
        )

        self.assertEqual(updated["baseline_relation"], "partially_deviated")
        self.assertEqual(updated["action"], "HOLD")
        self.assertEqual(updated["decision_state"], "WAIT")
        self.assertIn("强校验", updated["reasoning"])

    def test_decision_state_derives_action_state_without_prompt(self):
        self.assertEqual(
            SmartMonitorEngine._derive_decision_state({"action": "BUY", "action_detail": "加仓"}, has_position=True),
            "ADD_READY",
        )
        self.assertEqual(
            SmartMonitorEngine._derive_decision_state({"action": "SELL", "swing_execution_mode": "proactive_trim"}, has_position=True),
            "TRIM_PROFIT",
        )
        self.assertEqual(
            SmartMonitorEngine._derive_decision_state({"action": "HOLD", "baseline_relation": "followed"}, has_position=True),
            "HOLD_BASELINE",
        )


if __name__ == "__main__":
    unittest.main()
