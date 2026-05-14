from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from investment_action_utils import extract_first_number, normalize_condition_list


DECISION_AUDIT_VERSION = "decision_audit_v1"
BUY_SCORE_THRESHOLD = 75.0
SELL_SCORE_THRESHOLD = 72.0


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric else None


def _flag(flags: List[str], flag: str) -> None:
    if flag not in flags:
        flags.append(flag)


class SmartMonitorDecisionAuditor:
    """Deterministic quality gate for intraday LLM trade decisions."""

    @staticmethod
    def _baseline_quality(strategy_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(strategy_context, dict):
            return {}
        quality = strategy_context.get("baseline_quality")
        return quality if isinstance(quality, dict) else {}

    @staticmethod
    def _freshness_state(market_data: Optional[Dict[str, Any]]) -> str:
        freshness = (market_data or {}).get("realtime_freshness")
        if isinstance(freshness, dict):
            status = str(freshness.get("overall_status") or "").strip()
            if status:
                return status
            if freshness.get("asof_time") or freshness.get("update_time"):
                return "ready"
        return "ready"

    @staticmethod
    def _entry_bounds(strategy_context: Optional[Dict[str, Any]], decision: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        levels = decision.get("monitor_levels") if isinstance(decision.get("monitor_levels"), dict) else {}
        context = strategy_context if isinstance(strategy_context, dict) else {}
        entry_min = _float_or_none(levels.get("entry_min"))
        entry_max = _float_or_none(levels.get("entry_max"))
        if entry_min is None:
            entry_min = _float_or_none(context.get("entry_min"))
        if entry_max is None:
            entry_max = _float_or_none(context.get("entry_max"))
        return entry_min, entry_max

    @staticmethod
    def _execution_conditions(strategy_context: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
        context = strategy_context if isinstance(strategy_context, dict) else {}
        plan = context.get("execution_plan") if isinstance(context.get("execution_plan"), dict) else {}

        def _pick(key: str) -> List[str]:
            return normalize_condition_list(context.get(key) or plan.get(key), max_items=6)

        return {
            "entry_conditions": _pick("entry_conditions"),
            "exit_conditions": _pick("exit_conditions"),
            "hold_conditions": _pick("hold_conditions"),
            "invalidation_conditions": _pick("invalidation_conditions"),
        }

    @staticmethod
    def _has_memory_chase_risk(memory_context: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(memory_context, dict):
            return False
        parts: List[str] = [str(memory_context.get("memory_bias") or "")]
        for fact in memory_context.get("recalled_facts") or []:
            if isinstance(fact, dict):
                parts.append(str(fact.get("fact_content") or ""))
        text = " ".join(parts)
        return any(keyword in text for keyword in ("追高", "假突破", "冲高回落", "高位派发"))

    @staticmethod
    def _reasoning_contains_veto(reasoning: object, action: str) -> bool:
        text = str(reasoning or "").strip()
        if not text:
            return False
        if action == "BUY":
            return bool(re.search(r"暂不(?:执行)?(?:买入|加仓)|不(?:宜|建议).{0,8}(?:买入|加仓)|维持持有|继续持有", text))
        if action == "SELL":
            return bool(re.search(r"暂不(?:执行)?(?:卖出|减仓|清仓)|不(?:宜|建议).{0,8}(?:卖出|减仓|清仓)|继续持有", text))
        return False

    @staticmethod
    def _is_hard_risk_sell(decision: Dict[str, Any], market_data: Dict[str, Any], strategy_context: Optional[Dict[str, Any]]) -> bool:
        action = str(decision.get("action") or "").upper()
        if action != "SELL":
            return False
        current_price = _float_or_none((market_data or {}).get("current_price"))
        levels = decision.get("monitor_levels") if isinstance(decision.get("monitor_levels"), dict) else {}
        stop_loss = _float_or_none(levels.get("stop_loss"))
        if stop_loss is None and isinstance(strategy_context, dict):
            stop_loss = _float_or_none(strategy_context.get("stop_loss"))
        baseline_relation = str(decision.get("baseline_relation") or "").strip()
        detail = str(decision.get("action_detail") or "").strip()
        reasoning = str(decision.get("reasoning") or "")
        return bool(
            (current_price is not None and stop_loss is not None and current_price <= stop_loss)
            or baseline_relation == "invalidated"
            or detail in {"清仓", "止损", "防守减仓"}
            or any(keyword in reasoning for keyword in ("止损", "破位", "基线失效", "放量转弱"))
        )

    @staticmethod
    def _dynamic_entry_allows_buy(
        *,
        decision: Dict[str, Any],
        market_data: Dict[str, Any],
        strategy_context: Optional[Dict[str, Any]],
        current_price: Optional[float],
        entry_min: Optional[float],
        entry_max: Optional[float],
    ) -> bool:
        if current_price is None or entry_min is None or entry_max is None:
            return False
        if entry_min <= current_price <= entry_max:
            return True
        if current_price < entry_min:
            return False

        context = strategy_context if isinstance(strategy_context, dict) else {}
        final_decision = context.get("final_decision") if isinstance(context.get("final_decision"), dict) else {}
        mode = str(
            decision.get("entry_execution_mode")
            or context.get("entry_execution_mode")
            or final_decision.get("entry_execution_mode")
            or ""
        ).strip().lower()
        swing_mode = str(decision.get("swing_execution_mode") or "").strip().lower()
        if mode not in {"shallow_pullback", "breakout_confirm"}:
            if swing_mode in {"breakout_entry", "breakout_add"}:
                mode = "breakout_confirm"
            elif swing_mode in {"pullback_entry", "pullback_add"}:
                mode = "shallow_pullback"
        if mode not in {"shallow_pullback", "breakout_confirm"}:
            return False

        levels = decision.get("monitor_levels") if isinstance(decision.get("monitor_levels"), dict) else {}
        take_profit = _float_or_none(levels.get("take_profit"))
        if take_profit is None:
            take_profit = _float_or_none(context.get("take_profit") or final_decision.get("take_profit"))
        if take_profit is not None and current_price >= take_profit:
            return False

        atr14 = _float_or_none(
            (market_data or {}).get("atr14")
            or decision.get("atr14")
            or context.get("atr14")
            or final_decision.get("atr14")
        )
        pct_cap = 0.015 if mode == "shallow_pullback" else 0.03
        atr_multiplier = 0.5 if mode == "shallow_pullback" else 1.0
        pct_deviation = entry_max * pct_cap
        max_deviation = min(atr14 * atr_multiplier, pct_deviation) if atr14 and atr14 > 0 else pct_deviation
        return current_price <= entry_max + max_deviation

    def audit(
        self,
        *,
        decision: Dict[str, Any],
        strategy_context: Optional[Dict[str, Any]],
        market_data: Dict[str, Any],
        has_position: bool,
        account_info: Optional[Dict[str, Any]],
        risk_profile: Optional[Dict[str, Any]],
        memory_context: Optional[Dict[str, Any]],
        can_sell_today: bool,
        session_info: Optional[Dict[str, Any]],
        notify: bool,
        trading_hours_only: bool,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        audited = dict(decision or {})
        action = str(audited.get("action") or "").upper()
        flags: List[str] = []
        score = 100.0
        baseline_quality = self._baseline_quality(strategy_context)
        baseline_status = str(baseline_quality.get("status") or "").strip()
        freshness_state = self._freshness_state(market_data)
        is_trade_action = action in {"BUY", "SELL"}
        session_can_trade = bool((session_info or {}).get("can_trade"))
        manual_review_mode = bool(not notify)

        if baseline_status in {"needs_review", "missing"}:
            score -= 35
            _flag(flags, "baseline_needs_review")

        if is_trade_action and session_can_trade and freshness_state != "ready":
            penalty = 40 if freshness_state in {"stale", "unknown", "unavailable"} else 25
            score -= penalty
            _flag(flags, "realtime_not_ready")

        current_price = _float_or_none((market_data or {}).get("current_price"))
        entry_min, entry_max = self._entry_bounds(strategy_context, audited)
        conditions = self._execution_conditions(strategy_context)
        matched_conditions = normalize_condition_list(audited.get("matched_baseline_conditions"), max_items=8)
        unmet_conditions = normalize_condition_list(audited.get("unmet_baseline_conditions"), max_items=8)

        if action == "BUY":
            if current_price is not None and entry_min is not None and entry_max is not None and not (entry_min <= current_price <= entry_max):
                if self._dynamic_entry_allows_buy(
                    decision=audited,
                    market_data=market_data,
                    strategy_context=strategy_context,
                    current_price=current_price,
                    entry_min=entry_min,
                    entry_max=entry_max,
                ):
                    score -= 8
                    _flag(flags, "dynamic_entry_outside_range")
                else:
                    score -= 28
                    _flag(flags, "buy_outside_entry_range")
            if conditions["entry_conditions"] and not matched_conditions:
                score -= 18
                _flag(flags, "entry_conditions_unconfirmed")
            if self._has_memory_chase_risk(memory_context):
                score -= 10
                _flag(flags, "memory_chase_risk")
        elif action == "SELL":
            if has_position and not can_sell_today:
                score -= 40
                _flag(flags, "t1_sell_blocked")
            if conditions["exit_conditions"] or conditions["invalidation_conditions"]:
                if not matched_conditions and not self._is_hard_risk_sell(audited, market_data, strategy_context):
                    score -= 12
                    _flag(flags, "exit_conditions_unconfirmed")

        action_ratio = _float_or_none(audited.get("action_ratio_pct"))
        position_size_limit = _float_or_none((risk_profile or {}).get("position_size_pct"))
        target_position_pct = _float_or_none(audited.get("target_position_pct"))
        total_position_limit = _float_or_none((risk_profile or {}).get("total_position_pct"))
        if is_trade_action and action_ratio is not None and position_size_limit is not None and action_ratio > position_size_limit + 0.01:
            score -= 15
            _flag(flags, "action_ratio_exceeds_single_limit")
        if action == "BUY" and target_position_pct is not None and total_position_limit is not None and target_position_pct > total_position_limit + 0.01:
            score -= 20
            _flag(flags, "target_position_exceeds_total_limit")

        confidence = _float_or_none(audited.get("confidence"))
        if is_trade_action and confidence is not None and confidence < 68:
            score -= 10
            _flag(flags, "low_confidence")
        if self._reasoning_contains_veto(audited.get("reasoning"), action):
            score -= 30
            _flag(flags, "reasoning_action_conflict")

        score = max(0.0, min(100.0, round(score, 1)))
        threshold = BUY_SCORE_THRESHOLD if action == "BUY" else SELL_SCORE_THRESHOLD if action == "SELL" else 0.0
        hard_risk_sell = self._is_hard_risk_sell(audited, market_data, strategy_context)
        veto_reason = ""
        original_action = action

        if is_trade_action:
            if session_can_trade and freshness_state != "ready" and not manual_review_mode:
                veto_reason = "实时行情新鲜度不足，禁止输出可执行买卖信号。"
            elif baseline_status == "needs_review" and not hard_risk_sell and not manual_review_mode:
                veto_reason = "深度分析基线质量需要复核，禁止脱离低质量基线执行买卖。"
            elif score < threshold and not hard_risk_sell and not manual_review_mode:
                veto_reason = f"盘中决策质量分 {score:.1f} 低于 {action} 阈值 {threshold:.0f}。"

        if veto_reason:
            audited["original_action"] = original_action
            audited["action"] = "HOLD"
            audited["action_detail"] = "持有" if has_position else "观望"
            audited["action_ratio_pct"] = None
            audited["trade_intent"] = "hold"
            audited["position_delta_pct"] = 0.0
            audited["risk_level"] = "high" if "新鲜度" in veto_reason or "低质量" in veto_reason else audited.get("risk_level")
            reason = str(audited.get("reasoning") or "").strip()
            appendix = f"审计降级：{veto_reason}"
            audited["reasoning"] = f"{reason}\n\n{appendix}" if reason else appendix
            _flag(flags, "action_vetoed")

        audit = {
            "audit_version": DECISION_AUDIT_VERSION,
            "decision_quality_score": score,
            "quality_flags": flags,
            "veto_reason": veto_reason,
            "original_action": original_action,
            "final_action": audited.get("action"),
            "data_freshness_state": freshness_state,
            "baseline_quality_snapshot": baseline_quality,
            "matched_baseline_conditions": matched_conditions,
            "unmet_baseline_conditions": unmet_conditions,
            "hard_risk_sell": hard_risk_sell,
        }
        audited["decision_audit"] = audit
        return audited, audit
