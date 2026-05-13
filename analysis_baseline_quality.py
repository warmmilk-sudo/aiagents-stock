from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from investment_action_utils import (
    build_execution_plan,
    extract_first_number,
    normalize_swing_type,
    resolve_entry_range,
)


BASELINE_QUALITY_SCHEMA_VERSION = "baseline_quality_v2"

_POSITION_RATINGS = {"加仓", "持有", "减仓", "卖出"}
_RESEARCH_RATINGS = {"买入", "强烈买入", "观望"}
_ACTIONABLE_RESEARCH_RATINGS = {"买入", "强烈买入"}
_ACTIONABLE_POSITION_RATINGS = {"加仓", "减仓", "卖出"}
_REWARD_RISK_RATINGS = {"买入", "强烈买入", "加仓"}


def _append_flag(flags: list[str], notes: list[str], flag: str, note: str) -> None:
    if flag not in flags:
        flags.append(flag)
    if note and note not in notes:
        notes.append(note)


def _is_missing(value: object) -> bool:
    text = str(value or "").strip()
    return text in {"", "N/A", "None", "null", "nan"}


def _resolve_has_position(stock_info: Optional[Dict[str, Any]], has_position: Optional[bool]) -> bool:
    if has_position is not None:
        return bool(has_position)
    if isinstance(stock_info, dict):
        if stock_info.get("has_position") is not None:
            return bool(stock_info.get("has_position"))
        status = str(stock_info.get("position_status") or "").strip()
        if status:
            return "持仓" in status
    return False


def _reward_risk_ratio(
    *,
    anchor_price: Optional[float],
    take_profit: Optional[float],
    stop_loss: Optional[float],
) -> Optional[float]:
    if not anchor_price or anchor_price <= 0 or take_profit is None or stop_loss is None:
        return None
    upside = float(take_profit) - float(anchor_price)
    downside = float(anchor_price) - float(stop_loss)
    if upside <= 0 or downside <= 0:
        return None
    return upside / downside


def assess_baseline_quality(
    final_decision: Optional[Dict[str, Any]],
    *,
    stock_info: Optional[Dict[str, Any]] = None,
    has_position: Optional[bool] = None,
) -> Dict[str, Any]:
    """Assess whether a deep-analysis result is safe enough to drive intraday execution."""
    decision = final_decision if isinstance(final_decision, dict) else {}
    stock_info = stock_info if isinstance(stock_info, dict) else {}
    resolved_has_position = _resolve_has_position(stock_info, has_position)
    allowed_ratings = _POSITION_RATINGS if resolved_has_position else _RESEARCH_RATINGS
    actionable_ratings = _ACTIONABLE_POSITION_RATINGS if resolved_has_position else _ACTIONABLE_RESEARCH_RATINGS
    flags: list[str] = []
    notes: list[str] = []
    score = 100.0

    rating = str(decision.get("rating") or "").strip()
    if not rating:
        score -= 25
        _append_flag(flags, notes, "missing_rating", "缺少评级。")
    elif rating not in allowed_ratings:
        score -= 30
        _append_flag(flags, notes, "invalid_rating", f"评级 {rating} 不适用于当前仓位状态。")

    entry_min, entry_max = resolve_entry_range(decision)
    take_profit = extract_first_number(decision.get("take_profit"))
    stop_loss = extract_first_number(decision.get("stop_loss"))
    target_price = extract_first_number(decision.get("target_price"))
    current_price = extract_first_number(stock_info.get("current_price"),)
    anchor_price = current_price or ((entry_min + entry_max) / 2 if entry_min is not None and entry_max is not None else entry_min or entry_max)

    if entry_min is None or entry_max is None:
        score -= 15
        _append_flag(flags, notes, "missing_entry_range", "缺少完整进场区间。")
    elif entry_min > entry_max:
        score -= 30
        _append_flag(flags, notes, "invalid_entry_range", "进场区间上下沿反向。")

    if take_profit is None and target_price is not None:
        take_profit = target_price
    if take_profit is None:
        score -= 15
        _append_flag(flags, notes, "missing_take_profit", "缺少止盈位。")
    if stop_loss is None:
        score -= 15
        _append_flag(flags, notes, "missing_stop_loss", "缺少止损位。")

    if not resolved_has_position and all(value is not None for value in (entry_min, entry_max, take_profit, stop_loss)):
        if not (stop_loss < entry_min <= entry_max < take_profit):
            score -= 30
            _append_flag(flags, notes, "invalid_price_relationship", "空仓基线价格关系不满足 止损 < 入场 < 止盈。")
    elif resolved_has_position and take_profit is not None and stop_loss is not None and take_profit <= stop_loss:
        score -= 30
        _append_flag(flags, notes, "invalid_price_relationship", "持仓基线止盈位不高于止损位。")

    swing_type_code = normalize_swing_type(decision.get("swing_type"))
    if resolved_has_position and not swing_type_code:
        score -= 15
        _append_flag(flags, notes, "missing_swing_type", "持仓基线缺少微波段/标准波段。")
    if not resolved_has_position and swing_type_code:
        score -= 5
        _append_flag(flags, notes, "unexpected_swing_type", "空仓基线不应提前锁定持仓波段类型。")

    plan = build_execution_plan(decision)
    entry_conditions = plan.get("entry_conditions") or []
    exit_conditions = plan.get("exit_conditions") or []
    hold_conditions = plan.get("hold_conditions") or []
    invalidation_conditions = plan.get("invalidation_conditions") or []
    execution_plan_complete = bool(entry_conditions or exit_conditions or hold_conditions or invalidation_conditions)
    if rating in actionable_ratings and not execution_plan_complete:
        score -= 18
        _append_flag(flags, notes, "missing_execution_conditions", "动作型评级缺少结构化执行条件。")
    elif not execution_plan_complete:
        score -= 8
        _append_flag(flags, notes, "weak_execution_conditions", "缺少结构化执行条件，盯盘只能弱引用该基线。")

    rr = _reward_risk_ratio(anchor_price=anchor_price, take_profit=take_profit, stop_loss=stop_loss)
    if rating in _REWARD_RISK_RATINGS:
        if rr is None:
            score -= 8
            _append_flag(flags, notes, "reward_risk_unavailable", "收益风险比无法计算。")
        elif rr < 1.0:
            score -= 18
            _append_flag(flags, notes, "poor_reward_risk", f"收益风险比偏低：{rr:.2f}。")
        elif rr < 1.5:
            score -= 8
            _append_flag(flags, notes, "weak_reward_risk", f"收益风险比一般：{rr:.2f}。")

    if current_price is None:
        score -= 8
        _append_flag(flags, notes, "missing_current_price", "缺少当前价格，后续盯盘需以实时价重估。")

    data_source = str(stock_info.get("realtime_data_source") or stock_info.get("data_source") or "").strip()
    if not data_source:
        score -= 4
        _append_flag(flags, notes, "missing_realtime_source", "缺少实时行情来源标记。")

    score = max(0.0, min(100.0, round(score, 1)))
    critical_flags = {"invalid_rating", "invalid_entry_range", "invalid_price_relationship"}
    if critical_flags.intersection(flags) or score < 60:
        status = "needs_review"
    elif score < 75 or flags:
        status = "incomplete"
    else:
        status = "healthy"

    return {
        "schema_version": BASELINE_QUALITY_SCHEMA_VERSION,
        "status": status,
        "score": score,
        "quality_flags": flags,
        "notes": notes,
        "has_position": resolved_has_position,
        "rating": rating,
        "allowed_ratings": sorted(allowed_ratings),
        "entry_min": entry_min,
        "entry_max": entry_max,
        "take_profit": take_profit,
        "stop_loss": stop_loss,
        "reward_risk_ratio": round(rr, 2) if rr is not None else None,
        "execution_plan_complete": execution_plan_complete,
        "execution_condition_counts": {
            "entry": len(entry_conditions),
            "exit": len(exit_conditions),
            "hold": len(hold_conditions),
            "invalidation": len(invalidation_conditions),
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
