from __future__ import annotations

from typing import Any, Dict, Optional

from investment_action_utils import extract_first_number, resolve_entry_range


_POSITION_RATINGS = ("增持", "持有", "减持", "卖出")
_RESEARCH_RATINGS = ("买入", "强烈买入", "观望")

_HIGH_RISK_KEYWORDS = (
    "清仓",
    "卖出",
    "减仓",
    "破位",
    "跌破",
    "高风险",
    "重大风险",
    "不确定性高",
    "回撤较大",
    "规避",
)
_MEDIUM_RISK_KEYWORDS = (
    "止损",
    "压力位",
    "震荡",
    "波动",
    "等待",
    "观察",
    "留意",
    "分歧",
)
_HIGH_CONVICTION_POSITION_KEYWORDS = ("重仓", "高仓位", "大仓位")
_LOW_CONVICTION_POSITION_KEYWORDS = ("轻仓", "试仓", "小仓位")


def _clean_rating_label(value: Any) -> str:
    text = str(value or "").strip()
    return text


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _round_half(value: float) -> float:
    return round(value * 2) / 2


def _risk_level(text: str) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return "unknown"
    if any(keyword in normalized for keyword in _HIGH_RISK_KEYWORDS):
        return "high"
    if any(keyword in normalized for keyword in _MEDIUM_RISK_KEYWORDS):
        return "medium"
    return "low"


def _resolve_anchor_price(final_decision: Dict[str, Any], stock_info: Optional[Dict[str, Any]]) -> Optional[float]:
    current_price = extract_first_number((stock_info or {}).get("current_price"))
    entry_min, entry_max = resolve_entry_range(final_decision)
    if current_price is not None and current_price > 0:
        return current_price
    if entry_min is not None and entry_max is not None:
        return (entry_min + entry_max) / 2
    return entry_min if entry_min is not None else entry_max


def _position_confidence_adjustment(position_size: Any) -> float:
    text = str(position_size or "").strip()
    if not text:
        return 0.0
    if any(keyword in text for keyword in _HIGH_CONVICTION_POSITION_KEYWORDS):
        return 0.4
    if any(keyword in text for keyword in _LOW_CONVICTION_POSITION_KEYWORDS):
        return -0.2
    return 0.1


def calibrate_final_decision(
    final_decision: Optional[Dict[str, Any]],
    *,
    stock_info: Optional[Dict[str, Any]] = None,
    has_position: Optional[bool] = None,
) -> Dict[str, Any]:
    payload = dict(final_decision or {})

    raw_rating_text = payload.get("rating") or payload.get("investment_rating") or ""
    raw_confidence_value = payload.get("confidence_level")
    raw_rating = _clean_rating_label(raw_rating_text)
    raw_confidence = extract_first_number(raw_confidence_value)
    effective_has_position = bool(stock_info.get("has_position")) if has_position is None and isinstance(stock_info, dict) else bool(has_position)
    allowed_ratings = _POSITION_RATINGS if effective_has_position else _RESEARCH_RATINGS
    anchor_price = _resolve_anchor_price(payload, stock_info)
    target_price = extract_first_number(payload.get("target_price"))
    take_profit = extract_first_number(payload.get("take_profit")) or target_price
    stop_loss = extract_first_number(payload.get("stop_loss"))
    entry_min, entry_max = resolve_entry_range(payload)
    risk_text = " ".join(
        str(payload.get(key) or "").strip()
        for key in ("risk_warning", "operation_advice", "summary")
    ).strip()
    risk_level = _risk_level(risk_text)

    upside_ratio = None
    downside_ratio = None
    reward_risk_ratio = None
    if anchor_price and anchor_price > 0:
        if take_profit is not None:
            upside_ratio = (take_profit - anchor_price) / anchor_price
        if stop_loss is not None:
            downside_ratio = (anchor_price - stop_loss) / anchor_price
        if upside_ratio is not None and downside_ratio is not None and upside_ratio > 0 and downside_ratio > 0:
            reward_risk_ratio = upside_ratio / downside_ratio

    calibrated_rating = raw_rating if raw_rating else ("持有" if effective_has_position else "观望")
    rating_valid = calibrated_rating in allowed_ratings
    if not rating_valid:
        calibrated_rating = "持有" if effective_has_position else "观望"

    evidence_score = 5.0
    for key in ("target_price", "take_profit", "stop_loss", "holding_period", "position_size"):
        if str(payload.get(key) or "").strip():
            evidence_score += 0.25
    if entry_min is not None or entry_max is not None:
        evidence_score += 0.5
    if str(payload.get("operation_advice") or "").strip():
        evidence_score += 0.3
    if upside_ratio is not None:
        if upside_ratio >= 0.12:
            evidence_score += 0.9
        elif upside_ratio >= 0.06:
            evidence_score += 0.5
        elif upside_ratio <= 0.02:
            evidence_score -= 0.4
    if reward_risk_ratio is not None:
        if reward_risk_ratio >= 2.0:
            evidence_score += 1.1
        elif reward_risk_ratio >= 1.2:
            evidence_score += 0.5
        elif reward_risk_ratio < 0.8:
            evidence_score -= 0.9
    if downside_ratio is not None and downside_ratio > 0.12:
        evidence_score -= 0.6
    if risk_level == "high":
        evidence_score -= 1.1
    elif risk_level == "medium":
        evidence_score -= 0.5
    else:
        evidence_score += 0.1
    evidence_score += _position_confidence_adjustment(payload.get("position_size"))

    if raw_confidence is not None:
        calibrated_confidence = (raw_confidence * 0.45) + (evidence_score * 0.55)
    else:
        calibrated_confidence = evidence_score
    if raw_rating and not rating_valid:
        calibrated_confidence -= 0.7
    calibrated_confidence = _round_half(_clamp(calibrated_confidence, 1.0, 10.0))

    notes = []
    if raw_rating and not rating_valid:
        notes.append(
            f"模型原始评级为{raw_rating}，不在当前状态允许集合{','.join(allowed_ratings)}内，已回退为{calibrated_rating}"
        )
    if raw_confidence is not None:
        notes.append(f"模型原始信心度为{raw_confidence:g}分，已按统一规则重算")
    if reward_risk_ratio is not None:
        notes.append(f"收益风险比约为{reward_risk_ratio:.2f}")
    if risk_level != "unknown":
        notes.append(f"风险级别判定为{risk_level}")

    payload["rating"] = calibrated_rating
    payload["confidence_level"] = calibrated_confidence
    payload["calibration_version"] = "rule_v1"
    payload["raw_model_rating"] = raw_rating_text
    if raw_confidence_value not in (None, ""):
        payload["raw_model_confidence_level"] = raw_confidence_value
    payload["calibration_notes"] = notes
    return payload
