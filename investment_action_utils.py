from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from investment_db_utils import DEFAULT_ACCOUNT_NAME
from prompt_registry import render_prompt


_SWING_TYPE_ALIASES = {
    "micro_swing": "micro_swing",
    "微波段": "micro_swing",
    "超短": "micro_swing",
    "超短线": "micro_swing",
    "标准波段": "standard_swing",
    "波段": "standard_swing",
    "短中期": "standard_swing",
    "standard_swing": "standard_swing",
}

_SWING_TYPE_LABELS = {
    "micro_swing": "微波段",
    "standard_swing": "标准波段",
}

_SWING_TYPE_DEFAULTS = {
    "micro_swing": {
        "days_min": 2,
        "days_max": 5,
        "days_text": "2-5个交易日",
        "style_summary": "动量突破 / 事件驱动 / 快进快出",
        "exit_style": "快进快出，失败即撤，兑现优先",
        "execution_preference": "优先验证突破或事件驱动是否继续发酵，只吃流动性最好、确定性最高的那一段鱼身。",
        "reason": "当前更适合微波段，优先做高确定性的突破或事件催化，失速就撤。",
    },
    "standard_swing": {
        "days_min": 5,
        "days_max": 15,
        "days_text": "5-15个交易日",
        "style_summary": "动量突破 + 均值回归 / 板块轮动配合",
        "exit_style": "固定止盈区结合结构强弱动态上修",
        "execution_preference": "优先围绕阶段性主升浪或反弹主段做波段管理，兼顾突破确认、回踩低吸和板块资金共振。",
        "reason": "当前更适合标准波段，目标是捕捉阶段性主升浪或反弹趋势中的核心波段。",
    },
}

_STYLE_KEYWORDS = (
    ("momentum_breakout", "动量突破", ("突破", "放量突破", "趋势确认", "右侧", "新高", "加速上行")),
    ("mean_reversion", "均值回归", ("均值回归", "回踩", "箱体", "高抛低吸", "震荡区间", "回归均值")),
    ("sector_rotation", "板块轮动", ("板块轮动", "轮动", "资金共振", "主线", "资金面共振", "板块共振")),
    ("event_driven", "事件驱动", ("事件驱动", "政策利好", "政策催化", "公告催化", "突发", "消息驱动")),
    ("trailing_stop", "移动止盈", ("移动止盈", "Trailing Stop", "trailing stop", "上调离场底线", "抬高离场底线", "跟踪止盈")),
    ("strict_stop", "刚性止损", ("刚性止损", "严格止损", "止损纪律", "风控优先", "严格风控")),
)


def extract_first_number(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value)
    token = []
    numbers = []
    for char in text:
        if char.isdigit() or char in {".", "-"}:
            token.append(char)
            continue
        if token:
            numbers.append("".join(token))
            token = []
    if token:
        numbers.append("".join(token))

    for candidate in numbers:
        try:
            return float(candidate)
        except ValueError:
            continue
    return None


def parse_entry_range(value: Any) -> Tuple[Optional[float], Optional[float]]:
    if isinstance(value, dict):
        return extract_first_number(value.get("min")), extract_first_number(value.get("max"))

    if value in (None, ""):
        return None, None

    text = str(value).replace("~", "-").replace("至", "-").replace("到", "-")
    parts = [segment.strip() for segment in text.split("-") if segment.strip()]
    numbers = [extract_first_number(part) for part in parts]
    numbers = [number for number in numbers if number is not None]
    if len(numbers) >= 2:
        return numbers[0], numbers[1]

    number = extract_first_number(value)
    return number, number


def resolve_entry_range(final_decision: Optional[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    final_decision = final_decision or {}
    entry_min = extract_first_number(final_decision.get("entry_min"))
    entry_max = extract_first_number(final_decision.get("entry_max"))
    if entry_min is not None or entry_max is not None:
        return entry_min, entry_max
    return parse_entry_range(final_decision.get("entry_range"))


def normalize_swing_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text in _SWING_TYPE_ALIASES:
        return _SWING_TYPE_ALIASES[text]
    for alias, normalized in _SWING_TYPE_ALIASES.items():
        if alias and alias.lower() in text:
            return normalized
    return ""


def swing_type_label(value: Any) -> str:
    normalized = normalize_swing_type(value)
    return _SWING_TYPE_LABELS.get(normalized, str(value or "").strip())


def _duration_unit_to_trading_days(amount: float, unit: str) -> int:
    normalized_unit = str(unit or "").strip()
    if normalized_unit in {"个交易日", "交易日", "天"}:
        return max(1, int(round(amount)))
    if normalized_unit == "周":
        return max(1, int(round(amount * 5)))
    if normalized_unit in {"个月", "月"}:
        return max(1, int(round(amount * 20)))
    return max(1, int(round(amount)))


def _parse_holding_period_range(text: str) -> Tuple[Optional[int], Optional[int]]:
    normalized = str(text or "").strip()
    if not normalized:
        return None, None

    range_match = re.search(
        r"(?P<start>\d+(?:\.\d+)?)\s*(?P<unit1>个?交易日|天|周|个月|月)?\s*(?:-|~|至|到)\s*(?P<end>\d+(?:\.\d+)?)\s*(?P<unit2>个?交易日|天|周|个月|月)?",
        normalized,
    )
    if range_match:
        start_unit = range_match.group("unit1") or range_match.group("unit2") or "天"
        end_unit = range_match.group("unit2") or range_match.group("unit1") or "天"
        start_days = _duration_unit_to_trading_days(float(range_match.group("start")), start_unit)
        end_days = _duration_unit_to_trading_days(float(range_match.group("end")), end_unit)
        return min(start_days, end_days), max(start_days, end_days)

    single_match = re.search(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>个?交易日|天|周|个月|月)", normalized)
    if single_match:
        days = _duration_unit_to_trading_days(float(single_match.group("value")), single_match.group("unit"))
        return days, days

    return None, None


def _infer_strategy_style_tags(text: str) -> List[str]:
    blob = str(text or "")
    labels: List[str] = []
    for _code, label, keywords in _STYLE_KEYWORDS:
        if any(keyword in blob for keyword in keywords):
            labels.append(label)
    return labels


def infer_strategy_profile(
    *,
    holding_period: Any = None,
    swing_type: Any = None,
    swing_type_reason: Any = None,
    summary: Any = None,
    final_decision: Optional[Dict[str, Any]] = None,
    period: Any = None,
) -> Dict[str, Any]:
    final_decision = final_decision if isinstance(final_decision, dict) else {}
    holding_period_text = str(
        holding_period
        or final_decision.get("holding_period")
        or period
        or ""
    ).strip()
    explicit_swing_type = normalize_swing_type(
        swing_type
        or final_decision.get("swing_type")
        or final_decision.get("swing_strategy_type")
    )
    source_text = " ".join(
        part
        for part in (
            holding_period_text,
            str(swing_type or "").strip(),
            str(summary or "").strip(),
            str(final_decision.get("operation_advice") or "").strip(),
            str(final_decision.get("risk_warning") or "").strip(),
            str(final_decision.get("summary") or "").strip(),
        )
        if part
    )
    days_min, days_max = _parse_holding_period_range(holding_period_text or source_text)
    style_tags = _infer_strategy_style_tags(source_text)
    swing_type_code = explicit_swing_type

    if swing_type_code:
        defaults = _SWING_TYPE_DEFAULTS[swing_type_code]
        if not style_tags:
            default_style_summary = defaults["style_summary"]
            style_tags = [part.strip() for part in default_style_summary.split("/") if part.strip()]
        baseline_exit_style = defaults["exit_style"]
    else:
        defaults = {}
        baseline_exit_style = ""

    style_summary = " / ".join(style_tags)
    if "移动止盈" in style_tags:
        baseline_exit_style = "移动止盈为主，随着价格抬升不断上调离场底线"
    elif "刚性止损" in style_tags and swing_type_code == "micro_swing":
        baseline_exit_style = "快进快出 + 刚性止损，失败即撤"

    execution_parts: List[str] = []
    if swing_type_code == "micro_swing":
        execution_parts.append("优先验证突破或事件驱动是否继续发酵，失速则快速兑现")
    elif swing_type_code == "standard_swing":
        execution_parts.append("优先围绕阶段性主升或反弹主段执行，不情绪化追涨杀跌")

    if "动量突破" in style_tags:
        execution_parts.append("更看重放量突破、趋势确认和右侧跟随")
    if "均值回归" in style_tags:
        execution_parts.append("更重视回踩支撑和箱体边界，不追高")
    if "板块轮动" in style_tags:
        execution_parts.append("同步观察板块强弱与资金共振")
    if "事件驱动" in style_tags:
        execution_parts.append("催化的新鲜度和延续性需要更高权重")
    if "移动止盈" in style_tags:
        execution_parts.append("盈利段更适合用移动止盈抬高离场底线")

    effective_days_min = days_min or defaults.get("days_min")
    effective_days_max = days_max or defaults.get("days_max")
    days_text = (
        holding_period_text
        or (
            f"{effective_days_min}-{effective_days_max}个交易日"
            if effective_days_min is not None and effective_days_max is not None and effective_days_min != effective_days_max
            else f"{effective_days_min}个交易日"
            if effective_days_min is not None
            else ""
        )
        or defaults.get("days_text", "")
    )
    normalized_reason = str(
        swing_type_reason
        or final_decision.get("swing_type_reason")
        or defaults.get("reason", "")
    ).strip()

    return {
        "holding_period": holding_period_text or days_text,
        "swing_type": _SWING_TYPE_LABELS.get(swing_type_code, ""),
        "swing_type_code": swing_type_code,
        "swing_type_reason": normalized_reason,
        "swing_horizon_label": _SWING_TYPE_LABELS.get(swing_type_code, ""),
        "swing_horizon_days_min": effective_days_min,
        "swing_horizon_days_max": effective_days_max,
        "swing_horizon_days_text": days_text,
        "strategy_style_tags": style_tags,
        "strategy_style_summary": style_summary,
        "baseline_exit_style": baseline_exit_style,
        "intraday_execution_preference": "；".join(dict.fromkeys(execution_parts)),
    }


def normalize_strategy_context(strategy_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(strategy_context, dict):
        return {}

    normalized = dict(strategy_context)
    final_decision = normalized.get("final_decision")
    final_decision = final_decision if isinstance(final_decision, dict) else {}
    profile = infer_strategy_profile(
        holding_period=normalized.get("holding_period"),
        swing_type=normalized.get("swing_type"),
        swing_type_reason=normalized.get("swing_type_reason"),
        summary=normalized.get("summary"),
        final_decision=final_decision,
        period=normalized.get("period"),
    )
    for key, value in profile.items():
        normalized[key] = value
    return normalized


def build_holding_strategy_prompt_block(
    *,
    has_position: bool,
    strategy_context: Optional[Dict[str, Any]] = None,
    is_initial_holding_analysis: bool = False,
) -> str:
    if not has_position:
        return render_prompt("stock_analysis/sections/holding_strategy_no_position.txt")

    normalized = normalize_strategy_context(strategy_context or {})
    if is_initial_holding_analysis:
        return render_prompt("stock_analysis/sections/holding_strategy_initial_position.txt")

    if normalized:
        swing_type = str(normalized.get("swing_type") or "未明确").strip() or "未明确"
        horizon = str(
            normalized.get("swing_horizon_days_text")
            or normalized.get("holding_period")
            or "未明确"
        ).strip() or "未明确"
        style_summary = str(normalized.get("strategy_style_summary") or "未明确").strip() or "未明确"
        execution_preference = str(
            normalized.get("intraday_execution_preference")
            or normalized.get("summary")
            or "未明确"
        ).strip() or "未明确"
        return render_prompt(
            "stock_analysis/sections/holding_strategy_existing_baseline.txt",
            swing_type=swing_type,
            horizon=horizon,
            style_summary=style_summary,
            execution_preference=execution_preference,
        )

    return render_prompt("stock_analysis/sections/holding_strategy_missing_baseline.txt")


def build_strategy_context(
    final_decision: Optional[Dict[str, Any]],
    *,
    origin_analysis_id: Optional[int] = None,
    summary: Optional[str] = None,
    analysis_scope: str = "research",
    analysis_source: str = "manual",
) -> Dict[str, Any]:
    final_decision = final_decision or {}
    entry_min, entry_max = resolve_entry_range(final_decision)
    take_profit = extract_first_number(final_decision.get("take_profit"))
    stop_loss = extract_first_number(final_decision.get("stop_loss"))
    normalized_summary = str(
        summary
        or final_decision.get("operation_advice")
        or final_decision.get("summary")
        or final_decision.get("advice")
        or ""
    ).strip()

    context = {
        "origin_analysis_id": origin_analysis_id,
        "analysis_scope": analysis_scope,
        "analysis_source": analysis_source,
        "rating": str(final_decision.get("rating") or "持有").strip() or "持有",
        "entry_min": entry_min,
        "entry_max": entry_max,
        "take_profit": take_profit,
        "stop_loss": stop_loss,
        "summary": normalized_summary,
        "holding_period": str(final_decision.get("holding_period") or "").strip(),
        "swing_type": str(final_decision.get("swing_type") or final_decision.get("swing_strategy_type") or "").strip(),
        "swing_type_reason": str(final_decision.get("swing_type_reason") or "").strip(),
        "final_decision": final_decision,
    }
    return normalize_strategy_context(context)


def suggest_entry_price(strategy_context: Optional[Dict[str, Any]]) -> float:
    strategy_context = strategy_context or {}
    entry_min = extract_first_number(strategy_context.get("entry_min"))
    entry_max = extract_first_number(strategy_context.get("entry_max"))
    if entry_min is not None and entry_max is not None:
        return round((entry_min + entry_max) / 2, 3)
    if entry_max is not None:
        return round(entry_max, 3)
    if entry_min is not None:
        return round(entry_min, 3)
    return 0.0


def build_portfolio_note(strategy_context: Optional[Dict[str, Any]]) -> str:
    strategy_context = strategy_context or {}
    rating = str(strategy_context.get("rating") or "持有").strip() or "持有"
    summary = str(strategy_context.get("summary") or "").strip()
    if summary:
        return f"[{rating}] {summary}"
    return f"[{rating}] 来源于单点分析"


def build_analysis_action_payload(
    *,
    symbol: str,
    stock_name: str,
    final_decision: Optional[Dict[str, Any]],
    origin_analysis_id: Optional[int] = None,
    summary: Optional[str] = None,
    account_name: str = DEFAULT_ACCOUNT_NAME,
    analysis_scope: str = "research",
    analysis_source: str = "manual",
) -> Dict[str, Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_name = str(stock_name or normalized_symbol).strip() or normalized_symbol
    strategy_context = build_strategy_context(
        final_decision,
        origin_analysis_id=origin_analysis_id,
        summary=summary,
        analysis_scope=analysis_scope,
        analysis_source=analysis_source,
    )
    return {
        "symbol": normalized_symbol,
        "stock_name": normalized_name,
        "account_name": account_name or DEFAULT_ACCOUNT_NAME,
        "origin_analysis_id": origin_analysis_id,
        "strategy_context": strategy_context,
        "default_cost_price": suggest_entry_price(strategy_context),
        "default_note": build_portfolio_note(strategy_context),
    }
