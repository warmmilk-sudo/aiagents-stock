from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from investment_db_utils import DEFAULT_ACCOUNT_NAME


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


def build_strategy_context(
    final_decision: Optional[Dict[str, Any]],
    *,
    origin_analysis_id: Optional[int] = None,
    summary: Optional[str] = None,
    analysis_scope: str = "research",
    analysis_source: str = "manual",
) -> Dict[str, Any]:
    final_decision = final_decision or {}
    entry_min, entry_max = parse_entry_range(final_decision.get("entry_range"))
    take_profit = extract_first_number(final_decision.get("take_profit"))
    stop_loss = extract_first_number(final_decision.get("stop_loss"))
    normalized_summary = str(
        summary
        or final_decision.get("operation_advice")
        or final_decision.get("summary")
        or final_decision.get("advice")
        or ""
    ).strip()

    return {
        "origin_analysis_id": origin_analysis_id,
        "analysis_scope": analysis_scope,
        "analysis_source": analysis_source,
        "rating": str(final_decision.get("rating") or "持有").strip() or "持有",
        "entry_min": entry_min,
        "entry_max": entry_max,
        "take_profit": take_profit,
        "stop_loss": stop_loss,
        "summary": normalized_summary,
        "final_decision": final_decision,
    }


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
