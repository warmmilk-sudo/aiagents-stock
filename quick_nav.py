"""Helpers for lightweight mobile quick navigation bridging."""

from __future__ import annotations

from typing import MutableMapping

NAV_FLAGS = (
    "show_history",
    "show_monitor",
    "show_config",
    "show_main_force",
    "show_sector_strategy",
    "show_longhubang",
    "show_portfolio",
    "show_low_price_bull",
    "show_news_flow",
    "show_macro_cycle",
    "show_smart_monitor",
    "show_small_cap",
    "show_profit_growth",
    "show_value_stock",
)

QUICK_TO_FLAG = {
    "sector_strategy": "show_sector_strategy",
    "smart_monitor": "show_smart_monitor",
    "news_flow": "show_news_flow",
    "config": "show_config",
}

VALID_QUICK = frozenset({"home", *QUICK_TO_FLAG.keys()})


def normalize_quick(value: object) -> str | None:
    """Normalize `quick` query value to supported key."""
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in VALID_QUICK:
            return normalized
    return None


def clear_nav_flags(state: MutableMapping[str, object]) -> None:
    """Remove all legacy show flags from state."""
    for key in NAV_FLAGS:
        state.pop(key, None)


def apply_quick_to_state(state: MutableMapping[str, object], quick_value: object) -> bool:
    """
    Apply a quick-nav key to legacy session flags.

    Returns True if quick_value is valid and was applied.
    """
    quick = normalize_quick(quick_value)
    if quick is None:
        return False

    clear_nav_flags(state)
    target_flag = QUICK_TO_FLAG.get(quick)
    if target_flag:
        state[target_flag] = True
    return True


def clear_quick_param(params: MutableMapping[str, object]) -> None:
    """Remove `quick` key from query-like mapping."""
    params.pop("quick", None)
