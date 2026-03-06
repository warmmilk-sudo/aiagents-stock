"""Navigation helpers for Streamlit page routing."""

from __future__ import annotations

from typing import Dict, Iterable, Mapping, MutableMapping

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - test environments without Streamlit
    class _StreamlitFallback:
        session_state = {}

    st = _StreamlitFallback()

DEFAULT_PAGE = "home"

PAGE_KEYS = (
    "home",
    "sector_strategy",
    "smart_monitor",
    "main_force",
    "news_flow",
    "history",
    "monitor",
    "portfolio",
    "longhubang",
    "macro_cycle",
    "config",
    "low_price_bull",
    "small_cap",
    "profit_growth",
    "value_stock",
)
VALID_PAGES = frozenset(PAGE_KEYS)

LEGACY_FLAG_TO_PAGE: Dict[str, str] = {
    "show_history": "history",
    "show_monitor": "monitor",
    "show_main_force": "main_force",
    "show_low_price_bull": "low_price_bull",
    "show_small_cap": "small_cap",
    "show_profit_growth": "profit_growth",
    "show_value_stock": "value_stock",
    "show_sector_strategy": "sector_strategy",
    "show_longhubang": "longhubang",
    "show_smart_monitor": "smart_monitor",
    "show_portfolio": "portfolio",
    "show_news_flow": "news_flow",
    "show_macro_cycle": "macro_cycle",
    "show_config": "config",
}

PAGE_TO_LEGACY_FLAG = {page: flag for flag, page in LEGACY_FLAG_TO_PAGE.items()}


def normalize_page(page: str | None) -> str:
    """Normalize any candidate page value to a valid page key."""
    if isinstance(page, str):
        value = page.strip().lower()
        if value in VALID_PAGES:
            return value
    return DEFAULT_PAGE


def _extract_query_param_value(raw_value: object) -> str | None:
    """Extract query param value from Streamlit query APIs."""
    if raw_value is None:
        return None
    if isinstance(raw_value, str):
        return raw_value
    if isinstance(raw_value, Iterable) and not isinstance(raw_value, (bytes, bytearray, dict)):
        for item in raw_value:
            if isinstance(item, str):
                return item
    return None


def get_query_page(st_module=st) -> str | None:
    """Return the raw `page` query param if present."""
    if hasattr(st_module, "query_params"):
        try:
            return _extract_query_param_value(st_module.query_params.get("page"))
        except Exception:
            return None
    try:
        params = st_module.experimental_get_query_params()
    except Exception:
        return None
    return _extract_query_param_value(params.get("page"))


def set_query_page(page: str, st_module=st) -> None:
    """Set the page query param across Streamlit versions."""
    target = normalize_page(page)
    if hasattr(st_module, "query_params"):
        try:
            st_module.query_params["page"] = target
            return
        except Exception:
            pass
    try:
        st_module.experimental_set_query_params(page=target)
    except Exception:
        # Non-fatal for environments without query APIs.
        return


def clear_legacy_flags(session_state: MutableMapping[str, object]) -> None:
    """Remove all legacy `show_*` flags from session state."""
    for flag in LEGACY_FLAG_TO_PAGE:
        session_state.pop(flag, None)


def derive_page_from_legacy_flags(session_state: Mapping[str, object]) -> str | None:
    """Find first enabled legacy flag and map it to a page key."""
    for flag, page in LEGACY_FLAG_TO_PAGE.items():
        if bool(session_state.get(flag)):
            return page
    return None


def sync_legacy_flags(session_state: MutableMapping[str, object], page: str) -> None:
    """Keep exactly one legacy flag in sync with the active page."""
    normalized = normalize_page(page)
    clear_legacy_flags(session_state)
    flag = PAGE_TO_LEGACY_FLAG.get(normalized)
    if flag:
        session_state[flag] = True


def resolve_current_page(
    session_state: MutableMapping[str, object] | None = None,
    st_module=st,
) -> str:
    """
    Resolve current page with priority:
    URL query param > session_state.current_page > legacy flags > default.
    """
    state = session_state if session_state is not None else st_module.session_state

    query_page = normalize_page(get_query_page(st_module))
    if query_page != DEFAULT_PAGE or get_query_page(st_module):
        selected = query_page
    else:
        selected = normalize_page(state.get("current_page")) if isinstance(state.get("current_page"), str) else DEFAULT_PAGE
        if selected == DEFAULT_PAGE:
            legacy_page = derive_page_from_legacy_flags(state)
            if legacy_page:
                selected = legacy_page

    state["current_page"] = selected
    set_query_page(selected, st_module=st_module)
    sync_legacy_flags(state, selected)
    return selected


def navigate_to(
    page: str,
    session_state: MutableMapping[str, object] | None = None,
    st_module=st,
) -> str:
    """Navigate to a page by updating session + query + legacy flags."""
    state = session_state if session_state is not None else st_module.session_state
    target = normalize_page(page)
    state["current_page"] = target
    set_query_page(target, st_module=st_module)
    sync_legacy_flags(state, target)
    return target
