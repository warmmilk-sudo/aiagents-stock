import streamlit as st

from investment_db_utils import DEFAULT_ACCOUNT_NAME
from monitor_db import monitor_db
from ui_state_keys import (
    INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY,
    MONITOR_JUMP_HIGHLIGHT_KEY,
    SMART_MONITOR_ACTIVE_TAB_KEY,
)


def create_price_alert(
    *,
    symbol: str,
    name: str,
    rating: str,
    entry_range: dict,
    take_profit=None,
    stop_loss=None,
    check_interval: int = 30,
    notification_enabled: bool = True,
    quant_enabled: bool = False,
    quant_config=None,
    managed_by_portfolio: bool = False,
    account_name: str = DEFAULT_ACCOUNT_NAME,
    origin_analysis_id=None,
) -> int:
    return monitor_db.add_monitored_stock(
        symbol=symbol,
        name=name,
        rating=rating,
        entry_range=entry_range,
        take_profit=take_profit,
        stop_loss=stop_loss,
        check_interval=check_interval,
        notification_enabled=notification_enabled,
        quant_enabled=quant_enabled,
        quant_config=quant_config,
        managed_by_portfolio=managed_by_portfolio,
        account_name=account_name,
        origin_analysis_id=origin_analysis_id,
    )


def create_price_alert_from_analysis(
    *,
    symbol: str,
    name: str,
    entry_min: float,
    entry_max: float,
    rating: str,
    take_profit=None,
    stop_loss=None,
    check_interval: int = 30,
    notification_enabled: bool = True,
    account_name: str = DEFAULT_ACCOUNT_NAME,
    origin_analysis_id=None,
) -> int:
    return create_price_alert(
        symbol=symbol,
        name=name,
        rating=rating,
        entry_range={"min": entry_min, "max": entry_max},
        take_profit=take_profit,
        stop_loss=stop_loss,
        check_interval=check_interval,
        notification_enabled=notification_enabled,
        account_name=account_name,
        origin_analysis_id=origin_analysis_id,
    )


def jump_to_price_alert_workspace(symbol: str):
    view_keys = [
        'show_history',
        'show_deep_analysis',
        'show_monitor_service',
        'show_monitor',
        'show_main_force',
        'show_low_price_bull',
        'show_small_cap',
        'show_profit_growth',
        'show_value_stock',
        'show_sector_strategy',
        'show_longhubang',
        'show_smart_monitor',
        'show_portfolio',
        'show_news_flow',
        'show_macro_cycle',
        'show_config',
    ]
    for key in view_keys:
        st.session_state.pop(key, None)
    st.session_state.show_smart_monitor = True
    st.session_state[INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY] = "price_alert"
    st.session_state[SMART_MONITOR_ACTIVE_TAB_KEY] = "price_alert"
    st.session_state[MONITOR_JUMP_HIGHLIGHT_KEY] = symbol
