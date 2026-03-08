import streamlit as st

from monitor_db import monitor_db


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
    )


def jump_to_price_alert_workspace(symbol: str):
    view_keys = [
        'show_history',
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
    st.session_state.smart_monitor_active_tab = 'price_alert'
    st.session_state.monitor_jump_highlight = symbol
