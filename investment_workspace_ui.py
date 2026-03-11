from __future__ import annotations

import streamlit as st

from monitor_manager import (
    display_monitor_status,
    display_monitoring_registry,
    display_notification_management,
    display_price_alert_workspace,
    display_recent_monitor_events,
)
from portfolio_ui import display_portfolio_manager
from smart_monitor_ui import (
    _ensure_smart_monitor_runtime,
    render_ai_monitor_tasks_panel,
    render_history,
    render_realtime_analysis,
    render_settings,
)
from ui_state_keys import INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY


WORKSPACE_TABS = {
    "ai_monitor": "智能盯盘",
    "price_alert": "价格预警",
    "portfolio": "持仓",
    "activity": "成交与事件",
}


def set_investment_workspace_tab(tab_key: str) -> None:
    if tab_key in WORKSPACE_TABS:
        st.session_state[INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY] = tab_key


def _render_ai_monitor_panel() -> None:
    subtab = st.radio(
        "智能盯盘视图",
        ["任务管理", "实时分析", "设置"],
        horizontal=True,
        key="investment_workspace_ai_subtab",
        label_visibility="collapsed",
    )
    if subtab == "任务管理":
        render_ai_monitor_tasks_panel()
        return
    if subtab == "实时分析":
        render_realtime_analysis()
        return
    render_settings()


def _render_activity_panel() -> None:
    st.header("成交与事件")
    display_monitor_status()
    history_tab, registry_tab, event_tab, notification_tab = st.tabs(
        ["交易与决策", "监控注册表", "最近事件", "通知中心"]
    )
    with history_tab:
        render_history()
    with registry_tab:
        display_monitoring_registry()
    with event_tab:
        display_recent_monitor_events()
    with notification_tab:
        display_notification_management(key_prefix="investment_workspace_activity")


def display_investment_workspace(lightweight_model=None, reasoning_model=None) -> None:
    _ensure_smart_monitor_runtime(lightweight_model, reasoning_model)

    current_tab = st.session_state.get(INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY, "portfolio")
    if current_tab not in WORKSPACE_TABS:
        current_tab = "portfolio"
        st.session_state[INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY] = current_tab

    labels = list(WORKSPACE_TABS.values())
    current_label = WORKSPACE_TABS[current_tab]
    selected_label = st.radio(
        "投资工作台",
        labels,
        index=labels.index(current_label),
        horizontal=True,
        key="investment_workspace_tab_selector",
        label_visibility="collapsed",
    )
    selected_tab = next(
        key for key, label in WORKSPACE_TABS.items() if label == selected_label
    )
    st.session_state[INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY] = selected_tab

    if selected_tab == "ai_monitor":
        _render_ai_monitor_panel()
        return

    if selected_tab == "price_alert":
        display_price_alert_workspace()
        return

    if selected_tab == "portfolio":
        display_portfolio_manager(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        return

    _render_activity_panel()
