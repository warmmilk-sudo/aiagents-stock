#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared UI helpers for rendering analysis reports safely."""

from __future__ import annotations

import html
import json
from typing import Any, Dict, Optional

import streamlit as st


A_SHARE_UP_COLOR = "#d14b57"
A_SHARE_DOWN_COLOR = "#2f8f62"
A_SHARE_FLAT_COLOR = "#6b7280"

NON_MARKET_PALETTE = {
    "primary": "#2563eb",
    "secondary": "#d97706",
    "muted": "#475569",
    "teal": "#0f766e",
    "indigo": "#4f46e5",
    "gray": "#64748b",
}


def _safe_text(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _coerce_json_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return ""

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _normalize_mapping_input(value: Any) -> tuple[Dict[str, Any], bool]:
    if not value:
        return {}, False

    if isinstance(value, dict):
        return value, False

    parsed = _coerce_json_value(value)
    if isinstance(parsed, dict):
        return parsed, False

    return {}, True


def _normalize_agents_results(value: Any) -> tuple[Dict[str, Dict[str, Any]], bool]:
    normalized, invalid = _normalize_mapping_input(value)
    if not normalized:
        return {}, invalid

    result: Dict[str, Dict[str, Any]] = {}
    had_invalid_entry = False

    for key, agent_result in normalized.items():
        parsed = _coerce_json_value(agent_result)
        if isinstance(parsed, dict):
            result[str(key)] = parsed
            continue

        if parsed in (None, ""):
            had_invalid_entry = True
            continue

        had_invalid_entry = True
        result[str(key)] = {
            "agent_name": str(key),
            "analysis": str(parsed),
        }

    return result, invalid or had_invalid_entry


def _normalize_text_or_mapping(value: Any) -> tuple[Any, bool]:
    if not value:
        return None, False

    if isinstance(value, dict):
        return value, False

    parsed = _coerce_json_value(value)
    if isinstance(parsed, (dict, str)):
        return parsed, False

    if parsed in (None, ""):
        return None, False

    return json.dumps(parsed, ensure_ascii=False, default=str), True


def _normalize_discussion_result(value: Any) -> Any:
    if not value:
        return ""

    parsed = _coerce_json_value(value)
    if parsed in (None, ""):
        return ""

    return parsed


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def format_price(value: Any, precision: int = 2, currency: str = "¥") -> str:
    number = to_float(value)
    if number is None:
        return str(value)
    return f"{currency}{number:.{precision}f}"


def get_market_color(change_value: Any) -> str:
    number = to_float(change_value)
    if number is None:
        return A_SHARE_FLAT_COLOR
    if number > 0:
        return A_SHARE_UP_COLOR
    if number < 0:
        return A_SHARE_DOWN_COLOR
    return A_SHARE_FLAT_COLOR


def get_recommendation_color(rating: str) -> str:
    text = (rating or "").strip().lower()
    if any(token in text for token in ("买入", "增持", "buy", "add")):
        return NON_MARKET_PALETTE["primary"]
    if any(token in text for token in ("卖出", "减持", "sell", "reduce")):
        return NON_MARKET_PALETTE["muted"]
    return NON_MARKET_PALETTE["secondary"]


def get_action_color(action: str) -> str:
    return {
        "buy": NON_MARKET_PALETTE["primary"],
        "sell": NON_MARKET_PALETTE["muted"],
        "add_position": NON_MARKET_PALETTE["secondary"],
        "reduce_position": NON_MARKET_PALETTE["indigo"],
        "hold": NON_MARKET_PALETTE["gray"],
        "BUY": NON_MARKET_PALETTE["primary"],
        "SELL": NON_MARKET_PALETTE["muted"],
        "HOLD": NON_MARKET_PALETTE["gray"],
    }.get(action, NON_MARKET_PALETTE["gray"])


def render_a_share_change_metric(label: str, change_value: Any, *, precision: int = 2):
    number = to_float(change_value)
    if number is None:
        st.metric(label, f"{change_value}")
        return

    formatted = f"{number:.{precision}f}%"
    st.metric(label, formatted, f"{number:+.{precision}f}%", delta_color="inverse")


def render_stock_info_metrics(
    stock_info: Any,
    *,
    price_label: str = "当前价格",
    change_label: str = "涨跌幅",
):
    normalized_stock_info, invalid = _normalize_mapping_input(stock_info)
    if not normalized_stock_info:
        if invalid:
            st.warning("股票基础信息格式异常，无法展示详细指标。")
            return
        st.info("暂无股票基础信息")
        return

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        current_price = normalized_stock_info.get("current_price", "N/A")
        st.metric(price_label, f"{current_price}")

    with col2:
        render_a_share_change_metric(change_label, normalized_stock_info.get("change_percent", "N/A"))

    with col3:
        st.metric("市盈率", f"{normalized_stock_info.get('pe_ratio', 'N/A')}")

    with col4:
        st.metric("市净率", f"{normalized_stock_info.get('pb_ratio', 'N/A')}")

    with col5:
        market_cap = normalized_stock_info.get("market_cap", "N/A")
        if isinstance(market_cap, (int, float)):
            display = f"{market_cap / 1e9:.2f}B" if market_cap > 1e9 else f"{market_cap / 1e6:.2f}M"
            st.metric("市值", display)
        else:
            st.metric("市值", f"{market_cap}")


def render_agents_analysis_tabs(agents_results: Any):
    st.subheader("AI 分析师团队报告")
    normalized_agents_results, invalid = _normalize_agents_results(agents_results)
    if not normalized_agents_results:
        if invalid:
            st.warning("AI 分析师报告格式异常，无法展示详细内容。")
            return
        st.info("暂无 AI 分析师报告")
        return

    if invalid:
        st.caption("部分分析师报告格式异常，已按文本降级展示。")

    tab_names = []
    tab_contents = []
    for _, agent_result in normalized_agents_results.items():
        tab_names.append(agent_result.get("agent_name", "未知分析师"))
        tab_contents.append(agent_result)

    tabs = st.tabs(tab_names)
    for idx, tab in enumerate(tabs):
        with tab:
            agent_result = tab_contents[idx]
            focus_areas = agent_result.get("focus_areas", [])
            if not isinstance(focus_areas, list):
                focus_areas = [str(focus_areas)]

            st.markdown(
                f"""
                <div class="agent-card">
                    <h4>{_safe_text(agent_result.get('agent_name', '未知'))}</h4>
                    <p><strong>职责:</strong> {_safe_text(agent_result.get('agent_role', '未知'))}</p>
                    <p><strong>关注领域:</strong> {_safe_text(', '.join(str(item) for item in focus_areas))}</p>
                    <p><strong>分析时间:</strong> {_safe_text(agent_result.get('timestamp', '未知'))}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown("**分析报告：**")
            st.write(agent_result.get("analysis", "暂无分析"))


def render_team_discussion(discussion_result: Any):
    st.subheader("分析团队讨论")
    normalized_discussion = _normalize_discussion_result(discussion_result)
    if not normalized_discussion:
        st.info("暂无讨论记录")
        return

    st.markdown(
        """
        <div class="agent-card">
            <h4>团队综合讨论</h4>
            <p>以下内容为多位分析师综合讨论后的结论。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.write(normalized_discussion)


def render_final_decision(final_decision: Any):
    st.subheader("最终投资决策")
    normalized_final_decision, invalid = _normalize_text_or_mapping(final_decision)
    if not normalized_final_decision:
        st.info("暂无最终投资决策")
        return

    if invalid:
        st.caption("最终决策原始数据格式异常，已降级为文本展示。")

    if isinstance(normalized_final_decision, dict) and "decision_text" not in normalized_final_decision:
        col1, col2 = st.columns([1, 2])

        with col1:
            rating = normalized_final_decision.get("rating", "未知")
            rating_color = get_recommendation_color(rating)
            st.markdown(
                f"""
                <div class="decision-card">
                    <h3 style="text-align: center; color: {rating_color};">{_safe_text(rating)}</h3>
                    <h4 style="text-align: center;">投资评级</h4>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.metric("信心度", f"{normalized_final_decision.get('confidence_level', 'N/A')}/10")
            st.metric("目标价格", f"{normalized_final_decision.get('target_price', 'N/A')}")
            st.metric("建议仓位", f"{normalized_final_decision.get('position_size', 'N/A')}")

        with col2:
            st.markdown("**操作建议：**")
            st.write(normalized_final_decision.get("operation_advice", "暂无建议"))
            st.markdown("**关键位置：**")
            left, right = st.columns(2)
            with left:
                st.write(f"**进场区间：** {normalized_final_decision.get('entry_range', 'N/A')}")
                st.write(f"**止盈位：** {normalized_final_decision.get('take_profit', 'N/A')}")
            with right:
                st.write(f"**止损位：** {normalized_final_decision.get('stop_loss', 'N/A')}")
                st.write(f"**持有周期：** {normalized_final_decision.get('holding_period', 'N/A')}")

        risk_warning = normalized_final_decision.get("risk_warning", "")
        if risk_warning:
            st.markdown(
                f"""
                <div class="warning-card">
                    <h4>风险提示</h4>
                    <p>{_safe_text(risk_warning)}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
        return

    if isinstance(normalized_final_decision, dict):
        st.write(normalized_final_decision.get("decision_text", str(normalized_final_decision)))
        return

    st.write(normalized_final_decision)
