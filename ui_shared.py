#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared UI helpers for rendering analysis reports safely."""

from __future__ import annotations

import html
import json
import re
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


def get_dataframe_height(
    row_count: int,
    *,
    min_rows: int = 4,
    max_rows: int = 50,
    row_px: int = 35,
    header_px: int = 38,
    padding_px: int = 8,
) -> int:
    """Return an expanded dataframe height so most tables use page scrolling."""
    try:
        rows = max(int(row_count), 0)
    except (TypeError, ValueError):
        rows = 0

    visible_rows = min(max(rows, min_rows), max_rows)
    return header_px + visible_rows * row_px + padding_px


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

    if isinstance(parsed, str):
        parsed = re.sub(r"^\s*[【\[]?推理过程[】\]]?\s*", "", parsed, count=1)
        parsed = re.sub(r"^\s*推理过程[:：]\s*", "", parsed, count=1)
        parsed = parsed.strip()

    return parsed


STRUCTURED_FINAL_DECISION_KEYS = (
    "rating",
    "confidence_level",
    "target_price",
    "operation_advice",
    "entry_range",
    "take_profit",
    "stop_loss",
    "holding_period",
    "position_size",
    "risk_warning",
)


def _extract_embedded_json_mapping(text: str) -> tuple[Dict[str, Any], str]:
    if not text:
        return {}, ""

    decoder = json.JSONDecoder()
    candidate_mapping: Dict[str, Any] = {}
    candidate_prefix = ""

    for index, char in enumerate(text):
        if char != "{":
            continue

        try:
            parsed, end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue

        trailing = text[index + end :].strip()
        if trailing:
            continue
        if isinstance(parsed, dict):
            candidate_mapping = parsed
            candidate_prefix = text[:index].strip()

    return candidate_mapping, candidate_prefix


def _resolve_final_decision_content(final_decision: Any) -> tuple[Any, bool, str]:
    normalized_final_decision, invalid = _normalize_text_or_mapping(final_decision)
    extracted_reasoning = ""

    if isinstance(normalized_final_decision, dict):
        has_structured_keys = any(
            key in normalized_final_decision for key in STRUCTURED_FINAL_DECISION_KEYS
        )
        decision_text = str(normalized_final_decision.get("decision_text") or "").strip()
        if decision_text and not has_structured_keys:
            embedded_mapping, reasoning_prefix = _extract_embedded_json_mapping(decision_text)
            if embedded_mapping:
                extracted_reasoning = reasoning_prefix
                return embedded_mapping, invalid, extracted_reasoning

    return normalized_final_decision, invalid, extracted_reasoning


def _find_report_body_start(text: str) -> Optional[int]:
    report_patterns = (
        r"(?m)^#\s*.+(?:分析报告|报告).*$",
        r"(?m)^##\s*基本概况.*$",
        r"(?m)^(?:一、|1[\.、])\s*(?:趋势分析|基本概况|核心结论|技术分析|投资建议).*$",
        r"(?m)^(?:##\s*)?(?:一、|1[\.、])\s*(?:周期仪表盘|康波周期仪表盘|综合资产配置建议|不同人群的具体建议|核心观点总结|周金涛名言对照).*$",
    )
    positions = []
    for pattern in report_patterns:
        match = re.search(pattern, text)
        if match:
            positions.append(match.start())
    if not positions:
        return None
    return min(positions)


def _split_analysis_report_sections(value: Any) -> tuple[str, str]:
    parsed = _coerce_json_value(value)
    text = "" if parsed is None else str(parsed).strip()
    if not text:
        return "", ""

    marker = re.search(r"[\[【]推理过程[\]】]", text)
    if not marker:
        return text, ""

    before_marker = text[:marker.start()].strip()
    after_marker = text[marker.end():].lstrip("：:\n ").strip()

    if before_marker:
        body = before_marker
        reasoning = after_marker
    else:
        report_start = _find_report_body_start(after_marker)
        if report_start is not None and report_start > 0:
            reasoning = after_marker[:report_start].strip()
            body = after_marker[report_start:].strip()
        else:
            body = ""
            reasoning = after_marker

    body = re.sub(r"^\s*分析报告(?:正文)?\s*[:：]\s*", "", body, count=1)
    reasoning = re.sub(r"^\s*[\[【]?推理过程[\]】]?\s*", "", reasoning, count=1)
    reasoning = re.sub(r"^\s*推理过程[:：]\s*", "", reasoning, count=1)
    return body.strip(), reasoning.strip()


def _format_multiline_text_as_html(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""

    paragraphs = [segment.strip() for segment in re.split(r"\n{2,}", text) if segment.strip()]
    if not paragraphs:
        return ""

    rendered = []
    for paragraph in paragraphs:
        rendered.append(f"<p>{_safe_text(paragraph).replace(chr(10), '<br>')}</p>")
    return "".join(rendered)


def _render_reasoning_block(title: str, content: Any, *, description: str = ""):
    html_body = _format_multiline_text_as_html(content)
    if not html_body:
        return

    description_html = ""
    if description:
        description_html = (
            f'<div class="reasoning-section__description">{_safe_text(description)}</div>'
        )

    st.markdown(
        f"""
        <div class="reasoning-section">
            <div class="reasoning-section__title">{_safe_text(title)}</div>
            {description_html}
        </div>
        <div class="reasoning-body">
            {html_body}
        </div>
        """,
        unsafe_allow_html=True,
    )


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


def format_entry_range(entry_min: Any, entry_max: Any, *, precision: int = 2, currency: str = "¥") -> str:
    min_value = to_float(entry_min)
    max_value = to_float(entry_max)
    if min_value is None and max_value is None:
        return "N/A"
    if min_value is None:
        return format_price(max_value, precision=precision, currency=currency)
    if max_value is None:
        return format_price(min_value, precision=precision, currency=currency)
    return (
        f"{format_price(min_value, precision=precision, currency=currency)} - "
        f"{format_price(max_value, precision=precision, currency=currency)}"
    )


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


def render_agents_analysis_tabs(
    agents_results: Any,
    *,
    show_header: bool = True,
    preferred_order: Optional[list[str]] = None,
    tab_labels: Optional[Dict[str, str]] = None,
    include_other_agents: bool = True,
    split_reasoning: bool = False,
):
    if show_header:
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

    ordered_keys: list[str] = []
    if preferred_order:
        for key in preferred_order:
            if key in normalized_agents_results:
                ordered_keys.append(key)
        if include_other_agents:
            ordered_keys.extend(
                key for key in normalized_agents_results.keys() if key not in ordered_keys
            )
    else:
        ordered_keys = list(normalized_agents_results.keys())

    if not ordered_keys:
        st.info("暂无 AI 分析师报告")
        return

    tab_names = []
    tab_contents = []
    for key in ordered_keys:
        agent_result = normalized_agents_results[key]
        tab_names.append((tab_labels or {}).get(key, agent_result.get("agent_name", "未知分析师")))
        tab_contents.append(agent_result)

    tabs = st.tabs(tab_names)
    for idx, tab in enumerate(tabs):
        with tab:
            agent_result = tab_contents[idx]
            focus_areas = agent_result.get("focus_areas", [])
            if not isinstance(focus_areas, list):
                focus_areas = [str(focus_areas)]
            analysis_time = agent_result.get("analysis_time") or agent_result.get("timestamp") or "未知"

            st.markdown(
                f"""
                <div class="agent-card">
                    <h4>{_safe_text(agent_result.get('agent_name', '未知'))}</h4>
                    <p><strong>职责:</strong> {_safe_text(agent_result.get('agent_role', '未知'))}</p>
                    <p><strong>关注领域:</strong> {_safe_text(', '.join(str(item) for item in focus_areas))}</p>
                    <p><strong>分析时间:</strong> {_safe_text(analysis_time)}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if split_reasoning:
                report_body, reasoning_text = _split_analysis_report_sections(
                    agent_result.get("analysis", "")
                )
                st.markdown("**分析报告正文：**")
                if report_body:
                    st.markdown(report_body)
                elif reasoning_text:
                    st.info("未提取到结构化报告正文，已保留原始推理过程。")
                else:
                    st.info("暂无分析")

                if reasoning_text:
                    with st.expander("推理过程", expanded=False):
                        _render_reasoning_block("分析师推理过程", reasoning_text)
            else:
                st.markdown("**分析报告：**")
                st.write(agent_result.get("analysis", "暂无分析"))


def render_team_discussion(discussion_result: Any, *, show_header: bool = True):
    if show_header:
        st.subheader("分析团队讨论")

    normalized_discussion = _normalize_discussion_result(discussion_result)
    if not normalized_discussion:
        st.info("暂无讨论记录")
        return

    _render_reasoning_block(
        "团队综合讨论",
        normalized_discussion,
        description="以下内容为多位分析师综合讨论后的原始记录。",
    )


def render_reasoning_process(
    agents_results: Any,
    discussion_result: Any,
    *,
    expanded: bool = False,
    include_agents: bool = True,
    include_discussion: bool = True,
    extra_sections: Optional[list[tuple[str, Any]]] = None,
):
    normalized_agents_results, _ = _normalize_agents_results(agents_results)
    normalized_discussion = _normalize_discussion_result(discussion_result)
    normalized_extra_sections = []

    if not include_agents:
        normalized_agents_results = {}
    if not include_discussion:
        normalized_discussion = ""
    for title, content in extra_sections or []:
        parsed_content = _coerce_json_value(content)
        if parsed_content in (None, ""):
            continue
        normalized_extra_sections.append((title, str(parsed_content).strip()))

    if not normalized_agents_results and not normalized_discussion and not normalized_extra_sections:
        return

    if normalized_discussion and not normalized_agents_results and not normalized_extra_sections:
        expander_title = "团队综合讨论"
    elif normalized_discussion and normalized_extra_sections and not normalized_agents_results:
        expander_title = "团队综合讨论与决策推理"
    elif normalized_agents_results and not normalized_discussion and not normalized_extra_sections:
        expander_title = "分析师推理过程"
    else:
        expander_title = "推理过程详情"

    with st.expander(expander_title, expanded=expanded):
        if normalized_agents_results and (normalized_discussion or normalized_extra_sections):
            st.caption("这里保留完整的团队讨论与分析师原始推理内容，默认折叠。")
        elif normalized_discussion or normalized_extra_sections:
            st.caption("这里保留完整的团队讨论与决策推理原始内容，默认折叠。")
        else:
            st.caption("这里保留分析师原始推理内容，默认折叠。")

        if normalized_discussion:
            render_team_discussion(normalized_discussion, show_header=False)

        for index, (title, content) in enumerate(normalized_extra_sections):
            if normalized_discussion or index > 0:
                st.markdown("---")
            _render_reasoning_block(title, content)

        if normalized_agents_results:
            if normalized_discussion or normalized_extra_sections:
                st.markdown("---")
            st.markdown("#### 分析师原始报告")
            render_agents_analysis_tabs(normalized_agents_results, show_header=False)


def render_final_decision(final_decision: Any, *, show_header: bool = True):
    if show_header:
        st.subheader("最终投资决策")

    normalized_final_decision, invalid, _ = _resolve_final_decision_content(final_decision)
    if not normalized_final_decision:
        st.info("暂无最终投资决策")
        return

    if invalid:
        st.caption("最终决策原始数据格式异常，已降级为文本展示。")

    if isinstance(normalized_final_decision, dict) and "decision_text" not in normalized_final_decision:
        entry_range_display = normalized_final_decision.get("entry_range")
        if not entry_range_display:
            entry_range_display = format_entry_range(
                normalized_final_decision.get("entry_min"),
                normalized_final_decision.get("entry_max"),
            )
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
                st.write(f"**进场区间：** {entry_range_display}")
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
