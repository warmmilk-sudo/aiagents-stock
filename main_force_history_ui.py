#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主力选股批量分析历史记录 UI 模块
"""

from __future__ import annotations

import streamlit as st

from main_force_batch_db import batch_db
from main_force_batch_view import (
    build_batch_results_from_record,
    render_batch_results,
    summarize_batch_record,
)


def display_batch_history():
    """显示批量分析历史记录。"""

    back_col, spacer_col = st.columns([1, 4])
    with back_col:
        if st.button("返回主页面", width="stretch"):
            st.session_state.main_force_view_history = False
            st.rerun()

    st.markdown("## 主力选股批量分析历史记录")
    st.caption("历史记录会在当前位置展开完整批量分析结果，展示方式与智策历史报告保持一致。")

    try:
        stats = batch_db.get_statistics()
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("总记录数", f"{stats['total_records']} 条")
        with col2:
            st.metric("分析股票总数", f"{stats['total_stocks_analyzed']} 只")
        with col3:
            st.metric("成功分析", f"{stats['total_success']} 只")
        with col4:
            st.metric("成功率", f"{stats['success_rate']}%")
        with col5:
            st.metric("平均耗时", f"{stats['average_time']:.1f} 秒")
    except Exception as exc:
        st.warning(f"无法获取统计信息: {exc}")

    st.markdown("---")

    try:
        history_records = batch_db.get_all_history(limit=50)
        if not history_records:
            st.info("暂无批量分析历史记录。")
            return

        st.success(f"共找到 {len(history_records)} 条历史记录。")
        for index, record in enumerate(history_records):
            record_id = record.get("id")
            analysis_date = record.get("analysis_date") or "未知时间"
            expander_label = f"{analysis_date} | {summarize_batch_record(record)}"

            with st.expander(expander_label, expanded=index == 0):
                action_col1, action_col2 = st.columns([1, 1])
                with action_col1:
                    if st.button("删除", key=f"main_force_history_delete_{record_id}", width="stretch"):
                        if batch_db.delete_record(record_id):
                            st.success("历史记录已删除。")
                            st.rerun()
                        st.error("删除失败。")
                with action_col2:
                    if st.button("加载到当前结果", key=f"main_force_history_load_{record_id}", width="stretch"):
                        st.session_state.main_force_batch_results = build_batch_results_from_record(record)
                        st.session_state.main_force_view_history = False
                        st.success("已加载到当前结果页。")
                        st.rerun()

                render_batch_results(
                    build_batch_results_from_record(record),
                    key_prefix=f"main_force_history_{record_id}",
                    show_heading=False,
                    show_saved_status=False,
                )
    except Exception as exc:
        st.error(f"获取历史记录失败: {exc}")
