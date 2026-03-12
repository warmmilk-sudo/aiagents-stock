#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主力选股批量分析结果展示模块
"""

from __future__ import annotations

import re
from typing import Dict, List

import pandas as pd
import streamlit as st

from ui_shared import get_dataframe_height


def build_batch_results_from_record(record: Dict) -> Dict:
    results = list(record.get("results") or [])
    batch_count = int(record.get("batch_count") or len(results))
    success_count = int(record.get("success_count") or sum(1 for item in results if item.get("success")))
    failed_count = int(record.get("failed_count") or max(batch_count - success_count, 0))
    return {
        "results": results,
        "total": batch_count,
        "success": success_count,
        "failed": failed_count,
        "elapsed_time": float(record.get("total_time") or 0),
        "analysis_mode": record.get("analysis_mode") or "unknown",
        "analysis_date": record.get("analysis_date") or "",
        "saved_to_history": True,
        "history_record_id": record.get("id"),
    }


def summarize_batch_record(record: Dict) -> str:
    successful_results = [item for item in (record.get("results") or []) if item.get("success")]
    highlighted: List[str] = []
    for item in successful_results[:3]:
        stock_info = item.get("stock_info") or {}
        final_decision = item.get("final_decision") or {}
        symbol = item.get("symbol") or stock_info.get("symbol") or "-"
        name = stock_info.get("name") or stock_info.get("股票名称") or symbol
        rating = final_decision.get("rating") or final_decision.get("investment_rating")
        highlighted.append(f"{name}({symbol}){f' {rating}' if rating else ''}")

    summary_parts = [
        f"成功 {record.get('success_count', 0)}/{record.get('batch_count', 0)} 只",
        f"模式 {record.get('analysis_mode') or '-'}",
    ]
    if highlighted:
        summary_parts.append("重点: " + "、".join(highlighted))
    return " | ".join(summary_parts)


def render_batch_results(
    batch_results: Dict,
    *,
    key_prefix: str = "main_force_batch",
    show_heading: bool = True,
    show_saved_status: bool = True,
) -> None:
    results = list(batch_results.get("results") or [])
    total = int(batch_results.get("total") or len(results))
    success = int(batch_results.get("success") or sum(1 for item in results if item.get("success")))
    failed = int(batch_results.get("failed") or max(total - success, 0))
    elapsed_time = float(batch_results.get("elapsed_time") or 0)
    saved_to_history = bool(batch_results.get("saved_to_history"))
    save_error = batch_results.get("save_error")
    analysis_date = str(batch_results.get("analysis_date") or "").strip()
    analysis_mode = str(batch_results.get("analysis_mode") or "").strip()

    if show_heading:
        st.markdown("## 批量分析结果")

    if analysis_date or analysis_mode:
        meta_parts = []
        if analysis_date:
            meta_parts.append(f"生成时间: {analysis_date}")
        if analysis_mode:
            meta_parts.append(f"分析模式: {analysis_mode}")
        st.caption(" | ".join(meta_parts))

    if show_saved_status:
        if saved_to_history:
            st.success("分析结果已自动保存到历史记录，可点击右上角“批量分析历史”查看。")
        elif save_error:
            st.warning(f"历史记录保存失败: {save_error}，但结果仍可查看。")

    st.markdown("---")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("总计分析", f"{total} 只")
    with col2:
        success_ratio = f"{(success / total * 100):.1f}%" if total else "0.0%"
        st.metric("成功分析", f"{success} 只", delta=success_ratio)
    with col3:
        st.metric("失败分析", f"{failed} 只")
    with col4:
        st.metric("总耗时", f"{elapsed_time / 60:.1f} 分钟")

    successful_results = [item for item in results if item.get("success")]
    if successful_results:
        st.markdown("---")
        st.markdown(f"### 成功分析的股票 ({len(successful_results)} 只)")

        display_rows = []
        for item in successful_results:
            stock_info = item.get("stock_info") or {}
            final_decision = item.get("final_decision") or {}
            display_rows.append(
                {
                    "股票代码": stock_info.get("symbol") or item.get("symbol") or "",
                    "股票名称": stock_info.get("name") or stock_info.get("股票名称") or "",
                    "评级": final_decision.get("rating") or "未知",
                    "信心度": final_decision.get("confidence_level") or "N/A",
                    "进场区间": final_decision.get("entry_range") or "N/A",
                    "止盈位": final_decision.get("take_profit") or "N/A",
                    "止损位": final_decision.get("stop_loss") or "N/A",
                    "目标价": final_decision.get("target_price") or "N/A",
                }
            )

        df_display = pd.DataFrame(display_rows)
        for column in ("信心度", "止盈位", "止损位", "目标价"):
            if column in df_display.columns:
                df_display[column] = pd.to_numeric(df_display[column], errors="coerce")
        for column in ("股票代码", "股票名称", "评级", "进场区间"):
            if column in df_display.columns:
                df_display[column] = df_display[column].astype(str)

        st.dataframe(
            df_display,
            width="content",
            height=get_dataframe_height(len(df_display), max_rows=40),
        )

        st.markdown("---")
        st.markdown("### 详细分析报告")
        for index, item in enumerate(successful_results):
            stock_info = item.get("stock_info") or {}
            final_decision = item.get("final_decision") or {}
            symbol = stock_info.get("symbol") or item.get("symbol") or ""
            name = stock_info.get("name") or stock_info.get("股票名称") or symbol
            rating = final_decision.get("rating") or "未知"

            with st.expander(f"{symbol} - {name} | {rating}", expanded=False):
                metric_col1, metric_col2, metric_col3 = st.columns(3)
                with metric_col1:
                    st.metric("信心度", final_decision.get("confidence_level") or "N/A")
                with metric_col2:
                    st.metric("进场区间", final_decision.get("entry_range") or "N/A")
                with metric_col3:
                    st.metric("目标价", final_decision.get("target_price") or "N/A")

                price_col1, price_col2 = st.columns(2)
                with price_col1:
                    st.metric("止盈位", final_decision.get("take_profit") or "N/A")
                with price_col2:
                    st.metric("止损位", final_decision.get("stop_loss") or "N/A")

                st.markdown("#### 投资建议")
                advice = final_decision.get("operation_advice") or final_decision.get("advice") or "暂无建议"
                st.info(advice)

                if st.button("加入价格预警", key=f"{key_prefix}_monitor_{symbol}_{index}", width="content"):
                    entry_range = final_decision.get("entry_range") or ""
                    entry_min, entry_max = None, None
                    if isinstance(entry_range, str) and "-" in entry_range:
                        try:
                            parts = entry_range.split("-")
                            entry_min = float(parts[0].strip())
                            entry_max = float(parts[1].strip())
                        except (TypeError, ValueError):
                            entry_min, entry_max = None, None

                    take_profit = None
                    take_profit_text = final_decision.get("take_profit") or ""
                    if take_profit_text:
                        try:
                            numbers = re.findall(r"\d+\.?\d*", str(take_profit_text))
                            if numbers:
                                take_profit = float(numbers[0])
                        except (TypeError, ValueError):
                            take_profit = None

                    stop_loss = None
                    stop_loss_text = final_decision.get("stop_loss") or ""
                    if stop_loss_text:
                        try:
                            numbers = re.findall(r"\d+\.?\d*", str(stop_loss_text))
                            if numbers:
                                stop_loss = float(numbers[0])
                        except (TypeError, ValueError):
                            stop_loss = None

                    from price_alert_service import create_price_alert, jump_to_price_alert_workspace

                    try:
                        entry_range_payload = {}
                        if entry_min is not None and entry_max is not None:
                            entry_range_payload = {"min": entry_min, "max": entry_max}

                        create_price_alert(
                            symbol=symbol,
                            name=name,
                            rating=rating,
                            entry_range=entry_range_payload or None,
                            take_profit=take_profit,
                            stop_loss=stop_loss,
                        )
                        jump_to_price_alert_workspace(symbol)
                        st.success(f"{symbol} - {name} 已加入价格预警。")
                    except Exception as exc:
                        st.error(f"添加失败: {exc}")
    else:
        st.info("本次批量分析没有成功结果。")

    failed_results = [item for item in results if not item.get("success")]
    if failed_results:
        st.markdown("---")
        st.markdown(f"### 分析失败的股票 ({len(failed_results)} 只)")
        df_failed = pd.DataFrame(
            [
                {
                    "股票代码": item.get("symbol") or "",
                    "失败原因": item.get("error") or "未知错误",
                }
                for item in failed_results
            ]
        )
        st.dataframe(df_failed, width="content")
