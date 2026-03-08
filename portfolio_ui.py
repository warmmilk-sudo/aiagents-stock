#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
持仓管理UI模块
提供持仓股票的增删改查、批量分析、定时任务管理界面
"""

import html
import re
import uuid
import streamlit as st
import pandas as pd
from datetime import date, datetime
from typing import List, Dict

from portfolio_analysis_tasks import portfolio_analysis_task_manager
from portfolio_manager import portfolio_manager
from portfolio_scheduler import portfolio_scheduler
from ui_shared import (
    _resolve_final_decision_content,
    get_recommendation_color,
    render_agents_analysis_tabs,
    render_final_decision,
    render_reasoning_process,
)


AGENT_OPTIONS = [
    ("technical", "技术分析"),
    ("fundamental", "基本面"),
    ("fund_flow", "资金面"),
    ("risk", "风险管理"),
    ("sentiment", "市场情绪"),
    ("news", "新闻事件"),
]


def _render_static_table(df: pd.DataFrame) -> None:
    """Render small summary tables without the heavier dataframe grid."""
    st.table(df.reset_index(drop=True).style.hide(axis="index"))


def _format_percent(value, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{float(value) * 100:.{digits}f}%"


def _format_ratio(value, digits: int = 3) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.{digits}f}"


def _get_portfolio_account_options(stocks: List[Dict]) -> List[str]:
    accounts = sorted({stock.get("account_name", "默认账户") for stock in stocks})
    return ["全部账户"] + accounts


def _resolve_portfolio_account_filter(selected_account: str) -> str | None:
    return None if selected_account == "全部账户" else selected_account


def format_price(value) -> str:
    """统一价格显示精度为三位小数。"""
    try:
        return f"¥{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)


def _escape_text(value) -> str:
    return html.escape("" if value is None else str(value))


def _format_history_entry_range(entry_min, entry_max) -> str:
    if entry_min is None or entry_max is None:
        return "N/A"
    return f"{format_price(entry_min)} - {format_price(entry_max)}"


def _build_history_final_decision_display(
    record: Dict,
    final_decision: Dict,
    *,
    normalized_rating: str,
) -> Dict:
    if not isinstance(final_decision, dict):
        final_decision = {}

    summary = str(record.get("summary") or "").strip()
    summary = re.sub(
        r"^(?:投资)?评级\s*[:：]\s*(?:买入|持有|卖出|鎸佹湁|涔板叆|鍗栧嚭)?(?:[；;，,。]\s*|\s+)?",
        "",
        summary,
        count=1,
    ).strip(" ；;，,。")
    display = dict(final_decision)
    display.setdefault("rating", normalized_rating)
    display.setdefault("confidence_level", record.get("confidence", "N/A"))
    display.setdefault("target_price", record.get("target_price", "N/A"))
    display.setdefault("operation_advice", summary or "暂无最终结论摘要")
    display.setdefault(
        "entry_range",
        final_decision.get("entry_range") or _format_history_entry_range(
            record.get("entry_min"),
            record.get("entry_max"),
        ),
    )
    display.setdefault("take_profit", record.get("take_profit", "N/A"))
    display.setdefault("stop_loss", record.get("stop_loss", "N/A"))
    display.setdefault("holding_period", final_decision.get("holding_period", "N/A"))
    display.setdefault("position_size", final_decision.get("position_size", "N/A"))
    display.setdefault("risk_warning", final_decision.get("risk_warning", ""))
    return display


PORTFOLIO_ANALYSIS_SESSION_KEY = "portfolio_analysis_session_id"


def _ensure_portfolio_analysis_session_id() -> str:
    session_id = st.session_state.get(PORTFOLIO_ANALYSIS_SESSION_KEY)
    if not session_id:
        session_id = uuid.uuid4().hex
        st.session_state[PORTFOLIO_ANALYSIS_SESSION_KEY] = session_id
    return session_id

def _format_task_time(timestamp) -> str:
    if not timestamp:
        return ""
    try:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return ""


def _get_portfolio_analysis_task(task_type: str | None = None) -> Dict | None:
    session_id = _ensure_portfolio_analysis_session_id()
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_latest_task(session_id, task_type=task_type)


def _get_pending_portfolio_analysis_tasks(task_type: str | None = None) -> List[Dict]:
    session_id = _ensure_portfolio_analysis_session_id()
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_pending_tasks(session_id, task_type=task_type)


def _get_active_portfolio_analysis_task() -> Dict | None:
    session_id = _ensure_portfolio_analysis_session_id()
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_active_task(session_id)


def _get_running_portfolio_analysis_task() -> Dict | None:
    session_id = _ensure_portfolio_analysis_session_id()
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_running_task(session_id)


def _get_stock_pending_analysis_task(stock_id) -> Dict | None:
    for task in _get_pending_portfolio_analysis_tasks("single"):
        metadata = task.get("metadata") or {}
        if metadata.get("stock_id") == stock_id:
            return task
    return None


def _render_portfolio_analysis_task_notice():
    pending_tasks = _get_pending_portfolio_analysis_tasks()
    if not pending_tasks:
        return

    running_task = _get_running_portfolio_analysis_task()
    active_task = running_task or pending_tasks[0]
    task_label = active_task.get("label") or "当前分析任务"
    message = active_task.get("message") or "分析正在后台执行，切换页面后状态会保留。"
    current = active_task.get("current") or 0
    total = active_task.get("total") or 0
    suffix = f" ({current}/{total})" if total else ""
    queued_count = sum(1 for task in pending_tasks if task.get("status") == "queued")

    if running_task:
        queue_text = f" 队列中还有 {queued_count} 个任务。" if queued_count else ""
        st.info(f"{task_label}：{message}{suffix}{queue_text}")
    else:
        st.info(f"分析队列中有 {queued_count} 个任务，等待执行。")


def _start_single_stock_analysis_task(stock: Dict, lightweight_model=None, reasoning_model=None):
    session_id = _ensure_portfolio_analysis_session_id()
    code = stock.get("code", "")
    name = stock.get("name", code)
    stock_id = stock.get("id")
    existing_task = _get_stock_pending_analysis_task(stock_id)
    if existing_task:
        status_text = "正在分析" if existing_task.get("status") == "running" else "已在队列中"
        raise RuntimeError(f"{code} {name} {status_text}，请勿重复提交。")

    def runner(_task_id, report_progress):
        report_progress(
            current=0,
            total=1,
            step_code=code,
            step_status="analyzing",
            message=f"正在分析 {code} {name}",
        )
        result = portfolio_manager.analyze_single_stock(
            code,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        if not result.get("success"):
            raise RuntimeError(f"{code} 分析失败: {result.get('error', '未知错误')}")

        wrapped_result = {
            "success": True,
            "results": [
                {
                    "code": code,
                    "result": result,
                }
            ],
        }
        persistence_result = portfolio_manager.persist_analysis_results(
            wrapped_result,
            sync_realtime_monitor=True,
            analysis_source="portfolio_single_analysis",
            analysis_period="1y",
        )
        saved_count = len(persistence_result.get("saved_ids", []))
        sync_result = persistence_result.get("sync_result") or {}
        report_progress(
            current=1,
            total=1,
            step_code=code,
            step_status="success",
            message=f"{code} 分析完成，已保存 {saved_count} 条记录",
        )
        return {
            "task_type": "single",
            "stock_id": stock_id,
            "stock_code": code,
            "stock_name": name,
            "saved_count": saved_count,
            "sync_result": sync_result,
            "analysis_result": result,
        }

    portfolio_analysis_task_manager.start_task(
        session_id,
        task_type="single",
        label=f"{name} 分析任务",
        runner=runner,
        metadata={"stock_id": stock_id, "stock_code": code, "stock_name": name},
    )


def _build_batch_progress_message(current, total, code, status) -> str:
    status_map = {
        "analyzing": "正在分析",
        "success": "已完成",
        "failed": "失败",
        "error": "异常",
    }
    base = status_map.get(status, "处理中")
    if total:
        return f"{base} {code} ({current}/{total})"
    return f"{base} {code}"


def _start_batch_analysis_task(
    stocks: List[Dict],
    *,
    analysis_mode: str,
    max_workers: int,
    selected_agents: List[str] | None,
    auto_sync: bool,
    send_notification: bool,
    lightweight_model=None,
    reasoning_model=None,
):
    session_id = _ensure_portfolio_analysis_session_id()
    total = len(stocks)
    pending_batch_tasks = _get_pending_portfolio_analysis_tasks("batch")
    if pending_batch_tasks:
        active_batch_task = pending_batch_tasks[0]
        status_text = "正在分析" if active_batch_task.get("status") == "running" else "已在队列中"
        raise RuntimeError(f"批量分析任务{status_text}，请勿重复提交。")

    def runner(_task_id, report_progress):
        report_progress(
            current=0,
            total=total,
            step_status="analyzing",
            message=f"正在批量分析 {total} 只持仓股票",
        )

        def progress_callback(current, callback_total, code, status):
            report_progress(
                current=current,
                total=callback_total,
                step_code=code,
                step_status=status,
                message=_build_batch_progress_message(current, callback_total, code, status),
            )

        result = portfolio_manager.batch_analyze_portfolio(
            mode=analysis_mode,
            max_workers=max_workers,
            selected_agents=selected_agents,
            progress_callback=progress_callback,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        if not result.get("success"):
            raise RuntimeError(result.get("error", "批量分析失败"))

        persistence_result = portfolio_manager.persist_analysis_results(
            result,
            sync_realtime_monitor=auto_sync,
            analysis_source="portfolio_batch_analysis",
            analysis_period="1y",
        )

        if send_notification:
            from notification_service import notification_service

            notification_service.send_portfolio_analysis_notification(
                result,
                persistence_result.get("sync_result") if auto_sync else None,
            )

        report_progress(
            current=result.get("total", total),
            total=result.get("total", total) or total or 1,
            step_status="success",
            message=(
                f"批量分析完成：成功 {result.get('succeeded', 0)}，"
                f"失败 {result.get('failed', 0)}"
            ),
        )
        return {
            "task_type": "batch",
            "analysis_result": result,
            "persistence_result": persistence_result,
            "selected_agents": selected_agents,
            "auto_sync": auto_sync,
            "send_notification": send_notification,
        }

    portfolio_analysis_task_manager.start_task(
        session_id,
        task_type="batch",
        label="持仓批量分析任务",
        runner=runner,
        metadata={
            "stock_count": total,
            "analysis_mode": analysis_mode,
            "selected_agents": selected_agents or portfolio_manager.DEFAULT_ANALYSIS_AGENTS,
        },
    )


def _render_single_analysis_feedback():
    if _get_active_portfolio_analysis_task():
        return
    task = _get_portfolio_analysis_task("single")
    if not task or task.get("status") in {"queued", "running"}:
        return

    stock_code = task.get("metadata", {}).get("stock_code") or task.get("result", {}).get("stock_code") or "该股票"
    if task.get("status") == "success":
        result = task.get("result") or {}
        saved_count = result.get("saved_count", 0)
        sync_result = result.get("sync_result") or {}
        message = task.get("message") or f"{stock_code} 分析完成"
        st.success(f"{message}，已写入 {saved_count} 条分析记录。")
        if sync_result.get("total", 0) > 0:
            st.caption(
                f"监测同步：新增 {sync_result.get('added', 0)}，更新 {sync_result.get('updated', 0)}。"
            )
    elif task.get("error"):
        st.error(task["error"])


def _render_batch_analysis_feedback():
    task = _get_portfolio_analysis_task("batch")
    if not task:
        return

    if task.get("status") in {"queued", "running"}:
        pending_batch_tasks = _get_pending_portfolio_analysis_tasks("batch")
        queue_position = 0
        for index, pending_task in enumerate(pending_batch_tasks, start=1):
            if pending_task.get("id") == task.get("id"):
                queue_position = index
                break
        if task.get("status") == "running":
            started_at = _format_task_time(task.get("started_at") or task.get("created_at"))
            if started_at:
                st.caption(f"批量任务启动时间：{started_at}")
        elif queue_position:
            st.caption(f"批量分析已进入队列，当前排在第 {queue_position} 位。")
        return

    if task.get("status") == "failed":
        st.error(task.get("error") or "批量分析失败")
        return

    result_bundle = task.get("result") or {}
    result = result_bundle.get("analysis_result") or {}
    persistence_result = result_bundle.get("persistence_result") or {}
    sync_result = persistence_result.get("sync_result") or {}
    started_at = _format_task_time(task.get("started_at") or task.get("created_at"))
    finished_at = _format_task_time(task.get("finished_at"))

    st.success(task.get("message") or "批量分析完成。")
    if started_at or finished_at:
        time_parts = [part for part in [started_at, finished_at] if part]
        st.caption(" -> ".join(time_parts))

    col_r1, col_r2, col_r3, col_r4 = st.columns(4)
    with col_r1:
        st.metric("总计", result.get("total", 0))
    with col_r2:
        st.metric("成功", result.get("succeeded", 0))
    with col_r3:
        st.metric("失败", result.get("failed", 0))
    with col_r4:
        st.metric("耗时", f"{result.get('elapsed_time', 0):.1f}秒")

    saved_ids = persistence_result.get("saved_ids") or []
    if saved_ids:
        st.info(f"已保存 {len(saved_ids)} 条分析记录到“分析历史”。")

    if result_bundle.get("auto_sync"):
        if sync_result.get("total", 0) > 0:
            st.info(
                f"实时监测同步：新增 {sync_result.get('added', 0)}，更新 {sync_result.get('updated', 0)}。"
            )
        else:
            st.info("没有可同步的实时监测项。")

    if result_bundle.get("send_notification"):
        st.caption("完成通知已按当前配置发送。")

    if result.get("results"):
        st.markdown("### 分析结果详情")
        for item in result.get("results", []):
            display_analysis_result_card(item)


def display_portfolio_manager(lightweight_model=None, reasoning_model=None):
    """显示持仓管理主界面"""
    try:
        portfolio_manager.reconcile_portfolio_integrations()
    except Exception as e:
        st.warning(f"持仓同步检查执行失败: {e}")

    _render_portfolio_analysis_task_notice()
    
    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs([
        "持仓管理",
        "风险评估",
        "分析任务",
        "分析历史"
    ])
    
    with tab1:
        display_portfolio_stocks(lightweight_model, reasoning_model)
        
    with tab2:
        display_portfolio_risk()
    
    with tab3:
        display_analysis_task_center(lightweight_model, reasoning_model)
    
    with tab4:
        display_analysis_history()



def _legacy_display_portfolio_risk():
    """显示持仓风险评估"""
    st.markdown("### 🛡️ 组合风险评估")
    
    all_stocks = portfolio_manager.get_all_stocks()
    if not all_stocks:
        st.info("暂无持仓股票，无法评估组合风险。")
        return

    controls_col, config_col = st.columns([2.5, 1.2], gap="large")
    with controls_col:
        selected_account = st.selectbox(
            "选择账户进行评估",
            _get_portfolio_account_options(all_stocks),
            key="risk_account_selector",
        )
        account_filter = _resolve_portfolio_account_filter(selected_account)
    with config_col:
        current_rate_pct = portfolio_manager.get_risk_free_rate_annual() * 100
        risk_free_rate_pct = st.number_input(
            "无风险利率(年化%)",
            min_value=0.0,
            max_value=10.0,
            value=float(current_rate_pct),
            step=0.1,
            format="%.2f",
            key="portfolio_risk_free_rate_input",
            help="用于计算组合夏普比率，默认 1.50%",
        )
        if st.button("保存口径", key="save_portfolio_risk_free_rate", width="content"):
            portfolio_manager.set_risk_free_rate_annual(risk_free_rate_pct / 100)
            st.success("无风险利率已更新。")
            st.rerun()

    portfolio_manager.ensure_daily_snapshot(account_filter, source="page_load")
    result = portfolio_manager.calculate_portfolio_risk(account_name=account_filter)
    
    if result.get("status") == "error":
        st.warning(result.get("message", "评估失败"))
        return
        
    total_val = result.get("total_market_value", 0)
    total_cost = result.get("total_cost_value", 0)
    total_pnl = result.get("total_pnl", 0)
    total_pnl_pct = result.get("total_pnl_pct", 0) * 100
    coverage = result.get("data_coverage", {})
    m1, m2, m3 = st.columns(3)
    m1.metric("总持仓市值", f"¥{total_val:,.2f}")
    m2.metric("总持仓成本", f"¥{total_cost:,.2f}")
    m3.metric("总浮动盈亏", f"¥{total_pnl:,.2f}", delta=f"{total_pnl_pct:.2f}%", delta_color="inverse")
    
    st.markdown("---")

    warnings = result.get("risk_warnings", [])
    if result.get("high_concentration"):
        for w in warnings:
            st.error(w)
    else:
        for w in warnings:
            st.success(w)

    st.markdown("#### 定量风险指标")
    q1, q2, q3 = st.columns(3)
    q1.metric("年化波动率", _format_percent(result.get("annual_volatility")))
    q2.metric("Beta(沪深300)", _format_ratio(result.get("beta_hs300")))
    q3.metric("夏普比率", _format_ratio(result.get("sharpe_ratio")))

    metric_warnings = result.get("metric_warnings", [])
    if metric_warnings:
        for warning in metric_warnings:
            st.warning(warning)

    st.caption(
        f"参与计算持仓数 {coverage.get('stock_count', 0)} 只；"
        f"参考基准 {result.get('benchmark_label', '沪深300')}"
    )

    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🔄 行业集中度")
        industry_data = result.get("industry_distribution", [])
        if industry_data:
            df_ind = pd.DataFrame(industry_data)
            df_ind["占比"] = df_ind["weight"].apply(lambda x: f"{x*100:.1f}%")
            df_ind["市值"] = df_ind["market_value"].apply(lambda x: f"¥{x:,.2f}")
            _render_static_table(
                df_ind[["industry", "市值", "占比"]].rename(columns={"industry": "行业"})
            )
            
    with col2:
        st.markdown("#### 🎯 单票集中度")
        stock_data = result.get("stock_distribution", [])
        if stock_data:
            df_st = pd.DataFrame(stock_data)
            df_st["占比"] = df_st["weight"].apply(lambda x: f"{x*100:.1f}%")
            df_st["市值"] = df_st["market_value"].apply(lambda x: f"¥{x:,.2f}")
            df_st["盈亏"] = df_st["pnl"].apply(lambda x: f"¥{x:,.2f}")
            df_st["盈亏比例"] = df_st["pnl_pct"].apply(lambda x: f"{x*100:.2f}%")
            _render_static_table(
                df_st[["name", "市值", "占比", "盈亏比例"]].rename(columns={"name": "股票"})
            )


def display_portfolio_risk():
    """显示持仓风险评估。"""
    st.markdown("### 组合风险评估")

    all_stocks = portfolio_manager.get_all_stocks()
    if not all_stocks:
        st.info("暂无持仓股票，无法评估组合风险。")
        return

    selected_account = st.selectbox(
        "选择账户进行评估",
        _get_portfolio_account_options(all_stocks),
        key="risk_account_selector",
    )
    account_filter = _resolve_portfolio_account_filter(selected_account)

    portfolio_manager.ensure_daily_snapshot(account_filter, source="page_load")
    result = portfolio_manager.calculate_portfolio_risk(account_name=account_filter)

    if result.get("status") == "error":
        st.warning(result.get("message", "评估失败"))
        return

    total_val = result.get("total_market_value", 0)
    total_cost = result.get("total_cost_value", 0)
    total_pnl = result.get("total_pnl", 0)
    total_pnl_pct = result.get("total_pnl_pct", 0) * 100
    coverage = result.get("data_coverage", {})
    m1, m2, m3 = st.columns(3)
    m1.metric("总持仓市值", f"¥{total_val:,.2f}")
    m2.metric("总持仓成本", f"¥{total_cost:,.2f}")
    m3.metric("总浮动盈亏", f"¥{total_pnl:,.2f}", delta=f"{total_pnl_pct:.2f}%", delta_color="inverse")

    st.markdown("---")

    warnings = result.get("risk_warnings", [])
    if result.get("high_concentration"):
        for warning in warnings:
            st.error(warning)
    else:
        for warning in warnings:
            st.success(warning)

    st.markdown("#### 定量风险指标")
    q1, q2, q3 = st.columns(3)
    q1.metric("年化波动率", _format_percent(result.get("annual_volatility")))
    q2.metric("Beta(沪深300)", _format_ratio(result.get("beta_hs300")))
    q3.metric("夏普比率", _format_ratio(result.get("sharpe_ratio")))

    metric_warnings = result.get("metric_warnings", [])
    if metric_warnings:
        for warning in metric_warnings:
            st.warning(warning)

    st.caption(
        f"参与计算持仓数 {coverage.get('stock_count', 0)} 只；"
        f"参考基准：{result.get('benchmark_label', '沪深300')}"
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 行业集中度")
        industry_data = result.get("industry_distribution", [])
        if industry_data:
            df_ind = pd.DataFrame(industry_data)
            df_ind["占比"] = df_ind["weight"].apply(lambda x: f"{x * 100:.1f}%")
            df_ind["市值"] = df_ind["market_value"].apply(lambda x: f"¥{x:,.2f}")
            _render_static_table(
                df_ind[["industry", "市值", "占比"]].rename(columns={"industry": "行业"})
            )

    with col2:
        st.markdown("#### 单票集中度")
        stock_data = result.get("stock_distribution", [])
        if stock_data:
            df_st = pd.DataFrame(stock_data)
            df_st["占比"] = df_st["weight"].apply(lambda x: f"{x * 100:.1f}%")
            df_st["市值"] = df_st["market_value"].apply(lambda x: f"¥{x:,.2f}")
            df_st["盈亏比例"] = df_st["pnl_pct"].apply(lambda x: f"{x * 100:.2f}%")
            _render_static_table(
                df_st[["name", "市值", "占比", "盈亏比例"]].rename(columns={"name": "股票"})
            )


def display_portfolio_stocks(lightweight_model=None, reasoning_model=None):
    """显示持仓股票列表和管理"""
    st.markdown("### 持仓股票管理")
    _render_single_analysis_feedback()

    with st.expander("➕ 添加持仓股票", expanded=False):
        display_add_stock_form()

    all_stocks = portfolio_manager.get_all_latest_analysis()
    if not all_stocks:
        st.info("暂无持仓股票，请添加股票代码开始管理。")
        return

    selected_account = st.selectbox(
        "账号筛选",
        _get_portfolio_account_options(all_stocks),
        key="portfolio_account_selector",
    )
    account_filter = _resolve_portfolio_account_filter(selected_account)
    stocks = [
        stock for stock in all_stocks
        if account_filter is None or stock.get("account_name", "默认账户") == account_filter
    ]
    trade_summary_map = portfolio_manager.get_trade_summary_map(
        [stock.get("id") for stock in stocks if stock.get("id")]
    )
    for stock in stocks:
        stock.update(trade_summary_map.get(stock.get("id"), {}))

    portfolio_manager.ensure_daily_snapshot(account_filter, source="page_load")
    subtab_list, subtab_review = st.tabs(["持仓列表", "复盘报告"])

    with subtab_list:
        _render_portfolio_stock_list(stocks, lightweight_model=lightweight_model, reasoning_model=reasoning_model)

    with subtab_review:
        display_portfolio_review_reports(account_filter)


def _render_portfolio_stock_list(
    stocks: List[Dict],
    *,
    lightweight_model=None,
    reasoning_model=None,
) -> None:
    if not stocks:
        st.info("当前账户暂无持仓股票。")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("持仓股票数", len(stocks))
    with col2:
        auto_monitor_count = sum(1 for s in stocks if s.get("auto_monitor"))
        st.metric("启用自动监测", auto_monitor_count)
    with col3:
        total_cost = sum(
            s.get("cost_price", 0) * s.get("quantity", 0) 
            for s in stocks 
            if s.get("cost_price") and s.get("quantity")
        )
        st.metric("总持仓成本", f"¥{total_cost:,.2f}")
    
    st.markdown("---")
    
    for stock in stocks:
        display_stock_card(
            stock,
            latest_analysis=stock,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )


def display_portfolio_review_reports(account_filter: str | None) -> None:
    st.markdown("#### 周期复盘报告")
    account_label = account_filter or "全部账户"
    st.caption("默认生成最近一个完整的周 / 月 / 季复盘报告。")

    col1, col2, col3 = st.columns(3)
    period_specs = [
        ("week", "生成周报", col1),
        ("month", "生成月报", col2),
        ("quarter", "生成季报", col3),
    ]
    for period_type, label, col in period_specs:
        with col:
            if st.button(label, key=f"generate_review_{period_type}_{account_label}", width="stretch"):
                result = portfolio_manager.generate_review_report(account_name=account_filter, period_type=period_type)
                if result.get("status") == "success":
                    st.session_state["portfolio_latest_review_report"] = result
                    st.success(f"{label}已生成。")
                    st.rerun()
                else:
                    st.error(result.get("message", "生成复盘报告失败。"))

    latest_report = st.session_state.get("portfolio_latest_review_report")
    if (
        latest_report
        and latest_report.get("status") == "success"
        and latest_report.get("account_name") == account_label
    ):
        st.markdown("##### 最新生成")
        st.code(latest_report.get("report_markdown", ""), language="markdown")
        st.download_button(
            "下载最新 Markdown",
            data=latest_report.get("report_markdown", ""),
            file_name=f"portfolio_review_{latest_report.get('period_type', 'report')}.md",
            mime="text/markdown",
            key=f"download_latest_review_{account_label}",
        )

    reports = portfolio_manager.get_review_reports(account_name=account_filter, limit=12)
    if not reports:
        st.info("暂无已保存的复盘报告。")
        return

    st.markdown("##### 历史报告")
    for report in reports:
        with st.expander(
            f"{report.get('created_at', '')} | {report.get('period_type', '').upper()} | "
            f"{report.get('period_start', '')} ~ {report.get('period_end', '')}",
            expanded=False,
        ):
            st.caption(
                f"账户：{report.get('account_name', '全部账户')} | 数据口径："
                f"{ {'actual': '真实', 'estimated': '估算', 'mixed': '混合'}.get(report.get('data_mode'), '估算') }"
            )
            st.code(report.get("report_markdown", ""), language="markdown")
            st.download_button(
                "下载 Markdown",
                data=report.get("report_markdown", ""),
                file_name=f"portfolio_review_{report.get('id')}.md",
                mime="text/markdown",
                key=f"download_review_{report.get('id')}",
            )


def display_analysis_task_center(lightweight_model=None, reasoning_model=None):
    st.markdown("### 🔄 分析任务中心")
    _render_batch_analysis_feedback()

    stocks = portfolio_manager.get_all_stocks()
    if not stocks:
        st.warning("暂无持仓股票，请先添加股票。")
        return

    status = portfolio_scheduler.get_status()
    task_config = portfolio_scheduler.get_task_config()
    queue_active = bool(_get_pending_portfolio_analysis_tasks())
    batch_pending = bool(_get_pending_portfolio_analysis_tasks("batch"))

    st.session_state.setdefault("portfolio_task_analysis_mode", task_config.analysis_mode)
    st.session_state.setdefault("portfolio_task_max_workers", task_config.max_workers)
    st.session_state.setdefault(
        "portfolio_task_selected_agents",
        task_config.selected_agents or portfolio_manager.DEFAULT_ANALYSIS_AGENTS,
    )
    st.session_state.setdefault("portfolio_task_auto_sync", task_config.auto_monitor_sync)
    st.session_state.setdefault("portfolio_task_notify", task_config.notification_enabled)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("持仓股票数", len(stocks))
    m2.metric("调度器状态", "运行中" if status.get("is_running") else "已停止")
    m3.metric("队列状态", "任务排队中" if queue_active else "空闲")
    m4.metric("下次运行", status.get("next_run_time") or "未设置")

    st.markdown("#### 共享分析配置")
    c1, c2 = st.columns(2)
    with c1:
        analysis_mode = st.selectbox(
            "分析模式",
            options=["sequential", "parallel"],
            format_func=lambda value: "顺序分析" if value == "sequential" else "并行分析",
            key="portfolio_task_analysis_mode",
        )
    with c2:
        max_workers = st.number_input(
            "并行线程数",
            min_value=1,
            max_value=10,
            disabled=(analysis_mode == "sequential"),
            key="portfolio_task_max_workers",
        )

    selected_agents = st.multiselect(
        "参与分析的分析师",
        options=[code for code, _ in AGENT_OPTIONS],
        format_func=lambda code: dict(AGENT_OPTIONS).get(code, code),
        key="portfolio_task_selected_agents",
    )
    effective_agents = selected_agents or portfolio_manager.DEFAULT_ANALYSIS_AGENTS

    opt1, opt2 = st.columns(2)
    with opt1:
        auto_sync = st.checkbox(
            "自动同步到监测",
            key="portfolio_task_auto_sync",
        )
    with opt2:
        send_notification = st.checkbox(
            "发送完成通知",
            key="portfolio_task_notify",
        )

    if st.button("保存任务配置", key="save_portfolio_task_config", type="primary", width="content"):
        portfolio_scheduler.update_config(
            analysis_mode=analysis_mode,
            max_workers=max_workers if analysis_mode == "parallel" else 1,
            auto_sync_monitor=auto_sync,
            send_notification=send_notification,
            selected_agents=effective_agents,
        )
        st.success("任务配置已更新。")
        st.rerun()

    st.markdown("---")
    st.markdown("#### 任务操作")
    action1, action2, action3 = st.columns(3)
    with action1:
        action_label = "批量分析中" if batch_pending else "加入批量队列" if queue_active else "立即执行一次"
        if st.button(
            action_label,
            key="portfolio_task_run_once",
            width="stretch",
            disabled=batch_pending,
        ):
            portfolio_scheduler.update_config(
                analysis_mode=analysis_mode,
                max_workers=max_workers if analysis_mode == "parallel" else 1,
                auto_sync_monitor=auto_sync,
                send_notification=send_notification,
                selected_agents=effective_agents,
            )
            try:
                _start_batch_analysis_task(
                    stocks,
                    analysis_mode=analysis_mode,
                    max_workers=max_workers if analysis_mode == "parallel" else 1,
                    selected_agents=effective_agents,
                    auto_sync=auto_sync,
                    send_notification=send_notification,
                    lightweight_model=lightweight_model,
                    reasoning_model=reasoning_model,
                )
            except RuntimeError as exc:
                st.warning(str(exc))
                return
            st.success("分析任务已提交到后台队列。")
            st.rerun()
    with action2:
        if status.get("is_running"):
            if st.button("停止调度器", key="portfolio_task_stop_scheduler", width="stretch"):
                portfolio_scheduler.stop_scheduler()
                st.success("调度器已停止。")
                st.rerun()
        else:
            if st.button("启动调度器", key="portfolio_task_start_scheduler", width="stretch", type="primary"):
                portfolio_scheduler.update_config(
                    analysis_mode=analysis_mode,
                    max_workers=max_workers if analysis_mode == "parallel" else 1,
                    auto_sync_monitor=auto_sync,
                    send_notification=send_notification,
                    selected_agents=effective_agents,
                )
                if portfolio_scheduler.start_scheduler():
                    st.success("调度器已启动。")
                else:
                    st.warning("调度器启动失败，请检查持仓数量和时间配置。")
                st.rerun()
    with action3:
        if st.button("刷新状态", key="portfolio_task_refresh", width="stretch"):
            st.rerun()

    st.markdown("---")
    st.markdown("#### 定时时间")
    schedule_times = portfolio_scheduler.get_schedule_times()
    if schedule_times:
        time_cols = st.columns(min(4, max(1, len(schedule_times))))
        for index, time_str in enumerate(schedule_times):
            with time_cols[index % len(time_cols)]:
                st.info(f"⏰ {time_str}")
                if st.button("删除", key=f"portfolio_delete_schedule_{time_str}", width="content"):
                    if len(schedule_times) > 1:
                        portfolio_scheduler.remove_schedule_time(time_str)
                        st.success(f"已删除 {time_str}")
                        st.rerun()
                    else:
                        st.error("至少保留一个定时时间。")
    else:
        st.warning("暂无定时时间配置。")

    with st.expander("➕ 添加定时时间", expanded=False):
        add_col, add_btn_col = st.columns([3, 1])
        with add_col:
            new_time = st.time_input(
                "选择时间",
                value=datetime.strptime("15:05", "%H:%M").time(),
                key="portfolio_new_schedule_time",
            )
        with add_btn_col:
            st.write("")
            st.write("")
            if st.button("添加", key="portfolio_add_schedule_btn", type="primary", width="stretch"):
                time_str = new_time.strftime("%H:%M")
                if portfolio_scheduler.add_schedule_time(time_str):
                    st.success(f"已添加 {time_str}")
                else:
                    st.warning(f"{time_str} 已存在")
                st.rerun()


def display_stock_card(
    stock: Dict,
    latest_analysis: Dict | None = None,
    lightweight_model=None,
    reasoning_model=None,
):
    """显示单个股票卡片"""

    stock_id = stock.get("id")
    code = stock.get("code", "")
    cost_price = stock.get("cost_price")
    quantity = stock.get("quantity")
    note = stock.get("note", "")
    auto_monitor = stock.get("auto_monitor", True)
    view_model = portfolio_manager.build_stock_card_view_model(stock, latest_analysis)
    rating = view_model.get("rating", "待分析")
    rating_color = get_recommendation_color(rating)
    analysis_time_text = view_model.get("analysis_time_text") or "尚未分析"
    summary_text = view_model.get("summary_text", "")
    note_text = view_model.get("note_text", "")
    display_name = view_model.get("display_name") or code
    edit_state_key = f"portfolio_editing_{stock_id}"
    trade_state_key = f"portfolio_trading_{stock_id}"
    auto_monitor_key = f"portfolio_auto_monitor_{stock_id}"
    trade_type_key = f"portfolio_trade_type_{stock_id}"
    trade_date_key = f"portfolio_trade_date_{stock_id}"
    trade_price_key = f"portfolio_trade_price_{stock_id}"
    trade_quantity_key = f"portfolio_trade_quantity_{stock_id}"
    trade_note_key = f"portfolio_trade_note_{stock_id}"
    first_buy_date = stock.get("first_buy_date")
    trade_count = int(stock.get("trade_count") or 0)
    stock_pending_task = _get_stock_pending_analysis_task(stock_id)
    queue_active = bool(_get_pending_portfolio_analysis_tasks())
    analysis_disabled = bool(stock_pending_task)
    if stock_pending_task and stock_pending_task.get("status") == "running":
        analysis_button_label = "分析中"
    elif stock_pending_task:
        analysis_button_label = "已排队"
    elif queue_active:
        analysis_button_label = "加入队列"
    else:
        analysis_button_label = "分析"

    try:
        default_trade_price = float((latest_analysis or {}).get("current_price"))
    except (TypeError, ValueError):
        default_trade_price = float(cost_price) if cost_price not in (None, "") else 0.0

    with st.container(border=True):
        header_col, rating_col = st.columns([4.5, 1.2], gap="small")
        with header_col:
            st.markdown(f"#### {display_name}")
        with rating_col:
            st.markdown(
                (
                    "<div style='text-align:right; font-weight:600; "
                    f"color:{rating_color}; padding-top:0.35rem;'>{_escape_text(rating)}</div>"
                ),
                unsafe_allow_html=True,
            )

        info_pairs = [
            ("成本", view_model.get("cost_text") or "未设置"),
            ("数量", view_model.get("quantity_text") or "未设置"),
            ("盈亏", view_model.get("pnl_amount_text") or "N/A"),
            ("盈亏比", view_model.get("pnl_percent_text") or "N/A"),
        ]
        info_cols = st.columns(len(info_pairs), gap="small")
        for col, (label, value) in zip(info_cols, info_pairs):
            with col:
                st.caption(label)
                st.markdown(f"**{value}**")

        st.caption(f"最近分析：{analysis_time_text}")
        if summary_text:
            st.write(f"摘要：{summary_text}")
        if note_text:
            st.caption(f"备注：{note_text}")

        meta_bits = []
        if first_buy_date:
            meta_bits.append(f"建仓日期：{first_buy_date}")
        if trade_count:
            meta_bits.append(f"交易笔数：{trade_count}")
        if meta_bits:
            st.caption(" | ".join(meta_bits))

        action_col1, action_col2, action_col3, action_col4, action_col5 = st.columns(
            [1.15, 1, 1, 1, 1],
            gap="small",
        )
        with action_col1:
            toggle_value = st.toggle(
                "监测",
                value=view_model.get("auto_monitor", True),
                key=auto_monitor_key,
                help="自动监测：启用后会自动同步到监测",
            )
            if toggle_value != auto_monitor:
                success, msg = portfolio_manager.update_stock(stock_id, auto_monitor=toggle_value)
                if success:
                    st.success(msg)
                else:
                    st.session_state[auto_monitor_key] = auto_monitor
                    st.error(msg)
                st.rerun()
        with action_col2:
            if st.button(
                analysis_button_label,
                key=f"analyze_{stock_id}",
                help=(
                    "单独分析该持仓"
                    if not stock_pending_task and not queue_active
                    else "加入分析队列，等待前序任务完成后执行"
                    if not stock_pending_task
                    else "该股票已在后台分析队列中"
                ),
                disabled=analysis_disabled,
            ):
                run_single_stock_analysis(stock, lightweight_model, reasoning_model)
        with action_col3:
            if st.button("编辑", key=f"edit_{stock_id}", help="编辑"):
                st.session_state[edit_state_key] = True
                st.session_state.pop(trade_state_key, None)
                st.rerun()
        with action_col4:
            if st.button("交易", key=f"trade_{stock_id}", help="记录加仓或减仓"):
                st.session_state[trade_state_key] = True
                st.session_state[trade_type_key] = "buy"
                st.session_state[trade_date_key] = date.today()
                st.session_state[trade_price_key] = default_trade_price
                st.session_state[trade_quantity_key] = 100
                st.session_state[trade_note_key] = ""
                st.session_state.pop(edit_state_key, None)
                st.rerun()
        with action_col5:
            if st.button("删除", key=f"del_{stock_id}", help="删除"):
                success, msg = portfolio_manager.delete_stock(stock_id)
                if success:
                    st.session_state.pop(auto_monitor_key, None)
                    st.session_state.pop(edit_state_key, None)
                    st.session_state.pop(trade_state_key, None)
                    st.session_state.pop(trade_type_key, None)
                    st.session_state.pop(trade_date_key, None)
                    st.session_state.pop(trade_price_key, None)
                    st.session_state.pop(trade_quantity_key, None)
                    st.session_state.pop(trade_note_key, None)
                    st.success(msg)
                else:
                    st.error(msg)
                st.rerun()

        if st.session_state.get(edit_state_key):
            with st.form(key=f"edit_form_{stock_id}"):
                st.markdown(f"#### 编辑 {display_name}")
                st.caption(f"股票代码：{code}")

                col_a, col_b = st.columns(2)
                with col_a:
                    new_cost = st.number_input(
                        "成本价",
                        value=cost_price if cost_price else 0.0,
                        min_value=0.0,
                        step=0.001,
                        format="%.3f",
                    )
                    new_quantity = st.number_input(
                        "持仓数量",
                        value=quantity if quantity else 0,
                        min_value=0,
                        step=100,
                    )

                with col_b:
                    new_note = st.text_area("备注", value=note, height=80)

                col_submit, col_cancel = st.columns(2)
                with col_submit:
                    if st.form_submit_button("保存", type="primary"):
                        success, msg = portfolio_manager.update_stock(
                            stock_id,
                            cost_price=new_cost if new_cost > 0 else None,
                            quantity=new_quantity if new_quantity > 0 else None,
                            note=new_note,
                        )
                        del st.session_state[edit_state_key]
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
                        st.rerun()
                
                with col_cancel:
                    if st.form_submit_button("取消"):
                        del st.session_state[edit_state_key]
                        st.rerun()


        if st.session_state.get(trade_state_key):
            st.session_state.setdefault(trade_type_key, "buy")
            st.session_state.setdefault(trade_date_key, date.today())
            st.session_state.setdefault(trade_price_key, default_trade_price)
            st.session_state.setdefault(trade_quantity_key, 100)
            st.session_state.setdefault(trade_note_key, "")

            with st.form(key=f"trade_form_{stock_id}"):
                st.markdown(f"#### 加仓 / 减仓 - {display_name}")

                trade_col1, trade_col2 = st.columns(2)
                with trade_col1:
                    trade_type = st.radio(
                        "交易类型",
                        options=["buy", "sell"],
                        format_func=lambda value: "加仓" if value == "buy" else "减仓",
                        key=trade_type_key,
                        horizontal=True,
                    )
                    trade_date = st.date_input("成交日期", key=trade_date_key)

                with trade_col2:
                    trade_price = st.number_input(
                        "成交价格",
                        min_value=0.0,
                        step=0.001,
                        format="%.3f",
                        key=trade_price_key,
                    )
                    trade_quantity = st.number_input(
                        "成交数量",
                        min_value=1,
                        step=1,
                        key=trade_quantity_key,
                    )

                trade_note = st.text_area(
                    "交易备注",
                    key=trade_note_key,
                    height=80,
                    placeholder="可选，例如：补仓、止盈减仓、调仓",
                )

                trade_submit_col, trade_cancel_col = st.columns(2)
                with trade_submit_col:
                    if st.form_submit_button("保存交易", type="primary"):
                        success, msg, _ = portfolio_manager.record_trade(
                            stock_id=stock_id,
                            trade_type=trade_type,
                            quantity=int(trade_quantity),
                            price=float(trade_price),
                            trade_date=trade_date,
                            note=trade_note,
                        )
                        if success:
                            st.session_state.pop(trade_state_key, None)
                            st.session_state.pop(trade_type_key, None)
                            st.session_state.pop(trade_date_key, None)
                            st.session_state.pop(trade_price_key, None)
                            st.session_state.pop(trade_quantity_key, None)
                            st.session_state.pop(trade_note_key, None)
                            st.success(msg)
                        else:
                            st.error(msg)
                        st.rerun()
                with trade_cancel_col:
                    if st.form_submit_button("取消"):
                        st.session_state.pop(trade_state_key, None)
                        st.session_state.pop(trade_type_key, None)
                        st.session_state.pop(trade_date_key, None)
                        st.session_state.pop(trade_price_key, None)
                        st.session_state.pop(trade_quantity_key, None)
                        st.session_state.pop(trade_note_key, None)
                        st.rerun()

            recent_trades = portfolio_manager.get_trade_history(stock_id, limit=5)
            if recent_trades:
                st.caption("最近 5 笔交易")
                recent_trade_df = pd.DataFrame(
                    [
                        {
                            "日期": item.get("trade_date"),
                            "类型": "加仓" if item.get("trade_type") == "buy" else "减仓",
                            "价格": format_price(item.get("price")),
                            "数量": item.get("quantity"),
                            "备注": item.get("note") or "",
                        }
                        for item in recent_trades
                    ]
                )
                _render_static_table(recent_trade_df)


def run_single_stock_analysis(stock: Dict, lightweight_model=None, reasoning_model=None):
    """启动单只持仓后台分析任务。"""
    code = stock.get("code", "")
    name = stock.get("name", code)
    if not code:
        st.error("持仓股票缺少代码，无法分析。")
        return

    queue_active = bool(_get_pending_portfolio_analysis_tasks())
    try:
        _start_single_stock_analysis_task(stock, lightweight_model, reasoning_model)
    except RuntimeError as exc:
        st.warning(str(exc))
        return
    if queue_active:
        st.success(f"{code} {name} 已加入分析队列，会在前序任务完成后自动执行。")
    else:
        st.success(f"已开始后台分析 {code} {name}，切换页面后状态会保留。")
    st.rerun()


def display_add_stock_form():
    """显示添加股票表单"""
    st.session_state.setdefault("portfolio_add_code", "")
    st.session_state.setdefault("portfolio_add_cost_price", 0.0)
    st.session_state.setdefault("portfolio_add_quantity", 0)
    st.session_state.setdefault("portfolio_add_note", "")
    st.session_state.setdefault("portfolio_add_auto_monitor", True)
    st.session_state.setdefault("portfolio_add_buy_date", date.today())
    
    with st.form(key="add_stock_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            account_name = st.text_input("账户名称", value="默认账户", help="隔离不同账户的持仓")
            code = st.text_input(
                "股票代码*", 
                placeholder="例如: 600519、000001.SZ、00700.HK、AAPL",
                help="必填，支持自动识别股票名称，兼容 SH/SZ/HK/US 后缀格式",
                key="portfolio_add_code",
            )
            st.caption("股票名称会在保存时根据股票代码自动识别。")
        
        with col2:
            cost_price = st.number_input(
                "成本价", 
                min_value=0.0, 
                step=0.001,
                format="%.3f",
                help="可选，用于计算收益",
                key="portfolio_add_cost_price",
            )
            quantity = st.number_input(
                "持仓数量", 
                min_value=0, 
                step=100,
                help="可选，单位：股",
                key="portfolio_add_quantity",
            )
            buy_date = st.date_input(
                "建仓日期",
                key="portfolio_add_buy_date",
                help="填写成本价和数量时，会将这一天记为首笔建仓日期。",
            )
        
        note = st.text_area("备注", height=80, placeholder="可选，记录买入理由等信息", key="portfolio_add_note")
        auto_monitor = st.checkbox("分析后自动同步到监测", key="portfolio_add_auto_monitor")
        
        if st.form_submit_button("➕ 添加股票", type="primary"):
            if not code:
                st.error("请输入股票代码")
            else:
                try:
                    success, msg, stock_id = portfolio_manager.add_stock(
                        code=code.strip().upper(),
                        name=None,
                        cost_price=cost_price if cost_price > 0 else None,
                        quantity=quantity if quantity > 0 else None,
                        note=note.strip() if note else None,
                        auto_monitor=auto_monitor,
                        account_name=account_name.strip()
                    )
                    if not success:
                        st.error(msg)
                        return
                    if stock_id and cost_price > 0 and quantity > 0:
                        portfolio_manager.seed_initial_trade(
                            stock_id,
                            trade_date=buy_date,
                            note=note.strip() if note else "",
                        )
                    st.session_state["portfolio_add_code"] = ""
                    st.session_state["portfolio_add_cost_price"] = 0.0
                    st.session_state["portfolio_add_quantity"] = 0
                    st.session_state["portfolio_add_note"] = ""
                    st.session_state["portfolio_add_auto_monitor"] = True
                    st.session_state["portfolio_add_buy_date"] = date.today()
                    st.success(msg)
                    st.rerun()
                except Exception as e:
                    st.error(f"添加失败: {str(e)}")


def display_batch_analysis(lightweight_model=None, reasoning_model=None):
    """兼容旧入口，转向新的分析任务中心。"""
    display_analysis_task_center(lightweight_model, reasoning_model)


def display_analysis_result_card(item: Dict):
    """显示单个分析结果卡片"""
    
    code = item.get("code", "")
    result = item.get("result", {})
    
    # 检查分析是否成功
    if result.get("success"):
        final_decision = result.get("final_decision", {})
        stock_info = result.get("stock_info", {})
        
        # 使用正确的字段名
        rating = final_decision.get("rating", "未知")
        confidence = final_decision.get("confidence_level", "N/A")
        target_price = final_decision.get("target_price", "N/A")
        entry_range = final_decision.get("entry_range", "N/A")
        take_profit = final_decision.get("take_profit", "N/A")
        stop_loss = final_decision.get("stop_loss", "N/A")
        
        advice = final_decision.get("operation_advice") or final_decision.get("advice", "")
        with st.expander(f"{code} {stock_info.get('name', '')} | {rating} | 信心度 {confidence}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**进出场位置**")
                st.write(f"进场区间: {entry_range}")
                st.write(f"目标价: {target_price}")
            
            with col2:
                st.markdown("**风控位置**")
                st.write(f"止盈位: {take_profit}")
                st.write(f"止损位: {stop_loss}")
            
            if advice:
                st.markdown("**投资建议**")
                st.info(advice)
    
    else:
        # 分析失败
        error = result.get("error", "未知错误")
        with st.expander(f"{code} - 分析失败"):
            st.error(f"错误: {error}")


def display_scheduler_management():
    """兼容旧入口，转向新的分析任务中心。"""
    display_analysis_task_center()


def display_analysis_history():
    """显示分析历史"""
    
    st.markdown("### 分析历史记录")
    
    stocks = portfolio_manager.get_all_stocks()
    
    if not stocks:
        st.info("暂无持仓股票")
        return
    
    # 选择股票
    stock_codes = [s["code"] for s in stocks]
    selected_code = st.selectbox(
        "选择股票",
        options=["全部"] + stock_codes,
        help="查看特定股票的分析历史"
    )
    # 获取历史记录
    if selected_code == "全部":
        # 获取所有股票的最新历史
        all_history = []
        for stock in stocks:
            stock_id = stock["id"]
            history = portfolio_manager.db.get_latest_analysis_history(
                stock_id,
                limit=5,
            )
            for h in history:
                h["code"] = stock["code"]
                h["name"] = stock["name"]
            all_history.extend(history)
        
        # 按时间排序
        all_history.sort(key=lambda x: x.get("analysis_time", ""), reverse=True)
        history_list = all_history[:20]  # 只显示最近20条
    else:
        # 获取指定股票的历史
        stock = next((s for s in stocks if s["code"] == selected_code), None)
        if stock:
            history_list = portfolio_manager.db.get_latest_analysis_history(
                stock["id"],
                limit=20,
            )
            for h in history_list:
                h["code"] = stock["code"]
                h["name"] = stock["name"]
        else:
            history_list = []
    
    if not history_list:
        st.info(f"暂无分析历史记录")
        return
    
    # 显示历史记录
    st.caption(f"共 {len(history_list)} 条记录")
    
    for record in history_list:
        display_history_record(record)


def display_history_record(record: Dict):
    """显示单条历史记录"""
    
    code = record.get("code", "")
    name = record.get("name", "")
    analysis_time = record.get("analysis_time", "")
    rating = portfolio_manager._normalize_analysis_rating(record.get("rating"), default="待分析")
    confidence = record.get("confidence", 0)
    current_price = record.get("current_price")
    target_price = record.get("target_price")
    entry_min = record.get("entry_min")
    entry_max = record.get("entry_max")
    take_profit = record.get("take_profit")
    stop_loss = record.get("stop_loss")
    summary = record.get("summary", "")
    analysis_source = record.get("analysis_source", "portfolio_batch_analysis")
    has_full_report = bool(record.get("has_full_report"))
    
    source_map = {
        "portfolio_batch_analysis": "批量分析",
        "portfolio_single_analysis": "单股分析",
        "portfolio_scheduler": "定时分析",
    }
    source_label = source_map.get(analysis_source, "历史分析")
    rating_color = get_recommendation_color(rating)
    
    with st.expander(
        f"{code} {name} | {rating} | {analysis_time}",
        expanded=False
    ):
        if has_full_report:
            agents_results = record.get("agents_results") or {}
            discussion_result = record.get("discussion_result", "")
            raw_final_decision = record.get("final_decision") or {}
            resolved_final_decision, _, decision_reasoning = _resolve_final_decision_content(
                raw_final_decision
            )
            normalized_rating = portfolio_manager._normalize_analysis_rating(
                (resolved_final_decision or {}).get("rating") if isinstance(resolved_final_decision, dict) else rating,
                default=rating,
            )
            final_decision_display = _build_history_final_decision_display(
                record,
                resolved_final_decision if isinstance(resolved_final_decision, dict) else {},
                normalized_rating=normalized_rating,
            )

            render_final_decision(final_decision_display)
            st.subheader("分析师原始报告")
            render_agents_analysis_tabs(
                agents_results,
                show_header=False,
                preferred_order=["technical", "fundamental", "fund_flow", "risk_management"],
                tab_labels={
                    "technical": "技术",
                    "fundamental": "基本面",
                    "fund_flow": "资金",
                    "risk_management": "风险管理",
                },
                include_other_agents=False,
                split_reasoning=True,
            )
            render_reasoning_process(
                None,
                discussion_result,
                expanded=False,
                include_agents=False,
                extra_sections=[("最终决策推理", decision_reasoning)] if decision_reasoning else None,
            )
            return

        st.markdown(
            f"""
            <div class="decision-card">
                <strong style="color: {rating_color};">{rating}</strong>
                <span style="margin-left: 12px;">来源: {source_label}</span>
                <span style="margin-left: 12px;">置信度: {confidence}%</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("当时价格", format_price(current_price) if current_price else "N/A")
        with col2:
            entry_text = (
                f"{format_price(entry_min)} ~ {format_price(entry_max)}"
                if entry_min is not None and entry_max is not None
                else "N/A"
            )
            st.metric("进场区间", entry_text)
        with col3:
            st.metric("目标价", format_price(target_price) if target_price else "N/A")

        col4, col5 = st.columns(2)
        with col4:
            st.metric("止盈", format_price(take_profit) if take_profit else "N/A")
        with col5:
            st.metric("止损", format_price(stop_loss) if stop_loss else "N/A")

        if summary:
            st.markdown("**分析摘要**")
            st.info(summary)

