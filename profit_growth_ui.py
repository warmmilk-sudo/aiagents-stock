#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
净利增长策略UI模块
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict
from profit_growth_selector import profit_growth_selector
from notification_service import notification_service
from profit_growth_monitor import profit_growth_monitor
from ui_analysis_task_utils import (
    consume_finished_ui_analysis_task,
    get_latest_ui_analysis_task,
    get_ui_analysis_button_state,
    render_ui_analysis_task_live_card,
    start_ui_analysis_task,
)


PROFIT_GROWTH_TASK_TYPE = "profit_growth_selection"
PROFIT_GROWTH_TASK_DONE_KEY = "profit_growth_selection_last_handled_task"


@st.fragment(run_every=1.0)
def _render_profit_growth_task_fragment():
    render_ui_analysis_task_live_card(
        task_type=PROFIT_GROWTH_TASK_TYPE,
        title="净利增长选股任务状态",
        state_prefix="profit_growth_selection_live",
    )


def _run_profit_growth_selection_task(
    *,
    top_n: int,
    min_profit_growth: float,
    min_turnover_yi: float,
    max_turnover_yi: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
    filter_summary: str,
    report_progress,
):
    report_progress(current=0, total=2, message="正在拉取净利增长候选数据...")
    success, stocks_df, message = profit_growth_selector.get_profit_growth_stocks(
        top_n,
        min_profit_growth=min_profit_growth,
        min_turnover_yi=min_turnover_yi or None,
        max_turnover_yi=max_turnover_yi or None,
        sort_by=sort_by,
        exclude_st=exclude_st,
        exclude_kcb=exclude_kcb,
        exclude_cyb=exclude_cyb,
    )
    if not success:
        raise RuntimeError(message or "净利增长选股失败")

    report_progress(current=2, total=2, message="净利增长选股完成，正在同步结果...")
    return {
        "stocks_df": stocks_df,
        "message": message,
        "filter_summary": filter_summary,
        "selected_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _restore_profit_growth_result_from_latest_task() -> None:
    if st.session_state.get("profit_growth_stocks") is not None:
        return
    latest_task = get_latest_ui_analysis_task(PROFIT_GROWTH_TASK_TYPE)
    if not latest_task or latest_task.get("status") != "success":
        return
    payload = latest_task.get("result") or {}
    stocks_df = payload.get("stocks_df")
    if stocks_df is None:
        return
    st.session_state.profit_growth_stocks = stocks_df
    st.session_state.profit_growth_time = payload.get("selected_time")
    st.session_state.profit_growth_filter_summary = payload.get("filter_summary")


def build_profit_growth_filter_summary(
    *,
    min_profit_growth: float,
    min_turnover_yi: float,
    max_turnover_yi: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
) -> str:
    """Build a compact filter summary for the UI and notifications."""
    parts = [f"净利增长≥{min_profit_growth:.0f}%", sort_by]
    if min_turnover_yi > 0:
        parts.append(f"成交额≥{min_turnover_yi:.0f}亿")
    if max_turnover_yi > 0:
        parts.append(f"成交额≤{max_turnover_yi:.0f}亿")
    if exclude_st:
        parts.append("剔除ST")
    if exclude_kcb:
        parts.append("剔除科创板")
    if exclude_cyb:
        parts.append("剔除创业板")
    return "，".join(parts)


def display_profit_growth():
    """显示净利增长策略界面"""
    
    # 检查是否显示监控面板
    if st.session_state.get('show_profit_growth_monitor'):
        display_profit_growth_monitor_panel()
        
        # 返回按钮
        if st.button("返回选股", type="secondary"):
            del st.session_state.show_profit_growth_monitor
            st.rerun()
        return
    
    with st.expander("选股策略说明", expanded=False):
        st.markdown("""
        **筛选条件**：
        - 净利润增长率 ≥ 10%（净利润同比增长率）
        - 深圳A股
        - 非ST股票
        - 非创业板
        - 非科创板
        - 按成交额由小到大排名

        **量化交易策略**：
        - 资金量：5万元
        - 持股周期：5天
        - 仓位控制：满仓
        - 个股最大持仓：4成（40%）
        - 账户最大持股数：4只
        - 单日最大买入数：1只
        - 买入时机：开盘买入
        - 卖出时机：KDJ死叉或持股满5天

        > **注意**：当前监控服务暂时使用MA5下穿MA20作为卖出信号，后续将升级支持KDJ指标。
        """)

    col_top_n, col_hint = st.columns([2, 1])
    with col_top_n:
        top_n = st.slider(
            "筛选数量",
            min_value=3,
            max_value=10,
            value=5,
            step=1,
            help="选择展示的股票数量",
        )
    with col_hint:
        st.caption(f"默认返回前 {top_n} 只股票。")

    with st.expander("高级筛选参数", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            min_profit_growth = st.number_input(
                "最低净利增速(%)",
                min_value=0.0,
                max_value=1000.0,
                value=10.0,
                step=5.0,
            )
        with col2:
            min_turnover_yi = st.number_input(
                "最低成交额(亿)",
                min_value=0.0,
                max_value=1000.0,
                value=0.0,
                step=1.0,
                help="0 表示不限制",
            )
        with col3:
            max_turnover_yi = st.number_input(
                "最高成交额(亿)",
                min_value=0.0,
                max_value=1000.0,
                value=0.0,
                step=5.0,
                help="0 表示不限制",
            )

        col4, col5, col6, col7 = st.columns(4)
        with col4:
            sort_by = st.selectbox(
                "排序方式",
                ["成交额升序", "成交额降序", "净利润增长率降序", "股价升序"],
            )
        with col5:
            exclude_st = st.checkbox("剔除ST", value=True)
        with col6:
            exclude_kcb = st.checkbox("剔除科创板", value=True)
        with col7:
            exclude_cyb = st.checkbox("剔除创业板", value=True)

    filter_summary = build_profit_growth_filter_summary(
        min_profit_growth=min_profit_growth,
        min_turnover_yi=min_turnover_yi,
        max_turnover_yi=max_turnover_yi,
        sort_by=sort_by,
        exclude_st=exclude_st,
        exclude_kcb=exclude_kcb,
        exclude_cyb=exclude_cyb,
    )
    st.caption(f"当前筛选：{filter_summary}")
    _restore_profit_growth_result_from_latest_task()
    _render_profit_growth_task_fragment()

    finished_task = consume_finished_ui_analysis_task(PROFIT_GROWTH_TASK_TYPE, PROFIT_GROWTH_TASK_DONE_KEY)
    if finished_task:
        if finished_task.get("status") == "success":
            payload = finished_task.get("result") or {}
            st.session_state.profit_growth_stocks = payload.get("stocks_df")
            st.session_state.profit_growth_time = payload.get("selected_time")
            st.session_state.profit_growth_filter_summary = payload.get("filter_summary")
            st.success(payload.get("message") or "净利增长选股完成。")
        else:
            st.error(f"净利增长选股失败：{finished_task.get('error', '未知错误')}")

    action_label, action_disabled, action_help = get_ui_analysis_button_state(
        PROFIT_GROWTH_TASK_TYPE,
        "开始选股",
    )
    action_col, monitor_col = st.columns([3, 1])
    with action_col:
        run_selection = st.button(
            action_label,
            type="primary",
            width='stretch',
            disabled=action_disabled,
            help=action_help,
            key="profit_growth_start_selection",
        )
    with monitor_col:
        if st.button("策略监控", type="secondary", width='stretch', key="profit_growth_monitor_panel"):
            st.session_state.show_profit_growth_monitor = True
            st.rerun()

    if run_selection:
        try:
            start_ui_analysis_task(
                task_type=PROFIT_GROWTH_TASK_TYPE,
                label="净利增长选股",
                runner=lambda _task_id, report_progress: _run_profit_growth_selection_task(
                    top_n=top_n,
                    min_profit_growth=min_profit_growth,
                    min_turnover_yi=min_turnover_yi,
                    max_turnover_yi=max_turnover_yi,
                    sort_by=sort_by,
                    exclude_st=exclude_st,
                    exclude_kcb=exclude_kcb,
                    exclude_cyb=exclude_cyb,
                    filter_summary=filter_summary,
                    report_progress=report_progress,
                ),
                metadata={"top_n": top_n, "filter_summary": filter_summary},
            )
            st.info("已提交后台分析任务，可切换页面，返回后会自动同步进度和结果。")
            st.rerun()
        except RuntimeError as exc:
            st.warning(str(exc))
    
    # 显示选股结果
    if 'profit_growth_stocks' in st.session_state and st.session_state.profit_growth_stocks is not None:
        st.markdown("---")
        st.markdown("## 选股结果")
        
        stocks_df = st.session_state.profit_growth_stocks
        select_time = st.session_state.profit_growth_time
        filter_summary = st.session_state.get('profit_growth_filter_summary')
        
        st.info(f"选股时间：{select_time} | 股票数量：{len(stocks_df)} 只")
        if filter_summary:
            st.caption(f"筛选条件：{filter_summary}")
        
        # 显示股票列表
        display_stock_list(stocks_df)
        
        # 发送钉钉通知
        st.markdown("---")
        if st.button("发送钉钉通知", type="secondary", width='stretch'):
            send_dingtalk_notification(stocks_df, filter_summary)


def display_stock_list(stocks_df: pd.DataFrame):
    """显示股票列表"""
    
    for idx, row in stocks_df.iterrows():
        stock_code = row.get('股票代码', 'N/A')
        stock_name = row.get('股票简称', 'N/A')
        
        with st.expander(f"{idx+1}. {stock_code} {stock_name}", expanded=True):
            display_stock_detail(row)


def display_stock_detail(row: pd.Series):
    """显示股票详细信息"""
    
    # 获取所有可能的字段
    financial_fields = [
        ('净利润增长率', row.get('净利润增长率', row.get('净利润同比增长率', None))),
        ('成交额', row.get('成交额', row.get('成交额[20241213]', None))),
        ('股价', row.get('股价', row.get('最新价', None))),
        ('市盈率', row.get('市盈率', row.get('市盈率TTM', None))),
        ('市净率', row.get('市净率', row.get('市净率PB', None))),
        ('所属行业', row.get('所属行业', row.get('所属同花顺行业', None))),
    ]
    
    # 检查是否有任何有效数据
    has_any_data = any(is_valid_value(value) for _, value in financial_fields)
    
    # 决定布局
    if has_any_data:
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()
        col2 = None
    
    with col1:
        st.markdown("#### 基本信息")
        st.markdown(f"**股票代码**: {row.get('股票代码', 'N/A')}")
        st.markdown(f"**股票名称**: {row.get('股票简称', 'N/A')}")
    
    # 只有当有财务数据时才显示财务指标
    if col2 is not None:
        with col2:
            st.markdown("#### 财务指标")
            
            for field_name, value in financial_fields:
                if is_valid_value(value):
                    formatted_value = format_value(value, get_suffix(field_name))
                    st.markdown(f"**{field_name}**: {formatted_value}")
    
    # 添加监控按钮
    st.markdown("---")
    st.markdown("#### 策略监控")
    
    stock_code = row.get('股票代码', '')
    stock_name = row.get('股票简称', '')
    price = row.get('股价', row.get('最新价', None))
    
    # 去掉代码后缀
    if isinstance(stock_code, str) and '.' in stock_code:
        stock_code = stock_code.split('.')[0]
    
    # 转换价格
    try:
        price_float = float(price) if price and not pd.isna(price) else None
    except:
        price_float = None
    
    if stock_code and stock_name:
        add_stock_to_monitor_button(stock_code, stock_name, price_float)


def add_stock_to_monitor_button(stock_code: str, stock_name: str, price: float = None):
    """添加股票到监控的按钮"""
    
    button_key = f"add_monitor_{stock_code}"
    
    if st.button("加入策略监控", key=button_key, width='stretch'):
        
        # 获取价格
        if price is None:
            st.warning("无法获取股票价格，请手动输入。")
            return
        
        # 添加到监控
        success, message = profit_growth_monitor.add_stock(
            stock_code=stock_code,
            stock_name=stock_name,
            buy_price=price
        )
        
        if success:
            st.success(message)
        else:
            st.error(message)


def display_profit_growth_monitor_panel():
    """显示净利增长监控面板"""
    
    st.markdown("## 净利增长策略监控")
    st.markdown("---")
    
    # 获取监控中的股票
    monitoring_stocks = profit_growth_monitor.get_monitoring_stocks()
    
    # 标签页
    tab1, tab2, tab3 = st.tabs(["监控列表", "卖出提醒", "历史记录"])
    
    with tab1:
        display_monitoring_list(monitoring_stocks)
    
    with tab2:
        display_sell_alerts()
    
    with tab3:
        display_history()


def display_monitoring_list(stocks: List[Dict]):
    """显示监控列表"""
    
    st.markdown("### 持仓监控")
    
    if not stocks:
        st.info("监控列表为空，请先添加股票。")
        return
    
    st.info(f"当前监控 {len(stocks)} 只股票。")
    
    for stock in stocks:
        with st.expander(f"{stock['stock_code']} {stock['stock_name']}", expanded=False):
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.markdown(f"**买入价格**: {stock['buy_price']:.2f}元")
                st.markdown(f"**买入日期**: {stock['buy_date']}")
            
            with col2:
                st.markdown(f"**持股天数**: {stock['holding_days']}天")
                st.markdown(f"**加入时间**: {stock['add_time']}")
            
            with col3:
                if st.button("移除", key=f"remove_{stock['stock_code']}", width='stretch'):
                    success, msg = profit_growth_monitor.remove_stock(stock['stock_code'], "手动移除")
                    if success:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)


def display_sell_alerts():
    """显示卖出提醒"""
    
    st.markdown("### 卖出提醒")
    
    alerts = profit_growth_monitor.get_unprocessed_alerts()
    
    if not alerts:
        st.info("暂无新的卖出提醒。")
        return
    
    st.warning(f"有 {len(alerts)} 条待处理提醒。")
    
    for alert in alerts:
        with st.container():
            st.markdown("---")
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"### {alert['stock_code']} {alert['stock_name']}")
                st.markdown(f"**提醒类型**: {alert['alert_type']}")
                st.markdown(f"**提醒原因**: {alert['alert_reason']}")
                st.markdown(f"**提醒时间**: {alert['alert_time']}")
            
            with col2:
                st.markdown("#### 市场数据")
                
                current_price = alert.get('current_price')
                if current_price is not None:
                    try:
                        price_val = float(current_price)
                        st.markdown(f"**当前价格**: {price_val:.2f}元")
                    except (ValueError, TypeError):
                        st.markdown(f"**当前价格**: {current_price}")
                
                holding_days = alert.get('holding_days')
                if holding_days is not None:
                    st.markdown(f"**持有天数**: {holding_days}天")


def display_history():
    """显示历史记录"""
    
    st.markdown("### 历史记录")
    
    # 子标签
    sub_tab1, sub_tab2 = st.tabs(["提醒历史", "移除历史"])
    
    with sub_tab1:
        alerts = profit_growth_monitor.get_all_alerts(50)
        if alerts:
            st.info(f"共 {len(alerts)} 条提醒记录。")
            for alert in alerts:
                st.markdown(f"- **{alert['alert_time']}** | {alert['stock_code']} {alert['stock_name']} | {alert['alert_type']}")
        else:
            st.info("暂无提醒历史。")
    
    with sub_tab2:
        removed = profit_growth_monitor.get_removed_stocks(50)
        if removed:
            st.info(f"共 {len(removed)} 条移除记录。")
            for stock in removed:
                st.markdown(f"- **{stock['remove_time']}** | {stock['stock_code']} {stock['stock_name']} | {stock['remove_reason']}")
        else:
            st.info("暂无移除历史。")


def is_valid_value(value):
    """判断值是否有效"""
    if value is None:
        return False
    if pd.isna(value):
        return False
    if str(value).strip() in ['', 'N/A', 'nan', 'None']:
        return False
    return True


def format_value(value, suffix=''):
    """格式化显示值"""
    if isinstance(value, (int, float)):
        if abs(value) >= 100000000:  # 亿
            return f"{value/100000000:.2f}亿{suffix}"
        elif abs(value) >= 10000:  # 万
            return f"{value/10000:.2f}万{suffix}"
        else:
            return f"{value:.2f}{suffix}"
    return f"{value}{suffix}"


def get_suffix(field_name: str) -> str:
    """获取字段后缀"""
    suffix_map = {
        '净利润增长率': '%',
        '成交额': '元',
        '股价': '元',
    }
    return suffix_map.get(field_name, '')


def send_dingtalk_notification(stocks_df: pd.DataFrame, filter_summary: str | None = None):
    """发送钉钉通知"""
    
    try:
        if not notification_service.config['webhook_enabled']:
            st.warning("Webhook通知未启用，请在系统配置中启用。")
            return
        
        # 构建消息
        keyword = notification_service.config.get('webhook_keyword', 'aiagents通知')
        
        message_text = f"### {keyword} - 净利增长选股完成\n\n"
        if filter_summary:
            message_text += f"**筛选策略**: {filter_summary}\n\n"
        message_text += f"**筛选数量**: {len(stocks_df)} 只\n\n"
        message_text += "**精选股票**:\n\n"
        
        for idx, row in stocks_df.iterrows():
            stock_code = row.get('股票代码', 'N/A')
            stock_name = row.get('股票简称', 'N/A')
            message_text += f"{idx+1}. {stock_code} {stock_name}\n\n"
        
        message_text += f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message_text += "_此消息由AI股票分析系统自动发送_"
        
        # 直接发送钉钉Webhook
        if notification_service.config['webhook_type'] == 'dingtalk':
            import requests
            
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"{keyword}",
                    "text": message_text
                }
            }
            
            response = requests.post(
                notification_service.config['webhook_url'],
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                st.success("钉钉通知发送成功。")
            else:
                st.error(f"钉钉通知发送失败: HTTP {response.status_code}")
        else:
            st.warning("当前仅支持钉钉通知。")
    
    except Exception as e:
        st.error(f"发送通知失败: {str(e)}")
