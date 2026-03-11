"""
智能盯盘 - UI界面
集成到主程序的智能盯盘功能界面
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import html
import logging
import re
from typing import Dict, List, Optional
from dotenv import load_dotenv
import config

from investment_db_utils import DEFAULT_ACCOUNT_NAME
from smart_monitor_engine import SmartMonitorEngine
from smart_monitor_db import SmartMonitorDB
from config_manager import config_manager  # 使用主程序的配置管理器
from portfolio_manager import portfolio_manager
from ui_analysis_task_utils import (
    consume_finished_ui_analysis_task,
    get_active_ui_analysis_task,
    get_ui_analysis_button_state,
    render_ui_analysis_task_live_card,
    start_ui_analysis_task,
)
from ui_state_keys import (
    INVESTMENT_AI_TASK_PREFILL_KEY,
    INVESTMENT_PRICE_ALERT_PREFILL_KEY,
    PORTFOLIO_ADD_ACCOUNT_NAME_KEY,
    PORTFOLIO_ADD_ORIGIN_ANALYSIS_ID_KEY,
    SMART_MONITOR_ACTIVE_TAB_KEY,
    SMART_MONITOR_DB_KEY,
    SMART_MONITOR_ENGINE_KEY,
)
from ui_shared import (
    A_SHARE_DOWN_COLOR,
    A_SHARE_UP_COLOR,
    NON_MARKET_PALETTE,
    format_price,
    get_dataframe_height,
    get_action_color,
    get_market_color,
    render_a_share_change_metric,
)


# 加载环境变量
load_dotenv()

SMART_MONITOR_MANUAL_ANALYSIS_TASK_TYPE = "smart_monitor_manual_analysis"
SMART_MONITOR_MANUAL_ANALYSIS_DONE_KEY = "smart_monitor_manual_analysis_last_handled"


def _coerce_interval_setting(raw_value, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _get_default_ai_interval_minutes() -> int:
    return _coerce_interval_setting(
        getattr(config, "SMART_MONITOR_AI_INTERVAL_MINUTES", 60),
        default=60,
        minimum=1,
        maximum=240,
    )


def _get_default_alert_interval_minutes() -> int:
    return _coerce_interval_setting(
        getattr(config, "SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES", 3),
        default=3,
        minimum=3,
        maximum=120,
    )


def _get_default_position_size_pct() -> int:
    return _coerce_interval_setting(
        getattr(config, "SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT", 20),
        default=20,
        minimum=5,
        maximum=50,
    )


def _get_default_stop_loss_pct() -> int:
    return _coerce_interval_setting(
        getattr(config, "SMART_MONITOR_DEFAULT_STOP_LOSS_PCT", 5),
        default=5,
        minimum=1,
        maximum=20,
    )


def _get_default_take_profit_pct() -> int:
    return _coerce_interval_setting(
        getattr(config, "SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT", 10),
        default=10,
        minimum=1,
        maximum=30,
    )


def _apply_default_settings_to_existing_items(
    db: SmartMonitorDB,
    ai_interval_minutes: int,
    alert_interval_minutes: int,
) -> tuple[int, int, int]:
    ai_updated = 0
    alert_updated = 0
    task_defaults_updated = 0

    for task in db.get_monitor_tasks(enabled_only=False):
        task_updates = {
            "account_name": task.get("account_name"),
            "asset_id": task.get("asset_id"),
            "portfolio_stock_id": task.get("portfolio_stock_id"),
        }
        interval_changed = int(task.get("check_interval") or 0) != ai_interval_minutes * 60
        params_changed = any(
            int(task.get(key) or 0) != value
            for key, value in (
                ("position_size_pct", _get_default_position_size_pct()),
                ("stop_loss_pct", _get_default_stop_loss_pct()),
                ("take_profit_pct", _get_default_take_profit_pct()),
            )
        )
        if interval_changed:
            task_updates["check_interval"] = ai_interval_minutes * 60
        if params_changed:
            task_updates["position_size_pct"] = _get_default_position_size_pct()
            task_updates["stop_loss_pct"] = _get_default_stop_loss_pct()
            task_updates["take_profit_pct"] = _get_default_take_profit_pct()
        if interval_changed or params_changed:
            if db.update_monitor_task(task["stock_code"], task_updates):
                if interval_changed:
                    ai_updated += 1
                if params_changed:
                    task_defaults_updated += 1

    for item in db.monitoring_repository.list_items(monitor_type="price_alert"):
        if int(item.get("interval_minutes") or 0) == alert_interval_minutes:
            continue
        if db.monitoring_repository.update_item(int(item["id"]), {"interval_minutes": alert_interval_minutes}):
            alert_updated += 1

    return ai_updated, alert_updated, task_defaults_updated


def _legacy_smart_monitor_ui(lightweight_model=None, reasoning_model=None):
    """智能盯盘主界面"""

    # 使用说明
    with st.expander("快速使用指南", expanded=False):
        st.caption("基于 DeepSeek 的 A 股 AI 盯盘与价格提醒系统")
        st.markdown("""
        ### 快速开始
        
        **第一步：环境配置**
        1. 点击左侧菜单"系统配置"
        2. 填写 DeepSeek API Key（必需）
        3. 如需更快行情可配置 TDX 数据源（可选）
        
        **第二步：开始使用**
        - **盯盘列表**：管理 AI 盯盘任务，手工触发分析并处理信号
        - **监控任务**：添加股票到监控列表，定时生成提醒和待处理动作
        - **持仓管理**：查看资产账本中的持仓，并结合 AI 信号手工登记交易
        
        ---
        
        ### 核心功能
        
        | 功能 | 说明 |
        |------|------|
        | **监控任务** | 定时自动分析目标股票，生成提醒和待处理动作 |
        | **持仓管理** | 基于资产账本记录持仓成本，AI决策会考虑当前持仓情况 |
        | **历史记录** | 查看最新监测事件、系统状态与提醒记录 |
        | **系统设置** | 配置API、数据源和通知等 |
        
        ---
        
        ### AI决策逻辑
        
        **买入信号**（至少满足3个）：
        1. 趋势向上：价格 > MA5 > MA20 > MA60（多头排列）
        2. 量价配合：成交量 > 5日均量的120%（放量上涨）
        3. MACD金叉：MACD > 0 且DIF上穿DEA
        4. RSI健康：RSI在50-70区间（不超买不超卖）
        5. 突破关键位：突破前期高点或重要阻力位
        6. 布林带位置：价格接近布林中轨上方，有上行空间
        
        **卖出信号**（满足任一立即卖出）：
        1. 止损触发：亏损 ≥ -5%（明天开盘立即卖出）
        2. 止盈触发：盈利 ≥ +10%（锁定收益）
        3. 趋势转弱：跌破MA20/MA60，MACD死叉
        4. 放量下跌：成交量放大但价格下跌
        5. 技术破位：跌破重要支撑位
        
        ---
        
        ### A股T+1规则
        
        **关键限制**：
        - 今天买入的股票，**今天不能卖出**
        - 必须等到下一个交易日才能卖出
        - 系统会自动检查并遵守T+1规则
        
        **建议**：
        - **宁可错过，不可做错** - 买入前务必确认趋势
        - 单只股票仓位 ≤ 30%（T+1风险较大）
        - 止损位：-5%（明天开盘立即执行）
        - 止盈位：+8-15%（分批止盈）
        
        ---
        
        ### 使用技巧
        
        **新手建议**：
        1. 先把 AI 提醒当作辅助判断，不要直接重仓
        2. 小仓位试水（建议5-10%）
        3. 严格执行止损，不要心存侥幸
        4. 关注交易时段（9:30-11:30, 13:00-15:00）
        
        **高级功能**：
        - 在资产账本中维护持仓后，AI 会自动读取持仓状态
        - AI 会结合最新战略基线和当前持仓给出更准确建议
        - 可设置多个监控任务，同时盯盘多只股票
        
        ---
        
        ### 常见问题
        
        **Q: 提示"DeepSeek API调用失败"？**
        - 检查API Key是否正确
        - 确认API账户余额充足
        - 检查网络连接
        
        **Q: 数据显示为0或获取失败？**
        - 可能是非交易时间
        - AKShare接口可能暂时不可用
        - 尝试更换股票代码测试
        
        **Q: 系统会自动下单吗？**
        - 不会
        - AI 盯盘和价格预警都只会生成提醒和待处理动作
        - 交易需要你手工确认并登记
        
        ---
        
        ### 风险提示
        
        1. **股市有风险，投资需谨慎**
        2. AI决策仅供参考，不构成投资建议
        3. 建议先用少量仓位验证自己的执行纪律
        4. 严格控制仓位，不要满仓操作
        5. 不要投入超过承受能力的资金
        
        ---
        
        **祝您交易顺利。如有问题，请查看详细文档或联系技术支持。**
        """)
    
    st.markdown("---")
    
    if lightweight_model is None:
        lightweight_model = st.session_state.get('selected_lightweight_model', config.LIGHTWEIGHT_MODEL_NAME)
    if reasoning_model is None:
        reasoning_model = st.session_state.get('selected_reasoning_model', config.REASONING_MODEL_NAME)

    # 初始化组件（自动从配置读取）
    if SMART_MONITOR_ENGINE_KEY not in st.session_state:
        try:
            # SmartMonitorEngine会自动从config_manager读取配置
            st.session_state[SMART_MONITOR_ENGINE_KEY] = SmartMonitorEngine(
                lightweight_model=lightweight_model,
                reasoning_model=reasoning_model,
            )
            st.session_state[SMART_MONITOR_DB_KEY] = SmartMonitorDB()
        except Exception as e:
            st.error(f"初始化失败: {e}")
            st.error("请先在'环境配置'中完成基础配置")
            return
    else:
        st.session_state[SMART_MONITOR_ENGINE_KEY].set_model_overrides(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
    
    # 创建标签页
    tabs = st.tabs([
        "监控任务", 
        "持仓管理", 
        "历史记录",
        "系统设置"
    ])
    
    # 标签页1: 监控任务
    with tabs[0]:
        render_monitor_tasks()
    
    # 标签页2: 持仓管理
    with tabs[1]:
        render_position_management()
    
    # 标签页3: 历史记录
    with tabs[2]:
        render_history()
    
    # 标签页4: 系统设置
    with tabs[3]:
        render_settings()


def render_realtime_analysis(show_header: bool = True, title: str = "实时分析"):
    """实时分析界面"""

    if show_header:
        st.header(title)
    else:
        st.markdown(f"#### {title}")
    
    col1, col2 = st.columns([2.4, 1.2])
    
    with col1:
        stock_code = st.text_input(
            "输入股票代码",
            placeholder="例如: 600519",
            help="输入6位股票代码"
        )
    
    with col2:
        st.caption("执行模式")
        st.info("手工执行")
    
    if st.button("开始分析", type="primary"):
        if not stock_code:
            st.error("请输入股票代码")
            return
        
        if len(stock_code) != 6 or not stock_code.isdigit():
            st.error("股票代码格式错误，请输入6位数字")
            return
        
        # 显示进度
        with st.spinner('正在分析...'):
            engine = st.session_state[SMART_MONITOR_ENGINE_KEY]
            result = engine.analyze_stock(
                stock_code=stock_code,
                notify=True
            )
        
        if result['success']:
            # 显示分析结果
            display_analysis_result(result)
        else:
            st.error(f"分析失败: {result.get('error')}")


def display_analysis_result(result: dict):
    """显示分析结果"""
    
    stock_code = result['stock_code']
    stock_name = result['stock_name']
    decision = result['decision']
    market_data = result['market_data']
    session_info = result['session_info']
    
    st.success(f"分析完成: {stock_code} {stock_name}")
    
    # 交易时段信息
    st.info(f"当前时段: {session_info['session']} - {session_info['recommendation']}")
    
    # AI决策
    st.markdown("### AI决策")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # 决策动作
    action = decision['action']
    action_color = get_action_color(action)
    
    col1.markdown(
        f"""
        <div class="decision-card">
            <strong style="color: {action_color};">{action}</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col2.metric("信心度", f"{decision['confidence']}%")
    col3.metric("风险等级", decision.get('risk_level', 'N/A'))
    col4.metric("建议仓位", f"{decision.get('position_size_pct', 0)}%")
    
    # 决策理由
    st.markdown("**决策理由:**")
    st.text_area("决策理由", decision['reasoning'], height=150, disabled=True, label_visibility="hidden")
    
    # 市场数据
    st.markdown("### 市场数据")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("当前价", f"¥{market_data.get('current_price', 0):.2f}")
    with col2:
        render_a_share_change_metric("涨跌幅", market_data.get('change_pct', 0))
    col3.metric("成交量", f"{market_data.get('volume', 0):,.0f}手")
    col4.metric("换手率", f"{market_data.get('turnover_rate', 0):.2f}%")
    
    # 技术指标
    st.markdown("### 技术指标")
    
    tech_col1, tech_col2, tech_col3 = st.columns(3)
    
    with tech_col1:
        st.markdown("**均线系统**")
        st.write(f"MA5: ¥{market_data.get('ma5', 0):.2f}")
        st.write(f"MA20: ¥{market_data.get('ma20', 0):.2f}")
        st.write(f"MA60: ¥{market_data.get('ma60', 0):.2f}")
        st.write(f"趋势: {market_data.get('trend', 'N/A')}")
    
    with tech_col2:
        st.markdown("**动量指标**")
        st.write(f"MACD: {market_data.get('macd', 0):.4f}")
        st.write(f"DIF: {market_data.get('macd_dif', 0):.4f}")
        st.write(f"DEA: {market_data.get('macd_dea', 0):.4f}")
    
    with tech_col3:
        st.markdown("**摆动指标**")
        st.write(f"RSI(6): {market_data.get('rsi6', 0):.2f}")
        st.write(f"RSI(12): {market_data.get('rsi12', 0):.2f}")
        st.write(f"RSI(24): {market_data.get('rsi24', 0):.2f}")
    
    # 主力资金（已禁用 - 接口不稳定）
    # if 'main_force' in market_data:
    #     st.markdown("### 主力资金")
    #     mf = market_data['main_force']
    #     
    #     mf_col1, mf_col2, mf_col3 = st.columns(3)
    #     mf_col1.metric("主力净额", f"{mf['main_net']:,.2f}万", 
    #                   delta=f"{mf['main_net_pct']:+.2f}%")
    #     mf_col2.metric("超大单", f"{mf['super_net']:,.2f}万")
    #     mf_col3.metric("大单", f"{mf['big_net']:,.2f}万")
    #     
    #     st.info(f"主力动向: {mf['trend']}")
    
    # 执行结果（如果有）
    if result.get('execution_result'):
        exec_result = result['execution_result']
        st.markdown("### 执行结果")
        
        if exec_result.get('success'):
            st.success(exec_result.get('message', '执行成功'))
        else:
            st.error(exec_result.get('error', '执行失败'))


def render_monitor_tasks():
    """监控任务界面"""
    
    st.header("监控任务管理")
    
    db = st.session_state[SMART_MONITOR_DB_KEY]
    engine = st.session_state[SMART_MONITOR_ENGINE_KEY]
    
    # 添加新任务
    with st.expander("添加新监控任务", expanded=True):
        # 改回使用form，确保值正确提交
        with st.form("add_monitor_task_form", clear_on_submit=False):
            col1, col2 = st.columns(2)
            
            with col1:
                task_name = st.text_input("任务名称", placeholder="例如: 茅台盯盘")
                stock_code = st.text_input("股票代码", placeholder="例如: 600519")
                check_interval = st.slider("检查间隔(秒)", 60, 3600, 300)
                
                # 持仓信息
                st.markdown("---")
                st.markdown("**持仓信息**")
                has_position = st.checkbox("已持仓该股票", value=False,
                                          help="勾选后可填写持仓成本和数量，AI会考虑持仓情况")
                
                # 注意：在form内部，复选框的变化要到提交后才能看到
                # 所以持仓输入框始终显示，用户可以选择填写或不填写
                position_cost = st.number_input("持仓成本(元)", min_value=0.01, value=10.0, step=0.01,
                                               help="如果已持仓，填写买入时的成本价格（未持仓可忽略）")
                position_quantity = st.number_input("持仓数量(股)", min_value=100, value=100, step=100,
                                                   help="如果已持仓，填写持有的股票数量（未持仓可忽略）")
            
            with col2:
                st.caption("执行模式：手工确认")
                trading_hours_only = st.checkbox(
                    "仅交易时段监控", 
                    value=True,
                    help="开启后，只在交易日的交易时段（9:30-11:30, 13:00-15:00）进行AI分析"
                )
                position_size = st.slider("仓位百分比(%)", 5, 50, 20,
                                         help="新建仓位时使用的资金比例")
            
            # 添加任务按钮（表单提交按钮）
            submitted = st.form_submit_button("添加任务", type="primary", width='stretch')
        
        if submitted:
            # 验证必填项（form中直接使用局部变量）
            if not task_name or not stock_code:
                st.error("请填写必填项：任务名称和股票代码。")
            else:
                
                try:
                    # 检查是否已存在该股票的监控任务
                    existing_tasks = db.get_monitor_tasks(enabled_only=False)
                    existing_task = next((t for t in existing_tasks if t['stock_code'] == stock_code), None)
                    
                    if existing_task:
                        st.error(f"股票代码 {stock_code} 已存在监控任务。")
                        st.warning(f"任务名称: {existing_task['task_name']}")
                        st.info("请在下方任务列表中找到该任务，点击启动或删除后重新添加。")
                    else:
                        # 创建任务（初始状态为禁用，需要用户手动启动）
                        task_data = {
                            'task_name': task_name,
                            'stock_code': stock_code,
                            'enabled': 0,  # 关键修改：初始状态为禁用，不自动启动
                            'check_interval': check_interval,
                            'trading_hours_only': 1 if trading_hours_only else 0,
                            'position_size_pct': position_size,
                            'has_position': 1 if has_position else 0,
                            'position_cost': position_cost if has_position else 0,
                            'position_quantity': position_quantity if has_position else 0,
                            'position_date': datetime.now().strftime('%Y-%m-%d') if has_position else None
                        }
                        
                        task_id = db.add_monitor_task(task_data)
                        
                        st.success(f"任务创建成功。ID: {task_id}")
                        if has_position:
                            st.info(f"已记录持仓: {position_quantity}股 @ {position_cost:.2f}元")
                        st.info("任务已创建但未启动，请在下方任务列表中点击“启动”开始监控。")
                        
                        st.rerun()
                except Exception as e:
                    error_msg = str(e)
                    if "UNIQUE constraint failed" in error_msg:
                        st.error(f"股票代码 {stock_code} 已存在监控任务。")
                        st.info("请在下方任务列表中找到该任务。")
                    else:
                        st.error(f"创建失败: {error_msg}")
    
    # 显示任务列表
    st.markdown("### 监控任务列表")
    
    tasks = db.get_monitor_tasks(enabled_only=False)
    valid_codes = {task['stock_code'] for task in tasks}
    for running_code in list(engine.monitoring_stocks):
        if running_code not in valid_codes:
            engine.stop_monitor(running_code)
    
    if not tasks:
        st.info("暂无监控任务，点击上方'添加新监控任务'创建")
        return
    
    for task in tasks:
        with st.container():
            # 获取实时价格计算盈亏
            has_position = task.get('has_position', 0)
            position_cost = task.get('position_cost', 0)
            position_quantity = task.get('position_quantity', 0)
            
            # 尝试获取当前价格
            current_price = 0
            profit_loss = 0
            profit_loss_pct = 0
            
            if has_position and position_cost > 0 and position_quantity > 0:
                try:
                    # 获取实时行情
                    from smart_monitor_data import SmartMonitorDataFetcher
                    data_fetcher = SmartMonitorDataFetcher()
                    quote = data_fetcher.get_realtime_quote(task['stock_code'], retry=1)
                    if quote:
                        current_price = quote.get('current_price', 0)
                        if current_price > 0:
                            # 计算盈亏
                            cost_total = position_cost * position_quantity
                            current_total = current_price * position_quantity
                            profit_loss = current_total - cost_total
                            profit_loss_pct = (profit_loss / cost_total) * 100
                except Exception as e:
                    pass

            is_running = task['stock_code'] in engine.monitoring_stocks
            status = "已启用" if task['enabled'] else "已禁用"
            trading_mode = "仅交易时段" if task.get('trading_hours_only', 1) else "全时段"
            position_text = (
                f"持仓: {position_quantity}股 @ {position_cost:.2f}元"
                if has_position else
                "当前无持仓记录"
            )
            if current_price > 0:
                price_text = format_price(current_price, currency="¥")
            else:
                price_text = "等待行情"

            with st.container():
                header_col, status_col = st.columns([4, 1.4])

                with header_col:
                    st.markdown(f"**{task['task_name']}**")
                    st.caption(f"{task['stock_code']} · 间隔 {task['check_interval']} 秒")
                    if task.get('managed_by_portfolio'):
                        st.caption("来源: 持仓同步")

                with status_col:
                    st.caption("运行状态")
                    st.markdown(
                        f"<div style='text-align:right; font-weight:600;'>{'运行中' if is_running else '未运行'}</div>",
                        unsafe_allow_html=True,
                    )

                info_col1, info_col2 = st.columns(2)

                with info_col1:
                    st.caption(f"任务状态: {status}")
                    st.caption(f"模式: 手工确认 · {trading_mode}")
                    st.caption(position_text)

                with info_col2:
                    st.caption(f"当前价格: {price_text}")
                    if has_position and current_price > 0:
                        profit_color = get_market_color(profit_loss)
                        st.markdown(
                            f"<span style='color:{profit_color}; font-weight:600;'>"
                            f"浮动盈亏: {profit_loss:+.2f}元 ({profit_loss_pct:+.2f}%)"
                            "</span>",
                            unsafe_allow_html=True,
                        )
                    elif has_position:
                        st.caption("浮动盈亏: 等待行情")
                    else:
                        st.caption("浮动盈亏: 不适用")

                action_col1, action_col2 = st.columns(2)

                with action_col1:
                    if is_running:
                        if st.button("停止", key=f"stop_{task['id']}"):
                            engine.stop_monitor(task['stock_code'])
                            # 停止时更新数据库状态为禁用
                            db.update_monitor_task(task['stock_code'], {'enabled': 0})
                            st.success("已停止")
                            st.rerun()
                    else:
                        # 启动按钮始终可点击（只要任务未运行）
                        if st.button("启动", key=f"start_{task['id']}"):
                            # 启动监控
                            engine.start_monitor(
                                stock_code=task['stock_code'],
                                check_interval=task['check_interval'],
                                notify=True,
                                has_position=has_position == 1,
                                position_cost=position_cost,
                                position_quantity=position_quantity,
                                trading_hours_only=task.get('trading_hours_only', 1) == 1
                            )
                            # 启动时更新数据库状态为启用
                            db.update_monitor_task(task['stock_code'], {'enabled': 1})
                            st.success("已启动")
                            st.rerun()

                with action_col2:
                    if st.button("删除", key=f"del_{task['id']}"):
                        # 如果正在运行，先停止
                        if task['stock_code'] in engine.monitoring_stocks:
                            engine.stop_monitor(task['stock_code'])

                        db.delete_monitor_task(task['id'])
                        st.success("已删除")
                        st.rerun()

                # K线图和AI决策详情（可展开）
                with st.expander(f"K线图与AI决策 - {task['task_name']}", expanded=False):
                    _render_task_kline_and_decisions(task, db, engine)

                st.markdown(
                    "<div style='margin:0.45rem 0 0.7rem 0; border-bottom:1px solid rgba(148,163,184,0.18);'></div>",
                    unsafe_allow_html=True,
                )


def render_position_management():
    """持仓管理界面"""
    
    st.header("持仓管理")
    
    engine = st.session_state[SMART_MONITOR_ENGINE_KEY]
    positions = [
        stock for stock in portfolio_manager.get_all_stocks(auto_monitor_only=False)
        if int(stock.get("quantity") or 0) > 0
    ]
    if not positions:
        st.info("当前资产账本中无持仓。")
        return

    from smart_monitor_data import SmartMonitorDataFetcher

    fetcher = SmartMonitorDataFetcher()
    rows = []
    total_value = 0.0
    total_profit_loss = 0.0
    priced_positions = 0

    for stock in positions:
        quantity = int(stock.get("quantity") or 0)
        cost_price = float(stock.get("cost_price") or 0)
        current_price = None
        profit_loss = None
        profit_loss_pct = None
        market_value = float(cost_price * quantity)
        try:
            quote = fetcher.get_realtime_quote(stock["code"], retry=1)
            latest_price = float((quote or {}).get("current_price") or 0)
            if latest_price > 0:
                current_price = latest_price
                market_value = latest_price * quantity
                profit_loss = (latest_price - cost_price) * quantity if cost_price else None
                profit_loss_pct = ((latest_price - cost_price) / cost_price * 100) if cost_price else None
                priced_positions += 1
        except Exception:
            current_price = None
        total_value += market_value
        if profit_loss is not None:
            total_profit_loss += profit_loss
        rows.append(
            {
                "stock_code": stock["code"],
                "stock_name": stock.get("name", stock["code"]),
                "account_name": stock.get("account_name", DEFAULT_ACCOUNT_NAME),
                "quantity": quantity,
                "cost_price": cost_price or None,
                "current_price": current_price,
                "market_value": market_value,
                "profit_loss": profit_loss,
                "profit_loss_pct": profit_loss_pct,
                "source": "资产账本",
            }
        )

    st.markdown("### 账户概览")
    st.caption("数据来自资产账本与实时行情；系统不再连接券商实盘接口。")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("参考市值", f"¥{total_value:,.2f}")
    col2.metric("持仓数量", f"{len(rows)}个")
    col3.metric("已更新行情", f"{priced_positions}个")
    col4.metric("浮动盈亏", f"¥{total_profit_loss:,.2f}")
    
    st.markdown("### 持仓列表")
    
    # 转换为DataFrame
    df = pd.DataFrame(rows)
    
    # 显示表格
    st.dataframe(
        df[[
            'stock_code', 'stock_name', 'account_name', 'quantity',
            'cost_price', 'current_price', 'market_value', 'profit_loss', 'profit_loss_pct', 'source'
        ]],
        column_config={
            "stock_code": "代码",
            "stock_name": "名称",
            "account_name": "账户",
            "quantity": "持仓",
            "cost_price": "成本价",
            "current_price": "现价",
            "market_value": "参考市值",
            "profit_loss": "盈亏",
            "profit_loss_pct": "盈亏%",
            "source": "来源",
        },
        hide_index=True,
        width='stretch',
        height=get_dataframe_height(len(df), max_rows=40),
    )
    
    # 单只股票操作
    st.markdown("### 快速操作")
    
    selected_stock = st.selectbox(
        "选择股票",
        options=[f"{p['stock_code']} {p['stock_name']}" for p in rows]
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("AI分析", type="secondary"):
            stock_code = selected_stock.split()[0]
            with st.spinner("分析中..."):
                result = engine.analyze_stock(stock_code, notify=False)
                if result['success']:
                    display_analysis_result(result)
                else:
                    st.error(f"分析失败: {result.get('error')}")
    
    with col2:
        if st.button("交易提示", type="primary"):
            stock_code = selected_stock.split()[0]
            selected_position = next((p for p in rows if p['stock_code'] == stock_code), None)
            if selected_position:
                st.info("请到持仓管理中进行手工交易登记。")


def render_history(show_header: bool = True, title: str = "历史记录"):
    """历史记录界面"""

    if show_header:
        st.header(title)
    else:
        st.markdown(f"#### {title}")
    
    from monitor_service import monitor_service

    db = st.session_state[SMART_MONITOR_DB_KEY]
    monitor_service.ensure_started()
    monitor_service.ensure_stopped_if_idle()

    recent_notifications = db.monitoring_repository.get_all_recent_notifications(limit=200)
    notification_count = len(recent_notifications)
    notification_summary_col, notification_action_col = st.columns([5, 1.2])
    with notification_summary_col:
        st.caption(f"当前通知 {notification_count} 条。清除后不会影响 AI 决策记录。")
    with notification_action_col:
        if st.button(
            "清除通知",
            key="smart_monitor_clear_notifications",
            width="stretch",
            disabled=notification_count == 0,
            help="清空监测通知事件，不影响 AI 决策历史。",
        ):
            cleared_count = db.monitoring_repository.clear_all_notifications()
            st.success(f"已清除 {cleared_count} 条通知。")
            st.rerun()

    events = db.monitoring_repository.get_recent_events(limit=60)
    decisions = db.get_ai_decisions(limit=30)
    ai_events, monitor_events = _split_history_events(events)

    decision_tab, event_tab = st.tabs(["最新AI决策", "最新监测事件"])

    with decision_tab:
        if not decisions and not ai_events:
            st.info("暂无 AI 决策。")
        else:
            for decision in decisions:
                _render_ai_decision_notice(decision)
            for event in ai_events:
                _render_monitor_event_notice(event)

    with event_tab:
        if not monitor_events:
            st.info("暂无监测事件。")
        else:
            for event in monitor_events:
                _render_monitor_event_notice(event)


def _is_ai_decision_history_event(event: Dict) -> bool:
    event_type = str((event or {}).get("event_type") or "").strip().lower()
    return event_type == "ai_analysis"


def _split_history_events(events: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    ai_events: List[Dict] = []
    monitor_events: List[Dict] = []
    for event in events:
        if _is_ai_decision_history_event(event):
            ai_events.append(event)
        else:
            monitor_events.append(event)
    return ai_events, monitor_events


def render_settings(show_header: bool = True, title: str = "系统设置"):
    """智能盯盘设置界面。"""

    if show_header:
        st.header(title)
    else:
        st.markdown(f"#### {title}")
    
    db = st.session_state.get(SMART_MONITOR_DB_KEY)
    current_config = config_manager.read_env()
    current_ai_interval = _coerce_interval_setting(
        current_config.get("SMART_MONITOR_AI_INTERVAL_MINUTES"),
        default=_get_default_ai_interval_minutes(),
        minimum=1,
        maximum=240,
    )
    current_alert_interval = _coerce_interval_setting(
        current_config.get("SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES"),
        default=_get_default_alert_interval_minutes(),
        minimum=3,
        maximum=120,
    )

    current_position_size_pct = _coerce_interval_setting(
        current_config.get("SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT"),
        default=_get_default_position_size_pct(),
        minimum=5,
        maximum=50,
    )
    current_stop_loss_pct = _coerce_interval_setting(
        current_config.get("SMART_MONITOR_DEFAULT_STOP_LOSS_PCT"),
        default=_get_default_stop_loss_pct(),
        minimum=1,
        maximum=20,
    )
    current_take_profit_pct = _coerce_interval_setting(
        current_config.get("SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT"),
        default=_get_default_take_profit_pct(),
        minimum=1,
        maximum=30,
    )

    with st.form("smart_monitor_settings_form", clear_on_submit=False):
        ai_interval_minutes = st.slider(
            "轻 AI 分析默认间隔(分钟)",
            1,
            240,
            current_ai_interval,
            key="smart_monitor_settings_ai_interval",
            help="用于新建 AI 盯盘任务，以及持仓/盯盘自动托管时的默认值。",
        )
        alert_interval_minutes = st.slider(
            "价格预警默认间隔(分钟)",
            3,
            120,
            current_alert_interval,
            key="smart_monitor_settings_alert_interval",
            help="用于新建价格预警，以及自动托管价格预警时的默认值。",
        )
        position_size_pct = st.slider(
            "默认仓位百分比",
            5,
            50,
            current_position_size_pct,
            key="smart_monitor_settings_position_size_pct",
            help="用于新建 AI 盯盘任务的默认仓位建议。",
        )
        stop_loss_pct = st.slider(
            "默认止损百分比",
            1,
            20,
            current_stop_loss_pct,
            key="smart_monitor_settings_stop_loss_pct",
            help="用于新建 AI 盯盘任务的默认止损建议。",
        )
        take_profit_pct = st.slider(
            "默认止盈百分比",
            1,
            30,
            current_take_profit_pct,
            key="smart_monitor_settings_take_profit_pct",
            help="用于新建 AI 盯盘任务的默认止盈建议。",
        )
        apply_existing = st.checkbox(
            "同时同步当前已有的 AI 盯盘与价格预警",
            value=True,
            help="关闭后仅影响后续新建任务和自动托管投影。",
        )
        submitted = st.form_submit_button("保存智能盯盘设置", type="primary", width="stretch")

    if submitted:
        updates = {
            "SMART_MONITOR_AI_INTERVAL_MINUTES": str(ai_interval_minutes),
            "SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES": str(alert_interval_minutes),
            "SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT": str(position_size_pct),
            "SMART_MONITOR_DEFAULT_STOP_LOSS_PCT": str(stop_loss_pct),
            "SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT": str(take_profit_pct),
        }
        if config_manager.write_env(updates):
            config_manager.reload_config()
            ai_updated = 0
            alert_updated = 0
            task_defaults_updated = 0
            if apply_existing and db is not None:
                ai_updated, alert_updated, task_defaults_updated = _apply_default_settings_to_existing_items(
                    db,
                    ai_interval_minutes,
                    alert_interval_minutes,
                )
            st.success("智能盯盘设置已保存。")
            if apply_existing and db is not None:
                st.caption(f"已同步 {ai_updated} 个 AI 任务间隔、{task_defaults_updated} 个 AI 任务参数、{alert_updated} 个价格预警。")
            st.rerun()
        else:
            st.error("保存智能盯盘设置失败。")

    st.markdown("---")
    if st.button("重新加载配置", width="stretch"):
        config_manager.reload_config()
        st.success("配置已重新加载。")
        st.rerun()


def _render_task_kline_and_decisions(task: Dict, db: SmartMonitorDB, engine):
    """旧版任务详情保留占位，避免历史入口报错。"""
    st.info("新版 AI 工作台已简化此区域，如需单任务详情请使用“立即分析”或查看历史记录。")


def _ensure_smart_monitor_runtime(lightweight_model=None, reasoning_model=None):
    if lightweight_model is None:
        lightweight_model = st.session_state.get('selected_lightweight_model', config.LIGHTWEIGHT_MODEL_NAME)
    if reasoning_model is None:
        reasoning_model = st.session_state.get('selected_reasoning_model', config.REASONING_MODEL_NAME)

    if SMART_MONITOR_ENGINE_KEY not in st.session_state:
        st.session_state[SMART_MONITOR_ENGINE_KEY] = SmartMonitorEngine(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        st.session_state[SMART_MONITOR_DB_KEY] = SmartMonitorDB()
    else:
        st.session_state[SMART_MONITOR_ENGINE_KEY].set_model_overrides(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )

    return st.session_state[SMART_MONITOR_ENGINE_KEY], st.session_state[SMART_MONITOR_DB_KEY]


def _get_task_asset(db: SmartMonitorDB, task: Dict) -> Optional[Dict]:
    asset_id = task.get("asset_id")
    if asset_id:
        asset = db.asset_repository.get_asset(asset_id)
        if asset:
            return asset
    stock_code = task.get("stock_code")
    if not stock_code:
        return None
    return db.asset_repository.get_asset_by_symbol(
        stock_code,
        task.get("account_name") or DEFAULT_ACCOUNT_NAME,
    )


def _get_task_price_alert_item(db: SmartMonitorDB, task: Dict) -> Optional[Dict]:
    symbol = task.get("stock_code")
    if not symbol:
        return None
    account_name = task.get("account_name") or DEFAULT_ACCOUNT_NAME
    asset_id = task.get("asset_id")
    portfolio_stock_id = task.get("portfolio_stock_id")
    item = db.monitoring_repository.get_item_by_symbol(
        symbol,
        monitor_type="price_alert",
        account_name=account_name,
        asset_id=asset_id,
        portfolio_stock_id=portfolio_stock_id,
    )
    if item:
        return item
    return db.monitoring_repository.get_item_by_symbol(
        symbol,
        monitor_type="price_alert",
        account_name=account_name,
    )


def _resolve_price_alert_levels(alert_item: Optional[Dict]) -> Optional[Dict[str, object]]:
    if not alert_item:
        return None
    config_data = alert_item.get("config") or {}
    runtime_thresholds = config_data.get("runtime_thresholds")
    runtime_complete = isinstance(runtime_thresholds, dict) and all(
        runtime_thresholds.get(key) not in (None, "")
        for key in ("entry_min", "entry_max", "take_profit", "stop_loss")
    )
    if runtime_complete:
        return {
            "entry_min": runtime_thresholds.get("entry_min"),
            "entry_max": runtime_thresholds.get("entry_max"),
            "take_profit": runtime_thresholds.get("take_profit"),
            "stop_loss": runtime_thresholds.get("stop_loss"),
            "source": config_data.get("threshold_source") or "runtime_thresholds",
        }

    entry_range = config_data.get("entry_range") or {}
    if any(
        config_data.get(key) not in (None, "") for key in ("take_profit", "stop_loss")
    ) or any(entry_range.get(key) not in (None, "") for key in ("min", "max")):
        return {
            "entry_min": entry_range.get("min"),
            "entry_max": entry_range.get("max"),
            "take_profit": config_data.get("take_profit"),
            "stop_loss": config_data.get("stop_loss"),
            "source": config_data.get("threshold_source") or "strategy_context",
        }
    return None


def _get_latest_ai_decision_for_task(db: SmartMonitorDB, task: Dict) -> Optional[Dict]:
    stock_code = task.get("stock_code")
    if not stock_code:
        return None
    account_name = task.get("account_name") or DEFAULT_ACCOUNT_NAME
    asset_id = task.get("asset_id")
    decisions = db.get_ai_decisions(stock_code=stock_code, limit=10)
    for decision in decisions:
        account_info = decision.get("account_info") or {}
        decision_account_name = account_info.get("account_name")
        decision_asset_id = account_info.get("asset_id")
        if decision_account_name and decision_account_name != account_name:
            continue
        if asset_id and decision_asset_id and int(decision_asset_id) != int(asset_id):
            continue
        return decision
    return decisions[0] if decisions else None


def _format_asset_status(asset: Optional[Dict]) -> str:
    if not asset:
        return "未建资产"
    raw_status = str(asset.get("status") or asset.get("asset_status") or "").lower()
    return {
        "research": "研究池",
        "watchlist": "盯盘池",
        "portfolio": "持仓中",
    }.get(raw_status, raw_status or "未知")


def _format_position_summary(asset: Optional[Dict]) -> str:
    if not asset:
        return "尚未创建资产记录"
    quantity = int(asset.get("quantity") or 0)
    cost_price = float(asset.get("cost_price") or 0)
    raw_status = str(asset.get("status") or asset.get("asset_status") or "").lower()
    if raw_status == "portfolio" and quantity > 0 and cost_price > 0:
        return f"{quantity} 股 @ {cost_price:.3f}"
    return "当前无持仓"


def _get_pending_actions_for_task(db: SmartMonitorDB, task: Dict) -> List[Dict]:
    asset_id = task.get("asset_id")
    if not asset_id:
        return []
    return db.get_pending_actions(
        status="pending",
        account_name=task.get("account_name") or DEFAULT_ACCOUNT_NAME,
        asset_id=asset_id,
        limit=20,
    )


def _format_decision_action(action: Optional[str]) -> str:
    return {
        "BUY": "买入",
        "SELL": "卖出",
        "HOLD": "持有",
    }.get(str(action or "").upper(), str(action or "-"))


def _format_latest_decision_label(decision: Dict) -> str:
    confidence = decision.get("confidence")
    try:
        confidence_text = f"{int(round(float(confidence)))}%"
    except (TypeError, ValueError):
        confidence_text = "0%"
    return f"{_format_decision_action(decision.get('action'))}({confidence_text}) | {decision.get('decision_time') or '-'}"


def _format_decision_reasoning_brief(reasoning: object, max_length: int = 36) -> str:
    text = " ".join(str(reasoning or "").split())
    if not text:
        return ""

    sentences = [
        sentence.strip(" ，,:：")
        for sentence in re.split(r"[\u3002\uff1b;\uff01\uff1f!?]", text)
        if sentence.strip(" ，,:：")
    ]
    if not sentences:
        sentences = [text]

    action_keyword_weights = {
        "\u5efa\u8bae": 10,
        "\u7ee7\u7eed\u6301\u6709": 12,
        "\u6682\u4e0d\u64cd\u4f5c": 10,
        "\u6e05\u4ed3": 12,
        "\u5356\u51fa": 11,
        "\u4e70\u5165": 8,
        "\u51cf\u4ed3": 10,
        "\u52a0\u4ed3": 7,
        "\u79bb\u573a": 11,
        "\u89c2\u671b": 8,
    }
    action_keywords = tuple(action_keyword_weights.keys())
    context_keywords = (
        "\u672a\u89e6\u53d1",
        "\u5df2\u89e6\u53d1",
        "\u5df2\u8d85\u8fc7",
        "\u63a5\u8fd1",
        "\u8dcc\u7834",
        "\u8fbe\u5230",
        "\u4ed3\u4f4d",
        "\u4e0a\u9650",
        "\u6b62\u635f\u4f4d",
        "\u6b62\u76c8\u4f4d",
        "\u6b62\u635f\u7ebf",
        "\u6b62\u76c8\u7ebf",
        "\u6b63\u5e38\u5356\u51fa",
        "\u65e0\u6cd5\u52a0\u4ed3",
        "\u53ef\u7528\u8d44\u91d1",
        "\u98ce\u63a7",
        "\u98ce\u9669",
        "\u6d6e\u76c8",
        "\u6d6e\u4e8f",
        "\u6b62\u635f",
        "\u6b62\u76c8",
    )
    background_keywords = (
        "\u76d8\u540e",
        "\u65e0\u6cd5\u8fdb\u884c\u4ea4\u6613",
        "\u7b49\u5f85\u660e\u65e5\u5f00\u76d8",
        "\u7b49\u5f85\u6b21\u65e5\u5f00\u76d8",
        "\u590d\u76d8\u65f6\u6bb5",
    )
    monitoring_keywords = (
        "\u5f85",
        "\u89c2\u5bdf",
        "\u5173\u6ce8",
        "\u8ddf\u8e2a",
        "\u51fa\u73b0",
    )

    def _score_text(chunk: str) -> int:
        score = 0
        for keyword, weight in action_keyword_weights.items():
            if keyword in chunk:
                score += weight
        for keyword in context_keywords:
            if keyword in chunk:
                score += 4
        if any(char.isdigit() for char in chunk) and any(
            keyword in chunk
            for keyword in ("\u6b62\u635f", "\u6b62\u76c8", "\u4ed3\u4f4d")
        ):
            score += 2
        if chunk.startswith(("\u5efa\u8bae", "\u53ef", "\u7ee7\u7eed", "\u5e94", "\u9700", "\u5b9c", "\u65e0\u9700")):
            score += 3
        if any(keyword in chunk for keyword in background_keywords) and score < 10:
            score -= 6
        if any(keyword in chunk for keyword in monitoring_keywords) and not any(
            keyword in chunk for keyword in action_keywords
        ):
            score -= 4
        return score

    best_sentence = max(
        sentences,
        key=lambda sentence: (_score_text(sentence), -abs(len(sentence) - min(max_length + 6, 28))),
    )
    clauses = [
        clause.strip()
        for clause in re.split(r"[\uff0c,:\uff1a]", best_sentence)
        if clause.strip()
    ] or [best_sentence]
    best_index = max(
        range(len(clauses)),
        key=lambda index: (_score_text(clauses[index]), -abs(len(clauses[index]) - min(max_length, 18))),
    )
    best_clause = clauses[best_index]
    has_action = any(keyword in best_clause for keyword in action_keywords)
    has_context = any(keyword in best_clause for keyword in context_keywords)

    def _find_neighbor(index: int, *, step: int, keywords: tuple[str, ...]) -> str:
        cursor = index + step
        while 0 <= cursor < len(clauses):
            clause = clauses[cursor]
            if any(keyword in clause for keyword in keywords):
                return clause
            cursor += step
        return ""

    candidate = best_clause
    previous_clause = _find_neighbor(best_index, step=-1, keywords=context_keywords)
    next_clause = _find_neighbor(best_index, step=1, keywords=action_keywords)
    if has_action and previous_clause:
        candidate = f"{previous_clause}，{best_clause}"
    elif has_context and next_clause:
        candidate = f"{best_clause}，{next_clause}"

    for prefix in ("\u5f53\u524d", "\u4e14", "\u540c\u65f6", "\u5219"):
        if candidate.startswith(prefix):
            candidate = candidate[len(prefix):]
            break

    if len(candidate) <= max_length:
        return candidate
    if len(best_clause) <= max_length:
        return best_clause
    return best_clause[: max_length - 3].rstrip() + "..."


def _get_event_notice_style(event_type: str) -> tuple[str, str]:
    normalized = str(event_type or "").strip().lower()
    if normalized == "hold":
        return "#112f4d", "#60a5fa"
    if normalized == "entry":
        return "#102920", A_SHARE_DOWN_COLOR
    if normalized in {"sell", "sold", "take_profit"}:
        return "#34161c", A_SHARE_UP_COLOR
    if normalized in {"threshold_sync", "threshold_sync_skipped"}:
        return "#2b313c", NON_MARKET_PALETTE["gray"]
    if normalized in {"stop_loss", "error", "failed"}:
        return "#3a1d20", A_SHARE_UP_COLOR
    if normalized in {"buy", "ai_analysis"}:
        return "#494d12", "#f5e76b"
    return "#2d2344", "#a78bfa"


def _render_notice_card(message: str, *, background: str, foreground: str) -> None:
    st.markdown(
        (
            f"<div style='margin-bottom:0.5rem; padding:0.8rem 1rem; border-radius:0.7rem; "
            f"background:{background}; color:{foreground}; font-size:0.94rem; line-height:1.5; "
            f"border:1px solid rgba(255,255,255,0.06);'>"
            f"{message}"
            f"</div>"
        ),
        unsafe_allow_html=True,
    )


def _format_symbol_with_name(symbol: object, name: object) -> str:
    normalized_symbol = str(symbol or "").strip()
    normalized_name = str(name or "").strip()
    if normalized_name and normalized_symbol:
        return f"{normalized_name}({normalized_symbol})"
    return normalized_name or normalized_symbol or "-"


def _format_monitor_event_message(event: Dict) -> str:
    message = str(event.get("message") or "").strip()
    normalized_event_type = str(event.get("event_type") or "").strip().lower()
    if normalized_event_type not in {"entry", "take_profit", "stop_loss"}:
        return message

    symbol = str(event.get("symbol") or "").strip()
    name = str(event.get("name") or "").strip()
    stock_prefix = f"股票 {symbol} ({name}) "
    if symbol and name and message.startswith(stock_prefix):
        message = message[len(stock_prefix):].strip()
    return f"原因：{message}" if message else "原因：-"


def _render_monitor_event_notice(event: Dict) -> None:
    background, foreground = _get_event_notice_style(event.get("event_type"))
    event_type = html.escape(str(event.get("event_type") or "-").upper())
    created_at = html.escape(str(event.get("created_at") or "-"))
    symbol = html.escape(_format_symbol_with_name(event.get("symbol"), event.get("name")))
    message = html.escape(_format_monitor_event_message(event))
    _render_notice_card(
        (
            f"<div style='display:flex; justify-content:space-between; gap:0.75rem; align-items:flex-start;'>"
            f"<strong>{event_type}</strong>"
            f"<span style='opacity:0.72; white-space:nowrap;'>{created_at}</span>"
            f"</div>"
            f"<div style='margin-top:0.18rem; font-weight:600;'>{symbol}</div>"
            f"<div style='margin-top:0.2rem; opacity:0.92;'>{message}</div>"
        ),
        background=background,
        foreground=foreground,
    )


def _get_decision_notice_style(action: str) -> tuple[str, str]:
    normalized = str(action or "").strip().upper()
    if normalized == "BUY":
        return "#494d12", "#f5e76b"
    if normalized == "SELL":
        return "#102920", A_SHARE_DOWN_COLOR
    return "#112f4d", "#60a5fa"


def _render_ai_decision_notice(decision: Dict) -> None:
    background, foreground = _get_decision_notice_style(decision.get("action"))
    stock_display = html.escape(
        _format_symbol_with_name(decision.get("stock_code"), decision.get("stock_name"))
    )
    action_label = html.escape(_format_decision_action(decision.get("action")))
    confidence = decision.get("confidence")
    try:
        confidence_text = f"{int(round(float(confidence)))}%"
    except (TypeError, ValueError):
        confidence_text = "0%"
    trading_session = html.escape(str(decision.get("trading_session") or "-"))
    decision_time = html.escape(str(decision.get("decision_time") or "-"))
    reasoning_brief = html.escape(_format_decision_reasoning_brief(decision.get("reasoning")))
    risk_level = html.escape(str(decision.get("risk_level") or "-"))
    reasoning_html = (
        f"<div style='margin-top:0.2rem; opacity:0.9;'>要点: {reasoning_brief}</div>"
        if reasoning_brief
        else ""
    )
    _render_notice_card(
        (
            f"<div style='display:flex; justify-content:space-between; gap:0.75rem; align-items:flex-start;'>"
            f"<strong>{action_label}({confidence_text})</strong>"
            f"<span style='opacity:0.72; white-space:nowrap;'>{decision_time}</span>"
            f"</div>"
            f"<div style='margin-top:0.18rem; font-weight:600;'>{stock_display}</div>"
            f"<div style='margin-top:0.2rem; opacity:0.82;'>时段: {trading_session} | 风险: {risk_level}</div>"
            f"{reasoning_html}"
        ),
        background=background,
        foreground=foreground,
    )


def _render_full_decision_reasoning(reasoning: object) -> None:
    text = str(reasoning or "").strip()
    if not text:
        st.caption("暂无决策理由。")
        return
    escaped_text = html.escape(text).replace("\n", "<br>")
    st.markdown(
        (
            "<div style='margin-top:0.45rem; padding:0.85rem 1rem; border-radius:0.7rem; "
            "background:rgba(15,23,42,0.42); border:1px solid rgba(148,163,184,0.18); "
            "line-height:1.75; font-size:0.95rem; white-space:normal; overflow-wrap:anywhere;'>"
            f"{escaped_text}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_pending_action_trade_form(
    db: SmartMonitorDB,
    task: Dict,
    asset: Optional[Dict],
    pending_actions: List[Dict],
) -> None:
    task_id = task["id"]
    form_open_key = f"ai_task_trade_open_{task_id}"
    trade_mode_key = f"ai_task_trade_mode_{task_id}"
    if not st.session_state.get(form_open_key):
        return

    trade_mode = st.session_state.get(trade_mode_key, "buy")
    matched_pending = next((item for item in pending_actions if item.get("action_type") == trade_mode), None)
    payload = (matched_pending or {}).get("payload") or {}
    default_price = float(payload.get("current_price") or payload.get("market_data", {}).get("current_price") or asset.get("cost_price") or 0.0) if asset else float(payload.get("current_price") or payload.get("market_data", {}).get("current_price") or 0.0)
    default_quantity = int(asset.get("quantity") or 0) if trade_mode == "sell" and asset else 100
    default_quantity = default_quantity if default_quantity > 0 else 100

    with st.form(key=f"ai_task_trade_form_{task_id}"):
        st.markdown(f"#### 手工{'买入' if trade_mode == 'buy' else '卖出'}登记")
        st.caption(
            f"{task.get('stock_code')} {task.get('stock_name') or task.get('stock_code')} | "
            f"资产状态：{_format_asset_status(asset)}"
        )
        if matched_pending:
            st.warning(
                f"正在处理待办动作 #{matched_pending['id']} ({matched_pending['action_type'].upper()})"
            )

        trade_col1, trade_col2 = st.columns(2)
        with trade_col1:
            trade_date = st.date_input(
                "成交日期",
                value=datetime.now().date(),
                key=f"ai_task_trade_date_{task_id}",
            )
        with trade_col2:
            trade_quantity = st.number_input(
                "成交数量",
                min_value=1,
                value=default_quantity,
                step=1,
                key=f"ai_task_trade_quantity_{task_id}",
            )

        trade_price = st.number_input(
            "成交价格",
            min_value=0.0,
            value=default_price,
            step=0.001,
            format="%.3f",
            key=f"ai_task_trade_price_{task_id}",
        )
        trade_note = st.text_area(
            "备注",
            value="",
            height=80,
            placeholder="可选，例如：手工确认 AI 卖出信号",
            key=f"ai_task_trade_note_{task_id}",
        )

        submit_col, cancel_col = st.columns(2)
        with submit_col:
            submitted = st.form_submit_button("保存交易", type="primary", width="stretch")
        with cancel_col:
            cancelled = st.form_submit_button("取消", width="stretch")

        if submitted:
            trade_id = db.save_trade_record(
                {
                    "asset_id": (asset or {}).get("id") or task.get("asset_id"),
                    "trade_type": trade_mode,
                    "quantity": int(trade_quantity),
                    "price": float(trade_price),
                    "trade_date": trade_date.strftime("%Y-%m-%d"),
                    "note": (trade_note or "").strip(),
                    "trade_source": "manual",
                    "pending_action_id": (matched_pending or {}).get("id"),
                }
            )
            if trade_id:
                st.session_state.pop(form_open_key, None)
                st.session_state.pop(trade_mode_key, None)
                st.success("手工交易已登记，资产状态已同步更新。")
                st.rerun()
            st.error("保存交易失败，请检查数量、价格和当前持仓。")
        if cancelled:
            st.session_state.pop(form_open_key, None)
            st.session_state.pop(trade_mode_key, None)
            st.rerun()


def _start_manual_ai_analysis_task(task: Dict) -> None:
    task_id = task.get("id")
    stock_code = task.get("stock_code") or "UNKNOWN"
    stock_name = task.get("stock_name") or stock_code
    if not task_id:
        raise RuntimeError("监控任务缺少有效 ID，无法提交手工分析。")

    def runner(_background_task_id, report_progress):
        report_progress(
            current=0,
            total=1,
            step_code=stock_code,
            step_status="analyzing",
            message=f"正在执行 {stock_code} 的手工 AI 分析",
        )
        from monitor_service import monitor_service

        success = monitor_service.manual_update_stock(int(task_id))
        if not success:
            raise RuntimeError(f"{stock_code} AI 分析执行失败")
        report_progress(
            current=1,
            total=1,
            step_code=stock_code,
            step_status="success",
            message=f"{stock_code} AI 分析已完成",
        )
        return {
            "monitor_item_id": int(task_id),
            "stock_code": stock_code,
            "stock_name": stock_name,
        }

    start_ui_analysis_task(
        task_type=SMART_MONITOR_MANUAL_ANALYSIS_TASK_TYPE,
        label=f"{stock_name} 手工 AI 分析",
        runner=runner,
        metadata={
            "monitor_item_id": int(task_id),
            "stock_code": stock_code,
            "stock_name": stock_name,
        },
    )


def _consume_finished_manual_ai_analysis_task() -> None:
    finished_task = consume_finished_ui_analysis_task(
        SMART_MONITOR_MANUAL_ANALYSIS_TASK_TYPE,
        SMART_MONITOR_MANUAL_ANALYSIS_DONE_KEY,
    )
    if not finished_task:
        return
    if finished_task.get("status") != "success":
        st.error(f"手工 AI 分析失败：{finished_task.get('error', '未知错误')}")
        return

    result = finished_task.get("result") or {}
    stock_code = result.get("stock_code") or "目标股票"
    st.success(f"{stock_code} 的手工 AI 分析已完成。")


@st.fragment(run_every=2.0)
def _render_manual_ai_analysis_live_fragment():
    render_ui_analysis_task_live_card(
        task_type=SMART_MONITOR_MANUAL_ANALYSIS_TASK_TYPE,
        title="手工 AI 分析任务状态",
        state_prefix="smart_monitor_manual_analysis_live",
    )


def render_ai_monitor_tasks_panel(show_header: bool = True, title: str = "AI监控任务"):
    from monitor_service import monitor_service

    if show_header:
        st.header(title)
    else:
        st.markdown(f"#### {title}")

    _consume_finished_manual_ai_analysis_task()
    if get_active_ui_analysis_task(SMART_MONITOR_MANUAL_ANALYSIS_TASK_TYPE):
        _render_manual_ai_analysis_live_fragment()

    db = st.session_state[SMART_MONITOR_DB_KEY]
    monitor_service.ensure_started()
    monitor_service.ensure_stopped_if_idle()

    prefill = st.session_state.pop(INVESTMENT_AI_TASK_PREFILL_KEY, None)
    if not prefill:
        prefill = st.session_state.pop(INVESTMENT_PRICE_ALERT_PREFILL_KEY, None)
    if prefill:
        st.session_state["ai_task_form_account_name"] = prefill.get("account_name") or DEFAULT_ACCOUNT_NAME
        st.session_state["ai_task_form_task_name"] = prefill.get("task_name") or f"{prefill.get('stock_name') or prefill.get('symbol')}盯盘"
        st.session_state["ai_task_form_stock_code"] = prefill.get("symbol") or ""
        st.session_state["ai_task_form_stock_name"] = prefill.get("stock_name") or ""
        st.session_state["ai_task_form_interval_minutes"] = int(
            prefill.get("interval_minutes") or _get_default_ai_interval_minutes()
        )
        st.session_state["ai_task_form_trading_hours_only"] = bool(prefill.get("trading_hours_only", True))
        st.session_state["ai_task_form_position_size_pct"] = int(prefill.get("position_size_pct") or 20)
        st.session_state["ai_task_form_stop_loss_pct"] = int(prefill.get("stop_loss_pct") or 5)
        st.session_state["ai_task_form_take_profit_pct"] = int(prefill.get("take_profit_pct") or 10)
        st.session_state["ai_task_form_strategy_context"] = prefill.get("strategy_context") or {}
        st.session_state["ai_task_form_origin_analysis_id"] = prefill.get("origin_analysis_id")
        st.session_state["ai_task_form_notice"] = f"{prefill.get('symbol')} 的战略基线已带入智能盯盘表单。"

    strategy_context = st.session_state.get("ai_task_form_strategy_context") or {}

    with st.expander("新增 AI 监控任务", expanded=bool(st.session_state.get("ai_task_form_stock_code"))):
        if st.session_state.get("ai_task_form_notice"):
            st.info(st.session_state["ai_task_form_notice"])
        if strategy_context:
            st.caption(
                f"战略基线: 评级 {strategy_context.get('rating') or 'N/A'} | "
                f"进场 {strategy_context.get('entry_min') or '-'} - {strategy_context.get('entry_max') or '-'} | "
                f"止盈 {strategy_context.get('take_profit') or '-'} | 止损 {strategy_context.get('stop_loss') or '-'}"
            )
        with st.form("ai_monitor_task_form", clear_on_submit=False):
            col1, col2 = st.columns(2)

            with col1:
                account_name = st.text_input("账户名称", key="ai_task_form_account_name")
                task_name = st.text_input("任务名称", placeholder="例如: 茅台 AI 监控", key="ai_task_form_task_name")
                stock_code = st.text_input("股票代码", placeholder="例如: 600519", key="ai_task_form_stock_code")
                stock_name = st.text_input("股票名称", placeholder="可选", key="ai_task_form_stock_name")

            with col2:
                trading_hours_only = st.checkbox("仅交易时段执行", value=True, key="ai_task_form_trading_hours_only")

            submitted = st.form_submit_button("保存 AI 任务", type="primary", width='stretch')

        if submitted:
            normalized_account = (account_name or DEFAULT_ACCOUNT_NAME).strip() or DEFAULT_ACCOUNT_NAME
            normalized_code = (stock_code or "").strip().upper()
            if not task_name or not normalized_code:
                st.error("请填写账户、任务名称和股票代码。")
            else:
                db.upsert_monitor_task({
                    'task_name': task_name,
                    'stock_code': normalized_code,
                    'stock_name': stock_name.strip() or normalized_code,
                    'enabled': 1,
                    'check_interval': _get_default_ai_interval_minutes() * 60,
                    'trading_hours_only': 1 if trading_hours_only else 0,
                    'position_size_pct': _get_default_position_size_pct(),
                    'stop_loss_pct': _get_default_stop_loss_pct(),
                    'take_profit_pct': _get_default_take_profit_pct(),
                    'account_name': normalized_account,
                    'origin_analysis_id': st.session_state.get("ai_task_form_origin_analysis_id"),
                    'strategy_context': st.session_state.get("ai_task_form_strategy_context") or {},
                })
                st.session_state["ai_task_form_account_name"] = DEFAULT_ACCOUNT_NAME
                st.session_state["ai_task_form_task_name"] = ""
                st.session_state["ai_task_form_stock_code"] = ""
                st.session_state["ai_task_form_stock_name"] = ""
                monitor_service.ensure_started()
                st.session_state["ai_task_form_trading_hours_only"] = True
                st.session_state["ai_task_form_strategy_context"] = {}
                st.session_state["ai_task_form_origin_analysis_id"] = None
                st.session_state.pop("ai_task_form_notice", None)
                st.success(f"{normalized_code} AI 监控任务已保存。")
                st.rerun()

    tasks = db.get_monitor_tasks(enabled_only=False)
    if not tasks:
        st.info("暂无 AI 监控任务。")
        return

    st.markdown("### 任务列表")
    enabled_count = sum(1 for task in tasks if task.get('enabled'))
    disabled_count = len(tasks) - enabled_count
    run_button_label, run_button_disabled, run_button_help = get_ui_analysis_button_state(
        SMART_MONITOR_MANUAL_ANALYSIS_TASK_TYPE,
        "立即分析",
    )

    summary_col, enable_all_col, disable_all_col = st.columns([2.6, 1, 1])
    with summary_col:
        st.caption(f"共 {len(tasks)} 个任务，已启用 {enabled_count} 个，已停用 {disabled_count} 个。")
    with enable_all_col:
        if st.button(
            "一键启用全部",
            key="enable_all_ai_tasks",
            width='stretch',
            disabled=disabled_count == 0,
        ):
            changed_count = db.set_all_monitor_tasks_enabled(True)
            monitor_service.ensure_started()
            st.success(f"已启用 {changed_count} 个标的的盯盘任务，实时预警同步生效。")
            st.rerun()
    with disable_all_col:
        if st.button(
            "一键停用全部",
            key="disable_all_ai_tasks",
            width='stretch',
            disabled=enabled_count == 0,
        ):
            changed_count = db.set_all_monitor_tasks_enabled(False)
            monitor_service.ensure_stopped_if_idle()
            st.success(f"已停用 {changed_count} 个标的的盯盘任务，实时预警同步停用。")
            st.rerun()

    for task in tasks:
        alert_item = _get_task_price_alert_item(db, task)
        alert_levels = _resolve_price_alert_levels(alert_item)
        latest_decision = _get_latest_ai_decision_for_task(db, task)
        pending_actions = _get_pending_actions_for_task(db, task)

        with st.container():
            st.markdown(f"**{task['stock_code']}** {task.get('stock_name') or task['stock_code']}")

            strategy_context = task.get("strategy_context") or {}

            threshold_line = None
            if alert_levels:
                threshold_line = (
                    f"预警线: 进场 {alert_levels.get('entry_min') or '-'} - {alert_levels.get('entry_max') or '-'} | "
                    f"止盈 {alert_levels.get('take_profit') or '-'} | 止损 {alert_levels.get('stop_loss') or '-'}"
                )
            elif strategy_context:
                threshold_line = (
                    f"预警线: 进场 {strategy_context.get('entry_min') or '-'} - {strategy_context.get('entry_max') or '-'} | "
                    f"止盈 {strategy_context.get('take_profit') or '-'} | 止损 {strategy_context.get('stop_loss') or '-'}"
                )

            if threshold_line:
                st.caption(threshold_line)
            else:
                st.caption("预警线: 尚未形成")

            if alert_item:
                alert_meta = []
                current_price = alert_item.get("current_price")
                if current_price not in (None, ""):
                    alert_meta.append(f"最新价 {current_price}")
                if alert_item.get("last_checked"):
                    alert_meta.append(f"最近检查 {alert_item.get('last_checked')}")
                if alert_levels and alert_levels.get("source"):
                    alert_meta.append(f"阈值来源 {alert_levels.get('source')}")
                if alert_meta:
                    st.caption(" | ".join(alert_meta))

            if latest_decision:
                decision_label = _format_latest_decision_label(latest_decision)
                with st.expander(f"最新AI决策: {decision_label}", expanded=False):
                    decision_col1, decision_col2, decision_col3 = st.columns(3)
                    with decision_col1:
                        st.caption(f"交易时段: {latest_decision.get('trading_session') or '-'}")
                    with decision_col2:
                        st.caption(f"风险等级: {latest_decision.get('risk_level') or '-'}")
                    with decision_col3:
                        st.caption(f"建议仓位: {latest_decision.get('position_size_pct') or '-'}%")
                    _render_full_decision_reasoning(latest_decision.get("reasoning"))
            else:
                st.caption("尚无最新 AI 决策，可先执行一次“立即分析”。")

            action_columns = st.columns(3 if not task.get('managed_by_portfolio') else 2)
            action_col1, action_col2 = action_columns[:2]
            with action_col1:
                if st.button(
                    run_button_label,
                    key=f"run_ai_task_{task['id']}",
                    width='stretch',
                    disabled=run_button_disabled,
                    help=run_button_help,
                ):
                    try:
                        _start_manual_ai_analysis_task(task)
                        st.success("已提交手工 AI 分析任务。")
                        st.rerun()
                    except RuntimeError as exc:
                        st.error(str(exc))
            with action_col2:
                toggle_label = "停用" if task.get('enabled') else "启用"
                if st.button(toggle_label, key=f"toggle_ai_task_{task['id']}", width='stretch'):
                    db.set_monitor_task_enabled(int(task["id"]), not task.get("enabled"))
                    if task.get('enabled'):
                        monitor_service.ensure_stopped_if_idle()
                    else:
                        monitor_service.ensure_started()
                    st.success(f"{task['stock_code']} 的盯盘任务已{toggle_label}，实时预警同步更新。")
                    st.rerun()
            if not task.get('managed_by_portfolio'):
                action_col3 = action_columns[2]
                with action_col3:
                    if st.button(
                        "删除",
                        key=f"delete_ai_task_{task['id']}",
                        width='stretch',
                    ):
                        db.delete_monitor_task(task['id'])
                        st.success("任务已删除。")
                        st.rerun()

            st.markdown(
                "<div style='margin:0.45rem 0 0.7rem 0; border-bottom:1px solid rgba(148,163,184,0.18);'></div>",
                unsafe_allow_html=True,
            )


def smart_monitor_ui(lightweight_model=None, reasoning_model=None):
    """智能盯盘主界面（重构版）。"""
    _ensure_smart_monitor_runtime(lightweight_model, reasoning_model)

    desired_view = st.session_state.get(SMART_MONITOR_ACTIVE_TAB_KEY, "watchlist")
    view_key_to_label = {
        "watchlist": "盯盘列表",
        "ai_task": "盯盘列表",
        "realtime": "盯盘列表",
        "price_alert": "盯盘列表",
        "decision_events": "决策事件",
        "history": "决策事件",
        "settings": "系统设置",
    }
    labels = ["盯盘列表", "决策事件", "系统设置"]
    current_label = view_key_to_label.get(desired_view, "盯盘列表")
    selected_label = st.radio(
        "智能盯盘视图",
        labels,
        index=labels.index(current_label),
        horizontal=True,
        key="smart_monitor_view_selector",
        label_visibility="collapsed",
    )
    label_to_key = {
        "盯盘列表": "watchlist",
        "决策事件": "decision_events",
        "系统设置": "settings",
    }
    selected_view = label_to_key[selected_label]
    st.session_state[SMART_MONITOR_ACTIVE_TAB_KEY] = selected_view

    if selected_view == "watchlist":
        render_ai_monitor_tasks_panel(title="盯盘列表")
        return

    if selected_view == "decision_events":
        render_history(show_header=False, title="决策事件")
        return

    render_settings(show_header=False)


if __name__ == '__main__':
    smart_monitor_ui()

