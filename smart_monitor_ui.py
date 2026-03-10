"""
智能盯盘 - UI界面
集成到主程序的智能盯盘功能界面
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import os
from typing import Dict, List, Optional
from dotenv import load_dotenv
import config

from investment_db_utils import DEFAULT_ACCOUNT_NAME
from smart_monitor_engine import SmartMonitorEngine
from smart_monitor_db import SmartMonitorDB
from config_manager import config_manager  # 使用主程序的配置管理器
from portfolio_manager import portfolio_manager
from ui_state_keys import (
    INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY,
    INVESTMENT_AI_TASK_PREFILL_KEY,
    PORTFOLIO_ADD_ACCOUNT_NAME_KEY,
    PORTFOLIO_ADD_ORIGIN_ANALYSIS_ID_KEY,
    SMART_MONITOR_ACTIVE_TAB_KEY,
    SMART_MONITOR_DB_KEY,
    SMART_MONITOR_ENGINE_KEY,
)
from ui_shared import (
    format_price,
    get_dataframe_height,
    get_action_color,
    get_market_color,
    render_a_share_change_metric,
)


# 加载环境变量
load_dotenv()


def _legacy_smart_monitor_ui(lightweight_model=None, reasoning_model=None):
    """智能盯盘主界面"""

    # 使用说明
    with st.expander("快速使用指南", expanded=False):
        st.caption("参照AlphaArena项目，基于DeepSeek AI的A股自动化交易系统")
        st.markdown("""
        ### 快速开始
        
        **第一步：环境配置**
        1. 点击左侧菜单"系统配置"
        2. 填写 DeepSeek API Key（必需）
        3. 配置 miniQMT 账户（可选，用于实盘交易）
        4. 配置通知方式（可选，邮件/Webhook）
        
        **第二步：开始使用**
        - **实时分析**：输入股票代码，AI即时分析并给出交易建议
        - **监控任务**：添加股票到监控列表，定时自动分析
        - **持仓管理**：查看和管理当前持仓（已持仓股票可直接监控）
        
        ---
        
        ### 核心功能
        
        | 功能 | 说明 |
        |------|------|
        | **实时分析** | 输入股票代码，AI分析市场数据并给出买入/卖出/持有建议 |
        | **监控任务** | 定时自动分析目标股票，可设置自动交易 |
        | **持仓管理** | 记录持仓成本，实时显示盈亏，AI决策考虑持仓情况 |
        | **历史记录** | 查看所有AI决策历史、交易记录和通知记录 |
        | **系统设置** | 配置API、交易方式（实盘/模拟）、通知等 |
        
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
        1. 先使用"模拟交易"模式测试
        2. 小仓位试水（建议5-10%）
        3. 严格执行止损，不要心存侥幸
        4. 关注交易时段（9:30-11:30, 13:00-15:00）
        
        **高级功能**：
        - 在"监控任务"中勾选"已持仓"，填入成本价
        - AI会考虑当前盈亏情况给出更准确的建议
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
        
        **Q: 想实盘交易如何操作？**
        1. 下载并安装 [miniQMT](https://www.xtp-mini.com/)
        2. 启动miniQMT客户端并登录
        3. 在"系统设置"中填写账户ID
        4. 取消勾选"使用模拟交易"
        
        ---
        
        ### 风险提示
        
        1. **股市有风险，投资需谨慎**
        2. AI决策仅供参考，不构成投资建议
        3. 建议先使用模拟交易充分测试
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
        "实时分析", 
        "监控任务", 
        "持仓管理", 
        "历史记录",
        "系统设置"
    ])
    
    # 标签页1: 实时分析
    with tabs[0]:
        render_realtime_analysis()
    
    # 标签页2: 监控任务
    with tabs[1]:
        render_monitor_tasks()
    
    # 标签页3: 持仓管理
    with tabs[2]:
        render_position_management()
    
    # 标签页4: 历史记录
    with tabs[3]:
        render_history()
    
    # 标签页5: 系统设置
    with tabs[4]:
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
                auto_trade=False,
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
                auto_trade = st.checkbox("自动交易", value=False,
                                        help="AI决策后自动执行交易")
                trading_hours_only = st.checkbox(
                    "仅交易时段监控", 
                    value=True,
                    help="开启后，只在交易日的交易时段（9:30-11:30, 13:00-15:00）进行AI分析"
                )
                position_size = st.slider("仓位百分比(%)", 5, 50, 20,
                                         help="新建仓位时使用的资金比例")
                notify_email = st.text_input("通知邮箱（可选）")
            
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
                            'auto_trade': 1 if auto_trade else 0,
                            'trading_hours_only': 1 if trading_hours_only else 0,
                            'position_size_pct': position_size,
                            'notify_email': notify_email,
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
            auto_trade_status = "自动交易" if task['auto_trade'] else "仅监控"
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
                    st.caption(f"模式: {auto_trade_status} · {trading_mode}")
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
                                auto_trade=task['auto_trade'] == 1,
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
    qmt = engine.qmt
    
    # 获取账户信息
    account_info = qmt.get_account_info()
    
    st.markdown("### 账户概览")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("总资产", f"¥{account_info['total_value']:,.2f}")
    col2.metric("可用资金", f"¥{account_info['available_cash']:,.2f}")
    col3.metric("持仓数量", f"{account_info['positions_count']}个")
    col4.metric("总盈亏", f"¥{account_info['total_profit_loss']:,.2f}")
    
    # 获取 QMT 持仓 + 持仓分析同步持仓
    qmt_positions = qmt.get_all_positions()
    portfolio_positions = portfolio_manager.get_all_stocks(auto_monitor_only=True)
    merged_positions = []
    qmt_positions_by_code = {position['stock_code']: dict(position) for position in qmt_positions}

    for position in qmt_positions_by_code.values():
        position['source'] = 'QMT'
        merged_positions.append(position)

    for stock in portfolio_positions:
        existing = qmt_positions_by_code.get(stock['code'])
        if existing:
            if stock.get('cost_price') and not existing.get('cost_price'):
                existing['cost_price'] = stock['cost_price']
            if stock.get('quantity') and not existing.get('quantity'):
                existing['quantity'] = stock['quantity']
            existing['source'] = 'QMT + 持仓同步'
            continue

        merged_positions.append({
            'stock_code': stock['code'],
            'stock_name': stock.get('name', stock['code']),
            'quantity': stock.get('quantity') or 0,
            'can_sell': 0,
            'cost_price': stock.get('cost_price'),
            'current_price': None,
            'profit_loss': None,
            'profit_loss_pct': None,
            'source': '持仓同步',
        })

    if not merged_positions:
        st.info("当前无持仓")
        return

    portfolio_only_count = sum(1 for position in merged_positions if position.get('source') == '持仓同步')
    if portfolio_only_count:
        st.caption(f"额外展示 {portfolio_only_count} 个来自持仓分析同步的持仓。")
    
    st.markdown("### 持仓列表")
    
    # 转换为DataFrame
    df = pd.DataFrame(merged_positions)
    
    # 显示表格
    st.dataframe(
        df[[
            'stock_code', 'stock_name', 'quantity', 'can_sell',
            'cost_price', 'current_price', 'profit_loss', 'profit_loss_pct', 'source'
        ]],
        column_config={
            "stock_code": "代码",
            "stock_name": "名称",
            "quantity": "持仓",
            "can_sell": "可卖",
            "cost_price": "成本价",
            "current_price": "现价",
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
        options=[f"{p['stock_code']} {p['stock_name']}" for p in merged_positions]
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("AI分析", type="secondary"):
            stock_code = selected_stock.split()[0]
            with st.spinner("分析中..."):
                result = engine.analyze_stock(stock_code, auto_trade=False)
                if result['success']:
                    st.success("分析完成，查看'实时分析'标签页")
    
    with col2:
        if st.button("卖出", type="primary"):
            stock_code = selected_stock.split()[0]
            selected_position = next((p for p in merged_positions if p['stock_code'] == stock_code), None)
            if selected_position and selected_position.get('source') == '持仓同步':
                st.warning("该持仓来自持仓分析同步，不是 QMT 实盘持仓，无法直接卖出。")
            else:
                st.warning("请在'实时分析'中使用AI决策后卖出")


def render_history(show_header: bool = True, title: str = "历史记录"):
    """历史记录界面"""

    if show_header:
        st.header(title)
    else:
        st.markdown(f"#### {title}")
    
    db = st.session_state[SMART_MONITOR_DB_KEY]
    
    tab1, tab2, tab3, tab4 = st.tabs(["AI决策历史", "待人工动作", "交易记录", "信号事件"])
    
    # AI决策历史
    with tab1:
        st.subheader("AI决策历史")
        
        decisions = db.get_ai_decisions(limit=50)
        
        if not decisions:
            st.info("暂无决策记录")
        else:
            for dec in decisions:
                with st.expander(
                    f"{dec['decision_time']} - {dec['stock_code']} {dec['stock_name']} "
                    f"- {dec['action']} (信心度{dec['confidence']}%)"
                ):
                    col1, col2 = st.columns([1, 3])
                    
                    with col1:
                        st.write(f"**时段:** {dec['trading_session']}")
                        st.write(f"**风险:** {dec['risk_level']}")
                        st.write(f"**仓位:** {dec['position_size_pct']}%")
                        st.write(f"**执行模式:** {dec.get('execution_mode', 'manual_only')}")
                        st.write(f"**状态:** {dec.get('action_status', 'suggested')}")
                    
                    with col2:
                        st.write("**决策理由:**")
                        st.text(dec['reasoning'])

    with tab2:
        st.subheader("待人工动作")

        actions = db.get_pending_actions(status=None, limit=100)
        if not actions:
            st.info("暂无待人工动作。")
        else:
            pending_count = sum(1 for item in actions if item.get("status") == "pending")
            st.caption(f"共 {len(actions)} 条动作，其中待处理 {pending_count} 条。")
            for action in actions:
                payload = action.get("payload") or {}
                with st.expander(
                    f"{action.get('created_at')} - {action.get('symbol')} {action.get('name') or action.get('symbol')} "
                    f"- {str(action.get('action_type', '')).upper()} [{action.get('status')}]"
                ):
                    info_col1, info_col2 = st.columns(2)
                    with info_col1:
                        st.write(f"**账户:** {action.get('account_name') or DEFAULT_ACCOUNT_NAME}")
                        st.write(f"**资产状态:** {_format_asset_status(action)}")
                        st.write(f"**当前持仓:** {_format_position_summary(action)}")
                    with info_col2:
                        st.write(f"**决策ID:** {action.get('origin_decision_id') or '-'}")
                        st.write(f"**建议价格:** {payload.get('current_price') or payload.get('market_data', {}).get('current_price') or '-'}")
                        st.write(f"**备注:** {action.get('resolution_note') or '待处理'}")
                    decision_block = payload.get("decision") or {}
                    if decision_block:
                        st.write("**AI 建议摘要:**")
                        st.text(str(decision_block.get("reasoning") or "")[:300])
    
    # 交易记录
    with tab3:
        st.subheader("交易记录")
        
        trades = db.get_trade_records(limit=50)
        
        if not trades:
            st.info("暂无交易记录")
        else:
            df = pd.DataFrame(trades)
            st.dataframe(
                df[[
                    'trade_time', 'stock_code', 'stock_name', 'trade_type',
                    'quantity', 'price', 'amount', 'profit_loss'
                ]],
                column_config={
                    "trade_time": "时间",
                    "stock_code": "代码",
                    "stock_name": "名称",
                    "trade_type": "类型",
                    "quantity": "数量",
                    "price": "价格",
                    "amount": "金额",
                    "profit_loss": "盈亏"
                },
                hide_index=True,
                width='stretch',
                height=get_dataframe_height(len(df), max_rows=50),
            )
    
    with tab4:
        st.subheader("信号事件")
        events = db.monitoring_repository.get_recent_events(limit=50)
        if not events:
            st.info("暂无事件记录。")
        else:
            for event in events:
                details = db.monitoring_repository._safe_json_loads(event.get("details_json"), {})
                with st.expander(
                    f"{event.get('created_at')} - {event.get('symbol')} "
                    f"- {event.get('event_type')} {'[已发送]' if event.get('sent') else '[未发送]'}"
                ):
                    st.write(event.get("message"))
                    if details:
                        st.json(details)


def render_settings(show_header: bool = True, title: str = "系统设置"):
    """系统设置界面（跳转到主程序的环境配置）"""

    if show_header:
        st.header(title)
    else:
        st.markdown(f"#### {title}")
    
    st.info("""
    ### 配置说明
    
    智能盯盘使用主程序的统一配置系统，包括：
    - **DeepSeek API** - AI决策引擎
    - **MiniQMT** - 量化交易接口
    - **邮件通知** - SMTP配置
    - **Webhook** - 钉钉/飞书通知
    
    请前往主程序的 **"环境配置"** 页面进行统一配置。
    """)
    
    # 显示当前配置状态
    st.markdown("### 当前配置状态")
    
    config = config_manager.read_env()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**DeepSeek API**")
        api_key = config.get('DEEPSEEK_API_KEY', '')
        if api_key:
            st.success(f"已配置（{api_key[:8]}...）")
        else:
            st.error("未配置")
        
        st.markdown("**MiniQMT**")
        miniqmt_enabled = config.get('MINIQMT_ENABLED', 'false').lower() == 'true'
        if miniqmt_enabled:
            account_id = config.get('MINIQMT_ACCOUNT_ID', '')
            st.success(f"已启用（账户：{account_id or '未设置'}）")
        else:
            st.warning("未启用（使用模拟交易）")
    
    with col2:
        st.markdown("**邮件通知**")
        email_enabled = config.get('EMAIL_ENABLED', 'false').lower() == 'true'
        if email_enabled:
            email_to = config.get('EMAIL_TO', '')
            st.success(f"已启用（{email_to}）")
        else:
            st.warning("未启用")
        
        st.markdown("**Webhook通知**")
        webhook_enabled = config.get('WEBHOOK_ENABLED', 'false').lower() == 'true'
        if webhook_enabled:
            webhook_type = config.get('WEBHOOK_TYPE', 'dingtalk')
            st.success(f"已启用（{webhook_type}）")
        else:
            st.warning("未启用")
    
    st.markdown("---")
    
    # 快速跳转按钮
    st.markdown("### 配置管理")
    
    st.info("""
    **配置步骤：**
    1. 点击左侧菜单 → **"环境配置"**
    2. 填写所需的配置项
    3. 点击 **"保存配置"**
    4. 返回智能盯盘页面
    5. 刷新页面使配置生效
    """)
    
    if st.button("重新加载配置", type="primary"):
        config_manager.reload_config()
        st.success("配置已重新加载。")
        st.info("如果修改了配置，请刷新页面（Ctrl+R）。")
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


def render_ai_monitor_tasks_panel(show_header: bool = True, title: str = "AI监控任务"):
    from monitor_service import monitor_service

    if show_header:
        st.header(title)
    else:
        st.markdown(f"#### {title}")

    db = st.session_state[SMART_MONITOR_DB_KEY]
    service_status = "运行中" if monitor_service.running else "已停止"
    st.caption(f"监测服务状态: {service_status}。启用中的任务会由统一监测服务按分钟调度执行。")

    prefill = st.session_state.pop(INVESTMENT_AI_TASK_PREFILL_KEY, None)
    if prefill:
        st.session_state["ai_task_form_account_name"] = prefill.get("account_name") or DEFAULT_ACCOUNT_NAME
        st.session_state["ai_task_form_task_name"] = prefill.get("task_name") or f"{prefill.get('stock_name') or prefill.get('symbol')}盯盘"
        st.session_state["ai_task_form_stock_code"] = prefill.get("symbol") or ""
        st.session_state["ai_task_form_stock_name"] = prefill.get("stock_name") or ""
        st.session_state["ai_task_form_interval_minutes"] = int(prefill.get("interval_minutes") or 5)
        st.session_state["ai_task_form_trading_hours_only"] = bool(prefill.get("trading_hours_only", True))
        st.session_state["ai_task_form_position_size_pct"] = int(prefill.get("position_size_pct") or 20)
        st.session_state["ai_task_form_stop_loss_pct"] = int(prefill.get("stop_loss_pct") or 5)
        st.session_state["ai_task_form_take_profit_pct"] = int(prefill.get("take_profit_pct") or 10)
        st.session_state["ai_task_form_notify_email"] = prefill.get("notify_email") or ""
        st.session_state["ai_task_form_strategy_context"] = prefill.get("strategy_context") or {}
        st.session_state["ai_task_form_origin_analysis_id"] = prefill.get("origin_analysis_id")
        st.session_state["ai_task_form_notice"] = f"{prefill.get('symbol')} 的战略基线已带入智能盯盘表单。"

    strategy_context = st.session_state.get("ai_task_form_strategy_context") or {}
    preview_account_name = st.session_state.get("ai_task_form_account_name") or DEFAULT_ACCOUNT_NAME
    preview_symbol = str(st.session_state.get("ai_task_form_stock_code") or "").strip().upper()
    preview_asset = db.asset_repository.get_asset_by_symbol(preview_symbol, preview_account_name) if preview_symbol else None

    with st.expander("新增 AI 监控任务", expanded=bool(st.session_state.get("ai_task_form_stock_code"))):
        if st.session_state.get("ai_task_form_notice"):
            st.info(st.session_state["ai_task_form_notice"])
        if strategy_context:
            st.caption(
                f"战略基线: 评级 {strategy_context.get('rating') or 'N/A'} | "
                f"进场 {strategy_context.get('entry_min') or '-'} - {strategy_context.get('entry_max') or '-'} | "
                f"止盈 {strategy_context.get('take_profit') or '-'} | 止损 {strategy_context.get('stop_loss') or '-'}"
            )
        st.caption(
            f"当前资产状态：{_format_asset_status(preview_asset)} | 持仓摘要：{_format_position_summary(preview_asset)} | 执行模式：手工确认"
        )
        with st.form("ai_monitor_task_form", clear_on_submit=False):
            col1, col2 = st.columns(2)

            with col1:
                account_name = st.text_input("账户名称", key="ai_task_form_account_name")
                task_name = st.text_input("任务名称", placeholder="例如: 茅台 AI 监控", key="ai_task_form_task_name")
                stock_code = st.text_input("股票代码", placeholder="例如: 600519", key="ai_task_form_stock_code")
                stock_name = st.text_input("股票名称", placeholder="可选", key="ai_task_form_stock_name")
                interval_minutes = st.slider("检查间隔(分钟)", 1, 240, 5, key="ai_task_form_interval_minutes")
                st.caption("资产状态与持仓信息由 `assets` 主表实时投影，不在此页编辑。")

            with col2:
                trading_hours_only = st.checkbox("仅交易时段执行", value=True, key="ai_task_form_trading_hours_only")
                position_size_pct = st.slider("仓位百分比", 5, 50, 20, key="ai_task_form_position_size_pct")
                stop_loss_pct = st.slider("止损百分比", 1, 20, 5, key="ai_task_form_stop_loss_pct")
                take_profit_pct = st.slider("止盈百分比", 1, 30, 10, key="ai_task_form_take_profit_pct")
                notify_email = st.text_input("通知邮箱", placeholder="可选", key="ai_task_form_notify_email")

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
                    'check_interval': interval_minutes * 60,
                    'auto_trade': 0,
                    'trading_hours_only': 1 if trading_hours_only else 0,
                    'position_size_pct': position_size_pct,
                    'stop_loss_pct': stop_loss_pct,
                    'take_profit_pct': take_profit_pct,
                    'notify_email': notify_email.strip() or None,
                    'account_name': normalized_account,
                    'origin_analysis_id': st.session_state.get("ai_task_form_origin_analysis_id"),
                    'strategy_context': st.session_state.get("ai_task_form_strategy_context") or {},
                })
                st.session_state["ai_task_form_account_name"] = DEFAULT_ACCOUNT_NAME
                st.session_state["ai_task_form_task_name"] = ""
                st.session_state["ai_task_form_stock_code"] = ""
                st.session_state["ai_task_form_stock_name"] = ""
                st.session_state["ai_task_form_interval_minutes"] = 5
                st.session_state["ai_task_form_trading_hours_only"] = True
                st.session_state["ai_task_form_position_size_pct"] = 20
                st.session_state["ai_task_form_stop_loss_pct"] = 5
                st.session_state["ai_task_form_take_profit_pct"] = 10
                st.session_state["ai_task_form_notify_email"] = ""
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
            st.success(f"已启用 {changed_count} 个任务。")
            st.rerun()
    with disable_all_col:
        if st.button(
            "一键停用全部",
            key="disable_all_ai_tasks",
            width='stretch',
            disabled=enabled_count == 0,
        ):
            changed_count = db.set_all_monitor_tasks_enabled(False)
            st.success(f"已停用 {changed_count} 个任务。")
            st.rerun()

    for task in tasks:
        asset = _get_task_asset(db, task)
        pending_actions = _get_pending_actions_for_task(db, task)
        buy_pending = next((item for item in pending_actions if item.get("action_type") == "buy"), None)
        sell_pending = next((item for item in pending_actions if item.get("action_type") == "sell"), None)

        with st.container():
            st.markdown(f"**{task['stock_code']}** {task.get('stock_name') or task['stock_code']}")

            strategy_context = task.get("strategy_context") or {}
            summary_col1, summary_col2, summary_col3 = st.columns(3)
            with summary_col1:
                st.caption(f"资产状态: {_format_asset_status(asset)}")
            with summary_col2:
                st.caption(f"持仓摘要: {_format_position_summary(asset)}")
            with summary_col3:
                if strategy_context:
                    st.caption(
                        f"战略线: 止盈 {strategy_context.get('take_profit') or '-'} / 止损 {strategy_context.get('stop_loss') or '-'}"
                    )
                else:
                    st.caption("战略线: 尚未形成")

            if pending_actions:
                pending_labels = [f"#{item['id']} {str(item.get('action_type', '')).upper()}" for item in pending_actions]
                st.error(f"待人工处理动作: {' | '.join(pending_labels)}")

            action_columns = st.columns(5 if not task.get('managed_by_portfolio') else 4)
            action_col1, action_col2, action_col3, action_col4 = action_columns[:4]
            with action_col1:
                if st.button("立即分析", key=f"run_ai_task_{task['id']}", width='stretch'):
                    from monitor_service import monitor_service

                    if monitor_service.manual_update_stock(task['id']):
                        st.success("已触发一次 AI 分析。")
                    else:
                        st.error("触发 AI 分析失败。")
                    st.rerun()
            with action_col2:
                toggle_label = "停用" if task.get('enabled') else "启用"
                if st.button(toggle_label, key=f"toggle_ai_task_{task['id']}", width='stretch'):
                    db.update_monitor_task(
                        task['stock_code'],
                        {
                            'enabled': 0 if task.get('enabled') else 1,
                            'account_name': task.get('account_name') or DEFAULT_ACCOUNT_NAME,
                            'portfolio_stock_id': task.get('portfolio_stock_id'),
                        },
                    )
                    st.success(f"任务已{toggle_label}。")
                    st.rerun()
            with action_col3:
                if st.button("登记买入", key=f"buy_ai_task_{task['id']}", width='stretch'):
                    st.session_state[f"ai_task_trade_open_{task['id']}"] = True
                    st.session_state[f"ai_task_trade_mode_{task['id']}"] = "buy"
                    st.rerun()
            with action_col4:
                if st.button(
                    "登记卖出",
                    key=f"sell_ai_task_{task['id']}",
                    width='stretch',
                    disabled=asset is None or asset.get("status") != "portfolio" or int(asset.get("quantity") or 0) <= 0,
                ):
                    st.session_state[f"ai_task_trade_open_{task['id']}"] = True
                    st.session_state[f"ai_task_trade_mode_{task['id']}"] = "sell"
                    st.rerun()
            if not task.get('managed_by_portfolio'):
                action_col5 = action_columns[4]
                with action_col5:
                    if st.button(
                        "删除",
                        key=f"delete_ai_task_{task['id']}",
                        width='stretch',
                    ):
                        db.delete_monitor_task(task['id'])
                        st.success("任务已删除。")
                        st.rerun()

            signal_col1, signal_col2 = st.columns(2)
            with signal_col1:
                if buy_pending:
                    st.caption(
                        f"买入待办 #{buy_pending['id']} | 建议价 {((buy_pending.get('payload') or {}).get('current_price') or '-')}"
                    )
            with signal_col2:
                if sell_pending:
                    st.caption(
                        f"卖出待办 #{sell_pending['id']} | 当前仓位 {int((asset or {}).get('quantity') or 0)} 股"
                    )

            _render_pending_action_trade_form(db, task, asset, pending_actions)

            st.markdown(
                "<div style='margin:0.45rem 0 0.7rem 0; border-bottom:1px solid rgba(148,163,184,0.18);'></div>",
                unsafe_allow_html=True,
            )


def smart_monitor_ui(lightweight_model=None, reasoning_model=None):
    """智能盯盘主界面（重构版）。"""
    from monitor_manager import display_price_alert_workspace

    _ensure_smart_monitor_runtime(lightweight_model, reasoning_model)

    desired_tab = st.session_state.get(SMART_MONITOR_ACTIVE_TAB_KEY, "watchlist")
    tab_labels = [
        "盯盘列表",
        "价格预警",
        "决策事件",
    ]
    tab_key_to_label = {
        "watchlist": "盯盘列表",
        "realtime": "盯盘列表",
        "ai_task": "盯盘列表",
        "price_alert": "价格预警",
        "decision_events": "决策事件",
        "history": "决策事件",
        "settings": "决策事件",
    }

    if desired_tab in tab_key_to_label and desired_tab != "watchlist":
        st.caption(f"已打开 智能盯盘，请切换到“{tab_key_to_label[desired_tab]}”标签查看目标内容。")
        st.session_state.pop(SMART_MONITOR_ACTIVE_TAB_KEY, None)

    tabs = st.tabs(tab_labels)

    with tabs[0]:
        render_ai_monitor_tasks_panel(title="盯盘列表")
        with st.expander("实时分析（按需展开）", expanded=False):
            render_realtime_analysis(show_header=False)
    with tabs[1]:
        display_price_alert_workspace()
    with tabs[2]:
        render_history(show_header=False, title="决策事件")
        with st.expander("系统设置（按需展开）", expanded=False):
            render_settings(show_header=False)


if __name__ == '__main__':
    smart_monitor_ui()

