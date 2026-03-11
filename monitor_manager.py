#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票监测管理模块
支持添加、删除、编辑监测股票
卡片式布局，支持关键位置监测
"""

import streamlit as st
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List
import json

from investment_db_utils import DEFAULT_ACCOUNT_NAME
from monitor_db import monitor_db
from monitor_service import monitor_service
from notification_service import notification_service
from stock_data import StockDataFetcher
from stock_data_cache import extract_cache_meta, strip_cache_meta
from ui_shared import format_price, get_recommendation_color
from ui_state_keys import (
    INVESTMENT_PRICE_ALERT_PREFILL_KEY,
    MONITOR_DELETING_STOCK_ID_KEY,
    MONITOR_EDITING_STOCK_ID_KEY,
    MONITOR_JUMP_HIGHLIGHT_KEY,
)

def _legacy_display_monitor_manager():
    """显示监测管理主页面"""

    # 检查是否有跳转提示
    if MONITOR_JUMP_HIGHLIGHT_KEY in st.session_state:
        symbol = st.session_state[MONITOR_JUMP_HIGHLIGHT_KEY]
        st.success(f"✅ {symbol} 已成功加入监测列表！您可以在下方查看。")
        del st.session_state[MONITOR_JUMP_HIGHLIGHT_KEY]
    
    # 监测服务状态
    display_monitor_status()
    
    # 添加新股票监测
    display_add_stock_section()
    
    # 监测股票列表
    display_monitored_stocks()
    
    # 通知管理
    display_notification_management()

def display_monitor_status():
    """显示监测服务状态"""
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if monitor_service.running:
            st.success("监测服务运行中")
        else:
            st.warning("监测服务已停止")
    
    with col2:
        stocks = monitor_db.get_monitored_stocks()
        st.metric("监测股票", len(stocks))
    
    with col3:
        notifications = monitor_db.get_pending_notifications()
        st.metric("待处理通知", len(notifications))
    
    with col4:
        if monitor_service.running:
            if st.button("⏹️ 停止监测", type="secondary"):
                monitor_service.stop_monitoring()
                st.success("✅ 监测服务已停止")
                st.rerun()
        else:
            if st.button("▶️ 启动监测", type="primary"):
                monitor_service.start_monitoring()
                st.success("✅ 监测服务已启动")
                st.rerun()
    
    with col5:
        if st.button("🔄 刷新状态"):
            st.rerun()
    
    # 显示定时调度状态和配置
    display_scheduler_section()

def display_add_stock_section():
    """显示添加股票监测区域"""
    monitor_service.ensure_started()
    monitor_service.ensure_stopped_if_idle()

    prefill = st.session_state.pop(INVESTMENT_PRICE_ALERT_PREFILL_KEY, None)
    if prefill:
        strategy_context = prefill.get("strategy_context") or {}
        st.session_state["price_alert_form_account_name"] = prefill.get("account_name") or DEFAULT_ACCOUNT_NAME
        st.session_state["price_alert_form_symbol"] = prefill.get("symbol") or ""
        st.session_state["price_alert_form_name"] = prefill.get("stock_name") or ""
        st.session_state["price_alert_form_entry_min"] = float(strategy_context.get("entry_min") or 0.0)
        st.session_state["price_alert_form_entry_max"] = float(strategy_context.get("entry_max") or 0.0)
        st.session_state["price_alert_form_take_profit"] = float(strategy_context.get("take_profit") or 0.0)
        st.session_state["price_alert_form_stop_loss"] = float(strategy_context.get("stop_loss") or 0.0)
        st.session_state["price_alert_form_rating"] = strategy_context.get("rating") or "买入"
        st.session_state["price_alert_form_origin_analysis_id"] = prefill.get("origin_analysis_id")
        st.session_state["price_alert_form_strategy_context"] = strategy_context
        st.session_state["price_alert_form_notice"] = f"{prefill.get('symbol')} 的策略价位已带入价格预警表单。"

    st.session_state.setdefault("price_alert_form_account_name", DEFAULT_ACCOUNT_NAME)
    st.session_state.setdefault("price_alert_form_symbol", "")
    st.session_state.setdefault("price_alert_form_name", "")
    st.session_state.setdefault("price_alert_form_entry_min", 0.0)
    st.session_state.setdefault("price_alert_form_entry_max", 0.0)
    st.session_state.setdefault("price_alert_form_take_profit", 0.0)
    st.session_state.setdefault("price_alert_form_stop_loss", 0.0)
    st.session_state.setdefault("price_alert_form_rating", "买入")
    st.session_state.setdefault("price_alert_form_origin_analysis_id", None)
    st.session_state.setdefault("price_alert_form_strategy_context", {})

    st.markdown("### 添加价格预警")

    with st.expander("点击展开添加价格预警", expanded=bool(st.session_state.get("price_alert_form_symbol"))):
        if st.session_state.get("price_alert_form_notice"):
            st.info(st.session_state["price_alert_form_notice"])
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # 股票信息输入
            st.subheader("📈 股票信息")
            account_name = st.text_input("账户名称", key="price_alert_form_account_name")
            symbol = st.text_input(
                "股票代码",
                placeholder="例如: AAPL, 000001",
                help="支持美股和A股代码",
                key="price_alert_form_symbol",
            )
            name = st.text_input(
                "股票名称",
                placeholder="例如: 苹果公司",
                help="可选，用于显示",
                key="price_alert_form_name",
            )
            
            # 获取股票基本信息
            if symbol:
                if st.button("🔍 获取股票信息"):
                    with st.spinner("正在获取股票信息..."):
                        fetcher = StockDataFetcher()
                        stock_info = fetcher.get_stock_info(
                            symbol,
                            max_age_seconds=30,
                            allow_stale_on_failure=True,
                            cache_first=True,
                        )
                        
                        if "error" not in stock_info:
                            st.success("✅ 股票信息获取成功")
                            cache_meta = extract_cache_meta(stock_info)
                            if cache_meta and cache_meta.get("stale"):
                                st.warning(
                                    f"当前使用本地缓存数据，缓存时间：{cache_meta.get('fetched_at') or '未知时间'}"
                                )
                            st.session_state.temp_stock_info = strip_cache_meta(stock_info)
                        else:
                            st.error(f"❌ {stock_info['error']}")
        
        with col2:
            # 监测设置
            st.subheader("⚙️ 监测设置")
            
            # 关键位置设置
            st.markdown("**🎯 关键位置设置**")
            entry_min = st.number_input("进场区间最低价", step=0.01, format="%.2f", key="price_alert_form_entry_min")
            entry_max = st.number_input("进场区间最高价", step=0.01, format="%.2f", key="price_alert_form_entry_max")
            take_profit = st.number_input("止盈价位", step=0.01, format="%.2f", help="可选", key="price_alert_form_take_profit")
            stop_loss = st.number_input("止损价位", step=0.01, format="%.2f", help="可选", key="price_alert_form_stop_loss")
            
            # 监测参数
            st.markdown("**⏰ 监测参数**")
            check_interval = st.slider("监测间隔(分钟)", 3, 120, 3)
            notification_enabled = st.checkbox("启用通知", value=True)
            
            # 投资评级
            rating_options = ["买入", "持有", "卖出"]
            rating_value = st.session_state.get("price_alert_form_rating", "买入")
            rating = st.selectbox(
                "投资评级",
                rating_options,
                index=rating_options.index(rating_value) if rating_value in rating_options else 0,
                key="price_alert_form_rating",
            )
            
        # 添加按钮
        if st.button("保存价格预警", type="primary", width='stretch'):
            if symbol and entry_min > 0 and entry_max > 0 and entry_max > entry_min:
                try:
                    # 准备数据
                    entry_range = {"min": entry_min, "max": entry_max}

                    # 添加到数据库
                    stock_id = monitor_db.add_monitored_stock(
                        symbol=symbol.strip().upper(),
                        name=name or symbol,
                        rating=rating,
                        entry_range=entry_range,
                        take_profit=take_profit if take_profit > 0 else None,
                        stop_loss=stop_loss if stop_loss > 0 else None,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                        account_name=(account_name or DEFAULT_ACCOUNT_NAME).strip() or DEFAULT_ACCOUNT_NAME,
                        origin_analysis_id=st.session_state.get("price_alert_form_origin_analysis_id"),
                    )
                    
                    st.session_state["price_alert_form_account_name"] = DEFAULT_ACCOUNT_NAME
                    st.session_state["price_alert_form_symbol"] = ""
                    st.session_state["price_alert_form_name"] = ""
                    st.session_state["price_alert_form_entry_min"] = 0.0
                    st.session_state["price_alert_form_entry_max"] = 0.0
                    st.session_state["price_alert_form_take_profit"] = 0.0
                    st.session_state["price_alert_form_stop_loss"] = 0.0
                    st.session_state["price_alert_form_rating"] = "买入"
                    st.session_state["price_alert_form_origin_analysis_id"] = None
                    st.session_state["price_alert_form_strategy_context"] = {}
                    st.session_state.pop("price_alert_form_notice", None)
                    st.success(f"已成功添加 {symbol} 到价格预警")
                    st.balloons()
                    
                    # 立即更新一次价格
                    monitor_service.ensure_started()
                    monitor_service.manual_update_stock(stock_id)
                    
                    # 清空表单
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ 添加失败: {str(e)}")
            else:
                st.error("❌ 请填写完整的股票信息和有效的进场区间")

def display_monitored_stocks():
    """显示监测股票列表 - 卡片式布局"""
    
    st.markdown("### 价格预警列表")
    
    stocks = monitor_db.get_monitored_stocks()
    
    if not stocks:
        st.info("暂无价格预警，请先添加预警标的。")
        return
    
    # 筛选和搜索
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_term = st.text_input("搜索股票", placeholder="输入股票代码或名称")
    
    with col2:
        rating_filter = st.selectbox("评级筛选", ["全部", "买入", "持有", "卖出"])
    
    with col3:
        if st.button("刷新列表"):
            st.rerun()
    
    # 筛选股票
    filtered_stocks = stocks
    if search_term:
        filtered_stocks = [s for s in stocks if search_term.lower() in s['symbol'].lower() or search_term.lower() in s['name'].lower()]
    
    if rating_filter != "全部":
        filtered_stocks = [s for s in filtered_stocks if s['rating'] == rating_filter]
    
    if not filtered_stocks:
        st.warning("未找到匹配的股票")
        return
    
    for stock in filtered_stocks:
        display_stock_card(stock)
    
    # 显示编辑对话框
    if MONITOR_EDITING_STOCK_ID_KEY in st.session_state:
        display_edit_dialog(st.session_state[MONITOR_EDITING_STOCK_ID_KEY])
    
    # 显示删除确认对话框
    if MONITOR_DELETING_STOCK_ID_KEY in st.session_state:
        display_delete_confirm_dialog(st.session_state[MONITOR_DELETING_STOCK_ID_KEY])

def display_stock_card(stock: Dict):
    """显示单个股票监测卡片"""

    entry_range = stock.get("entry_range")
    if entry_range and isinstance(entry_range, dict):
        entry_text = (
            f"{format_price(entry_range.get('min'), currency='¥')} - "
            f"{format_price(entry_range.get('max'), currency='¥')}"
        )
    else:
        entry_text = "未设置"

    take_profit = stock.get("take_profit")
    stop_loss = stock.get("stop_loss")
    take_profit_text = format_price(take_profit, currency="¥") if take_profit else "未设置"
    stop_loss_text = format_price(stop_loss, currency="¥") if stop_loss else "未设置"

    current_price = stock.get("current_price")
    if current_price and current_price != "N/A":
        current_price_text = format_price(current_price, currency="¥")
    else:
        current_price_text = "等待更新"

    last_checked_text = "从未检查"
    if stock.get("last_checked"):
        try:
            last_checked = datetime.fromisoformat(stock["last_checked"])
            last_checked_text = last_checked.strftime("%m-%d %H:%M")
        except ValueError:
            last_checked_text = str(stock["last_checked"])

    notify_text = "通知开启" if stock.get("notification_enabled") else "通知关闭"
    rating = stock.get("rating", "未评级")
    rating_color = get_recommendation_color(rating)

    with st.container():
        header_col, status_col = st.columns([4, 1.4])

        with header_col:
            st.markdown(f"**{stock['symbol']}** {stock['name']}")
            if stock.get('managed_by_portfolio'):
                st.caption("来源: 持仓同步")
            elif stock.get("source") == "ai_monitor":
                st.caption("来源: 智能盯盘")

            st.markdown(
                f"评级: <span style='color: {rating_color}; font-weight: 600;'>{rating}</span>",
                unsafe_allow_html=True,
            )

        with status_col:
            st.caption("当前价格")
            st.markdown(
                f"<div style='text-align:right; font-weight:600;'>{current_price_text}</div>",
                unsafe_allow_html=True,
            )

        info_col1, info_col2 = st.columns(2)

        with info_col1:
            st.caption(f"进场区间: {entry_text}")
            st.caption(f"止盈位: {take_profit_text}")
            st.caption(f"止损位: {stop_loss_text}")

        with info_col2:
            st.caption(f"监测间隔: {stock['check_interval']} 分钟")
            st.caption(f"最后检查: {last_checked_text}")
            st.caption(f"监控状态: {'已启用' if stock.get('enabled', True) else '已停用'}")
            st.caption(notify_text)

        action_col1, action_col2 = st.columns(2)

        with action_col1:
            if st.button("更新", key=f"update_{stock['id']}"):
                if monitor_service.manual_update_stock(stock['id']):
                    st.success("更新成功")
                else:
                    st.error("更新失败")

        with action_col2:
            if st.button("编辑", key=f"edit_{stock['id']}"):
                st.session_state[MONITOR_EDITING_STOCK_ID_KEY] = stock['id']
                st.rerun()

        toggle_col, delete_col = st.columns(2)

        with toggle_col:
            current_status = stock['notification_enabled']
            if current_status:
                if st.button("关闭通知", key=f"notify_{stock['id']}"):
                    monitor_db.toggle_notification(stock['id'], False)
                    st.success("已关闭通知")
                    st.rerun()
            else:
                if st.button("开启通知", key=f"notify_{stock['id']}"):
                    monitor_db.toggle_notification(stock['id'], True)
                    st.success("已开启通知")
                    st.rerun()

        with delete_col:
            if st.button("删除", key=f"delete_{stock['id']}"):
                st.session_state[MONITOR_DELETING_STOCK_ID_KEY] = stock['id']
                st.rerun()

        st.markdown(
            "<div style='margin:0.45rem 0 0.7rem 0; border-bottom:1px solid rgba(148,163,184,0.18);'></div>",
            unsafe_allow_html=True,
        )

def _legacy_display_edit_dialog(stock_id: int):
    """显示编辑股票对话框"""
    
    stock = monitor_db.get_stock_by_id(stock_id)
    if not stock:
        st.error("❌ 股票不存在")
        del st.session_state[MONITOR_EDITING_STOCK_ID_KEY]
        return
    
    st.markdown("---")
    st.markdown(f"### ✏️ 编辑监测 - {stock['symbol']} {stock['name']}")
    
    with st.form(key=f"edit_form_{stock_id}"):
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("🎯 关键位置")
            entry_range = stock.get('entry_range', {})
            entry_min = st.number_input("进场区间最低价", value=float(entry_range.get('min', 0)), step=0.01, format="%.2f")
            entry_max = st.number_input("进场区间最高价", value=float(entry_range.get('max', 0)), step=0.01, format="%.2f")
            take_profit = st.number_input("止盈价位", value=float(stock['take_profit']) if stock['take_profit'] else 0.0, step=0.01, format="%.2f")
            stop_loss = st.number_input("止损价位", value=float(stock['stop_loss']) if stock['stop_loss'] else 0.0, step=0.01, format="%.2f")
        
        with col2:
            st.subheader("⚙️ 监测设置")
            check_interval = st.slider("监测间隔(分钟)", 3, 120, stock['check_interval'])
            rating = st.selectbox("投资评级", ["买入", "持有", "卖出"], 
                                 index=["买入", "持有", "卖出"].index(stock['rating']) if stock['rating'] in ["买入", "持有", "卖出"] else 0)
            notification_enabled = st.checkbox("启用通知", value=stock['notification_enabled'])
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            submit = st.form_submit_button("✅ 保存修改", type="primary", width='stretch')
        
        with col2:
            cancel = st.form_submit_button("❌ 取消", width='stretch')
        
        if submit:
            if entry_min > 0 and entry_max > 0 and entry_max > entry_min:
                try:
                    # 更新数据库
                    new_entry_range = {"min": entry_min, "max": entry_max}

                    monitor_db.update_monitored_stock(
                        stock_id=stock_id,
                        rating=rating,
                        entry_range=new_entry_range,
                        take_profit=take_profit if take_profit > 0 else None,
                        stop_loss=stop_loss if stop_loss > 0 else None,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                    )
                    
                    st.success("✅ 修改已保存")
                    del st.session_state[MONITOR_EDITING_STOCK_ID_KEY]
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 保存失败: {str(e)}")
            else:
                st.error("❌ 请输入有效的进场区间")
        
        if cancel:
            del st.session_state[MONITOR_EDITING_STOCK_ID_KEY]
            st.rerun()

def _legacy_display_delete_confirm_dialog(stock_id: int):
    """显示删除确认对话框"""
    
    stock = monitor_db.get_stock_by_id(stock_id)
    if not stock:
        st.error("❌ 股票不存在或已被删除")
        if MONITOR_DELETING_STOCK_ID_KEY in st.session_state:
            del st.session_state[MONITOR_DELETING_STOCK_ID_KEY]
        st.rerun()
        return
    
    st.markdown("---")
    st.markdown(f"### ⚠️ 确认删除")
    
    st.warning(f"""
    您确定要删除以下监测吗？
    
    **股票代码**: {stock['symbol']}
    
    **股票名称**: {stock['name']}
    
    **投资评级**: {stock['rating']}
    
    此操作不可撤销！
    """)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🗑️ 确认删除", type="primary", width='stretch', key=f"confirm_delete_{stock_id}"):
            try:
                result = monitor_db.remove_monitored_stock(stock_id)
                if result:
                    # 清理session state
                    if MONITOR_DELETING_STOCK_ID_KEY in st.session_state:
                        del st.session_state[MONITOR_DELETING_STOCK_ID_KEY]
                    
                    st.success("✅ 已成功删除监测")
                    st.balloons()
                    time.sleep(0.8)  # 短暂延迟，让用户看到成功消息
                    st.rerun()
                else:
                    st.error("❌ 删除失败：股票不存在或已被删除")
                    time.sleep(1)
                    if MONITOR_DELETING_STOCK_ID_KEY in st.session_state:
                        del st.session_state[MONITOR_DELETING_STOCK_ID_KEY]
                    st.rerun()
            except Exception as e:
                st.error(f"❌ 删除失败：{str(e)}")
                time.sleep(1)
                if MONITOR_DELETING_STOCK_ID_KEY in st.session_state:
                    del st.session_state[MONITOR_DELETING_STOCK_ID_KEY]
                st.rerun()
    
    with col2:
        if st.button("❌ 取消", width='stretch', key=f"cancel_delete_{stock_id}"):
            del st.session_state[MONITOR_DELETING_STOCK_ID_KEY]
            st.rerun()

def display_notification_management():
    """显示通知管理"""
    
    st.markdown("### 🔔 通知管理")
    st.markdown("---")
    
    # 通知设置
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📧 邮件通知设置")
        
        # 获取当前邮件配置状态
        email_config = notification_service.get_email_config_status()
        
        # 显示配置状态
        if email_config['configured']:
            st.success("✅ 邮件配置已完成")
        else:
            st.warning("⚠️ 邮件未配置或配置不完整")
        
        # 显示配置信息
        st.info(f"""
        **当前配置：**
        - SMTP服务器: {email_config['smtp_server']}
        - SMTP端口: {email_config['smtp_port']}
        - 发送邮箱: {email_config['email_from']}
        - 接收邮箱: {email_config['email_to']}
        - 启用状态: {'是' if email_config['enabled'] else '否'}
        """)
        
        st.markdown("---")
        st.markdown("**⚙️ 配置说明**")
        st.caption("""
        在 `.env` 文件中配置以下参数：
        ```
        EMAIL_ENABLED=true
        SMTP_SERVER=smtp.qq.com
        SMTP_PORT=587
        EMAIL_FROM=your_email@qq.com
        EMAIL_PASSWORD=your_authorization_code
        EMAIL_TO=receiver@example.com
        ```
        
        💡 提示：
        - 端口：587 (TLS) 或 465 (SSL)
        - 密码：使用邮箱授权码，不是登录密码
        - QQ邮箱授权码获取：设置 → 账户 → POP3/IMAP/SMTP → 生成授权码
        """)
        
        # 测试邮件按钮
        if email_config['configured']:
            if st.button("📧 发送测试邮件", type="primary", width='stretch'):
                with st.spinner("正在发送测试邮件..."):
                    success, message = notification_service.send_test_email()
                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                    else:
                        st.error(f"❌ {message}")
        else:
            st.button("📧 发送测试邮件", type="primary", width='stretch', disabled=True)
            st.caption("请先在.env文件中配置邮件参数")
    
    with col2:
        st.subheader("📱 通知历史")
        
        # 显示所有通知（包括已发送和未发送的）
        all_notifications = monitor_db.get_all_recent_notifications(limit=10)
        
        if all_notifications:
            # 显示通知列表
            for notification in all_notifications:
                notification_type = notification['type']
                color_map = {
                    'entry': '入场',
                    'take_profit': '止盈',
                    'stop_loss': '止损',
                }
                icon = color_map.get(notification_type, '通知')
                
                # 显示已发送状态
                sent_status = "✅ 已发送" if notification.get('sent') else "⏳ 待发送"
                
                # 显示通知信息
                st.info(f"[{icon}] **{notification['symbol']}** - {notification['message']}\n\n_{notification['triggered_at']}_ | {sent_status}")
            
            # 显示待发送通知数量
            pending_count = len([n for n in all_notifications if not n.get('sent')])
            if pending_count > 0:
                st.warning(f"⚠️ 有 {pending_count} 条待发送通知")
            
            # 清空通知按钮
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ 标记已读"):
                    monitor_db.mark_all_notifications_sent()
                    st.success("✅ 所有通知已标记为已读")
                    st.rerun()
            
            with col_b:
                if st.button("🗑️ 清空通知"):
                    monitor_db.clear_all_notifications()
                    st.success("✅ 通知已清空")
                    st.rerun()
        else:
            st.info("📭 暂无通知")

def display_scheduler_section():
    """显示定时调度配置区域"""
    st.markdown("---")
    st.markdown("### ⏰ 定时自动启动/关闭")
    
    # 获取调度器实例
    scheduler = monitor_service.get_scheduler()
    status = scheduler.get_status()
    
    # 状态显示
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if status['scheduler_enabled']:
            st.info("定时已启用")
        else:
            st.info("定时未启用")
    
    with col2:
        if status['scheduler_running']:
            st.info("调度器运行中")
        else:
            st.info("调度器未运行")
    
    with col3:
        if status['is_trading_day']:
            st.success(f"📅 交易日")
        else:
            st.info("📅 非交易日")
    
    with col4:
        if status['is_trading_time']:
            st.success("⏰ 交易时间内")
        else:
            st.info(f"⏰ {status['next_trading_time']}")
    
    # 配置设置
    with st.expander("⚙️ 定时调度设置", expanded=False):
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("📊 市场选择")
            
            market = st.selectbox(
                "选择市场",
                ["CN", "US", "HK"],
                index=["CN", "US", "HK"].index(scheduler.config.get('market', 'CN')),
                help="CN=中国A股, US=美股, HK=港股"
            )
            
            market_names = {
                "CN": "中国A股",
                "US": "美股",
                "HK": "港股"
            }
            st.info(f"**当前市场**: {market_names.get(market, market)}")
            
            # 显示交易时间
            trading_hours = scheduler.config['trading_hours'].get(market, [])
            st.markdown("**📅 交易时间：**")
            for i, period in enumerate(trading_hours, 1):
                st.caption(f"时段{i}: {period['start']} - {period['end']}")
            
            # 交易日设置
            st.markdown("**📅 交易日设置**")
            trading_days = st.multiselect(
                "选择交易日",
                options=[1, 2, 3, 4, 5, 6, 7],
                default=scheduler.config.get('trading_days', [1, 2, 3, 4, 5]),
                format_func=lambda x: ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][x-1],
                help="选择哪些日期为交易日"
            )
        
        with col2:
            st.subheader("⚙️ 调度参数")
            
            enabled = st.checkbox(
                "启用定时调度",
                value=scheduler.config.get('enabled', False),
                help="启用后将在交易时间自动启动监测服务"
            )
            
            auto_stop = st.checkbox(
                "收盘后自动停止",
                value=scheduler.config.get('auto_stop', True),
                help="在交易时间结束后自动停止监测服务"
            )
            
            pre_market_minutes = st.slider(
                "提前启动(分钟)",
                min_value=0,
                max_value=30,
                value=scheduler.config.get('pre_market_minutes', 5),
                help="在开盘前提前多少分钟启动"
            )
            
            post_market_minutes = st.slider(
                "延后停止(分钟)",
                min_value=0,
                max_value=30,
                value=scheduler.config.get('post_market_minutes', 5),
                help="在收盘后延后多少分钟停止"
            )
            
            st.markdown("---")
            
            # 说明信息
            st.info("""
            **💡 使用说明：**
            - 启用定时调度后，系统将在交易时间自动启动监测
            - 非交易时间或非交易日将自动停止监测（如启用自动停止）
            - 调度器独立运行，不影响手动启动/停止
            - 支持中国A股、美股、港股交易时间
            """)
        
        # 保存按钮
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("💾 保存设置", type="primary", width='stretch'):
                try:
                    # 更新配置
                    scheduler.update_config(
                        enabled=enabled,
                        market=market,
                        trading_days=trading_days,
                        auto_stop=auto_stop,
                        pre_market_minutes=pre_market_minutes,
                        post_market_minutes=post_market_minutes
                    )
                    
                    st.success("✅ 设置已保存")
                    st.balloons()
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 保存失败: {e}")
        
        with col2:
            if status['scheduler_running']:
                if st.button("⏹️ 停止调度器", width='stretch'):
                    scheduler.stop_scheduler()
                    st.info("⏸️ 调度器已停止")
                    time.sleep(0.5)
                    st.rerun()
            else:
                if enabled:
                    if st.button("▶️ 启动调度器", type="secondary", width='stretch'):
                        scheduler.start_scheduler()
                        st.success("✅ 调度器已启动")
                        time.sleep(0.5)
                        st.rerun()
                else:
                    st.button("▶️ 启动调度器", width='stretch', disabled=True)
                    st.caption("请先启用定时调度")
        
        with col3:
            if st.button("🔄 刷新状态", width='stretch'):
                st.rerun()

def _legacy_get_monitor_summary():
    """获取监测摘要信息"""
    stocks = monitor_db.get_monitored_stocks()
    
    summary = {
        'total_stocks': len(stocks),
        'stocks_needing_update': len(monitor_service.get_stocks_needing_update()),
        'pending_notifications': len(monitor_db.get_pending_notifications()),
        'active_monitoring': monitor_service.running
    }
    
    return summary


def _show_monitor_jump_success():
    if MONITOR_JUMP_HIGHLIGHT_KEY in st.session_state:
        symbol = st.session_state[MONITOR_JUMP_HIGHLIGHT_KEY]
        st.success(f"{symbol} 已成功加入价格预警。")
        del st.session_state[MONITOR_JUMP_HIGHLIGHT_KEY]


def display_price_alert_workspace():
    """价格预警工作台，供 AI 盯盘页复用。"""
    st.header("价格预警")
    monitor_service.ensure_started()
    monitor_service.ensure_stopped_if_idle()
    _show_monitor_jump_success()
    display_add_stock_section()
    display_monitored_stocks()


def display_monitoring_registry():
    """监测服务注册表，只读展示统一监控项。"""
    import pandas as pd

    st.markdown("### 监控注册表")

    col1, col2, col3 = st.columns([1.2, 1, 1])
    with col1:
        monitor_type = st.selectbox("类型过滤", ["全部", "AI监控任务", "价格预警"], index=0)
    with col2:
        source_filter = st.selectbox("来源过滤", ["全部", "手工", "持仓"], index=0)
    with col3:
        enabled_only = st.checkbox("仅显示启用项", value=False)

    monitor_type_value = None
    if monitor_type == "AI监控任务":
        monitor_type_value = "ai_task"
    elif monitor_type == "价格预警":
        monitor_type_value = "price_alert"

    managed_filter = None
    if source_filter == "手工":
        managed_filter = False
    elif source_filter == "持仓":
        managed_filter = True

    items = monitor_service.get_registry_items(
        monitor_type=monitor_type_value,
        managed_by_portfolio=managed_filter,
        enabled_only=enabled_only,
    )

    if not items:
        st.info("当前没有可展示的监控项。")
        return

    rows = []
    for item in items:
        rows.append({
            "代码": item["symbol"],
            "名称": item.get("name") or item["symbol"],
            "类型": "AI监控任务" if item["monitor_type"] == "ai_task" else "价格预警",
            "来源": "持仓" if item.get("managed_by_portfolio") else "手工",
            "启用": "是" if item.get("enabled") else "否",
            "间隔(分钟)": item.get("interval_minutes"),
            "交易时段": "仅交易时段" if item.get("trading_hours_only") else "全天",
            "最后状态": item.get("last_status") or "-",
            "最后检查": item.get("last_checked") or "-",
        })

    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        hide_index=True,
        width='stretch',
        height=min(680, max(220, 38 * (len(df) + 1))),
    )


def display_recent_monitor_events():
    """展示统一事件流。"""
    st.markdown("### 最近事件")
    events = monitor_service.get_recent_events(limit=30)
    if not events:
        st.info("暂无事件记录。")
        return

    for event in events[:20]:
        event_time = event.get("created_at", "-")
        event_type = event.get("event_type", "-")
        sent_text = "已发送" if event.get("sent") else "未发送"
        st.caption(
            f"{event_time} | {event.get('symbol')} | {event_type} | {sent_text} | "
            f"{event.get('message')}"
        )


def display_monitor_manager():
    """监测服务页，只承担运维与状态展示。"""
    st.header("监测服务")
    display_monitor_status()
    display_recent_monitor_events()
    display_notification_management()


def get_monitor_summary():
    """获取统一监控摘要信息。"""
    items = monitor_service.get_registry_items()
    price_alerts = [item for item in items if item["monitor_type"] == "price_alert"]

    return {
        'total_stocks': len(price_alerts),
        'stocks_needing_update': len(monitor_service.get_stocks_needing_update()),
        'pending_notifications': len(monitor_db.get_pending_notifications()),
        'active_monitoring': monitor_service.running,
    }


def display_edit_dialog(stock_id: int):
    """重构后的价格预警编辑框。"""
    stock = monitor_db.get_stock_by_id(stock_id)
    if not stock:
        st.error("价格预警不存在。")
        st.session_state.pop(MONITOR_EDITING_STOCK_ID_KEY, None)
        return

    st.markdown("---")
    st.markdown(f"### 编辑价格预警 - {stock['symbol']} {stock['name']}")

    managed = bool(stock.get('managed_by_portfolio') or stock.get("source") == "ai_monitor")
    if managed:
        owner_label = "持仓分析" if stock.get("managed_by_portfolio") else "智能盯盘"
        st.info(f"该价格预警来自{owner_label}托管。这里仅允许调整通知开关，核心价位请回到上游源头修改。")

    with st.form(key=f"price_alert_edit_{stock_id}"):
        if managed:
            notification_enabled = st.checkbox("启用通知", value=stock['notification_enabled'])
            submit = st.form_submit_button("保存", type="primary", width='stretch')
            if submit:
                monitor_db.toggle_notification(stock_id, notification_enabled)
                st.success("通知设置已更新。")
                st.session_state.pop(MONITOR_EDITING_STOCK_ID_KEY, None)
                st.rerun()
        else:
            col1, col2 = st.columns(2)
            entry_range = stock.get('entry_range', {}) or {}
            with col1:
                entry_min = st.number_input("进场区间最低价", value=float(entry_range.get('min', 0) or 0), step=0.01, format="%.2f")
                entry_max = st.number_input("进场区间最高价", value=float(entry_range.get('max', 0) or 0), step=0.01, format="%.2f")
                take_profit = st.number_input("止盈价", value=float(stock['take_profit'] or 0), step=0.01, format="%.2f")
                stop_loss = st.number_input("止损价", value=float(stock['stop_loss'] or 0), step=0.01, format="%.2f")
            with col2:
                check_interval = st.slider("监测间隔(分钟)", 3, 240, int(stock['check_interval']))
                rating = st.selectbox("投资评级", ["买入", "持有", "卖出"], index=["买入", "持有", "卖出"].index(stock['rating']) if stock['rating'] in ["买入", "持有", "卖出"] else 0)
                notification_enabled = st.checkbox("启用通知", value=stock['notification_enabled'])

            submit = st.form_submit_button("保存", type="primary", width='stretch')
            if submit:
                if entry_min <= 0 or entry_max <= 0 or entry_max <= entry_min:
                    st.error("请输入有效的进场区间。")
                else:
                    monitor_db.update_monitored_stock(
                        stock_id=stock_id,
                        rating=rating,
                        entry_range={"min": entry_min, "max": entry_max},
                        take_profit=take_profit if take_profit > 0 else None,
                        stop_loss=stop_loss if stop_loss > 0 else None,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                    )
                    monitor_service.ensure_started()
                    st.success("价格预警已更新。")
                    st.session_state.pop(MONITOR_EDITING_STOCK_ID_KEY, None)
                    st.rerun()

    if st.button("取消编辑", key=f"cancel_edit_price_alert_{stock_id}", width='stretch'):
        st.session_state.pop(MONITOR_EDITING_STOCK_ID_KEY, None)
        st.rerun()


def display_delete_confirm_dialog(stock_id: int):
    """重构后的删除确认框。"""
    stock = monitor_db.get_stock_by_id(stock_id)
    if not stock:
        st.error("价格预警不存在或已删除。")
        st.session_state.pop(MONITOR_DELETING_STOCK_ID_KEY, None)
        st.rerun()
        return

    st.markdown("---")
    st.markdown("### 删除价格预警")

    if stock.get('managed_by_portfolio') or stock.get("source") == "ai_monitor":
        owner_label = "持仓分析" if stock.get("managed_by_portfolio") else "智能盯盘"
        st.warning(f"该价格预警来自{owner_label}托管，不能在这里删除。请到上游源头移除。")
        if st.button("关闭", key=f"close_delete_blocked_{stock_id}", width='stretch'):
            st.session_state.pop(MONITOR_DELETING_STOCK_ID_KEY, None)
            st.rerun()
        return

    st.warning(
        f"确认删除 {stock['symbol']} {stock['name']} 的价格预警？此操作不可撤销。"
    )
    col1, col2 = st.columns(2)
    with col1:
        if st.button("确认删除", type="primary", key=f"confirm_delete_price_alert_{stock_id}", width='stretch'):
            monitor_db.remove_monitored_stock(stock_id)
            monitor_service.ensure_stopped_if_idle()
            st.success("价格预警已删除。")
            st.session_state.pop(MONITOR_DELETING_STOCK_ID_KEY, None)
            st.rerun()
    with col2:
        if st.button("取消", key=f"cancel_delete_price_alert_{stock_id}", width='stretch'):
            st.session_state.pop(MONITOR_DELETING_STOCK_ID_KEY, None)
            st.rerun()
