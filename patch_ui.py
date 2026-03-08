import sys

with open("f:/zfywork/aiagents-stock/portfolio_ui.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1. Add "风险评估" tab
old_tabs = '''    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs([
        "持仓管理",
        "批量分析",
        "定时任务",
        "分析历史"
    ])
    
    with tab1:
        display_portfolio_stocks(lightweight_model, reasoning_model)
    
    with tab2:
        display_batch_analysis(lightweight_model, reasoning_model)
    
    with tab3:
        display_scheduler_management()
    
    with tab4:
        display_analysis_history()'''

new_tabs = '''    # 创建标签页
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "持仓管理",
        "风险评估",
        "批量分析",
        "定时任务",
        "分析历史"
    ])
    
    with tab1:
        display_portfolio_stocks(lightweight_model, reasoning_model)
        
    with tab2:
        display_portfolio_risk()
    
    with tab3:
        display_batch_analysis(lightweight_model, reasoning_model)
    
    with tab4:
        display_scheduler_management()
    
    with tab5:
        display_analysis_history()'''
code = code.replace(old_tabs, new_tabs)

# 2. Add Risk Assessment method
risk_method = '''
def display_portfolio_risk():
    """显示持仓风险评估"""
    st.markdown("### 🛡️ 组合风险评估")
    
    # 账号筛选
    all_stocks = portfolio_manager.get_all_stocks()
    if not all_stocks:
        st.info("暂无持仓股票，无法评估组合风险。")
        return
        
    accounts = ["全部账户"] + list(sorted(set(s.get("account_name", "默认账户") for s in all_stocks)))
    selected_account = st.selectbox("选择账户进行评估", accounts, key="risk_account_selector")
    
    account_filter = None if selected_account == "全部账户" else selected_account
    
    result = portfolio_manager.calculate_portfolio_risk(account_name=account_filter)
    
    if result.get("status") == "error":
        st.warning(result.get("message", "评估失败"))
        return
        
    total_val = result.get("total_market_value", 0)
    st.metric("总持仓市值", f"¥{total_val:,.2f}")
    
    # 显示警告
    warnings = result.get("risk_warnings", [])
    if result.get("high_concentration"):
        for w in warnings:
            st.error(w)
    else:
        for w in warnings:
            st.success(w)
            
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🔄 行业集中度")
        industry_data = result.get("industry_distribution", [])
        if industry_data:
            df_ind = pd.DataFrame(industry_data)
            df_ind["占比"] = df_ind["weight"].apply(lambda x: f"{x*100:.1f}%")
            df_ind["市值"] = df_ind["market_value"].apply(lambda x: f"¥{x:,.2f}")
            st.dataframe(df_ind[["industry", "市值", "占比"]].rename(columns={"industry": "行业"}), hide_index=True, use_container_width=True)
            
    with col2:
        st.markdown("#### 🎯 单票集中度")
        stock_data = result.get("stock_distribution", [])
        if stock_data:
            df_st = pd.DataFrame(stock_data)
            df_st["占比"] = df_st["weight"].apply(lambda x: f"{x*100:.1f}%")
            df_st["市值"] = df_st["market_value"].apply(lambda x: f"¥{x:,.2f}")
            st.dataframe(df_st[["name", "市值", "占比"]].rename(columns={"name": "股票"}), hide_index=True, use_container_width=True)
'''

# insert risk method
if "def display_portfolio_risk" not in code:
    code = code.replace("def display_portfolio_stocks", risk_method + "\n\ndef display_portfolio_stocks")

# 3. Modify display_portfolio_stocks to add account filter
old_stocks = '''    # 获取所有持仓股票
    stocks = portfolio_manager.get_all_stocks()
    
    if not stocks:'''

new_stocks = '''    # 获取所有持仓股票
    all_stocks = portfolio_manager.get_all_stocks()
    
    accounts = ["全部账户"] + list(sorted(set(s.get("account_name", "默认账户") for s in all_stocks)))
    selected_account = st.selectbox("账号筛选", accounts, key="portfolio_account_selector")
    
    if selected_account == "全部账户":
        stocks = all_stocks
    else:
        stocks = [s for s in all_stocks if s.get("account_name", "默认账户") == selected_account]
        
    if not stocks:'''
code = code.replace(old_stocks, new_stocks)

# 4. Modify display_add_stock_form
old_form = '''    with st.form(key="add_stock_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            code = st.text_input(
                "股票代码*", 
                placeholder="例如: 600519、000001.SZ、00700.HK、AAPL",'''

new_form = '''    with st.form(key="add_stock_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            account_name = st.text_input("账户名称", value="默认账户", help="隔离不同账户的持仓")
            code = st.text_input(
                "股票代码*", 
                placeholder="例如: 600519、000001.SZ、00700.HK、AAPL",'''
code = code.replace(old_form, new_form)

old_add_method = '''                    success, msg, _ = portfolio_manager.add_stock(
                        code=code.strip().upper(),
                        name=None,
                        cost_price=cost_price if cost_price > 0 else None,
                        quantity=quantity if quantity > 0 else None,
                        note=note.strip() if note else None,
                        auto_monitor=auto_monitor
                    )'''
new_add_method = '''                    success, msg, _ = portfolio_manager.add_stock(
                        code=code.strip().upper(),
                        name=None,
                        cost_price=cost_price if cost_price > 0 else None,
                        quantity=quantity if quantity > 0 else None,
                        note=note.strip() if note else None,
                        auto_monitor=auto_monitor,
                        account_name=account_name.strip()
                    )'''
code = code.replace(old_add_method, new_add_method)

# Write back
with open("f:/zfywork/aiagents-stock/portfolio_ui.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Done UI patch")
