import sys

with open("f:/zfywork/aiagents-stock/portfolio_manager.py", "r", encoding="utf-8") as f:
    code = f.read()

old_risk_init = '''        total_market_value = 0.0
        stock_values = []
        industry_values = {}'''

new_risk_init = '''        total_market_value = 0.0
        total_cost_value = 0.0
        stock_values = []
        industry_values = {}'''
code = code.replace(old_risk_init, new_risk_init)

old_risk_loop = '''            if current_price is None or current_price == 0:
                cost_price = stock.get("cost_price", 0)
                current_price = float(cost_price) if cost_price else 0.0
                
            market_value = current_price * quantity_value
            total_market_value += market_value'''

new_risk_loop = '''            cost_price = float(stock.get("cost_price", 0) or 0.0)
            if current_price is None or current_price == 0:
                current_price = cost_price
                
            market_value = current_price * quantity_value
            cost_value = cost_price * quantity_value
            total_market_value += market_value
            total_cost_value += cost_value'''
code = code.replace(old_risk_loop, new_risk_loop)

old_risk_stock_append = '''            stock_values.append({
                "code": stock['code'],
                "name": stock['name'],
                "market_value": market_value,
                "industry": industry
            })'''

new_risk_stock_append = '''            
            pnl = market_value - cost_value
            pnl_pct = (pnl / cost_value) if cost_value > 0 else 0.0

            stock_values.append({
                "code": stock['code'],
                "name": stock['name'],
                "market_value": market_value,
                "cost_value": cost_value,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "industry": industry
            })'''
code = code.replace(old_risk_stock_append, new_risk_stock_append)

old_risk_return = '''        return {
            "status": "success",
            "total_market_value": total_market_value,
            "stock_distribution": stock_values,
            "industry_distribution": industry_distribution,
            "high_concentration": high_concentration,
            "risk_warnings": risk_warnings
        }'''

new_risk_return = '''        total_pnl = total_market_value - total_cost_value
        total_pnl_pct = (total_pnl / total_cost_value) if total_cost_value > 0 else 0.0

        return {
            "status": "success",
            "total_market_value": total_market_value,
            "total_cost_value": total_cost_value,
            "total_pnl": total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "stock_distribution": stock_values,
            "industry_distribution": industry_distribution,
            "high_concentration": high_concentration,
            "risk_warnings": risk_warnings
        }'''
code = code.replace(old_risk_return, new_risk_return)

with open("f:/zfywork/aiagents-stock/portfolio_manager.py", "w", encoding="utf-8") as f:
    f.write(code)

with open("f:/zfywork/aiagents-stock/portfolio_ui.py", "r", encoding="utf-8") as f:
    ui_code = f.read()

old_ui_metrics = '''    total_val = result.get("total_market_value", 0)
    st.metric("总持仓市值", f"¥{total_val:,.2f}")
    
    # 显示警告'''

new_ui_metrics = '''    total_val = result.get("total_market_value", 0)
    total_cost = result.get("total_cost_value", 0)
    total_pnl = result.get("total_pnl", 0)
    total_pnl_pct = result.get("total_pnl_pct", 0) * 100
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("总持仓市值", f"¥{total_val:,.2f}")
    m2.metric("总持仓成本", f"¥{total_cost:,.2f}")
    m3.metric("总浮动盈亏", f"¥{total_pnl:,.2f}", delta=f"{total_pnl_pct:.2f}%", delta_color="inverse")
    
    st.markdown("---")
    # 显示警告'''
ui_code = ui_code.replace(old_ui_metrics, new_ui_metrics)

old_ui_stock_df = '''            df_st["占比"] = df_st["weight"].apply(lambda x: f"{x*100:.1f}%")
            df_st["市值"] = df_st["market_value"].apply(lambda x: f"¥{x:,.2f}")
            st.dataframe(df_st[["name", "市值", "占比"]].rename(columns={"name": "股票"}), hide_index=True, width="stretch")'''

new_ui_stock_df = '''            df_st["占比"] = df_st["weight"].apply(lambda x: f"{x*100:.1f}%")
            df_st["市值"] = df_st["market_value"].apply(lambda x: f"¥{x:,.2f}")
            df_st["盈亏"] = df_st["pnl"].apply(lambda x: f"¥{x:,.2f}")
            df_st["盈亏比例"] = df_st["pnl_pct"].apply(lambda x: f"{x*100:.2f}%")
            st.dataframe(df_st[["name", "市值", "占比", "盈亏比例"]].rename(columns={"name": "股票"}), hide_index=True, width="stretch")'''
ui_code = ui_code.replace(old_ui_stock_df, new_ui_stock_df)

with open("f:/zfywork/aiagents-stock/portfolio_ui.py", "w", encoding="utf-8") as f:
    f.write(ui_code)

print("Done")
