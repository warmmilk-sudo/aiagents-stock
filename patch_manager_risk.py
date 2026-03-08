import sys

code = ""
with open("f:/zfywork/aiagents-stock/portfolio_manager.py", "r", encoding="utf-8") as f:
    code = f.read()

risk_method = '''    
    # ==================== 风险评估 ====================
    
    def calculate_portfolio_risk(self, account_name: Optional[str] = None) -> Dict:
        """
        计算持仓风险指标评估，包括单票集中度、行业集中度等。
        
        Args:
            account_name: 指定账户名称进行过滤，若为None则计算所有账户。
            
        Returns:
            Dict: 包含风险指标评估结果的字典
        """
        stocks = self.get_all_stocks()
        if account_name:
            stocks = [s for s in stocks if s.get("account_name", "默认账户") == account_name]
            
        if not stocks:
            return {"status": "error", "message": "没有持仓记录，无法评估风险"}
            
        total_market_value = 0.0
        stock_values = []
        industry_values = {}
        
        for stock in stocks:
            quantity = stock.get('quantity')
            try:
                quantity_value = int(quantity) if quantity not in (None, "") else 0
            except (TypeError, ValueError):
                quantity_value = 0
                
            latest_analysis = self.get_latest_analysis(stock['id'])
            
            # 获取当前价格
            current_price = None
            industry = "未知行业"
            
            if latest_analysis:
                current_price = latest_analysis.get('current_price')
                stock_info = latest_analysis.get('stock_info')
                if isinstance(stock_info, dict):
                    if current_price is None or current_price == 0:
                        try:
                            # 尝试获取字符串中的第一个浮点数
                            cp_str = str(stock_info.get("current_price", ""))
                            import re
                            match = re.search(r'\\d+(\\.\\d+)?', cp_str)
                            if match:
                                current_price = float(match.group())
                        except:
                            pass
                    industry = stock_info.get("industry", "未知行业")
            
            if current_price is None or current_price == 0:
                cost_price = stock.get("cost_price", 0)
                current_price = float(cost_price) if cost_price else 0.0
                
            market_value = current_price * quantity_value
            total_market_value += market_value
            
            stock_values.append({
                "code": stock['code'],
                "name": stock['name'],
                "market_value": market_value,
                "industry": industry
            })
            
            industry_values[industry] = industry_values.get(industry, 0) + market_value
            
        if total_market_value == 0:
            return {"status": "error", "message": "持仓总市值为空，请更新价格或数量"}
            
        # 计算单票权重
        for sv in stock_values:
            sv["weight"] = sv["market_value"] / total_market_value
            
        stock_values.sort(key=lambda x: x["weight"], reverse=True)
        
        # 计算行业权重
        industry_distribution = []
        for ind, val in industry_values.items():
            industry_distribution.append({
                "industry": ind,
                "market_value": val,
                "weight": val / total_market_value
            })
            
        industry_distribution.sort(key=lambda x: x["weight"], reverse=True)
        
        # 风险评估结果
        risk_warnings = []
        high_concentration = False
        
        if stock_values and stock_values[0]["weight"] > 0.3:
            risk_warnings.append(f"单票超载预警：{stock_values[0]['name']} 占比达到 {stock_values[0]['weight']*100:.1f}%，超过安全线(30%)。")
            high_concentration = True
            
        if industry_distribution and industry_distribution[0]["weight"] > 0.4:
            risk_warnings.append(f"行业集中度预警：{industry_distribution[0]['industry']} 占比达到 {industry_distribution[0]['weight']*100:.1f}%，超过安全线(40%)。")
            high_concentration = True
            
        if not risk_warnings:
            risk_warnings.append("仓位结构健康，未发现明显集中度风险。")
            
        return {
            "status": "success",
            "total_market_value": total_market_value,
            "stock_distribution": stock_values,
            "industry_distribution": industry_distribution,
            "high_concentration": high_concentration,
            "risk_warnings": risk_warnings
        }
'''

# insert risk_method right before the ending `portfolio_manager = PortfolioManager()`
target_marker = "# 创建全局实例\\nportfolio_manager = PortfolioManager()"
code = code.replace("# 创建全局实例\\nportfolio_manager = PortfolioManager()", risk_method + "\\n\\n# 创建全局实例\\nportfolio_manager = PortfolioManager()")

# Check if target_marker exists
if target_marker not in code and "# 创建全局实例" in code:
    code = code.replace("# 创建全局实例", risk_method + "\\n\\n# 创建全局实例")

with open("f:/zfywork/aiagents-stock/portfolio_manager.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Done")
