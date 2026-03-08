import sys

code = ""
with open("f:/zfywork/aiagents-stock/portfolio_manager.py", "r", encoding="utf-8") as f:
    code = f.read()

# Replace add_stock signature
old_add_sig = '''    def add_stock(self, code: str, name: Optional[str], cost_price: Optional[float] = None,
                  quantity: Optional[int] = None, note: str = "", 
                  auto_monitor: bool = True) -> Tuple[bool, str, Optional[int]]:'''
new_add_sig = '''    def add_stock(self, code: str, name: Optional[str], cost_price: Optional[float] = None,
                  quantity: Optional[int] = None, note: str = "", 
                  auto_monitor: bool = True, account_name: str = "默认账户") -> Tuple[bool, str, Optional[int]]:'''
code = code.replace(old_add_sig, new_add_sig)

old_add_existing = '''            existing = self.db.get_stock_by_code(code)'''
new_add_existing = '''            existing = self.db.get_stock_by_code(code, account_name)'''
code = code.replace(old_add_existing, new_add_existing)

old_add_call = '''            stock_id = self.db.add_stock(code, name, cost_price, quantity, note, auto_monitor)'''
new_add_call = '''            stock_id = self.db.add_stock(code, name, cost_price, quantity, note, auto_monitor, account_name)'''
code = code.replace(old_add_call, new_add_call)

# In persist_analysis_results or wherever get_stock_by_code is called.
old_get_stock = '''            stock = self.db.get_stock_by_code(code)
            if not stock:'''
new_get_stock = '''            stocks = self.db.get_stocks_by_code(code)
            if not stocks:
                print(f"[WARN] 未找到持仓股票: {code}，跳过保存")
                continue
            stock = stocks[0]
            if not stock:'''
code = code.replace(old_get_stock, new_get_stock)

with open("f:/zfywork/aiagents-stock/portfolio_manager.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Done")
