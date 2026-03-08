import sys

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
new_add_call = '''            stock_id = self.db.add_stock(code, name, cost_price, quantity, note, auto_monitor, account_name=account_name)'''
code = code.replace(old_add_call, new_add_call)

# In batch_analyze_portfolio and other places, get_stock_by_code might need account matching?
# Actually what is the return value of Manager's get_stock_by_code? Manger doesn't have it... Oh wait.
# The tests might use `portfolio_manager.db.get_stock_by_code`.

# Check if there are other usages of `get_stock_by_code(code)` without account within manager.
# "stock = self.db.get_stock_by_code(code)"
old_get = '''stock = self.db.get_stock_by_code(code)'''
new_get = '''stocks = self.db.get_stocks_by_code(code)
            stock = stocks[0] if stocks else None'''
code = code.replace(old_get, new_get)

# Revert the replacement inside add_stock because it replaced with `stocks[0] if stocks else None, account_name)` which will compile error
# Let's fix that up by running the replace on exact strings before we do the general catch-all

with open("f:/zfywork/aiagents-stock/portfolio_manager.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Done")
