import sys

with open("f:/zfywork/aiagents-stock/portfolio_db.py", "r", encoding="utf-8") as f:
    content = f.read()

wrong_method = '''    def get_stocks_by_code(self, code: str) -> List[Dict]:
        """
        根据股票代码获取该股票在所有账户中的持仓信息
        
        Args:
            code: 股票代码
            
        Returns:
            匹配的股票信息字典列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT * FROM portfolio_stocks WHERE code = ?', (code,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()'''

# first, let's remove it if it's there
content = content.replace(wrong_method, "")
content = content.replace("    print(\"\\n[OK] 数据库测试完成\")\n\n\n\n", "    print(\"\\n[OK] 数据库测试完成\")\n")

# now we inject it in the correct place, right after get_stock_by_code
correct_method = '''    def get_stocks_by_code(self, code: str) -> List[Dict]:
        """
        根据股票代码获取该股票在所有账户中的持仓信息
        
        Args:
            code: 股票代码
            
        Returns:
            匹配的股票信息字典列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT * FROM portfolio_stocks WHERE code = ?', (code,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()'''

target_to_insert_after = '''    def get_stock_by_code(self, code: str, account_name: str = "默认账户") -> Optional[Dict]:
        """
        根据股票代码获取持仓股票信息
        
        Args:
            code: 股票代码
            
        Returns:
            股票信息字典，不存在则返回None
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT * FROM portfolio_stocks WHERE code = ? AND account_name = ?', (code, account_name))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        finally:
            conn.close()'''

if target_to_insert_after in content:
    content = content.replace(target_to_insert_after, target_to_insert_after + "\n\n" + correct_method)
else:
    print("WARNING: target_to_insert_after NOT FOUND")

with open("f:/zfywork/aiagents-stock/portfolio_db.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Patch applied for get_stocks_by_code")
