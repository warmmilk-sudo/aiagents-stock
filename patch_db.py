import sys

with open("f:/zfywork/aiagents-stock/portfolio_db.py", "r", encoding="utf-8") as f:
    code = f.read()

# Replace _init_database
old_init = '''            # 创建持仓股票表
            cursor.execute(\'\'\'
                CREATE TABLE IF NOT EXISTS portfolio_stocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    cost_price REAL,
                    quantity INTEGER,
                    note TEXT,
                    auto_monitor BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            \'\'\')'''

new_init = '''            cursor.execute("PRAGMA table_info(portfolio_stocks)")
            columns = {row[1] for row in cursor.fetchall()}
            
            if columns and 'account_name' not in columns:
                print("[INFO] 执行持仓表结构升级：支持多账户，更改唯一约束")
                cursor.execute('ALTER TABLE portfolio_stocks RENAME TO portfolio_stocks_old')
                cursor.execute(\'\'\'
                    CREATE TABLE IF NOT EXISTS portfolio_stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_name TEXT DEFAULT '默认账户',
                        code TEXT NOT NULL,
                        name TEXT NOT NULL,
                        cost_price REAL,
                        quantity INTEGER,
                        note TEXT,
                        auto_monitor BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(code, account_name)
                    )
                \'\'\')
                cursor.execute(\'\'\'
                    INSERT INTO portfolio_stocks (id, account_name, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at)
                    SELECT id, '默认账户', code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at
                    FROM portfolio_stocks_old
                \'\'\')
                cursor.execute('DROP TABLE portfolio_stocks_old')
            elif not columns:
                cursor.execute(\'\'\'
                    CREATE TABLE IF NOT EXISTS portfolio_stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        account_name TEXT DEFAULT '默认账户',
                        code TEXT NOT NULL,
                        name TEXT NOT NULL,
                        cost_price REAL,
                        quantity INTEGER,
                        note TEXT,
                        auto_monitor BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(code, account_name)
                    )
                \'\'\')'''

code = code.replace(old_init, new_init)

# Replace add_stock signature
old_add_sig = '''    def add_stock(self, code: str, name: str, cost_price: Optional[float] = None,
                  quantity: Optional[int] = None, note: str = "", 
                  auto_monitor: bool = True) -> int:'''

new_add_sig = '''    def add_stock(self, code: str, name: str, cost_price: Optional[float] = None,
                  quantity: Optional[int] = None, note: str = "", 
                  auto_monitor: bool = True, account_name: str = "默认账户") -> int:'''

code = code.replace(old_add_sig, new_add_sig)

old_add_doc = '''            auto_monitor: 是否自动同步到监测列表
            
        Returns:'''
new_add_doc = '''            auto_monitor: 是否自动同步到监测列表
            account_name: 账户名称
            
        Returns:'''
code = code.replace(old_add_doc, new_add_doc)

old_add_sql = '''            cursor.execute(\'\'\'
                INSERT INTO portfolio_stocks 
                (code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            \'\'\', (code, name, cost_price, quantity, note, auto_monitor, 
                  datetime.now(), datetime.now()))'''
                  
new_add_sql = '''            cursor.execute(\'\'\'
                INSERT INTO portfolio_stocks 
                (account_name, code, name, cost_price, quantity, note, auto_monitor, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            \'\'\', (account_name, code, name, cost_price, quantity, note, auto_monitor, 
                  datetime.now(), datetime.now()))'''
                  
code = code.replace(old_add_sql, new_add_sql)

old_add_err = '''        except sqlite3.IntegrityError as e:
            print(f"[ERROR] 股票代码已存在: {code}")
            raise ValueError(f"股票代码 {code} 已存在") from e'''
            
new_add_err = '''        except sqlite3.IntegrityError as e:
            print(f"[ERROR] 股票代码在账户 {account_name} 中已存在: {code}")
            raise ValueError(f"股票代码 {code} 在账户 {account_name} 中已存在") from e'''
            
code = code.replace(old_add_err, new_add_err)

# Update stock
old_update_f = "allowed_fields = ['code', 'name', 'cost_price', 'quantity', 'note', 'auto_monitor']"
new_update_f = "allowed_fields = ['account_name', 'code', 'name', 'cost_price', 'quantity', 'note', 'auto_monitor']"
code = code.replace(old_update_f, new_update_f)

# get_stock_by_code
old_get_sig = '''    def get_stock_by_code(self, code: str) -> Optional[Dict]:'''
new_get_sig = '''    def get_stock_by_code(self, code: str, account_name: str = "默认账户") -> Optional[Dict]:'''
code = code.replace(old_get_sig, new_get_sig)

old_get_sql = "cursor.execute('SELECT * FROM portfolio_stocks WHERE code = ?', (code,))"
new_get_sql = "cursor.execute('SELECT * FROM portfolio_stocks WHERE code = ? AND account_name = ?', (code, account_name))"
code = code.replace(old_get_sql, new_get_sql)

# Add get_stocks_by_code
code += '''
    def get_stocks_by_code(self, code: str) -> List[Dict]:
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
            conn.close()
'''

with open("f:/zfywork/aiagents-stock/portfolio_db.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Done")
