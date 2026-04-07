"""
资金流向数据获取模块
优先使用 Tushare 获取个股资金流向，AkShare 仅作为兜底补充。
"""

import pandas as pd
import sys
import io
import warnings
from datetime import datetime, timedelta
import akshare as ak
from data_source_manager import data_source_manager

warnings.filterwarnings('ignore')

# 设置标准输出编码为UTF-8
def _setup_stdout_encoding():
    """在Windows命令行环境设置标准输出编码。"""
    if sys.platform == 'win32' and not hasattr(sys.stdout, '_original_stream'):
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')
        except Exception:
            pass

_setup_stdout_encoding()


class FundFlowAkshareDataFetcher:
    """资金流向数据获取类（多源策略：Tushare 主源，AkShare 兜底）"""
    
    def __init__(self):
        self.days = 30  # 获取最近30个交易日
        self.available = True
        print("[OK] 资金流向数据获取器初始化成功（多源策略：Tushare 主源）")

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            if value is None or pd.isna(value):
                return default
            return float(value)
        except Exception:
            return default

    def _build_tushare_fund_flow_frame(self, symbol, market):
        if not data_source_manager.tushare_available:
            return None

        print(f"   [Tushare] 正在获取资金流向数据...")
        ts_code = data_source_manager._convert_to_ts_code(symbol)
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.days * 2)).strftime('%Y%m%d')

        flow_df = data_source_manager.tushare_api.moneyflow(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        if flow_df is None or flow_df.empty:
            print(f"   [Tushare] ❌ 未找到资金流向数据")
            return None

        daily_df = data_source_manager.tushare_api.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        if daily_df is None or daily_df.empty:
            print(f"   [Tushare] ⚠ 未找到行情数据，将仅使用资金流向原始字段")
            daily_df = pd.DataFrame(columns=["trade_date", "close", "pct_chg"])

        merged = flow_df.merge(
            daily_df.loc[:, [col for col in ["trade_date", "close", "pct_chg"] if col in daily_df.columns]],
            on="trade_date",
            how="left",
        )

        records = []
        for _, row in merged.iterrows():
            buy_sm = self._safe_float(row.get("buy_sm_amount"))
            sell_sm = self._safe_float(row.get("sell_sm_amount"))
            buy_md = self._safe_float(row.get("buy_md_amount"))
            sell_md = self._safe_float(row.get("sell_md_amount"))
            buy_lg = self._safe_float(row.get("buy_lg_amount"))
            sell_lg = self._safe_float(row.get("sell_lg_amount"))
            buy_elg = self._safe_float(row.get("buy_elg_amount"))
            sell_elg = self._safe_float(row.get("sell_elg_amount"))

            total_amount = (
                buy_sm + sell_sm + buy_md + sell_md +
                buy_lg + sell_lg + buy_elg + sell_elg
            )

            def _ratio(net_amount):
                if total_amount <= 0:
                    return 0.0
                return net_amount / total_amount * 100

            small_net = buy_sm - sell_sm
            medium_net = buy_md - sell_md
            large_net = buy_lg - sell_lg
            extra_large_net = buy_elg - sell_elg
            main_net = large_net + extra_large_net

            records.append(
                {
                    "日期": str(row.get("trade_date", "N/A")),
                    "收盘价": self._safe_float(row.get("close"), default=None),
                    "涨跌幅": self._safe_float(row.get("pct_chg"), default=None),
                    "主力净流入-净额": main_net,
                    "主力净流入-净占比": round(_ratio(main_net), 2),
                    "超大单净流入-净额": extra_large_net,
                    "超大单净流入-净占比": round(_ratio(extra_large_net), 2),
                    "大单净流入-净额": large_net,
                    "大单净流入-净占比": round(_ratio(large_net), 2),
                    "中单净流入-净额": medium_net,
                    "中单净流入-净占比": round(_ratio(medium_net), 2),
                    "小单净流入-净额": small_net,
                    "小单净流入-净占比": round(_ratio(small_net), 2),
                }
            )

        df = pd.DataFrame(records)
        df = df.dropna(subset=["日期"]).sort_values("日期", ascending=False).reset_index(drop=True)
        df = df.head(self.days)
        print(f"   [Tushare] ✅ 成功获取 {len(df)} 条资金流向数据")
        return df
    
    def get_fund_flow_data(self, symbol):
        """
        获取个股资金流向数据
        
        Args:
            symbol: 股票代码（6位数字）
            
        Returns:
            dict: 包含资金流向数据的字典
        """
        data = {
            "symbol": symbol,
            "fund_flow_data": None,
            "data_success": False,
            "source": "akshare"
        }
        
        # 只支持中国股票
        if not self._is_chinese_stock(symbol):
            data["error"] = "资金流向数据仅支持中国A股股票"
            return data
        
        try:
            print(f"[资金流向] 正在获取 {symbol} 的资金流向数据...")
            
            # 确定市场
            market = self._get_market(symbol)
            
            # 获取资金流向数据
            fund_flow_data = self._get_individual_fund_flow(symbol, market)
            
            if fund_flow_data:
                data["fund_flow_data"] = fund_flow_data
                data["source"] = fund_flow_data.get("source", data.get("source"))
                print(f"   [OK] 成功获取 {len(fund_flow_data.get('data', []))} 个交易日的资金流向数据")
                data["data_success"] = True
                print("[完成] 资金流向数据获取完成")
            else:
                print("[警告] 未能获取到资金流向数据")
                
        except Exception as e:
            print(f"[ERROR] 获取资金流向数据失败: {e}")
            data["error"] = str(e)
        
        return data
    
    def _is_chinese_stock(self, symbol):
        """判断是否为中国股票"""
        return symbol.isdigit() and len(symbol) == 6
    
    def _get_market(self, symbol):
        """
        根据股票代码判断市场
        上海证券交易所: sh (60开头, 688开头)
        深圳证券交易所: sz (00开头, 30开头)
        北京证券交易所: bj (8开头, 4开头)
        """
        if symbol.startswith('60') or symbol.startswith('688'):
            return 'sh'
        elif symbol.startswith('00') or symbol.startswith('30'):
            return 'sz'
        elif symbol.startswith('8') or symbol.startswith('4'):
            return 'bj'
        else:
            # 默认深圳
            return 'sz'
    
    def _get_individual_fund_flow(self, symbol, market):
        """获取个股资金流向数据（支持akshare和tushare自动切换）"""
        try:
            df = None
            if data_source_manager.tushare_available:
                try:
                    df = self._build_tushare_fund_flow_frame(symbol, market)
                except Exception as te:
                    print(f"   [Tushare] ❌ 获取失败: {te}")

            if df is None or df.empty:
                print(f"   [Akshare] Tushare失败，尝试获取资金流向 (市场: {market})...")
                df = ak.stock_individual_fund_flow(stock=symbol, market=market)
                if df is None or df.empty:
                    print(f"   [Akshare] ❌ 未找到资金流向数据")
                    return None
            
            if "日期" in df.columns:
                df = df.sort_values("日期", ascending=False).reset_index(drop=True).head(self.days)
            else:
                # akshare 返回的数据通常按时间正序排列（从旧到新）
                df = df.tail(self.days)
                df = df.iloc[::-1].reset_index(drop=True)
            
            # 转换为字典列表
            data_list = []
            for idx, row in df.iterrows():
                item = {}
                for col in df.columns:
                    value = row.get(col)
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        continue
                    try:
                        # 保持数值类型
                        if isinstance(value, (int, float)):
                            item[col] = value
                        else:
                            item[col] = str(value)
                    except:
                        item[col] = "N/A"
                if item:
                    data_list.append(item)
            
            return {
                "data": data_list,
                "days": len(data_list),
                "columns": df.columns.tolist(),
                "market": market,
                "source": "tushare" if "主力净流入-净额" in df.columns and "小单买入" not in df.columns else "akshare",
                "query_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            print(f"   获取资金流向数据异常: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def format_fund_flow_for_ai(self, data):
        """
        将资金流向数据格式化为适合AI阅读的文本
        """
        if not data or not data.get("data_success"):
            return "未能获取资金流向数据"
        
        text_parts = []
        
        fund_flow_data = data.get("fund_flow_data")
        if fund_flow_data:
            text_parts.append(f"""
【个股资金流向数据】
数据源：{data.get('source', fund_flow_data.get('source', 'N/A'))}
股票代码：{data.get('symbol', 'N/A')}
市场：{fund_flow_data.get('market', 'N/A').upper()}
交易日数：最近{fund_flow_data.get('days', 0)}个交易日
查询时间：{fund_flow_data.get('query_time', 'N/A')}

═══════════════════════════════════════
[资金流向详细数据]
═══════════════════════════════════════
""")
            
            # 显示每个交易日的数据
            for idx, item in enumerate(fund_flow_data.get('data', []), 1):
                date = item.get('日期', 'N/A')
                close_price = item.get('收盘价', 'N/A')
                change_pct = item.get('涨跌幅', 'N/A')
                
                text_parts.append(f"""
第 {idx} 个交易日 ({date}):
  基本信息:
    - 收盘价: {close_price}
    - 涨跌幅: {change_pct}%
  
  主力资金:
    - 主力净流入-净额: {item.get('主力净流入-净额', 'N/A')}
    - 主力净流入-净占比: {item.get('主力净流入-净占比', 'N/A')}%
  
  超大单:
    - 超大单净流入-净额: {item.get('超大单净流入-净额', 'N/A')}
    - 超大单净流入-净占比: {item.get('超大单净流入-净占比', 'N/A')}%
  
  大单:
    - 大单净流入-净额: {item.get('大单净流入-净额', 'N/A')}
    - 大单净流入-净占比: {item.get('大单净流入-净占比', 'N/A')}%
  
  中单:
    - 中单净流入-净额: {item.get('中单净流入-净额', 'N/A')}
    - 中单净流入-净占比: {item.get('中单净流入-净占比', 'N/A')}%
  
  小单:
    - 小单净流入-净额: {item.get('小单净流入-净额', 'N/A')}
    - 小单净流入-净占比: {item.get('小单净流入-净占比', 'N/A')}%
""")
            
            # 添加统计汇总
            text_parts.append("""
═══════════════════════════════════════
[统计汇总 - 最近30个交易日]
═══════════════════════════════════════
""")
            
            # 计算统计数据
            data_list = fund_flow_data.get('data', [])
            if data_list:
                # 主力净流入统计
                main_inflow_list = [item.get('主力净流入-净额', 0) for item in data_list if isinstance(item.get('主力净流入-净额'), (int, float))]
                if main_inflow_list:
                    total_main_inflow = sum(main_inflow_list)
                    avg_main_inflow = total_main_inflow / len(main_inflow_list)
                    positive_days = len([x for x in main_inflow_list if x > 0])
                    negative_days = len([x for x in main_inflow_list if x < 0])
                    
                    text_parts.append(f"""
主力资金统计:
  - 累计净流入: {total_main_inflow:.2f}
  - 平均每日净流入: {avg_main_inflow:.2f}
  - 净流入天数: {positive_days}天
  - 净流出天数: {negative_days}天
  - 净流入占比: {positive_days/len(main_inflow_list)*100:.1f}%
""")
                
                # 涨跌幅统计
                change_pct_list = [item.get('涨跌幅', 0) for item in data_list if isinstance(item.get('涨跌幅'), (int, float))]
                if change_pct_list:
                    avg_change = sum(change_pct_list) / len(change_pct_list)
                    up_days = len([x for x in change_pct_list if x > 0])
                    down_days = len([x for x in change_pct_list if x < 0])
                    
                    text_parts.append(f"""
股价统计:
  - 平均涨跌幅: {avg_change:.2f}%
  - 上涨天数: {up_days}天
  - 下跌天数: {down_days}天
  - 上涨占比: {up_days/len(change_pct_list)*100:.1f}%
""")
        
        return "\n".join(text_parts)


# 测试函数
if __name__ == "__main__":
    print("测试资金流向数据获取（多源策略）...")
    print("="*60)
    
    fetcher = FundFlowAkshareDataFetcher()
    
    if not fetcher.available:
        print("[ERROR] 资金流向数据获取器不可用")
        sys.exit(1)
    
    # 测试股票
    test_symbols = [
        ("000001", "平安银行"),
        ("600519", "贵州茅台"),
        ("000858", "五粮液")
    ]
    
    for symbol, name in test_symbols:
        print(f"\n{'='*60}")
        print(f"正在测试股票: {name} ({symbol})")
        print(f"{'='*60}\n")
        
        data = fetcher.get_fund_flow_data(symbol)
        
        if data.get("data_success"):
            print("\n" + "="*60)
            print("资金流向数据获取成功！")
            print("="*60)
            
            formatted_text = fetcher.format_fund_flow_for_ai(data)
            # 只显示前2000个字符
            preview = formatted_text[:2000] if len(formatted_text) > 2000 else formatted_text
            print(preview)
            if len(formatted_text) > 2000:
                print(f"... (共 {len(formatted_text)} 字符)")
        else:
            print(f"\n获取失败: {data.get('error', '未知错误')}")
        
        print("\n")
