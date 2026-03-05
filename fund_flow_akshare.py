"""
资金流向数据获取模块
Tushare优先，AkShare兜底获取个股资金流向
"""

import pandas as pd
import sys
import io
import warnings
from datetime import datetime, timedelta
import akshare as ak
from data_source_manager import data_source_manager

warnings.filterwarnings('ignore')

# 设置标准输出编码为UTF-8（仅在命令行环境，避免streamlit冲突）
def _setup_stdout_encoding():
    """仅在命令行环境设置标准输出编码"""
    if sys.platform == 'win32' and not hasattr(sys.stdout, '_original_stream'):
        try:
            # 检测是否在streamlit环境中
            import streamlit
            # 在streamlit中不修改stdout
            return
        except ImportError:
            # 不在streamlit环境，可以安全修改
            try:
                original_stdout = sys.stdout
                wrapped = io.TextIOWrapper(original_stdout.buffer, encoding='utf-8', errors='ignore')
                setattr(wrapped, '_original_stream', original_stdout)
                sys.stdout = wrapped
            except Exception:
                pass

_setup_stdout_encoding()


class FundFlowAkshareDataFetcher:
    """资金流向数据获取类（Tushare优先，AkShare兜底）"""
    
    def __init__(self):
        self.days = 30  # 获取最近30个交易日
        self.available = True
        self.prefer_tushare = data_source_manager.tushare_available
        source_text = "tushare-first" if self.prefer_tushare else "akshare-only/fallback"
        print(f"[OK] 资金流向数据获取器初始化成功（{source_text}）")
    
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
            "source": "unknown",
            "source_chain": [],
            "error_detail": {}
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
            
            if fund_flow_data and fund_flow_data.get("data"):
                data["fund_flow_data"] = fund_flow_data
                print(f"   [OK] 成功获取 {len(fund_flow_data.get('data', []))} 个交易日的资金流向数据")
                data["data_success"] = True
                data["source"] = fund_flow_data.get("source", "unknown")
                data["source_chain"] = fund_flow_data.get("source_chain", [])
                data["error_detail"] = fund_flow_data.get("error_detail", {})
                print("[完成] 资金流向数据获取完成")
            else:
                print("[警告] 未能获取到资金流向数据")
                data["source"] = fund_flow_data.get("source") if isinstance(fund_flow_data, dict) else "unknown"
                data["source_chain"] = fund_flow_data.get("source_chain", []) if isinstance(fund_flow_data, dict) else []
                data["error_detail"] = fund_flow_data.get("error_detail", {}) if isinstance(fund_flow_data, dict) else {}
                
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
        source_chain = []
        errors = {}

        def _to_records(df):
            df = df.tail(self.days)
            df = df.iloc[::-1].reset_index(drop=True)
            data_list = []
            for _, row in df.iterrows():
                item = {}
                for col in df.columns:
                    value = row.get(col)
                    if value is None or (isinstance(value, float) and pd.isna(value)):
                        continue
                    if isinstance(value, (int, float)):
                        item[col] = value
                    else:
                        item[col] = str(value)
                if item:
                    data_list.append(item)
            return data_list

        def _from_tushare():
            ts_code = data_source_manager._convert_to_ts_code(symbol)
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=self.days * 3)).strftime('%Y%m%d')
            df = data_source_manager.tushare_api.moneyflow(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            if df is None or df.empty:
                return None
            if 'trade_date' in df.columns:
                df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)

            # 映射为东方财富常用字段，便于复用既有展示逻辑
            df['日期'] = df.get('trade_date')
            df['收盘价'] = pd.to_numeric(df.get('close', 0), errors='coerce').fillna(0.0)
            df['涨跌幅'] = pd.to_numeric(df.get('pct_chg', 0), errors='coerce').fillna(0.0)

            buy_elg = pd.to_numeric(df.get('buy_elg_amount', 0), errors='coerce').fillna(0.0)
            sell_elg = pd.to_numeric(df.get('sell_elg_amount', 0), errors='coerce').fillna(0.0)
            buy_lg = pd.to_numeric(df.get('buy_lg_amount', 0), errors='coerce').fillna(0.0)
            sell_lg = pd.to_numeric(df.get('sell_lg_amount', 0), errors='coerce').fillna(0.0)
            buy_md = pd.to_numeric(df.get('buy_md_amount', 0), errors='coerce').fillna(0.0)
            sell_md = pd.to_numeric(df.get('sell_md_amount', 0), errors='coerce').fillna(0.0)
            buy_sm = pd.to_numeric(df.get('buy_sm_amount', 0), errors='coerce').fillna(0.0)
            sell_sm = pd.to_numeric(df.get('sell_sm_amount', 0), errors='coerce').fillna(0.0)

            main_net = pd.to_numeric(df.get('net_mf_amount', 0), errors='coerce').fillna(0.0)
            super_net = buy_elg - sell_elg
            large_net = buy_lg - sell_lg
            medium_net = buy_md - sell_md
            small_net = buy_sm - sell_sm

            total_amount = buy_elg + sell_elg + buy_lg + sell_lg + buy_md + sell_md + buy_sm + sell_sm
            total_amount = total_amount.replace(0, pd.NA)

            df['主力净流入-净额'] = main_net
            df['主力净流入-净占比'] = (main_net / total_amount * 100).fillna(0.0)
            df['超大单净流入-净额'] = super_net
            df['超大单净流入-净占比'] = (super_net / total_amount * 100).fillna(0.0)
            df['大单净流入-净额'] = large_net
            df['大单净流入-净占比'] = (large_net / total_amount * 100).fillna(0.0)
            df['中单净流入-净额'] = medium_net
            df['中单净流入-净占比'] = (medium_net / total_amount * 100).fillna(0.0)
            df['小单净流入-净额'] = small_net
            df['小单净流入-净占比'] = (small_net / total_amount * 100).fillna(0.0)

            keep_cols = [
                '日期', '收盘价', '涨跌幅',
                '主力净流入-净额', '主力净流入-净占比',
                '超大单净流入-净额', '超大单净流入-净占比',
                '大单净流入-净额', '大单净流入-净占比',
                '中单净流入-净额', '中单净流入-净占比',
                '小单净流入-净额', '小单净流入-净占比'
            ]
            return df[keep_cols]

        def _from_akshare():
            return ak.stock_individual_fund_flow(stock=symbol, market=market)

        ordered_sources = []
        if data_source_manager.tushare_available:
            ordered_sources.append(("tushare", _from_tushare))
        ordered_sources.append(("akshare", _from_akshare))

        for src_name, src_func in ordered_sources:
            source_chain.append(src_name)
            try:
                print(f"   [{src_name.capitalize()}] 正在获取资金流向数据...")
                df = src_func()
                if df is None or df.empty:
                    errors[src_name] = "empty result"
                    continue

                records = _to_records(df)
                if not records:
                    errors[src_name] = "empty records"
                    continue

                return {
                    "data": records,
                    "days": len(records),
                    "columns": df.columns.tolist(),
                    "market": market,
                    "query_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "source": src_name,
                    "source_chain": source_chain,
                    "error_detail": errors
                }
            except Exception as e:
                errors[src_name] = f"{type(e).__name__}: {e}"

        print(f"   获取资金流向数据失败: {errors}")
        return {
            "data": [],
            "days": 0,
            "columns": [],
            "market": market,
            "query_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": None,
            "source_chain": source_chain,
            "error_detail": errors
        }
    
    def format_fund_flow_for_ai(self, data):
        """
        将资金流向数据格式化为适合AI阅读的文本
        """
        if not data or not data.get("data_success"):
            return "未能获取资金流向数据"
        
        text_parts = []
        
        fund_flow_data = data.get("fund_flow_data")
        if fund_flow_data:
            source = data.get('source', fund_flow_data.get('source', 'unknown'))
            text_parts.append(f"""
【个股资金流向数据 - {source}】
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
    print("测试资金流向数据获取（Tushare优先 + AkShare兜底）...")
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

