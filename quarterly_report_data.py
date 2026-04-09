"""
季报数据获取模块
优先使用 Tushare 获取个股最近 8 期季度财务报告。
"""

import pandas as pd
import sys
import io
import warnings
import time
from datetime import datetime
from data_source_manager import data_source_manager
from stock_data_cache import stock_data_cache_service

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


class QuarterlyReportDataFetcher:
    """季报数据获取类（Tushare 主源）"""
    
    def __init__(self, cache_service=None):
        self.periods = 8  # 获取最近8期季报
        self.available = True
        self.cache_service = cache_service or stock_data_cache_service
        self.tushare_retry_count = 2
        self.tushare_retry_delay_seconds = 0.6
        print("✓ 季报数据获取器初始化成功（Tushare 主源）")

    def _call_tushare_dataframe(self, api_name, **kwargs):
        helper = getattr(data_source_manager, "call_tushare_api", None)
        if callable(helper):
            return helper(api_name, **kwargs)

        method = getattr(getattr(data_source_manager, "tushare_api", None), api_name, None)
        if method is None:
            return None

        result = None
        for attempt in range(1, self.tushare_retry_count + 1):
            try:
                result = method(**kwargs)
            except Exception as exc:
                if attempt < self.tushare_retry_count:
                    time.sleep(self.tushare_retry_delay_seconds)
                    continue
                raise exc

            if result is not None and not result.empty:
                return result
            if attempt < self.tushare_retry_count:
                time.sleep(self.tushare_retry_delay_seconds)
        return result
    
    def get_quarterly_reports(
        self,
        symbol,
        max_age_seconds=604800,
        allow_stale_on_failure=True,
        cache_first=True,
    ):
        return self.cache_service.get_stock_quarterly(
            symbol=symbol,
            fetch_fn=lambda: self._fetch_quarterly_reports_live(symbol),
            max_age_seconds=max_age_seconds,
            allow_stale_on_failure=allow_stale_on_failure,
            cache_first=cache_first,
        )

    def _fetch_quarterly_reports_live(self, symbol):
        """
        获取股票的季报数据
        
        Args:
            symbol: 股票代码（6位数字）
            
        Returns:
            dict: 包含季报数据的字典
        """
        data = {
            "symbol": symbol,
            "income_statement": None,      # 利润表
            "balance_sheet": None,         # 资产负债表
            "cash_flow": None,             # 现金流量表
            "financial_indicators": None,   # 财务指标
            "data_success": False,
            "source": "tushare"
        }
        
        # 只支持中国股票
        if not self._is_chinese_stock(symbol):
            data["error"] = "季报数据仅支持中国A股股票"
            return data
        
        try:
            print(f"📊 正在获取 {symbol} 的季报数据...")
            
            # 获取利润表
            income_data = self._get_income_statement(symbol)
            if income_data:
                data["income_statement"] = income_data
                data["source"] = income_data.get("source", data["source"])
                print(f"   ✓ 成功获取 {len(income_data.get('data', []))} 期利润表数据")
            
            # 获取资产负债表
            balance_data = self._get_balance_sheet(symbol)
            if balance_data:
                data["balance_sheet"] = balance_data
                data["source"] = balance_data.get("source", data["source"])
                print(f"   ✓ 成功获取 {len(balance_data.get('data', []))} 期资产负债表数据")
            
            # 获取现金流量表
            cash_flow_data = self._get_cash_flow(symbol)
            if cash_flow_data:
                data["cash_flow"] = cash_flow_data
                data["source"] = cash_flow_data.get("source", data["source"])
                print(f"   ✓ 成功获取 {len(cash_flow_data.get('data', []))} 期现金流量表数据")
            
            # 获取财务指标
            indicators_data = self._get_financial_indicators(symbol)
            if indicators_data:
                data["financial_indicators"] = indicators_data
                data["source"] = indicators_data.get("source", data["source"])
                print(f"   ✓ 成功获取 {len(indicators_data.get('data', []))} 期财务指标数据")
            
            # 如果至少有一个成功，则标记为成功
            if income_data or balance_data or cash_flow_data or indicators_data:
                data["data_success"] = True
                print("✅ 季报数据获取完成")
            else:
                print("⚠️ 未能获取到季报数据")
                
        except Exception as e:
            print(f"❌ 获取季报数据失败: {e}")
            data["error"] = str(e)
        
        return data
    
    def _is_chinese_stock(self, symbol):
        """判断是否为中国股票"""
        return symbol.isdigit() and len(symbol) == 6

    @staticmethod
    def _is_valid_value(value):
        return value is not None and not (isinstance(value, float) and pd.isna(value))

    def _normalize_tushare_financial_df(self, df):
        if df is None or df.empty:
            return None

        sort_columns = [col for col in ["end_date", "ann_date", "f_ann_date"] if col in df.columns]
        if sort_columns:
            df = df.sort_values(sort_columns, ascending=False)
        if "end_date" in df.columns:
            df = df.drop_duplicates(subset=["end_date"], keep="first")
        return df.head(self.periods).reset_index(drop=True)

    def _build_tushare_records(self, df, field_map):
        df = self._normalize_tushare_financial_df(df)
        if df is None or df.empty:
            return None

        data_list = []
        for _, row in df.iterrows():
            item = {}
            report_date = row.get("end_date")
            if self._is_valid_value(report_date):
                item["报告期"] = str(report_date)
            for source_field, target_field in field_map.items():
                value = row.get(source_field)
                if self._is_valid_value(value):
                    item[target_field] = str(value)
            if item:
                data_list.append(item)

        if not data_list:
            return None

        return {
            "data": data_list,
            "periods": len(data_list),
            "columns": list(data_list[0].keys()),
            "query_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "tushare",
        }
    
    def _get_income_statement(self, symbol):
        """获取利润表数据"""
        try:
            if data_source_manager.tushare_available:
                df = self._call_tushare_dataframe("income", ts_code=data_source_manager._convert_to_ts_code(symbol))
                tushare_data = self._build_tushare_records(
                    df,
                    {
                        "total_revenue": "营业总收入",
                        "revenue": "营业收入",
                        "total_cogs": "营业总成本",
                        "operate_profit": "营业利润",
                        "total_profit": "利润总额",
                        "n_income": "净利润",
                        "n_income_attr_p": "归属于母公司所有者的净利润",
                        "basic_eps": "基本每股收益",
                        "diluted_eps": "稀释每股收益",
                        "sell_exp": "销售费用",
                        "admin_exp": "管理费用",
                        "fin_exp": "财务费用",
                        "rd_exp": "研发费用",
                    },
                )
                if tushare_data:
                    return tushare_data

            print("   Tushare未返回利润表数据")
            return None
            
        except Exception as e:
            print(f"   获取利润表异常: {e}")
            return None
    
    def _get_balance_sheet(self, symbol):
        """获取资产负债表数据"""
        try:
            if data_source_manager.tushare_available:
                df = self._call_tushare_dataframe("balancesheet", ts_code=data_source_manager._convert_to_ts_code(symbol))
                tushare_data = self._build_tushare_records(
                    df,
                    {
                        "total_assets": "资产总计",
                        "total_cur_assets": "流动资产合计",
                        "total_nca": "非流动资产合计",
                        "total_liab": "负债合计",
                        "total_cur_liab": "流动负债合计",
                        "total_ncl": "非流动负债合计",
                        "total_hldr_eqy_inc_min_int": "所有者权益合计",
                        "total_hldr_eqy_exc_min_int": "归属于母公司股东权益合计",
                    },
                )
                if tushare_data:
                    return tushare_data

            print("   Tushare未返回资产负债表数据")
            return None
            
        except Exception as e:
            print(f"   获取资产负债表异常: {e}")
            return None
    
    def _get_cash_flow(self, symbol):
        """获取现金流量表数据"""
        try:
            if data_source_manager.tushare_available:
                df = self._call_tushare_dataframe("cashflow", ts_code=data_source_manager._convert_to_ts_code(symbol))
                tushare_data = self._build_tushare_records(
                    df,
                    {
                        "n_cashflow_act": "经营活动产生的现金流量净额",
                        "n_cashflow_inv_act": "投资活动产生的现金流量净额",
                        "n_cash_flows_fnc_act": "筹资活动产生的现金流量净额",
                        "n_incr_cash_cash_equ": "现金及现金等价物净增加额",
                        "c_cash_equ_end_period": "期末现金及现金等价物余额",
                    },
                )
                if tushare_data:
                    return tushare_data

            print("   Tushare未返回现金流量表数据")
            return None
            
        except Exception as e:
            print(f"   获取现金流量表异常: {e}")
            return None
    
    def _get_financial_indicators(self, symbol):
        """获取财务指标数据"""
        try:
            if data_source_manager.tushare_available:
                df = self._call_tushare_dataframe("fina_indicator", ts_code=data_source_manager._convert_to_ts_code(symbol))
                tushare_data = self._build_tushare_records(
                    df,
                    {
                        "roe": "净资产收益率",
                        "roa": "总资产净利率",
                        "netprofit_margin": "销售净利率",
                        "grossprofit_margin": "销售毛利率",
                        "debt_to_assets": "资产负债率",
                        "current_ratio": "流动比率",
                        "quick_ratio": "速动比率",
                        "ar_turn": "应收账款周转率",
                        "assets_turn": "总资产周转率",
                        "eps": "每股收益",
                        "bps": "每股净资产",
                        "cfps": "每股经营现金流",
                    },
                )
                if tushare_data:
                    return tushare_data

            print("   Tushare未返回财务指标数据")
            return None
            
        except Exception as e:
            print(f"   获取财务指标异常: {e}")
            return None
    
    def format_quarterly_reports_for_ai(self, data):
        """
        将季报数据格式化为适合AI阅读的文本
        """
        if not data or not data.get("data_success"):
            return "未能获取季报数据"
        
        text_parts = []
        text_parts.append(f"""
【季度财务报告数据 - {data.get('source', 'unknown')}数据源】
股票代码：{data.get('symbol', 'N/A')}
数据期数：最近{self.periods}期季报

""")
        
        # 利润表数据
        if data.get("income_statement"):
            income_data = data["income_statement"]
            text_parts.append(f"""
═══════════════════════════════════════
📊 利润表（最近{income_data.get('periods', 0)}期）
═══════════════════════════════════════
""")
            
            # 提取关键指标
            key_fields = ['报告期', '营业总收入', '营业收入', '营业总成本', '营业利润', 
                         '利润总额', '净利润', '归属于母公司所有者的净利润', 
                         '基本每股收益', '稀释每股收益']
            
            for idx, item in enumerate(income_data.get('data', []), 1):
                text_parts.append(f"\n第 {idx} 期:")
                for field in key_fields:
                    if field in item:
                        text_parts.append(f"  {field}: {item[field]}")
                
                # 显示其他重要字段（如果有）
                other_fields = ['销售费用', '管理费用', '财务费用', '研发费用']
                for field in other_fields:
                    if field in item:
                        text_parts.append(f"  {field}: {item[field]}")
        
        # 资产负债表数据
        if data.get("balance_sheet"):
            balance_data = data["balance_sheet"]
            text_parts.append(f"""

═══════════════════════════════════════
📊 资产负债表（最近{balance_data.get('periods', 0)}期）
═══════════════════════════════════════
""")
            
            # 提取关键指标
            key_fields = ['报告期', '资产总计', '流动资产合计', '非流动资产合计',
                         '负债合计', '流动负债合计', '非流动负债合计',
                         '所有者权益合计', '归属于母公司股东权益合计']
            
            for idx, item in enumerate(balance_data.get('data', []), 1):
                text_parts.append(f"\n第 {idx} 期:")
                for field in key_fields:
                    if field in item:
                        text_parts.append(f"  {field}: {item[field]}")
        
        # 现金流量表数据
        if data.get("cash_flow"):
            cash_flow_data = data["cash_flow"]
            text_parts.append(f"""

═══════════════════════════════════════
📊 现金流量表（最近{cash_flow_data.get('periods', 0)}期）
═══════════════════════════════════════
""")
            
            # 提取关键指标
            key_fields = ['报告期', '经营活动产生的现金流量净额', 
                         '投资活动产生的现金流量净额', '筹资活动产生的现金流量净额',
                         '现金及现金等价物净增加额', '期末现金及现金等价物余额']
            
            for idx, item in enumerate(cash_flow_data.get('data', []), 1):
                text_parts.append(f"\n第 {idx} 期:")
                for field in key_fields:
                    if field in item:
                        text_parts.append(f"  {field}: {item[field]}")
        
        # 财务指标数据
        if data.get("financial_indicators"):
            indicators_data = data["financial_indicators"]
            text_parts.append(f"""

═══════════════════════════════════════
📊 关键财务指标（最近{indicators_data.get('periods', 0)}期）
═══════════════════════════════════════
""")
            
            # 提取关键指标
            key_fields = ['报告期', '净资产收益率', '总资产净利率', '销售净利率',
                         '销售毛利率', '资产负债率', '流动比率', '速动比率',
                         '应收账款周转率', '存货周转率', '总资产周转率',
                         '每股收益', '每股净资产', '每股经营现金流']
            
            for idx, item in enumerate(indicators_data.get('data', []), 1):
                text_parts.append(f"\n第 {idx} 期:")
                for field in key_fields:
                    if field in item:
                        text_parts.append(f"  {field}: {item[field]}")
        
        return "\n".join(text_parts)


# 测试函数
if __name__ == "__main__":
    print("测试季报数据获取（Tushare 主源）...")
    print("="*60)
    
    fetcher = QuarterlyReportDataFetcher()
    
    if not fetcher.available:
        print("❌ 季报数据获取器不可用")
        sys.exit(1)
    
    # 测试股票
    test_symbols = ["000001", "600519"]  # 平安银行、贵州茅台
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"正在测试股票: {symbol}")
        print(f"{'='*60}\n")
        
        data = fetcher.get_quarterly_reports(symbol)
        
        if data.get("data_success"):
            print("\n" + "="*60)
            print("季报数据获取成功！")
            print("="*60)
            
            formatted_text = fetcher.format_quarterly_reports_for_ai(data)
            print(formatted_text)
        else:
            print(f"\n获取失败: {data.get('error', '未知错误')}")
        
        print("\n")
