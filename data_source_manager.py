"""
数据源管理器
优先使用 Tushare/TDX。
"""

import logging
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import config
from tushare_utils import create_tushare_pro

# 加载环境变量
load_dotenv()


class DataSourceManager:
    """数据源管理器 - 优先 Tushare/TDX"""

    _MISSING_TEXT_VALUES = {"", "-", "--", "N/A", "NA", "未知", "null", "None", "nan"}
    _INDUSTRY_LABELS = {
        "所处行业",
        "所属行业",
        "所属同花顺行业",
        "所属申万行业",
        "申万行业",
        "证监会行业",
        "所属证监会行业",
        "行业分类",
        "行业",
    }
    _INDUSTRY_LABEL_EXCLUDES = ("市盈率", "市净率", "涨跌", "换手", "资金", "排名", "概念", "指数")

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.tushare_token = os.getenv('TUSHARE_TOKEN', '')
        self.tushare_url = os.getenv('TUSHARE_URL', 'https://api.tushare.pro')
        self.tushare_available = False
        self.tushare_api = None
        self.tushare_retry_count = max(1, int(os.getenv('TUSHARE_RETRY_COUNT', '2') or 2))
        self.tushare_retry_delay_seconds = max(0.2, float(os.getenv('TUSHARE_RETRY_DELAY_SECONDS', '0.6') or 0.6))
        self.tdx_fetcher = None
        self.tdx_enabled = bool(config.TDX_CONFIG.get('enabled', False) or config.TDX_CONFIG.get('base_url', ''))
        self.tdx_base_url = str(config.TDX_CONFIG.get('base_url', '') or '').strip()
        self.tdx_timeout_seconds = max(5, int(getattr(config, 'TDX_TIMEOUT_SECONDS', 10) or 10))
        
        # 初始化tushare
        if self.tushare_token:
            try:
                self.tushare_api, self.tushare_url = create_tushare_pro(
                    token=self.tushare_token,
                    base_url=self.tushare_url,
                )
                self.tushare_available = self.tushare_api is not None
                if self.tushare_available:
                    print(f"[Tushare] 数据源初始化成功，地址: {self.tushare_url}")
                else:
                    print("[Tushare] 数据源未初始化，未创建API客户端")
            except Exception as e:
                print(f"[Tushare] 数据源初始化失败: {e}")
                self.tushare_available = False
        else:
            print("[INFO] 未配置 Tushare Token")

    def _coerce_quote_number(self, value, default=None):
        if value is None:
            return default
        try:
            if pd.isna(value):
                return default
        except Exception:
            pass
        if isinstance(value, str):
            normalized = value.strip().replace(',', '')
            if normalized in self._MISSING_TEXT_VALUES:
                return default
            value = normalized
        try:
            numeric = float(value)
            return int(numeric) if numeric.is_integer() else numeric
        except Exception:
            return value if value not in (None, '') else default

    def _get_tdx_fetcher(self):
        if not self.tdx_enabled or not self.tdx_base_url:
            return None
        existing_fetcher = self.tdx_fetcher
        if existing_fetcher is not None and getattr(existing_fetcher, 'available', True):
            return existing_fetcher
        try:
            from smart_monitor_tdx_data import SmartMonitorTDXDataFetcher

            candidate_fetcher = SmartMonitorTDXDataFetcher(
                base_url=self.tdx_base_url,
                timeout_seconds=self.tdx_timeout_seconds,
            )
            if getattr(candidate_fetcher, 'available', True):
                self.tdx_fetcher = candidate_fetcher
                return candidate_fetcher
            self.logger.warning("TDX数据源可用性探测失败: %s", self.tdx_base_url)
        except Exception as exc:
            self.logger.warning("TDX数据源初始化失败: %s", exc)
        self.tdx_fetcher = None
        return None

    def _normalize_tdx_quote(self, symbol, quote):
        if not isinstance(quote, dict) or not quote:
            return {}

        price = self._coerce_quote_number(quote.get('price', quote.get('current_price')), default=None)
        try:
            if price is None or float(price) <= 0:
                return {}
        except Exception:
            return {}

        return {
            'symbol': symbol,
            'name': self._clean_text_value(quote.get('name')),
            'price': float(price),
            'current_price': float(price),
            'change_percent': self._coerce_quote_number(quote.get('change_percent', quote.get('change_pct'))),
            'change': self._coerce_quote_number(quote.get('change', quote.get('change_amount'))),
            'volume': self._coerce_quote_number(quote.get('volume')),
            'amount': self._coerce_quote_number(quote.get('amount')),
            'high': self._coerce_quote_number(quote.get('high')),
            'low': self._coerce_quote_number(quote.get('low')),
            'open': self._coerce_quote_number(quote.get('open')),
            'pre_close': self._coerce_quote_number(quote.get('pre_close')),
            'update_time': self._clean_text_value(quote.get('update_time')),
            'data_source': 'tdx',
        }

    def _clean_text_value(self, value):
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass

        text = str(value).strip()
        return None if text in self._MISSING_TEXT_VALUES else text

    def _get_row_value(self, row, *candidates):
        for field in candidates:
            if field in row.index:
                return row[field]
        return None

    def _extract_industry_from_stock_info(self, stock_info: pd.DataFrame) -> str:
        if stock_info is None or stock_info.empty:
            return ""

        fuzzy_match = ""
        for _, row in stock_info.iterrows():
            key = self._clean_text_value(
                self._get_row_value(row, 'item', '项目', '名称', '字段', 'title', 'key')
            ).replace(" ", "")
            value = self._clean_text_value(
                self._get_row_value(row, 'value', '值', '内容', 'data', 'val')
            )
            if not key or not value:
                continue

            if key in self._INDUSTRY_LABELS:
                return value

            if "行业" in key and not any(excluded in key for excluded in self._INDUSTRY_LABEL_EXCLUDES):
                fuzzy_match = fuzzy_match or value

        return fuzzy_match

    @staticmethod
    def _is_cn_fund_like_symbol(symbol: str) -> bool:
        if not symbol or len(symbol) != 6 or not symbol.isdigit():
            return False
        return (
            symbol.startswith('5')
            or symbol.startswith('11')
            or symbol.startswith('12')
            or symbol.startswith('15')
            or symbol.startswith('16')
            or symbol.startswith('18')
        )

    def call_tushare_api(self, api_name, *, empty_ok=False, **kwargs):
        """调用 Tushare 接口，并在异常或空返回时做轻量重试。"""
        if not self.tushare_available or self.tushare_api is None:
            return None

        method = getattr(self.tushare_api, api_name, None)
        if method is None:
            raise AttributeError(f"Tushare API 不支持方法: {api_name}")

        last_error = None
        for attempt in range(1, self.tushare_retry_count + 1):
            try:
                result = method(**kwargs)
            except Exception as exc:
                last_error = exc
                if attempt < self.tushare_retry_count:
                    print(
                        f"[Tushare] {api_name} 调用异常，第{attempt}/{self.tushare_retry_count}次重试前等待 "
                        f"{self.tushare_retry_delay_seconds:.1f}s: {exc}"
                    )
                    time.sleep(self.tushare_retry_delay_seconds)
                    continue
                print(f"[Tushare] {api_name} 调用失败: {exc}")
                return None

            is_empty = result is None or (isinstance(result, pd.DataFrame) and result.empty)
            if not is_empty or empty_ok:
                return result

            if attempt < self.tushare_retry_count:
                print(
                    f"[Tushare] {api_name} 返回空数据，第{attempt}/{self.tushare_retry_count}次重试前等待 "
                    f"{self.tushare_retry_delay_seconds:.1f}s"
                )
                time.sleep(self.tushare_retry_delay_seconds)

        return result
    
    def _fetch_stock_hist_data_from_tushare(self, symbol, start_date=None, end_date=None, adjust='qfq'):
        if not self.tushare_available:
            return None

        print(f"[Tushare] 正在获取 {symbol} 的历史数据...")
        ts_code = self._convert_to_ts_code(symbol)
        adj_dict = {'qfq': 'qfq', 'hfq': 'hfq', '': None}
        adj = adj_dict.get(adjust, 'qfq')

        df = None
        try:
            df = self.call_tushare_api(
                'daily',
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                adj=adj,
            )
        except TypeError:
            df = None
        except Exception as exc:
            print(f"[Tushare] daily(adj) 获取失败: {exc}")

        if df is None or df.empty:
            try:
                df = self.call_tushare_api(
                    'daily',
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    empty_ok=False,
                )
            except Exception as exc:
                print(f"[Tushare] daily 获取失败: {exc}")
                df = None

        if df is None or df.empty:
            if self._is_cn_fund_like_symbol(symbol):
                try:
                    df = self.call_tushare_api(
                        'fund_daily',
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                    )
                except Exception as exc:
                    print(f"[Tushare] fund_daily 获取失败: {exc}")
                    df = None

        if df is None or df.empty:
            print(f"[Tushare] 未返回 {symbol} 的历史数据")
            return None

        df = df.rename(columns={
            'trade_date': 'date',
            'vol': 'volume',
            'amount': 'amount'
        })
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        df['volume'] = df['volume'] * 100
        df['amount'] = df['amount'] * 1000

        print(f"[Tushare] 获取成功，共 {len(df)} 条数据")
        return df

    def get_stock_hist_data(self, symbol, start_date=None, end_date=None, adjust='qfq'):
        """
        获取股票历史数据（优先 Tushare）
        
        Args:
            symbol: 股票代码（6位数字）
            start_date: 开始日期（格式：'20240101'或'2024-01-01'）
            end_date: 结束日期
            adjust: 复权类型（'qfq'前复权, 'hfq'后复权, ''不复权）
            
        Returns:
            DataFrame: 包含日期、开盘、收盘、最高、最低、成交量等列
        """
        # 标准化日期格式
        if start_date:
            start_date = start_date.replace('-', '')
        if end_date:
            end_date = end_date.replace('-', '')
        else:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if self.tushare_available:
            try:
                df = self._fetch_stock_hist_data_from_tushare(symbol, start_date=start_date, end_date=end_date, adjust=adjust)
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                print(f"[Tushare] 获取失败: {e}")

        print("[ERROR] Tushare 未返回历史数据")
        return None
    
    def get_stock_basic_info(self, symbol):
        """
        获取股票基本信息（优先 Tushare）
        
        Args:
            symbol: 股票代码
            
        Returns:
            dict: 股票基本信息
        """
        info = {
            "symbol": symbol,
            "name": None,
            "industry": None,
            "market": None
        }
        
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的基本信息...")
                
                ts_code = self._convert_to_ts_code(symbol)
                df = self.call_tushare_api(
                    'stock_basic',
                    ts_code=ts_code,
                    fields='ts_code,name,area,industry,market,list_date',
                )

                if (df is None or df.empty) and self._is_cn_fund_like_symbol(symbol):
                    df = self.call_tushare_api(
                        'fund_basic',
                        ts_code=ts_code,
                        market='E',
                        fields='ts_code,name,management,custodian,found_date,due_date,list_date,issue_date,market',
                    )
                
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    info['name'] = self._clean_text_value(row.get('name')) or info['name']
                    info['industry'] = (
                        self._clean_text_value(row.get('industry'))
                        or self._clean_text_value(row.get('management'))
                        or info['industry']
                    )
                    info['market'] = self._clean_text_value(row.get('market')) or info['market']
                    info['list_date'] = self._clean_text_value(row.get('list_date')) or info.get('list_date')
                    
                    print(f"[Tushare] 成功获取基本信息")
                    return info
            except Exception as e:
                print(f"[Tushare] 获取失败: {e}")

        return info
    
    def get_realtime_quotes(self, symbol):
        """
        获取实时行情数据（优先 TDX，不回退到日线数据）
        
        Args:
            symbol: 股票代码
            
        Returns:
            dict: 实时行情数据
        """
        quotes = {}

        tdx_fetcher = self._get_tdx_fetcher()
        if tdx_fetcher is not None:
            try:
                print(f"[TDX] 正在获取 {symbol} 的实时行情...")
                tdx_quote = self._normalize_tdx_quote(symbol, tdx_fetcher.get_realtime_quote(symbol))
                if tdx_quote:
                    print("[TDX] 成功获取实时行情")
                    return tdx_quote
            except Exception as e:
                print(f"[TDX] 获取失败: {e}")

        return quotes
    
    def get_financial_data(self, symbol, report_type='income'):
        """
        获取财务数据（优先 Tushare）
        
        Args:
            symbol: 股票代码
            report_type: 报表类型（'income'利润表, 'balance'资产负债表, 'cashflow'现金流量表）
            
        Returns:
            DataFrame: 财务数据
        """
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的财务数据...")
                
                ts_code = self._convert_to_ts_code(symbol)
                
                if report_type == 'income':
                    df = self.call_tushare_api('income', ts_code=ts_code)
                elif report_type == 'balance':
                    df = self.call_tushare_api('balancesheet', ts_code=ts_code)
                elif report_type == 'cashflow':
                    df = self.call_tushare_api('cashflow', ts_code=ts_code)
                else:
                    df = None
                
                if df is not None and not df.empty:
                    print(f"[Tushare] 成功获取财务数据")
                    return df
            except Exception as e:
                print(f"[Tushare] 获取失败: {e}")

        return None
    
    def _convert_to_ts_code(self, symbol):
        """
        将6位股票代码转换为tushare格式（带市场后缀）
        
        Args:
            symbol: 6位股票代码
            
        Returns:
            str: tushare格式代码（如：000001.SZ）
        """
        if not symbol or len(symbol) != 6:
            return symbol
        
        # 根据代码判断市场
        if symbol.startswith('6') or symbol.startswith('5') or symbol.startswith('11'):
            # 上海主板
            return f"{symbol}.SH"
        elif (
            symbol.startswith('0')
            or symbol.startswith('3')
            or symbol.startswith('12')
            or symbol.startswith('15')
            or symbol.startswith('16')
            or symbol.startswith('18')
        ):
            # 深圳主板和创业板
            return f"{symbol}.SZ"
        elif symbol.startswith('8') or symbol.startswith('4'):
            # 北交所
            return f"{symbol}.BJ"
        else:
            # 默认深圳
            return f"{symbol}.SZ"
    
    def _convert_from_ts_code(self, ts_code):
        """
        将tushare格式代码转换为6位代码
        
        Args:
            ts_code: tushare格式代码（如：000001.SZ）
            
        Returns:
            str: 6位股票代码
        """
        if '.' in ts_code:
            return ts_code.split('.')[0]
        return ts_code


# 全局数据源管理器实例
data_source_manager = DataSourceManager()
