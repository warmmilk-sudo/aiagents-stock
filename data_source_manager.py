"""
数据源管理器
实现akshare和tushare的自动切换机制
"""

import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import config
from tushare_utils import create_tushare_pro

# 加载环境变量
load_dotenv()


class DataSourceManager:
    """数据源管理器 - 实现akshare与tushare自动切换"""

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
            print("[INFO] 未配置Tushare Token，将仅使用Akshare数据源")

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
    
    def _fetch_stock_hist_data_from_tushare(self, symbol, start_date=None, end_date=None, adjust='qfq'):
        if not self.tushare_available:
            return None

        print(f"[Tushare] 正在获取 {symbol} 的历史数据...")
        ts_code = self._convert_to_ts_code(symbol)
        adj_dict = {'qfq': 'qfq', 'hfq': 'hfq', '': None}
        adj = adj_dict.get(adjust, 'qfq')

        df = self.tushare_api.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            adj=adj
        )

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

    def _fetch_stock_hist_data_from_akshare(self, symbol, start_date=None, end_date=None, adjust='qfq'):
        import akshare as ak

        print(f"[Akshare] 正在获取 {symbol} 的历史数据...")
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )

        if df is None or df.empty:
            print(f"[Akshare] 未返回 {symbol} 的历史数据")
            return None

        df = df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'pct_change',
            '涨跌额': 'change',
            '换手率': 'turnover'
        })
        df['date'] = pd.to_datetime(df['date'])
        print(f"[Akshare] 获取成功，共 {len(df)} 条数据")
        return df

    def get_stock_hist_data(self, symbol, start_date=None, end_date=None, adjust='qfq'):
        """
        获取股票历史数据（优先akshare，失败时使用tushare）
        
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

        try:
            df = self._fetch_stock_hist_data_from_akshare(symbol, start_date=start_date, end_date=end_date, adjust=adjust)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            print(f"[Akshare] 获取失败: {e}")
        
        # 两个数据源都失败
        print("[ERROR] 所有数据源均获取失败")
        return None
    
    def get_stock_basic_info(self, symbol):
        """
        获取股票基本信息（优先akshare，失败时使用tushare）
        
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
                df = self.tushare_api.stock_basic(
                    ts_code=ts_code,
                    fields='ts_code,name,area,industry,market,list_date'
                )
                
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    info['name'] = self._clean_text_value(row.get('name')) or info['name']
                    info['industry'] = self._clean_text_value(row.get('industry')) or info['industry']
                    info['market'] = self._clean_text_value(row.get('market')) or info['market']
                    info['list_date'] = self._clean_text_value(row.get('list_date')) or info.get('list_date')
                    
                    print(f"[Tushare] 成功获取基本信息")
                    return info
            except Exception as e:
                print(f"[Tushare] 获取失败: {e}")

        try:
            import akshare as ak
            print(f"[Akshare] Tushare失败，尝试获取 {symbol} 的基本信息...")

            stock_info = ak.stock_individual_info_em(symbol=symbol)
            if stock_info is not None and not stock_info.empty:
                extracted_industry = self._extract_industry_from_stock_info(stock_info)
                if extracted_industry:
                    info['industry'] = extracted_industry

                for _, row in stock_info.iterrows():
                    key = self._clean_text_value(
                        self._get_row_value(row, 'item', '项目', '名称', '字段', 'title', 'key')
                    ).replace(" ", "")
                    value = self._clean_text_value(
                        self._get_row_value(row, 'value', '值', '内容', 'data', 'val')
                    )
                    if not key or not value:
                        continue

                    if key == '股票简称':
                        info['name'] = value
                    elif key in self._INDUSTRY_LABELS:
                        info['industry'] = value
                    elif key == '上市时间':
                        info['list_date'] = value
                    elif key == '总市值':
                        info['market_cap'] = value
                    elif key == '流通市值':
                        info['circulating_market_cap'] = value

                if info['name'] is not None or info['industry'] is not None:
                    print(f"[Akshare] 成功获取基本信息")
        except Exception as e:
            print(f"[Akshare] 获取失败: {e}")

        return info
    
    def get_realtime_quotes(self, symbol):
        """
        获取实时行情数据（优先TDX，失败时回退AkShare，不回退到Tushare日线）
        
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

        try:
            import akshare as ak
            print(f"[Akshare] TDX失败，尝试获取 {symbol} 的实时行情...")

            df = ak.stock_zh_a_spot_em()
            stock_df = df[df['代码'] == symbol]

            if not stock_df.empty:
                row = stock_df.iloc[0]
                quotes = {
                    'symbol': symbol,
                    'name': row['名称'],
                    'price': row['最新价'],
                    'change_percent': row['涨跌幅'],
                    'change': row['涨跌额'],
                    'volume': row['成交量'],
                    'amount': row['成交额'],
                    'high': row['最高'],
                    'low': row['最低'],
                    'open': row['今开'],
                    'pre_close': row['昨收'],
                    'data_source': 'akshare'
                }
                print(f"[Akshare] 成功获取实时行情")
                return quotes
        except Exception as e:
            print(f"[Akshare] 获取失败: {e}")

        return quotes
    
    def get_financial_data(self, symbol, report_type='income'):
        """
        获取财务数据（优先akshare，失败时使用tushare）
        
        Args:
            symbol: 股票代码
            report_type: 报表类型（'income'利润表, 'balance'资产负债表, 'cashflow'现金流量表）
            
        Returns:
            DataFrame: 财务数据
        """
        # 优先使用akshare
        try:
            import akshare as ak
            print(f"[Akshare] 正在获取 {symbol} 的财务数据...")
            
            if report_type == 'income':
                df = ak.stock_financial_report_sina(stock=symbol, symbol="利润表")
            elif report_type == 'balance':
                df = ak.stock_financial_report_sina(stock=symbol, symbol="资产负债表")
            elif report_type == 'cashflow':
                df = ak.stock_financial_report_sina(stock=symbol, symbol="现金流量表")
            else:
                df = None
            
            if df is not None and not df.empty:
                print(f"[Akshare] 成功获取财务数据")
                return df
        except Exception as e:
            print(f"[Akshare] 获取失败: {e}")
        
        # akshare失败，尝试tushare
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的财务数据（备用数据源）...")
                
                ts_code = self._convert_to_ts_code(symbol)
                
                if report_type == 'income':
                    df = self.tushare_api.income(ts_code=ts_code)
                elif report_type == 'balance':
                    df = self.tushare_api.balancesheet(ts_code=ts_code)
                elif report_type == 'cashflow':
                    df = self.tushare_api.cashflow(ts_code=ts_code)
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
        if symbol.startswith('6'):
            # 上海主板
            return f"{symbol}.SH"
        elif symbol.startswith('0') or symbol.startswith('3'):
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
