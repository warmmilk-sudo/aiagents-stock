"""
数据源管理器

按数据类型管理优先级，而不是全局一刀切：
1. A股历史日线: AkShare -> Tushare pro_bar
2. 盘中实时行情: Tushare 1 分钟线 -> TDX -> 新浪轻量接口
3. 财务报表: 仅 AkShare/Sina 格式，保持下游字段稳定
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Dict, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

import pandas as pd

from data_source_policy import policy


class DataSourceManager:
    """数据源管理器 - 按数据类型执行不同的优先级链路"""

    SOURCE_PRIORITY = {
        "hist_daily": ("akshare", "tushare_pro_bar"),
        "realtime": ("tushare_1min", "tdx", "sina_http"),
        "financial": ("akshare_sina",),
        "basic_info": ("tushare", "akshare"),
    }

    def __init__(self):
        self.tushare_token = policy.tushare_token
        self.tushare_available = policy.tushare_available
        self.tushare_api = policy.tushare_api
        self.prefer_tushare = policy.prefer_tushare  # 兼容旧调用方，实际优先级由方法自行决定

        if self.tushare_token:
            if self.tushare_available:
                print("[INFO] Tushare data source initialized")
            else:
                print(f"[WARN] Tushare init failed: {policy.tushare_init_error}")
        else:
            print("[INFO] TUSHARE_TOKEN not configured, using AkShare-only mode")

    def get_stock_hist_data(self, symbol, start_date=None, end_date=None, adjust='qfq'):
        """
        获取股票历史日线数据。

        优先级：
        1. AkShare `stock_zh_a_hist`，盘后免费且字段完整
        2. Tushare `pro_bar(freq='D')`，作为 AkShare 失败时的稳定兜底
        """
        start_date = self._normalize_date(start_date)
        end_date = self._normalize_date(end_date) or datetime.now().strftime('%Y%m%d')
        adj = self._normalize_adjust(adjust)

        try:
            import akshare as ak

            print(f"[Akshare] 正在获取 {symbol} 的历史数据...")
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date or "19700101",
                end_date=end_date,
                adjust=adjust or "",
            )
            if df is not None and not df.empty:
                df = self._normalize_akshare_hist_df(df)
                print(f"[Akshare] OK fetched {len(df)} rows")
                return df
        except Exception as e:
            print(f"[Akshare] failed: {e}")

        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的历史数据（备用数据源）...")
                df = self._fetch_tushare_pro_bar(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adj,
                    freq='D',
                )
                if df is not None and not df.empty:
                    df = self._normalize_tushare_hist_df(df)
                    print(f"[Tushare] OK fetched {len(df)} rows")
                    return df
            except Exception as e:
                print(f"[Tushare] failed: {e}")

        print("[ERROR] All history data sources failed")
        return None
    
    def get_stock_basic_info(self, symbol):
        """
        获取股票基本信息（优先tushare，失败时使用akshare）
        
        Args:
            symbol: 股票代码
            
        Returns:
            dict: 股票基本信息
        """
        info = {
            "symbol": symbol,
            "name": "未知",
            "industry": "未知",
            "market": "未知"
        }
        
        # 优先使用tushare
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的基本信息...")
                
                ts_code = self._convert_to_ts_code(symbol)
                df = self.tushare_api.stock_basic(
                    ts_code=ts_code,
                    fields='ts_code,name,area,industry,market,list_date'
                )
                
                if df is not None and not df.empty:
                    info['name'] = df.iloc[0]['name']
                    info['industry'] = df.iloc[0]['industry']
                    info['market'] = df.iloc[0]['market']
                    info['list_date'] = df.iloc[0]['list_date']
                    
                    print("[Tushare] OK fetched basic info")
                    return info
            except Exception as e:
                print(f"[Tushare] failed: {e}")
        
        # tushare失败，尝试akshare
        try:
            import akshare as ak
            print(f"[Akshare] 正在获取 {symbol} 的基本信息（备用数据源）...")
            
            stock_info = ak.stock_individual_info_em(symbol=symbol)
            if stock_info is not None and not stock_info.empty:
                for _, row in stock_info.iterrows():
                    key = row['item']
                    value = row['value']
                    
                    if key == '股票简称':
                        info['name'] = value
                    elif key == '所处行业':
                        info['industry'] = value
                    elif key == '上市时间':
                        info['list_date'] = value
                    elif key == '总市值':
                        info['market_cap'] = value
                    elif key == '流通市值':
                        info['circulating_market_cap'] = value
                
                print("[Akshare] OK fetched basic info")
                return info
        except Exception as e:
            print(f"[Akshare] failed: {e}")
        
        return info
    
    def get_realtime_quotes(self, symbol):
        """
        获取盘中实时行情数据。

        优先级：
        1. Tushare `pro_bar(freq='1MIN')`
        2. 已配置的 TDX 接口
        3. 新浪轻量 HTTP 单股接口

        明确弃用 `ak.stock_zh_a_spot_em()`，避免全市场抓取导致高延迟和封禁风险。
        """
        if self.tushare_available:
            try:
                print(f"[Tushare] 正在获取 {symbol} 的实时行情...")
                quotes = self._get_realtime_quote_from_tushare(symbol)
                if quotes:
                    print("[Tushare] OK fetched realtime quote")
                    return quotes
            except Exception as e:
                print(f"[Tushare] failed: {e}")

        try:
            print(f"[TDX] 正在获取 {symbol} 的实时行情（备用数据源）...")
            quotes = self._get_realtime_quote_from_tdx(symbol)
            if quotes:
                print("[TDX] OK fetched realtime quote")
                return quotes
        except Exception as e:
            print(f"[TDX] failed: {e}")

        try:
            print(f"[Sina] 正在获取 {symbol} 的实时行情（轻量备用数据源）...")
            quotes = self._get_realtime_quote_from_sina(symbol)
            if quotes:
                print("[Sina] OK fetched realtime quote")
                return quotes
        except Exception as e:
            print(f"[Sina] failed: {e}")

        return {}
    
    def get_financial_data(self, symbol, report_type='income'):
        """
        获取财务数据。

        仅使用 AkShare `stock_financial_report_sina`，保持新浪格式与现有下游清洗逻辑一致。
        """
        report_map = {
            'income': "利润表",
            'balance': "资产负债表",
            'cashflow': "现金流量表",
        }
        report_name = report_map.get(report_type)
        if report_name is None:
            return None

        try:
            import akshare as ak

            print(f"[Akshare] 正在获取 {symbol} 的财务数据...")
            sina_stock = self._convert_to_sina_financial_code(symbol)
            df = ak.stock_financial_report_sina(stock=sina_stock, symbol=report_name)
            if df is not None and not df.empty:
                if '报告日' in df.columns:
                    df = df.sort_values('报告日', ascending=False).reset_index(drop=True)
                print("[Akshare] OK fetched financial data")
                return df
        except Exception as e:
            print(f"[Akshare] failed: {e}")

        return None

    def _get_realtime_quote_from_tushare(self, symbol: str) -> Optional[Dict]:
        """使用 Tushare 1 分钟线生成当前最新行情。"""
        today = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
        df = self._fetch_tushare_pro_bar(
            symbol=symbol,
            start_date=start_date,
            end_date=today,
            adjust=None,
            freq='1MIN',
            limit=800,
        )
        if df is None or df.empty:
            return None

        time_column = 'trade_time' if 'trade_time' in df.columns else 'trade_date'
        df = df.copy()
        df['_quote_time'] = pd.to_datetime(df[time_column], errors='coerce')
        df = df.dropna(subset=['_quote_time']).sort_values('_quote_time').reset_index(drop=True)
        if df.empty:
            return None

        row = df.iloc[-1]
        ts_code = self._convert_to_ts_code(symbol)
        prev_close = self._get_previous_close_from_tushare(ts_code, today)
        price = self._safe_float(row.get('close'))
        change = None if prev_close in (None, 0) or price is None else round(price - prev_close, 4)
        change_percent = None
        if prev_close not in (None, 0) and price is not None:
            change_percent = round(change / prev_close * 100, 4)

        return self._build_realtime_quote(
            symbol=symbol,
            name=self._get_stock_name_from_tushare(ts_code) or "未知",
            price=price,
            change=change,
            change_percent=change_percent,
            volume=self._safe_float(row.get('vol')),
            amount=self._safe_float(row.get('amount')),
            high=self._safe_float(row.get('high')),
            low=self._safe_float(row.get('low')),
            open_price=self._safe_float(row.get('open')),
            pre_close=prev_close,
            update_time=str(row.get(time_column)),
            data_source='tushare_1min',
        )

    def _get_realtime_quote_from_tdx(self, symbol: str) -> Optional[Dict]:
        """从本地 TDX 接口获取实时行情。"""
        if os.getenv("TDX_ENABLED", "false").lower() != "true":
            return None

        from smart_monitor_tdx_data import SmartMonitorTDXDataFetcher

        base_url = os.getenv("TDX_BASE_URL", "http://127.0.0.1:5000")
        quote = SmartMonitorTDXDataFetcher(base_url=base_url).get_realtime_quote(symbol)
        if not quote:
            return None

        return self._build_realtime_quote(
            symbol=symbol,
            name=quote.get('name'),
            price=self._safe_float(quote.get('current_price')),
            change=self._safe_float(quote.get('change_amount')),
            change_percent=self._safe_float(quote.get('change_pct')),
            volume=self._safe_float(quote.get('volume')),
            amount=self._safe_float(quote.get('amount')),
            high=self._safe_float(quote.get('high')),
            low=self._safe_float(quote.get('low')),
            open_price=self._safe_float(quote.get('open')),
            pre_close=self._safe_float(quote.get('pre_close')),
            turnover_rate=self._safe_float(quote.get('turnover_rate')),
            volume_ratio=self._safe_float(quote.get('volume_ratio')),
            update_time=quote.get('update_time'),
            data_source='tdx',
        )

    def _get_realtime_quote_from_sina(self, symbol: str) -> Optional[Dict]:
        """从新浪轻量级单股 HTTP 接口获取实时行情。"""
        sina_code = self._convert_to_sina_code(symbol)
        if sina_code is None:
            return None

        request = Request(
            url=f"http://hq.sinajs.cn/list={sina_code}",
            headers={
                "Referer": "https://finance.sina.com.cn",
                "User-Agent": "Mozilla/5.0",
            },
        )

        try:
            with urlopen(request, timeout=5) as response:
                raw = response.read().decode('gbk', errors='ignore')
        except URLError:
            return None

        match = re.search(r'=\"(.*)\"', raw)
        if not match:
            return None

        parts = match.group(1).split(',')
        if len(parts) < 10 or not parts[0]:
            return None

        name = parts[0]
        open_price = self._safe_float(parts[1])
        pre_close = self._safe_float(parts[2])
        price = self._safe_float(parts[3])
        high = self._safe_float(parts[4])
        low = self._safe_float(parts[5])
        volume = self._safe_float(parts[8])
        amount = self._safe_float(parts[9])
        update_time = ""
        if len(parts) >= 32:
            update_time = f"{parts[30]} {parts[31]}".strip()

        change = None if price is None or pre_close in (None, 0) else round(price - pre_close, 4)
        change_percent = None
        if change is not None and pre_close not in (None, 0):
            change_percent = round(change / pre_close * 100, 4)

        return self._build_realtime_quote(
            symbol=symbol,
            name=name,
            price=price,
            change=change,
            change_percent=change_percent,
            volume=volume,
            amount=amount,
            high=high,
            low=low,
            open_price=open_price,
            pre_close=pre_close,
            update_time=update_time,
            data_source='sina_http',
        )

    def _fetch_tushare_pro_bar(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
        adjust: Optional[str],
        freq: str,
        limit: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """统一封装 Tushare pro_bar，兼容日线和分钟线。"""
        if not self.tushare_available:
            return None

        tushare = self._get_tushare_sdk()
        return tushare.pro_bar(
            ts_code=self._convert_to_ts_code(symbol),
            api=self.tushare_api,
            start_date=start_date or "",
            end_date=end_date or "",
            freq=freq,
            asset='E',
            adj=adjust,
            limit=limit,
        )

    def _get_previous_close_from_tushare(self, ts_code: str, today: str) -> Optional[float]:
        """获取用于计算实时涨跌的昨收价。"""
        if not self.tushare_api:
            return None

        try:
            df = self.tushare_api.daily(ts_code=ts_code, end_date=today)
        except Exception:
            return None

        if df is None or df.empty:
            return None

        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
        latest = df.iloc[0]
        latest_trade_date = str(latest.get('trade_date', ''))

        if latest_trade_date == today:
            return self._safe_float(latest.get('pre_close'))
        return self._safe_float(latest.get('close'))

    def _get_stock_name_from_tushare(self, ts_code: str) -> Optional[str]:
        """获取股票名称，避免实时返回字典缺失 name。"""
        if not self.tushare_api:
            return None

        try:
            df = self.tushare_api.stock_basic(ts_code=ts_code, fields='ts_code,name')
        except Exception:
            return None

        if df is None or df.empty:
            return None
        return df.iloc[0].get('name')

    def _normalize_akshare_hist_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """统一 AkShare 日线字段。"""
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
            '换手率': 'turnover',
        })
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
        return df

    def _normalize_tushare_hist_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """统一 Tushare 日线字段，并修正成交量/额单位。"""
        df = df.rename(columns={
            'trade_date': 'date',
            'vol': 'volume',
            'pct_chg': 'pct_change',
        })
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce') * 100
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce') * 1000
        return df

    def _build_realtime_quote(
        self,
        symbol: str,
        name: Optional[str],
        price: Optional[float],
        change: Optional[float],
        change_percent: Optional[float],
        volume: Optional[float],
        amount: Optional[float],
        high: Optional[float],
        low: Optional[float],
        open_price: Optional[float],
        pre_close: Optional[float],
        update_time: Optional[str],
        data_source: str,
        turnover_rate: Optional[float] = None,
        volume_ratio: Optional[float] = None,
    ) -> Dict:
        """输出统一行情字典，兼容旧字段与新字段。"""
        return {
            'symbol': symbol,
            'code': symbol,
            'name': name or "未知",
            'price': price,
            'current_price': price,
            'change_percent': change_percent,
            'change_pct': change_percent,
            'change': change,
            'change_amount': change,
            'volume': volume,
            'amount': amount,
            'high': high,
            'low': low,
            'open': open_price,
            'pre_close': pre_close,
            'turnover_rate': turnover_rate,
            'volume_ratio': volume_ratio,
            'update_time': update_time,
            'data_source': data_source,
        }

    def _get_tushare_sdk(self):
        """延迟导入 tushare，便于测试替换。"""
        import tushare as ts

        return ts

    def _normalize_date(self, value: Optional[str]) -> Optional[str]:
        """将日期参数统一成 YYYYMMDD。"""
        if not value:
            return None
        return str(value).replace('-', '').strip()

    def _normalize_adjust(self, adjust: Optional[str]) -> Optional[str]:
        """规范复权参数。"""
        if adjust is None:
            return 'qfq'
        adjust = str(adjust).strip().lower()
        if adjust in {'', 'none', 'null'}:
            return None
        if adjust in {'qfq', 'hfq'}:
            return adjust
        return 'qfq'

    def _safe_float(self, value) -> Optional[float]:
        """安全转换为 float。"""
        if value in (None, '', 'None', 'nan'):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
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

    def _convert_to_sina_code(self, symbol: str) -> Optional[str]:
        """转换为新浪实时接口代码。"""
        if not symbol or len(symbol) != 6 or not symbol.isdigit():
            return None
        if symbol.startswith('6'):
            return f"sh{symbol}"
        if symbol.startswith(('0', '3')):
            return f"sz{symbol}"
        if symbol.startswith(('4', '8')):
            return f"bj{symbol}"
        return None

    def _convert_to_sina_financial_code(self, symbol: str) -> str:
        """转换为 AkShare 新浪财报接口要求的带市场前缀代码。"""
        sina_code = self._convert_to_sina_code(symbol)
        return sina_code or symbol
    
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

