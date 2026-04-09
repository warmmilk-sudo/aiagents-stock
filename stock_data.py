import yfinance as yf
import pandas as pd
import numpy as np
import ta
from datetime import datetime, timedelta
import requests
import json
from pywencai_runtime import setup_pywencai_runtime_env

setup_pywencai_runtime_env()
import pywencai
from data_source_manager import data_source_manager
from stock_data_cache import stock_data_cache_service

class StockDataFetcher:
    """股票数据获取类"""
    
    def __init__(self, cache_service=None):
        self.data = None
        self.info = None
        self.financial_data = None
        self.data_source_manager = data_source_manager
        self.cache_service = cache_service or stock_data_cache_service
        
    def get_stock_info(self, symbol, max_age_seconds=300, allow_stale_on_failure=True, cache_first=True):
        """获取股票基本信息"""
        try:
            return self.cache_service.get_stock_info(
                symbol=symbol,
                market=self._detect_market(symbol),
                fetch_fn=lambda: self._fetch_stock_info_live(symbol),
                max_age_seconds=max_age_seconds,
                allow_stale_on_failure=allow_stale_on_failure,
                cache_first=cache_first,
            )
        except Exception as e:
            return {"error": f"获取股票信息失败: {str(e)}"}

    def get_realtime_quote(self, symbol, retry=1):
        """获取实时行情，仅使用实时源，不回退到历史日线。"""
        import math
        import time

        if not self._is_chinese_stock(symbol):
            return None

        for attempt in range(max(1, int(retry or 1))):
            try:
                quote = self.data_source_manager.get_realtime_quotes(symbol)
                if isinstance(quote, dict) and quote:
                    price = quote.get("price")
                    try:
                        price_value = float(price)
                    except (TypeError, ValueError):
                        return None

                    if not math.isfinite(price_value) or price_value <= 0:
                        return None

                    return {
                        "code": symbol,
                        "symbol": symbol,
                        "name": quote.get("name"),
                        "current_price": price_value,
                        "price": price_value,
                        "change_percent": quote.get("change_percent"),
                        "change_amount": quote.get("change", quote.get("change_amount")),
                        "volume": quote.get("volume"),
                        "amount": quote.get("amount"),
                        "high": quote.get("high"),
                        "low": quote.get("low"),
                        "open": quote.get("open"),
                        "pre_close": quote.get("pre_close"),
                        "data_source": quote.get("data_source"),
                    }
            except Exception as e:
                print(f"[WARN] 获取实时行情失败 ({symbol}): {e}")

            if attempt < max(1, int(retry or 1)) - 1:
                time.sleep(1)

        return None
    
    def get_stock_data(
        self,
        symbol,
        period="1y",
        interval="1d",
        max_age_seconds=86400,
        allow_stale_on_failure=True,
        cache_first=True,
        adjust="qfq",
    ):
        """获取股票历史数据"""
        try:
            return self.cache_service.get_stock_history(
                symbol=symbol,
                period=period,
                interval=interval,
                adjust=adjust,
                fetch_fn=lambda: self._fetch_stock_data_live(symbol, period, interval, adjust),
                max_age_seconds=max_age_seconds,
                allow_stale_on_failure=allow_stale_on_failure,
                cache_first=cache_first,
            )
        except Exception as e:
            return {"error": f"获取股票数据失败: {str(e)}"}
    
    def _detect_market(self, symbol):
        if self._is_chinese_stock(symbol):
            return "cn"
        if self._is_hk_stock(symbol):
            return "hk"
        return "us"

    def _fetch_stock_info_live(self, symbol):
        if self._is_chinese_stock(symbol):
            return self._get_chinese_stock_info(symbol)
        if self._is_hk_stock(symbol):
            return self._get_hk_stock_info(symbol)
        return self._get_us_stock_info(symbol)

    def _fetch_stock_data_live(self, symbol, period="1y", interval="1d", adjust="qfq"):
        if self._is_chinese_stock(symbol):
            return self._get_chinese_stock_data(symbol, period, adjust=adjust)
        if self._is_hk_stock(symbol):
            return self._get_hk_stock_data(symbol, period, adjust=adjust)
        return self._get_us_stock_data(symbol, period, interval)

    def _is_chinese_stock(self, symbol):
        """判断是否为中国A股"""
        # 简单判断：包含数字且长度为6位的认为是中国A股
        return symbol.isdigit() and len(symbol) == 6
    
    def _is_hk_stock(self, symbol):
        """判断是否为港股"""
        # 港股代码通常是1-5位数字，或者前面带HK/hk前缀
        if symbol.upper().startswith('HK'):
            return True
        # 纯数字且长度在1-5位之间，认为可能是港股
        if symbol.isdigit() and 1 <= len(symbol) <= 5:
            return True
        return False
    
    def _normalize_hk_code(self, symbol):
        """规范化港股代码为5位格式（如700 -> 00700）"""
        # 移除HK前缀
        if symbol.upper().startswith('HK'):
            symbol = symbol[2:]
        # 补齐到5位
        return symbol.zfill(5)

    def _normalize_hk_yahoo_symbol(self, symbol):
        """规范化港股代码为 Yahoo Finance 格式（如 700 -> 0700.HK）。"""
        hk_code = self._normalize_hk_code(symbol)
        return f"{int(hk_code):04d}.HK"

    @staticmethod
    def _is_missing_scalar(value):
        if value is None:
            return True
        try:
            return bool(pd.isna(value))
        except Exception:
            return False

    def _call_tushare_dataframe(self, api_name, **kwargs):
        if not getattr(self.data_source_manager, "tushare_available", False):
            return None

        helper = getattr(self.data_source_manager, "call_tushare_api", None)
        if callable(helper):
            return helper(api_name, **kwargs)

        method = getattr(getattr(self.data_source_manager, "tushare_api", None), api_name, None)
        if method is None:
            return None
        return method(**kwargs)

    def _fill_cn_valuation_from_tushare(self, symbol, info):
        if not self.data_source_manager.tushare_available:
            return info

        ts_code = self.data_source_manager._convert_to_ts_code(symbol)
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=20)).strftime('%Y%m%d')

        try:
            print(f"[Tushare] 正在获取 {symbol} 的估值补充信息...")
            df = self._call_tushare_dataframe(
                "daily_basic",
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,pe,pb,total_mv',
            )
            if df is not None and not df.empty:
                df = df.sort_values('trade_date', ascending=False)
                for _, row in df.iterrows():
                    if self._is_missing_scalar(info.get('pe_ratio')) and not self._is_missing_scalar(row.get('pe')):
                        info['pe_ratio'] = row.get('pe')
                    if self._is_missing_scalar(info.get('pb_ratio')) and not self._is_missing_scalar(row.get('pb')):
                        info['pb_ratio'] = row.get('pb')
                    if self._is_missing_scalar(info.get('market_cap')) and not self._is_missing_scalar(row.get('total_mv')):
                        info['market_cap'] = row.get('total_mv')
                    if not any(self._is_missing_scalar(info.get(key)) for key in ('pe_ratio', 'pb_ratio', 'market_cap')):
                        break
                print("[Tushare] ✅ 成功获取估值补充信息")
                return info
        except Exception as te:
            print(f"[Tushare] ❌ 获取 daily_basic 估值信息失败: {te}")

        try:
            bak_df = self._call_tushare_dataframe(
                "bak_basic",
                trade_date=end_date,
                fields='ts_code,pe,pb,total_mv,industry,name',
            )
            if bak_df is not None and not bak_df.empty:
                row_df = bak_df[bak_df['ts_code'] == ts_code]
                if not row_df.empty:
                    row = row_df.iloc[0]
                    if self._is_missing_scalar(info.get('name')) and not self._is_missing_scalar(row.get('name')):
                        info['name'] = row.get('name')
                    if self._is_missing_scalar(info.get('industry')) and not self._is_missing_scalar(row.get('industry')):
                        info['industry'] = row.get('industry')
                    if self._is_missing_scalar(info.get('pe_ratio')) and not self._is_missing_scalar(row.get('pe')):
                        info['pe_ratio'] = row.get('pe')
                    if self._is_missing_scalar(info.get('pb_ratio')) and not self._is_missing_scalar(row.get('pb')):
                        info['pb_ratio'] = row.get('pb')
                    if self._is_missing_scalar(info.get('market_cap')) and not self._is_missing_scalar(row.get('total_mv')):
                        info['market_cap'] = row.get('total_mv')
                    print("[Tushare] ✅ 使用 bak_basic 补全估值信息")
        except Exception as te:
            print(f"[Tushare] ❌ 获取 bak_basic 估值信息失败: {te}")

        return info

    def _fill_cn_market_metrics_from_history(self, symbol, info):
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=400)).strftime('%Y%m%d')
        hist_df = self.data_source_manager.get_stock_hist_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust='qfq',
        )
        if hist_df is None or hist_df.empty:
            return info

        try:
            if self._is_missing_scalar(info.get('52_week_high')) and 'high' in hist_df.columns:
                info['52_week_high'] = float(hist_df['high'].tail(252).max())
            if self._is_missing_scalar(info.get('52_week_low')) and 'low' in hist_df.columns:
                info['52_week_low'] = float(hist_df['low'].tail(252).min())
        except Exception:
            pass

        if self._is_missing_scalar(info.get('beta')) and self.data_source_manager.tushare_available:
            try:
                benchmark_df = self._call_tushare_dataframe(
                    "index_daily",
                    ts_code='000300.SH',
                    start_date=start_date,
                    end_date=end_date,
                    fields='trade_date,close',
                )
                if benchmark_df is not None and not benchmark_df.empty:
                    stock_returns = (
                        hist_df.loc[:, ['date', 'close']]
                        .dropna()
                        .sort_values('date')
                        .assign(stock_ret=lambda df: df['close'].pct_change())
                    )
                    benchmark_returns = (
                        benchmark_df.loc[:, ['trade_date', 'close']]
                        .dropna()
                        .rename(columns={'trade_date': 'date'})
                    )
                    benchmark_returns['date'] = pd.to_datetime(benchmark_returns['date'], errors='coerce')
                    benchmark_returns = (
                        benchmark_returns
                        .dropna(subset=['date'])
                        .sort_values('date')
                        .assign(index_ret=lambda df: df['close'].pct_change())
                    )
                    merged = stock_returns.merge(
                        benchmark_returns[['date', 'index_ret']],
                        on='date',
                        how='inner',
                    ).dropna(subset=['stock_ret', 'index_ret']).tail(252)
                    if len(merged) >= 60:
                        variance = merged['index_ret'].var()
                        if variance and not np.isnan(variance):
                            beta_value = merged['stock_ret'].cov(merged['index_ret']) / variance
                            if np.isfinite(beta_value):
                                info['beta'] = round(float(beta_value), 4)
            except Exception as exc:
                print(f"[Tushare] ❌ 计算 Beta 失败: {exc}")

        return info
    
    def _get_chinese_stock_info(self, symbol):
        """获取中国股票基本信息。"""
        try:
            # 初始化基本信息
            info = {
                "symbol": symbol,
                "name": None,
                "current_price": None,
                "change_percent": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "ps_ratio": None,
                "market_cap": None,
                "sector": None,
                "industry": None,
                "beta": None,
                "52_week_high": None,
                "52_week_low": None,
                "market": "中国A股",
                "exchange": "上海/深圳证券交易所"
            }
            
            # 先尝试使用数据源管理器获取基本信息
            basic_info = self.data_source_manager.get_stock_basic_info(symbol)
            if basic_info:
                info.update(basic_info)
            
            info = self._fill_cn_valuation_from_tushare(symbol, info)
            if self.data_source_manager.tushare_available:
                try:
                    ts_code = self.data_source_manager._convert_to_ts_code(symbol)
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y%m%d')
                    ps_df = self._call_tushare_dataframe(
                        "daily_basic",
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        fields='ts_code,trade_date,ps,ps_ttm',
                    )
                    if ps_df is not None and not ps_df.empty:
                        ps_df = ps_df.sort_values('trade_date', ascending=False)
                        for _, row in ps_df.iterrows():
                            ps_ttm = row.get('ps_ttm')
                            ps = row.get('ps')
                            candidate = ps_ttm if not self._is_missing_scalar(ps_ttm) else ps
                            if not self._is_missing_scalar(candidate):
                                info['ps_ratio'] = candidate
                                break
                except Exception as te:
                    print(f"[Tushare] ❌ 获取 PS 估值信息失败: {te}")

            if self._is_missing_scalar(info.get('sector')) and not self._is_missing_scalar(info.get('industry')):
                info['sector'] = info.get('industry')
            info = self._fill_cn_market_metrics_from_history(symbol, info)
            if self._is_missing_scalar(info.get('current_price')) or self._is_missing_scalar(info.get('change_percent')):
                realtime_quote = self.get_realtime_quote(symbol, retry=1)
                if realtime_quote:
                    if self._is_missing_scalar(info.get('name')) and realtime_quote.get('name'):
                        info['name'] = realtime_quote.get('name')
                    if self._is_missing_scalar(info.get('current_price')):
                        info['current_price'] = realtime_quote.get('current_price')
                    if self._is_missing_scalar(info.get('change_percent')):
                        info['change_percent'] = realtime_quote.get('change_percent')
            
            # 不再使用历史日线收盘价回填 current_price/change_percent。
            # 如果实时源不可用，这里保持 None，避免把收盘价误当成盘中现价。
            
            return info
            
        except Exception as e:
            print(f"获取中国股票信息完全失败: {e}")
            return {
                "symbol": symbol,
                "name": None,
                "current_price": None,
                "change_percent": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "market_cap": None,
                "market": "中国A股",
                "exchange": "上海/深圳证券交易所"
            }
    
    def _get_hk_stock_info(self, symbol):
        """获取港股基本信息"""
        try:
            hk_code = self._normalize_hk_code(symbol)
            yahoo_symbol = self._normalize_hk_yahoo_symbol(symbol)
            
            info = {
                "symbol": hk_code,
                "name": None,
                "current_price": None,
                "change_percent": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "market_cap": None,
                "market": "香港股市",
                "exchange": "香港交易所"
            }
            
            try:
                ticker = yf.Ticker(yahoo_symbol)
                ticker_info = ticker.info or {}
                history = ticker.history(period="5d", interval="1d")

                info["name"] = ticker_info.get("longName") or ticker_info.get("shortName")
                info["current_price"] = ticker_info.get("currentPrice") or ticker_info.get("regularMarketPrice")
                info["market_cap"] = ticker_info.get("marketCap")
                info["pe_ratio"] = ticker_info.get("trailingPE") or ticker_info.get("forwardPE")
                info["pb_ratio"] = ticker_info.get("priceToBook")
                info["exchange"] = ticker_info.get("exchange") or info["exchange"]

                if history is not None and not history.empty and len(history) >= 2:
                    latest_close = float(history["Close"].iloc[-1])
                    prev_close = float(history["Close"].iloc[-2])
                    if info["current_price"] in (None, 0):
                        info["current_price"] = latest_close
                    if prev_close:
                        info["change_percent"] = (float(info["current_price"]) - prev_close) / prev_close * 100
            except Exception as e:
                print(f"获取港股实时数据失败: {e}")
            
            # 不再使用历史日线收盘价回填 current_price/change_percent。
            
            return info
            
        except Exception as e:
            print(f"获取港股信息完全失败: {e}")
            return {
                "symbol": symbol,
                "name": None,
                "current_price": None,
                "change_percent": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "market_cap": None,
                "market": "香港股市",
                "exchange": "香港交易所"
            }
    
    def _get_us_stock_info(self, symbol):
        """获取美股基本信息"""
        import time
        
        try:
            # 添加延迟避免频率限制
            time.sleep(1)
            
            ticker = yf.Ticker(symbol)
            
            # 不再使用历史收盘价回填 current_price；只信任行情源或 ticker.info 的现价字段。
            current_price = None
            change_percent = None
            
            # 获取基本信息
            try:
                info = ticker.info
                
                # 获取市盈率，优先使用trailing PE，其次forward PE
                pe_ratio = info.get('trailingPE') if info.get('trailingPE') is not None else info.get('forwardPE')
                if pe_ratio is None or (isinstance(pe_ratio, float) and np.isnan(pe_ratio)):
                    pe_ratio = None
                
                # 获取市净率
                pb_ratio = info.get('priceToBook')
                if pb_ratio is None or (isinstance(pb_ratio, float) and np.isnan(pb_ratio)):
                    pb_ratio = None
                
                # 如果行情源没有获取到价格，尝试从info获取
                if current_price is None:
                    current_price = info.get('currentPrice') if info.get('currentPrice') is not None else info.get('regularMarketPrice')
                
                if change_percent is None:
                    change_percent = info.get('regularMarketChangePercent')
                    if change_percent is not None:
                        change_percent = change_percent * 100  # 转换为百分比
                
                return {
                    "symbol": symbol,
                    "name": info.get('longName') if info.get('longName') is not None else info.get('shortName'),
                    "current_price": current_price,
                    "change_percent": change_percent,
                    "market_cap": info.get('marketCap'),
                    "pe_ratio": pe_ratio,
                    "pb_ratio": pb_ratio,
                    "dividend_yield": info.get('dividendYield'),
                    "beta": info.get('beta'),
                    "52_week_high": info.get('fiftyTwoWeekHigh'),
                    "52_week_low": info.get('fiftyTwoWeekLow'),
                    "sector": info.get('sector'),
                    "industry": info.get('industry'),
                    "market": "美股",
                    "exchange": info.get('exchange')
                }
                
            except Exception as e:
                return {
                    "symbol": symbol,
                    "name": None,
                    "current_price": current_price,
                    "change_percent": change_percent,
                    "market_cap": None,
                    "pe_ratio": None,
                    "pb_ratio": None,
                    "dividend_yield": None,
                    "beta": None,
                    "52_week_high": None,
                    "52_week_low": None,
                    "sector": None,
                    "industry": None,
                    "market": "美股",
                    "exchange": None
                }
                
        except Exception as e:
            return {"error": f"获取美股信息失败: {str(e)}"}
    
    def _get_chinese_stock_data(self, symbol, period="1y", adjust="qfq"):
        """获取中国股票历史数据。"""
        try:
            # 计算日期范围
            end_date = datetime.now().strftime('%Y%m%d')
            if period == "1y":
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            elif period == "6mo":
                start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')
            elif period == "3mo":
                start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
            else:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            # 使用数据源管理器获取数据
            df = self.data_source_manager.get_stock_hist_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            if df is not None and not df.empty:
                # 标准化列名为大写（与原有格式保持一致）
                df = df.rename(columns={
                    'date': 'Date',
                    'open': 'Open',
                    'close': 'Close',
                    'high': 'High',
                    'low': 'Low',
                    'volume': 'Volume'
                })
                
                # 确保Date列为datetime类型
                if 'Date' not in df.columns and df.index.name == 'date':
                    df.index.name = 'Date'
                elif 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'])
                    df.set_index('Date', inplace=True)
                
                print(f"✅ 成功获取 {symbol} 的历史数据，共 {len(df)} 条记录")
                return df
            else:
                return {"error": "所有数据源均无法获取历史数据"}
                
        except Exception as e:
            return {"error": f"获取中国股票数据失败: {str(e)}"}
    
    def _get_hk_stock_data(self, symbol, period="1y", adjust="qfq"):
        """获取港股历史数据"""
        try:
            yahoo_symbol = self._normalize_hk_yahoo_symbol(symbol)
            ticker = yf.Ticker(yahoo_symbol)
            yf_period = period if period in {"1mo", "3mo", "6mo", "1y", "2y", "5y"} else "1y"
            df = ticker.history(period=yf_period, interval="1d", auto_adjust=(adjust == "qfq"))

            if df is not None and not df.empty:
                return df
            else:
                return {"error": "无法获取港股历史数据"}
                
        except Exception as e:
            return {"error": f"获取港股数据失败: {str(e)}"}
    
    def _get_us_stock_data(self, symbol, period="1y", interval="1d"):
        """获取美股历史数据"""
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval)
            if not df.empty:
                return df
            else:
                return {"error": "无法获取历史数据"}
        except Exception as e:
            return {"error": f"获取美股数据失败: {str(e)}"}
    
    def calculate_technical_indicators(self, df):
        """计算技术指标"""
        try:
            if isinstance(df, dict) and "error" in df:
                return df
                
            # 移动平均线
            df['MA5'] = ta.trend.sma_indicator(df['Close'], window=5)
            df['MA10'] = ta.trend.sma_indicator(df['Close'], window=10)
            df['MA20'] = ta.trend.sma_indicator(df['Close'], window=20)
            df['MA60'] = ta.trend.sma_indicator(df['Close'], window=60)
            
            # RSI
            df['RSI'] = ta.momentum.rsi(df['Close'], window=14)
            
            # MACD
            macd = ta.trend.MACD(df['Close'])
            df['MACD'] = macd.macd()
            df['MACD_signal'] = macd.macd_signal()
            df['MACD_histogram'] = macd.macd_diff()
            
            # 布林带
            bollinger = ta.volatility.BollingerBands(df['Close'])
            df['BB_upper'] = bollinger.bollinger_hband()
            df['BB_middle'] = bollinger.bollinger_mavg()
            df['BB_lower'] = bollinger.bollinger_lband()
            
            # KDJ指标
            df['K'] = ta.momentum.stoch(df['High'], df['Low'], df['Close'])
            df['D'] = ta.momentum.stoch_signal(df['High'], df['Low'], df['Close'])
            
            # 成交量指标
            df['Volume_MA5'] = ta.trend.sma_indicator(df['Volume'], window=5)
            df['Volume_ratio'] = df['Volume'] / df['Volume_MA5']
            
            return df
            
        except Exception as e:
            return {"error": f"计算技术指标失败: {str(e)}"}

    def _coerce_float(self, value):
        try:
            if value is None or pd.isna(value):
                return None
            numeric = float(value)
            if not np.isfinite(numeric):
                return None
            return numeric
        except Exception:
            return None

    def _extract_latest_trade_date(self, df):
        if df is None or not hasattr(df, "empty") or df.empty:
            return None

        try:
            latest_index = pd.to_datetime(df.index[-1], errors="coerce")
            if not pd.isna(latest_index):
                return latest_index.strftime("%Y%m%d")
        except Exception:
            pass

        if "Date" in getattr(df, "columns", []):
            latest_date = pd.to_datetime(df["Date"].iloc[-1], errors="coerce")
            if not pd.isna(latest_date):
                return latest_date.strftime("%Y%m%d")
        return None

    def _summarize_real_chip_metrics(self, chips_df, perf_row, latest_price, source_label):
        default_result = {
            "chip_data_source": source_label,
            "chip_trade_date": "N/A",
            "chip_peak_shape": "N/A",
            "main_chip_peak_price": "N/A",
            "secondary_chip_peak_price": "N/A",
            "chip_concentration": "N/A",
            "average_chip_cost": "N/A",
            "cost_band_70": "N/A",
            "cost_band_90": "N/A",
            "current_price_position": "N/A",
            "upper_pressure_peak": "N/A",
            "lower_support_peak": "N/A",
            "profit_ratio_estimate": "N/A",
            "trap_ratio_estimate": "N/A",
        }

        if chips_df is None or chips_df.empty:
            return default_result

        chip_view = chips_df.copy()
        chip_view["price"] = pd.to_numeric(chip_view["price"], errors="coerce")
        chip_view["percent"] = pd.to_numeric(chip_view["percent"], errors="coerce")
        chip_view = chip_view.dropna(subset=["price", "percent"]).sort_values("price").reset_index(drop=True)
        if chip_view.empty:
            return default_result

        total_percent = float(chip_view["percent"].sum())
        if total_percent <= 0:
            return default_result

        chip_view["weight"] = chip_view["percent"] / total_percent
        prices = chip_view["price"].to_numpy(dtype=float)
        peak_weights = chip_view["percent"].to_numpy(dtype=float)

        peak_indexes = []
        for index, value in enumerate(peak_weights):
            left = peak_weights[index - 1] if index > 0 else value
            right = peak_weights[index + 1] if index < len(peak_weights) - 1 else value
            if value > 0 and value >= left and value >= right:
                peak_indexes.append(index)
        if not peak_indexes:
            peak_indexes = [int(np.argmax(peak_weights))]

        peak_indexes = sorted(set(peak_indexes), key=lambda idx: peak_weights[idx], reverse=True)
        main_peak_index = peak_indexes[0]
        main_peak_price = float(prices[main_peak_index])

        min_gap = max(0.1, float(np.nanmedian(np.diff(prices))) * 3) if len(prices) > 1 else 0.1
        secondary_peak_index = None
        for peak_index in peak_indexes[1:]:
            if abs(float(prices[peak_index]) - main_peak_price) >= min_gap and peak_weights[peak_index] >= peak_weights[main_peak_index] * 0.35:
                secondary_peak_index = peak_index
                break

        significant_peaks = [idx for idx in peak_indexes if peak_weights[idx] >= peak_weights[main_peak_index] * 0.25]
        if len(significant_peaks) <= 1:
            chip_peak_shape = "单峰密集"
        elif len(significant_peaks) == 2:
            chip_peak_shape = "双峰博弈"
        else:
            chip_peak_shape = "多峰发散"

        def _percentile_price(target_ratio):
            cumulative = chip_view["weight"].cumsum()
            match_index = cumulative.searchsorted(target_ratio, side="left")
            match_index = int(np.clip(match_index, 0, len(chip_view) - 1))
            return float(chip_view.iloc[match_index]["price"])

        avg_cost = self._coerce_float(perf_row.get("weight_avg")) if perf_row is not None else None
        if avg_cost is None:
            avg_cost = float((chip_view["price"] * chip_view["weight"]).sum())

        cost_5pct = self._coerce_float(perf_row.get("cost_5pct")) if perf_row is not None else None
        cost_15pct = self._coerce_float(perf_row.get("cost_15pct")) if perf_row is not None else None
        cost_50pct = self._coerce_float(perf_row.get("cost_50pct")) if perf_row is not None else None
        cost_85pct = self._coerce_float(perf_row.get("cost_85pct")) if perf_row is not None else None
        cost_95pct = self._coerce_float(perf_row.get("cost_95pct")) if perf_row is not None else None

        if cost_5pct is None:
            cost_5pct = _percentile_price(0.05)
        if cost_15pct is None:
            cost_15pct = _percentile_price(0.15)
        if cost_50pct is None:
            cost_50pct = _percentile_price(0.50)
        if cost_85pct is None:
            cost_85pct = _percentile_price(0.85)
        if cost_95pct is None:
            cost_95pct = _percentile_price(0.95)

        band_70_width_pct = ((cost_85pct - cost_15pct) / avg_cost * 100) if avg_cost else None
        if band_70_width_pct is None:
            concentration_level = "N/A"
        elif band_70_width_pct <= 12:
            concentration_level = "高"
        elif band_70_width_pct <= 22:
            concentration_level = "中"
        else:
            concentration_level = "低"

        winner_rate = self._coerce_float(perf_row.get("winner_rate")) if perf_row is not None else None
        if winner_rate is None:
            winner_rate = float(chip_view.loc[chip_view["price"] <= latest_price, "weight"].sum() * 100)
        winner_rate = max(0.0, min(100.0, winner_rate))
        trap_rate = max(0.0, 100.0 - winner_rate)

        current_vs_peak_pct = ((latest_price / main_peak_price) - 1) * 100 if main_peak_price else None
        current_vs_avg_pct = ((latest_price / avg_cost) - 1) * 100 if avg_cost else None
        if current_vs_peak_pct is None:
            current_price_position = "N/A"
        elif current_vs_peak_pct >= 5:
            current_price_position = f"显著站上主峰 {current_vs_peak_pct:.1f}%"
        elif current_vs_peak_pct >= 1:
            current_price_position = f"略高于主峰 {current_vs_peak_pct:.1f}%"
        elif current_vs_peak_pct <= -5:
            current_price_position = f"显著跌破主峰 {abs(current_vs_peak_pct):.1f}%"
        elif current_vs_peak_pct <= -1:
            current_price_position = f"略低于主峰 {abs(current_vs_peak_pct):.1f}%"
        else:
            current_price_position = "贴近主峰震荡"
        if current_vs_avg_pct is not None:
            current_price_position = f"{current_price_position}，相对平均成本 {current_vs_avg_pct:+.1f}%"

        upper_pressure_peak = "N/A"
        lower_support_peak = "N/A"
        for peak_index in sorted(significant_peaks, key=lambda idx: prices[idx]):
            peak_price = float(prices[peak_index])
            if peak_price > latest_price and upper_pressure_peak == "N/A":
                upper_pressure_peak = round(peak_price, 2)
            if peak_price < latest_price:
                lower_support_peak = round(peak_price, 2)

        trade_date_value = None
        if "trade_date" in chip_view.columns and not chip_view["trade_date"].empty:
            trade_date_value = str(chip_view["trade_date"].iloc[0])

        return {
            "chip_data_source": source_label,
            "chip_trade_date": trade_date_value or "N/A",
            "chip_peak_shape": chip_peak_shape,
            "main_chip_peak_price": round(main_peak_price, 2),
            "secondary_chip_peak_price": round(float(prices[secondary_peak_index]), 2) if secondary_peak_index is not None else "N/A",
            "chip_concentration": f"{concentration_level} (70%成本带宽 {band_70_width_pct:.1f}%)" if band_70_width_pct is not None else "N/A",
            "average_chip_cost": round(avg_cost, 2) if avg_cost is not None else "N/A",
            "cost_band_70": f"{cost_15pct:.2f}-{cost_85pct:.2f}",
            "cost_band_90": f"{cost_5pct:.2f}-{cost_95pct:.2f}",
            "current_price_position": current_price_position,
            "upper_pressure_peak": upper_pressure_peak,
            "lower_support_peak": lower_support_peak,
            "profit_ratio_estimate": f"{winner_rate:.1f}%",
            "trap_ratio_estimate": f"{trap_rate:.1f}%",
            "median_chip_cost": round(cost_50pct, 2),
        }

    def _get_chip_peak_metrics_from_tushare(self, symbol, latest_price, latest_trade_date):
        if not symbol or not self._is_chinese_stock(symbol):
            return None
        manager = getattr(self, "data_source_manager", None)
        if manager is None or not getattr(manager, "tushare_available", False):
            print(f"[Chip] {symbol} 未启用Tushare，跳过真实筹码分布")
            return None

        tushare_api = getattr(manager, "tushare_api", None)
        if tushare_api is None:
            print(f"[Chip] {symbol} Tushare客户端不可用，跳过真实筹码分布")
            return None

        try:
            ts_code = manager._convert_to_ts_code(symbol)
        except Exception:
            print(f"[Chip] {symbol} 股票代码转换失败，跳过真实筹码分布")
            return None

        end_date = latest_trade_date or datetime.now().strftime("%Y%m%d")
        try:
            start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=14)).strftime("%Y%m%d")
        except Exception:
            start_date = (datetime.now() - timedelta(days=14)).strftime("%Y%m%d")

        print(f"[Tushare] 正在获取 {symbol} 的筹码分布，时间范围 {start_date}-{end_date}...")

        try:
            perf_df = tushare_api.cyq_perf(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except Exception as e:
            print(f"[Tushare] 获取筹码胜率失败: {e}")
            perf_df = None

        try:
            chips_df = tushare_api.cyq_chips(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except Exception as e:
            print(f"[Tushare] 获取筹码分布失败: {e}")
            chips_df = None

        if chips_df is None or chips_df.empty:
            print(f"[Tushare] 未获取到 {symbol} 的筹码分布数据")
            return None

        chips_view = chips_df.copy()
        chips_view["trade_date"] = chips_view["trade_date"].astype(str)
        latest_available_trade_date = str(chips_view["trade_date"].max())
        latest_chips = chips_view.loc[chips_view["trade_date"] == latest_available_trade_date].copy()
        print(f"[Tushare] 成功获取 {symbol} 的筹码分布，使用交易日 {latest_available_trade_date}，共 {len(latest_chips)} 个价格点")

        perf_row = None
        if perf_df is not None and not perf_df.empty:
            perf_view = perf_df.copy()
            perf_view["trade_date"] = perf_view["trade_date"].astype(str)
            latest_perf = perf_view.loc[perf_view["trade_date"] == latest_available_trade_date]
            if latest_perf.empty:
                latest_perf = perf_view.sort_values("trade_date", ascending=False).head(1)
            if not latest_perf.empty:
                perf_row = latest_perf.iloc[0]

        return self._summarize_real_chip_metrics(
            chips_df=latest_chips,
            perf_row=perf_row,
            latest_price=latest_price,
            source_label="tushare.cyq_chips/cyq_perf",
        )

    def _calculate_chip_peak_metrics(self, df):
        """基于历史 OHLCV 近似估算筹码峰结构。"""
        default_result = {
            "chip_data_source": "ohlcv_volume_profile_estimate",
            "chip_trade_date": "N/A",
            "chip_peak_shape": "N/A",
            "main_chip_peak_price": "N/A",
            "secondary_chip_peak_price": "N/A",
            "chip_concentration": "N/A",
            "average_chip_cost": "N/A",
            "cost_band_70": "N/A",
            "cost_band_90": "N/A",
            "current_price_position": "N/A",
            "upper_pressure_peak": "N/A",
            "lower_support_peak": "N/A",
            "profit_ratio_estimate": "N/A",
            "trap_ratio_estimate": "N/A",
        }

        if df is None or not hasattr(df, "empty") or df.empty:
            return default_result

        required_columns = {"High", "Low", "Close", "Volume"}
        if not required_columns.issubset(set(df.columns)):
            return default_result

        recent = df.loc[:, ["High", "Low", "Close", "Volume"]].replace([np.inf, -np.inf], np.nan).dropna().tail(120)
        if len(recent) < 20:
            return default_result

        price_low = float(recent["Low"].min())
        price_high = float(recent["High"].max())
        if not np.isfinite(price_low) or not np.isfinite(price_high) or price_high <= price_low:
            return default_result

        bin_count = 24
        bin_edges = np.linspace(price_low, price_high, bin_count + 1)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        volume_profile = np.zeros(bin_count, dtype=float)

        for _, row in recent.iterrows():
            low = float(row["Low"])
            high = float(row["High"])
            close = float(row["Close"])
            volume = float(row["Volume"])
            if not np.isfinite(volume) or volume <= 0:
                continue

            if not np.isfinite(low) or not np.isfinite(high) or high <= low:
                target_price = close if np.isfinite(close) else low
                index = int(np.clip(np.searchsorted(bin_edges, target_price, side="right") - 1, 0, bin_count - 1))
                volume_profile[index] += volume
                continue

            overlaps = np.maximum(0, np.minimum(bin_edges[1:], high) - np.maximum(bin_edges[:-1], low))
            overlap_sum = float(overlaps.sum())
            if overlap_sum <= 0:
                index = int(np.clip(np.searchsorted(bin_edges, close, side="right") - 1, 0, bin_count - 1))
                volume_profile[index] += volume
            else:
                volume_profile += volume * (overlaps / overlap_sum)

        total_volume = float(volume_profile.sum())
        if total_volume <= 0:
            return default_result

        cumulative_profile = np.cumsum(volume_profile) / total_volume

        def _band_price(target_ratio):
            band_index = int(np.clip(np.searchsorted(cumulative_profile, target_ratio, side="left"), 0, bin_count - 1))
            return float(bin_centers[band_index])

        peak_indexes = []
        for index, value in enumerate(volume_profile):
            left = volume_profile[index - 1] if index > 0 else value
            right = volume_profile[index + 1] if index < bin_count - 1 else value
            if value > 0 and value >= left and value >= right:
                peak_indexes.append(index)

        if not peak_indexes:
            peak_indexes = [int(np.argmax(volume_profile))]

        peak_indexes = sorted(set(peak_indexes), key=lambda idx: volume_profile[idx], reverse=True)
        main_peak_index = peak_indexes[0]

        secondary_peak_index = None
        for peak_index in peak_indexes[1:]:
            if abs(peak_index - main_peak_index) >= 2 and volume_profile[peak_index] >= volume_profile[main_peak_index] * 0.35:
                secondary_peak_index = peak_index
                break

        significant_peak_indexes = [idx for idx in peak_indexes if volume_profile[idx] >= volume_profile[main_peak_index] * 0.25]
        if len(significant_peak_indexes) <= 1:
            chip_peak_shape = "单峰密集"
        elif len(significant_peak_indexes) == 2:
            chip_peak_shape = "双峰博弈"
        else:
            chip_peak_shape = "多峰发散"

        neighborhood = slice(max(0, main_peak_index - 1), min(bin_count, main_peak_index + 2))
        concentration_ratio = float(volume_profile[neighborhood].sum() / total_volume)
        if concentration_ratio >= 0.5:
            concentration_level = "高"
        elif concentration_ratio >= 0.3:
            concentration_level = "中"
        else:
            concentration_level = "低"

        current_price = float(recent["Close"].iloc[-1])
        main_peak_price = float(bin_centers[main_peak_index])
        current_vs_peak_pct = ((current_price / main_peak_price) - 1) * 100 if main_peak_price else 0.0
        if current_vs_peak_pct >= 5:
            current_price_position = f"显著站上主峰 {current_vs_peak_pct:.1f}%"
        elif current_vs_peak_pct >= 1:
            current_price_position = f"略高于主峰 {current_vs_peak_pct:.1f}%"
        elif current_vs_peak_pct <= -5:
            current_price_position = f"显著跌破主峰 {abs(current_vs_peak_pct):.1f}%"
        elif current_vs_peak_pct <= -1:
            current_price_position = f"略低于主峰 {abs(current_vs_peak_pct):.1f}%"
        else:
            current_price_position = "贴近主峰震荡"

        upper_pressure_peak = "N/A"
        lower_support_peak = "N/A"
        for peak_index in sorted(peak_indexes, key=lambda idx: bin_centers[idx]):
            center_price = float(bin_centers[peak_index])
            if center_price > current_price and upper_pressure_peak == "N/A":
                upper_pressure_peak = round(center_price, 2)
            if center_price < current_price:
                lower_support_peak = round(center_price, 2)

        profit_ratio = float(volume_profile[bin_centers <= current_price].sum() / total_volume) * 100
        trap_ratio = max(0.0, 100 - profit_ratio)
        avg_cost = float(np.dot(bin_centers, volume_profile) / total_volume)
        cost_15pct = _band_price(0.15)
        cost_85pct = _band_price(0.85)
        cost_5pct = _band_price(0.05)
        cost_95pct = _band_price(0.95)

        return {
            "chip_data_source": "ohlcv_volume_profile_estimate",
            "chip_trade_date": self._extract_latest_trade_date(df) or "N/A",
            "chip_peak_shape": chip_peak_shape,
            "main_chip_peak_price": round(main_peak_price, 2),
            "secondary_chip_peak_price": round(float(bin_centers[secondary_peak_index]), 2) if secondary_peak_index is not None else "N/A",
            "chip_concentration": f"{concentration_level} ({concentration_ratio * 100:.1f}%)",
            "average_chip_cost": round(avg_cost, 2),
            "cost_band_70": f"{cost_15pct:.2f}-{cost_85pct:.2f}",
            "cost_band_90": f"{cost_5pct:.2f}-{cost_95pct:.2f}",
            "current_price_position": current_price_position,
            "upper_pressure_peak": upper_pressure_peak,
            "lower_support_peak": lower_support_peak,
            "profit_ratio_estimate": f"{profit_ratio:.1f}%",
            "trap_ratio_estimate": f"{trap_ratio:.1f}%",
        }
    
    def get_latest_indicators(self, df, symbol=None):
        """获取最新的技术指标值"""
        try:
            if isinstance(df, dict) and "error" in df:
                return df
                
            latest = df.iloc[-1]

            indicators = {
                "price": latest['Close'],
                "ma5": latest['MA5'],
                "ma10": latest['MA10'], 
                "ma20": latest['MA20'],
                "ma60": latest['MA60'],
                "rsi": latest['RSI'],
                "macd": latest['MACD'],
                "macd_signal": latest['MACD_signal'],
                "bb_upper": latest['BB_upper'],
                "bb_lower": latest['BB_lower'],
                "k_value": latest['K'],
                "d_value": latest['D'],
                "volume_ratio": latest['Volume_ratio']
            }
            real_chip_metrics = self._get_chip_peak_metrics_from_tushare(
                symbol=symbol,
                latest_price=self._coerce_float(latest.get("Close")) or 0.0,
                latest_trade_date=self._extract_latest_trade_date(df),
            )
            if real_chip_metrics:
                print(f"[Chip] {symbol or 'N/A'} 使用真实筹码分布: {real_chip_metrics.get('chip_data_source')} ({real_chip_metrics.get('chip_trade_date')})")
                indicators.update(real_chip_metrics)
            else:
                print(f"[Chip] {symbol or 'N/A'} 未获取到真实筹码分布，回退到OHLCV近似筹码峰")
                indicators.update(self._calculate_chip_peak_metrics(df))
            return indicators
        except Exception as e:
            return {"error": f"获取最新指标失败: {str(e)}"}
    
    def _fetch_financial_data_live(self, symbol):
        if self._is_chinese_stock(symbol):
            return self._get_chinese_financial_data(symbol)
        if self._is_hk_stock(symbol):
            return self._get_hk_financial_data(symbol)
        return self._get_us_financial_data(symbol)

    def get_financial_data(self, symbol, max_age_seconds=86400, allow_stale_on_failure=True, cache_first=True):
        """获取详细财务数据"""
        try:
            return self.cache_service.get_stock_financial(
                symbol=symbol,
                market=self._detect_market(symbol),
                fetch_fn=lambda: self._fetch_financial_data_live(symbol),
                max_age_seconds=max_age_seconds,
                allow_stale_on_failure=allow_stale_on_failure,
                cache_first=cache_first,
            )
        except Exception as e:
            return {"error": f"获取财务数据失败: {str(e)}"}
    
    def _get_chinese_financial_data(self, symbol):
        """获取中国股票财务数据"""
        financial_data = {
            "symbol": symbol,
            "balance_sheet": None,  # 资产负债表
            "income_statement": None,  # 利润表
            "cash_flow": None,  # 现金流量表
            "financial_ratios": {},  # 财务比率
            "quarter_data": None,  # 季度数据
        }
        
        try:
            from quarterly_report_data import QuarterlyReportDataFetcher

            quarterly = QuarterlyReportDataFetcher(cache_service=self.cache_service).get_quarterly_reports(symbol)
            if quarterly and quarterly.get("data_success"):
                if quarterly.get("balance_sheet"):
                    financial_data["balance_sheet"] = quarterly["balance_sheet"].get("data")
                if quarterly.get("income_statement"):
                    financial_data["income_statement"] = quarterly["income_statement"].get("data")
                if quarterly.get("cash_flow"):
                    financial_data["cash_flow"] = quarterly["cash_flow"].get("data")
                if quarterly.get("financial_indicators", {}).get("data"):
                    financial_data["financial_ratios"] = quarterly["financial_indicators"]["data"][0]
                financial_data["quarter_data"] = quarterly
            
            return financial_data
            
        except Exception as e:
            print(f"获取中国股票财务数据失败: {e}")
            return financial_data
    
    # 已删除 _get_quarter_data_from_wencai 方法
    # 季报数据现在统一由 quarterly_report_data.py 模块使用 Tushare 获取
    # 获取最近8期完整季报（利润表、资产负债表、现金流量表）
    # 避免重复获取，提高效率
    
    def _get_hk_financial_data(self, symbol):
        """获取港股财务数据"""
        hk_code = self._normalize_hk_code(symbol)
        yahoo_symbol = self._normalize_hk_yahoo_symbol(symbol)
        
        financial_data = {
            "symbol": hk_code,
            "balance_sheet": None,
            "income_statement": None,
            "cash_flow": None,
            "financial_ratios": {},
            "quarter_data": None,
            "data_source": "yfinance",
            "note": "港股财务数据来自 Yahoo Finance"
        }
        
        try:
            print(f"正在获取港股 {hk_code} 的财务指标...")
            try:
                ticker = yf.Ticker(yahoo_symbol)
                info = ticker.info or {}
                financial_data["financial_ratios"] = {
                    "市盈率": self._safe_convert(info.get("trailingPE") or info.get("forwardPE")),
                    "市净率": self._safe_convert(info.get("priceToBook")),
                    "总市值": self._safe_convert(info.get("marketCap")),
                    "股息率TTM": self._safe_convert(info.get("dividendYield")),
                    "每股收益": self._safe_convert(info.get("trailingEps")),
                    "每股净资产": self._safe_convert(info.get("bookValue")),
                    "营业收入": self._safe_convert(info.get("totalRevenue")),
                    "净利润": self._safe_convert(info.get("netIncomeToCommon")),
                }
                if any(value is not None for value in financial_data["financial_ratios"].values()):
                    print(f"✅ 成功获取港股 {hk_code} 的财务指标")
                else:
                    print(f"⚠️ 未获取到港股 {hk_code} 的财务指标数据")
                    financial_data["note"] = "未获取到财务数据"
                    
            except Exception as e:
                print(f"⚠️ 获取港股财务指标失败: {e}")
                financial_data["note"] = f"获取财务数据失败: {str(e)}"
            
            return financial_data
            
        except Exception as e:
            print(f"获取港股财务数据异常: {e}")
            financial_data["note"] = f"获取失败: {str(e)}"
            return financial_data
    
    def _get_us_financial_data(self, symbol):
        """获取美股财务数据"""
        financial_data = {
            "symbol": symbol,
            "balance_sheet": None,
            "income_statement": None,
            "cash_flow": None,
            "financial_ratios": {},
            "quarter_data": None,
        }
        
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # 1. 资产负债表
            try:
                balance_sheet = stock.balance_sheet
                if balance_sheet is not None and not balance_sheet.empty:
                    financial_data["balance_sheet"] = balance_sheet.iloc[:, :4].to_dict('index')
            except Exception as e:
                print(f"获取资产负债表失败: {e}")
            
            # 2. 利润表
            try:
                income_stmt = stock.income_stmt
                if income_stmt is not None and not income_stmt.empty:
                    financial_data["income_statement"] = income_stmt.iloc[:, :4].to_dict('index')
            except Exception as e:
                print(f"获取利润表失败: {e}")
            
            # 3. 现金流量表
            try:
                cash_flow = stock.cashflow
                if cash_flow is not None and not cash_flow.empty:
                    financial_data["cash_flow"] = cash_flow.iloc[:, :4].to_dict('index')
            except Exception as e:
                print(f"获取现金流量表失败: {e}")
            
            # 4. 财务比率（从info中提取）
            financial_data["financial_ratios"] = {
                "ROE": info.get('returnOnEquity'),
                "ROA": info.get('returnOnAssets'),
                "毛利率": info.get('grossMargins'),
                "营业利润率": info.get('operatingMargins'),
                "净利率": info.get('profitMargins'),
                "资产负债率": info.get('debtToEquity'),
                "流动比率": info.get('currentRatio'),
                "速动比率": info.get('quickRatio'),
                "EPS": info.get('trailingEps'),
                "每股账面价值": info.get('bookValue'),
                "股息率": info.get('dividendYield'),
                "派息率": info.get('payoutRatio'),
                "收入增长": info.get('revenueGrowth'),
                "盈利增长": info.get('earningsGrowth'),
            }
            
            return financial_data
            
        except Exception as e:
            print(f"获取美股财务数据失败: {e}")
            return financial_data
    
    # 已删除 get_fund_flow_data 方法（使用问财）
    # 资金流向数据现在统一由 fund_flow_data.py 模块使用 Tushare 获取
    # 获取近20个交易日的详细资金流向数据（主力、超大单、大单、中单、小单）
    # 避免重复获取，提高效率和数据质量
    # 
    # 删除说明：
    # - 删除了约160行代码
    # - 删除原因：重复获取，数据格式不规整，日期范围不准确
    # - 新方案：使用 Tushare moneyflow 接口
    # - 新方案优势：数据标准化、准确获取最近20个交易日、6类资金详细分类
    
    def get_risk_data(self, symbol):
        """
        获取股票风险数据（限售解禁、大股东减持、重要事件）
        只支持中国A股
        """
        try:
            # 只有中国A股才支持风险数据查询
            if not self._is_chinese_stock(symbol):
                return {
                    'symbol': symbol,
                    'data_success': False,
                    'error': '仅支持中国A股风险数据查询'
                }
            
            # 使用风险数据获取器
            from risk_data_fetcher import RiskDataFetcher
            fetcher = RiskDataFetcher()
            risk_data = fetcher.get_risk_data(symbol)
            
            return risk_data
            
        except Exception as e:
            return {
                'symbol': symbol,
                'data_success': False,
                'error': f'获取风险数据失败: {str(e)}'
            }
    
    def _safe_convert(self, value):
        """安全地转换数值"""
        if value is None or value == '' or (isinstance(value, float) and np.isnan(value)):
            return None
        try:
            if isinstance(value, str):
                # 移除百分号和逗号
                value = value.replace('%', '').replace(',', '')
                return float(value)
            return value
        except:
            return value
    
    def _calculate_main_fund_ratio(self, main_fund, total_fund):
        """计算主力资金占比"""
        try:
            if main_fund is not None and total_fund not in (None, 0):
                ratio = (main_fund / total_fund) * 100
                return f"{ratio:.2f}%"
        except:
            pass
        return None
