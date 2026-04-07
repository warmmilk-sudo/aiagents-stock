import yfinance as yf
import akshare as ak
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
    
    def _get_chinese_stock_info(self, symbol):
        """获取中国股票基本信息（支持akshare和tushare数据源自动切换）"""
        try:
            # 初始化基本信息
            info = {
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
            
            # 先尝试使用数据源管理器获取基本信息
            basic_info = self.data_source_manager.get_stock_basic_info(symbol)
            if basic_info:
                info.update(basic_info)
            
            if self.data_source_manager.tushare_available:
                try:
                    print(f"[Tushare] 正在获取 {symbol} 的估值补充信息...")
                    ts_code = self.data_source_manager._convert_to_ts_code(symbol)
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=10)).strftime('%Y%m%d')
                    df = self.data_source_manager.tushare_api.daily_basic(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        fields='ts_code,trade_date,pe,pb,total_mv'
                    )
                    if df is not None and not df.empty:
                        row = df.sort_values('trade_date', ascending=False).iloc[0]
                        info['pe_ratio'] = row.get('pe') if row.get('pe') not in (None, '') else info['pe_ratio']
                        info['pb_ratio'] = row.get('pb') if row.get('pb') not in (None, '') else info['pb_ratio']
                        info['market_cap'] = row.get('total_mv') if row.get('total_mv') not in (None, '') else info['market_cap']
                        print(f"[Tushare] ✅ 成功获取估值补充信息")
                except Exception as te:
                    print(f"[Tushare] ❌ 获取估值补充信息失败: {te}")
            else:
                try:
                    print(f"[Akshare] 正在获取 {symbol} 的个股详细信息...")
                    stock_info = ak.stock_individual_info_em(symbol=symbol)
                    if stock_info is not None and not stock_info.empty:
                        for _, row in stock_info.iterrows():
                            key = row['item']
                            value = row['value']
                            
                            if key == '股票简称':
                                info['name'] = value
                            elif key == '总市值':
                                try:
                                    if value and value != '-':
                                        info['market_cap'] = float(value)
                                except:
                                    pass
                            elif key == '市盈率-动态':
                                try:
                                    if value and value != '-':
                                        pe_value = float(value)
                                        if 0 < pe_value <= 1000:
                                            info['pe_ratio'] = pe_value
                                except:
                                    pass
                            elif key == '市净率':
                                try:
                                    if value and value != '-':
                                        pb_value = float(value)
                                        if 0 < pb_value <= 100:
                                            info['pb_ratio'] = pb_value
                                except:
                                    pass
                except Exception as e:
                    print(f"[Akshare] 获取个股详细信息失败: {e}")
            
            # 不再使用历史日线收盘价回填 current_price/change_percent。
            # 如果实时源不可用，这里保持 None，避免把收盘价误当成盘中现价。
            
            # 方法3: 使用百度估值数据获取市盈率和市净率
            if info['pe_ratio'] is None:
                try:
                    pe_data = ak.stock_zh_valuation_baidu(symbol=symbol, indicator="市盈率(TTM)")
                    if pe_data is not None and not pe_data.empty:
                        latest_pe = pe_data.iloc[-1]['value']
                        if latest_pe and latest_pe != '-':
                            pe_val = float(latest_pe)
                            if 0 < pe_val <= 1000:
                                info['pe_ratio'] = pe_val
                except Exception as e:
                    print(f"获取市盈率失败: {e}")
            
            if info['pb_ratio'] is None:
                try:
                    pb_data = ak.stock_zh_valuation_baidu(symbol=symbol, indicator="市净率")
                    if pb_data is not None and not pb_data.empty:
                        latest_pb = pb_data.iloc[-1]['value']
                        if latest_pb and latest_pb != '-':
                            pb_val = float(latest_pb)
                            if 0 < pb_val <= 100:
                                info['pb_ratio'] = pb_val
                except Exception as e:
                    print(f"获取市净率失败: {e}")
            
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
            # 规范化港股代码
            hk_code = self._normalize_hk_code(symbol)
            
            # 初始化基本信息
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
            
            # 方法1: 获取港股实时行情
            try:
                # 使用akshare获取港股实时数据
                realtime_df = ak.stock_hk_spot_em()
                if realtime_df is not None and not realtime_df.empty:
                    # 查找对应股票
                    stock_data = realtime_df[realtime_df['代码'] == hk_code]
                    if not stock_data.empty:
                        row = stock_data.iloc[0]
                        info['name'] = row.get('名称')
                        info['current_price'] = row.get('最新价')
                        info['change_percent'] = row.get('涨跌幅')
                        
                        # 市值（港元）
                        market_cap = row.get('总市值')
                        if market_cap is not None:
                            try:
                                info['market_cap'] = float(market_cap)
                            except:
                                pass
                        
                        # 市盈率
                        pe = row.get('市盈率')
                        if pe not in (None, '-'):
                            try:
                                pe_val = float(pe)
                                if 0 < pe_val <= 1000:
                                    info['pe_ratio'] = pe_val
                            except:
                                pass
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
        """获取中国股票历史数据（支持akshare和tushare数据源自动切换）"""
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
            
            # 使用数据源管理器获取数据（自动切换akshare和tushare）
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
            # 规范化港股代码
            hk_code = self._normalize_hk_code(symbol)
            
            # 计算日期范围
            end_date = datetime.now().strftime('%Y%m%d')
            if period == "1y":
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            elif period == "6mo":
                start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')
            elif period == "3mo":
                start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
            elif period == "1mo":
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            else:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            # 获取港股历史数据
            df = ak.stock_hk_hist(symbol=hk_code, period="daily", 
                                start_date=start_date, end_date=end_date, adjust=adjust)
            
            if df is not None and not df.empty:
                # 重命名列以匹配标准格式
                df = df.rename(columns={
                    '日期': 'Date',
                    '开盘': 'Open',
                    '收盘': 'Close',
                    '最高': 'High',
                    '最低': 'Low',
                    '成交量': 'Volume'
                })
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index('Date', inplace=True)
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
            # 1. 获取资产负债表
            try:
                balance_sheet = ak.stock_financial_abstract_ths(symbol=symbol, indicator="资产负债表")
                if balance_sheet is not None and not balance_sheet.empty:
                    financial_data["balance_sheet"] = balance_sheet.head(8).to_dict('records')
            except Exception as e:
                print(f"获取资产负债表失败: {e}")
            
            # 2. 获取利润表
            try:
                income_statement = ak.stock_financial_abstract_ths(symbol=symbol, indicator="利润表")
                if income_statement is not None and not income_statement.empty:
                    financial_data["income_statement"] = income_statement.head(8).to_dict('records')
            except Exception as e:
                print(f"获取利润表失败: {e}")
            
            # 3. 获取现金流量表
            try:
                cash_flow = ak.stock_financial_abstract_ths(symbol=symbol, indicator="现金流量表")
                if cash_flow is not None and not cash_flow.empty:
                    financial_data["cash_flow"] = cash_flow.head(8).to_dict('records')
            except Exception as e:
                print(f"获取现金流量表失败: {e}")
            
            # 4. 获取主要财务指标
            try:
                financial_abstract = ak.stock_financial_abstract(symbol=symbol)
                if financial_abstract is not None and not financial_abstract.empty:
                    # 提取关键财务指标
                    key_indicators = [
                        '净资产收益率(ROE)', '总资产报酬率(ROA)', '销售毛利率', '销售净利率',
                        '资产负债率', '流动比率', '速动比率', '存货周转率', '应收账款周转率',
                        '总资产周转率', '营业收入同比增长', '净利润同比增长'
                    ]
                    
                    # 筛选出包含关键指标的行
                    indicator_rows = financial_abstract[financial_abstract['指标'].isin(key_indicators)]
                    
                    if not indicator_rows.empty:
                        # 获取最新的报告期数据（第一列日期）
                        date_columns = [col for col in financial_abstract.columns if col not in ['选项', '指标']]
                        if date_columns:
                            latest_date = date_columns[0]  # 最新日期列
                            
                            # 构建财务比率字典
                            financial_ratios = {"报告期": latest_date}
                            
                            # 提取每个指标的最新值
                            for _, row in indicator_rows.iterrows():
                                indicator_name = row['指标']
                                value = row.get(latest_date)
                                if value is None or (isinstance(value, float) and pd.isna(value)):
                                    financial_ratios[indicator_name] = None
                                else:
                                    financial_ratios[indicator_name] = str(value)
                            
                            financial_data["financial_ratios"] = financial_ratios
            except Exception as e:
                print(f"获取财务指标失败: {e}")
            
            # 注意：季报数据现在由 quarterly_report_data.py 模块使用 akshare 获取（8期完整季报）
            # 不再使用问财获取季报，避免重复
            
            return financial_data
            
        except Exception as e:
            print(f"获取中国股票财务数据失败: {e}")
            return financial_data
    
    # 已删除 _get_quarter_data_from_wencai 方法
    # 季报数据现在统一由 quarterly_report_data.py 模块使用 akshare 获取
    # 获取最近8期完整季报（利润表、资产负债表、现金流量表）
    # 避免重复获取，提高效率
    
    def _get_hk_financial_data(self, symbol):
        """获取港股财务数据"""
        hk_code = self._normalize_hk_code(symbol)
        
        financial_data = {
            "symbol": hk_code,
            "balance_sheet": None,
            "income_statement": None,
            "cash_flow": None,
            "financial_ratios": {},
            "quarter_data": None,
            "data_source": "eastmoney",
            "note": "港股财务数据来自东方财富"
        }
        
        try:
            # 使用akshare获取港股财务指标（东方财富数据源）
            print(f"正在获取港股 {hk_code} 的财务指标...")
            try:
                financial_indicator = ak.stock_hk_financial_indicator_em(symbol=hk_code)
                
                if financial_indicator is not None and not financial_indicator.empty:
                    # 将财务指标数据转换为字典
                    indicator_dict = financial_indicator.iloc[0].to_dict()
                    
                    # 整理财务比率数据
                    financial_data["financial_ratios"] = {
                        "基本每股收益": self._safe_convert(indicator_dict.get('基本每股收益(元)')),
                        "每股净资产": self._safe_convert(indicator_dict.get('每股净资产(元)')),
                        "每股股息TTM": self._safe_convert(indicator_dict.get('每股股息TTM(港元)')),
                        "派息比率": self._safe_convert(indicator_dict.get('派息比率(%)')),
                        "每股经营现金流": self._safe_convert(indicator_dict.get('每股经营现金流(元)')),
                        "股息率TTM": self._safe_convert(indicator_dict.get('股息率TTM(%)')),
                        "总市值": self._safe_convert(indicator_dict.get('总市值(港元)')),
                        "港股市值": self._safe_convert(indicator_dict.get('港股市值(港元)')),
                        "营业总收入": self._safe_convert(indicator_dict.get('营业总收入')),
                        "营业收入环比增长": self._safe_convert(indicator_dict.get('营业总收入滚动环比增长(%)')),
                        "销售净利率": self._safe_convert(indicator_dict.get('销售净利率(%)')),
                        "净利润": self._safe_convert(indicator_dict.get('净利润')),
                        "净利润环比增长": self._safe_convert(indicator_dict.get('净利润滚动环比增长(%)')),
                        "ROE股东权益回报率": self._safe_convert(indicator_dict.get('股东权益回报率(%)')),
                        "市盈率": self._safe_convert(indicator_dict.get('市盈率')),
                        "市净率": self._safe_convert(indicator_dict.get('市净率')),
                        "ROA总资产回报率": self._safe_convert(indicator_dict.get('总资产回报率(%)')),
                        "法定股本": self._safe_convert(indicator_dict.get('法定股本(股)')),
                        "已发行股本": self._safe_convert(indicator_dict.get('已发行股本(股)')),
                        "每手股": self._safe_convert(indicator_dict.get('每手股')),
                    }
                    
                    print(f"✅ 成功获取港股 {hk_code} 的财务指标")
                    print(f"   ROE: {financial_data['financial_ratios']['ROE股东权益回报率']}")
                    print(f"   市盈率: {financial_data['financial_ratios']['市盈率']}")
                    print(f"   市净率: {financial_data['financial_ratios']['市净率']}")
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
    # 资金流向数据现在统一由 fund_flow_akshare.py 模块使用 akshare 获取
    # 获取近20个交易日的详细资金流向数据（主力、超大单、大单、中单、小单）
    # 避免重复获取，提高效率和数据质量
    # 
    # 删除说明：
    # - 删除了约160行代码
    # - 删除原因：重复获取，数据格式不规整，日期范围不准确
    # - 新方案：使用 akshare 的 stock_individual_fund_flow 接口
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
