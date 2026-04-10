"""
智能盯盘 - A股数据获取模块
实时行情优先使用 TDX，结构化日线与补充数据优先使用 Tushare。
盘中分析可强制使用 TDX。
"""

import logging
import os
import time
import pandas as pd
from typing import Dict, Optional
from datetime import datetime, timedelta
import config
from tushare_utils import create_tushare_pro


class SmartMonitorDataFetcher:
    """A股数据获取器（实时价优先 TDX，结构化日线优先 Tushare）"""
    
    def __init__(self, use_tdx: bool = None, tdx_base_url: str = None):
        """
        初始化数据获取器
        
        Args:
            use_tdx: 是否使用TDX数据源（可选，从配置读取）
            tdx_base_url: TDX接口地址（可选，从配置读取）
        """
        self.logger = logging.getLogger(__name__)
        self.intraday_tdx_retry_count = max(
            1,
            int(getattr(config, "SMART_MONITOR_INTRADAY_TDX_RETRY_COUNT", 3) or 3),
        )
        
        # TDX数据源配置
        if use_tdx is None:
            use_tdx = config.TDX_CONFIG.get('enabled', False)

        tdx_timeout_seconds = 10
        try:
            if tdx_base_url is None:
                tdx_base_url = config.TDX_CONFIG.get('base_url', '')
            tdx_timeout_seconds = int(config.TDX_TIMEOUT_SECONDS or tdx_timeout_seconds)
        except Exception:
            if tdx_base_url is None:
                tdx_base_url = ''

        self.use_tdx = use_tdx
        self.tdx_fetcher = None
        
        if self.use_tdx:
            try:
                if not str(tdx_base_url or '').strip():
                    raise ValueError("TDX_BASE_URL 未配置")
                from smart_monitor_tdx_data import SmartMonitorTDXDataFetcher
                candidate_fetcher = SmartMonitorTDXDataFetcher(
                    base_url=tdx_base_url,
                    timeout_seconds=max(5, tdx_timeout_seconds),
                )
                if getattr(candidate_fetcher, 'available', True):
                    self.tdx_fetcher = candidate_fetcher
                    self.logger.info(f"TDX数据源已启用: {tdx_base_url}")
                else:
                    self.logger.warning(f"TDX数据源不可达: {tdx_base_url}，将降级到补充数据源")
                    self.use_tdx = False
            except Exception as e:
                self.logger.warning(f"TDX数据源初始化失败: {e}，将降级到补充数据源")
                self.use_tdx = False
        else:
            self.logger.info("TDX数据源未启用，实时行情将使用补充数据源")
        
        # 初始化 Tushare（结构化主数据源）
        self.ts_pro = None
        tushare_token = os.getenv('TUSHARE_TOKEN', '')
        
        if tushare_token:
            try:
                self.ts_pro, tushare_url = create_tushare_pro(token=tushare_token)
                self.logger.info(f"Tushare数据源初始化成功，地址: {tushare_url}")
            except Exception as e:
                self.logger.warning(f"Tushare初始化失败: {e}")


        else:
            self.logger.info("未配置Tushare Token，将仅使用补充数据源")

    def _build_precision_error(self, message: str, stock_code: str) -> Dict:
        return {
            "precision_status": "failed",
            "precision_mode": "tdx_quote_tushare_daily",
            "precision_error": f"{stock_code} {message}",
            "data_source": "tdx",
            "tdx_retry_count": self.intraday_tdx_retry_count,
        }

    @staticmethod
    def _merge_quote_and_indicators(quote: Optional[Dict], indicators: Optional[Dict]) -> Dict:
        """Prefer realtime quote fields when merging indicator payloads."""
        result: Dict = {}
        if indicators:
            result.update(indicators)
        if quote:
            result.update(quote)
        return result

    def _attach_tdx_intraday_context(self, stock_code: str, result: Optional[Dict]) -> Dict:
        if not result or not (self.use_tdx and self.tdx_fetcher):
            return result
        try:
            intraday_context = self.tdx_fetcher.get_intraday_context(stock_code)
            if intraday_context:
                result["intraday_context"] = intraday_context
        except Exception as exc:
            self.logger.warning("[%s] 注入TDX盘中特征失败: %s", stock_code, exc)
        return result

    def _log_timed_stage(self, stock_code: str, stage_name: str, started_at: float, success: bool) -> float:
        elapsed = time.perf_counter() - started_at
        self.logger.info(
            "[%s] %s完成，耗时 %.2fs，success=%s",
            stock_code,
            stage_name,
            elapsed,
            success,
        )
        return elapsed

    def _call_tdx_with_retry(self, stock_code: str, operation_label: str, callback):
        last_error = ""
        for attempt in range(1, self.intraday_tdx_retry_count + 1):
            try:
                payload = callback()
                if payload:
                    if isinstance(payload, dict):
                        payload.setdefault("data_source", "tdx")
                        payload[f"tdx_{operation_label}_retry_attempts"] = attempt
                    return payload
                last_error = "empty_response"
                self.logger.warning(
                    "盘中分析要求使用TDX，%s %s 未返回有效数据 (%s/%s)",
                    stock_code,
                    operation_label,
                    attempt,
                    self.intraday_tdx_retry_count,
                )
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self.logger.warning(
                    "盘中分析要求使用TDX，%s %s 获取异常 (%s/%s): %s",
                    stock_code,
                    operation_label,
                    attempt,
                    self.intraday_tdx_retry_count,
                    exc,
                )

            if attempt < self.intraday_tdx_retry_count:
                time.sleep(1)

        self.logger.error(
            "盘中分析TDX %s 最终失败 %s，已重试 %s 次，最后错误: %s",
            operation_label,
            stock_code,
            self.intraday_tdx_retry_count,
            last_error or "unknown_error",
        )
        return None

    def _get_intraday_tdx_comprehensive_data(self, stock_code: str) -> Dict:
        total_started_at = time.perf_counter()
        if not (self.use_tdx and self.tdx_fetcher):
            self.logger.warning(
                "[%s] 盘中严格模式综合数据获取失败，耗时 %.2fs，原因=TDX未启用或不可用",
                stock_code,
                time.perf_counter() - total_started_at,
            )
            return self._build_precision_error("盘中分析必须使用TDX数据，但当前TDX未启用或不可用。", stock_code)

        quote_started_at = time.perf_counter()
        quote = self._call_tdx_with_retry(
            stock_code,
            "quote",
            lambda: self.tdx_fetcher.get_realtime_quote(stock_code),
        )
        self._log_timed_stage(stock_code, "盘中严格模式TDX实时行情获取", quote_started_at, bool(quote))
        if not quote:
            self.logger.warning(
                "[%s] 盘中严格模式综合数据获取失败，耗时 %.2fs，原因=TDX实时行情连续失败",
                stock_code,
                time.perf_counter() - total_started_at,
            )
            return self._build_precision_error(
                f"盘中分析必须使用TDX数据，实时行情连续{self.intraday_tdx_retry_count}次获取失败。",
                stock_code,
            )

        indicators_started_at = time.perf_counter()
        indicators = self._get_technical_indicators_from_tushare(stock_code, "daily")
        self._log_timed_stage(stock_code, "盘中严格模式Tushare日线指标获取", indicators_started_at, bool(indicators))
        if not indicators:
            self.logger.warning(
                "[%s] 盘中严格模式综合数据获取失败，耗时 %.2fs，原因=Tushare日线指标缺失",
                stock_code,
                time.perf_counter() - total_started_at,
            )
            return self._build_precision_error(
                "盘中分析必须使用Tushare日线技术指标，但Tushare未返回有效数据。",
                stock_code,
            )

        result = self._merge_quote_and_indicators(quote, indicators)
        result = self._attach_tdx_intraday_context(stock_code, result)
        result.setdefault("technical_data_source", "tushare")
        result["precision_status"] = "validated"
        result["precision_mode"] = "tdx_quote_tushare_daily"
        result["tdx_retry_count"] = self.intraday_tdx_retry_count
        self.logger.info(
            "[%s] 盘中严格模式综合数据获取完成，耗时 %.2fs，precision_status=%s",
            stock_code,
            time.perf_counter() - total_started_at,
            result["precision_status"],
        )
        return result

    def _stock_code_to_ts_code(self, stock_code: str) -> Optional[str]:
        """将A股代码转换为Tushare代码。"""
        if stock_code.startswith('6'):
            return f"{stock_code}.SH"
        if stock_code.startswith(('0', '3')):
            return f"{stock_code}.SZ"
        return None

    def _fetch_tushare_daily_history(self, stock_code: str, days: int = 400) -> Optional[pd.DataFrame]:
        if not self.ts_pro:
            return None

        ts_code = self._stock_code_to_ts_code(stock_code)
        if not ts_code:
            self.logger.error(f"无法转换Tushare代码 {stock_code}")
            return None

        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

        try:
            try:
                df = self.ts_pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj='qfq',
                )
                adjustment_mode = "qfq"
            except TypeError:
                df = self.ts_pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                )
                adjustment_mode = "raw"

            if df is None or df.empty:
                self.logger.error(f"Tushare未返回 {stock_code} 的历史数据")
                return None

            required_raw_cols = ["trade_date", "open", "high", "low", "close", "vol"]
            missing_raw_cols = [col for col in required_raw_cols if col not in df.columns]
            if missing_raw_cols:
                self.logger.error(f"Tushare数据缺少列 {stock_code}: {missing_raw_cols}")
                return None

            df = df.copy()
            for col in ["open", "high", "low", "close", "vol", "amount"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.dropna(subset=["trade_date", "open", "high", "low", "close", "vol"])
            if df.empty:
                self.logger.error(f"Tushare历史数据清洗后为空 {stock_code}")
                return None

            df = (
                df.sort_values("trade_date", ascending=True)
                .drop_duplicates(subset=["trade_date"], keep="last")
                .reset_index(drop=True)
            )

            df = df.rename(columns={
                "trade_date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "vol": "成交量",
                "amount": "成交额",
            })
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
            df = df.dropna(subset=["日期", "开盘", "最高", "最低", "收盘", "成交量"])
            if len(df) < 60:
                self.logger.warning(f"Tushare历史数据不足 {stock_code}（仅{len(df)}条）")
                return None

            self.logger.info(
                "✅ Tushare成功获取 %s 历史数据，共%s条，复权模式: %s",
                stock_code,
                len(df),
                adjustment_mode,
            )
            return df
        except Exception as e:
            self.logger.error(f"Tushare获取历史数据失败 {stock_code}: {type(e).__name__}: {str(e)}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return None

    def _resolve_stock_name(self, stock_code: str) -> Optional[str]:
        """解析股票名称，优先 Tushare。"""
        if self.ts_pro:
            try:
                ts_code = self._stock_code_to_ts_code(stock_code)
                if ts_code:
                    stock_basic = self.ts_pro.stock_basic(ts_code=ts_code, fields='name')
                    if stock_basic is not None and not stock_basic.empty:
                        return stock_basic.iloc[0]['name']
            except Exception as e:
                self.logger.warning(f"Tushare获取股票名称失败 {stock_code}: {type(e).__name__}: {str(e)[:80]}")

        return None
    
    def get_realtime_quote(self, stock_code: str, retry: int = 1) -> Optional[Dict]:
        """
        获取实时行情。
        优先使用TDX，不再用日线数据冒充实时数据。
        
        Args:
            stock_code: 股票代码（如：600519）
            retry: 重试次数（默认1次，避免IP封禁）
            
        Returns:
            实时行情数据
        """
        import math
        
        # 方法1: 尝试使用TDX（如果启用）
        if self.use_tdx and self.tdx_fetcher:
            try:
                quote = self.tdx_fetcher.get_realtime_quote(stock_code)
                if isinstance(quote, dict) and quote:
                    current_price = quote.get("current_price", quote.get("price"))
                    try:
                        price_value = float(current_price)
                    except (TypeError, ValueError):
                        price_value = 0.0
                    if math.isfinite(price_value) and price_value > 0:
                        normalized_quote = dict(quote)
                        normalized_quote["current_price"] = price_value
                        normalized_quote["price"] = price_value
                        return normalized_quote
                    self.logger.warning(f"TDX返回无效价格 {stock_code}")
                else:
                    self.logger.warning(f"TDX获取失败 {stock_code}")
            except Exception as e:
                self.logger.warning(f"TDX获取异常 {stock_code}: {e}")

        self.logger.error(f"TDX不可用，无法获取 {stock_code} 的实时行情")
        return None
    
    def get_technical_indicators(self, stock_code: str, period: str = 'daily', retry: int = 1) -> Optional[Dict]:
        """
        计算技术指标。
        日线优先使用Tushare，非日线周期优先使用TDX。

        Args:
            stock_code: 股票代码
            period: 周期（daily/weekly/monthly）
            retry: 重试次数（默认1次）
            
        Returns:
            技术指标数据
        """
        import time
        
        normalized_period = str(period or "daily").strip().lower()

        if normalized_period == "daily" and self.ts_pro:
            indicators = self._get_technical_indicators_from_tushare(stock_code, normalized_period)
            if indicators:
                return indicators
            self.logger.error(f"Tushare日线技术指标获取失败 {stock_code}，不再使用其他数据源冒充日线数据")
            return None

        # 方法1: 尝试使用TDX（如果启用）
        if self.use_tdx and self.tdx_fetcher:
            try:
                indicators = self.tdx_fetcher.get_technical_indicators(stock_code, normalized_period)
                if indicators:
                    return indicators
                else:
                    self.logger.warning(f"TDX计算技术指标失败 {stock_code}")
            except Exception as e:
                self.logger.warning(f"TDX计算技术指标异常 {stock_code}: {e}")

        if self.ts_pro and normalized_period == "daily":
            return self._get_technical_indicators_from_tushare(stock_code, normalized_period)

        self.logger.error(f"无法获取 {stock_code} 技术指标，当前周期={normalized_period}")
        return None
    
    def _calculate_all_indicators(self, df: pd.DataFrame, stock_code: str) -> Optional[Dict]:
        """
        根据历史数据计算所有技术指标
        
        Args:
            df: 历史数据DataFrame
            stock_code: 股票代码
            
        Returns:
            技术指标数据
        """
        try:
            if df.empty or len(df) < 60:
                self.logger.warning(f"股票 {stock_code} 历史数据不足")
                return None
            
            # 计算均线
            df['ma5'] = df['收盘'].rolling(window=5).mean()
            df['ma20'] = df['收盘'].rolling(window=20).mean()
            df['ma60'] = df['收盘'].rolling(window=60).mean()
            
            # 计算MACD
            df = self._calculate_macd(df)
            
            # 计算RSI
            df = self._calculate_rsi(df, periods=[6, 12, 24])
            
            # 计算KDJ
            df = self._calculate_kdj(df)
            
            # 计算布林带
            df = self._calculate_bollinger(df)
            
            # 计算量能均线
            df['vol_ma5'] = df['成交量'].rolling(window=5).mean()
            df['vol_ma10'] = df['成交量'].rolling(window=10).mean()
            
            # 取最后一行数据
            latest = df.iloc[-1]
            
            # 判断趋势
            current_price = float(latest['收盘'])
            ma5 = float(latest['ma5'])
            ma20 = float(latest['ma20'])
            ma60 = float(latest['ma60'])
            
            if current_price > ma5 > ma20 > ma60:
                trend = 'up'
            elif current_price < ma5 < ma20 < ma60:
                trend = 'down'
            else:
                trend = 'sideways'
            
            # 布林带位置
            boll_upper = float(latest['boll_upper'])
            boll_mid = float(latest['boll_mid'])
            boll_lower = float(latest['boll_lower'])
            
            if current_price >= boll_upper:
                boll_position = '上轨附近（超买）'
            elif current_price <= boll_lower:
                boll_position = '下轨附近（超卖）'
            elif current_price > boll_mid:
                boll_position = '中轨上方'
            else:
                boll_position = '中轨下方'
                
            # --- 语义化标签生成 (Semantic Labels) ---
            semantic_labels = []
            
            # 1. 均线形态
            if trend == 'up':
                semantic_labels.append("均线多头排列")
            elif trend == 'down':
                semantic_labels.append("均线空头排列")
                
            # 2. MACD形态
            macd = float(latest['macd'])
            dif = float(latest['dif'])
            dea = float(latest['dea'])
            prev_macd = float(df.iloc[-2]['macd'])
            prev_dif = float(df.iloc[-2]['dif'])
            if dif > 0 and dea > 0 and macd > 0 and prev_macd <= 0:
                semantic_labels.append("MACD水上金叉")
            elif dif < 0 and dea < 0 and macd > 0 and prev_macd <= 0:
                semantic_labels.append("MACD水下金叉(反弹概率大)")
            elif macd < 0 and prev_macd >= 0:
                semantic_labels.append("MACD高位死叉")
                
            # 3. KDJ超买超卖
            kdj_k = float(latest['kdj_k'])
            kdj_j = float(latest['kdj_j'])
            if kdj_j > 100 or kdj_k > 80:
                semantic_labels.append("KDJ严重超买(风险变大)")
            elif kdj_j < 0 or kdj_k < 20:
                semantic_labels.append("KDJ严重超卖(具备反弹条件)")
                
            # 4. 布林带极端
            if current_price > boll_upper * 1.02:
                semantic_labels.append("强势突破布林上轨")
            elif current_price < boll_lower * 0.98:
                semantic_labels.append("跌出布林下轨极限范围")
            
            return {
                'ma5': ma5,
                'ma20': ma20,
                'ma60': ma60,
                'trend': trend,
                'macd_dif': float(latest['dif']),
                'macd_dea': float(latest['dea']),
                'macd': float(latest['macd']),
                'rsi6': float(latest['rsi6']),
                'rsi12': float(latest['rsi12']),
                'rsi24': float(latest['rsi24']),
                'kdj_k': float(latest['kdj_k']),
                'kdj_d': float(latest['kdj_d']),
                'kdj_j': float(latest['kdj_j']),
                'boll_upper': boll_upper,
                'boll_mid': boll_mid,
                'boll_lower': boll_lower,
                'boll_position': boll_position,
                'vol_ma5': float(latest['vol_ma5']),
                'volume_ratio_vs_vol_ma5': float(latest['成交量']) / float(latest['vol_ma5']) if latest['vol_ma5'] > 0 else None,
                'semantic_labels': semantic_labels  # 新增语义标签
            }
            
        except Exception as e:
            self.logger.error(f"计算技术指标失败 {stock_code}: {e}")
            return None
    
    def _get_technical_indicators_from_tushare(self, stock_code: str, period: str = 'daily') -> Optional[Dict]:
        """
        使用Tushare获取历史数据并计算技术指标
        
        Args:
            stock_code: 股票代码（6位）
            period: 周期（daily/weekly/monthly）
            
        Returns:
            技术指标数据
        """
        try:
            if str(period or "daily").strip().lower() != "daily":
                self.logger.warning(f"Tushare日线技术指标仅支持 daily，当前周期={period}")
                return None

            df = self._fetch_tushare_daily_history(stock_code)
            if df is None:
                return None

            indicators = self._calculate_all_indicators(df, stock_code)
            if indicators:
                indicators["technical_data_source"] = "tushare"
                indicators["technical_period"] = "daily"
            return indicators
        except Exception as e:
            self.logger.error(f"Tushare获取历史数据失败 {stock_code}: {type(e).__name__}: {str(e)}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return None
    
    def get_main_force_flow(self, stock_code: str, retry: int = 2) -> Optional[Dict]:
        """
        获取主力资金流向（带重试机制）
        
        Args:
            stock_code: 股票代码
            retry: 重试次数（默认2次）
            
        Returns:
            主力资金数据
        """
        import time
        
        if self.ts_pro:
            tushare_result = self._get_main_force_from_tushare(stock_code)
            if tushare_result:
                return tushare_result

        self.logger.error(f"Tushare未返回有效数据，无法获取 {stock_code} 资金流向")
        return None
    
    def get_comprehensive_data(self, stock_code: str, intraday_strict: bool = False) -> Dict:
        """
        获取综合数据（实时行情+技术指标）
        注意：已移除主力资金流向数据，因为该接口不稳定且AI决策不依赖此数据
        注意：盘中决策暂不自动注入大盘/板块上下文，避免把非实时数据误当作实时背景
        
        Args:
            stock_code: 股票代码
            intraday_strict: 盘中严格模式，强制只使用TDX
            
        Returns:
            综合数据
        """
        total_started_at = time.perf_counter()
        mode_label = "intraday_strict" if intraday_strict else "best_effort"
        self.logger.info("[%s] 开始获取综合数据，mode=%s", stock_code, mode_label)
        if intraday_strict:
            result = self._get_intraday_tdx_comprehensive_data(stock_code)
            self.logger.info(
                "[%s] 综合数据获取结束，mode=%s，耗时 %.2fs，success=%s，precision_status=%s",
                stock_code,
                mode_label,
                time.perf_counter() - total_started_at,
                bool(result),
                (result or {}).get("precision_status"),
            )
            return result

        # 实时行情
        quote_started_at = time.perf_counter()
        quote = self.get_realtime_quote(stock_code)
        quote_elapsed = self._log_timed_stage(stock_code, "实时行情获取", quote_started_at, bool(quote))
        
        # 技术指标
        indicators_started_at = time.perf_counter()
        indicators = self.get_technical_indicators(stock_code)
        indicators_elapsed = self._log_timed_stage(stock_code, "技术指标获取", indicators_started_at, bool(indicators))
        result = self._merge_quote_and_indicators(quote, indicators)
        
        # 主力资金（已禁用 - 接口不稳定）
        # main_force = self.get_main_force_flow(stock_code)
        # if main_force:
        #     result['main_force'] = main_force

        if result:
            result.setdefault("precision_status", "best_effort")
            result.setdefault("precision_mode", "fallback_allowed")
            result = self._attach_tdx_intraday_context(stock_code, result)
        self.logger.info(
            "[%s] 综合数据获取结束，mode=%s，耗时 %.2fs，quote=%.2fs，indicators=%.2fs，success=%s，precision_status=%s",
            stock_code,
            mode_label,
            time.perf_counter() - total_started_at,
            quote_elapsed,
            indicators_elapsed,
            bool(result),
            (result or {}).get("precision_status"),
        )
        
        return result
    
    # ========== 技术指标计算方法 ==========
    
    def _calculate_macd(self, df: pd.DataFrame, 
                       fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """计算MACD指标"""
        ema_fast = df['收盘'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['收盘'].ewm(span=slow, adjust=False).mean()
        
        df['dif'] = ema_fast - ema_slow
        df['dea'] = df['dif'].ewm(span=signal, adjust=False).mean()
        df['macd'] = (df['dif'] - df['dea']) * 2
        
        return df
    
    def _calculate_rsi(self, df: pd.DataFrame, periods: list = [6, 12, 24]) -> pd.DataFrame:
        """计算RSI指标"""
        for period in periods:
            delta = df['收盘'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            df[f'rsi{period}'] = 100 - (100 / (1 + rs))
        
        return df
    
    def _calculate_kdj(self, df: pd.DataFrame, n: int = 9, 
                      m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """计算KDJ指标"""
        low_list = df['最低'].rolling(window=n).min()
        high_list = df['最高'].rolling(window=n).max()
        
        rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
        
        df['kdj_k'] = rsv.ewm(com=m1-1, adjust=False).mean()
        df['kdj_d'] = df['kdj_k'].ewm(com=m2-1, adjust=False).mean()
        df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']
        
        return df
    
    def _calculate_bollinger(self, df: pd.DataFrame, 
                           period: int = 20, std_num: int = 2) -> pd.DataFrame:
        """计算布林带"""
        df['boll_mid'] = df['收盘'].rolling(window=period).mean()
        std = df['收盘'].rolling(window=period).std()
        
        df['boll_upper'] = df['boll_mid'] + std_num * std
        df['boll_lower'] = df['boll_mid'] - std_num * std
        
        return df


    # ========== Tushare备用数据源方法（非实时兼容） ==========
    
    def _get_realtime_quote_from_tushare(self, stock_code: str) -> Optional[Dict]:
        """已废弃：Tushare 不再用于实时行情。"""
        return None
    
    def _get_main_force_from_tushare(self, stock_code: str) -> Optional[Dict]:
        """
        从Tushare获取主力资金流向（备用数据源）
        注意：资金流向接口需要较高积分
        
        Args:
            stock_code: 股票代码
            
        Returns:
            主力资金数据
        """
        try:
            # 转换股票代码格式
            if stock_code.startswith('6'):
                ts_code = f"{stock_code}.SH"
            elif stock_code.startswith(('0', '3')):
                ts_code = f"{stock_code}.SZ"
            else:
                return None
            
            # 尝试获取资金流向数据（需要120积分）
            today = datetime.now().strftime('%Y%m%d')
            df = self.ts_pro.moneyflow(ts_code=ts_code, start_date=today, end_date=today)
            
            if df.empty:
                # 获取最近一个交易日
                df = self.ts_pro.moneyflow(ts_code=ts_code, end_date=today)
                df = df.head(1)
            
            if df.empty:
                self.logger.warning(f"Tushare未找到股票 {stock_code} 的资金流向数据")
                return None
            
            row = df.iloc[0]
            
            # 计算主力净额（大单+超大单）
            required_fields = (
                'buy_lg_amount',
                'buy_elg_amount',
                'sell_lg_amount',
                'sell_elg_amount',
                'net_mf_amount',
                'buy_md_amount',
                'sell_md_amount',
                'buy_sm_amount',
                'sell_sm_amount',
            )
            if any(field not in row.index or pd.isna(row[field]) for field in required_fields):
                return None

            buy_lg_amount = float(row['buy_lg_amount'])
            buy_elg_amount = float(row['buy_elg_amount'])
            sell_lg_amount = float(row['sell_lg_amount'])
            sell_elg_amount = float(row['sell_elg_amount'])
            
            main_net = (buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount) / 10000
            
            # 计算净占比
            net_mf_amount = float(row['net_mf_amount'])
            if net_mf_amount == 0:
                return None
            main_net_pct = (main_net / net_mf_amount * 100)
            
            # 判断主力动向
            if main_net > 0 and main_net_pct > 5:
                trend = '大幅流入'
            elif main_net > 0:
                trend = '小幅流入'
            elif main_net < 0 and main_net_pct < -5:
                trend = '大幅流出'
            elif main_net < 0:
                trend = '小幅流出'
            else:
                trend = '观望'
            
            self.logger.info(f"✅ Tushare降级成功，获取到 {stock_code} 资金流向")
            
            return {
                'main_net': main_net,
                'main_net_pct': main_net_pct,
                'super_net': (buy_elg_amount - sell_elg_amount) / 10000,
                'big_net': (buy_lg_amount - sell_lg_amount) / 10000,
                'mid_net': float(row['buy_md_amount'] - row['sell_md_amount']) / 10000,
                'small_net': float(row['buy_sm_amount'] - row['sell_sm_amount']) / 10000,
                'trend': trend
            }
            
        except Exception as e:
            error_msg = str(e)
            if "权限" in error_msg or "积分" in error_msg:
                self.logger.warning(f"⚠️ Tushare资金流向接口需要120积分，当前积分不足")
                self.logger.info("💡 获取积分方法：")
                self.logger.info("   1. 完善个人信息 +100积分")
                self.logger.info("   2. 每日签到累积 +30积分（30天）")
                self.logger.info("   3. 参与社区互动获得积分")
                self.logger.info("   详情: https://tushare.pro/document/1?doc_id=13")
                self.logger.info("   智能盯盘会继续运行，仅缺少资金流向数据")
            else:
                self.logger.error(f"Tushare获取资金流向失败 {stock_code}: {error_msg[:100]}")
            return None


if __name__ == '__main__':
    # 测试代码
    logging.basicConfig(level=logging.INFO)
    
    fetcher = SmartMonitorDataFetcher()
    
    # 测试贵州茅台
    print("测试获取贵州茅台(600519)数据...")
    data = fetcher.get_comprehensive_data('600519')
    
    if data:
        print("\n实时行情:")
        print(f"  当前价: {data.get('current_price')} 元")
        print(f"  涨跌幅: {data.get('change_pct')}%")
        
        print("\n技术指标:")
        print(f"  MA5: {data.get('ma5', 0):.2f}")
        print(f"  MA20: {data.get('ma20', 0):.2f}")
        print(f"  MACD: {data.get('macd', 0):.4f}")
        print(f"  RSI(6): {data.get('rsi6', 0):.2f}")
        
        if 'main_force' in data:
            print("\n主力资金:")
            print(f"  主力净额: {data['main_force']['main_net']:.2f}万")
            print(f"  主力动向: {data['main_force']['trend']}")
