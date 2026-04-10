"""
智能盯盘 - DeepSeek AI 决策引擎
适配A股T+1交易规则的AI决策系统
"""

import ast
import json
import logging
import math
import re
from typing import Any, Dict, List, Optional
from datetime import datetime, time
import time as time_module

import pytz
import requests

import config
from investment_db_utils import DEFAULT_ACCOUNT_NAME
from model_routing import ModelTier, resolve_model_name
from prompt_registry import build_messages, render_prompt


class SmartMonitorDeepSeek:
    """A股智能盯盘 - DeepSeek AI决策引擎"""

    SYSTEM_TEMPLATE = "smart_monitor/intraday_decision.system.txt"
    USER_TEMPLATE = "smart_monitor/intraday_decision.user.txt"
    SECTION_TIMER_TEMPLATE = "smart_monitor/sections/timer.txt"
    SECTION_DATA_SCOPE_TEMPLATE = "smart_monitor/sections/data_scope.txt"
    SECTION_REALTIME_FRESHNESS_TEMPLATE = "smart_monitor/sections/realtime_freshness.txt"
    SECTION_STOCK_TEMPLATE = "smart_monitor/sections/stock.txt"
    SECTION_TECHNICAL_TEMPLATE = "smart_monitor/sections/technical.txt"
    SECTION_VOLUME_TEMPLATE = "smart_monitor/sections/volume.txt"
    SECTION_EXECUTION_CONTEXT_TEMPLATE = "smart_monitor/sections/execution_context.txt"
    SECTION_ACCOUNT_RISK_PROFILE_TEMPLATE = "smart_monitor/sections/account_risk_profile.txt"
    SECTION_INTRADAY_FLOW_TEMPLATE = "smart_monitor/sections/intraday_flow.txt"
    SECTION_STRATEGY_CONTEXT_TEMPLATE = "smart_monitor/sections/strategy_context.txt"
    SECTION_AI_PATTERN_RECOGNITION_TEMPLATE = "smart_monitor/sections/ai_pattern_recognition.txt"
    SECTION_POSITION_HOLDING_TEMPLATE = "smart_monitor/sections/position_holding.txt"
    SECTION_POSITION_EMPTY_TEMPLATE = "smart_monitor/sections/position_empty.txt"

    def __init__(self, api_key: str, model: str = None,
                 lightweight_model: str = None, reasoning_model: str = None):
        """
        初始化DeepSeek客户端
        
        Args:
            api_key: DeepSeek API密钥
        """
        self.api_key = api_key
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.base_url = config.DEEPSEEK_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.logger = logging.getLogger(__name__)
        self.http_timeout_seconds = max(
            15,
            int(getattr(config, "SMART_MONITOR_HTTP_TIMEOUT_SECONDS", 30) or 30),
        )
        self.http_retry_count = max(
            0,
            int(getattr(config, "SMART_MONITOR_HTTP_RETRY_COUNT", 1) or 1),
        )
        self.reasoning_max_tokens = max(
            1500,
            int(getattr(config, "SMART_MONITOR_REASONING_MAX_TOKENS", 3000) or 3000),
        )

    def set_model_overrides(self, model: str = None,
                            lightweight_model: str = None,
                            reasoning_model: str = None) -> None:
        """更新当前会话的模型覆盖配置。"""
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model

    @staticmethod
    def _resolve_risk_profile(risk_profile: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        defaults = dict(config.get_smart_monitor_risk_defaults())
        payload = risk_profile or {}

        def clamp(value: Any, minimum: int, maximum: int, fallback: int) -> int:
            try:
                numeric = int(value)
            except (TypeError, ValueError):
                numeric = fallback
            return max(minimum, min(maximum, numeric))

        return {
            "position_size_pct": clamp(payload.get("position_size_pct"), 0, 100, defaults["position_size_pct"]),
            "total_position_pct": clamp(payload.get("total_position_pct"), 0, 100, defaults["total_position_pct"]),
            "stop_loss_pct": clamp(payload.get("stop_loss_pct"), 0, 100, defaults["stop_loss_pct"]),
            "take_profit_pct": clamp(payload.get("take_profit_pct"), 0, 100, defaults["take_profit_pct"]),
        }

    def is_trading_time(self) -> bool:
        """
        判断当前是否在A股交易时间内
        
        Returns:
            bool: 是否可以交易
        """
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(beijing_tz)
        current_time = now.time()
        
        # 排除周末
        if now.weekday() >= 5:
            return False
        
        # 上午：9:30-11:30
        morning_start = time(9, 30)
        morning_end = time(11, 30)
        
        # 下午：13:00-15:00
        afternoon_start = time(13, 0)
        afternoon_end = time(15, 0)
        
        is_trading = (
            (morning_start <= current_time <= morning_end) or
            (afternoon_start <= current_time <= afternoon_end)
        )
        
        return is_trading

    def get_trading_session(self) -> Dict:
        """
        获取当前交易时段信息（A股版本）
        
        Returns:
            Dict: 时段信息
        """
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(beijing_tz)
        current_time = now.time()
        current_time_text = now.strftime("%H:%M")
        
        # 判断是否交易日
        if now.weekday() >= 5:
            return {
                'session': '休市',
                'volatility': 'none',
                'recommendation': '周末不可交易',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }
        
        # 开盘前（9:00-9:30）：集合竞价时段
        if time(9, 0) <= current_time < time(9, 30):
            return {
                'session': '集合竞价',
                'volatility': 'high',
                'recommendation': '可观察盘面情绪，准备开盘交易',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }
        
        # 上午盘（9:30-11:30）
        elif time(9, 30) <= current_time <= time(11, 30):
            return {
                'session': '上午盘',
                'volatility': 'high',
                'recommendation': '交易活跃，波动较大',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': True
            }
        
        # 午间休市（11:30-13:00）
        elif time(11, 30) < current_time < time(13, 0):
            return {
                'session': '午间休市',
                'volatility': 'none',
                'recommendation': '不可交易，可分析上午盘面',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }
        
        # 下午盘（13:00-15:00）
        elif time(13, 0) <= current_time <= time(15, 0):
            # 尾盘最后半小时（14:30-15:00）
            if current_time >= time(14, 30):
                return {
                    'session': '尾盘',
                    'volatility': 'high',
                    'recommendation': '尾盘波动大，谨慎操作',
                    'beijing_hour': now.hour,
                    'beijing_time': current_time_text,
                    'can_trade': True
                }
            else:
                return {
                    'session': '下午盘',
                    'volatility': 'medium',
                    'recommendation': '波动趋缓，适合布局',
                    'beijing_hour': now.hour,
                    'beijing_time': current_time_text,
                    'can_trade': True
                }
        
        # 盘后（15:00之后）
        else:
            return {
                'session': '盘后',
                'volatility': 'none',
                'recommendation': '收盘后，可复盘分析',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }

    @staticmethod
    def _get_intraday_session_progress(session_info: Dict[str, Any]) -> Optional[float]:
        """Return completed fraction of today's regular trading session."""
        session = str(session_info.get("session") or "")
        if session == "休市":
            return None

        beijing_time = str(session_info.get("beijing_time") or "").strip()
        if not beijing_time:
            return None

        try:
            hour_text, minute_text = beijing_time.split(":", 1)
            current_minutes = int(hour_text) * 60 + int(minute_text)
        except (ValueError, TypeError):
            return None

        morning_open = 9 * 60 + 30
        morning_close = 11 * 60 + 30
        afternoon_open = 13 * 60
        afternoon_close = 15 * 60
        total_minutes = 240

        if current_minutes < morning_open:
            elapsed_minutes = 0
        elif current_minutes <= morning_close:
            elapsed_minutes = current_minutes - morning_open
        elif current_minutes < afternoon_open:
            elapsed_minutes = 120
        elif current_minutes <= afternoon_close:
            elapsed_minutes = 120 + (current_minutes - afternoon_open)
        else:
            elapsed_minutes = total_minutes

        progress = elapsed_minutes / total_minutes if total_minutes > 0 else None
        if progress is None:
            return None
        return max(0.0, min(1.0, progress))

    def chat_completion(self, messages: List[Dict], model: str = None,
                       temperature: float = 0.7, max_tokens: int = 2000,
                       tier: ModelTier = ModelTier.LIGHTWEIGHT) -> Dict:
        """
        调用DeepSeek API
        
        Args:
            messages: 对话消息列表
            model: 模型名称
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            API响应
        """
        model_to_use = resolve_model_name(
            tier=tier,
            explicit_model=model,
            forced_model=self.model,
            lightweight_model=self.lightweight_model,
            reasoning_model=self.reasoning_model,
        )

        if "reasoner" in model_to_use.lower() and max_tokens <= 2000:
            max_tokens = max(max_tokens, self.reasoning_max_tokens)
        
        payload = {
            "model": model_to_use,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        endpoint = f"{self.base_url.rstrip('/')}/chat/completions"
        request_timeout = (10, self.http_timeout_seconds)
        total_attempts = self.http_retry_count + 1
        retryable_errors = (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
        last_error: Optional[Exception] = None

        for attempt_index in range(total_attempts):
            try:
                response = requests.post(
                    endpoint,
                    headers=self.headers,
                    json=payload,
                    timeout=request_timeout
                )
                response.raise_for_status()
                return response.json()
            except retryable_errors as exc:
                last_error = exc
                if attempt_index >= self.http_retry_count:
                    break
                self.logger.warning(
                    "DeepSeek API请求超时或连接失败，准备重试 (%s/%s)，model=%s，read_timeout=%ss: %s",
                    attempt_index + 1,
                    total_attempts,
                    model_to_use,
                    self.http_timeout_seconds,
                    exc,
                )
                time_module.sleep(min(2, attempt_index + 1))
            except Exception as exc:
                self.logger.error(
                    "DeepSeek API调用失败，model=%s，timeout=%ss: %s",
                    model_to_use,
                    self.http_timeout_seconds,
                    exc,
                )
                raise

        if last_error is not None:
            self.logger.error(
                "DeepSeek API调用失败，重试后仍未成功，model=%s，timeout=%ss: %s",
                model_to_use,
                self.http_timeout_seconds,
                last_error,
            )
            raise last_error
        raise RuntimeError("DeepSeek API调用失败: unknown_request_error")

    def analyze_stock_and_decide(self, stock_code: str, market_data: Dict,
                                 account_info: Dict, has_position: bool = False,
                                 position_cost: float = 0, position_quantity: int = 0,
                                 account_name: str = DEFAULT_ACCOUNT_NAME,
                                 asset_id: Optional[int] = None,
                                 portfolio_stock_id: Optional[int] = None,
                                 strategy_context: Optional[Dict] = None,
                                 risk_profile: Optional[Dict[str, Any]] = None) -> Dict:
        """
        分析股票并做出交易决策（A股T+1规则）
        
        Args:
            stock_code: 股票代码（如：600519）
            market_data: 市场数据
            account_info: 账户信息
            has_position: 是否已持有该股票
            position_cost: 持仓成本价格
            position_quantity: 持仓数量
            
        Returns:
            交易决策
        """
        # 获取交易时段
        session_info = self.get_trading_session()
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        messages = self._build_prompt_messages(
            stock_code, market_data, account_info,
            has_position, session_info, position_cost, position_quantity,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            strategy_context=strategy_context,
            risk_profile=resolved_risk_profile,
        )

        try:
            response = self.chat_completion(
                messages,
                temperature=0.1,
                max_tokens=1600,
                tier=ModelTier.LIGHTWEIGHT,
            )
            ai_response = response['choices'][0]['message']['content']
            
            # 解析JSON决策
            decision = self._parse_decision(ai_response, risk_profile=resolved_risk_profile)
            decision = self._enforce_action_policy(decision, has_position=has_position)
            
            return {
                'success': True,
                'decision': decision,
                'raw_response': ai_response
            }
            
        except Exception as e:
            self.logger.error(f"AI决策失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _build_prompt_context(self, stock_code: str, market_data: Dict,
                              account_info: Dict, has_position: bool,
                              session_info: Dict, position_cost: float = 0,
                              position_quantity: int = 0,
                              account_name: str = DEFAULT_ACCOUNT_NAME,
                              asset_id: Optional[int] = None,
                              portfolio_stock_id: Optional[int] = None,
                              strategy_context: Optional[Dict] = None,
                              risk_profile: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Build template context for intraday decision prompts."""
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)

        def _to_float(value: object) -> Optional[float]:
            if value in (None, ""):
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(numeric):
                return None
            return numeric

        def _fmt_number(value: object, digits: int = 2, *, signed: bool = False, comma: bool = False) -> str:
            numeric = _to_float(value)
            if numeric is None:
                return "N/A"
            sign = "+" if signed else ""
            separator = "," if comma else ""
            return f"{numeric:{sign}{separator}.{digits}f}"

        def _fmt_money(value: object, *, signed: bool = False) -> str:
            text = _fmt_number(value, digits=2, signed=signed, comma=True)
            return f"¥{text}" if text != "N/A" else "N/A"

        def _fmt_pct(value: object) -> str:
            text = _fmt_number(value, digits=2, signed=True)
            return f"{text}%" if text != "N/A" else "N/A"

        def _fmt_volume(value: object) -> str:
            text = _fmt_number(value, digits=0, comma=True)
            return f"{text}手" if text != "N/A" else "N/A"

        def _volume_state_label(value: object) -> str:
            numeric = _to_float(value)
            if numeric is None:
                return "N/A"
            if numeric > 1.2:
                return "放量"
            if numeric < 0.8:
                return "缩量"
            return "正常"

        def _intraday_position_label(value: object) -> str:
            numeric = _to_float(value)
            if numeric is None:
                return "N/A"
            if numeric >= 85:
                return "接近日内高位"
            if numeric <= 15:
                return "接近日内低位"
            return "处于日内中位"

        turnover_rate_text = _fmt_pct(market_data.get("turnover_rate"))
        turnover_rate_line = f"换手率: {turnover_rate_text}\n" if turnover_rate_text != "N/A" else ""
        current_price = _to_float(market_data.get("current_price"))
        current_price_text = _fmt_money(current_price)
        current_volume = _to_float(market_data.get("volume"))
        vol_ma5 = _to_float(market_data.get("vol_ma5"))
        realtime_volume_ratio = _to_float(market_data.get("volume_ratio"))
        trading_progress = self._get_intraday_session_progress(session_info)
        trading_progress_text = f"{trading_progress * 100:.1f}%" if trading_progress is not None else "N/A"
        projected_full_day_volume = None
        projected_volume_ratio_vs_vol_ma5 = None
        if current_volume is not None and trading_progress is not None and 0 < trading_progress <= 1:
            projected_full_day_volume = current_volume / trading_progress
        if projected_full_day_volume is not None and vol_ma5 is not None and vol_ma5 > 0:
            projected_volume_ratio_vs_vol_ma5 = projected_full_day_volume / vol_ma5
        intraday_volume_note = (
            "说明: 盘中累计成交量不能直接与历史全天均量比较，优先参考实时量比，其次参考按当前交易进度折算的全天成交量。"
            if trading_progress is not None and trading_progress < 1
            else "说明: 当前为非连续交易时段或已收盘，可直接参考全天累计成交量与历史均量。"
        )
        intraday_context = market_data.get("intraday_context") if isinstance(market_data.get("intraday_context"), dict) else {}
        timer_section = render_prompt(
            self.SECTION_TIMER_TEMPLATE,
            session_name=session_info["session"],
            beijing_time=session_info.get("beijing_time") or f"{session_info['beijing_hour']:02d}:00",
            volatility=str(session_info["volatility"]).upper(),
            recommendation=session_info["recommendation"],
            can_trade_text="是" if session_info["can_trade"] else "否",
        )

        data_scope_section = render_prompt(self.SECTION_DATA_SCOPE_TEMPLATE)
        realtime_freshness = market_data.get("realtime_freshness") if isinstance(market_data.get("realtime_freshness"), dict) else {}

        def _freshness_label(status: Any) -> str:
            mapping = {
                "ready": "可直接用于盘中执行",
                "degraded": "可参考但应保守使用",
                "stale": "不适合盘中执行判断",
                "fresh": "新鲜",
                "stale_delay": "延迟过久",
                "stale": "延迟过久",
                "same_day_service_time": "同日服务响应时间",
                "same_day_out_of_session": "同日但非交易时段",
                "same_day_snapshot": "同日盘中快照",
                "cross_day": "跨日旧数据",
                "out_of_session": "时间不在交易时段",
                "unavailable": "不可用",
            }
            return mapping.get(str(status or "").strip(), "未知")

        quote_freshness = realtime_freshness.get("quote") if isinstance(realtime_freshness.get("quote"), dict) else {}
        minute_freshness = realtime_freshness.get("minute") if isinstance(realtime_freshness.get("minute"), dict) else {}
        trade_freshness = realtime_freshness.get("trade") if isinstance(realtime_freshness.get("trade"), dict) else {}
        minute_quality = realtime_freshness.get("minute_quality") if isinstance(realtime_freshness.get("minute_quality"), dict) else {}
        realtime_freshness_section = render_prompt(
            self.SECTION_REALTIME_FRESHNESS_TEMPLATE,
            asof_time=realtime_freshness.get("asof_time", "N/A"),
            is_trading_now_text="是" if realtime_freshness.get("is_trading_now") else "否",
            intraday_decision_ready_text="是" if realtime_freshness.get("intraday_decision_ready") else "否",
            overall_status_text=_freshness_label(realtime_freshness.get("overall_status")),
            quote_timestamp=quote_freshness.get("timestamp", "N/A"),
            quote_status_text=_freshness_label(quote_freshness.get("status")),
            minute_timestamp=minute_freshness.get("timestamp", "N/A"),
            minute_status_text=_freshness_label(minute_freshness.get("status")),
            trade_timestamp=trade_freshness.get("timestamp", "N/A"),
            trade_status_text=_freshness_label(trade_freshness.get("status")),
            minute_coverage_ratio_text=(
                f"{float(minute_quality.get('coverage_ratio')) * 100:.1f}%"
                if minute_quality.get("coverage_ratio") is not None
                else "N/A"
            ),
            minute_max_gap_text=(
                f"{int(minute_quality.get('max_gap'))} 分钟"
                if minute_quality.get("max_gap") is not None
                else "N/A"
            ),
            minute_quality_text=minute_quality.get("label", "未提供分时质量"),
            freshness_summary=realtime_freshness.get("summary", "未提供实时新鲜度校验结果"),
        )

        rsi6_value = _to_float(market_data.get("rsi6"))
        if rsi6_value is not None and rsi6_value > 80:
            rsi6_state = "[超买]"
        elif rsi6_value is not None and rsi6_value < 20:
            rsi6_state = "[超卖]"
        elif rsi6_value is not None:
            rsi6_state = "[正常]"
        else:
            rsi6_state = "[N/A]"

        stock_section = render_prompt(
            self.SECTION_STOCK_TEMPLATE,
            stock_code=stock_code,
            stock_name=market_data.get("name", "N/A"),
            data_source=str(market_data.get("data_source", "N/A")).upper(),
            update_time=market_data.get("update_time", "N/A"),
            current_price=current_price_text,
            change_pct=_fmt_pct(market_data.get("change_pct")),
            change_amount=_fmt_money(market_data.get("change_amount"), signed=True),
            high=_fmt_money(market_data.get("high")),
            low=_fmt_money(market_data.get("low")),
            open_price=_fmt_money(market_data.get("open")),
            pre_close=_fmt_money(market_data.get("pre_close")),
            volume=_fmt_volume(market_data.get("volume")),
            amount=_fmt_money(market_data.get("amount")),
        )

        technical_section = render_prompt(
            self.SECTION_TECHNICAL_TEMPLATE,
            ma5=_fmt_money(market_data.get("ma5")),
            ma20=_fmt_money(market_data.get("ma20")),
            ma60=_fmt_money(market_data.get("ma60")),
            trend_label="多头排列" if market_data.get("trend") == "up" else "空头排列" if market_data.get("trend") == "down" else "N/A",
            macd_dif=_fmt_number(market_data.get("macd_dif"), digits=4),
            macd_dea=_fmt_number(market_data.get("macd_dea"), digits=4),
            macd=_fmt_number(market_data.get("macd"), digits=4),
            rsi6=_fmt_number(market_data.get("rsi6")),
            rsi6_state=rsi6_state,
            rsi12=_fmt_number(market_data.get("rsi12")),
            rsi24=_fmt_number(market_data.get("rsi24")),
            kdj_k=_fmt_number(market_data.get("kdj_k")),
            kdj_d=_fmt_number(market_data.get("kdj_d")),
            kdj_j=_fmt_number(market_data.get("kdj_j")),
            boll_upper=_fmt_money(market_data.get("boll_upper")),
            boll_mid=_fmt_money(market_data.get("boll_mid")),
            boll_lower=_fmt_money(market_data.get("boll_lower")),
            boll_position=market_data.get("boll_position", "N/A") or "N/A",
        )

        volume_section = render_prompt(
            self.SECTION_VOLUME_TEMPLATE,
            current_volume=_fmt_volume(current_volume),
            trading_progress=trading_progress_text,
            projected_full_day_volume=_fmt_volume(projected_full_day_volume),
            vol_ma5=_fmt_volume(market_data.get("vol_ma5")),
            realtime_volume_ratio=_fmt_number(realtime_volume_ratio),
            realtime_volume_state=_volume_state_label(realtime_volume_ratio),
            projected_volume_ratio_vs_vol_ma5=_fmt_number(projected_volume_ratio_vs_vol_ma5),
            projected_volume_state=_volume_state_label(projected_volume_ratio_vs_vol_ma5),
            intraday_volume_note=intraday_volume_note,
            turnover_rate_line=turnover_rate_line.strip(),
        )

        execution_context_section = render_prompt(
            self.SECTION_EXECUTION_CONTEXT_TEMPLATE,
            available_cash=f"¥{account_info.get('available_cash', 0):,.2f}",
            total_value=f"¥{account_info.get('total_value', 0):,.2f}",
            total_market_value=f"¥{account_info.get('total_market_value', 0):,.2f}",
            position_usage_pct=f"{account_info.get('position_usage_pct', 0) * 100:.2f}%",
            positions_count=account_info.get("positions_count", 0),
            account_name=account_name,
            asset_id=asset_id or "N/A",
            portfolio_stock_id=portfolio_stock_id or "N/A",
        )

        account_risk_profile_section = render_prompt(
            self.SECTION_ACCOUNT_RISK_PROFILE_TEMPLATE,
            position_size_pct=resolved_risk_profile["position_size_pct"],
            total_position_pct=resolved_risk_profile["total_position_pct"],
            stop_loss_pct=resolved_risk_profile["stop_loss_pct"],
            take_profit_pct=resolved_risk_profile["take_profit_pct"],
        )
        optional_sections: List[str] = []
        if intraday_context:
            observations = intraday_context.get("intraday_observations") if isinstance(intraday_context.get("intraday_observations"), list) else []
            observation_text = " / ".join(str(item) for item in observations[:4] if item) or "N/A"
            signal_labels = intraday_context.get("intraday_signal_labels") if isinstance(intraday_context.get("intraday_signal_labels"), list) else []
            signal_label_text = " / ".join(str(item) for item in signal_labels[:4] if item) or "N/A"
            optional_sections.append(render_prompt(
                self.SECTION_INTRADAY_FLOW_TEMPLATE,
                minute_point_count=intraday_context.get("minute_point_count", "N/A"),
                filled_minute_point_count=intraday_context.get("filled_minute_point_count", "N/A"),
                minute_coverage_ratio=(
                    f"{float(intraday_context.get('minute_coverage_ratio')) * 100:.1f}%"
                    if intraday_context.get("minute_coverage_ratio") is not None
                    else "N/A"
                ),
                max_minute_gap=(
                    f"{int(intraday_context.get('max_minute_gap'))} 分钟"
                    if intraday_context.get("max_minute_gap") is not None
                    else "N/A"
                ),
                intraday_high=_fmt_money(intraday_context.get("intraday_high")),
                intraday_low=_fmt_money(intraday_context.get("intraday_low")),
                intraday_range_pct=_fmt_pct(intraday_context.get("intraday_range_pct")),
                intraday_vwap=_fmt_money(intraday_context.get("intraday_vwap")),
                price_position_pct=_fmt_number(intraday_context.get("price_position_pct")),
                intraday_position_label=_intraday_position_label(intraday_context.get("price_position_pct")),
                last_5m_change_pct=_fmt_pct(intraday_context.get("last_5m_change_pct")),
                last_15m_change_pct=_fmt_pct(intraday_context.get("last_15m_change_pct")),
                last_30m_change_pct=_fmt_pct(intraday_context.get("last_30m_change_pct")),
                recent_5m_volume=_fmt_volume(intraday_context.get("recent_5m_volume")),
                previous_5m_volume=_fmt_volume(intraday_context.get("previous_5m_volume")),
                volume_acceleration_ratio=_fmt_number(intraday_context.get("volume_acceleration_ratio")),
                volume_acceleration_state=_volume_state_label(intraday_context.get("volume_acceleration_ratio")),
                trade_tick_count=intraday_context.get("trade_tick_count", "N/A"),
                latest_trade_time=intraday_context.get("latest_trade_time", "N/A"),
                avg_trade_volume=_fmt_volume(intraday_context.get("avg_trade_volume")),
                largest_trade_volume=_fmt_volume(intraday_context.get("largest_trade_volume")),
                intraday_bias_text=intraday_context.get("intraday_bias_text", "N/A"),
                signal_label_text=signal_label_text,
                observation_text=observation_text,
            ))
        if strategy_context:
            optional_sections.append(render_prompt(
                self.SECTION_STRATEGY_CONTEXT_TEMPLATE,
                analysis_date=strategy_context.get("analysis_date", "N/A"),
                analysis_source=strategy_context.get("analysis_source", "N/A"),
                rating=strategy_context.get("rating", "N/A"),
                summary=strategy_context.get("summary", "N/A"),
                entry_min=strategy_context.get("entry_min", "N/A"),
                entry_max=strategy_context.get("entry_max", "N/A"),
                take_profit=strategy_context.get("take_profit", "N/A"),
                stop_loss=strategy_context.get("stop_loss", "N/A"),
            ))
        # --- 注入语义化标签分析 ---
        labels = market_data.get('semantic_labels', [])
        if labels:
            optional_sections.append(render_prompt(
                self.SECTION_AI_PATTERN_RECOGNITION_TEMPLATE,
                labels_block="\n".join(f"- {label}" for label in labels if label),
            ))

        # 如果已持有该股票
        if has_position and position_cost > 0 and position_quantity > 0:
            cost_total = position_cost * position_quantity
            current_total = current_price * position_quantity if current_price is not None else None
            profit_loss = (current_total - cost_total) if current_total is not None else None
            profit_loss_pct = (profit_loss / cost_total * 100) if profit_loss is not None and cost_total > 0 else None
            current_total_text = f"¥{current_total:,.2f}" if current_total is not None else "N/A"
            profit_loss_text = (
                f"¥{profit_loss:,.2f} ({profit_loss_pct:+.2f}%)"
                if profit_loss is not None and profit_loss_pct is not None
                else "N/A"
            )
            current_price_text = _fmt_money(current_price)

            position_section = render_prompt(
                self.SECTION_POSITION_HOLDING_TEMPLATE,
                stock_code=stock_code,
                position_quantity=position_quantity,
                position_cost=f"¥{position_cost:.2f}",
                current_price=current_price_text,
                current_total=current_total_text,
                profit_loss_text=profit_loss_text,
                stop_loss_pct=resolved_risk_profile["stop_loss_pct"],
            )
        else:
            position_section = render_prompt(
                self.SECTION_POSITION_EMPTY_TEMPLATE,
                position_size_pct=resolved_risk_profile["position_size_pct"],
            )

        # 主力资金数据（已禁用 - 接口不稳定）
        # if 'main_force' in market_data:
        #     mf = market_data['main_force']
        #     prompt += f"""
        # [MONEY] 主力资金流向
        # ═══════════════════════════════════════════════════════════
        # 主力净额: ¥{mf.get('main_net', 0):,.2f}万 ({mf.get('main_net_pct', 0):+.2f}%)
        # 超大单: ¥{mf.get('super_net', 0):,.2f}万
        # 大单: ¥{mf.get('big_net', 0):,.2f}万
        # 中单: ¥{mf.get('mid_net', 0):,.2f}万
        # 小单: ¥{mf.get('small_net', 0):,.2f}万
        # 主力动向: {mf.get('trend', '观望')}
        # """

        optional_sections_text = "\n\n".join(section.strip() for section in optional_sections if str(section).strip())
        if has_position:
            position_mode_title = "当前有持仓。"
            position_mode_rules = "\n".join([
                "- 本次只允许在 SELL / HOLD 之间决策",
                "- 不要讨论 BUY、加仓或重新开仓",
                "- 先判断是否达到止盈/止损/破位条件，再判断是否继续持有",
            ])
        else:
            position_mode_title = "当前无持仓。"
            position_mode_rules = "\n".join([
                "- 本次只允许在 BUY / HOLD 之间决策",
                "- 不要讨论 SELL、减仓或止盈卖出",
                "- 只有当战略基线支持且盘中信号足够清晰时，才允许给出 BUY",
            ])
        return {
            "position_size_pct": str(resolved_risk_profile["position_size_pct"]),
            "total_position_pct": str(resolved_risk_profile["total_position_pct"]),
            "stop_loss_pct": str(resolved_risk_profile["stop_loss_pct"]),
            "take_profit_pct": str(resolved_risk_profile["take_profit_pct"]),
            "stop_loss_pct_float": f"{float(resolved_risk_profile['stop_loss_pct']):.1f}",
            "take_profit_pct_float": f"{float(resolved_risk_profile['take_profit_pct']):.1f}",
            "position_mode_title": position_mode_title,
            "position_mode_rules": position_mode_rules,
            "timer_section": timer_section.strip(),
            "data_scope_section": data_scope_section,
            "realtime_freshness_section": realtime_freshness_section.strip(),
            "stock_section": stock_section.strip(),
            "technical_section": technical_section.strip(),
            "volume_section": volume_section.strip(),
            "execution_context_section": execution_context_section.strip(),
            "account_risk_profile_section": account_risk_profile_section.strip(),
            "optional_sections": optional_sections_text,
            "position_section": position_section.strip(),
        }

    def _build_prompt_messages(self, stock_code: str, market_data: Dict,
                               account_info: Dict, has_position: bool,
                               session_info: Dict, position_cost: float = 0,
                               position_quantity: int = 0,
                               account_name: str = DEFAULT_ACCOUNT_NAME,
                               asset_id: Optional[int] = None,
                               portfolio_stock_id: Optional[int] = None,
                               strategy_context: Optional[Dict] = None,
                               risk_profile: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
        context = self._build_prompt_context(
            stock_code, market_data, account_info,
            has_position, session_info, position_cost, position_quantity,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            strategy_context=strategy_context,
            risk_profile=risk_profile,
        )
        return build_messages(self.SYSTEM_TEMPLATE, self.USER_TEMPLATE, **context)

    def _build_a_stock_prompt(self, stock_code: str, market_data: Dict,
                              account_info: Dict, has_position: bool,
                              session_info: Dict, position_cost: float = 0,
                              position_quantity: int = 0,
                              account_name: str = DEFAULT_ACCOUNT_NAME,
                              asset_id: Optional[int] = None,
                              portfolio_stock_id: Optional[int] = None,
                              strategy_context: Optional[Dict] = None,
                              risk_profile: Optional[Dict[str, Any]] = None) -> str:
        """构建A股分析提示词。"""
        return self._build_prompt_messages(
            stock_code, market_data, account_info,
            has_position, session_info, position_cost, position_quantity,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            strategy_context=strategy_context,
            risk_profile=risk_profile,
        )[1]["content"]

    @staticmethod
    def _iter_json_candidates(ai_response: str) -> List[str]:
        text = str(ai_response or "").strip()
        if not text:
            return []

        candidates: List[str] = []
        for match in re.finditer(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE):
            candidate = str(match.group(1) or "").strip()
            if candidate:
                candidates.append(candidate)

        braced = SmartMonitorDeepSeek._extract_balanced_braces(text)
        if braced:
            candidates.append(braced)

        candidates.append(text)

        deduped: List[str] = []
        seen = set()
        for candidate in candidates:
            normalized = candidate.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _extract_balanced_braces(text: str) -> Optional[str]:
        for start_index, char in enumerate(text):
            if char != "{":
                continue
            depth = 0
            quote_char = ""
            escape = False
            for index in range(start_index, len(text)):
                current_char = text[index]
                if quote_char:
                    if escape:
                        escape = False
                    elif current_char == "\\":
                        escape = True
                    elif current_char == quote_char:
                        quote_char = ""
                    continue
                if current_char in {'"', "'"}:
                    quote_char = current_char
                    continue
                if current_char == "{":
                    depth += 1
                elif current_char == "}":
                    depth -= 1
                    if depth == 0:
                        return text[start_index:index + 1]
        return None

    @staticmethod
    def _strip_json_comments(text: str) -> str:
        result: List[str] = []
        quote_char = ""
        escape = False
        index = 0
        while index < len(text):
            current_char = text[index]
            next_char = text[index + 1] if index + 1 < len(text) else ""
            if quote_char:
                result.append(current_char)
                if escape:
                    escape = False
                elif current_char == "\\":
                    escape = True
                elif current_char == quote_char:
                    quote_char = ""
                index += 1
                continue

            if current_char in {'"', "'"}:
                quote_char = current_char
                result.append(current_char)
                index += 1
                continue

            if current_char == "/" and next_char == "/":
                index += 2
                while index < len(text) and text[index] not in "\r\n":
                    index += 1
                continue

            if current_char == "/" and next_char == "*":
                index += 2
                while index + 1 < len(text) and text[index:index + 2] != "*/":
                    index += 1
                index += 2
                continue

            result.append(current_char)
            index += 1
        return "".join(result)

    @staticmethod
    def _quote_unquoted_keys(text: str) -> str:
        pattern = re.compile(r'([{\[,]\s*)([A-Za-z_\u4e00-\u9fff][A-Za-z0-9_\-\u4e00-\u9fff]*)(\s*:)')
        return pattern.sub(r'\1"\2"\3', text)

    @staticmethod
    def _quote_known_string_values(text: str) -> str:
        replacements = {
            "action": r"BUY|SELL|HOLD|买入|卖出|持有|观望|等待|加仓|减仓|止盈|止损",
            "risk_level": r"low|medium|high|低|中|高",
        }
        normalized = text
        for field, options in replacements.items():
            pattern = re.compile(
                rf'("{field}"\s*:\s*)(?P<value>{options})(\s*[,}}])',
                re.IGNORECASE,
            )
            normalized = pattern.sub(r'\1"\g<value>"\3', normalized)
        return normalized

    @staticmethod
    def _strip_trailing_commas(text: str) -> str:
        return re.sub(r",\s*([}\]])", r"\1", text)

    @staticmethod
    def _replace_json_literals_for_python(text: str) -> str:
        replacements = {"true": "True", "false": "False", "null": "None"}
        result: List[str] = []
        quote_char = ""
        escape = False
        index = 0
        while index < len(text):
            current_char = text[index]
            if quote_char:
                result.append(current_char)
                if escape:
                    escape = False
                elif current_char == "\\":
                    escape = True
                elif current_char == quote_char:
                    quote_char = ""
                index += 1
                continue

            if current_char in {'"', "'"}:
                quote_char = current_char
                result.append(current_char)
                index += 1
                continue

            replaced = False
            for source, target in replacements.items():
                end_index = index + len(source)
                if (
                    text[index:end_index] == source
                    and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_"))
                    and (end_index >= len(text) or not (text[end_index].isalnum() or text[end_index] == "_"))
                ):
                    result.append(target)
                    index = end_index
                    replaced = True
                    break
            if replaced:
                continue

            result.append(current_char)
            index += 1
        return "".join(result)

    @staticmethod
    def _sanitize_json_like_text(text: str) -> str:
        translation = str.maketrans({
            "“": '"',
            "”": '"',
            "‘": "'",
            "’": "'",
            "，": ",",
            "：": ":",
            "；": ";",
        })
        sanitized = str(text or "").strip().translate(translation)
        sanitized = SmartMonitorDeepSeek._strip_json_comments(sanitized)
        sanitized = SmartMonitorDeepSeek._quote_unquoted_keys(sanitized)
        sanitized = SmartMonitorDeepSeek._quote_known_string_values(sanitized)
        sanitized = SmartMonitorDeepSeek._strip_trailing_commas(sanitized)
        return sanitized

    @staticmethod
    def _coerce_numeric(value: Any, *, default: float = 0.0, scale_fraction_to_pct: bool = False) -> float:
        if isinstance(value, bool):
            return float(default)
        if isinstance(value, (int, float)):
            number = float(value)
        else:
            text = str(value or "").replace(",", "").strip()
            match = re.search(r"-?\d+(?:\.\d+)?", text)
            if not match:
                return float(default)
            number = float(match.group(0))
        if scale_fraction_to_pct and 0 <= number <= 1:
            number *= 100
        return number

    @staticmethod
    def _normalize_action_value(value: Any) -> str:
        text = str(value or "").strip().upper()
        mapping = {
            "BUY": "BUY",
            "买入": "BUY",
            "加仓": "BUY",
            "建仓": "BUY",
            "SELL": "SELL",
            "卖出": "SELL",
            "减仓": "SELL",
            "止盈": "SELL",
            "止损": "SELL",
            "HOLD": "HOLD",
            "持有": "HOLD",
            "观望": "HOLD",
            "等待": "HOLD",
        }
        return mapping.get(text, "HOLD")

    @staticmethod
    def _normalize_risk_level(value: Any) -> str:
        text = str(value or "").strip().lower()
        mapping = {
            "low": "low",
            "低": "low",
            "medium": "medium",
            "中": "medium",
            "high": "high",
            "高": "high",
        }
        return mapping.get(text, "medium")

    @staticmethod
    def _normalize_key_price_levels(value: Any) -> Dict[str, float]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, float] = {}
        for key in ("support", "resistance", "stop_loss"):
            raw_value = value.get(key)
            if raw_value in (None, ""):
                continue
            try:
                normalized[key] = float(SmartMonitorDeepSeek._coerce_numeric(raw_value))
            except (TypeError, ValueError):
                continue
        return normalized

    def _normalize_decision_payload(self, decision: Dict[str, Any], risk_profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        reasoning_text = decision.get("reasoning")
        if isinstance(reasoning_text, (dict, list)):
            reasoning_text = json.dumps(reasoning_text, ensure_ascii=False)
        reasoning = str(reasoning_text or "").strip()
        if not reasoning:
            raise ValueError("缺少必需字段: reasoning")

        normalized: Dict[str, Any] = {
            "action": self._normalize_action_value(decision.get("action")),
            "confidence": int(max(0, min(100, round(self._coerce_numeric(
                decision.get("confidence"),
                default=0,
                scale_fraction_to_pct=True,
            ))))),
            "reasoning": reasoning,
            "position_size_pct": int(max(0, min(100, round(self._coerce_numeric(
                decision.get("position_size_pct"),
                default=resolved_risk_profile["position_size_pct"],
            ))))),
            "stop_loss_pct": round(max(0.0, self._coerce_numeric(
                decision.get("stop_loss_pct"),
                default=float(resolved_risk_profile["stop_loss_pct"]),
            )), 2),
            "take_profit_pct": round(max(0.0, self._coerce_numeric(
                decision.get("take_profit_pct"),
                default=float(resolved_risk_profile["take_profit_pct"]),
            )), 2),
            "risk_level": self._normalize_risk_level(decision.get("risk_level")),
            "key_price_levels": self._normalize_key_price_levels(decision.get("key_price_levels")),
        }

        monitor_levels = self._normalize_monitor_levels(decision)
        if monitor_levels:
            normalized["monitor_levels"] = monitor_levels
        return normalized

    def _salvage_decision_fields(self, text: str) -> Optional[Dict[str, Any]]:
        normalized = self._sanitize_json_like_text(text)
        action_match = re.search(r'(?i)(?:^|[,{]\s*)"action"\s*:\s*"?([A-Za-z\u4e00-\u9fff]+)', normalized)
        confidence_match = re.search(r'(?i)(?:^|[,{]\s*)"confidence"\s*:\s*"?([0-9]+(?:\.[0-9]+)?%?)', normalized)
        reasoning_match = re.search(
            r'(?is)(?:^|[,{]\s*)"reasoning"\s*:\s*"?(.*?)(?:"?\s*(?:,\s*"[A-Za-z_][A-Za-z0-9_]*"\s*:|\}\s*$))',
            normalized,
        )
        if not action_match or not confidence_match or not reasoning_match:
            return None
        return {
            "action": action_match.group(1),
            "confidence": confidence_match.group(1),
            "reasoning": reasoning_match.group(1).strip().strip('"').strip(),
        }

    def _decode_decision_text(self, ai_response: str) -> Dict[str, Any]:
        errors: List[str] = []
        for candidate in self._iter_json_candidates(ai_response):
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, str):
                    parsed = json.loads(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"strict_json: {exc}")

            sanitized = self._sanitize_json_like_text(candidate)
            try:
                parsed = json.loads(sanitized)
                if isinstance(parsed, str):
                    parsed = json.loads(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"sanitized_json: {exc}")

            try:
                python_like = self._replace_json_literals_for_python(sanitized)
                parsed = ast.literal_eval(python_like)
                if isinstance(parsed, str):
                    parsed = ast.literal_eval(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"python_literal: {exc}")

        salvaged = self._salvage_decision_fields(ai_response)
        if salvaged:
            return salvaged

        error_message = errors[-1] if errors else "未找到可解析的JSON对象"
        raise ValueError(error_message)

    def _parse_decision(self, ai_response: str, risk_profile: Optional[Dict[str, Any]] = None) -> Dict:
        """解析AI决策响应。"""
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        try:
            decoded = self._decode_decision_text(ai_response)
            return self._normalize_decision_payload(decoded, risk_profile=resolved_risk_profile)
        except Exception as e:
            self.logger.error("解析AI决策失败: %s; response=%s", e, str(ai_response or "")[:300])
            return {
                'action': 'HOLD',
                'confidence': 0,
                'reasoning': f'AI响应解析失败: {str(e)}',
                'position_size_pct': 0,
                'stop_loss_pct': float(resolved_risk_profile["stop_loss_pct"]),
                'take_profit_pct': float(resolved_risk_profile["take_profit_pct"]),
                'risk_level': 'high',
                'key_price_levels': {},
            }

    @staticmethod
    def _normalize_monitor_levels(decision: Dict) -> Optional[Dict]:
        raw_levels = decision.get("monitor_levels")
        if isinstance(raw_levels, dict):
            candidates = raw_levels
        else:
            candidates = {
                "entry_min": decision.get("entry_min"),
                "entry_max": decision.get("entry_max"),
                "take_profit": decision.get("take_profit"),
                "stop_loss": decision.get("stop_loss"),
            }
            entry_range = decision.get("entry_range")
            if isinstance(entry_range, dict):
                candidates["entry_min"] = candidates.get("entry_min") or entry_range.get("min")
                candidates["entry_max"] = candidates.get("entry_max") or entry_range.get("max")

        normalized: Dict[str, float] = {}
        for key in ("entry_min", "entry_max", "take_profit", "stop_loss"):
            value = candidates.get(key)
            if value in (None, ""):
                return None
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError):
                return None
        return normalized

    def _enforce_action_policy(self, decision: Dict, has_position: bool) -> Dict:
        def _append_constraint_reasoning(message: str) -> None:
            original_reasoning = str(decision.get("reasoning") or "").strip()
            if original_reasoning:
                decision["reasoning"] = f"{original_reasoning}\n\n[动作约束] {message}"
            else:
                decision["reasoning"] = message

        allowed_actions = {"BUY", "SELL", "HOLD"}
        action = str(decision.get("action", "HOLD") or "HOLD").upper()
        if action not in allowed_actions:
            decision["action"] = "HOLD"
            _append_constraint_reasoning(
                f"原始动作 {action} 不在允许集合 {sorted(allowed_actions)} 中，已降级为 HOLD。"
            )
            decision["risk_level"] = "high"
            return decision

        if not has_position and action == "SELL":
            decision["action"] = "HOLD"
            _append_constraint_reasoning("当前无持仓，SELL 不可执行，已降级为 HOLD。")
            decision["risk_level"] = "high"
            return decision

        decision["action"] = action
        return decision
