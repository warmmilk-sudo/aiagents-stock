"""
智能盯盘 - 主引擎
整合 LLM 决策、数据获取、待办生成、通知等功能
"""

import logging
import time
import inspect
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any, Dict, List, Optional
from datetime import date, datetime, timedelta
import threading

import config
from smart_monitor_deepseek import SmartMonitorDeepSeek
from smart_monitor_data import SmartMonitorDataFetcher
from smart_monitor_db import SmartMonitorDB
from notification_service import notification_service  # 复用主程序的通知服务
from asset_repository import STATUS_PORTFOLIO
from investment_action_utils import normalize_strategy_context, normalize_swing_type, swing_type_label
from investment_db_utils import DEFAULT_ACCOUNT_NAME, normalize_account_name
from investment_lifecycle_service import InvestmentLifecycleService, investment_lifecycle_service
from internal_events import event_bus, Events


class SmartMonitorEngine:
    """智能盯盘引擎"""

    DATA_FETCH_TIMEOUT_SECONDS = 45
    AI_DECISION_TIMEOUT_SECONDS = 25
    BASELINE_ANALYSTS_CONFIG = {
        "technical": True,
        "fundamental": True,
        "fund_flow": True,
        "risk": True,
        "sentiment": False,
        "news": False,
    }
    
    def __init__(self, llm_api_key: str = None, model: str = None,
                 lightweight_model: str = None, reasoning_model: str = None,
                 lifecycle_service: InvestmentLifecycleService = None):
        """
        初始化智能盯盘引擎
        
        Args:
            llm_api_key: LLM API密钥（可选，从配置读取）
        """
        self.logger = logging.getLogger(__name__)
        
        # LLM API
        if llm_api_key is None:
            llm_api_key = config.WARMMILK_API_KEY
        llm_base_url = config.WARMMILK_BASE_URL

        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        
        # 初始化各个模块
        self.llm_client = SmartMonitorDeepSeek(
            llm_api_key,
            base_url=llm_base_url,
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        self.data_fetch_timeout_seconds = max(
            int(
                getattr(
                    config,
                    "SMART_MONITOR_DATA_FETCH_TIMEOUT_SECONDS",
                    self.DATA_FETCH_TIMEOUT_SECONDS,
                ) or self.DATA_FETCH_TIMEOUT_SECONDS
            ),
            int(getattr(config, "TDX_TIMEOUT_SECONDS", 10) or 10) + 5,
        )
        self.ai_decision_timeout_seconds = max(
            int(getattr(config, "SMART_MONITOR_AI_TIMEOUT_SECONDS", self.AI_DECISION_TIMEOUT_SECONDS) or self.AI_DECISION_TIMEOUT_SECONDS),
            int(getattr(self.llm_client, "http_timeout_seconds", self.AI_DECISION_TIMEOUT_SECONDS) or self.AI_DECISION_TIMEOUT_SECONDS) + 10,
        )
        self.data_fetcher = SmartMonitorDataFetcher()
        self.db = SmartMonitorDB()
        self.notification = notification_service  # 使用主程序的通知服务
        self.lifecycle_service = lifecycle_service or investment_lifecycle_service
        
        # 监控控制(保留字典为了停止特定监控时注销事件)
        self.monitoring_stocks = set()
        self.monitoring_contexts = {}
        
        # 注册事件总线监听
        event_bus.subscribe(Events.STOCK_ABNORMAL_FLUCTUATION, self._on_radar_event)
        
        self.logger.info(
            "智能盯盘引擎初始化完成, 已订阅事件总线。data_fetch_timeout=%ss ai_timeout=%ss",
            self.data_fetch_timeout_seconds,
            self.ai_decision_timeout_seconds,
        )

    def set_model_overrides(self, model: str = None,
                            lightweight_model: str = None,
                            reasoning_model: str = None) -> None:
        """更新当前会话中后续分析使用的模型。"""
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.llm_client.set_model_overrides(
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )

    @staticmethod
    def _safe_float(value: object) -> float:
        try:
            return float(value) if value not in (None, "") else 0.0
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _safe_int(value: object) -> int:
        try:
            return int(value) if value not in (None, "") else 0
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _previous_trading_day(reference_day: date) -> date:
        current = reference_day
        if current.weekday() < 5:
            current -= timedelta(days=1)
        while current.weekday() >= 5:
            current -= timedelta(days=1)
        return current

    @classmethod
    def _is_strategy_context_stale(cls, strategy_context: Optional[Dict]) -> bool:
        if not isinstance(strategy_context, dict) or not strategy_context:
            return True
        analysis_time = cls._parse_decision_time(strategy_context.get("analysis_date"))
        if analysis_time is None:
            return True
        latest_allowed_day = cls._previous_trading_day(datetime.now().date())
        return analysis_time.date() < latest_allowed_day

    @staticmethod
    def _normalize_position_date(value: object) -> Optional[str]:
        text = str(value or "").strip()
        if not text:
            return None
        return text[:10] if len(text) >= 10 else text

    def _resolve_position_snapshot(
        self,
        *,
        stock_code: str,
        account_name: str,
        task_context: Optional[Dict],
        asset: Optional[Dict],
        has_position: bool,
        position_cost: float,
        position_quantity: int,
        position_date: Optional[str],
    ) -> Dict[str, object]:
        resolved_has_position = bool(has_position)
        resolved_position_cost = float(position_cost or 0)
        resolved_position_quantity = int(position_quantity or 0)
        resolved_position_date = self._normalize_position_date(position_date) or self._normalize_position_date((task_context or {}).get("position_date"))

        if asset:
            resolved_has_position = asset.get("status") == STATUS_PORTFOLIO and int(asset.get("quantity") or 0) > 0
            resolved_position_cost = float(asset.get("cost_price") or 0)
            resolved_position_quantity = int(asset.get("quantity") or 0)
            resolved_position_date = (
                self._normalize_position_date((task_context or {}).get("position_date"))
                or self._normalize_position_date(asset.get("last_trade_at"))
                or self._normalize_position_date(asset.get("created_at"))
            )

        self.logger.info(
            "[%s] 决策前持仓快照: has_position=%s cost=%.2f quantity=%s position_date=%s",
            stock_code,
            resolved_has_position,
            resolved_position_cost,
            resolved_position_quantity,
            resolved_position_date or "N/A",
        )
        return {
            "has_position": resolved_has_position,
            "position_cost": resolved_position_cost,
            "position_quantity": resolved_position_quantity,
            "position_date": resolved_position_date,
        }

    @staticmethod
    def _resolve_reference_trading_date(market_data: Optional[Dict[str, object]] = None) -> datetime.date:
        freshness = (market_data or {}).get("realtime_freshness") if isinstance(market_data, dict) else {}
        candidates = []
        if isinstance(freshness, dict):
            candidates.append(freshness.get("asof_time"))
        if isinstance(market_data, dict):
            candidates.append(market_data.get("update_time"))
        for candidate in candidates:
            parsed = SmartMonitorEngine._parse_decision_time(candidate)
            if parsed:
                return parsed.date()
        return datetime.now().date()

    def _can_sell_today(
        self,
        *,
        has_position: bool,
        position_date: Optional[str],
        market_data: Optional[Dict[str, object]] = None,
    ) -> bool:
        if not has_position:
            return False
        parsed_position_date = self._parse_decision_time(position_date)
        if not parsed_position_date:
            return True
        reference_date = self._resolve_reference_trading_date(market_data)
        return parsed_position_date.date() < reference_date

    def _enforce_t1_sell_constraint(
        self,
        *,
        decision: Dict[str, object],
        has_position: bool,
        can_sell_today: bool,
        account_info: Dict[str, object],
        risk_profile: Optional[Dict[str, int]],
    ) -> Dict[str, object]:
        normalized_action = str(decision.get("action") or "").upper()
        if normalized_action != "SELL" or not has_position or can_sell_today:
            return decision

        original_reasoning = str(decision.get("reasoning") or "").strip()
        blocked_reason = "受A股T+1限制，今日新开仓位不可卖出，已降级为 HOLD。"
        decision["action"] = "HOLD"
        if hasattr(self.llm_client, "_resolve_action_detail"):
            decision["action_detail"] = self.llm_client._resolve_action_detail(
                decision.get("action_detail"),
                action="HOLD",
                has_position=has_position,
            )
        else:
            decision["action_detail"] = "持有" if has_position else "观望"
        decision["action_ratio_pct"] = None
        decision["risk_level"] = "high"
        decision["reasoning"] = f"{original_reasoning}\n\n补充说明：{blocked_reason}" if original_reasoning else f"补充说明：{blocked_reason}"
        if hasattr(self.llm_client, "_attach_execution_targets"):
            return self.llm_client._attach_execution_targets(
                decision,
                account_info=account_info,
                risk_profile=risk_profile,
            )
        return decision

    def _resolve_account_holding_price(
        self,
        stock: Dict,
        *,
        account_name: str,
        focus_asset_id: Optional[int],
        focus_portfolio_stock_id: Optional[int],
        focus_symbol: str,
        current_market_price: float,
    ) -> float:
        stock_id = stock.get("id")
        stock_symbol = str(stock.get("code") or stock.get("symbol") or "").strip().upper()
        if (
            current_market_price > 0
            and (
                (focus_asset_id and int(stock_id or 0) == int(focus_asset_id))
                or (focus_portfolio_stock_id and int(stock_id or 0) == int(focus_portfolio_stock_id))
                or (focus_symbol and stock_symbol == focus_symbol)
            )
        ):
            return current_market_price
        return 0.0

    def _build_account_info(
        self,
        *,
        account_name: str,
        asset: Optional[Dict],
        stock_code: str,
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
        has_position: bool,
        position_cost: float,
        position_quantity: int,
        position_date: Optional[str],
        current_market_price: float,
    ) -> Dict:
        normalized_account_name = normalize_account_name(account_name) or DEFAULT_ACCOUNT_NAME
        account_stocks = [
            stock
            for stock in self.db.portfolio_db.get_all_stocks(auto_monitor_only=False)
            if normalize_account_name(stock.get("account_name")) == normalized_account_name
        ]

        total_market_value = 0.0
        total_profit_loss = 0.0
        positions_count = 0
        current_position: Optional[Dict[str, object]] = None
        normalized_symbol = str(stock_code or "").strip().upper()

        for stock in account_stocks:
            quantity = self._safe_int(stock.get("quantity"))
            if quantity <= 0:
                continue
            cost_price = self._safe_float(stock.get("cost_price"))
            latest_price = self._resolve_account_holding_price(
                stock,
                account_name=normalized_account_name,
                focus_asset_id=asset_id,
                focus_portfolio_stock_id=portfolio_stock_id,
                focus_symbol=normalized_symbol,
                current_market_price=current_market_price,
            )
            if latest_price <= 0:
                latest_price = cost_price
            market_value = latest_price * quantity
            cost_value = cost_price * quantity
            total_market_value += market_value
            total_profit_loss += market_value - cost_value
            positions_count += 1

            if (
                (asset_id and int(stock.get("id") or 0) == int(asset_id))
                or (portfolio_stock_id and int(stock.get("id") or 0) == int(portfolio_stock_id))
                or str(stock.get("code") or stock.get("symbol") or "").strip().upper() == normalized_symbol
            ):
                current_position = {
                    "quantity": quantity,
                    "cost_price": cost_price,
                    "current_price": latest_price,
                    "market_value": market_value,
                    "position_date": position_date,
                    "status": stock.get("status") or (asset or {}).get("status"),
                }

        configured_total_assets = self._safe_float(
            getattr(self.db.portfolio_db, "get_account_total_assets", lambda *_args, **_kwargs: 0.0)(
                normalized_account_name,
                0.0,
            )
        )
        fallback_position_value = position_cost * position_quantity if has_position else 0.0
        effective_total_value = configured_total_assets if configured_total_assets > 0 else max(
            total_market_value,
            fallback_position_value,
        )
        available_cash = max(0.0, effective_total_value - total_market_value) if effective_total_value > 0 else 0.0
        position_usage_pct = (total_market_value / effective_total_value) if effective_total_value > 0 else 0.0

        if current_position is None and has_position:
            latest_price = current_market_price if current_market_price > 0 else position_cost
            current_market_value = latest_price * position_quantity
            current_position = {
                "quantity": position_quantity,
                "cost_price": position_cost,
                "current_price": latest_price,
                "market_value": current_market_value,
                "position_date": position_date,
                "status": (asset or {}).get("status"),
            }
            total_market_value += current_market_value
            total_profit_loss += current_market_value - fallback_position_value
            positions_count += 1
            if configured_total_assets <= 0:
                effective_total_value = max(effective_total_value, current_market_value)
                available_cash = max(0.0, effective_total_value - total_market_value) if effective_total_value > 0 else 0.0
                position_usage_pct = (total_market_value / effective_total_value) if effective_total_value > 0 else 0.0

        account_info = {
            "account_name": normalized_account_name,
            "available_cash": available_cash,
            "total_value": effective_total_value,
            "configured_total_assets": configured_total_assets,
            "total_market_value": total_market_value,
            "position_usage_pct": position_usage_pct,
            "positions_count": positions_count,
            "total_profit_loss": total_profit_loss,
        }
        if current_position:
            current_market_value = self._safe_float(current_position.get("market_value"))
            account_info["current_position"] = {
                **current_position,
                "position_pct": (current_market_value / effective_total_value) if effective_total_value > 0 else 0.0,
            }
        return account_info

    def _resolve_task_risk_profile(self, account_name: str, task_context: Optional[Dict]) -> Dict[str, int]:
        account_profile = self.db.monitoring_repository.resolve_shared_risk_profile()
        overrides = task_context or {}
        return {
            "position_size_pct": int(overrides.get("position_size_pct") or account_profile["position_size_pct"]),
            "total_position_pct": int(overrides.get("total_position_pct") or account_profile["total_position_pct"]),
            "stop_loss_pct": int(overrides.get("stop_loss_pct") or account_profile["stop_loss_pct"]),
            "take_profit_pct": int(overrides.get("take_profit_pct") or account_profile["take_profit_pct"]),
        }

    @staticmethod
    def _run_with_timeout(func, timeout_seconds: int, *args, **kwargs):
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError as exc:
            future.cancel()
            raise TimeoutError(f"operation_timed_out_after_{timeout_seconds}s") from exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def _map_action_to_rating(action: str, fallback: str = "持有") -> str:
        normalized = str(action or "").upper()
        if normalized == "BUY":
            return "买入"
        if normalized == "SELL":
            return "卖出"
        return fallback or "持有"

    @staticmethod
    def _build_entry_range_text(entry_min: Optional[float], entry_max: Optional[float]) -> str:
        if entry_min is None and entry_max is None:
            return "N/A"
        if entry_min is None:
            return f"{entry_max:.3f}"
        if entry_max is None:
            return f"{entry_min:.3f}"
        return f"{entry_min:.3f}-{entry_max:.3f}"

    @staticmethod
    def _format_price_text(value: Optional[float]) -> str:
        if value is None:
            return "N/A"
        return f"{float(value):.3f}"

    @staticmethod
    def _canonical_action_detail(action: object, detail: object) -> str:
        normalized_action = str(action or "").strip().upper()
        text = str(detail or "").strip()
        if normalized_action == "HOLD":
            return "持有" if text in {"", "持有", "观望", "等待"} else text
        if normalized_action == "BUY":
            return text or "买入"
        if normalized_action == "SELL":
            return text or "卖出"
        return text

    @staticmethod
    def _reasoning_with_appendix(reasoning: object, appendix: str) -> str:
        base = str(reasoning or "").strip()
        appendix = str(appendix or "").strip()
        if not appendix:
            return base
        if not base:
            return appendix
        return f"{base}\n\n补充说明：{appendix}"

    @staticmethod
    def _reasoning_vetoes_buy(reasoning: object) -> bool:
        text = str(reasoning or "").strip()
        if not text:
            return False
        veto_patterns = (
            r"暂不(?:执行)?(?:加仓|买入)",
            r"不(?:宜|建议|执行).{0,6}(?:加仓|买入)",
            r"等待.{0,30}(?:再|后再)考虑.{0,12}(?:加仓|买入)",
            r"(?:建议|应|宜)?(?:维持|继续)持有",
            r"暂不执行",
        )
        return any(re.search(pattern, text) for pattern in veto_patterns)

    @staticmethod
    def _reasoning_vetoes_sell(reasoning: object) -> bool:
        text = str(reasoning or "").strip()
        if not text:
            return False
        veto_patterns = (
            r"暂不(?:执行)?(?:减仓|卖出|清仓)",
            r"不(?:宜|建议|执行).{0,6}(?:减仓|卖出|清仓)",
            r"等待.{0,30}(?:再|后再)考虑.{0,12}(?:减仓|卖出|清仓)",
            r"(?:建议|应|宜)?(?:维持|继续)持有",
            r"暂不执行",
        )
        return any(re.search(pattern, text) for pattern in veto_patterns)

    def _reconcile_reasoning_action_consistency(
        self,
        *,
        decision: Dict[str, object],
        has_position: bool,
        account_info: Optional[Dict[str, object]],
        risk_profile: Optional[Dict[str, object]],
    ) -> Dict[str, object]:
        action = str(decision.get("action") or "").strip().upper()
        reasoning = str(decision.get("reasoning") or "").strip()
        vetoed_by_reasoning = (
            (action == "BUY" and self._reasoning_vetoes_buy(reasoning))
            or (action == "SELL" and self._reasoning_vetoes_sell(reasoning))
        )
        if not vetoed_by_reasoning:
            return decision

        decision["action"] = "HOLD"
        if hasattr(self.llm_client, "_resolve_action_detail"):
            decision["action_detail"] = self.llm_client._resolve_action_detail(
                decision.get("action_detail"),
                action="HOLD",
                has_position=has_position,
            )
        else:
            decision["action_detail"] = "持有" if has_position else "观望"

        if hasattr(self.llm_client, "_resolve_swing_execution_mode"):
            decision["swing_execution_mode"] = self.llm_client._resolve_swing_execution_mode(
                decision.get("swing_execution_mode"),
                action="HOLD",
                action_detail=decision.get("action_detail"),
                has_position=has_position,
                reasoning=reasoning,
            )
        else:
            decision["swing_execution_mode"] = "trend_hold" if has_position else "watch_hold"

        decision["action_ratio_pct"] = None
        if hasattr(self.llm_client, "_attach_execution_targets"):
            decision = self.llm_client._attach_execution_targets(
                decision,
                account_info=account_info,
                risk_profile=risk_profile,
            )
        return decision

    @classmethod
    def _extract_monitor_levels_from_strategy_context(cls, strategy_context: Optional[Dict]) -> Optional[Dict]:
        if not isinstance(strategy_context, dict) or not strategy_context:
            return None
        raw_levels = {
            "entry_min": strategy_context.get("entry_min"),
            "entry_max": strategy_context.get("entry_max"),
            "take_profit": strategy_context.get("take_profit"),
            "stop_loss": strategy_context.get("stop_loss"),
        }
        return cls._normalize_monitor_levels_payload(raw_levels)

    @staticmethod
    def _rating_supports_buying(rating: object) -> bool:
        text = str(rating or "").strip()
        return any(keyword in text for keyword in ("买入", "强烈买入", "加仓"))

    def _build_plan_signal(
        self,
        *,
        strategy_context: Optional[Dict],
        market_data: Dict,
        has_position: bool,
    ) -> Optional[Dict[str, object]]:
        monitor_levels = self._extract_monitor_levels_from_strategy_context(strategy_context)
        current_price = self._float_or_none((market_data or {}).get("current_price"))
        if current_price is None or not monitor_levels:
            return None

        take_profit = self._float_or_none(monitor_levels.get("take_profit"))
        stop_loss = self._float_or_none(monitor_levels.get("stop_loss"))
        entry_min = self._float_or_none(monitor_levels.get("entry_min"))
        entry_max = self._float_or_none(monitor_levels.get("entry_max"))
        rating = (strategy_context or {}).get("rating")

        if has_position and stop_loss is not None and current_price <= stop_loss:
            return {
                "action": "SELL",
                "action_detail": "止损",
                "reasoning": f"当前价格 {current_price:.3f} 已触发深度分析交易计划的止损位 {stop_loss:.3f}。",
                "monitor_levels": monitor_levels,
            }
        if has_position and take_profit is not None and current_price >= take_profit:
            return {
                "action": "SELL",
                "action_detail": "止盈",
                "reasoning": f"当前价格 {current_price:.3f} 已触发深度分析交易计划的止盈位 {take_profit:.3f}。",
                "monitor_levels": monitor_levels,
            }
        if (
            entry_min is not None
            and entry_max is not None
            and entry_min <= current_price <= entry_max
            and self._rating_supports_buying(rating)
        ):
            return {
                "action": "BUY",
                "action_detail": "加仓" if has_position else "买入",
                "reasoning": (
                    f"当前价格 {current_price:.3f} 落在深度分析交易计划进场区间 "
                    f"{entry_min:.3f}-{entry_max:.3f} 内。"
                ),
                "monitor_levels": monitor_levels,
            }
        return None

    def _apply_strategy_plan_guardrails(
        self,
        *,
        decision: Dict[str, object],
        strategy_context: Optional[Dict],
        market_data: Dict,
        has_position: bool,
    ) -> Dict[str, object]:
        decision_monitor_levels = self._normalize_monitor_levels(decision)
        plan_monitor_levels = self._extract_monitor_levels_from_strategy_context(strategy_context)
        if plan_monitor_levels and not decision_monitor_levels:
            decision["monitor_levels"] = plan_monitor_levels

        current_price = self._float_or_none((market_data or {}).get("current_price"))
        plan_signal = self._build_plan_signal(
            strategy_context=strategy_context,
            market_data=market_data,
            has_position=has_position,
        )
        action = str(decision.get("action") or "").upper()
        rating = (strategy_context or {}).get("rating")

        if plan_signal and action == "HOLD":
            decision["action"] = str(plan_signal.get("action") or "HOLD")
            decision["action_detail"] = str(plan_signal.get("action_detail") or decision.get("action_detail") or "").strip()
            decision["reasoning"] = self._reasoning_with_appendix(
                decision.get("reasoning"),
                str(plan_signal.get("reasoning") or ""),
            )
            decision["monitor_levels"] = plan_signal.get("monitor_levels") or plan_monitor_levels or decision_monitor_levels or {}
            action = str(decision.get("action") or "").upper()

        if action == "BUY" and plan_monitor_levels:
            if not self._rating_supports_buying(rating):
                decision["action"] = "HOLD"
                decision["action_detail"] = "观望" if not has_position else "持有"
                decision["action_ratio_pct"] = None
                decision["reasoning"] = self._reasoning_with_appendix(
                    decision.get("reasoning"),
                    "最新深度分析基线未给出买入/加仓评级，盘中规则禁止脱离交易计划追价买入。",
                )
            elif current_price is not None and plan_monitor_levels:
                entry_min = self._float_or_none(plan_monitor_levels.get("entry_min"))
                entry_max = self._float_or_none(plan_monitor_levels.get("entry_max"))
                if (
                    entry_min is not None
                    and entry_max is not None
                    and not (entry_min <= current_price <= entry_max)
                ):
                    decision["action"] = "HOLD"
                    decision["action_detail"] = "观望" if not has_position else "持有"
                    decision["action_ratio_pct"] = None
                    decision["reasoning"] = self._reasoning_with_appendix(
                        decision.get("reasoning"),
                        (
                            f"当前价格 {current_price:.3f} 不在深度分析交易计划进场区间 "
                            f"{entry_min:.3f}-{entry_max:.3f} 内，已阻断追高/偏离计划买入。"
                        ),
                    )

        if action == "SELL" and has_position and plan_monitor_levels and current_price is not None:
            take_profit = self._float_or_none(plan_monitor_levels.get("take_profit"))
            stop_loss = self._float_or_none(plan_monitor_levels.get("stop_loss"))
            sell_triggered = (
                (take_profit is not None and current_price >= take_profit)
                or (stop_loss is not None and current_price <= stop_loss)
            )
            if not sell_triggered:
                decision["action"] = "HOLD"
                decision["action_detail"] = "持有"
                decision["action_ratio_pct"] = None
                decision["reasoning"] = self._reasoning_with_appendix(
                    decision.get("reasoning"),
                    "当前价格尚未触发深度分析交易计划的止盈/止损阈值，盘中规则不执行脱离计划的卖出动作。",
                )

        return decision

    def _standardize_decision_schema(
        self,
        *,
        decision: Dict[str, object],
        strategy_context: Optional[Dict],
    ) -> Dict[str, object]:
        monitor_levels = self._normalize_monitor_levels(decision) or self._extract_monitor_levels_from_strategy_context(strategy_context)
        if monitor_levels and not self._normalize_monitor_levels(decision):
            decision["monitor_levels"] = monitor_levels

        entry_min = self._float_or_none((monitor_levels or {}).get("entry_min"))
        entry_max = self._float_or_none((monitor_levels or {}).get("entry_max"))
        take_profit = self._float_or_none((monitor_levels or {}).get("take_profit"))
        stop_loss = self._float_or_none((monitor_levels or {}).get("stop_loss"))
        confidence_level = decision.get("confidence_level")
        if confidence_level in (None, ""):
            confidence_level = decision.get("confidence")

        decision["rating"] = str(
            decision.get("rating")
            or self._map_action_to_rating(str(decision.get("action") or ""), fallback="持有")
            or (strategy_context or {}).get("rating")
            or "持有"
        ).strip() or "持有"
        decision["confidence_level"] = confidence_level
        decision["entry_range"] = decision.get("entry_range") or self._build_entry_range_text(entry_min, entry_max)
        decision["take_profit"] = decision.get("take_profit") if decision.get("take_profit") not in (None, "") else take_profit
        decision["stop_loss"] = decision.get("stop_loss") if decision.get("stop_loss") not in (None, "") else stop_loss
        decision["advice"] = str(
            decision.get("advice")
            or decision.get("reasoning")
            or (strategy_context or {}).get("summary")
            or ""
        ).strip()
        return decision

    def _prepare_market_structure_features(self, market_data: Dict[str, object]) -> Dict[str, object]:
        prepared = dict(market_data or {})
        feature_beacons = self.llm_client._derive_feature_beacons(prepared)
        prepared["feature_beacons"] = feature_beacons
        trend_anchor_type, trend_anchor_value = self.llm_client._select_trend_anchor(prepared)
        prepared["trend_anchor_type"] = trend_anchor_type
        prepared["trend_anchor_value"] = trend_anchor_value
        return prepared

    @classmethod
    def _resolve_effective_swing_type_code(
        cls,
        *,
        strategy_context: Optional[Dict[str, object]],
        decision: Optional[Dict[str, object]] = None,
    ) -> str:
        upgraded_code = normalize_swing_type((decision or {}).get("upgraded_swing_type"))
        if upgraded_code:
            return upgraded_code
        return normalize_swing_type((strategy_context or {}).get("swing_type_code") or (strategy_context or {}).get("swing_type"))

    @classmethod
    def _apply_atr_guardrail(
        cls,
        *,
        decision: Dict[str, object],
        strategy_context: Optional[Dict[str, object]],
        market_data: Optional[Dict[str, object]],
    ) -> tuple[Dict[str, object], Optional[Dict[str, object]]]:
        current_price = cls._float_or_none((market_data or {}).get("current_price"))
        atr14 = cls._float_or_none(
            (decision or {}).get("atr14")
            or (market_data or {}).get("atr14")
            or (strategy_context or {}).get("atr14")
        )
        swing_type_code = cls._resolve_effective_swing_type_code(
            strategy_context=strategy_context,
            decision=decision,
        )
        if current_price is None or atr14 is None:
            return decision, None

        multiplier = 2.5 if swing_type_code == "standard_swing" else 1.2
        atr_stop_floor = round(current_price - atr14 * multiplier, 3)
        decision["atr14"] = round(atr14, 4)
        decision["atr14_pct"] = round((atr14 / current_price) * 100, 4) if current_price > 0 else None
        decision["atr_stop_floor"] = atr_stop_floor

        clamped_fields: List[str] = []
        top_level_stop = cls._float_or_none(decision.get("stop_loss"))
        if top_level_stop is not None and top_level_stop < atr_stop_floor:
            decision["stop_loss"] = atr_stop_floor
            clamped_fields.append("stop_loss")

        key_levels = dict(decision.get("key_price_levels") or {}) if isinstance(decision.get("key_price_levels"), dict) else {}
        key_stop = cls._float_or_none(key_levels.get("stop_loss"))
        if key_stop is not None and key_stop < atr_stop_floor:
            key_levels["stop_loss"] = atr_stop_floor
            decision["key_price_levels"] = key_levels
            clamped_fields.append("key_price_levels.stop_loss")

        monitor_levels = cls._normalize_monitor_levels_payload(
            decision.get("monitor_levels") if isinstance(decision, dict) else None
        )
        if monitor_levels:
            if float(monitor_levels["stop_loss"]) < atr_stop_floor:
                monitor_levels["stop_loss"] = atr_stop_floor
                decision["monitor_levels"] = monitor_levels
                clamped_fields.append("monitor_levels.stop_loss")

        if not clamped_fields:
            return decision, None

        return decision, {
            "atr_stop_floor": atr_stop_floor,
            "clamped_fields": clamped_fields,
            "swing_type_code": swing_type_code or "micro_swing",
        }

    @classmethod
    def _build_position_cycle_runtime_snapshot(
        cls,
        *,
        decision: Dict[str, object],
        strategy_context: Optional[Dict[str, object]],
        market_data: Optional[Dict[str, object]],
        position_date: Optional[str],
    ) -> Dict[str, object]:
        effective_swing_type_code = cls._resolve_effective_swing_type_code(
            strategy_context=strategy_context,
            decision=decision,
        )
        effective_swing_type = swing_type_label(effective_swing_type_code)
        decision["upgraded_swing_type"] = swing_type_label(decision.get("upgraded_swing_type"))
        current_price = cls._float_or_none((market_data or {}).get("current_price"))
        atr14 = cls._float_or_none(
            decision.get("atr14")
            or (market_data or {}).get("atr14")
            or (strategy_context or {}).get("atr14")
        )
        atr14_pct = cls._float_or_none(
            decision.get("atr14_pct")
            or (market_data or {}).get("atr14_pct")
            or (strategy_context or {}).get("atr14_pct")
        )
        anchor_type = str(
            decision.get("trend_anchor_type")
            or (market_data or {}).get("trend_anchor_type")
            or (strategy_context or {}).get("trend_anchor_type")
            or ("MA20" if atr14_pct is not None and atr14_pct > 4.0 else "MA10")
        ).strip().upper()
        anchor_value = cls._float_or_none(
            decision.get("trend_anchor_value")
            or (market_data or {}).get("trend_anchor_value")
            or (market_data or {}).get("ma20" if anchor_type == "MA20" else "ma10")
            or (strategy_context or {}).get("trend_anchor_value")
        )
        holding_days = SmartMonitorDeepSeek._estimate_holding_trading_days(
            position_date=position_date,
            market_data=market_data,
        )
        account_position = (
            (market_data or {}).get("account_position")
            if isinstance((market_data or {}).get("account_position"), dict)
            else {}
        )
        cost_price = cls._float_or_none(account_position.get("cost_price"))
        profit_loss_pct = None
        if current_price is not None and cost_price is not None and cost_price > 0:
            profit_loss_pct = (current_price - cost_price) / cost_price * 100
        structure_state = str(decision.get("structure_state") or "结构状态未明确").strip() or "结构状态未明确"
        trend_following_active = bool(
            effective_swing_type_code == "standard_swing"
            and holding_days is not None
            and holding_days >= 10
            and profit_loss_pct is not None
            and atr14_pct is not None
            and profit_loss_pct >= atr14_pct * 2
            and current_price is not None
            and anchor_value is not None
            and current_price >= anchor_value
            and structure_state != "筑顶高位派发"
        )
        decision["trend_following_active"] = trend_following_active
        decision["trend_anchor_type"] = anchor_type
        decision["trend_anchor_value"] = anchor_value
        if decision.get("atr_stop_floor") in (None, "") and current_price is not None and atr14 is not None:
            multiplier = 2.5 if effective_swing_type_code == "standard_swing" else 1.2
            decision["atr_stop_floor"] = round(current_price - atr14 * multiplier, 3)

        return {
            "holding_period": str((strategy_context or {}).get("holding_period") or "").strip(),
            "structure_state": structure_state,
            "structure_state_reason": str(decision.get("structure_state_reason") or "").strip(),
            "atr14": atr14,
            "atr14_pct": atr14_pct,
            "atr_stop_floor": cls._float_or_none(decision.get("atr_stop_floor")),
            "trend_anchor_type": anchor_type,
            "trend_anchor_value": anchor_value,
            "trend_following_active": trend_following_active,
            "trend_following_activated_at": (
                str((strategy_context or {}).get("trend_following_activated_at") or "").strip()
                or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ) if trend_following_active else None,
            "feature_beacons": (
                decision.get("feature_beacons")
                if isinstance(decision.get("feature_beacons"), list)
                else ((market_data or {}).get("feature_beacons") if isinstance((market_data or {}).get("feature_beacons"), list) else [])
            ),
            "swing_type": effective_swing_type,
        }

    @classmethod
    def _resolve_upgrade_writeback(
        cls,
        *,
        decision: Dict[str, object],
        strategy_context: Optional[Dict[str, object]],
        market_data: Optional[Dict[str, object]],
    ) -> Optional[Dict[str, str]]:
        current_swing_type_code = normalize_swing_type((strategy_context or {}).get("swing_type"))
        upgraded_swing_type_code = normalize_swing_type(decision.get("upgraded_swing_type"))
        feature_beacons = (
            [str(item).strip() for item in decision.get("feature_beacons", []) if str(item).strip()]
            if isinstance(decision.get("feature_beacons"), list)
            else []
        )
        if not feature_beacons:
            feature_beacons = [str(item).strip() for item in ((market_data or {}).get("feature_beacons") or []) if str(item).strip()]
        if (
            not bool(decision.get("swing_type_upgrade"))
            or current_swing_type_code != "micro_swing"
            or upgraded_swing_type_code != "standard_swing"
            or not feature_beacons
        ):
            decision["swing_type_upgrade"] = False
            decision["upgraded_swing_type"] = ""
            return None
        decision["upgraded_swing_type"] = "标准波段"
        return {
            "swing_type": "标准波段",
            "swing_type_reason": str(decision.get("upgrade_reason") or "").strip() or str(decision.get("reasoning") or "").strip(),
        }

    def _sync_position_cycle_runtime_state(
        self,
        *,
        asset_id: Optional[int],
        decision_id: int,
        decision: Dict[str, object],
        strategy_context: Optional[Dict[str, object]],
        market_data: Optional[Dict[str, object]],
        position_date: Optional[str],
        has_position: bool,
    ) -> None:
        if asset_id is None or not has_position:
            return
        asset = self.db.asset_repository.get_asset(int(asset_id))
        if not asset or str(asset.get("status") or "").strip().lower() != STATUS_PORTFOLIO:
            return

        upgrade_writeback = self._resolve_upgrade_writeback(
            decision=decision,
            strategy_context=strategy_context,
            market_data=market_data,
        )
        effective_swing_type = (
            upgrade_writeback["swing_type"]
            if upgrade_writeback
            else str((strategy_context or {}).get("swing_type") or "").strip()
        )
        effective_reason = (
            upgrade_writeback["swing_type_reason"]
            if upgrade_writeback
            else str((strategy_context or {}).get("swing_type_reason") or "").strip()
        )
        runtime_snapshot = self._build_position_cycle_runtime_snapshot(
            decision=decision,
            strategy_context={
                **(strategy_context or {}),
                **({"swing_type": effective_swing_type, "swing_type_reason": effective_reason} if effective_swing_type else {}),
            },
            market_data=market_data,
            position_date=position_date,
        )
        baseline_source = str(
            (strategy_context or {}).get("position_cycle_baseline_source")
            or (strategy_context or {}).get("analysis_source")
            or "smart_monitor_intraday"
        ).strip() or "smart_monitor_intraday"
        baseline_analysis_id = (strategy_context or {}).get("position_cycle_baseline_analysis_id") or (strategy_context or {}).get("origin_analysis_id")
        self.db.asset_repository.set_open_position_cycle_baseline(
            int(asset_id),
            swing_type=effective_swing_type,
            swing_type_reason=effective_reason,
            holding_period=runtime_snapshot.get("holding_period"),
            baseline_source=baseline_source,
            baseline_analysis_id=baseline_analysis_id,
            baseline_decision_id=decision_id,
            overwrite=True,
            baseline_snapshot_extra=runtime_snapshot,
        )

    @staticmethod
    def _normalize_notification_class(value: object) -> str:
        normalized = str(value or "").strip().lower().replace("-", "_")
        aliases = {
            "focus": "price_alert",
            "focus_alert": "price_alert",
            "关注": "price_alert",
            "关注提醒": "price_alert",
            "price": "price_alert",
            "price_alert": "price_alert",
            "价格": "price_alert",
            "价格提醒": "price_alert",
            "risk": "risk_alert",
            "risk_alert": "risk_alert",
            "风险": "risk_alert",
            "风险预警": "risk_alert",
            "profit": "profit_alert",
            "profit_alert": "profit_alert",
            "收益": "profit_alert",
            "收益信号": "profit_alert",
            "system": "system_alert",
            "system_alert": "system_alert",
            "系统": "system_alert",
            "系统通知": "system_alert",
            "other": "other_alert",
            "other_alert": "other_alert",
            "提醒": "other_alert",
        }
        return aliases.get(normalized, "")

    @staticmethod
    def _notification_class_label(value: object) -> str:
        normalized = SmartMonitorEngine._normalize_notification_class(value)
        labels = {
            "price_alert": "价格提醒",
            "risk_alert": "风险预警",
            "profit_alert": "收益信号",
            "system_alert": "系统通知",
            "other_alert": "其他提醒",
        }
        return labels.get(normalized, "提醒")

    @classmethod
    def _derive_notification_class(
        cls,
        *,
        action: str,
        action_detail: str,
        swing_execution_mode: str,
        has_position: bool,
        decision: Optional[Dict] = None,
    ) -> str:
        decision = decision or {}
        explicit = cls._normalize_notification_class(decision.get("notification_class"))
        if explicit:
            return explicit

        normalized_action = str(action or "").upper()
        normalized_detail = str(action_detail or "").strip()
        normalized_swing = str(swing_execution_mode or "").strip().lower()

        if normalized_action == "BUY":
            if not has_position:
                return "price_alert"
            if normalized_swing in {"pullback_add", "breakout_add"} or "加仓" in normalized_detail:
                return "price_alert"
            return "price_alert"

        if normalized_action == "SELL":
            if normalized_detail == "清仓" or normalized_swing == "defensive_exit":
                return "risk_alert"
            if normalized_swing == "proactive_trim" or "止盈" in normalized_detail or "锁盈" in normalized_detail:
                return "profit_alert"
            if normalized_swing == "defensive_trim" or "减仓" in normalized_detail:
                return "risk_alert"
            return "price_alert"

        if normalized_action == "HOLD":
            return "price_alert"

        return "price_alert"

    @staticmethod
    def _normalize_monitor_levels_payload(raw_levels: Optional[Dict]) -> Optional[Dict]:
        if not isinstance(raw_levels, dict):
            return None
        normalized: Dict[str, float] = {}
        for key in ("entry_min", "entry_max", "take_profit", "stop_loss"):
            value = raw_levels.get(key)
            if value in (None, ""):
                return None
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError):
                return None
        take_profit_max = raw_levels.get("take_profit_max")
        if take_profit_max not in (None, ""):
            try:
                normalized["take_profit_max"] = float(take_profit_max)
            except (TypeError, ValueError):
                pass
        if (
            normalized.get("take_profit_max") is not None
            and normalized["take_profit_max"] < normalized["take_profit"]
        ):
            normalized["take_profit_max"] = normalized["take_profit"]
        return normalized

    @classmethod
    def _normalize_monitor_levels(cls, decision: Dict) -> Optional[Dict]:
        return cls._normalize_monitor_levels_payload(
            decision.get("monitor_levels") if isinstance(decision, dict) else None
        )

    @classmethod
    def _classify_decision_change(
        cls,
        *,
        latest_decision: Optional[Dict],
        current_decision: Dict,
    ) -> Dict[str, bool]:
        latest_action = str((latest_decision or {}).get("action") or "").upper()
        current_action = str(current_decision.get("action") or "").upper()
        latest_action_detail = cls._canonical_action_detail(latest_action, (latest_decision or {}).get("action_detail"))
        current_action_detail = cls._canonical_action_detail(current_action, current_decision.get("action_detail"))
        latest_swing_mode = str((latest_decision or {}).get("swing_execution_mode") or "").strip()
        current_swing_mode = str(current_decision.get("swing_execution_mode") or "").strip()
        latest_action_ratio_pct = cls._float_or_none((latest_decision or {}).get("action_ratio_pct"))
        current_action_ratio_pct = cls._float_or_none(current_decision.get("action_ratio_pct"))
        action_changed = (
            not latest_action
            or latest_action != current_action
            or (current_action_detail and latest_action_detail != current_action_detail)
            or (current_swing_mode and latest_swing_mode != current_swing_mode)
            or (
                current_action_ratio_pct is not None
                and latest_action_ratio_pct is not None
                and abs(current_action_ratio_pct - latest_action_ratio_pct) > 0.01
            )
            or (current_action_ratio_pct is not None and latest_action_ratio_pct is None)
        )

        latest_levels = cls._normalize_monitor_levels_payload((latest_decision or {}).get("monitor_levels"))
        current_levels = cls._normalize_monitor_levels(current_decision)
        thresholds_changed = False
        if current_levels is not None:
            if latest_levels is None:
                thresholds_changed = True
            else:
                thresholds_changed = any(
                    abs(float(latest_levels[key]) - float(current_levels[key])) > 0.01
                    for key in ("entry_min", "entry_max", "take_profit", "stop_loss")
                ) or (
                    cls._float_or_none(latest_levels.get("take_profit_max")) is not None
                    or cls._float_or_none(current_levels.get("take_profit_max")) is not None
                ) and (
                    abs(
                        float(cls._float_or_none(latest_levels.get("take_profit_max")) or latest_levels["take_profit"])
                        - float(cls._float_or_none(current_levels.get("take_profit_max")) or current_levels["take_profit"])
                    ) > 0.01
                )

        return {
            "action_changed": action_changed,
            "thresholds_changed": thresholds_changed,
            "decision_changed": action_changed or thresholds_changed,
        }

    @staticmethod
    def _build_decision_context_delta(
        *,
        latest_decision: Optional[Dict],
        current_decision: Dict,
        market_data: Dict,
        change_flags: Dict[str, bool],
    ) -> Dict[str, object]:
        latest_decision = latest_decision or {}
        current_intraday = market_data.get("intraday_context") if isinstance(market_data.get("intraday_context"), dict) else {}
        previous_intraday = latest_decision.get("decision_context") if isinstance(latest_decision.get("decision_context"), dict) else {}

        def _to_float(value: object) -> Optional[float]:
            try:
                return float(value) if value not in (None, "") else None
            except (TypeError, ValueError):
                return None

        def _round_delta(value: Optional[float]) -> Optional[float]:
            if value is None:
                return None
            return round(value, 4)

        def _format_action_signature(action: object, detail: object, ratio_pct: object) -> str:
            detail_text = str(detail or action or "").strip()
            try:
                numeric = float(ratio_pct) if ratio_pct not in (None, "") else None
            except (TypeError, ValueError):
                numeric = None
            if numeric is not None and str(action or "").strip().upper() in {"BUY", "SELL"} and numeric > 0:
                if abs(numeric - round(numeric)) < 1e-6:
                    return f"{detail_text}{int(round(numeric))}%"
                return f"{detail_text}{numeric:.1f}%"
            return detail_text

        def _format_swing_mode_label(value: object) -> str:
            mapping = {
                "pullback_entry": "回踩建仓",
                "breakout_entry": "突破建仓",
                "pullback_add": "回踩确认加仓",
                "breakout_add": "突破确认加仓",
                "proactive_trim": "主动减仓锁盈",
                "defensive_trim": "防守减仓",
                "defensive_exit": "防守清仓",
                "trend_hold": "趋势持有",
                "watch_hold": "观察持有",
            }
            normalized = str(value or "").strip().lower()
            return mapping.get(normalized, normalized)

        delta_payload: Dict[str, object] = {
            "previous_action": str(latest_decision.get("action") or "").upper() or None,
            "previous_action_detail": str(latest_decision.get("action_detail") or "").strip() or None,
            "previous_swing_execution_mode": str(
                latest_decision.get("swing_execution_mode")
                or previous_intraday.get("swing_execution_mode")
                or ""
            ).strip() or None,
            "swing_execution_mode": str(current_decision.get("swing_execution_mode") or "").strip() or None,
            "previous_action_ratio_pct": latest_decision.get("action_ratio_pct"),
            "previous_confidence": latest_decision.get("confidence"),
            "previous_risk_level": latest_decision.get("risk_level"),
            "previous_decision_time": latest_decision.get("decision_time"),
            "action_changed": bool(change_flags.get("action_changed")),
            "thresholds_changed": bool(change_flags.get("thresholds_changed")),
            "decision_changed": bool(change_flags.get("decision_changed")),
            "structure_state": str(current_decision.get("structure_state") or "").strip() or None,
            "structure_state_reason": str(current_decision.get("structure_state_reason") or "").strip() or None,
            "trend_following_active": bool(current_decision.get("trend_following_active")),
            "trend_anchor_type": str(current_decision.get("trend_anchor_type") or "").strip() or None,
            "trend_anchor_value": _to_float(current_decision.get("trend_anchor_value")),
            "atr14": _to_float(current_decision.get("atr14")),
            "atr14_pct": _to_float(current_decision.get("atr14_pct")),
            "atr_stop_floor": _to_float(current_decision.get("atr_stop_floor")),
            "swing_type_upgrade": bool(current_decision.get("swing_type_upgrade")),
            "upgraded_swing_type": str(current_decision.get("upgraded_swing_type") or "").strip() or None,
            "upgrade_reason": str(current_decision.get("upgrade_reason") or "").strip() or None,
            "feature_beacons": current_decision.get("feature_beacons") if isinstance(current_decision.get("feature_beacons"), list) else [],
        }

        previous_bias_text = str(previous_intraday.get("intraday_bias_text") or "").strip()
        current_bias_text = str(current_intraday.get("intraday_bias_text") or "").strip()
        if previous_bias_text or current_bias_text:
            delta_payload["previous_intraday_bias_text"] = previous_bias_text or None
            delta_payload["intraday_bias_changed"] = previous_bias_text != current_bias_text

        previous_last_5m = _to_float(previous_intraday.get("last_5m_change_pct"))
        current_last_5m = _to_float(current_intraday.get("last_5m_change_pct"))
        if previous_last_5m is not None and current_last_5m is not None:
            delta_payload["last_5m_change_delta"] = _round_delta(current_last_5m - previous_last_5m)

        previous_position_pct = _to_float(previous_intraday.get("price_position_pct"))
        current_position_pct = _to_float(current_intraday.get("price_position_pct"))
        if previous_position_pct is not None and current_position_pct is not None:
            delta_payload["price_position_pct_delta"] = _round_delta(current_position_pct - previous_position_pct)

        previous_volume_acc = _to_float(previous_intraday.get("volume_acceleration_ratio"))
        current_volume_acc = _to_float(current_intraday.get("volume_acceleration_ratio"))
        if previous_volume_acc is not None and current_volume_acc is not None:
            delta_payload["volume_acceleration_ratio_delta"] = _round_delta(current_volume_acc - previous_volume_acc)

        previous_labels = previous_intraday.get("intraday_signal_labels")
        previous_labels = previous_labels if isinstance(previous_labels, list) else []
        current_labels = current_intraday.get("intraday_signal_labels")
        current_labels = current_labels if isinstance(current_labels, list) else []
        new_labels = [str(label).strip() for label in current_labels if str(label).strip() and str(label).strip() not in previous_labels]
        if new_labels:
            delta_payload["new_intraday_signal_labels"] = new_labels[:3]

        summary_parts: List[str] = []
        previous_action = str(latest_decision.get("action") or "").upper()
        previous_action_detail = SmartMonitorEngine._canonical_action_detail(previous_action, latest_decision.get("action_detail"))
        previous_swing_execution_mode = str(
            latest_decision.get("swing_execution_mode") or previous_intraday.get("swing_execution_mode") or ""
        ).strip()
        previous_action_ratio_pct = latest_decision.get("action_ratio_pct")
        current_action = str(current_decision.get("action") or "").upper()
        current_action_detail = SmartMonitorEngine._canonical_action_detail(current_action, current_decision.get("action_detail"))
        current_swing_execution_mode = str(current_decision.get("swing_execution_mode") or "").strip()
        current_action_ratio_pct = current_decision.get("action_ratio_pct")
        if previous_action and (
            previous_action != current_action
            or (current_action_detail and previous_action_detail != current_action_detail)
            or (current_swing_execution_mode and previous_swing_execution_mode != current_swing_execution_mode)
            or (
                _to_float(previous_action_ratio_pct) is not None
                and _to_float(current_action_ratio_pct) is not None
                and abs(float(previous_action_ratio_pct) - float(current_action_ratio_pct)) > 0.01
            )
        ):
            previous_action_text = _format_action_signature(previous_action, previous_action_detail, previous_action_ratio_pct)
            current_action_text = _format_action_signature(current_action, current_action_detail, current_action_ratio_pct)
            summary_parts.append(f"动作由{previous_action_text}变为{current_action_text}")
        if previous_swing_execution_mode and current_swing_execution_mode and previous_swing_execution_mode != current_swing_execution_mode:
            summary_parts.append(
                f"执行类型由{_format_swing_mode_label(previous_swing_execution_mode)}变为{_format_swing_mode_label(current_swing_execution_mode)}"
            )
        if delta_payload.get("intraday_bias_changed"):
            summary_parts.append(f"盘中偏向由“{previous_bias_text or '未记录'}”变为“{current_bias_text or '未记录'}”")
        if delta_payload.get("thresholds_changed"):
            summary_parts.append("预警价格区间已更新")
        if new_labels:
            summary_parts.append(f"新增盘中标签：{' / '.join(new_labels[:2])}")
        if not summary_parts:
            summary_parts.append("与上一轮相比未出现足以改变结论的显著新变化")
        delta_payload["delta_summary"] = "；".join(summary_parts)

        return {key: value for key, value in delta_payload.items() if value not in (None, [], {}, "")}

    @staticmethod
    def _parse_decision_time(value: object) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        for candidate in (text.replace("Z", "+00:00"), text):
            try:
                return datetime.fromisoformat(candidate)
            except ValueError:
                continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _same_trading_day(left: object, right: object) -> bool:
        left_dt = SmartMonitorEngine._parse_decision_time(left)
        right_dt = SmartMonitorEngine._parse_decision_time(right)
        if left_dt and right_dt:
            return left_dt.date() == right_dt.date()
        if left_dt:
            return left_dt.date() == datetime.now().date()
        if right_dt:
            return right_dt.date() == datetime.now().date()
        return False

    @staticmethod
    def _float_or_none(value: object) -> Optional[float]:
        try:
            return float(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def _resolve_latest_strategy_context(
        self,
        *,
        stock_code: str,
        account_name: str,
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
        provided_strategy_context: Optional[Dict],
        task_context: Optional[Dict],
    ) -> Dict:
        task_strategy_context = (
            (task_context or {}).get("strategy_context")
            or ((task_context or {}).get("config") or {}).get("strategy_context")
            or {}
        )
        if task_strategy_context:
            return normalize_strategy_context(task_strategy_context)

        latest_context = self.db.analysis_repository.get_latest_strategy_context(
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            symbol=stock_code,
            account_name=account_name,
        ) or {}
        if latest_context:
            return normalize_strategy_context(latest_context)

        return normalize_strategy_context(provided_strategy_context) if provided_strategy_context else {}

    def _find_ai_task_item(
        self,
        stock_code: str,
        account_name: str,
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
    ) -> Optional[Dict]:
        return self.db.monitoring_repository.get_item_by_symbol(
            stock_code,
            monitor_type='ai_task',
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )

    def _sync_runtime_thresholds(
        self,
        *,
        stock_code: str,
        stock_name: str,
        decision: Dict,
        decision_id: int,
        account_name: str,
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
        strategy_context: Optional[Dict],
    ) -> bool:
        from monitor_db import monitor_db as realtime_monitor_db

        task_item = self._find_ai_task_item(stock_code, account_name, asset_id, portfolio_stock_id)
        monitor_levels = self._normalize_monitor_levels(decision)
        if not monitor_levels:
            if task_item:
                self.db.monitoring_repository.record_event(
                    item_id=task_item["id"],
                    event_type="threshold_sync_skipped",
                    message="AI 未返回完整 monitor_levels，跳过实时预警阈值回写",
                    notification_pending=False,
                    sent=True,
                    details={"decision_id": decision_id},
                )
            return False

        asset = self.db.asset_repository.get_asset(asset_id) if asset_id else None
        managed_by_portfolio = bool(asset and asset.get("status") == STATUS_PORTFOLIO)
        inherited_interval = 3
        inherited_notification = True
        inherited_trading_hours = True

        existing_alert = realtime_monitor_db.get_monitor_by_code(
            stock_code,
            managed_only=True if managed_by_portfolio else None,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        if existing_alert:
            inherited_interval = int(existing_alert.get("check_interval") or inherited_interval)
            inherited_notification = bool(existing_alert.get("notification_enabled", inherited_notification))
            inherited_trading_hours = bool(existing_alert.get("trading_hours_only", inherited_trading_hours))
            updated = realtime_monitor_db.apply_runtime_thresholds(
                existing_alert["id"],
                rating=self._map_action_to_rating(decision.get("action"), fallback=(strategy_context or {}).get("rating") or "持有"),
                monitor_levels=monitor_levels,
                origin_decision_id=decision_id,
            )
            if task_item:
                self.db.monitoring_repository.record_event(
                    item_id=task_item["id"],
                    event_type="threshold_sync",
                    message="已更新绑定价格预警的运行时阈值",
                    notification_pending=False,
                    sent=True,
                    details={"decision_id": decision_id, "price_alert_id": existing_alert["id"]},
                )
            return updated

        if task_item:
            inherited_interval = 3
            inherited_notification = bool(task_item.get("notification_enabled", inherited_notification))
            inherited_trading_hours = bool(task_item.get("trading_hours_only", inherited_trading_hours))

        price_alert_id = realtime_monitor_db.add_monitored_stock(
            symbol=stock_code,
            name=stock_name or stock_code,
            rating=self._map_action_to_rating(decision.get("action"), fallback=(strategy_context or {}).get("rating") or "持有"),
            entry_range={"min": monitor_levels["entry_min"], "max": monitor_levels["entry_max"]},
            take_profit=monitor_levels["take_profit"],
            stop_loss=monitor_levels["stop_loss"],
            check_interval=inherited_interval,
            notification_enabled=inherited_notification,
            trading_hours_only=inherited_trading_hours,
            managed_by_portfolio=managed_by_portfolio,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            origin_analysis_id=(strategy_context or {}).get("origin_analysis_id"),
        )
        updated = realtime_monitor_db.apply_runtime_thresholds(
            price_alert_id,
            rating=self._map_action_to_rating(decision.get("action"), fallback=(strategy_context or {}).get("rating") or "持有"),
            monitor_levels=monitor_levels,
            origin_decision_id=decision_id,
        )
        if task_item:
            self.db.monitoring_repository.record_event(
                item_id=task_item["id"],
                event_type="threshold_sync",
                message="已创建并写入绑定价格预警的运行时阈值",
                notification_pending=False,
                sent=True,
                details={"decision_id": decision_id, "price_alert_id": price_alert_id},
            )
        return updated

    def _refresh_analysis_baseline_before_decision(
        self,
        *,
        stock_code: str,
        account_name: str,
        strategy_context: Optional[Dict] = None,
        has_position: bool = False,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> bool:
        asset_service = getattr(self.lifecycle_service, "asset_service", None)
        sync_func = getattr(asset_service, "sync_managed_monitors_for_symbol", None)
        refresh_performed = False

        if self._is_strategy_context_stale(strategy_context):
            try:
                from batch_analysis_service import analyze_single_stock_for_batch

                result = analyze_single_stock_for_batch(
                    symbol=stock_code,
                    period=getattr(config, "DATA_PERIOD", "1y"),
                    enabled_analysts_config=dict(self.BASELINE_ANALYSTS_CONFIG),
                    selected_model=self.model,
                    selected_lightweight_model=self.lightweight_model,
                    selected_reasoning_model=self.reasoning_model,
                    save_to_global_history=True,
                    has_position=has_position,
                    account_name=account_name,
                    asset_id=asset_id,
                    portfolio_stock_id=portfolio_stock_id,
                )
                if result.get("success"):
                    refresh_performed = True
                    self.logger.info(
                        "[%s] 缺失或过期的深度分析基线已通过统一分析入口刷新: analysis_id=%s",
                        stock_code,
                        result.get("record_id"),
                    )
                else:
                    self.logger.warning(
                        "[%s] 统一分析基线刷新失败: %s",
                        stock_code,
                        result.get("error") or "unknown_error",
                    )
            except Exception as exc:
                self.logger.warning("[%s] 调用统一分析入口刷新基线失败: %s", stock_code, exc)

        if not callable(sync_func):
            return refresh_performed

        try:
            sync_result = sync_func(stock_code, account_name=account_name)
            self.logger.info(
                "[%s] 决策前已同步最新分析基线: ai=%s alert=%s removed=%s",
                stock_code,
                int((sync_result or {}).get("ai_tasks_upserted", 0) or 0),
                int((sync_result or {}).get("price_alerts_upserted", 0) or 0),
                int((sync_result or {}).get("removed", 0) or 0),
            )
            return True
        except Exception as exc:
            self.logger.warning("[%s] 决策前同步最新分析基线失败: %s", stock_code, exc)
            return refresh_performed
    
    def analyze_stock(self, stock_code: str, notify: bool = True, has_position: bool = False,
                      position_cost: float = 0, position_quantity: int = 0,
                      position_date: Optional[str] = None,
                      trading_hours_only: bool = True,
                      account_name: str = DEFAULT_ACCOUNT_NAME,
                      asset_id: Optional[int] = None,
                      portfolio_stock_id: Optional[int] = None,
                      strategy_context: Optional[Dict] = None,
                      require_active_task: bool = False) -> Dict:
        """
        分析单只股票并做出决策
        
        Args:
            stock_code: 股票代码
            notify: 是否发送通知
            has_position: 是否已持仓（可选）
            position_cost: 持仓成本（可选）
            position_quantity: 持仓数量（可选）
            position_date: 持仓日期（可选）
            trading_hours_only: 是否仅在交易时段分析（可选，默认True）
            
        Returns:
            分析结果
        """
        try:
            self.logger.info(f"[{stock_code}] 开始分析...")
            
            # 1. 检查交易时段
            session_info = self.llm_client.get_trading_session()
            self.logger.info(f"[{stock_code}] 当前时段: {session_info['session']}")
            
            # 如果启用了仅交易时段分析，且当前不在交易时段，则跳过分析
            if trading_hours_only and not session_info.get('can_trade', False):
                self.logger.info(f"[{stock_code}] 非交易时段，跳过分析")
                return {
                    'success': False,
                    'error': f"非交易时段（{session_info['session']}），跳过分析",
                    'session_info': session_info,
                    'skipped': True
                }
            
            # 2. 获取市场数据
            market_data_kwargs = {}
            try:
                if "intraday_strict" in inspect.signature(self.data_fetcher.get_comprehensive_data).parameters:
                    market_data_kwargs["intraday_strict"] = bool(session_info.get("can_trade", False))
            except (TypeError, ValueError):
                market_data_kwargs["intraday_strict"] = bool(session_info.get("can_trade", False))
            market_data = self._run_with_timeout(
                self.data_fetcher.get_comprehensive_data,
                self.data_fetch_timeout_seconds,
                stock_code,
                **market_data_kwargs,
            )
            if market_data and market_data.get("precision_status") == "failed":
                return {
                    'success': False,
                    'error': market_data.get('precision_error') or '盘中TDX数据获取失败',
                    'session_info': session_info,
                }
            if not market_data:
                return {
                    'success': False,
                    'error': '获取市场数据失败'
                }
            market_data = self._prepare_market_structure_features(market_data)
            
            task_context = self.db.get_monitor_task_by_code(
                stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
            ) or {}

            if require_active_task and not task_context.get("id"):
                self.logger.info("[%s] 盯盘任务已删除，中断执行", stock_code)
                return {
                    "success": False,
                    "skipped": True,
                    "error": "task_deleted"
                }
            
            # 立即生效检查：如果任务已禁用，则跳过执行
            if task_context.get("id") and not task_context.get("enabled"):
                self.logger.info("[%s] 盯盘任务已禁用，中断执行", stock_code)
                return {
                    "success": False,
                    "skipped": True,
                    "error": "task_disabled"
                }

            asset_id = asset_id or task_context.get("asset_id")
            account_name = task_context.get("account_name") or account_name
            asset = None
            if asset_id:
                asset = self.db.asset_repository.get_asset(asset_id)
            if not asset and stock_code:
                asset = self.db.asset_repository.get_asset_by_symbol(stock_code, account_name)
                if asset:
                    asset_id = asset.get("id")
            if asset:
                position_snapshot = self._resolve_position_snapshot(
                    stock_code=stock_code,
                    account_name=account_name,
                    task_context=task_context,
                    asset=asset,
                    has_position=has_position,
                    position_cost=position_cost,
                    position_quantity=position_quantity,
                    position_date=position_date,
                )
            else:
                position_snapshot = self._resolve_position_snapshot(
                    stock_code=stock_code,
                    account_name=account_name,
                    task_context=task_context,
                    asset=None,
                    has_position=has_position,
                    position_cost=position_cost,
                    position_quantity=position_quantity,
                    position_date=position_date,
                )
            has_position = bool(position_snapshot["has_position"])
            position_cost = float(position_snapshot["position_cost"] or 0)
            position_quantity = int(position_snapshot["position_quantity"] or 0)
            position_date = self._normalize_position_date(position_snapshot.get("position_date"))
            can_sell_today = self._can_sell_today(
                has_position=has_position,
                position_date=position_date,
                market_data=market_data,
            )
            account_info = self._build_account_info(
                account_name=account_name,
                asset=asset,
                stock_code=stock_code,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
                has_position=has_position,
                position_cost=position_cost,
                position_quantity=position_quantity,
                position_date=position_date,
                current_market_price=self._safe_float(market_data.get("current_price")),
            )
            if isinstance(account_info.get("current_position"), dict):
                market_data["account_position"] = dict(account_info.get("current_position") or {})
            portfolio_stock_id = portfolio_stock_id or task_context.get("portfolio_stock_id") or (
                asset_id if asset and asset.get("status") == STATUS_PORTFOLIO else None
            )

            initial_strategy_context = self._resolve_latest_strategy_context(
                stock_code=stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
                provided_strategy_context=strategy_context,
                task_context=task_context,
            )
            self._refresh_analysis_baseline_before_decision(
                stock_code=stock_code,
                account_name=account_name,
                strategy_context=initial_strategy_context,
                has_position=has_position,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
            )

            task_context = self.db.get_monitor_task_by_code(
                stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
            ) or task_context
            strategy_context = self._resolve_latest_strategy_context(
                stock_code=stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
                provided_strategy_context=strategy_context,
                task_context=task_context,
            )
            risk_profile = self._resolve_task_risk_profile(account_name, task_context)
            if strategy_context:
                self.logger.info(
                    "[%s] 使用最新分析基线: analysis_id=%s scope=%s time=%s",
                    stock_code,
                    strategy_context.get("origin_analysis_id"),
                    strategy_context.get("analysis_scope"),
                    strategy_context.get("analysis_date"),
                )
            latest_decision = self.db.get_latest_ai_decision_for_context(
                stock_code=stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
            )

            # 5. 调用 LLM AI 决策
            ai_result = self._run_with_timeout(
                self.llm_client.analyze_stock_and_decide,
                self.ai_decision_timeout_seconds,
                stock_code=stock_code,
                market_data=market_data,
                account_info=account_info,
                has_position=has_position,
                position_cost=position_cost,
                position_quantity=position_quantity,
                position_date=position_date,
                can_sell_today=can_sell_today,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
                strategy_context=strategy_context,
                risk_profile=risk_profile,
            )

            if not ai_result['success']:
                return {
                    'success': False,
                    'error': 'AI决策失败',
                    'details': ai_result
                }

            if require_active_task:
                latest_task = self.db.get_monitor_task_by_code(
                    stock_code,
                    account_name=account_name,
                    asset_id=asset_id,
                    portfolio_stock_id=portfolio_stock_id,
                ) or {}
                if not latest_task.get("id") or not latest_task.get("enabled"):
                    self.logger.info("[%s] 盯盘任务已删除或禁用，丢弃本次分析结果", stock_code)
                    return {
                        "success": False,
                        "skipped": True,
                        "error": "task_deleted"
                    }
            
            original_action = str((ai_result.get("decision") or {}).get("action") or "").upper()
            decision = ai_result['decision']
            decision = self._apply_strategy_plan_guardrails(
                decision=decision,
                strategy_context=strategy_context,
                market_data=market_data,
                has_position=has_position,
            )
            decision = self._enforce_t1_sell_constraint(
                decision=decision,
                has_position=has_position,
                can_sell_today=can_sell_today,
                account_info=account_info,
                risk_profile=risk_profile,
            )
            decision = self._standardize_decision_schema(
                decision=decision,
                strategy_context=strategy_context,
            )
            decision, atr_guardrail_info = self._apply_atr_guardrail(
                decision=decision,
                strategy_context=strategy_context,
                market_data=market_data,
            )
            decision = self._reconcile_reasoning_action_consistency(
                decision=decision,
                has_position=has_position,
                account_info=account_info,
                risk_profile=risk_profile,
            )
            t1_sell_blocked = original_action == "SELL" and str(decision.get("action") or "").upper() != "SELL"
            
            self.logger.info(f"[{stock_code}] AI决策: {decision['action']} "
                           f"(信心度: {decision.get('confidence_level', decision.get('confidence', 'N/A'))})")
            self.logger.info(f"[{stock_code}] 决策理由: {decision['reasoning'][:100]}...")
            change_flags = self._classify_decision_change(
                latest_decision=latest_decision,
                current_decision=decision,
            )
            action_changed = bool(change_flags["action_changed"])
            thresholds_changed = bool(change_flags["thresholds_changed"])
            decision_changed = bool(change_flags["decision_changed"])
            actionable_signal = str(decision.get("action", "")).upper() in {"BUY", "SELL"}

            # 6. 保存AI决策到数据库
            decision_id = self.db.save_ai_decision({
                'stock_code': stock_code,
                'stock_name': market_data.get('name'),
                'account_name': account_name,
                'asset_id': asset_id,
                'portfolio_stock_id': portfolio_stock_id,
                'origin_analysis_id': strategy_context.get('origin_analysis_id') if strategy_context else None,
                'trading_session': session_info['session'],
                'action': decision['action'],
                'action_detail': decision.get('action_detail'),
                'swing_execution_mode': decision.get('swing_execution_mode'),
                'action_ratio_pct': decision.get('action_ratio_pct'),
                'trade_intent': decision.get('trade_intent'),
                'current_position_pct': decision.get('current_position_pct'),
                'target_position_pct': decision.get('target_position_pct'),
                'position_delta_pct': decision.get('position_delta_pct'),
                'confidence': decision['confidence'],
                'confidence_level': decision.get('confidence_level'),
                'rating': decision.get('rating'),
                'entry_range': decision.get('entry_range'),
                'take_profit': decision.get('take_profit'),
                'stop_loss': decision.get('stop_loss'),
                'advice': decision.get('advice'),
                'reasoning': decision['reasoning'],
                'position_size_pct': decision.get('position_size_pct'),
                'stop_loss_pct': decision.get('stop_loss_pct'),
                'take_profit_pct': decision.get('take_profit_pct'),
                'risk_level': decision.get('risk_level'),
                'key_price_levels': decision.get('key_price_levels', {}),
                'monitor_levels': decision.get('monitor_levels', {}),
                'decision_context': self._build_decision_context_delta(
                    latest_decision=latest_decision,
                    current_decision=decision,
                    market_data=market_data,
                    change_flags=change_flags,
                ) | (
                    {"atr_guardrail_clamped": atr_guardrail_info}
                    if atr_guardrail_info
                    else {}
                ),
                'market_data': market_data,
                'account_info': account_info,
                'execution_mode': 'manual_only',
                'action_status': 'pending' if actionable_signal and action_changed else 'suggested',
            })

            self._sync_position_cycle_runtime_state(
                asset_id=asset_id,
                decision_id=decision_id,
                decision=decision,
                strategy_context=strategy_context,
                market_data=market_data,
                position_date=position_date,
                has_position=has_position,
            )

            if decision_changed:
                self._sync_runtime_thresholds(
                    stock_code=stock_code,
                    stock_name=market_data.get('name') or stock_code,
                    decision=decision,
                    decision_id=decision_id,
                    account_name=account_name,
                    asset_id=asset_id,
                    portfolio_stock_id=portfolio_stock_id,
                    strategy_context=strategy_context,
                )

            # 7. 手工执行模式下只生成待处理动作
            execution_result = None
            pending_action = None
            if action_changed and actionable_signal:
                pending_action = self._create_pending_action(
                    stock_code=stock_code,
                    stock_name=market_data.get('name') or stock_code,
                    asset_id=asset_id,
                    decision_id=decision_id,
                    decision=decision,
                    market_data=market_data,
                    account_name=account_name,
                )
                execution_result = pending_action

            # 8. 发送通知
            if notify and action_changed and not t1_sell_blocked:
                self._send_notification(
                    stock_code=stock_code,
                    stock_name=market_data.get('name'),
                    decision=decision,
                    execution_result=execution_result,
                    market_data=market_data,
                    has_position=has_position,
                    position_cost=position_cost,
                    position_quantity=position_quantity,
                    position_date=position_date,
                    can_sell_today=can_sell_today,
                    session_info=session_info,
                    account_name=account_name,
                    asset_id=asset_id,
                    portfolio_stock_id=portfolio_stock_id,
                )
            
            return {
                'success': True,
                'stock_code': stock_code,
                'stock_name': market_data.get('name'),
                'session_info': session_info,
                'market_data': market_data,
                'decision': decision,
                'decision_id': decision_id,
                'decision_changed': decision_changed,
                'action_changed': action_changed,
                'thresholds_changed': thresholds_changed,
                'execution_result': execution_result,
                'pending_action': pending_action,
                'account_name': account_name,
                'asset_id': asset_id,
                'portfolio_stock_id': portfolio_stock_id,
                'strategy_context': strategy_context or {},
                'final_decision': {
                    'rating': decision.get('rating'),
                    'confidence_level': decision.get('confidence_level'),
                    'entry_range': decision.get('entry_range'),
                    'take_profit': decision.get('take_profit'),
                    'stop_loss': decision.get('stop_loss'),
                    'advice': decision.get('advice'),
                },
            }
            
        except Exception as e:
            self.logger.error(f"[{stock_code}] 分析失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }

    def _create_pending_action(
        self,
        *,
        stock_code: str,
        stock_name: str,
        asset_id: Optional[int],
        decision_id: int,
        decision: Dict,
        market_data: Dict,
        account_name: str,
    ) -> Dict:
        resolved_asset_id = asset_id
        if resolved_asset_id is None:
            _, _, resolved_asset_id = self.db.asset_service.promote_to_watchlist(
                symbol=stock_code,
                stock_name=stock_name or stock_code,
                account_name=account_name,
                note="来自 AI 盯盘信号",
            )
        if resolved_asset_id is None:
            return {
                "success": False,
                "error": "asset_resolve_failed",
            }
        payload = {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "account_name": account_name,
            "current_price": market_data.get("current_price"),
            "decision": decision,
            "market_data": {
                "current_price": market_data.get("current_price"),
                "change_pct": market_data.get("change_pct"),
                "change_amount": market_data.get("change_amount"),
            },
        }
        pending_action_id = self.db.create_pending_action(
            asset_id=resolved_asset_id,
            action_type=str(decision.get("action", "")).lower(),
            origin_decision_id=decision_id,
            payload=payload,
        )
        return {
            "success": True,
            "mode": "manual_only",
            "pending_action_id": pending_action_id,
            "message": f"已创建待人工处理动作: {decision.get('action')}",
        }
    def _send_notification(
        self,
        stock_code: str,
        stock_name: str,
        decision: Dict,
        execution_result: Optional[Dict],
        market_data: Dict,
        has_position: Optional[bool] = None,
        position_cost: Optional[float] = None,
        position_quantity: Optional[int] = None,
        position_date: Optional[str] = None,
        can_sell_today: Optional[bool] = None,
        session_info: Optional[Dict] = None,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ):
        try:
            action = str(decision.get('action', '')).upper()
            if action not in ['BUY', 'SELL']:
                self.logger.info(f"[{stock_code}] 决策为 {action}，不发送通知")
                return

            def _to_float(value: object) -> Optional[float]:
                try:
                    if value in (None, ""):
                        return None
                    numeric = float(value)
                except (TypeError, ValueError):
                    return None
                return numeric if numeric == numeric else None

            def _fmt_money(value: object, *, signed: bool = False) -> str:
                numeric = _to_float(value)
                if numeric is None:
                    return "N/A"
                sign = "+" if signed else ""
                return f"¥{numeric:{sign},.2f}"

            def _fmt_pct(value: object) -> str:
                numeric = _to_float(value)
                if numeric is None:
                    return "N/A"
                return f"{numeric:+.2f}%"

            def _fmt_volume(value: object) -> str:
                numeric = _to_float(value)
                if numeric is None:
                    return "N/A"
                return f"{numeric:,.0f}手"

            def _clean_reason(value: object, limit: int = 120) -> str:
                text = " ".join(str(value or "").split()).strip()
                if not text:
                    return "盘中出现新的执行信号，请结合实时价格与阈值复核。"
                text = re.sub(r"(?:\.{3,}|…+)(?:（已截断）)?", "。", text)
                text = re.sub(r"。{2,}", "。", text)
                return text.strip()

            def _swing_mode_label(value: object) -> str:
                mapping = {
                    "pullback_entry": "回踩建仓",
                    "breakout_entry": "突破建仓",
                    "pullback_add": "回踩确认加仓",
                    "breakout_add": "突破确认加仓",
                    "proactive_trim": "主动减仓锁盈",
                    "defensive_trim": "防守减仓",
                    "defensive_exit": "防守清仓",
                    "trend_hold": "趋势持有",
                    "watch_hold": "观察持有",
                }
                normalized = str(value or "").strip().lower()
                return mapping.get(normalized, "")

            task = self.db.get_monitor_task_by_code(
                stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
            )
            task_config = task or {}
            if has_position is None:
                has_position = bool(task_config.get('has_position', 0))
            if position_cost is None:
                position_cost = float(task_config.get('position_cost', 0) or 0)
            if position_quantity is None:
                position_quantity = int(task_config.get('position_quantity', 0) or 0)
            if position_date is None:
                position_date = self._normalize_position_date(task_config.get('position_date'))
            if session_info is None:
                session_info = self.llm_client.get_trading_session()
            if can_sell_today is None:
                can_sell_today = self._can_sell_today(
                    has_position=bool(has_position),
                    position_date=position_date,
                    market_data=market_data,
                )
            if action == "SELL" and has_position and not can_sell_today:
                self.logger.info("[%s] 当天买入受T+1限制，跳过卖出通知", stock_code)
                return

            current_price = _to_float(market_data.get('current_price'))
            profit_loss_pct = None
            if has_position and position_cost and current_price is not None:
                profit_loss_pct = (current_price - position_cost) / position_cost * 100
            current_price_text = _fmt_money(current_price)

            action_text = {'BUY': '买入', 'SELL': '卖出'}.get(action, action)
            action_detail = str(decision.get("action_detail") or action_text).strip() or action_text
            swing_execution_mode = str(decision.get("swing_execution_mode") or "").strip().lower()
            swing_execution_label = _swing_mode_label(swing_execution_mode)
            reasoning = str(decision.get('reasoning', '') or "").strip()
            reasoning_summary = _clean_reason(reasoning, 150)
            notification_class = self._derive_notification_class(
                action=action,
                action_detail=action_detail,
                swing_execution_mode=swing_execution_mode,
                has_position=bool(has_position),
                decision=decision,
            )
            notification_label = self._notification_class_label(notification_class)
            notification_category = "price"
            if notification_class == "profit_alert":
                trigger_summary = (
                    "出现止盈收益信号" if action_detail == "减仓"
                    else "出现收益信号"
                )
            elif notification_class == "risk_alert":
                trigger_summary = (
                    "出现清仓风险信号" if action_detail == "清仓"
                    else "出现风险预警信号"
                )
            elif action == "BUY":
                if not has_position:
                    trigger_summary = "出现入场价格信号"
                elif swing_execution_mode == "breakout_add":
                    trigger_summary = "出现突破加仓价格信号"
                else:
                    trigger_summary = "出现加仓价格信号"
            elif action_detail == "清仓":
                trigger_summary = "出现清仓价格信号"
            elif action_detail == "减仓":
                trigger_summary = "出现减仓价格信号"
            else:
                trigger_summary = "出现新的价格信号"
            key_levels = decision.get('key_price_levels', {}) or {}

            notification_data = {
                'symbol': stock_code,
                'name': stock_name,
                'type': '智能盯盘',
                'notification_class': notification_class,
                'notification_category': notification_category,
                'notification_label': notification_label,
                'notification_class_label': notification_label,
                'trigger_summary': trigger_summary,
                'notification_reason': reasoning,
                'action': action,
                'action_text': action_text,
                'action_detail': action_detail,
                'swing_execution_mode': swing_execution_mode,
                'swing_execution_label': swing_execution_label,
                'triggered_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'current_price': current_price,
                'change_pct': market_data.get('change_pct'),
                'change_amount': _fmt_money(market_data.get('change_amount'), signed=True),
                'volume': _fmt_volume(market_data.get('volume')),
                'turnover_rate': market_data.get('turnover_rate'),
                'position_status': '已持仓' if has_position else '未持仓',
                'position_cost': f"{position_cost:.2f}" if has_position and position_cost else 'N/A',
                'position_quantity': position_quantity if has_position else 0,
                'profit_loss_pct': f"{profit_loss_pct:+.2f}" if profit_loss_pct is not None else 'N/A',
                'trading_session': session_info.get('session', '未知'),
                'rating': decision.get('rating'),
                'confidence_level': decision.get('confidence_level', decision.get('confidence')),
                'entry_range': decision.get('entry_range'),
                'take_profit': decision.get('take_profit'),
                'stop_loss': decision.get('stop_loss'),
                'pending_action_id': execution_result.get('pending_action_id') if execution_result else None,
                'pending_action_error': execution_result.get('error') if execution_result and not execution_result.get('success') else None,
            }
            rendered_notification = self.notification.build_smart_monitor_notification_message(notification_data)
            message = rendered_notification["message"]
            content = rendered_notification["content"]
            notification_data['message'] = message
            notification_data['details'] = content
            notification_data['notification_explanation'] = rendered_notification.get("explanation", "")

            self.db.save_notification({
                'stock_code': stock_code,
                'notify_type': 'decision',
                'subject': f"智能盯盘 - {message}",
                'content': content,
                'status': 'pending',
            })

            task_item = self._find_ai_task_item(stock_code, account_name, asset_id, portfolio_stock_id)
            if task_item:
                self.db.monitoring_repository.record_event(
                    item_id=task_item['id'],
                    event_type=action.lower(),
                    message=message,
                    details=notification_data,
                    notification_pending=True,
                    sent=False,
                )
                self.logger.info(f"[{stock_code}] {action_text}通知已入队")
        except Exception as exc:
            self.logger.error(f"[{stock_code}] 发送通知失败: {exc}")

    def start_monitor(
        self,
        stock_code: str,
        check_interval: int = 300,
        notify: bool = True,
        has_position: bool = False,
        position_cost: float = 0,
        position_quantity: int = 0,
        trading_hours_only: bool = True,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
        strategy_context: Optional[Dict] = None,
    ):
        self.logger.info(
            "[%s] start_monitor 已废弃，AI 执行统一由 MonitoringOrchestrator 调度",
            stock_code,
        )

    def stop_monitor(self, stock_code: str):
        self.logger.info("[%s] stop_monitor 已废弃，统一调度器不再使用旧事件总线路径", stock_code)

    def _on_radar_event(self, **kwargs):
        stock_code = kwargs.get('stock_code')
        if stock_code:
            self.logger.debug("[%s] 忽略旧雷达事件回调，统一调度器已接管执行", stock_code)


if __name__ == '__main__':
    # 测试代码
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    engine = SmartMonitorEngine(
        llm_api_key=config.WARMMILK_API_KEY,
    )
    
    # 测试分析贵州茅台
    print("\n测试分析贵州茅台(600519)...")
    result = engine.analyze_stock('600519', notify=False)
    
    if result['success']:
        print(f"\n分析成功!")
        print(f"  决策: {result['decision']['action']}")
        print(f"  信心度: {result['decision']['confidence']}%")
        print(f"  理由: {result['decision']['reasoning'][:100]}...")
    else:
        print(f"\n分析失败: {result.get('error')}")
