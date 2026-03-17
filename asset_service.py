from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import config

from analysis_repository import AnalysisRepository, analysis_repository
from asset_repository import (
    STATUS_PORTFOLIO,
    STATUS_RESEARCH,
    STATUS_WATCHLIST,
    AssetRepository,
    asset_repository,
)
from investment_db_utils import DEFAULT_ACCOUNT_NAME
from monitoring_repository import MonitoringRepository


class AssetService:
    """Lifecycle orchestration for research, watchlist, and portfolio assets."""

    def __init__(
        self,
        *,
        asset_store: Optional[AssetRepository] = None,
        analysis_store: Optional[AnalysisRepository] = None,
        monitoring_store: Optional[MonitoringRepository] = None,
    ):
        self.asset_repository = asset_store or asset_repository
        self.analysis_repository = analysis_store or analysis_repository
        self.monitoring_repository = monitoring_store

    @staticmethod
    def get_default_ai_interval_minutes() -> int:
        return max(1, int(getattr(config, "SMART_MONITOR_AI_INTERVAL_MINUTES", 60) or 60))

    @staticmethod
    def get_default_alert_interval_minutes() -> int:
        return max(
            3,
            int(getattr(config, "SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES", 3) or 3),
        )

    def _get_strategy_context(self, asset: Dict) -> Optional[Dict]:
        return self.analysis_repository.get_latest_strategy_context(
            asset_id=asset.get("id"),
            symbol=asset.get("symbol"),
            account_name=asset.get("account_name") or DEFAULT_ACCOUNT_NAME,
        )

    @staticmethod
    def _normalize_runtime_thresholds(config: Optional[Dict]) -> Optional[Dict]:
        if not isinstance(config, dict):
            return None
        runtime_thresholds = config.get("runtime_thresholds")
        if not isinstance(runtime_thresholds, dict):
            return None
        keys = ("entry_min", "entry_max", "take_profit", "stop_loss")
        normalized: Dict[str, float] = {}
        for key in keys:
            value = runtime_thresholds.get(key)
            if value in (None, ""):
                return None
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError):
                return None
        return normalized

    @staticmethod
    def _should_preserve_runtime_thresholds(existing_item: Optional[Dict], strategy_context: Optional[Dict]) -> bool:
        if not existing_item:
            return False
        existing_config = existing_item.get("config") or {}
        if AssetService._normalize_runtime_thresholds(existing_config) is None:
            return False
        existing_origin = existing_item.get("origin_analysis_id")
        next_origin = (strategy_context or {}).get("origin_analysis_id")
        if existing_origin is None or next_origin is None:
            return True
        return int(existing_origin) == int(next_origin)

    @staticmethod
    def _matches_followup_search(item: Dict, search_term: str) -> bool:
        normalized_search = str(search_term or "").strip().lower()
        if not normalized_search:
            return True
        haystacks = (
            str(item.get("symbol") or "").lower(),
            str(item.get("name") or "").lower(),
            str(item.get("account_name") or "").lower(),
            str(item.get("note") or "").lower(),
            str(item.get("latest_analysis_summary") or "").lower(),
        )
        return any(normalized_search in value for value in haystacks)

    def _resolve_account_risk_profile(self, account_name: Optional[str]) -> Dict[str, int]:
        if self.monitoring_repository and hasattr(self.monitoring_repository, "get_account_risk_profile"):
            profile = self.monitoring_repository.get_account_risk_profile(account_name)
            return {
                "position_size_pct": int(profile["position_size_pct"]),
                "total_position_pct": int(profile["total_position_pct"]),
                "stop_loss_pct": int(profile["stop_loss_pct"]),
                "take_profit_pct": int(profile["take_profit_pct"]),
            }
        return dict(config.get_smart_monitor_risk_defaults())

    def _build_ai_task_payload(self, asset: Dict, existing_item: Optional[Dict] = None) -> Dict:
        existing_item = existing_item or {}
        existing_config = existing_item.get("config") or {}
        status = asset.get("status")
        account_name = asset.get("account_name") or DEFAULT_ACCOUNT_NAME
        account_risk = self._resolve_account_risk_profile(account_name)
        return {
            "symbol": asset["symbol"],
            "name": asset.get("name") or asset["symbol"],
            "monitor_type": "ai_task",
            "source": "portfolio" if status == STATUS_PORTFOLIO else "ai_monitor",
            "enabled": bool(asset.get("monitor_enabled", True)),
            "interval_minutes": int(
                existing_item.get("interval_minutes") or self.get_default_ai_interval_minutes()
            ),
            "trading_hours_only": bool(existing_item.get("trading_hours_only", True)),
            "notification_enabled": bool(existing_item.get("notification_enabled", True)),
            "managed_by_portfolio": status == STATUS_PORTFOLIO,
            "account_name": account_name,
            "asset_id": asset["id"],
            "portfolio_stock_id": asset["id"] if status == STATUS_PORTFOLIO else None,
            "origin_analysis_id": asset.get("origin_analysis_id"),
            "config": {
                "task_name": existing_config.get("task_name") or f"{asset.get('name') or asset['symbol']}盯盘",
                "position_size_pct": account_risk["position_size_pct"],
                "total_position_pct": account_risk["total_position_pct"],
                "stop_loss_pct": account_risk["stop_loss_pct"],
                "take_profit_pct": account_risk["take_profit_pct"],
                "notify_email": existing_config.get("notify_email"),
                "notify_webhook": existing_config.get("notify_webhook"),
                "position_date": existing_config.get("position_date"),
            },
        }

    def _build_price_alert_payload(self, asset: Dict, strategy_context: Dict, existing_item: Optional[Dict] = None) -> Dict:
        existing_item = existing_item or {}
        existing_config = existing_item.get("config") or {}
        status = asset.get("status")
        entry_min = strategy_context.get("entry_min")
        entry_max = strategy_context.get("entry_max")
        take_profit = strategy_context.get("take_profit")
        stop_loss = strategy_context.get("stop_loss")
        has_complete_strategy_levels = all(
            value is not None for value in (entry_min, entry_max, take_profit, stop_loss)
        )
        config = {
            "rating": strategy_context.get("rating") or existing_config.get("rating") or "持有",
            "entry_range": existing_config.get("entry_range") or {},
            "take_profit": existing_config.get("take_profit"),
            "stop_loss": existing_config.get("stop_loss"),
            "strategy_context": strategy_context if strategy_context else (existing_config.get("strategy_context") or {}),
            "threshold_source": existing_config.get("threshold_source") or "pending_ai",
            "threshold_updated_at": existing_config.get("threshold_updated_at"),
            "runtime_thresholds": None,
            "origin_decision_id": None,
        }
        if has_complete_strategy_levels:
            config.update(
                {
                    "entry_range": {"min": float(entry_min), "max": float(entry_max)},
                    "take_profit": float(take_profit),
                    "stop_loss": float(stop_loss),
                    "strategy_context": strategy_context,
                    "threshold_source": "strategy_context",
                    "threshold_updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        if self._should_preserve_runtime_thresholds(existing_item, strategy_context):
            for key in ("runtime_thresholds", "origin_decision_id", "threshold_source", "threshold_updated_at"):
                if key in existing_config:
                    config[key] = existing_config[key]
        return {
            "symbol": asset["symbol"],
            "name": asset.get("name") or asset["symbol"],
            "monitor_type": "price_alert",
            "source": "portfolio" if status == STATUS_PORTFOLIO else "ai_monitor",
            "enabled": bool(asset.get("monitor_enabled", True)),
            "interval_minutes": int(
                existing_item.get("interval_minutes") or self.get_default_alert_interval_minutes()
            ),
            "trading_hours_only": bool(existing_item.get("trading_hours_only", True)),
            "notification_enabled": bool(existing_item.get("notification_enabled", True)),
            "managed_by_portfolio": status == STATUS_PORTFOLIO,
            "account_name": asset.get("account_name") or DEFAULT_ACCOUNT_NAME,
            "asset_id": asset["id"],
            "portfolio_stock_id": asset["id"] if status == STATUS_PORTFOLIO else None,
            "origin_analysis_id": strategy_context.get("origin_analysis_id") or asset.get("origin_analysis_id"),
            "config": config,
        }

    def list_followup_assets(
        self,
        *,
        account_name: Optional[str] = None,
        statuses: Optional[Tuple[str, ...]] = None,
        search_term: str = "",
        limit: Optional[int] = 30,
    ) -> List[Dict]:
        requested_statuses = statuses or (STATUS_WATCHLIST, STATUS_RESEARCH)
        normalized_statuses = tuple(
            status
            for status in requested_statuses
            if status in {STATUS_RESEARCH, STATUS_WATCHLIST}
        )
        if not normalized_statuses:
            return []

        assets: List[Dict] = []
        for status in normalized_statuses:
            assets.extend(
                self.asset_repository.list_assets(
                    status=status,
                    account_name=account_name,
                    include_deleted=False,
                )
            )

        result: List[Dict] = []
        for asset in assets:
            strategy_context = self._get_strategy_context(asset) or {}
            item = dict(asset)
            item["strategy_context"] = strategy_context
            item["followup_status_label"] = "关注中" if asset.get("status") == STATUS_WATCHLIST else "看过"
            item["latest_analysis_id"] = (
                strategy_context.get("origin_analysis_id") or asset.get("origin_analysis_id")
            )
            item["latest_analysis_time"] = strategy_context.get("analysis_date") or ""
            item["latest_analysis_scope"] = strategy_context.get("analysis_scope") or ""
            item["latest_analysis_source"] = strategy_context.get("analysis_source") or ""
            item["latest_analysis_rating"] = strategy_context.get("rating") or ""
            item["latest_analysis_summary"] = (
                strategy_context.get("summary")
                or asset.get("note")
                or ""
            )
            if not self._matches_followup_search(item, search_term):
                continue
            result.append(item)

        result.sort(
            key=lambda item: (
                1 if item.get("status") == STATUS_WATCHLIST else 0,
                str(item.get("latest_analysis_time") or ""),
                str(item.get("updated_at") or ""),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
        if limit is not None and limit > 0:
            return result[:limit]
        return result

    def sync_managed_monitors(self, asset_id: int) -> Dict[str, int]:
        if not self.monitoring_repository:
            return {"ai_tasks_upserted": 0, "price_alerts_upserted": 0, "removed": 0}

        asset = self.asset_repository.get_asset(asset_id)
        if not asset:
            return {"ai_tasks_upserted": 0, "price_alerts_upserted": 0, "removed": 0}

        status = asset.get("status")
        if status == STATUS_RESEARCH:
            removed = 0
            for monitor_type in ("ai_task", "price_alert"):
                if self.monitoring_repository.delete_by_symbol(
                    asset["symbol"],
                    monitor_type=monitor_type,
                    account_name=asset.get("account_name"),
                    asset_id=asset["id"],
                ):
                    removed += 1
            return {"ai_tasks_upserted": 0, "price_alerts_upserted": 0, "removed": removed}

        existing_ai = self.monitoring_repository.get_item_by_symbol(
            asset["symbol"],
            monitor_type="ai_task",
            account_name=asset.get("account_name"),
            asset_id=asset["id"],
        )
        self.monitoring_repository.upsert_item(self._build_ai_task_payload(asset, existing_ai))
        strategy_context = self._get_strategy_context(asset) or {}
        existing_alert = self.monitoring_repository.get_item_by_symbol(
            asset["symbol"],
            monitor_type="price_alert",
            account_name=asset.get("account_name"),
            asset_id=asset["id"],
        )
        alert_payload = self._build_price_alert_payload(asset, strategy_context, existing_alert)
        self.monitoring_repository.upsert_item(alert_payload)
        return {"ai_tasks_upserted": 1, "price_alerts_upserted": 1, "removed": 0}

    def set_monitoring_enabled(self, asset_id: int, enabled: bool) -> Dict[str, int]:
        asset = self.asset_repository.get_asset(asset_id)
        if not asset:
            return {"ai_tasks_upserted": 0, "price_alerts_upserted": 0, "removed": 0}

        self.asset_repository.update_asset(asset_id, monitor_enabled=bool(enabled))
        return self.sync_managed_monitors(asset_id)

    def create_or_update_research_asset(
        self,
        *,
        symbol: str,
        stock_name: str,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        summary: str = "",
        origin_analysis_id: Optional[int] = None,
    ) -> int:
        return self.asset_repository.create_or_update_research_asset(
            symbol=symbol,
            name=stock_name,
            account_name=account_name,
            note=summary,
            origin_analysis_id=origin_analysis_id,
        )

    def promote_to_watchlist(
        self,
        *,
        symbol: str,
        stock_name: str,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        note: str = "",
        origin_analysis_id: Optional[int] = None,
        monitor_enabled: bool = True,
    ) -> Tuple[bool, str, Optional[int]]:
        asset_id = self.asset_repository.promote_to_watchlist(
            symbol=symbol,
            name=stock_name,
            account_name=account_name,
            note=note,
            origin_analysis_id=origin_analysis_id,
            monitor_enabled=monitor_enabled,
        )
        self.sync_managed_monitors(asset_id)
        return True, f"已加入盯盘: {symbol}", asset_id

    def promote_to_portfolio(
        self,
        *,
        symbol: str,
        stock_name: str,
        cost_price: float,
        quantity: int,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        note: str = "",
        origin_analysis_id: Optional[int] = None,
        monitor_enabled: bool = True,
    ) -> Tuple[bool, str, Optional[int]]:
        asset = self.asset_repository.get_asset_by_symbol(symbol, account_name)
        if asset:
            self.asset_repository.update_asset(
                asset["id"],
                name=stock_name or asset.get("name") or symbol,
                note=note or asset.get("note"),
                monitor_enabled=monitor_enabled,
                origin_analysis_id=origin_analysis_id or asset.get("origin_analysis_id"),
                status=STATUS_PORTFOLIO,
                cost_price=float(cost_price),
                quantity=int(quantity),
                last_trade_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            asset_id = int(asset["id"])
        else:
            asset_id = self.asset_repository.create_or_update_research_asset(
                symbol=symbol,
                name=stock_name,
                account_name=account_name,
                note=note,
                origin_analysis_id=origin_analysis_id,
                monitor_enabled=monitor_enabled,
            )
            self.asset_repository.transition_asset_status(
                asset_id,
                STATUS_PORTFOLIO,
                cost_price=float(cost_price),
                quantity=int(quantity),
                note=note,
                origin_analysis_id=origin_analysis_id,
                last_trade_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        warning = ""
        try:
            self.sync_managed_monitors(asset_id)
        except Exception as exc:
            print(f"[WARN] 设为持仓后同步监测失败 ({symbol}): {exc}")
            warning = f"（监测同步失败: {exc}）"
        return True, f"已设为持仓: {symbol}{warning}", asset_id

    def clear_position_to_watchlist(self, asset_id: int, *, note: str = "", last_trade_at: Optional[str] = None) -> bool:
        changed = self.asset_repository.transition_asset_status(
            asset_id,
            STATUS_WATCHLIST,
            note=note,
            last_trade_at=last_trade_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        if changed:
            self.sync_managed_monitors(asset_id)
        return changed

    def remove_from_watchlist(self, asset_id: int, *, note: str = "") -> bool:
        changed = self.asset_repository.transition_asset_status(asset_id, STATUS_RESEARCH, note=note)
        if changed:
            self.sync_managed_monitors(asset_id)
        return changed

    def record_manual_trade(
        self,
        *,
        asset_id: int,
        trade_type: str,
        quantity: int,
        price: float,
        trade_date: Optional[str] = None,
        note: str = "",
        trade_source: str = "manual",
        pending_action_id: Optional[int] = None,
    ) -> Tuple[bool, str, Optional[Dict]]:
        asset = self.asset_repository.get_asset(asset_id)
        if not asset:
            return False, f"未找到资产ID: {asset_id}", None

        normalized_trade_type = str(trade_type or "").strip().lower()
        if normalized_trade_type not in {"buy", "sell", "clear"}:
            return False, "交易类型仅支持 buy/sell/clear", None

        trade_quantity = int(quantity or 0)
        trade_price = float(price or 0)
        if trade_price <= 0:
            return False, "成交价格必须大于 0", None

        current_quantity = int(asset.get("quantity") or 0)
        current_cost = float(asset.get("cost_price") or 0)
        effective_trade_date = trade_date or datetime.now().strftime("%Y-%m-%d")
        effective_trade_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clear_requested = normalized_trade_type == "clear"

        if clear_requested:
            if current_quantity <= 0:
                return False, "当前没有可清仓的持仓数量", None
            trade_quantity = current_quantity
            normalized_trade_type = "sell"

        if trade_quantity <= 0:
            return False, "交易数量必须大于 0", None
        if not clear_requested and trade_quantity % 100 != 0:
            action_label = "加仓" if normalized_trade_type == "buy" else "减仓"
            return False, f"{action_label}数量必须是100的整数倍", None

        if normalized_trade_type == "buy":
            if asset.get("status") == STATUS_PORTFOLIO and current_quantity > 0 and current_cost > 0:
                new_quantity = current_quantity + trade_quantity
                new_cost = ((current_cost * current_quantity) + (trade_price * trade_quantity)) / new_quantity
            else:
                new_quantity = trade_quantity
                new_cost = trade_price
            self.asset_repository.add_trade_history(
                asset_id,
                trade_type="buy",
                trade_date=effective_trade_date,
                price=trade_price,
                quantity=trade_quantity,
                note=note,
                trade_source=trade_source,
            )
            self.asset_repository.update_asset(
                asset_id,
                status=STATUS_PORTFOLIO,
                cost_price=new_cost,
                quantity=new_quantity,
                last_trade_at=effective_trade_time,
            )
            if pending_action_id:
                self.asset_repository.update_pending_action(
                    pending_action_id,
                    status="accepted",
                    resolution_note=f"手工登记买入 {trade_quantity} 股 @ {trade_price:.3f}",
                )
            self.sync_managed_monitors(asset_id)
            updated_asset = self.asset_repository.get_asset(asset_id)
            return True, "买入记录已保存", updated_asset

        if current_quantity <= 0:
            return False, "当前没有可卖出的持仓数量", None
        if trade_quantity > current_quantity:
            return False, "减仓数量不能超过当前持仓数量", None

        remaining_quantity = current_quantity - trade_quantity
        self.asset_repository.add_trade_history(
            asset_id,
            trade_type="sell",
            trade_date=effective_trade_date,
            price=trade_price,
            quantity=trade_quantity,
            note=note,
            trade_source=trade_source,
        )
        if remaining_quantity > 0:
            new_cost = ((current_cost * current_quantity) - (trade_price * trade_quantity)) / remaining_quantity
            if abs(new_cost) < 1e-12:
                new_cost = 0.0
            self.asset_repository.update_asset(
                asset_id,
                status=STATUS_PORTFOLIO,
                cost_price=new_cost,
                quantity=remaining_quantity,
                last_trade_at=effective_trade_time,
            )
        else:
            self.asset_repository.transition_asset_status(
                asset_id,
                STATUS_WATCHLIST,
                note=note or asset.get("note"),
                last_trade_at=effective_trade_time,
            )
        if pending_action_id:
            self.asset_repository.update_pending_action(
                pending_action_id,
                status="accepted",
                resolution_note=(
                    f"手工登记清仓 {trade_quantity} 股 @ {trade_price:.3f}"
                    if clear_requested
                    else f"手工登记卖出 {trade_quantity} 股 @ {trade_price:.3f}"
                ),
            )
        self.sync_managed_monitors(asset_id)
        updated_asset = self.asset_repository.get_asset(asset_id)
        return True, ("清仓记录已保存" if clear_requested else "卖出记录已保存"), updated_asset


asset_service = AssetService()
