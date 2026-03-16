from datetime import datetime
from typing import Dict, List, Optional

from analysis_repository import AnalysisRepository
from monitoring_repository import MonitoringRepository, resolve_monitoring_db_path


class StockMonitorDatabase:
    """Compatibility facade for price alerts backed by the unified monitoring repository."""

    def __init__(self, db_path: str = "stock_monitor.db"):
        self.db_path = db_path
        canonical_db = resolve_monitoring_db_path(db_path)
        self.repository = MonitoringRepository(canonical_db)
        self.analysis_repository = AnalysisRepository(canonical_db, legacy_analysis_db_path=canonical_db)
        self.repository.migrate_legacy_stock_db(db_path)

    @staticmethod
    def _build_config(
        rating: str,
        entry_range: Optional[Dict],
        take_profit: Optional[float],
        stop_loss: Optional[float],
        *,
        runtime_thresholds: Optional[Dict] = None,
        threshold_source: Optional[str] = None,
        threshold_updated_at: Optional[str] = None,
        origin_decision_id: Optional[int] = None,
        strategy_context: Optional[Dict] = None,
    ) -> Dict:
        config = {
            "rating": rating,
            "entry_range": entry_range or {},
            "take_profit": take_profit,
            "stop_loss": stop_loss,
        }
        if runtime_thresholds:
            config["runtime_thresholds"] = runtime_thresholds
        if threshold_source:
            config["threshold_source"] = threshold_source
        if threshold_updated_at:
            config["threshold_updated_at"] = threshold_updated_at
        if origin_decision_id is not None:
            config["origin_decision_id"] = origin_decision_id
        if strategy_context:
            config["strategy_context"] = strategy_context
        return config

    @staticmethod
    def _to_float(value) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _default_check_interval_minutes(self) -> int:
        raw = self.repository.get_metadata("smart_monitor_realtime_monitor_interval_minutes")
        try:
            if raw is not None:
                return max(1, min(10, int(raw)))
        except (TypeError, ValueError):
            pass
        return 3

    @classmethod
    def _normalize_monitor_levels(cls, payload: Optional[Dict]) -> Optional[Dict]:
        if not isinstance(payload, dict):
            return None
        levels = {
            "entry_min": cls._to_float(payload.get("entry_min")),
            "entry_max": cls._to_float(payload.get("entry_max")),
            "take_profit": cls._to_float(payload.get("take_profit")),
            "stop_loss": cls._to_float(payload.get("stop_loss")),
        }
        if not all(value is not None for value in levels.values()):
            return None
        return levels

    def _resolve_strategy_context(self, item: Dict, config: Dict) -> Dict:
        strategy_context = self.analysis_repository.get_latest_strategy_context(
            asset_id=item.get("asset_id"),
            portfolio_stock_id=item.get("portfolio_stock_id"),
            symbol=item.get("symbol"),
            account_name=item.get("account_name"),
        )
        if strategy_context:
            return strategy_context
        raw_context = config.get("strategy_context") or {}
        return raw_context if isinstance(raw_context, dict) else {}

    def _resolve_effective_levels(self, item: Dict, config: Dict) -> Dict:
        runtime_levels = self._normalize_monitor_levels(config.get("runtime_thresholds"))
        strategy_context = self._resolve_strategy_context(item, config)
        strategy_levels = self._normalize_monitor_levels(
            {
                "entry_min": strategy_context.get("entry_min"),
                "entry_max": strategy_context.get("entry_max"),
                "take_profit": strategy_context.get("take_profit"),
                "stop_loss": strategy_context.get("stop_loss"),
            }
        )
        static_levels = self._normalize_monitor_levels(
            {
                "entry_min": (config.get("entry_range") or {}).get("min"),
                "entry_max": (config.get("entry_range") or {}).get("max"),
                "take_profit": config.get("take_profit"),
                "stop_loss": config.get("stop_loss"),
            }
        )
        if runtime_levels:
            return {"levels": runtime_levels, "threshold_source": config.get("threshold_source") or "ai_runtime"}
        if strategy_levels:
            return {"levels": strategy_levels, "threshold_source": "strategy_context"}
        if static_levels:
            return {"levels": static_levels, "threshold_source": config.get("threshold_source") or "manual"}
        return {
            "levels": {
                "entry_min": None,
                "entry_max": None,
                "take_profit": None,
                "stop_loss": None,
            },
            "threshold_source": config.get("threshold_source") or "manual",
        }

    def _item_to_stock(self, item: Dict) -> Dict:
        config = item.get("config") or {}
        effective = self._resolve_effective_levels(item, config)
        levels = effective["levels"]
        return {
            "id": item["id"],
            "symbol": item["symbol"],
            "name": item.get("name") or item["symbol"],
            "enabled": bool(item.get("enabled", True)),
            "source": item.get("source") or "manual",
            "rating": config.get("rating", "持有"),
            "entry_range": {
                "min": levels.get("entry_min"),
                "max": levels.get("entry_max"),
            } if levels.get("entry_min") is not None and levels.get("entry_max") is not None else {},
            "take_profit": levels.get("take_profit"),
            "stop_loss": levels.get("stop_loss"),
            "current_price": item.get("current_price"),
            "last_checked": item.get("last_checked"),
            "check_interval": item.get("interval_minutes", 30),
            "notification_enabled": bool(item.get("notification_enabled", True)),
            "trading_hours_only": bool(item.get("trading_hours_only", True)),
            "managed_by_portfolio": bool(item.get("managed_by_portfolio", False)),
            "account_name": item.get("account_name"),
            "asset_id": item.get("asset_id"),
            "portfolio_stock_id": item.get("portfolio_stock_id"),
            "origin_analysis_id": item.get("origin_analysis_id"),
            "threshold_source": effective.get("threshold_source"),
            "threshold_updated_at": config.get("threshold_updated_at"),
            "origin_decision_id": config.get("origin_decision_id"),
            "runtime_thresholds": config.get("runtime_thresholds") or {},
            "strategy_context": self._resolve_strategy_context(item, config),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
        }

    def add_monitored_stock(
        self,
        symbol: str,
        name: str,
        rating: str,
        entry_range: Dict,
        take_profit: float,
        stop_loss: float,
        check_interval: Optional[int] = None,
        notification_enabled: bool = True,
        trading_hours_only: bool = True,
        managed_by_portfolio: bool = False,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
        origin_analysis_id: Optional[int] = None,
    ) -> int:
        item_data = {
            "symbol": symbol,
            "name": name or symbol,
            "monitor_type": "price_alert",
            "source": "portfolio" if managed_by_portfolio else "manual",
            "enabled": True,
            "interval_minutes": int(check_interval or self._default_check_interval_minutes()),
            "trading_hours_only": trading_hours_only,
            "notification_enabled": notification_enabled,
            "managed_by_portfolio": managed_by_portfolio,
            "account_name": account_name,
            "asset_id": asset_id,
            "portfolio_stock_id": portfolio_stock_id,
            "origin_analysis_id": origin_analysis_id,
            "config": self._build_config(
                rating,
                entry_range,
                take_profit,
                stop_loss,
                threshold_source="manual",
                threshold_updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        }
        if managed_by_portfolio:
            return self.repository.upsert_item(item_data)
        return self.repository.create_item(item_data)

    def get_monitored_stocks(self) -> List[Dict]:
        items = self.repository.list_items(monitor_type="price_alert")
        return [self._item_to_stock(item) for item in items]

    def update_stock_price(self, stock_id: int, price: float):
        self.repository.update_runtime(
            stock_id,
            current_price=price,
            last_status="price_updated",
            last_message=f"最新价格 {price}",
        )

    def update_last_checked(self, stock_id: int):
        self.repository.update_runtime(stock_id, last_status="checked")

    def has_latest_notification_type(self, stock_id: int, notification_type: str) -> bool:
        return self.repository.has_latest_notification_type(stock_id, notification_type)

    def has_recent_notification(self, stock_id: int, notification_type: str, minutes: int = 60) -> bool:
        return self.has_latest_notification_type(stock_id, notification_type)

    def add_notification(self, stock_id: int, notification_type: str, message: str):
        self.repository.record_event(
            item_id=stock_id,
            event_type=notification_type,
            message=message,
            notification_pending=True,
            suppress_if_latest_same_type=True,
        )

    def get_pending_notifications(self) -> List[Dict]:
        return self.repository.get_pending_notifications()

    def get_all_recent_notifications(self, limit: int = 10) -> List[Dict]:
        return self.repository.get_all_recent_notifications(limit=limit)

    def mark_notification_sent(self, notification_id: int):
        self.repository.mark_notification_sent(notification_id)

    def ignore_notification(self, event_id: int):
        self.repository.ignore_notification(event_id)

    def mark_all_notifications_sent(self):
        return self.repository.mark_all_notifications_sent()

    def clear_all_notifications(self):
        return self.repository.clear_all_notifications()

    def remove_monitored_stock(self, stock_id: int):
        return self.repository.delete_item(stock_id)

    def update_monitored_stock(
        self,
        stock_id: int,
        rating: str,
        entry_range: Dict,
        take_profit: float,
        stop_loss: float,
        check_interval: Optional[int],
        notification_enabled: bool,
        trading_hours_only: bool = None,
        managed_by_portfolio: Optional[bool] = None,
    ):
        item = self.repository.get_item(stock_id)
        if not item or item.get("monitor_type") != "price_alert":
            return False

        updates = {
            "interval_minutes": int(check_interval or self._default_check_interval_minutes()),
            "notification_enabled": notification_enabled,
            "config": self._build_config(
                rating,
                entry_range,
                take_profit,
                stop_loss,
                threshold_source="manual",
                threshold_updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        }
        if trading_hours_only is not None:
            updates["trading_hours_only"] = trading_hours_only
        if managed_by_portfolio is not None:
            updates["managed_by_portfolio"] = managed_by_portfolio
            updates["source"] = "portfolio" if managed_by_portfolio else item.get("source", "manual")
        return self.repository.update_item(stock_id, updates)

    def toggle_notification(self, stock_id: int, enabled: bool):
        return self.repository.set_notification_enabled(stock_id, enabled)

    def get_stock_by_id(self, stock_id: int) -> Optional[Dict]:
        item = self.repository.get_item(stock_id)
        if not item or item.get("monitor_type") != "price_alert":
            return None
        return self._item_to_stock(item)

    def get_monitor_by_code(
        self,
        symbol: str,
        managed_only: Optional[bool] = None,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> Optional[Dict]:
        item = self.repository.get_item_by_symbol(
            symbol,
            monitor_type="price_alert",
            managed_only=managed_only,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        return self._item_to_stock(item) if item else None

    def apply_runtime_thresholds(
        self,
        stock_id: int,
        *,
        rating: str,
        monitor_levels: Dict,
        origin_decision_id: int,
    ) -> bool:
        item = self.repository.get_item(stock_id)
        if not item or item.get("monitor_type") != "price_alert":
            return False
        normalized_levels = self._normalize_monitor_levels(monitor_levels)
        if not normalized_levels:
            return False
        config = dict(item.get("config") or {})
        config.update(
            self._build_config(
                rating,
                {
                    "min": normalized_levels["entry_min"],
                    "max": normalized_levels["entry_max"],
                },
                normalized_levels["take_profit"],
                normalized_levels["stop_loss"],
                runtime_thresholds=normalized_levels,
                threshold_source="ai_runtime",
                threshold_updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                origin_decision_id=origin_decision_id,
                strategy_context=config.get("strategy_context") if isinstance(config.get("strategy_context"), dict) else None,
            )
        )
        return self.repository.update_item(stock_id, {"config": config})

    def remove_monitor_by_code(
        self,
        symbol: str,
        managed_only: bool = False,
        account_name: Optional[str] = None,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> bool:
        return self.repository.delete_by_symbol(
            symbol,
            monitor_type="price_alert",
            managed_only=managed_only,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )

    def batch_add_or_update_monitors(self, monitors_data: List[Dict]) -> Dict[str, int]:
        added = 0
        updated = 0
        failed = 0

        for data in monitors_data:
            try:
                symbol = data.get("code") or data.get("symbol")
                name = data.get("name", symbol)
                rating = data.get("rating", "持有")
                entry_min = data.get("entry_min")
                entry_max = data.get("entry_max")
                take_profit = data.get("take_profit")
                stop_loss = data.get("stop_loss")
                check_interval = data.get("check_interval", 30)
                notification_enabled = data.get("notification_enabled", True)
                trading_hours_only = data.get("trading_hours_only", True)
                managed_by_portfolio = data.get("managed_by_portfolio", False)
                account_name = data.get("account_name")
                asset_id = data.get("asset_id")
                portfolio_stock_id = data.get("portfolio_stock_id")
                origin_analysis_id = data.get("origin_analysis_id")

                if not symbol or not all(v is not None for v in (entry_min, entry_max, take_profit, stop_loss)):
                    failed += 1
                    continue

                existing = self.get_monitor_by_code(
                    symbol,
                    managed_only=True if managed_by_portfolio else None,
                    account_name=account_name,
                    asset_id=asset_id,
                    portfolio_stock_id=portfolio_stock_id,
                )
                if existing and (managed_by_portfolio or not existing.get("managed_by_portfolio")):
                    self.update_monitored_stock(
                        existing["id"],
                        rating=rating,
                        entry_range={"min": entry_min, "max": entry_max},
                        take_profit=take_profit,
                        stop_loss=stop_loss,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                        trading_hours_only=trading_hours_only,
                        managed_by_portfolio=managed_by_portfolio,
                    )
                    updated += 1
                else:
                    self.add_monitored_stock(
                        symbol=symbol,
                        name=name,
                        rating=rating,
                        entry_range={"min": entry_min, "max": entry_max},
                        take_profit=take_profit,
                        stop_loss=stop_loss,
                        check_interval=check_interval,
                        notification_enabled=notification_enabled,
                        trading_hours_only=trading_hours_only,
                        managed_by_portfolio=managed_by_portfolio,
                        asset_id=asset_id,
                        account_name=account_name,
                        portfolio_stock_id=portfolio_stock_id,
                        origin_analysis_id=origin_analysis_id,
                    )
                    added += 1
            except Exception:
                failed += 1

        return {
            "added": added,
            "updated": updated,
            "failed": failed,
            "total": added + updated + failed,
        }


monitor_db = StockMonitorDatabase()
