import json
from typing import Dict, List, Optional

from monitoring_repository import MonitoringRepository, resolve_monitoring_db_path


class StockMonitorDatabase:
    """Compatibility facade for price alerts backed by the unified monitoring repository."""

    def __init__(self, db_path: str = "stock_monitor.db"):
        self.db_path = db_path
        self.repository = MonitoringRepository(resolve_monitoring_db_path(db_path))
        self.repository.migrate_legacy_stock_db(db_path)

    @staticmethod
    def _build_config(
        rating: str,
        entry_range: Optional[Dict],
        take_profit: Optional[float],
        stop_loss: Optional[float],
        quant_enabled: bool = False,
        quant_config: Optional[Dict] = None,
    ) -> Dict:
        return {
            "rating": rating,
            "entry_range": entry_range or {},
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "quant_enabled": bool(quant_enabled),
            "quant_config": quant_config or {},
        }

    @staticmethod
    def _item_to_stock(item: Dict) -> Dict:
        config = item.get("config") or {}
        return {
            "id": item["id"],
            "symbol": item["symbol"],
            "name": item.get("name") or item["symbol"],
            "rating": config.get("rating", "持有"),
            "entry_range": config.get("entry_range") or {},
            "take_profit": config.get("take_profit"),
            "stop_loss": config.get("stop_loss"),
            "current_price": item.get("current_price"),
            "last_checked": item.get("last_checked"),
            "check_interval": item.get("interval_minutes", 30),
            "notification_enabled": bool(item.get("notification_enabled", True)),
            "trading_hours_only": bool(item.get("trading_hours_only", True)),
            "quant_enabled": bool(config.get("quant_enabled", False)),
            "quant_config": config.get("quant_config") or {},
            "managed_by_portfolio": bool(item.get("managed_by_portfolio", False)),
            "account_name": item.get("account_name"),
            "portfolio_stock_id": item.get("portfolio_stock_id"),
            "origin_analysis_id": item.get("origin_analysis_id"),
            "strategy_context": config.get("strategy_context") or {},
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
        check_interval: int = 30,
        notification_enabled: bool = True,
        trading_hours_only: bool = True,
        quant_enabled: bool = False,
        quant_config: Dict = None,
        managed_by_portfolio: bool = False,
        account_name: Optional[str] = None,
        portfolio_stock_id: Optional[int] = None,
        origin_analysis_id: Optional[int] = None,
    ) -> int:
        item_data = {
            "symbol": symbol,
            "name": name or symbol,
            "monitor_type": "price_alert",
            "source": "portfolio" if managed_by_portfolio else "manual",
            "enabled": True,
            "interval_minutes": check_interval,
            "trading_hours_only": trading_hours_only,
            "notification_enabled": notification_enabled,
            "managed_by_portfolio": managed_by_portfolio,
            "account_name": account_name,
            "portfolio_stock_id": portfolio_stock_id,
            "origin_analysis_id": origin_analysis_id,
            "config": self._build_config(
                rating,
                entry_range,
                take_profit,
                stop_loss,
                quant_enabled=quant_enabled,
                quant_config=quant_config,
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

    def has_recent_notification(self, stock_id: int, notification_type: str, minutes: int = 60) -> bool:
        return self.repository.has_recent_notification(stock_id, notification_type, minutes=minutes)

    def add_notification(self, stock_id: int, notification_type: str, message: str):
        self.repository.record_event(
            item_id=stock_id,
            event_type=notification_type,
            message=message,
            notification_pending=True,
        )

    def get_pending_notifications(self) -> List[Dict]:
        return self.repository.get_pending_notifications()

    def get_all_recent_notifications(self, limit: int = 10) -> List[Dict]:
        return self.repository.get_all_recent_notifications(limit=limit)

    def mark_notification_sent(self, notification_id: int):
        self.repository.mark_notification_sent(notification_id)

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
        check_interval: int,
        notification_enabled: bool,
        trading_hours_only: bool = None,
        quant_enabled: bool = None,
        quant_config: Dict = None,
        managed_by_portfolio: Optional[bool] = None,
    ):
        item = self.repository.get_item(stock_id)
        if not item or item.get("monitor_type") != "price_alert":
            return False

        current_config = dict(item.get("config") or {})
        updates = {
            "interval_minutes": check_interval,
            "notification_enabled": notification_enabled,
            "config": self._build_config(
                rating,
                entry_range,
                take_profit,
                stop_loss,
                quant_enabled=current_config.get("quant_enabled", False) if quant_enabled is None else quant_enabled,
                quant_config=current_config.get("quant_config", {}) if quant_config is None else quant_config,
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
        portfolio_stock_id: Optional[int] = None,
    ) -> Optional[Dict]:
        item = self.repository.get_item_by_symbol(
            symbol,
            monitor_type="price_alert",
            managed_only=managed_only,
            account_name=account_name,
            portfolio_stock_id=portfolio_stock_id,
        )
        return self._item_to_stock(item) if item else None

    def remove_monitor_by_code(
        self,
        symbol: str,
        managed_only: bool = False,
        account_name: Optional[str] = None,
        portfolio_stock_id: Optional[int] = None,
    ) -> bool:
        return self.repository.delete_by_symbol(
            symbol,
            monitor_type="price_alert",
            managed_only=managed_only,
            account_name=account_name,
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
                portfolio_stock_id = data.get("portfolio_stock_id")
                origin_analysis_id = data.get("origin_analysis_id")

                if not symbol or not all(v is not None for v in (entry_min, entry_max, take_profit, stop_loss)):
                    failed += 1
                    continue

                existing = self.get_monitor_by_code(
                    symbol,
                    managed_only=True if managed_by_portfolio else None,
                    account_name=account_name,
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

