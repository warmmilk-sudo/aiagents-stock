from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from analysis_repository import AnalysisRepository, analysis_repository
from investment_db_utils import DEFAULT_ACCOUNT_NAME
from monitor_db import StockMonitorDatabase, monitor_db
from monitoring_repository import MonitoringRepository
from portfolio_db import PortfolioDB, portfolio_db


class InvestmentLifecycleService:
    """Coordinates canonical positions, strategy projections, and monitor write-back."""

    DEFAULT_AI_INTERVAL_SECONDS = 300
    DEFAULT_ALERT_INTERVAL_MINUTES = 60

    def __init__(
        self,
        portfolio_store: Optional[PortfolioDB] = None,
        realtime_monitor_store: Optional[StockMonitorDatabase] = None,
        analysis_store: Optional[AnalysisRepository] = None,
        monitoring_store: Optional[MonitoringRepository] = None,
    ):
        self.portfolio_db = portfolio_store or portfolio_db
        self.realtime_monitor_db = realtime_monitor_store or monitor_db
        self.analysis_repository = analysis_store or analysis_repository
        self.monitoring_repository = monitoring_store or self.realtime_monitor_db.repository

    def _build_strategy_context(self, stock: Dict) -> Optional[Dict]:
        return self.analysis_repository.get_latest_strategy_context(
            portfolio_stock_id=stock.get("id"),
            symbol=stock.get("code"),
            account_name=stock.get("account_name") or DEFAULT_ACCOUNT_NAME,
        )

    def _build_ai_task_projection(self, stock: Dict, strategy_context: Optional[Dict], existing: Optional[Dict]) -> Dict:
        existing = existing or {}
        existing_config = existing.get("config") or {}
        quantity = int(stock.get("quantity") or 0)
        cost_price = float(stock.get("cost_price") or 0)
        has_position = bool(quantity > 0 and cost_price > 0)
        task_name = existing_config.get("task_name") or f"{stock.get('name') or stock['code']}盯盘"
        origin_analysis_id = (
            (strategy_context or {}).get("origin_analysis_id")
            or stock.get("origin_analysis_id")
            or existing.get("origin_analysis_id")
        )
        return {
            "symbol": stock["code"],
            "name": stock.get("name") or stock["code"],
            "monitor_type": "ai_task",
            "source": "portfolio",
            "enabled": bool(existing.get("enabled", False)),
            "interval_minutes": int(existing.get("interval_minutes") or max(1, self.DEFAULT_AI_INTERVAL_SECONDS // 60)),
            "trading_hours_only": bool(existing.get("trading_hours_only", True)),
            "notification_enabled": bool(existing.get("notification_enabled", True)),
            "managed_by_portfolio": True,
            "account_name": stock.get("account_name") or DEFAULT_ACCOUNT_NAME,
            "portfolio_stock_id": stock["id"],
            "origin_analysis_id": origin_analysis_id,
            "config": {
                "task_name": task_name,
                "auto_trade": bool(existing_config.get("auto_trade", False)),
                "position_size_pct": existing_config.get("position_size_pct", 20),
                "stop_loss_pct": existing_config.get("stop_loss_pct", 5),
                "take_profit_pct": existing_config.get("take_profit_pct", 10),
                "qmt_account_id": existing_config.get("qmt_account_id"),
                "notify_email": existing_config.get("notify_email"),
                "notify_webhook": existing_config.get("notify_webhook"),
                "has_position": has_position,
                "position_cost": cost_price if has_position else 0,
                "position_quantity": quantity if has_position else 0,
                "position_date": existing_config.get("position_date") or datetime.now().strftime("%Y-%m-%d"),
                "strategy_context": strategy_context or {},
            },
        }

    def _build_price_alert_projection(self, stock: Dict, strategy_context: Optional[Dict], existing: Optional[Dict]) -> Optional[Dict]:
        if not strategy_context:
            return None
        entry_min = strategy_context.get("entry_min")
        entry_max = strategy_context.get("entry_max")
        take_profit = strategy_context.get("take_profit")
        stop_loss = strategy_context.get("stop_loss")
        if not all(value is not None for value in (entry_min, entry_max, take_profit, stop_loss)):
            return None
        existing = existing or {}
        existing_config = existing.get("config") or {}
        return {
            "symbol": stock["code"],
            "name": stock.get("name") or stock["code"],
            "monitor_type": "price_alert",
            "source": "portfolio",
            "enabled": True,
            "interval_minutes": int(existing.get("interval_minutes") or self.DEFAULT_ALERT_INTERVAL_MINUTES),
            "trading_hours_only": bool(existing.get("trading_hours_only", True)),
            "notification_enabled": bool(existing.get("notification_enabled", True)),
            "managed_by_portfolio": True,
            "account_name": stock.get("account_name") or DEFAULT_ACCOUNT_NAME,
            "portfolio_stock_id": stock["id"],
            "origin_analysis_id": strategy_context.get("origin_analysis_id"),
            "config": {
                "rating": strategy_context.get("rating") or "持有",
                "entry_range": {"min": float(entry_min), "max": float(entry_max)},
                "take_profit": float(take_profit),
                "stop_loss": float(stop_loss),
                "quant_enabled": bool(existing_config.get("quant_enabled", False)),
                "quant_config": existing_config.get("quant_config") or {},
                "strategy_context": strategy_context,
            },
        }

    def _iter_target_stocks(
        self,
        *,
        stock_id: Optional[int] = None,
        account_name: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> List[Dict]:
        if stock_id is not None:
            stock = self.portfolio_db.get_stock(stock_id)
            return [stock] if stock else []
        stocks = self.portfolio_db.get_all_stocks(auto_monitor_only=False)
        result = []
        for stock in stocks:
            if stock.get("position_status", "active") != "active":
                continue
            if account_name and stock.get("account_name") != account_name:
                continue
            if symbol and stock.get("code") != symbol:
                continue
            result.append(stock)
        return result

    def _delete_managed_items_for_position(self, stock: Dict) -> Dict[str, int]:
        deleted_ai = 0
        deleted_alert = 0
        account_name = stock.get("account_name") or DEFAULT_ACCOUNT_NAME
        portfolio_stock_id = stock.get("id")
        if self.monitoring_repository.delete_by_symbol(
            stock["code"],
            monitor_type="ai_task",
            managed_only=True,
            account_name=account_name,
            portfolio_stock_id=portfolio_stock_id,
        ):
            deleted_ai = 1
        if self.monitoring_repository.delete_by_symbol(
            stock["code"],
            monitor_type="price_alert",
            managed_only=True,
            account_name=account_name,
            portfolio_stock_id=portfolio_stock_id,
        ):
            deleted_alert = 1
        return {"ai_task_deleted": deleted_ai, "price_alert_deleted": deleted_alert}

    def sync_position(
        self,
        *,
        stock_id: Optional[int] = None,
        account_name: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> Dict[str, int]:
        summary = {
            "positions": 0,
            "ai_tasks_upserted": 0,
            "price_alerts_upserted": 0,
            "price_alerts_removed": 0,
            "ai_tasks_removed": 0,
        }
        for stock in self._iter_target_stocks(stock_id=stock_id, account_name=account_name, symbol=symbol):
            summary["positions"] += 1
            if not stock.get("auto_monitor", True) or stock.get("position_status", "active") != "active":
                deleted = self._delete_managed_items_for_position(stock)
                summary["ai_tasks_removed"] += deleted["ai_task_deleted"]
                summary["price_alerts_removed"] += deleted["price_alert_deleted"]
                continue

            strategy_context = self._build_strategy_context(stock)
            existing_task = self.monitoring_repository.get_item_by_symbol(
                stock["code"],
                monitor_type="ai_task",
                managed_only=True,
                account_name=stock.get("account_name") or DEFAULT_ACCOUNT_NAME,
                portfolio_stock_id=stock["id"],
            )
            self.monitoring_repository.upsert_item(
                self._build_ai_task_projection(stock, strategy_context, existing_task)
            )
            summary["ai_tasks_upserted"] += 1

            existing_alert = self.monitoring_repository.get_item_by_symbol(
                stock["code"],
                monitor_type="price_alert",
                managed_only=True,
                account_name=stock.get("account_name") or DEFAULT_ACCOUNT_NAME,
                portfolio_stock_id=stock["id"],
            )
            alert_payload = self._build_price_alert_projection(stock, strategy_context, existing_alert)
            if alert_payload:
                self.monitoring_repository.upsert_item(alert_payload)
                summary["price_alerts_upserted"] += 1
            else:
                if self.monitoring_repository.delete_by_symbol(
                    stock["code"],
                    monitor_type="price_alert",
                    managed_only=True,
                    account_name=stock.get("account_name") or DEFAULT_ACCOUNT_NAME,
                    portfolio_stock_id=stock["id"],
                ):
                    summary["price_alerts_removed"] += 1
        return summary

    def create_position_from_analysis(
        self,
        *,
        symbol: str,
        stock_name: str,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        cost_price: Optional[float] = None,
        quantity: Optional[int] = None,
        note: str = "",
        auto_monitor: bool = True,
        origin_analysis_id: Optional[int] = None,
    ) -> Tuple[bool, str, Optional[int]]:
        existing = self.portfolio_db.get_stock_by_code(symbol, account_name)
        if existing:
            update_fields = {
                "name": stock_name or existing.get("name") or symbol,
                "cost_price": cost_price if cost_price is not None else existing.get("cost_price"),
                "quantity": quantity if quantity is not None else existing.get("quantity"),
                "note": note or existing.get("note") or "",
                "auto_monitor": auto_monitor,
                "position_status": "active",
                "origin_analysis_id": origin_analysis_id or existing.get("origin_analysis_id"),
            }
            self.portfolio_db.update_stock(existing["id"], **update_fields)
            self.sync_position(stock_id=existing["id"])
            return True, f"已更新持仓: {symbol}", existing["id"]

        stock_id = self.portfolio_db.add_stock(
            symbol,
            stock_name or symbol,
            cost_price,
            quantity,
            note,
            auto_monitor,
            account_name,
        )
        if origin_analysis_id:
            self.portfolio_db.update_stock(stock_id, origin_analysis_id=origin_analysis_id)
        self.sync_position(stock_id=stock_id)
        return True, f"已创建持仓: {symbol}", stock_id

    def apply_monitor_execution(
        self,
        *,
        stock_code: str,
        stock_name: str,
        trade_type: str,
        quantity: int,
        price: float,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        portfolio_stock_id: Optional[int] = None,
        origin_analysis_id: Optional[int] = None,
        note: str = "",
        ai_decision_id: Optional[int] = None,
        order_id: Optional[str] = None,
        order_status: Optional[str] = None,
        trade_date: Optional[str] = None,
        trade_source: str = "ai_monitor",
    ) -> Dict:
        normalized_type = str(trade_type or "").strip().lower()
        if normalized_type not in {"buy", "sell"}:
            return {"success": False, "error": "unsupported_trade_type"}
        trade_quantity = int(quantity or 0)
        trade_price = float(price or 0)
        if trade_quantity <= 0 or trade_price <= 0:
            return {"success": False, "error": "invalid_trade_payload"}

        stock = self.portfolio_db.get_stock(portfolio_stock_id) if portfolio_stock_id else None
        if not stock:
            stock = self.portfolio_db.get_stock_by_code(stock_code, account_name)

        if normalized_type == "buy":
            if not stock:
                created, message, new_stock_id = self.create_position_from_analysis(
                    symbol=stock_code,
                    stock_name=stock_name,
                    account_name=account_name,
                    cost_price=trade_price,
                    quantity=trade_quantity,
                    note=note,
                    auto_monitor=True,
                    origin_analysis_id=origin_analysis_id,
                )
                if not created or new_stock_id is None:
                    return {"success": False, "error": message}
                stock = self.portfolio_db.get_stock(new_stock_id)
            else:
                current_quantity = int(stock.get("quantity") or 0)
                current_cost = float(stock.get("cost_price") or 0)
                total_cost = (current_cost * current_quantity) + (trade_price * trade_quantity)
                new_quantity = current_quantity + trade_quantity
                new_cost = total_cost / new_quantity if new_quantity > 0 else trade_price
                self.portfolio_db.update_stock(
                    stock["id"],
                    name=stock_name or stock.get("name") or stock_code,
                    quantity=new_quantity,
                    cost_price=new_cost,
                    auto_monitor=True,
                    position_status="active",
                    origin_analysis_id=origin_analysis_id or stock.get("origin_analysis_id"),
                    last_trade_at=trade_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                stock = self.portfolio_db.get_stock(stock["id"])
            self.portfolio_db.add_trade_history(
                stock_id=stock["id"],
                trade_type="buy",
                trade_date=trade_date or datetime.now().strftime("%Y-%m-%d"),
                price=trade_price,
                quantity=trade_quantity,
                note=note or f"AI决策#{ai_decision_id or '-'} order={order_id or '-'} status={order_status or '-'}",
                trade_source=trade_source,
            )
            self.sync_position(stock_id=stock["id"])
            return {"success": True, "portfolio_stock_id": stock["id"], "trade_type": "buy"}

        if not stock:
            return {"success": False, "error": "position_not_found"}
        current_quantity = int(stock.get("quantity") or 0)
        if current_quantity <= 0:
            return {"success": False, "error": "position_not_found"}
        sell_quantity = min(trade_quantity, current_quantity)
        self.portfolio_db.add_trade_history(
            stock_id=stock["id"],
            trade_type="sell",
            trade_date=trade_date or datetime.now().strftime("%Y-%m-%d"),
            price=trade_price,
            quantity=sell_quantity,
            note=note or f"AI决策#{ai_decision_id or '-'} order={order_id or '-'} status={order_status or '-'}",
            trade_source=trade_source,
        )
        remaining = current_quantity - sell_quantity
        if remaining > 0:
            self.portfolio_db.update_stock(
                stock["id"],
                quantity=remaining,
                cost_price=stock.get("cost_price"),
                position_status="active",
                last_trade_at=trade_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            self.sync_position(stock_id=stock["id"])
            return {"success": True, "portfolio_stock_id": stock["id"], "trade_type": "sell", "remaining_quantity": remaining}

        self.portfolio_db.update_stock(
            stock["id"],
            quantity=None,
            cost_price=None,
            position_status="closed",
            last_trade_at=trade_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        deleted = self._delete_managed_items_for_position(stock)
        return {
            "success": True,
            "portfolio_stock_id": stock["id"],
            "trade_type": "sell",
            "remaining_quantity": 0,
            **deleted,
        }


investment_lifecycle_service = InvestmentLifecycleService()
