import logging
import os
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

from miniqmt_interface import miniqmt
from monitor_db import monitor_db
from monitoring_repository import MonitoringRepository
from notification_service import notification_service
from smart_monitor_engine import SmartMonitorEngine
from stock_data import StockDataFetcher

try:
    from smart_monitor_tdx_data import SmartMonitorTDXDataFetcher

    TDX_AVAILABLE = True
except ImportError:
    TDX_AVAILABLE = False


class MonitoringOrchestrator:
    """Unified execution engine for AI tasks and price alerts."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.repository: MonitoringRepository = monitor_db.repository
        self.engine = SmartMonitorEngine()
        self.fetcher = StockDataFetcher()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.poll_seconds = 30

        self.tdx_fetcher = None
        self.use_tdx = False
        tdx_enabled = os.getenv("TDX_ENABLED", "false").lower() == "true"
        tdx_base_url = os.getenv("TDX_BASE_URL", "http://192.168.1.222:8181")
        if tdx_enabled and TDX_AVAILABLE:
            try:
                self.tdx_fetcher = SmartMonitorTDXDataFetcher(base_url=tdx_base_url)
                self.use_tdx = True
                self.logger.info(f"TDX数据源已启用: {tdx_base_url}")
            except (OSError, RuntimeError, ValueError, TypeError) as exc:
                self.logger.warning(f"TDX数据源初始化失败，将使用默认数据源: {exc}")
            except Exception:
                self.logger.exception("TDX数据源初始化出现未知异常，将降级使用默认数据源")

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.info("监测服务已启动")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.logger.info("监测服务已停止")

    def get_status(self) -> Dict:
        items = self.repository.list_items()
        return {
            "running": self.running,
            "total_items": len(items),
            "ai_tasks": len([item for item in items if item["monitor_type"] == "ai_task"]),
            "price_alerts": len([item for item in items if item["monitor_type"] == "price_alert"]),
            "pending_notifications": len(self.repository.get_pending_notifications()),
        }

    def get_registry_items(
        self,
        monitor_type: Optional[str] = None,
        managed_by_portfolio: Optional[bool] = None,
        enabled_only: bool = False,
    ) -> List[Dict]:
        return self.repository.list_items(
            monitor_type=monitor_type,
            managed_by_portfolio=managed_by_portfolio,
            enabled_only=enabled_only,
        )

    def get_recent_events(self, limit: int = 50) -> List[Dict]:
        return self.repository.get_recent_events(limit=limit)

    def get_due_items(self) -> List[Dict]:
        return self.repository.get_due_items(now=datetime.now(), service_running=self.running)

    def get_stocks_needing_update(self) -> List[Dict]:
        due_items = self.repository.get_due_items(now=datetime.now(), service_running=True)
        stocks: List[Dict] = []
        for item in due_items:
            if item["monitor_type"] != "price_alert":
                continue
            stock = monitor_db.get_stock_by_id(item["id"])
            if stock:
                stocks.append(stock)
        return stocks

    def manual_update_item(self, item_id: int) -> bool:
        item = self.repository.get_item(item_id)
        if not item:
            return False
        return self._dispatch_item(item, force=True)

    def manual_update_stock(self, stock_id: int) -> bool:
        return self.manual_update_item(stock_id)

    def run_once(self):
        due_items = self.repository.get_due_items(now=datetime.now(), service_running=self.running)
        for item in due_items:
            self._dispatch_item(item)

    def _run_loop(self):
        while self.running:
            try:
                self.run_once()
                time.sleep(self.poll_seconds)
            except Exception:
                self.logger.exception("监测服务执行异常，下一轮将重试")
                time.sleep(min(self.poll_seconds, 10))

    def _dispatch_item(self, item: Dict, force: bool = False) -> bool:
        symbol = item.get("symbol", "UNKNOWN") if isinstance(item, dict) else "UNKNOWN"
        item_id = item.get("id") if isinstance(item, dict) else None
        try:
            monitor_type = item.get("monitor_type")
            if monitor_type == "price_alert":
                return self._process_price_alert(item, force=force)
            if monitor_type == "ai_task":
                return self._process_ai_task(item)
            self.logger.warning("未知监控类型: %s", monitor_type)
            return False
        except (KeyError, TypeError, ValueError) as exc:
            self.logger.warning("[%s] 监控项数据异常，已降级失败: %s", symbol, exc)
            if item_id is not None:
                self.repository.update_runtime(
                    item_id,
                    last_status="failed",
                    last_message=f"invalid_item:{exc}",
                )
            return False
        except Exception as exc:
            self.logger.exception("[%s] 处理监控项出现未知异常", symbol)
            if item_id is not None:
                self.repository.update_runtime(
                    item_id,
                    last_status="failed",
                    last_message=str(exc),
                )
            return False

    def _process_ai_task(self, item: Dict) -> bool:
        config = item.get("config") or {}
        result = self.engine.analyze_stock(
            stock_code=item["symbol"],
            auto_trade=bool(config.get("auto_trade", False)),
            notify=bool(item.get("notification_enabled", True)),
            has_position=bool(config.get("has_position", False)),
            position_cost=float(config.get("position_cost", 0) or 0),
            position_quantity=int(config.get("position_quantity", 0) or 0),
            trading_hours_only=bool(item.get("trading_hours_only", True)),
        )

        if result.get("success"):
            action = result["decision"]["action"].upper()
            message = f"AI决策: {action}"
            self.repository.update_runtime(
                item["id"],
                last_status=action.lower(),
                last_message=message,
            )
            self.repository.record_event(
                item_id=item["id"],
                event_type="ai_analysis",
                message=message,
                notification_pending=False,
                sent=True,
            )
            return True

        if result.get("skipped"):
            self.repository.update_runtime(
                item["id"],
                last_status="skipped",
                last_message=result.get("error", "交易时段外跳过"),
            )
            return False

        self.repository.update_runtime(
            item["id"],
            last_status="error",
            last_message=result.get("error", "AI分析失败"),
        )
        return False

    def _process_price_alert(self, item: Dict, force: bool = False) -> bool:
        stock = monitor_db.get_stock_by_id(item["id"])
        if not stock:
            return False

        if (
            not force
            and stock.get("trading_hours_only")
            and not self._is_trading_time()
        ):
            self.repository.update_item(
                item["id"],
                {
                    "last_status": "waiting_trading_hours",
                    "last_message": "非交易时段，等待执行",
                },
            )
            return False

        current_price = self._get_latest_price(stock["symbol"])
        if current_price and current_price > 0:
            monitor_db.update_stock_price(stock["id"], current_price)
            self._check_trigger_conditions(stock, current_price)
            return True

        monitor_db.update_last_checked(stock["id"])
        return False

    def _is_a_stock(self, symbol: str) -> bool:
        return symbol.isdigit() and len(symbol) == 6

    def _get_latest_price(self, symbol: str) -> Optional[float]:
        if self.use_tdx and self._is_a_stock(symbol):
            try:
                quote = self.tdx_fetcher.get_realtime_quote(symbol)
                if quote and quote.get("current_price"):
                    return float(quote["current_price"])
            except (ValueError, TypeError, RuntimeError, OSError, ConnectionError, TimeoutError) as exc:
                self.logger.warning(f"[{symbol}] TDX获取失败，降级默认数据源: {exc}")
            except Exception:
                self.logger.exception("[%s] TDX获取出现未知异常，降级默认数据源", symbol)

        try:
            stock_info = self.fetcher.get_stock_info(
                symbol,
                max_age_seconds=30,
                allow_stale_on_failure=True,
                cache_first=True,
            )
            current_price = stock_info.get("current_price")
            if current_price and current_price != "N/A":
                return float(current_price)
        except (ValueError, TypeError, RuntimeError, OSError, ConnectionError, TimeoutError) as exc:
            self.logger.warning(f"[{symbol}] 默认数据源获取失败: {exc}")
        except Exception:
            self.logger.exception("[%s] 默认数据源获取出现未知异常", symbol)
        return None

    def _check_trigger_conditions(self, stock: Dict, current_price: float):
        if not stock.get("notification_enabled", True):
            return

        entry_range = stock.get("entry_range", {})
        take_profit = stock.get("take_profit")
        stop_loss = stock.get("stop_loss")

        if entry_range and entry_range.get("min") and entry_range.get("max"):
            if entry_range["min"] <= current_price <= entry_range["max"]:
                if not monitor_db.has_recent_notification(stock["id"], "entry", minutes=60):
                    message = (
                        f"股票 {stock['symbol']} ({stock['name']}) 价格 {current_price} "
                        f"进入进场区间 [{entry_range['min']}-{entry_range['max']}]"
                    )
                    monitor_db.add_notification(stock["id"], "entry", message)
                    notification_service.send_notifications()
                if stock.get("quant_enabled", False):
                    self._execute_quant_trade(stock, "entry", current_price)

        if take_profit and current_price >= take_profit:
            if not monitor_db.has_recent_notification(stock["id"], "take_profit", minutes=60):
                message = (
                    f"股票 {stock['symbol']} ({stock['name']}) 价格 {current_price} "
                    f"达到止盈位 {take_profit}"
                )
                monitor_db.add_notification(stock["id"], "take_profit", message)
                notification_service.send_notifications()
            if stock.get("quant_enabled", False):
                self._execute_quant_trade(stock, "take_profit", current_price)

        if stop_loss and current_price <= stop_loss:
            if not monitor_db.has_recent_notification(stock["id"], "stop_loss", minutes=60):
                message = (
                    f"股票 {stock['symbol']} ({stock['name']}) 价格 {current_price} "
                    f"达到止损位 {stop_loss}"
                )
                monitor_db.add_notification(stock["id"], "stop_loss", message)
                notification_service.send_notifications()
            if stock.get("quant_enabled", False):
                self._execute_quant_trade(stock, "stop_loss", current_price)

    def _execute_quant_trade(self, stock: Dict, signal_type: str, current_price: float):
        try:
            if not miniqmt.is_connected():
                self.logger.warning(f"MiniQMT未连接，无法执行 {stock['symbol']} 的量化交易")
                return

            quant_config = stock.get("quant_config", {})
            if not quant_config:
                self.logger.warning(f"[{stock['symbol']}] 未配置量化参数")
                return

            signal = {
                "type": signal_type,
                "price": current_price,
                "message": f"{signal_type} signal triggered",
            }
            position_size = quant_config.get("max_position_pct", 0.2)
            success, msg = miniqmt.execute_strategy_signal(
                stock["id"],
                stock["symbol"],
                signal,
                position_size,
            )
            if success:
                monitor_db.add_notification(stock["id"], "quant_trade", f"量化交易执行: {msg}")
                notification_service.send_notifications()
            else:
                self.logger.warning(f"[{stock['symbol']}] 量化交易失败: {msg}")
        except (ValueError, TypeError, RuntimeError, OSError, ConnectionError, TimeoutError) as exc:
            self.logger.warning(f"[{stock['symbol']}] 执行量化交易失败，已降级: {exc}")
        except Exception:
            self.logger.exception("[%s] 执行量化交易出现未知异常", stock.get("symbol", "UNKNOWN"))

    def _is_trading_time(self) -> bool:
        try:
            from monitor_scheduler import get_scheduler

            scheduler = get_scheduler()
            if scheduler is None:
                return True
            return scheduler.is_trading_time()
        except ImportError as exc:
            self.logger.warning("监控调度器不可用，默认按可交易时段处理: %s", exc)
            return True
        except Exception:
            self.logger.exception("读取调度器状态失败，默认按可交易时段处理")
            return True
