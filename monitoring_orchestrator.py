import asyncio
import logging
import os
import threading
from datetime import datetime
from typing import Dict, List, Optional, Set

import config
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
    """Unified async execution engine for AI tasks and price alerts."""

    TICK_SECONDS = 5
    AI_CONCURRENCY = 2
    PRICE_ALERT_CONCURRENCY = 4
    PRICE_FETCH_TIMEOUT_SECONDS = 5
    TDX_FETCH_TIMEOUT_SECONDS = 10
    AI_TASK_TIMEOUT_SECONDS = 40
    NOTIFICATION_TIMEOUT_SECONDS = 10

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.repository: MonitoringRepository = monitor_db.repository
        self.engine = SmartMonitorEngine()
        self.AI_TASK_TIMEOUT_SECONDS = max(
            int(getattr(self.engine, "ai_decision_timeout_seconds", self.AI_TASK_TIMEOUT_SECONDS) or self.AI_TASK_TIMEOUT_SECONDS) + 5,
            int(getattr(config, "SMART_MONITOR_AI_TIMEOUT_SECONDS", self.AI_TASK_TIMEOUT_SECONDS) or self.AI_TASK_TIMEOUT_SECONDS) + 5,
        )
        self.TDX_FETCH_TIMEOUT_SECONDS = max(
            5,
            int(getattr(config, "TDX_TIMEOUT_SECONDS", self.TDX_FETCH_TIMEOUT_SECONDS) or self.TDX_FETCH_TIMEOUT_SECONDS),
        )
        self.fetcher = StockDataFetcher()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._ai_semaphore: Optional[asyncio.Semaphore] = None
        self._price_semaphore: Optional[asyncio.Semaphore] = None
        self._notify_semaphore: Optional[asyncio.Semaphore] = None
        self._background_tasks: Set[asyncio.Task] = set()
        self._inflight_item_ids: Set[int] = set()
        self._inflight_lock = threading.Lock()

        self.tdx_fetcher = None
        self.use_tdx = False
        tdx_config = getattr(config, "TDX_CONFIG", {}) or {}
        tdx_enabled = bool(tdx_config.get("enabled", False))
        tdx_base_url = str(tdx_config.get("base_url") or "").strip()
        if tdx_enabled and not tdx_base_url:
            self.logger.warning("TDX 已启用，但未配置 TDX_BASE_URL，已降级使用默认数据源")
        elif tdx_enabled and TDX_AVAILABLE:
            try:
                candidate_fetcher = SmartMonitorTDXDataFetcher(
                    base_url=tdx_base_url,
                    timeout_seconds=self.TDX_FETCH_TIMEOUT_SECONDS,
                )
                if getattr(candidate_fetcher, "available", True):
                    self.tdx_fetcher = candidate_fetcher
                    self.use_tdx = True
                    self.logger.info("TDX数据源已启用: %s", tdx_base_url)
                else:
                    self.logger.warning("TDX地址不可达，已降级使用默认数据源: %s", tdx_base_url)
            except (OSError, RuntimeError, ValueError, TypeError) as exc:
                self.logger.warning("TDX数据源初始化失败，将使用默认数据源: %s", exc)
            except Exception:
                self.logger.exception("TDX数据源初始化出现未知异常，将降级使用默认数据源")

    def has_enabled_items(self) -> bool:
        return bool(self.repository.list_items(enabled_only=True))

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, name="MonitoringOrchestrator", daemon=True)
        self.thread.start()
        self.logger.info("监测服务已启动")

    def ensure_started(self):
        if self.has_enabled_items():
            self.start()

    def stop(self):
        if not self.running:
            return
        self.running = False
        if self.loop and self._stop_event:
            try:
                self.loop.call_soon_threadsafe(self._stop_event.set)
            except RuntimeError:
                pass
        if self.thread:
            self.thread.join(timeout=5)
        self.thread = None
        self.loop = None
        self._stop_event = None
        self.logger.info("监测服务已停止")

    def ensure_stopped_if_idle(self):
        if self.running and not self.has_enabled_items():
            self.stop()

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
        self.ensure_started()
        return self._run_coroutine_sync(self._dispatch_item_async(item, force=True), timeout=self.AI_TASK_TIMEOUT_SECONDS)

    def manual_update_stock(self, stock_id: int) -> bool:
        return self.manual_update_item(stock_id)

    def run_once(self):
        self.ensure_started()
        if not self.running:
            return
        self._run_coroutine_sync(self._run_tick(), timeout=self.PRICE_FETCH_TIMEOUT_SECONDS + self.AI_TASK_TIMEOUT_SECONDS)

    def _run_coroutine_sync(self, coro, *, timeout: int) -> bool:
        if self.loop and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            try:
                return bool(future.result(timeout=timeout))
            except Exception:
                future.cancel()
                return False
        return bool(asyncio.run(coro))

    def _run_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._stop_event = asyncio.Event()
        self._ai_semaphore = asyncio.Semaphore(self.AI_CONCURRENCY)
        self._price_semaphore = asyncio.Semaphore(self.PRICE_ALERT_CONCURRENCY)
        self._notify_semaphore = asyncio.Semaphore(1)
        try:
            self.loop.run_until_complete(self._serve())
        finally:
            pending = [task for task in asyncio.all_tasks(self.loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self.loop.close()

    async def _serve(self):
        notifier_task = asyncio.create_task(self._notification_loop())
        try:
            while self.running and self._stop_event and not self._stop_event.is_set():
                await self._run_tick()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.TICK_SECONDS)
                except asyncio.TimeoutError:
                    continue
        finally:
            notifier_task.cancel()
            await asyncio.gather(notifier_task, return_exceptions=True)
            if self._background_tasks:
                await asyncio.gather(*list(self._background_tasks), return_exceptions=True)

    async def _run_tick(self):
        due_items = await asyncio.to_thread(
            self.repository.get_due_items,
            now=datetime.now(),
            service_running=self.running,
        )
        for item in due_items:
            item_id = int(item.get("id") or 0)
            if item_id <= 0:
                continue
            if self._is_item_inflight(item_id):
                continue
            self._schedule_background_task(self._dispatch_with_semaphore(item))

    def _schedule_background_task(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _is_item_inflight(self, item_id: int) -> bool:
        with self._inflight_lock:
            return item_id in self._inflight_item_ids

    def _set_item_inflight(self, item_id: int, inflight: bool) -> bool:
        with self._inflight_lock:
            if inflight:
                if item_id in self._inflight_item_ids:
                    return False
                self._inflight_item_ids.add(item_id)
                return True
            self._inflight_item_ids.discard(item_id)
            return True

    async def _dispatch_with_semaphore(self, item: Dict, *, force: bool = False) -> bool:
        item_id = int(item.get("id") or 0)
        if item_id <= 0 or not self._set_item_inflight(item_id, True):
            return False
        semaphore = self._price_semaphore if item.get("monitor_type") == "price_alert" else self._ai_semaphore
        try:
            async with semaphore:
                return await self._dispatch_item_async(item, force=force)
        finally:
            self._set_item_inflight(item_id, False)

    async def _dispatch_item_async(self, item: Dict, force: bool = False) -> bool:
        symbol = item.get("symbol", "UNKNOWN") if isinstance(item, dict) else "UNKNOWN"
        item_id = item.get("id") if isinstance(item, dict) else None
        try:
            monitor_type = item.get("monitor_type")
            if monitor_type == "price_alert":
                return await self._process_price_alert(item, force=force)
            if monitor_type == "ai_task":
                return await self._process_ai_task(item, force=force)
            self.logger.warning("未知监控类型: %s", monitor_type)
            return False
        except (KeyError, TypeError, ValueError) as exc:
            self.logger.warning("[%s] 监控项数据异常，已降级失败: %s", symbol, exc)
            if item_id is not None:
                await asyncio.to_thread(
                    self.repository.update_runtime,
                    item_id,
                    last_status="failed",
                    last_message=f"invalid_item:{exc}",
                )
            return False
        except Exception as exc:
            self.logger.exception("[%s] 处理监控项出现未知异常", symbol)
            if item_id is not None:
                await asyncio.to_thread(
                    self.repository.update_runtime,
                    item_id,
                    last_status="failed",
                    last_message=str(exc),
                )
            return False

    async def _process_ai_task(self, item: Dict, force: bool = False) -> bool:
        try:
            result = await self._await_to_thread(
                self.engine.analyze_stock,
                self.AI_TASK_TIMEOUT_SECONDS,
                stock_code=item["symbol"],
                notify=bool(item.get("notification_enabled", True)),
                trading_hours_only=bool(item.get("trading_hours_only", True)) and not force,
                account_name=item.get("account_name"),
                asset_id=item.get("asset_id"),
                portfolio_stock_id=item.get("portfolio_stock_id"),
            )
        except TimeoutError:
            await asyncio.to_thread(
                self.repository.update_runtime,
                item["id"],
                last_status="timeout",
                last_message="AI分析超时",
            )
            return False

        if result.get("success"):
            action = result["decision"]["action"].upper()
            message = f"AI决策: {action}"
            await asyncio.to_thread(
                self.repository.update_runtime,
                item["id"],
                last_status=action.lower(),
                last_message=message,
            )
            if result.get("decision_changed", True):
                await asyncio.to_thread(
                    self.repository.record_event,
                    item_id=item["id"],
                    event_type="ai_analysis",
                    message=message,
                    notification_pending=False,
                    sent=True,
                )
            return True

        if result.get("skipped"):
            await asyncio.to_thread(
                self.repository.update_runtime,
                item["id"],
                last_status="skipped",
                last_message=result.get("error", "交易时段外跳过"),
            )
            return False

        await asyncio.to_thread(
            self.repository.update_runtime,
            item["id"],
            last_status="error",
            last_message=result.get("error", "AI分析失败"),
        )
        return False

    async def _process_price_alert(self, item: Dict, force: bool = False) -> bool:
        stock = await asyncio.to_thread(monitor_db.get_stock_by_id, item["id"])
        if not stock:
            return False

        if not force and stock.get("trading_hours_only") and not self._is_trading_time():
            await asyncio.to_thread(
                self.repository.update_item,
                item["id"],
                {
                    "last_status": "waiting_trading_hours",
                    "last_message": "非交易时段，等待执行",
                },
            )
            return False

        current_price = await self._get_latest_price(item["symbol"])
        if current_price and current_price > 0:
            await asyncio.to_thread(monitor_db.update_stock_price, stock["id"], current_price)
            await asyncio.to_thread(self._check_trigger_conditions, stock, current_price)
            return True

        await asyncio.to_thread(monitor_db.update_last_checked, stock["id"])
        return False

    async def _await_to_thread(self, func, timeout_seconds: int, *args, **kwargs):
        async with asyncio.timeout(timeout_seconds):
            return await asyncio.to_thread(func, *args, **kwargs)

    def _is_a_stock(self, symbol: str) -> bool:
        return symbol.isdigit() and len(symbol) == 6

    async def _get_latest_price(self, symbol: str) -> Optional[float]:
        if self.use_tdx and self._is_a_stock(symbol):
            try:
                quote = await self._await_to_thread(
                    self.tdx_fetcher.get_realtime_quote,
                    self.TDX_FETCH_TIMEOUT_SECONDS,
                    symbol,
                )
                if quote and quote.get("current_price"):
                    return float(quote["current_price"])
            except TimeoutError:
                self.logger.warning("[%s] TDX获取超时，降级默认数据源", symbol)
            except (ValueError, TypeError, RuntimeError, OSError, ConnectionError) as exc:
                self.logger.warning("[%s] TDX获取失败，降级默认数据源: %s", symbol, exc)
            except Exception:
                self.logger.exception("[%s] TDX获取出现未知异常，降级默认数据源", symbol)

        try:
            stock_info = await self._await_to_thread(
                self.fetcher.get_stock_info,
                self.PRICE_FETCH_TIMEOUT_SECONDS,
                symbol,
                max_age_seconds=30,
                allow_stale_on_failure=True,
                cache_first=True,
            )
            current_price = stock_info.get("current_price")
            if current_price and current_price != "N/A":
                return float(current_price)
        except TimeoutError:
            self.logger.warning("[%s] 默认数据源获取超时", symbol)
        except (ValueError, TypeError, RuntimeError, OSError, ConnectionError) as exc:
            self.logger.warning("[%s] 默认数据源获取失败: %s", symbol, exc)
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

        if take_profit and current_price >= take_profit:
            if not monitor_db.has_recent_notification(stock["id"], "take_profit", minutes=60):
                message = (
                    f"股票 {stock['symbol']} ({stock['name']}) 价格 {current_price} "
                    f"达到止盈位 {take_profit}"
                )
                monitor_db.add_notification(stock["id"], "take_profit", message)

        if stop_loss and current_price <= stop_loss:
            if not monitor_db.has_recent_notification(stock["id"], "stop_loss", minutes=60):
                message = (
                    f"股票 {stock['symbol']} ({stock['name']}) 价格 {current_price} "
                    f"达到止损位 {stop_loss}"
                )
                monitor_db.add_notification(stock["id"], "stop_loss", message)

    async def _notification_loop(self):
        while self.running and self._stop_event and not self._stop_event.is_set():
            try:
                pending_notifications = await asyncio.to_thread(self.repository.get_pending_notifications)
                if pending_notifications:
                    async with self._notify_semaphore:
                        await self._await_to_thread(
                            notification_service.send_notifications,
                            self.NOTIFICATION_TIMEOUT_SECONDS,
                        )
            except TimeoutError:
                self.logger.warning("通知发送超时，下一轮继续重试")
            except Exception:
                self.logger.exception("通知发送任务异常")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.TICK_SECONDS)
            except asyncio.TimeoutError:
                continue

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
