#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Real-time monitoring scheduler.

Supports:
- auto start/stop around trading sessions
- pre-market start offset
- post-market stop offset
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional

import schedule


logger = logging.getLogger(__name__)


class TradingTimeScheduler:
    """Trading-time based scheduler for monitoring service."""

    def __init__(self, monitor_service):
        self.monitor_service = monitor_service
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        config_file = "monitor_schedule_config.json"
        default_config = {
            "enabled": False,
            "market": "CN",
            "trading_hours": {
                "CN": [
                    {"start": "09:30", "end": "11:30"},
                    {"start": "13:00", "end": "15:00"},
                ],
                "US": [
                    {"start": "21:30", "end": "04:00"},
                ],
                "HK": [
                    {"start": "09:30", "end": "12:00"},
                    {"start": "13:00", "end": "16:00"},
                ],
            },
            "trading_days": [1, 2, 3, 4, 5],
            "auto_stop": True,
            "pre_market_minutes": 5,
            "post_market_minutes": 5,
        }

        if os.path.exists(config_file):
            try:
                with open(config_file, "r", encoding="utf-8") as file:
                    loaded_config = json.load(file)
                    default_config.update(loaded_config)
            except Exception as exc:
                logger.warning("Failed to load monitor scheduler config, using defaults: %s", exc)
        return default_config

    def _save_config(self):
        config_file = "monitor_schedule_config.json"
        try:
            with open(config_file, "w", encoding="utf-8") as file:
                json.dump(self.config, file, indent=2, ensure_ascii=False)
            logger.info("Scheduler config saved")
        except Exception as exc:
            logger.error("Failed to save scheduler config: %s", exc)

    def update_config(self, **kwargs):
        self.config.update(kwargs)
        self._save_config()

    @staticmethod
    def _parse_time(value: str) -> dtime:
        return datetime.strptime(value, "%H:%M").time()

    @staticmethod
    def _shift_hhmm(value: str, minutes: int) -> str:
        base = datetime.combine(date(2000, 1, 1), TradingTimeScheduler._parse_time(value))
        shifted = base + timedelta(minutes=minutes)
        return shifted.strftime("%H:%M")

    @staticmethod
    def _is_in_period(current: dtime, start: dtime, end: dtime) -> bool:
        if start <= end:
            return start <= current <= end
        return current >= start or current <= end

    def _is_trading_date(self, target_date: date) -> bool:
        weekday = target_date.weekday() + 1  # 1=Monday
        return weekday in self.config.get("trading_days", [1, 2, 3, 4, 5])

    def is_trading_day(self, now: Optional[datetime] = None) -> bool:
        current = now or datetime.now()
        return self._is_trading_date(current.date())

    def _effective_periods(self) -> List[Dict[str, str]]:
        market = self.config.get("market", "CN")
        periods = self.config.get("trading_hours", {}).get(market, [])
        pre_minutes = int(self.config.get("pre_market_minutes", 5) or 0)
        post_minutes = int(self.config.get("post_market_minutes", 5) or 0)

        effective: List[Dict[str, str]] = []
        for period in periods:
            start = (period.get("start") or "").strip()
            end = (period.get("end") or "").strip()
            if not start or not end:
                continue
            try:
                effective_start = self._shift_hhmm(start, -pre_minutes)
                effective_end = self._shift_hhmm(end, post_minutes)
            except ValueError:
                continue
            effective.append(
                {
                    "start": effective_start,
                    "end": effective_end,
                    "raw_start": start,
                    "raw_end": end,
                }
            )
        return effective

    def is_trading_time(self, now: Optional[datetime] = None) -> bool:
        current = now or datetime.now()
        current_time = current.time()
        periods = self._effective_periods()
        if not periods:
            return False

        for period in periods:
            start = self._parse_time(period["start"])
            end = self._parse_time(period["end"])
            if start <= end:
                if self._is_trading_date(current.date()) and self._is_in_period(current_time, start, end):
                    return True
                continue

            # Cross-day session (for example 21:30-04:00)
            if current_time >= start and self._is_trading_date(current.date()):
                return True
            if current_time <= end and self._is_trading_date(current.date() - timedelta(days=1)):
                return True

        return False

    def get_next_trading_time(self, now: Optional[datetime] = None) -> str:
        current = now or datetime.now()
        if self.is_trading_time(current):
            return "交易时段内"

        periods = self._effective_periods()
        if not periods:
            return "未配置交易时段"

        for offset in range(0, 8):
            day = current.date() + timedelta(days=offset)
            if not self._is_trading_date(day):
                continue
            for period in periods:
                start = self._parse_time(period["start"])
                candidate = datetime.combine(day, start)
                if candidate > current:
                    return candidate.strftime("%Y-%m-%d %H:%M")
        return "交易时段已结束"

    def start_scheduler(self):
        if self.running:
            logger.info("Scheduler already running")
            return
        if not self.config.get("enabled", False):
            logger.info("Scheduler not enabled")
            return

        self.running = True
        self.thread = threading.Thread(target=self._schedule_loop, daemon=True)
        self.thread.start()
        logger.info("Scheduler started")

    def stop_scheduler(self):
        self.running = False
        schedule.clear("monitor_scheduler")
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Scheduler stopped")

    def _schedule_loop(self):
        schedule.clear("monitor_scheduler")
        periods = self._effective_periods()

        for period in periods:
            start_time = period["start"]
            end_time = period["end"]
            schedule.every().day.at(start_time).do(self._auto_start_monitoring).tag("monitor_scheduler")
            logger.info(
                "Registered start job %s (raw %s)",
                start_time,
                period["raw_start"],
            )
            if self.config.get("auto_stop", True):
                schedule.every().day.at(end_time).do(self._auto_stop_monitoring).tag("monitor_scheduler")
                logger.info(
                    "Registered stop job %s (raw %s)",
                    end_time,
                    period["raw_end"],
                )

        while self.running:
            try:
                schedule.run_pending()

                in_trading = self.is_trading_time()
                if in_trading and not self.monitor_service.running:
                    logger.info("Detected trading window, auto starting monitoring service")
                    self.monitor_service.start_monitoring()

                if not in_trading and self.monitor_service.running and self.config.get("auto_stop", True):
                    logger.info("Detected non-trading window, auto stopping monitoring service")
                    self.monitor_service.stop_monitoring()
            except Exception:
                logger.exception("Scheduler loop error")
            time.sleep(60)

    def _auto_start_monitoring(self):
        if self.is_trading_day():
            logger.info("Scheduled start trigger at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            if not self.monitor_service.running:
                self.monitor_service.start_monitoring()
        else:
            logger.info("Skip scheduled start on non-trading day")

    def _auto_stop_monitoring(self):
        logger.info("Scheduled stop trigger at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if self.monitor_service.running:
            self.monitor_service.stop_monitoring()

    def get_status(self) -> Dict:
        return {
            "scheduler_running": self.running,
            "scheduler_enabled": self.config.get("enabled", False),
            "is_trading_day": self.is_trading_day(),
            "is_trading_time": self.is_trading_time(),
            "market": self.config.get("market", "CN"),
            "next_trading_time": self.get_next_trading_time(),
            "monitor_service_running": self.monitor_service.running,
            "auto_stop": self.config.get("auto_stop", True),
        }


_scheduler_instance = None


def get_scheduler(monitor_service=None):
    global _scheduler_instance
    if _scheduler_instance is None and monitor_service is not None:
        _scheduler_instance = TradingTimeScheduler(monitor_service)
    return _scheduler_instance
