from monitoring_orchestrator import MonitoringOrchestrator


class StockMonitorService:
    """Compatibility wrapper around the unified monitoring orchestrator."""

    def __init__(self):
        self.orchestrator = MonitoringOrchestrator()

    @property
    def running(self):
        return self.orchestrator.running

    @property
    def thread(self):
        return self.orchestrator.thread

    def start_monitoring(self):
        self.orchestrator.start()

    def stop_monitoring(self):
        self.orchestrator.stop()

    def manual_update_stock(self, stock_id: int):
        return self.orchestrator.manual_update_stock(stock_id)

    def get_stocks_needing_update(self):
        return self.orchestrator.get_stocks_needing_update()

    def get_scheduler(self):
        from monitor_scheduler import get_scheduler

        return get_scheduler(self)

    def get_status(self):
        return self.orchestrator.get_status()

    def get_registry_items(self, *args, **kwargs):
        return self.orchestrator.get_registry_items(*args, **kwargs)

    def get_recent_events(self, limit: int = 50):
        return self.orchestrator.get_recent_events(limit=limit)


monitor_service = StockMonitorService()
