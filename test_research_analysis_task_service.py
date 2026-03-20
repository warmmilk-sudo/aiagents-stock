import unittest
import sys
import types
from unittest.mock import MagicMock, patch

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)
sys.modules.setdefault("yfinance", types.SimpleNamespace())
sys.modules.setdefault("akshare", types.SimpleNamespace())
sys.modules.setdefault("ta", types.SimpleNamespace())
sys.modules.setdefault("pywencai", types.SimpleNamespace())
sys.modules.setdefault("pandas", types.SimpleNamespace())
sys.modules.setdefault("numpy", types.SimpleNamespace())
batch_analysis_stub = types.ModuleType("batch_analysis_service")
batch_analysis_stub.analyze_single_stock_for_batch = lambda *args, **kwargs: {}
sys.modules.setdefault("batch_analysis_service", batch_analysis_stub)

stub_specs = {
    "main_force_pdf_generator": {
        "MainForcePDFGenerator": type("MainForcePDFGenerator", (), {}),
        "generate_main_force_markdown_report": lambda *args, **kwargs: "",
    },
    "main_force_analysis": {
        "MainForceAnalyzer": type("MainForceAnalyzer", (), {}),
    },
    "sector_strategy_pdf": {
        "SectorStrategyPDFGenerator": type("SectorStrategyPDFGenerator", (), {}),
    },
    "sector_strategy_data": {
        "SectorStrategyDataFetcher": type("SectorStrategyDataFetcher", (), {}),
    },
    "sector_strategy_db": {
        "SectorStrategyDatabase": type("SectorStrategyDatabase", (), {}),
    },
    "sector_strategy_engine": {
        "SectorStrategyEngine": type("SectorStrategyEngine", (), {}),
    },
    "sector_strategy_scheduler": {
        "sector_strategy_scheduler": types.SimpleNamespace(),
    },
    "strategy_markdown_reports": {
        "generate_longhubang_markdown_report": lambda *args, **kwargs: "",
        "generate_sector_markdown_report": lambda *args, **kwargs: "",
    },
    "stock_data": {
        "StockDataFetcher": type("StockDataFetcher", (), {}),
    },
    "portfolio_manager": {
        "portfolio_manager": types.SimpleNamespace(
            get_stock_count=lambda *args, **kwargs: 0,
            batch_analyze_portfolio=lambda *args, **kwargs: {},
            persist_single_analysis_result=lambda *args, **kwargs: {},
        ),
    },
    "portfolio_scheduler": {
        "portfolio_scheduler": types.SimpleNamespace(),
    },
    "low_price_bull_monitor": {
        "low_price_bull_monitor": types.SimpleNamespace(),
    },
    "low_price_bull_selector": {
        "LowPriceBullSelector": type("LowPriceBullSelector", (), {}),
    },
    "low_price_bull_service": {
        "low_price_bull_service": types.SimpleNamespace(),
    },
    "low_price_bull_strategy": {
        "LowPriceBullStrategy": type("LowPriceBullStrategy", (), {}),
    },
    "monitor_service": {
        "monitor_service": types.SimpleNamespace(
            get_scheduler=lambda: None,
            get_status=lambda: {},
            get_recent_events=lambda *args, **kwargs: [],
            get_registry_items=lambda *args, **kwargs: [],
            ensure_scheduler_state=lambda: None,
        )
    },
    "notification_service": {
        "notification_service": types.SimpleNamespace(),
    },
}
for module_name, attributes in stub_specs.items():
    module = types.ModuleType(module_name)
    for attr_name, attr_value in attributes.items():
        setattr(module, attr_name, attr_value)
    sys.modules.setdefault(module_name, module)

from backend import services


class ResearchAnalysisTaskServiceTests(unittest.TestCase):
    def test_submit_research_analysis_task_allows_queue_when_task_exists(self):
        captured: dict = {}

        def fake_start_task(session_key, **kwargs):
            captured["session_key"] = session_key
            captured.update(kwargs)
            return "task-queued"

        with patch.object(services.config, "DEEPSEEK_API_KEY", "test-key"), \
             patch.object(
                 services.portfolio_analysis_task_manager,
                 "get_active_task",
                 return_value={"id": "running-task", "status": "running"},
             ), \
             patch.object(services.portfolio_analysis_task_manager, "start_task", side_effect=fake_start_task):
            task_id = services.submit_research_analysis_task(
                session_key="session-queue",
                symbols=["600519"],
                period="1y",
                batch_mode="顺序分析",
                max_workers=1,
                analysts={"technical": True, "fundamental": False, "fund_flow": False, "risk": False, "sentiment": False, "news": False},
                lightweight_model=None,
                reasoning_model=None,
            )

        self.assertEqual(task_id, "task-queued")
        self.assertEqual(captured["session_key"], "session-queue")
        self.assertEqual(captured["label"], "深度分析 600519")

    def test_submit_research_analysis_task_records_clamped_worker_metadata(self):
        captured: dict = {}

        def fake_start_task(session_key, **kwargs):
            captured.update(kwargs)
            return "task-123"

        with patch.object(services.config, "DEEPSEEK_API_KEY", "test-key"), \
             patch.object(services.portfolio_analysis_task_manager, "get_active_task", return_value=None), \
             patch.object(services.portfolio_analysis_task_manager, "start_task", side_effect=fake_start_task):
            task_id = services.submit_research_analysis_task(
                session_key="session-a",
                symbols=["600519", "000001"],
                period="1y",
                batch_mode="多线程并行",
                max_workers=99,
                analysts={"technical": True, "fundamental": False, "fund_flow": False, "risk": False, "sentiment": False, "news": False},
                lightweight_model=None,
                reasoning_model=None,
            )

        self.assertEqual(task_id, "task-123")
        self.assertEqual(captured["metadata"]["max_workers"], 5)
        self.assertEqual(captured["metadata"]["batch_mode"], "多线程并行")

    def test_parallel_batch_runner_uses_clamped_worker_count(self):
        captured: dict = {}

        def fake_start_task(session_key, **kwargs):
            captured.update(kwargs)
            return "task-456"

        with patch.object(services.config, "DEEPSEEK_API_KEY", "test-key"), \
             patch.object(services.portfolio_analysis_task_manager, "get_active_task", return_value=None), \
             patch.object(services.portfolio_analysis_task_manager, "start_task", side_effect=fake_start_task):
            services.submit_research_analysis_task(
                session_key="session-b",
                symbols=["600519", "000001"],
                period="1y",
                batch_mode="多线程并行",
                max_workers=0,
                analysts={"technical": True, "fundamental": False, "fund_flow": False, "risk": False, "sentiment": False, "news": False},
                lightweight_model=None,
                reasoning_model=None,
            )

        runner = captured["runner"]
        executor_instance = MagicMock()
        futures = []

        def fake_submit(fn, symbol):
            future = MagicMock()
            future.result.return_value = {
                "symbol": symbol,
                "success": True,
                "saved_to_db": True,
            }
            futures.append(future)
            return future

        executor_instance.submit.side_effect = fake_submit

        with patch.object(services, "analyze_single_stock_for_batch", return_value={"success": True, "saved_to_db": True}), \
             patch("backend.services.concurrent.futures.ThreadPoolExecutor") as executor_cls, \
             patch("backend.services.concurrent.futures.as_completed", side_effect=lambda items: list(items)):
            executor_cls.return_value.__enter__.return_value = executor_instance
            result = runner("task-456", lambda **kwargs: None)

        self.assertEqual(executor_cls.call_args.kwargs["max_workers"], 1)
        self.assertEqual(result["max_workers"], 1)

    def test_sequential_batch_runner_ignores_parallel_worker_setting(self):
        captured: dict = {}

        def fake_start_task(session_key, **kwargs):
            captured.update(kwargs)
            return "task-789"

        with patch.object(services.config, "DEEPSEEK_API_KEY", "test-key"), \
             patch.object(services.portfolio_analysis_task_manager, "get_active_task", return_value=None), \
             patch.object(services.portfolio_analysis_task_manager, "start_task", side_effect=fake_start_task):
            services.submit_research_analysis_task(
                session_key="session-c",
                symbols=["600519", "000001"],
                period="1y",
                batch_mode="顺序分析",
                max_workers=5,
                analysts={"technical": True, "fundamental": False, "fund_flow": False, "risk": False, "sentiment": False, "news": False},
                lightweight_model=None,
                reasoning_model=None,
            )

        runner = captured["runner"]
        with patch.object(
            services,
            "analyze_single_stock_for_batch",
            return_value={"success": True, "saved_to_db": True},
        ), patch("backend.services.concurrent.futures.ThreadPoolExecutor") as executor_cls:
            result = runner("task-789", lambda **kwargs: None)

        executor_cls.assert_not_called()
        self.assertEqual(result["max_workers"], 1)

    def test_submit_portfolio_analysis_task_allows_queue_when_task_exists(self):
        captured: dict = {}

        def fake_start_task(session_key, **kwargs):
            captured["session_key"] = session_key
            captured.update(kwargs)
            return "portfolio-task-queued"

        with patch.object(services.config, "DEEPSEEK_API_KEY", "test-key"), \
             patch.object(
                 services.portfolio_analysis_task_manager,
                 "get_active_task",
                 return_value={"id": "running-task", "status": "running"},
             ), \
             patch.object(services.portfolio_manager, "get_stock_count", return_value=3), \
             patch.object(services.portfolio_analysis_task_manager, "start_task", side_effect=fake_start_task):
            task_id = services.submit_portfolio_analysis_task(
                session_key="session-portfolio",
                account_name=None,
                period="1y",
                batch_mode="顺序分析",
                max_workers=1,
                analysts={"technical": True, "fundamental": False, "fund_flow": False, "risk": False, "sentiment": False, "news": False},
            )

        self.assertEqual(task_id, "portfolio-task-queued")
        self.assertEqual(captured["session_key"], "session-portfolio")
        self.assertEqual(captured["label"], "全部账户持仓批量分析")


if __name__ == "__main__":
    unittest.main()
