import unittest
from unittest.mock import MagicMock, patch

from backend import services


class ResearchAnalysisTaskServiceTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
