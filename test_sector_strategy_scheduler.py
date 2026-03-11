import unittest
import sys
import types
from unittest.mock import patch

sys.modules.setdefault("streamlit", types.SimpleNamespace())

try:
    from sector_strategy_scheduler import (
        SECTOR_STRATEGY_TASK_TYPE,
        SectorStrategyScheduler,
    )
except ModuleNotFoundError:
    SECTOR_STRATEGY_TASK_TYPE = None
    SectorStrategyScheduler = None


@unittest.skipIf(SectorStrategyScheduler is None, "sector strategy scheduler dependencies unavailable")
class SectorStrategySchedulerQueueTests(unittest.TestCase):
    def test_manual_run_enqueues_background_task(self):
        scheduler = SectorStrategyScheduler()

        with patch("sector_strategy_scheduler.enqueue_ui_analysis_task", return_value="task-1") as enqueue:
            self.assertTrue(scheduler.manual_run())

        enqueue.assert_called_once()
        self.assertEqual(enqueue.call_args.kwargs["task_type"], SECTOR_STRATEGY_TASK_TYPE)
        self.assertEqual(enqueue.call_args.kwargs["label"], "手动智策分析")
        self.assertEqual(enqueue.call_args.kwargs["metadata"], {"trigger": "manual"})

    def test_scheduled_trigger_uses_same_background_queue(self):
        scheduler = SectorStrategyScheduler()

        with patch("sector_strategy_scheduler.enqueue_ui_analysis_task", return_value="task-2") as enqueue:
            scheduler._run_analysis_safe()

        enqueue.assert_called_once()
        self.assertEqual(enqueue.call_args.kwargs["task_type"], SECTOR_STRATEGY_TASK_TYPE)
        self.assertEqual(enqueue.call_args.kwargs["label"], "定时智策分析")
        self.assertEqual(enqueue.call_args.kwargs["metadata"], {"trigger": "scheduled"})


if __name__ == "__main__":
    unittest.main()
