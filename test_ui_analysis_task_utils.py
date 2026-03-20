import unittest
from unittest.mock import patch

import ui_analysis_task_utils as task_utils


class UiAnalysisTaskUtilsTests(unittest.TestCase):
    def test_start_ui_analysis_task_enqueues_even_when_active_task_exists(self):
        runner = lambda _task_id, report_progress: {}

        with patch.object(
            task_utils,
            "get_active_ui_analysis_task",
            return_value={"id": "running-task", "status": "running"},
        ) as get_active, patch.object(task_utils, "enqueue_ui_analysis_task", return_value="task-1") as enqueue:
            task_id = task_utils.start_ui_analysis_task(
                task_type="sector_strategy_analysis",
                label="板块策略分析",
                runner=runner,
                metadata={"scope": "test"},
            )

        self.assertEqual(task_id, "task-1")
        get_active.assert_not_called()
        enqueue.assert_called_once_with(
            task_type="sector_strategy_analysis",
            label="板块策略分析",
            runner=runner,
            metadata={"scope": "test"},
        )


if __name__ == "__main__":
    unittest.main()
