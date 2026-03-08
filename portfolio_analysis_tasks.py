"""Background queue registry for portfolio analysis workflows."""

from __future__ import annotations

import threading
import time
import traceback
import uuid
from typing import Any, Callable, Dict, Optional


TaskRunner = Callable[[str, Callable[..., None]], Dict[str, Any]]


class PortfolioAnalysisTaskManager:
    """Keep portfolio analysis tasks alive across Streamlit reruns."""

    PENDING_STATUSES = {"queued", "running"}

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._task_runners: Dict[str, TaskRunner] = {}
        self._session_workers: Dict[str, threading.Thread] = {}

    def _list_session_tasks_locked(
        self,
        session_id: str,
        *,
        task_type: Optional[str] = None,
        statuses: Optional[set[str]] = None,
        newest_first: bool = True,
    ) -> list[Dict[str, Any]]:
        tasks = [
            dict(task)
            for task in self._tasks.values()
            if task.get("session_id") == session_id
            and (task_type is None or task.get("task_type") == task_type)
            and (statuses is None or task.get("status") in statuses)
        ]
        tasks.sort(key=lambda item: item.get("created_at", 0.0), reverse=newest_first)
        return tasks

    def _list_tasks_locked(
        self,
        *,
        task_type: Optional[str] = None,
        statuses: Optional[set[str]] = None,
        newest_first: bool = True,
    ) -> list[Dict[str, Any]]:
        tasks = [
            dict(task)
            for task in self._tasks.values()
            if (task_type is None or task.get("task_type") == task_type)
            and (statuses is None or task.get("status") in statuses)
        ]
        tasks.sort(key=lambda item: item.get("created_at", 0.0), reverse=newest_first)
        return tasks

    def _update_task_locked(self, task_id: str, **updates: Any) -> None:
        task = self._tasks.get(task_id)
        if not task:
            return
        task.update(updates)
        task["updated_at"] = time.time()

    def _ensure_session_worker_locked(self, session_id: str) -> Optional[threading.Thread]:
        worker = self._session_workers.get(session_id)
        if worker is not None and worker.is_alive():
            return None

        worker = threading.Thread(
            target=self._run_session_queue,
            args=(session_id,),
            daemon=True,
            name=f"portfolio-analysis-queue-{session_id[:8]}",
        )
        self._session_workers[session_id] = worker
        return worker

    def _pop_next_queued_task_locked(self, session_id: str) -> Optional[Dict[str, Any]]:
        queued_tasks = self._list_session_tasks_locked(
            session_id,
            statuses={"queued"},
            newest_first=False,
        )
        if not queued_tasks:
            return None

        next_task = queued_tasks[0]
        self._update_task_locked(
            next_task["id"],
            status="running",
            started_at=time.time(),
        )
        return dict(self._tasks[next_task["id"]])

    def start_task(
        self,
        session_id: str,
        *,
        task_type: str,
        label: str,
        runner: TaskRunner,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        with self._lock:
            task_id = uuid.uuid4().hex
            now = time.time()
            self._tasks[task_id] = {
                "id": task_id,
                "session_id": session_id,
                "task_type": task_type,
                "label": label,
                "status": "queued",
                "message": "等待前序任务完成后开始执行",
                "current": 0,
                "total": 0,
                "progress": 0.0,
                "step_code": "",
                "step_status": "",
                "result": None,
                "error": "",
                "traceback": "",
                "metadata": dict(metadata or {}),
                "created_at": now,
                "updated_at": now,
                "started_at": None,
                "finished_at": None,
            }
            self._task_runners[task_id] = runner
            worker = self._ensure_session_worker_locked(session_id)

        if worker is not None:
            worker.start()
        return task_id

    def _run_session_queue(self, session_id: str) -> None:
        while True:
            with self._lock:
                task = self._pop_next_queued_task_locked(session_id)
                if task is None:
                    worker = self._session_workers.get(session_id)
                    if worker is threading.current_thread():
                        self._session_workers.pop(session_id, None)
                    return
                runner = self._task_runners.get(task["id"])

            if runner is None:
                self.update_task(
                    task["id"],
                    status="failed",
                    error="任务执行器丢失",
                    finished_at=time.time(),
                )
                continue

            self._run_task(task["id"], runner)

    def _run_task(self, task_id: str, runner: TaskRunner) -> None:
        def report_progress(**updates: Any) -> None:
            current = updates.get("current")
            total = updates.get("total")
            if isinstance(current, (int, float)) and isinstance(total, (int, float)) and total:
                updates["progress"] = max(0.0, min(1.0, float(current) / float(total)))
            self.update_task(task_id, **updates)

        try:
            result = runner(task_id, report_progress)
        except Exception as exc:  # pragma: no cover - exercised by tests via public state
            self.update_task(
                task_id,
                status="failed",
                error=str(exc),
                traceback=traceback.format_exc(),
                finished_at=time.time(),
            )
            with self._lock:
                self._task_runners.pop(task_id, None)
            return

        final_updates = {
            "status": "success",
            "result": result,
            "finished_at": time.time(),
        }
        task_snapshot = self.get_task(task_id) or {}
        total = task_snapshot.get("total") or 0
        current = task_snapshot.get("current") or 0
        if total and current < total:
            final_updates["current"] = total
            final_updates["progress"] = 1.0
        elif total:
            final_updates["progress"] = 1.0
        self.update_task(task_id, **final_updates)
        with self._lock:
            self._task_runners.pop(task_id, None)

    def update_task(self, task_id: str, **updates: Any) -> None:
        with self._lock:
            self._update_task_locked(task_id, **updates)

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            task = self._tasks.get(task_id)
            return dict(task) if task else None

    def get_running_task(self, session_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            tasks = self._list_session_tasks_locked(
                session_id,
                statuses={"running"},
            )
            return tasks[0] if tasks else None

    def get_running_task_any(self, *, task_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        with self._lock:
            tasks = self._list_tasks_locked(task_type=task_type, statuses={"running"})
            return tasks[0] if tasks else None

    def get_pending_tasks(
        self,
        session_id: str,
        *,
        task_type: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        with self._lock:
            return self._list_session_tasks_locked(
                session_id,
                task_type=task_type,
                statuses=self.PENDING_STATUSES,
                newest_first=False,
            )

    def get_pending_tasks_any(
        self,
        *,
        task_type: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        with self._lock:
            return self._list_tasks_locked(
                task_type=task_type,
                statuses=self.PENDING_STATUSES,
                newest_first=False,
            )

    def get_active_task(self, session_id: str) -> Optional[Dict[str, Any]]:
        running = self.get_running_task(session_id)
        if running:
            return running
        pending = self.get_pending_tasks(session_id)
        return pending[0] if pending else None

    def get_active_task_any(self, *, task_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        running = self.get_running_task_any(task_type=task_type)
        if running:
            return running
        pending = self.get_pending_tasks_any(task_type=task_type)
        return pending[0] if pending else None

    def has_active_task(self, session_id: str) -> bool:
        return bool(self.get_pending_tasks(session_id))

    def get_latest_task(
        self,
        session_id: str,
        *,
        task_type: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            tasks = self._list_session_tasks_locked(session_id, task_type=task_type)
            return tasks[0] if tasks else None

    def get_latest_task_any(self, *, task_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        with self._lock:
            tasks = self._list_tasks_locked(task_type=task_type)
            return tasks[0] if tasks else None

    def count_queued_tasks(
        self,
        session_id: str,
        *,
        task_type: Optional[str] = None,
    ) -> int:
        with self._lock:
            return len(
                self._list_session_tasks_locked(
                    session_id,
                    task_type=task_type,
                    statuses={"queued"},
                    newest_first=False,
                )
            )

    def prune_session_tasks(self, session_id: str, *, keep: int = 8) -> None:
        with self._lock:
            tasks = self._list_session_tasks_locked(session_id)
            for task in tasks[keep:]:
                self._tasks.pop(task["id"], None)
                self._task_runners.pop(task["id"], None)


portfolio_analysis_task_manager = PortfolioAnalysisTaskManager()
