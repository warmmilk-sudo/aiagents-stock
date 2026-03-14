"""Background task helpers for selector/strategy domains (UI-independent)."""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from portfolio_analysis_tasks import portfolio_analysis_task_manager


TaskRunner = Callable[[str, Callable[..., None]], Dict[str, Any]]


def build_ui_analysis_session_id(task_type: str) -> str:
    return f"ui-analysis-{task_type}"


def get_active_ui_analysis_task(task_type: str) -> Optional[Dict[str, Any]]:
    session_id = build_ui_analysis_session_id(task_type)
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_active_task(session_id)


def get_latest_ui_analysis_task(task_type: str) -> Optional[Dict[str, Any]]:
    session_id = build_ui_analysis_session_id(task_type)
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_latest_task(session_id)


def enqueue_ui_analysis_task(
    *,
    task_type: str,
    label: str,
    runner: TaskRunner,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    session_id = build_ui_analysis_session_id(task_type)
    return portfolio_analysis_task_manager.start_task(
        session_id,
        task_type=task_type,
        label=label,
        runner=runner,
        metadata=metadata or {},
    )


def start_ui_analysis_task(
    *,
    task_type: str,
    label: str,
    runner: TaskRunner,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    active_task = get_active_ui_analysis_task(task_type)
    if active_task:
        status_text = "正在分析" if active_task.get("status") == "running" else "已在队列中"
        raise RuntimeError(f"{label}{status_text}，请勿重复提交。")
    return enqueue_ui_analysis_task(
        task_type=task_type,
        label=label,
        runner=runner,
        metadata=metadata,
    )
