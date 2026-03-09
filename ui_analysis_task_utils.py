"""Shared background task helpers for selector/strategy analysis pages."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Optional

import streamlit as st

from portfolio_analysis_tasks import portfolio_analysis_task_manager


TaskRunner = Callable[[str, Callable[..., None]], Dict[str, Any]]


def _build_session_id(task_type: str) -> str:
    return f"ui-analysis-{task_type}"


def _build_refresh_signature(task: Optional[Dict[str, Any]]) -> str:
    if not task:
        return ""
    return "|".join(
        [
            str(task.get("id") or ""),
            str(task.get("label") or ""),
            str(task.get("status") or ""),
            str(task.get("current") or 0),
            str(task.get("total") or 0),
            str(task.get("progress") or 0.0),
            str(task.get("started_at") or ""),
        ]
    )


def _format_task_time(timestamp: Any) -> str:
    if not timestamp:
        return ""
    try:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return ""


def _format_progress_text(current: int, total: int) -> str:
    if total > 0:
        return f"已完成（{current}/{total}）"
    return f"已完成（{current}）"


def get_active_ui_analysis_task(task_type: str) -> Optional[Dict[str, Any]]:
    session_id = _build_session_id(task_type)
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_active_task(session_id)


def get_latest_ui_analysis_task(task_type: str) -> Optional[Dict[str, Any]]:
    session_id = _build_session_id(task_type)
    portfolio_analysis_task_manager.prune_session_tasks(session_id)
    return portfolio_analysis_task_manager.get_latest_task(session_id)


def start_ui_analysis_task(
    *,
    task_type: str,
    label: str,
    runner: TaskRunner,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    session_id = _build_session_id(task_type)
    active_task = get_active_ui_analysis_task(task_type)
    if active_task:
        status_text = "正在分析" if active_task.get("status") == "running" else "已在队列中"
        raise RuntimeError(f"{label}{status_text}，请勿重复提交。")

    return portfolio_analysis_task_manager.start_task(
        session_id,
        task_type=task_type,
        label=label,
        runner=runner,
        metadata=metadata or {},
    )


def consume_finished_ui_analysis_task(task_type: str, state_key: str) -> Optional[Dict[str, Any]]:
    task = get_latest_ui_analysis_task(task_type)
    if not task or task.get("status") in {"queued", "running"}:
        return None

    if st.session_state.get(state_key) == task.get("id"):
        return None
    st.session_state[state_key] = task.get("id")
    return task


def get_ui_analysis_button_state(task_type: str, default_label: str) -> tuple[str, bool, str]:
    active_task = get_active_ui_analysis_task(task_type)
    if not active_task:
        return default_label, False, ""

    if active_task.get("status") == "running":
        return f"{default_label}（分析中）", True, "当前任务正在执行，请等待完成"
    return f"{default_label}（排队中）", True, "当前任务已在队列中，请等待执行"


def render_ui_analysis_task_card(task_type: str, title: str) -> None:
    active_task = get_active_ui_analysis_task(task_type)
    if not active_task:
        return

    progress = float(active_task.get("progress") or 0.0)
    current = int(active_task.get("current") or 0)
    total = int(active_task.get("total") or 0)
    started_at = _format_task_time(active_task.get("started_at"))
    display_title = active_task.get("label") or title

    with st.container(border=True):
        st.markdown(f"#### {display_title}")
        st.progress(progress, text=_format_progress_text(current, total))
        st.caption(f"开始时间：{started_at or '待开始'}")


def render_ui_analysis_task_live_card(task_type: str, title: str, state_prefix: str) -> None:
    """Render live task card and trigger full rerun when status changes."""
    active_task = get_active_ui_analysis_task(task_type)
    task_state_key = f"{state_prefix}_task_id"
    signature_key = f"{state_prefix}_signature"
    previous_task_id = st.session_state.get(task_state_key)
    previous_signature = st.session_state.get(signature_key)

    if not active_task:
        if previous_task_id or previous_signature:
            st.session_state.pop(task_state_key, None)
            st.session_state.pop(signature_key, None)
            st.rerun()
        return

    task_id = active_task.get("id")
    st.session_state[task_state_key] = task_id
    refresh_signature = _build_refresh_signature(active_task)
    if previous_signature != refresh_signature:
        st.session_state[signature_key] = refresh_signature
        if previous_signature is not None:
            st.rerun()

    render_ui_analysis_task_card(task_type, title)
