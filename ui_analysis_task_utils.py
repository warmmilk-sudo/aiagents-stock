"""Shared background task helpers for selector/strategy analysis pages."""

from __future__ import annotations

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
            str(task.get("status") or ""),
            str(task.get("current") or 0),
            str(task.get("total") or 0),
            str(task.get("progress") or 0.0),
            str(task.get("message") or ""),
        ]
    )


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

    status = active_task.get("status") or "queued"
    progress = float(active_task.get("progress") or 0.0)
    message = active_task.get("message") or "任务处理中..."
    current = int(active_task.get("current") or 0)
    total = int(active_task.get("total") or 0)

    queue_tasks = portfolio_analysis_task_manager.get_pending_tasks(_build_session_id(task_type))
    queue_position = 0
    for idx, queued_task in enumerate(queue_tasks, start=1):
        if queued_task.get("id") == active_task.get("id"):
            queue_position = idx
            break

    with st.container(border=True):
        st.markdown(f"#### {title}")
        st.write(message)
        st.progress(progress, text=f"{int(progress * 100)}%")

        c1, c2, c3 = st.columns(3)
        c1.metric("状态", "进行中" if status == "running" else "排队中")
        c2.metric("进度", f"{current}/{total}" if total else str(current))
        c3.metric("队列位置", queue_position or len(queue_tasks))
        st.caption("分析在后台执行，可切换页面，返回后状态会自动同步。")


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
