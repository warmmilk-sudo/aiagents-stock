"""
agent_memory_daemon.py
Background daemon for post-analysis memory processing.

Listens for ANALYSIS_COMPLETED events on the internal event bus and
performs asynchronous memory tasks:
  1. Save working memory from the report.
  2. Extract factual memories via LLM.
  3. Check if long-term profile compression is needed.

All operations run in daemon threads to avoid blocking the front-end.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional

from internal_events import EventBus, Events, event_bus

logger = logging.getLogger(__name__)

# New event type for analysis completion
ANALYSIS_COMPLETED = "analysis_completed"

# Ensure the constant is registered on the Events class
if not hasattr(Events, "ANALYSIS_COMPLETED"):
    Events.ANALYSIS_COMPLETED = ANALYSIS_COMPLETED

_daemon_started = False
_daemon_lock = threading.Lock()


def _on_analysis_completed(**kwargs: Any) -> None:
    """
    Event handler called asynchronously after an analysis report is saved.

    Expected kwargs:
      - stock_code: str
      - stock_name: str
      - analysis_date: str
      - rating: str
      - summary: str
      - discussion_summary: str  (trimmed team discussion text)
      - final_decision: dict
      - source_analysis_id: int | None
    """
    stock_code = kwargs.get("stock_code", "")
    stock_name = kwargs.get("stock_name", "")
    analysis_date = kwargs.get("analysis_date", "")

    if not stock_code:
        logger.warning("Memory daemon received event without stock_code, skipping")
        return

    logger.info("[MemoryDaemon] Processing completed analysis for %s (%s)", stock_code, analysis_date)

    # Lazy import to avoid circular dependencies at module load time
    from agent_memory_service import agent_memory_service

    try:
        # 1. Save working memory
        agent_memory_service.save_working_memory_from_report(
            stock_code=stock_code,
            analysis_date=analysis_date,
            rating=kwargs.get("rating", ""),
            summary=kwargs.get("summary", ""),
            final_decision=kwargs.get("final_decision"),
        )
        logger.info("[MemoryDaemon] Working memory saved for %s", stock_code)
    except Exception as exc:
        logger.error("[MemoryDaemon] Failed saving working memory for %s: %s", stock_code, exc)

    try:
        # 2. Extract factual memories via LLM
        facts = agent_memory_service.extract_facts_from_report(
            stock_code=stock_code,
            stock_name=stock_name,
            analysis_date=analysis_date,
            rating=kwargs.get("rating", ""),
            summary=kwargs.get("summary", ""),
            discussion_summary=kwargs.get("discussion_summary", ""),
            source_analysis_id=kwargs.get("source_analysis_id"),
        )
        logger.info("[MemoryDaemon] Extracted %d facts for %s", len(facts), stock_code)
    except Exception as exc:
        logger.error("[MemoryDaemon] Fact extraction failed for %s: %s", stock_code, exc)

    try:
        # 3. Check if long-term profile needs compression
        compressed = agent_memory_service.maybe_compress_long_term(
            stock_code=stock_code,
            stock_name=stock_name,
        )
        if compressed:
            logger.info("[MemoryDaemon] Long-term profile compressed for %s", stock_code)
    except Exception as exc:
        logger.error("[MemoryDaemon] Profile compression failed for %s: %s", stock_code, exc)


def start_memory_daemon() -> None:
    """Subscribe the daemon handler to the event bus (idempotent)."""
    global _daemon_started
    with _daemon_lock:
        if _daemon_started:
            return
        event_bus.subscribe(ANALYSIS_COMPLETED, _on_analysis_completed)
        _daemon_started = True
        logger.info("[MemoryDaemon] Started — listening for %s events", ANALYSIS_COMPLETED)


def publish_analysis_completed(
    *,
    stock_code: str,
    stock_name: str = "",
    analysis_date: str = "",
    rating: str = "",
    summary: str = "",
    discussion_summary: str = "",
    final_decision: Optional[Dict] = None,
    source_analysis_id: Optional[int] = None,
) -> None:
    """
    Convenience function to publish an analysis-completed event.
    Called from the analysis pipeline after saving the report.
    """
    event_bus.publish(
        ANALYSIS_COMPLETED,
        stock_code=stock_code,
        stock_name=stock_name,
        analysis_date=analysis_date,
        rating=rating,
        summary=summary,
        discussion_summary=discussion_summary,
        final_decision=final_decision,
        source_analysis_id=source_analysis_id,
    )
