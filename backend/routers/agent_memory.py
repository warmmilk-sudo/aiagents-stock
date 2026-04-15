"""API router for the agent memory module (CRUD operations)."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel

from backend.api import ApiError, success_payload
from backend.auth import require_session


router = APIRouter(prefix="/api/memory", tags=["agent-memory"])


class FactPatchPayload(BaseModel):
    importance_score: Optional[float] = None
    is_ignored: Optional[bool] = None


class ProfilePutPayload(BaseModel):
    macro_profile: str


def _get_db():
    from agent_memory_db import agent_memory_db
    return agent_memory_db


def _get_service():
    from agent_memory_service import agent_memory_service
    return agent_memory_service


@router.get("/{stock_code}")
def get_memory_archive(request: Request, stock_code: str) -> dict:
    """Return the full memory archive for a stock (all three tiers)."""
    require_session(request)
    db = _get_db()
    facts = db.get_factual_memories(stock_code, include_ignored=True)
    for fact in facts:
        fact.pop("embedding", None)
    return success_payload({
        "stock_code": stock_code,
        "long_term_profile": db.get_long_term_profile(stock_code),
        "working_memories": db.get_working_memory(stock_code),
        "factual_memories": facts,
        "summary": db.get_memory_summary(stock_code),
    })


@router.get("/{stock_code}/facts")
def list_facts(request: Request, stock_code: str, include_ignored: bool = True) -> dict:
    """List factual memories for a stock."""
    require_session(request)
    facts = _get_db().get_factual_memories(stock_code, include_ignored=include_ignored)
    # Strip embedding from response (too large)
    for f in facts:
        f.pop("embedding", None)
    return success_payload(facts)


@router.patch("/{stock_code}/facts/{fact_id}")
def patch_fact(request: Request, stock_code: str, fact_id: int, payload: FactPatchPayload) -> dict:
    """Update importance score or ignore/restore a fact (bias correction)."""
    require_session(request)
    db = _get_db()
    fact = db.get_factual_memory(fact_id)
    if not fact or fact.get("stock_code") != stock_code:
        raise ApiError(404, "未找到该记忆条目", error_code="memory_fact_not_found")

    changed = False
    if payload.importance_score is not None:
        changed = db.update_factual_importance(fact_id, payload.importance_score) or changed
    if payload.is_ignored is not None:
        if payload.is_ignored:
            changed = db.ignore_factual_memory(fact_id) or changed
        else:
            changed = db.restore_factual_memory(fact_id) or changed

    return success_payload({"updated": changed})


@router.delete("/{stock_code}/facts/{fact_id}")
def delete_fact(request: Request, stock_code: str, fact_id: int) -> dict:
    """Permanently delete a factual memory."""
    require_session(request)
    db = _get_db()
    fact = db.get_factual_memory(fact_id)
    if not fact or fact.get("stock_code") != stock_code:
        raise ApiError(404, "未找到该记忆条目", error_code="memory_fact_not_found")
    db.delete_factual_memory(fact_id)
    return success_payload({"deleted": True}, message="记忆已删除")


@router.put("/{stock_code}/profile")
def update_profile(request: Request, stock_code: str, payload: ProfilePutPayload) -> dict:
    """Manually edit the long-term macro profile for a stock."""
    require_session(request)
    _get_db().save_long_term_profile(stock_code, payload.macro_profile)
    return success_payload({"stock_code": stock_code, "macro_profile": payload.macro_profile})


@router.post("/{stock_code}/rebuild")
def rebuild_memory(request: Request, stock_code: str) -> dict:
    """Manually reconstruct the agent memory for a single stock."""
    require_session(request)
    _get_service().backfill_from_analysis_history(
        stock_code=stock_code,
        clear_existing=True,
        compress_after=True,
    )
    return success_payload({"stock_code": stock_code}, message="记忆重建完成")
