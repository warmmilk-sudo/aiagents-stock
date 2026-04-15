"""
agent_memory_service.py
Business-logic layer for the multi-agent memory module.

Responsibilities:
  - Assemble memory context for injection into agent prompts (fast track).
  - Extract facts from completed reports via LLM (daemon track).
  - Compute time-decay scores (forgetting curve).
  - Perform hybrid retrieval using numpy cosine similarity + decay.
  - Compress long-term profiles when fact count exceeds threshold.
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np

from agent_memory_db import AgentMemoryDB, agent_memory_db
from deepseek_client import DeepSeekClient, EmbeddingClient
from model_routing import ModelTier
from prompt_registry import build_messages

logger = logging.getLogger(__name__)

# How many top-scoring facts to inject into the prompt
RECALL_TOP_K = int(os.getenv("MEMORY_RECALL_TOP_K", "5"))
# How many cross-sector facts to include
CROSS_SECTOR_TOP_K = int(os.getenv("MEMORY_CROSS_SECTOR_TOP_K", "2"))
# Fact count threshold to trigger long-term profile compression
COMPRESS_THRESHOLD = int(os.getenv("MEMORY_COMPRESS_THRESHOLD", "30"))

# Weight balance: similarity vs decay in final score
WEIGHT_SIMILARITY = float(os.getenv("MEMORY_WEIGHT_SIMILARITY", "0.6"))
WEIGHT_DECAY = float(os.getenv("MEMORY_WEIGHT_DECAY", "0.4"))


def _coerce_sort_datetime(record: Dict[str, Any]) -> datetime:
    return (
        _parse_datetime(str(record.get("analysis_date") or ""))
        or _parse_datetime(str(record.get("analysis_time_text") or ""))
        or datetime.min
    )


def _parse_datetime(text: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    return None


def compute_decay_score(importance: float, timestamp_str: str, now: Optional[datetime] = None) -> float:
    """
    Compute the active score of a memory using an exponential decay formula.

    Score = importance * exp(-λ * Δdays)

    Where λ depends on the importance level:
      - importance >= 90: λ ≈ 0.002  → half-life ~346 days (long-lasting trauma)
      - importance >= 70: λ ≈ 0.010  → half-life ~69 days
      - importance <  70: λ ≈ 0.030  → half-life ~23 days (fades quickly)
    """
    if now is None:
        now = datetime.now()
    ts = _parse_datetime(timestamp_str)
    if ts is None:
        return importance * 0.5  # Fallback: moderate penalty

    delta_days = max(0, (now - ts).total_seconds() / 86400)

    if importance >= 90:
        lam = 0.002
    elif importance >= 70:
        lam = 0.010
    else:
        lam = 0.030

    return importance * math.exp(-lam * delta_days)


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    """Compute cosine similarity between two vectors using numpy."""
    a = np.array(vec_a, dtype=np.float32)
    b = np.array(vec_b, dtype=np.float32)
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0:
        return 0.0
    return float(dot / norm)


def _batch_cosine_similarity(query_vec: list[float], candidate_vecs: list[list[float]]) -> list[float]:
    """Batch cosine similarity: query vs N candidates. Returns N scores."""
    if not candidate_vecs:
        return []
    q = np.array(query_vec, dtype=np.float32)
    mat = np.array(candidate_vecs, dtype=np.float32)
    dots = mat @ q
    norms = np.linalg.norm(mat, axis=1) * np.linalg.norm(q)
    norms[norms == 0] = 1.0  # Avoid division by zero
    return (dots / norms).tolist()


class AgentMemoryService:
    """Service for assembling, retrieving, and managing agent memories."""

    def __init__(
        self,
        db: Optional[AgentMemoryDB] = None,
        embedding_client: Optional[EmbeddingClient] = None,
        llm_client: Optional[DeepSeekClient] = None,
    ):
        self.db = db or agent_memory_db
        self._embedding_client = embedding_client
        self._llm_client = llm_client

    @property
    def embedding_client(self) -> EmbeddingClient:
        if self._embedding_client is None:
            self._embedding_client = EmbeddingClient()
        return self._embedding_client

    @property
    def llm_client(self) -> DeepSeekClient:
        if self._llm_client is None:
            self._llm_client = DeepSeekClient()
        return self._llm_client

    # ------------------------------------------------------------------
    # FRONT-TRACK: Memory recall & context assembly
    # ------------------------------------------------------------------

    def assemble_memory_context(
        self,
        stock_code: str,
        current_summary: str = "",
        stock_name: str = "",
    ) -> Dict[str, Any]:
        """
        Build the complete memory context dict for a given stock.

        This is the FAST-TRACK method called before an analysis run.
        It reads pre-processed data from SQLite and does numpy scoring.
        Returns a dict ready for prompt injection.
        """
        # 1. Long-term profile
        profile_row = self.db.get_long_term_profile(stock_code)
        long_term_profile = (profile_row or {}).get("macro_profile", "")

        # 2. Working memory (recent decisions)
        working_memories = self.db.get_working_memory(stock_code, limit=3)

        # 3. Factual recall with hybrid scoring
        recalled_facts = self._recall_facts(stock_code, current_summary)

        return {
            "long_term_profile": long_term_profile,
            "working_memories": working_memories,
            "recalled_facts": recalled_facts,
            "recalled_fact_ids": [f["id"] for f in recalled_facts],
        }

    def format_memory_prompt_block(self, memory_context: Dict[str, Any]) -> str:
        """
        Format the memory context into a text block suitable for prompt injection.
        Returns empty string if there's nothing to inject.
        """
        sections: list[str] = []

        profile = memory_context.get("long_term_profile", "")
        if profile:
            sections.append(f"【个股长期底色（宏观画像）】\n{profile}")

        working = memory_context.get("working_memories", [])
        if working:
            lines = []
            for mem in working:
                date = mem.get("analysis_date", "")
                summary = mem.get("decision_summary", "")
                lines.append(f"- {date}: {summary}")
            sections.append("【近期决策回顾】\n" + "\n".join(lines))

        facts = memory_context.get("recalled_facts", [])
        if facts:
            lines = []
            for fact in facts:
                ts = fact.get("timestamp", "")
                content = fact.get("fact_content", "")
                score = fact.get("_active_score", 0)
                source = fact.get("stock_code", "")
                prefix = f"[{source}] " if source != facts[0].get("_query_stock", source) else ""
                lines.append(f"- {prefix}{ts}: {content} (活跃度={score:.0f})")
            sections.append("【唤醒的历史关键事实】\n" + "\n".join(lines))

        if not sections:
            return ""
        return "\n\n".join(sections)

    def _recall_facts(self, stock_code: str, current_summary: str) -> List[Dict]:
        """
        Hybrid retrieval combining embedding similarity and decay scoring.
        Falls back to pure decay scoring if embedding is unavailable.
        """
        all_facts = self.db.get_factual_memories(stock_code, include_ignored=False)
        now = datetime.now()

        # Try to get query embedding for similarity scoring
        query_vec: Optional[list[float]] = None
        if current_summary and self.embedding_client.is_available:
            try:
                query_vec = self.embedding_client.get_embedding(current_summary[:512])
            except Exception as exc:
                logger.warning("Failed to get query embedding for recall: %s", exc)

        scored: list[tuple[float, Dict]] = []
        for fact in all_facts:
            decay_score = compute_decay_score(fact["importance_score"], fact["timestamp"], now)

            sim_score = 0.0
            if query_vec and fact.get("embedding"):
                sim_score = _cosine_similarity(query_vec, fact["embedding"])
                sim_score = max(0.0, sim_score)  # Clamp negative

            if query_vec:
                final = WEIGHT_SIMILARITY * sim_score * 100 + WEIGHT_DECAY * decay_score
            else:
                final = decay_score

            fact["_active_score"] = final
            fact["_sim_score"] = sim_score
            fact["_decay_score"] = decay_score
            fact["_query_stock"] = stock_code
            scored.append((final, fact))

        # Sort descending and take top-K
        scored.sort(key=lambda x: x[0], reverse=True)
        result = [item[1] for item in scored[:RECALL_TOP_K]]

        # Cross-sector association: find related facts from other stocks
        if all_facts:
            all_tags: set[str] = set()
            for fact in all_facts:
                for tag in (fact.get("concept_tags") or []):
                    all_tags.add(tag)
            if all_tags:
                cross_facts = self.db.search_cross_sector_facts(
                    concept_tags=list(all_tags),
                    exclude_stock_code=stock_code,
                    importance_threshold=80,
                    limit=CROSS_SECTOR_TOP_K,
                )
                for cf in cross_facts:
                    cf["_active_score"] = compute_decay_score(cf["importance_score"], cf["timestamp"], now)
                    cf["_query_stock"] = stock_code
                result.extend(cross_facts)

        return result

    # ------------------------------------------------------------------
    # DAEMON-TRACK: Fact extraction & profile compression
    # ------------------------------------------------------------------

    def extract_facts_from_report(
        self,
        stock_code: str,
        stock_name: str,
        analysis_date: str,
        rating: str,
        summary: str,
        discussion_summary: str,
        source_analysis_id: Optional[int] = None,
    ) -> List[Dict]:
        """
        Call LLM to extract structured fact memories from a completed report.
        Saves them to the factual memory pool with embeddings.
        """
        messages = build_messages(
            "stock_analysis/memory_extract.system.txt",
            "stock_analysis/memory_extract.user.txt",
            stock_name=stock_name,
            stock_code=stock_code,
            analysis_date=analysis_date,
            rating=rating or "未知",
            summary=summary or "无摘要",
            discussion_summary=(discussion_summary or "无讨论内容")[:4000],
        )

        try:
            raw_response = self.llm_client.call_api(
                messages,
                max_tokens=2000,
                temperature=0.3,
                tier=ModelTier.LIGHTWEIGHT,
            )
        except Exception as exc:
            logger.error("Memory extraction LLM call failed for %s: %s", stock_code, exc)
            return []

        facts = self._parse_facts_response(raw_response)

        # Get embeddings for all facts in one batch
        fact_texts = [f["fact_content"] for f in facts if f.get("fact_content")]
        embeddings: list[Optional[list[float]]] = [None] * len(fact_texts)
        if fact_texts and self.embedding_client.is_available:
            try:
                embeddings = self.embedding_client.get_embeddings(fact_texts)
            except Exception as exc:
                logger.warning("Batch embedding failed for %s: %s", stock_code, exc)

        # Save each fact to DB
        saved: list[Dict] = []
        for i, fact in enumerate(facts):
            content = fact.get("fact_content", "").strip()
            if not content:
                continue
            fact_id = self.db.save_factual_memory(
                stock_code=stock_code,
                fact_content=content,
                timestamp=analysis_date,
                importance_score=float(fact.get("importance_score", 50)),
                category=fact.get("category", "general"),
                concept_tags=fact.get("concept_tags", []),
                embedding=embeddings[i] if i < len(embeddings) else None,
                source_analysis_id=source_analysis_id,
            )
            fact["id"] = fact_id
            saved.append(fact)

        logger.info("[%s] Extracted and saved %d facts from report", stock_code, len(saved))
        return saved

    def maybe_compress_long_term(self, stock_code: str, stock_name: str = "") -> bool:
        """
        Check if fact_count_since_update exceeds threshold,
        and if so, call LLM to rewrite the long-term profile.
        Returns True if compression was performed.
        """
        profile_row = self.db.get_long_term_profile(stock_code)
        existing_profile = (profile_row or {}).get("macro_profile", "")
        fact_count = (profile_row or {}).get("fact_count_since_update", 0)

        # Also compress if there's no profile yet but we have facts
        if profile_row and fact_count < COMPRESS_THRESHOLD:
            return False

        # Gather recent facts for compression context
        all_facts = self.db.get_factual_memories(stock_code, include_ignored=False)
        if not all_facts and not existing_profile:
            return False

        # Take the most recent/important facts (up to 20)
        sorted_facts = sorted(all_facts, key=lambda f: f.get("timestamp", ""), reverse=True)[:20]
        recent_facts_text = "\n".join(
            f"- [{f.get('timestamp', '')}] (重要度{f.get('importance_score', 0):.0f}) {f.get('fact_content', '')}"
            for f in sorted_facts
        )

        if not existing_profile:
            existing_profile = "暂无历史画像，这是首次建立该股票的宏观底色。"

        messages = build_messages(
            "stock_analysis/memory_compress.system.txt",
            "stock_analysis/memory_compress.user.txt",
            stock_name=stock_name or stock_code,
            stock_code=stock_code,
            existing_profile=existing_profile,
            recent_facts=recent_facts_text or "暂无新增事实。",
        )

        try:
            new_profile = self.llm_client.call_api(
                messages,
                max_tokens=1000,
                temperature=0.3,
                tier=ModelTier.REASONING,
            )
        except Exception as exc:
            logger.error("Long-term profile compression failed for %s: %s", stock_code, exc)
            return False

        new_profile = new_profile.strip()
        if len(new_profile) < 50:
            logger.warning("Compressed profile too short for %s, skipping", stock_code)
            return False

        self.db.save_long_term_profile(stock_code, new_profile)
        logger.info("[%s] Long-term profile compressed/updated (%d chars)", stock_code, len(new_profile))
        return True

    def save_working_memory_from_report(
        self,
        stock_code: str,
        analysis_date: str,
        rating: str,
        summary: str,
        final_decision: Optional[Dict] = None,
    ) -> None:
        """Save a working-memory entry from a completed analysis."""
        decision_summary = f"评级: {rating or '未知'}"
        if summary:
            decision_summary += f" | {summary[:200]}"
        self.db.save_working_memory(
            stock_code=stock_code,
            analysis_date=analysis_date,
            decision_summary=decision_summary,
            strategy=final_decision,
        )

    def backfill_from_analysis_history(
        self,
        stock_code: str,
        *,
        clear_existing: bool = False,
        compress_after: bool = True,
    ) -> Dict[str, Any]:
        """
        Rebuild memory content for a stock from existing analysis-history records.
        """
        from analysis_history_service import analysis_history_service

        history_records = analysis_history_service.list_records_by_symbol(stock_code)
        ordered_records = sorted(
            (
                analysis_history_service.get_record(int(record["id"]))
                for record in history_records
                if record.get("id") not in (None, "")
            ),
            key=_coerce_sort_datetime,
        )
        ordered_records = [record for record in ordered_records if record]

        if clear_existing:
            deleted = self.db.clear_stock_memory(stock_code)
        else:
            deleted = {"memory_working": 0, "memory_factual": 0, "memory_long_term": 0}

        if not ordered_records:
            return {
                "stock_code": stock_code,
                "stock_name": "",
                "record_count": 0,
                "working_saved": 0,
                "facts_saved": 0,
                "compressed": False,
                "deleted": deleted,
                "summary": self.db.get_memory_summary(stock_code),
            }

        working_saved = 0
        facts_saved = 0
        stock_name = str(ordered_records[-1].get("stock_name") or stock_code)

        for record in ordered_records:
            analysis_date = str(record.get("analysis_date") or record.get("analysis_time_text") or "").strip()
            if not analysis_date:
                continue
            final_decision = record.get("final_decision") if isinstance(record.get("final_decision"), dict) else {}
            rating = str(
                (final_decision or {}).get("rating")
                or record.get("decision_label")
                or record.get("rating")
                or ""
            )
            summary = str(record.get("summary") or "")
            self.save_working_memory_from_report(
                stock_code=stock_code,
                analysis_date=analysis_date,
                rating=rating,
                summary=summary,
                final_decision=final_decision,
            )
            working_saved += 1

            facts = self.extract_facts_from_report(
                stock_code=stock_code,
                stock_name=str(record.get("stock_name") or stock_name or stock_code),
                analysis_date=analysis_date,
                rating=rating,
                summary=summary,
                discussion_summary=str(record.get("discussion_result") or ""),
                source_analysis_id=int(record["id"]) if record.get("id") not in (None, "") else None,
            )
            facts_saved += len(facts)

        compressed = False
        if compress_after:
            compressed = self.maybe_compress_long_term(stock_code=stock_code, stock_name=stock_name)

        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "record_count": len(ordered_records),
            "working_saved": working_saved,
            "facts_saved": facts_saved,
            "compressed": compressed,
            "deleted": deleted,
            "summary": self.db.get_memory_summary(stock_code),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_facts_response(raw: str) -> list[Dict]:
        """Parse the LLM response for fact extraction (expects JSON array)."""
        text = raw.strip()
        # Try to find a JSON array in the response
        start = text.find("[")
        end = text.rfind("]")
        if start < 0 or end < 0 or end <= start:
            logger.warning("No JSON array found in memory extraction response")
            return []
        try:
            facts = json.loads(text[start:end + 1])
            if not isinstance(facts, list):
                return []
            return [f for f in facts if isinstance(f, dict) and f.get("fact_content")]
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse memory extraction JSON: %s", exc)
            return []


# Module-level singleton
agent_memory_service = AgentMemoryService()
