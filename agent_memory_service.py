"""
agent_memory_service.py
Business-logic layer for the multi-agent memory module.

Responsibilities:
  - Assemble memory context for injection into agent prompts (fast track).
  - Extract facts from completed reports via LLM (daemon track).
  - Compute time-decay scores (forgetting curve).
  - Perform hybrid retrieval using cosine similarity + decay.
  - Compress long-term profiles when fact count exceeds threshold.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from agent_memory_db import AgentMemoryDB, agent_memory_db
from llm_client import EmbeddingClient, LLMClient
from model_routing import ModelTier
from prompt_registry import build_messages
from investment_action_utils import build_execution_plan

logger = logging.getLogger(__name__)

# How many top-scoring facts to inject into the prompt
RECALL_TOP_K = int(os.getenv("MEMORY_RECALL_TOP_K", "5"))
# How many cross-sector facts to include
CROSS_SECTOR_TOP_K = int(os.getenv("MEMORY_CROSS_SECTOR_TOP_K", "2"))
# Fact count threshold to trigger long-term profile compression
COMPRESS_THRESHOLD = int(os.getenv("MEMORY_COMPRESS_THRESHOLD", "30"))

LEGACY_LONG_TERM_PROFILE_MARKERS = (
    "长期画像由历史回填本地生成",
    "核心记忆以历史深度分析中的执行计划、进出场条件、基线失效条件和风控纪律为主",
    "A股市场中被持续跟踪的研究标的",
    "被持续跟踪的研究标的",
)

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
    """Compute cosine similarity without requiring optional numeric packages."""
    if not vec_a or not vec_b:
        return 0.0
    if len(vec_a) != len(vec_b):
        return 0.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for index in range(len(vec_a)):
        try:
            a = float(vec_a[index])
            b = float(vec_b[index])
        except (TypeError, ValueError):
            continue
        dot += a * b
        norm_a += a * a
        norm_b += b * b
    if norm_a <= 0 or norm_b <= 0:
        return 0.0
    return float(dot / (math.sqrt(norm_a) * math.sqrt(norm_b)))


def _batch_cosine_similarity(query_vec: list[float], candidate_vecs: list[list[float]]) -> list[float]:
    """Batch cosine similarity: query vs N candidates. Returns N scores."""
    return [_cosine_similarity(query_vec, candidate) for candidate in candidate_vecs]


def _collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


class AgentMemoryService:
    """Service for assembling, retrieving, and managing agent memories."""

    def __init__(
        self,
        db: Optional[AgentMemoryDB] = None,
        embedding_client: Optional[EmbeddingClient] = None,
        llm_client: Optional[LLMClient] = None,
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
    def llm_client(self) -> LLMClient:
        if self._llm_client is None:
            self._llm_client = LLMClient()
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
        final_decision: Optional[Dict] = None,
        source_analysis_id: Optional[int] = None,
    ) -> List[Dict]:
        """
        Call LLM to extract structured fact memories from a completed report.
        Saves them to the factual memory pool with embeddings.
        """
        execution_plan = build_execution_plan(final_decision if isinstance(final_decision, dict) else {})
        messages = build_messages(
            "stock_analysis/memory_extract.system.txt",
            "stock_analysis/memory_extract.user.txt",
            stock_name=stock_name,
            stock_code=stock_code,
            analysis_date=analysis_date,
            rating=rating or "未知",
            summary=summary or "无摘要",
            execution_plan=json.dumps(execution_plan, ensure_ascii=False),
            discussion_summary=(discussion_summary or "无讨论内容")[:4000],
        )

        try:
            raw_response = self.llm_client.call_api(
                messages,
                max_tokens=2000,
                sampling_profile="factual",
                tier=ModelTier.LIGHTWEIGHT,
            )
        except Exception as exc:
            logger.error("Memory extraction LLM call failed for %s: %s", stock_code, exc)
            raw_response = ""

        facts = self._parse_facts_response(raw_response) if raw_response else []
        if not facts:
            facts = self._fallback_facts_from_report(
                stock_code=stock_code,
                stock_name=stock_name,
                rating=rating,
                summary=summary,
                final_decision=final_decision if isinstance(final_decision, dict) else {},
            )

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

    def maybe_compress_long_term(self, stock_code: str, stock_name: str = "", force: bool = False) -> bool:
        """
        Check if fact_count_since_update exceeds threshold,
        and if so, call LLM to rewrite the long-term profile.
        Returns True if compression was performed.
        """
        profile_row = self.db.get_long_term_profile(stock_code)
        existing_profile = (profile_row or {}).get("macro_profile", "")
        fact_count = (profile_row or {}).get("fact_count_since_update", 0)

        # Also compress if there's no profile yet but we have facts
        if profile_row and fact_count < COMPRESS_THRESHOLD and not force:
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
                sampling_profile="factual",
                tier=ModelTier.REASONING,
            )
        except Exception as exc:
            logger.error("Long-term profile compression failed for %s: %s", stock_code, exc)
            new_profile = self._fallback_long_term_profile(
                stock_code=stock_code,
                stock_name=stock_name,
                facts=sorted_facts,
                existing_profile=existing_profile,
            )

        new_profile = self._sanitize_long_term_profile(new_profile)
        if self._contains_legacy_profile_marker(new_profile):
            logger.warning("Compressed profile retained legacy fallback wording for %s; using local template", stock_code)
            new_profile = self._fallback_long_term_profile(
                stock_code=stock_code,
                stock_name=stock_name,
                facts=sorted_facts,
                existing_profile=existing_profile,
            )
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

    def save_intraday_outcome_memory(
        self,
        *,
        stock_code: str,
        stock_name: str,
        decision_time: str,
        action: str,
        decision_context: Optional[Dict[str, Any]],
        outcome_snapshot: Dict[str, Any],
    ) -> Optional[int]:
        """Persist deterministic post-trade outcome feedback as factual memory."""
        if not isinstance(outcome_snapshot, dict) or not outcome_snapshot:
            return None
        decision_id = outcome_snapshot.get("decision_id")
        if decision_id in (None, ""):
            return None

        marker = f"盘中结果回填#{decision_id}"
        existing = self.db.get_factual_memories(stock_code, include_ignored=True)
        if any(marker in str(item.get("fact_content") or "") for item in existing):
            return None

        decision_context = decision_context if isinstance(decision_context, dict) else {}
        relation = str(decision_context.get("baseline_relation") or "unknown").strip() or "unknown"
        state = str(decision_context.get("decision_state") or "unknown").strip() or "unknown"
        label = str(outcome_snapshot.get("outcome_label") or "neutral").strip() or "neutral"
        content = (
            f"{stock_name or stock_code} {marker}: 动作{str(action or '').upper()}，状态{state}，"
            f"基线关系{relation}，决策价{outcome_snapshot.get('decision_price')}，"
            f"后续最高{outcome_snapshot.get('max_forward_price')}、最低{outcome_snapshot.get('min_forward_price')}，"
            f"最大上行{outcome_snapshot.get('max_upside_pct')}%，最大回撤{outcome_snapshot.get('max_drawdown_pct')}%，"
            f"最新收益{outcome_snapshot.get('latest_return_pct')}%，结果标签{label}。"
        )
        importance = 78.0
        if label in {"risk_realized", "missed_upside", "early_exit_risk"}:
            importance = 90.0
        elif label in {"favorable_follow_through", "risk_avoided", "avoided_drawdown"}:
            importance = 84.0
        if relation in {"invalidated", "partially_deviated"}:
            importance = max(importance, 86.0)

        return self.db.save_factual_memory(
            stock_code=stock_code,
            fact_content=content,
            timestamp=decision_time or str(outcome_snapshot.get("evaluated_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            importance_score=importance,
            category="execution_result",
            concept_tags=[
                stock_code,
                "intraday_outcome",
                f"action:{str(action or '').upper()}",
                f"state:{state}",
                f"baseline:{relation}",
                f"outcome:{label}",
            ],
            embedding=None,
            source_analysis_id=None,
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
                final_decision=final_decision,
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

    @staticmethod
    def _fallback_facts_from_report(
        *,
        stock_code: str,
        stock_name: str,
        rating: str,
        summary: str,
        final_decision: Dict[str, Any],
    ) -> list[Dict[str, Any]]:
        """Deterministic fact extraction used when the LLM is unavailable."""
        plan = build_execution_plan(final_decision if isinstance(final_decision, dict) else {})
        facts: list[Dict[str, Any]] = []

        def _append(condition_key: str, label: str, category: str, importance: float, tag: str) -> None:
            for condition in plan.get(condition_key) or []:
                text = str(condition or "").strip()
                if not text:
                    continue
                facts.append({
                    "fact_content": f"{stock_name or stock_code} {label}: {text}",
                    "importance_score": importance,
                    "category": category,
                    "concept_tags": [stock_code, tag, "execution_plan"],
                })

        _append("entry_conditions", "进场/加仓条件", "execution", 86, "entry_condition")
        _append("exit_conditions", "离场/减仓条件", "risk", 90, "exit_condition")
        _append("hold_conditions", "继续持有/观望条件", "execution", 78, "hold_condition")
        _append("invalidation_conditions", "基线失效条件", "risk", 92, "invalidation_condition")

        execution_summary = str(plan.get("execution_plan_summary") or "").strip()
        if execution_summary:
            facts.insert(0, {
                "fact_content": f"{stock_name or stock_code} 执行计划: {execution_summary}",
                "importance_score": 88,
                "category": "execution",
                "concept_tags": [stock_code, "execution_plan"],
            })

        operation_advice = str(final_decision.get("operation_advice") or "").strip()
        if operation_advice and not any(operation_advice in str(f.get("fact_content") or "") for f in facts):
            facts.append({
                "fact_content": f"{stock_name or stock_code} 操作纪律: {operation_advice[:180]}",
                "importance_score": 80,
                "category": "execution",
                "concept_tags": [stock_code, "operation_advice"],
            })

        risk_warning = str(final_decision.get("risk_warning") or "").strip()
        if risk_warning:
            facts.append({
                "fact_content": f"{stock_name or stock_code} 风控纪律: {risk_warning[:180]}",
                "importance_score": 84,
                "category": "risk",
                "concept_tags": [stock_code, "risk_discipline"],
            })

        if not facts and (summary or rating):
            facts.append({
                "fact_content": f"{stock_name or stock_code} 历史分析结论: {rating or '未评级'}；{str(summary or '').strip()[:180]}",
                "importance_score": 60,
                "category": "general",
                "concept_tags": [stock_code, "analysis_summary"],
            })
        return facts[:8]

    @staticmethod
    def _contains_legacy_profile_marker(text: str) -> bool:
        return any(marker in str(text or "") for marker in LEGACY_LONG_TERM_PROFILE_MARKERS)

    @staticmethod
    def _sanitize_long_term_profile(text: str) -> str:
        cleaned_lines: list[str] = []
        for raw_line in str(text or "").replace("\r\n", "\n").split("\n"):
            line = raw_line.strip()
            if not line:
                continue
            if any(marker in line for marker in LEGACY_LONG_TERM_PROFILE_MARKERS):
                continue
            line = line.replace("近期高优先级事实：", "").replace("既有画像摘要：", "")
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines).strip()

    @staticmethod
    def _clean_profile_fact_text(text: str, *, stock_code: str, stock_name: str) -> str:
        cleaned = str(text or "").strip()
        if not cleaned:
            return ""
        name = stock_name or stock_code
        cleaned = re.sub(r"^[-*]\s*", "", cleaned)
        cleaned = cleaned.replace("**", "").replace("###", "")
        cleaned = re.sub(r"^\[[^\]]+\]\s*", "", cleaned)
        cleaned = re.sub(r"^\(重要度\d+(?:\.\d+)?\)\s*", "", cleaned)
        cleaned = re.sub(rf"^(?:{re.escape(name)}|{re.escape(stock_code)})\s*", "", cleaned)
        cleaned = re.sub(r"^(?:执行计划|进场/加仓条件|离场/减仓条件|继续持有/观望条件|基线失效条件|操作纪律|风控纪律|历史分析结论)\s*[:：]\s*", "", cleaned)
        cleaned = _collapse_spaces(cleaned)
        return cleaned[:220]

    @classmethod
    def _select_profile_fact(
        cls,
        facts: List[Dict[str, Any]],
        *,
        stock_code: str,
        stock_name: str,
        keywords: tuple[str, ...],
        categories: tuple[str, ...] = (),
        used: Optional[set[str]] = None,
    ) -> str:
        used = used if used is not None else set()
        for fact in facts:
            category = str(fact.get("category") or "").strip()
            content = str(fact.get("fact_content") or "").strip()
            if categories and category not in categories:
                continue
            if keywords and not any(keyword in content for keyword in keywords):
                continue
            cleaned = cls._clean_profile_fact_text(content, stock_code=stock_code, stock_name=stock_name)
            if cleaned and cleaned not in used:
                used.add(cleaned)
                return cleaned
        for fact in facts:
            category = str(fact.get("category") or "").strip()
            content = str(fact.get("fact_content") or "").strip()
            if categories and category not in categories:
                continue
            if keywords and not any(keyword in content for keyword in keywords):
                continue
            cleaned = cls._clean_profile_fact_text(content, stock_code=stock_code, stock_name=stock_name)
            if cleaned and cleaned not in used:
                used.add(cleaned)
                return cleaned
        return ""

    @classmethod
    def _select_profile_facts(
        cls,
        facts: List[Dict[str, Any]],
        *,
        stock_code: str,
        stock_name: str,
        selectors: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...],
        used: Optional[set[str]] = None,
        limit: int = 3,
    ) -> list[str]:
        selected: list[str] = []
        used = used if used is not None else set()
        for keywords, categories in selectors:
            fact = cls._select_profile_fact(
                facts,
                stock_code=stock_code,
                stock_name=stock_name,
                keywords=keywords,
                categories=categories,
                used=used,
            )
            if fact:
                selected.append(fact)
            if len(selected) >= limit:
                break
        return selected

    @staticmethod
    def _existing_profile_opening(existing_profile: str, *, stock_code: str, stock_name: str) -> str:
        if AgentMemoryService._contains_legacy_profile_marker(existing_profile):
            return ""
        cleaned = AgentMemoryService._sanitize_long_term_profile(existing_profile)
        if not cleaned or AgentMemoryService._contains_legacy_profile_marker(cleaned):
            return ""
        first_sentence = re.split(r"(?<=[。！？])", cleaned, maxsplit=1)[0].strip()
        if not first_sentence or AgentMemoryService._contains_legacy_profile_marker(first_sentence):
            return ""
        name = stock_name or stock_code
        if "是" not in first_sentence or re.search(r"是\s*[，,。；;]", first_sentence):
            return ""
        first_sentence = re.sub(rf"^(?:{re.escape(name)}|{re.escape(stock_code)})\s*[（(]{re.escape(stock_code)}[）)]", f"{name}（{stock_code}）", first_sentence)
        if not first_sentence.startswith(f"{name}（{stock_code}）"):
            first_sentence = re.sub(rf"^(?:{re.escape(name)}|{re.escape(stock_code)})", f"{name}（{stock_code}）", first_sentence)
        if not first_sentence.startswith(f"{name}（{stock_code}）"):
            return ""
        if not AgentMemoryService._is_business_profile_clause(first_sentence):
            return ""
        return first_sentence.rstrip("。！？")[:260] + "。"

    @staticmethod
    def _is_business_profile_clause(text: str) -> bool:
        normalized = str(text or "").strip()
        if not normalized:
            return False
        business_keywords = (
            "公司",
            "企业",
            "厂商",
            "供应商",
            "龙头",
            "赛道",
            "行业",
            "产业",
            "业务",
            "主营",
            "产品",
            "客户",
            "订单",
            "产能",
            "营收",
            "收入",
            "净利润",
            "领域",
            "材料",
            "设备",
            "半导体",
            "新能源",
            "消费电子",
            "汽车电子",
            "光模块",
            "PCB",
        )
        trading_starts = (
            "当前",
            "触发",
            "有效跌破",
            "股价",
            "收盘价",
            "持仓",
            "禁止",
            "单日",
            "连续",
            "回踩",
            "突破",
            "止盈",
            "止损",
            "中报预告",
            "股东大会",
            "当日无",
            "高管减持",
        )
        trading_terms = (
            "支撑位",
            "压力位",
            "止损线",
            "止盈位",
            "主力净流",
            "仓位",
            "清仓",
            "减仓",
            "加仓",
            "观望",
            "低吸",
            "换手率",
        )
        clause = re.sub(r"^[^是]{1,40}是", "", normalized, count=1).strip()
        if clause.startswith(trading_starts):
            return False
        if any(term in clause for term in trading_terms) and not any(keyword in clause for keyword in business_keywords):
            return False
        return any(keyword in normalized for keyword in business_keywords)

    @staticmethod
    def _fallback_long_term_profile(
        *,
        stock_code: str,
        stock_name: str,
        facts: List[Dict[str, Any]],
        existing_profile: str,
    ) -> str:
        name = stock_name or stock_code
        sorted_facts = sorted(
            facts or [],
            key=lambda item: (float(item.get("importance_score") or 0), str(item.get("timestamp") or "")),
            reverse=True,
        )
        used: set[str] = set()
        opening = AgentMemoryService._existing_profile_opening(existing_profile, stock_code=stock_code, stock_name=name)
        if not opening:
            business = AgentMemoryService._select_profile_fact(
                sorted_facts,
                stock_code=stock_code,
                stock_name=name,
                categories=("fundamental",),
                keywords=(),
                used=used,
            ) or AgentMemoryService._select_profile_fact(
                sorted_facts,
                stock_code=stock_code,
                stock_name=name,
                keywords=("核心", "业务", "行业", "赛道", "龙头", "客户", "订单", "产能", "增长", "净利润", "营收"),
                used=used,
            )
            if business and not AgentMemoryService._is_business_profile_clause(business):
                business = ""
            if business:
                opening = f"{name}（{stock_code}）是{business.rstrip('。')}。"
            else:
                opening = f"{name}（{stock_code}）的长期底色以业务验证、关键筹码位、资金承接和风控纪律为主。"
        execution_parts = AgentMemoryService._select_profile_facts(
            sorted_facts,
            stock_code=stock_code,
            stock_name=name,
            selectors=(
                (("微波段", "标准波段", "仓位", "执行计划", "轻仓", "低吸", "加仓", "止盈"), ("execution", "general")),
                (("筹码", "支撑", "压力", "主峰", "回踩", "突破", "量比", "主力", "换手", "成本"), ()),
                (("进场", "加仓", "低吸", "回踩", "突破"), ("execution",)),
                (("止盈", "止损", "清仓", "减仓", "跌破", "失效"), ("risk", "execution")),
            ),
            used=used,
            limit=4,
        )
        risk = AgentMemoryService._select_profile_fact(
            sorted_facts,
            stock_code=stock_code,
            stock_name=name,
            categories=("risk",),
            keywords=("风险", "跌破", "减持", "解禁", "不及预期", "回调", "止损", "净流出"),
            used=used,
        )

        execution_clause = "；".join(execution_parts) if execution_parts else "筹码、资金与价格支撑信号仍是判断趋势延续性的核心依据，后续需围绕业绩兑现、关键公告和资金流向变化滚动验证"
        risk_clause = risk or "业绩兑现不及预期、资金承接转弱、关键支撑失守以及市场系统性波动风险"

        profile = (
            f"{opening}"
            f"当前其交易画像显示{execution_clause}。"
            f"主要风险包括{risk_clause}。"
        )
        return AgentMemoryService._sanitize_long_term_profile(profile)


# Module-level singleton
agent_memory_service = AgentMemoryService()
