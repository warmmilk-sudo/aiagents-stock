from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent_memory_db import agent_memory_db
from agent_memory_service import AgentMemoryService, LEGACY_LONG_TERM_PROFILE_MARKERS
from analysis_history_service import analysis_history_service
from llm_client import LLMClient
from model_routing import ModelTier


TARGET_WHERE = " OR ".join(["macro_profile LIKE ?"] * len(LEGACY_LONG_TERM_PROFILE_MARKERS))
TARGET_PARAMS = tuple(f"%{marker}%" for marker in LEGACY_LONG_TERM_PROFILE_MARKERS)


def _collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _strip_markdown(text: str) -> str:
    cleaned = str(text or "")
    cleaned = cleaned.replace("```", "").replace("**", "").replace("###", "")
    cleaned = re.sub(r"^\s*(长期底色|宏观底色|摘要)\s*[:：]\s*", "", cleaned.strip())
    return cleaned.strip()


def _parse_sort_time(value: object) -> tuple[str, str]:
    text = str(value or "").strip()
    return (text[:19], text)


def _get_legacy_rows(stock_code: str = "", limit: int | None = None) -> List[Dict[str, Any]]:
    clauses = [f"({TARGET_WHERE})"]
    params: list[Any] = list(TARGET_PARAMS)
    if stock_code:
        clauses.append("stock_code = ?")
        params.append(stock_code)
    sql = (
        "SELECT stock_code, macro_profile, last_updated, fact_count_since_update "
        "FROM memory_long_term "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY stock_code"
    )
    if limit is not None:
        sql += " LIMIT ?"
        params.append(max(0, int(limit)))

    conn = sqlite3.connect(agent_memory_db.db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]
    finally:
        conn.close()


def _backup_rows(rows: Iterable[Dict[str, Any]]) -> str:
    rows = list(rows)
    if not rows:
        return ""
    backup_dir = ROOT / "data_backups"
    backup_dir.mkdir(exist_ok=True)
    backup_path = backup_dir / f"agent_memory_long_term_legacy_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.jsonl"
    with backup_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    return str(backup_path)


def _latest_stock_name(stock_code: str, legacy_profile: str) -> str:
    records = analysis_history_service.list_records_by_symbol(stock_code)
    for record in records:
        name = str(record.get("stock_name") or "").strip()
        if name:
            return name
    match = re.match(r"^\s*([^（(]+)[（(]" + re.escape(stock_code), legacy_profile or "")
    return match.group(1).strip() if match else stock_code


def _build_analysis_context(stock_code: str, *, max_records: int) -> str:
    records = []
    for summary in analysis_history_service.list_records_by_symbol(stock_code):
        record_id = summary.get("id")
        if record_id in (None, ""):
            continue
        record = analysis_history_service.get_record(int(record_id))
        if record:
            records.append(record)
    records.sort(key=lambda item: (_parse_sort_time(item.get("analysis_date")), int(item.get("id") or 0)), reverse=True)

    chunks: list[str] = []
    for record in records[:max_records]:
        final_decision = record.get("final_decision") if isinstance(record.get("final_decision"), dict) else {}
        details = [
            f"日期：{record.get('analysis_date') or ''}",
            f"评级：{record.get('rating') or final_decision.get('rating') or ''}",
            f"摘要：{record.get('summary') or ''}",
        ]
        operation = str(final_decision.get("operation_advice") or "").strip()
        risk = str(final_decision.get("risk_warning") or "").strip()
        swing_reason = str(final_decision.get("swing_type_reason") or "").strip()
        if operation:
            details.append(f"操作框架：{operation}")
        if risk:
            details.append(f"风险提示：{risk}")
        if swing_reason:
            details.append(f"持仓逻辑：{swing_reason}")
        discussion = _collapse_spaces(str(record.get("discussion_result") or ""))[:1600]
        if discussion:
            details.append(f"讨论要点：{discussion}")
        chunks.append("\n".join(details))
    return "\n\n".join(chunks) or "暂无历史分析记录。"


def _build_fact_context(stock_code: str, stock_name: str, *, max_facts: int) -> tuple[str, List[Dict[str, Any]]]:
    facts = agent_memory_db.get_factual_memories(stock_code, include_ignored=False)
    facts.sort(key=lambda item: (str(item.get("timestamp") or ""), float(item.get("importance_score") or 0)), reverse=True)

    lines: list[str] = []
    selected: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for fact in facts:
        cleaned = AgentMemoryService._clean_profile_fact_text(
            str(fact.get("fact_content") or ""),
            stock_code=stock_code,
            stock_name=stock_name,
        )
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        selected.append(fact)
        lines.append(
            f"- [{fact.get('timestamp') or ''}] {fact.get('category') or 'general'} "
            f"(重要度{float(fact.get('importance_score') or 0):.0f}) {cleaned}"
        )
        if len(lines) >= max_facts:
            break
    return "\n".join(lines) or "暂无事实记忆。", selected


def _build_messages(
    *,
    stock_code: str,
    stock_name: str,
    legacy_profile: str,
    analysis_context: str,
    fact_context: str,
) -> list[Dict[str, str]]:
    old_profile = AgentMemoryService._sanitize_long_term_profile(legacy_profile)
    system = (
        "你是一位资深A股研究员，正在重写股票档案里的长期底色。"
        "只输出一个自然段，180-360个中文字左右，不要标题、列表、Markdown。"
        "结构必须接近：股票名称（代码）是……。当前其……；……。主要风险包括……。"
        "优先保留业务定位、核心赛道、增长/业绩验证、筹码结构、支撑/压力、资金行为、关键数值和风险。"
        "不要臆造资料里不存在的事实。"
        "严禁出现“长期画像由历史回填本地生成”“核心记忆以历史深度分析”“近期高优先级事实”“既有画像摘要”等系统痕迹。"
    )
    user = f"""股票：{stock_name}（{stock_code}）

旧底色中可参考的事实片段（旧模板句已剔除，不要沿用其表达）：
{old_profile or "无"}

最近历史分析记录：
{analysis_context}

事实记忆：
{fact_context}

参考结构示例：
意华股份（002897）是国内聚焦连接器赛道的核心厂商，核心业务布局AI算力基建配套数通高速连接器、汽车连接器两大高景气领域，已形成双轮驱动的增长结构。当前其筹码结构于67.6元附近形成高度单峰密集，70%筹码成本带宽仅4.8%，64.8元为70%筹码成本下沿，是当前市场共识的核心趋势支撑位；市场对其2026年一季度净利润的共识预期阈值为1亿元，单季度净利润若低于该水平则视为业绩不及预期。主要风险包括下游算力基建落地节奏不及预期、汽车产销波动影响需求，连接器行业竞争加剧挤压利润，以及筹码结构松动引发的趋势性波动风险。

请按示例的文字结构重写成{stock_name}自己的长期底色，不能照抄示例内容。"""
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _finalize_profile(raw_profile: str) -> str:
    profile = AgentMemoryService._sanitize_long_term_profile(_strip_markdown(raw_profile))
    profile = re.sub(r"\n+", " ", profile)
    return _collapse_spaces(profile)


def _generate_profile(
    *,
    llm_client: LLMClient,
    stock_code: str,
    stock_name: str,
    legacy_profile: str,
    analysis_context: str,
    fact_context: str,
    fallback_facts: List[Dict[str, Any]],
) -> tuple[str, str]:
    messages = _build_messages(
        stock_code=stock_code,
        stock_name=stock_name,
        legacy_profile=legacy_profile,
        analysis_context=analysis_context,
        fact_context=fact_context,
    )
    try:
        raw = llm_client.call_api(
            messages,
            max_tokens=1200,
            sampling_profile="factual",
            tier=ModelTier.REASONING,
        )
        profile = _finalize_profile(raw)
        if len(profile) >= 50 and not AgentMemoryService._contains_legacy_profile_marker(profile):
            return profile, "llm"
    except Exception as exc:
        print(f"[WARN] {stock_code} LLM重写失败，使用本地兜底: {exc}", file=sys.stderr)

    fallback = AgentMemoryService._fallback_long_term_profile(
        stock_code=stock_code,
        stock_name=stock_name,
        facts=fallback_facts,
        existing_profile=legacy_profile,
    )
    return _finalize_profile(fallback), "fallback"


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild legacy-generated long-term stock profiles.")
    parser.add_argument("--apply", action="store_true", help="Write rebuilt profiles to agent_memory.db.")
    parser.add_argument("--stock-code", default="", help="Only rebuild one stock code.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of matched profiles.")
    parser.add_argument("--max-records", type=int, default=4, help="Latest analysis records per stock used as context.")
    parser.add_argument("--max-facts", type=int, default=28, help="Fact memories per stock used as context.")
    args = parser.parse_args()

    stock_code = str(args.stock_code or "").strip().upper()
    rows = _get_legacy_rows(stock_code=stock_code, limit=args.limit)
    backup_path = _backup_rows(rows) if args.apply else ""
    llm_client = LLMClient()

    results = []
    for index, row in enumerate(rows, start=1):
        code = str(row["stock_code"]).strip().upper()
        stock_name = _latest_stock_name(code, row.get("macro_profile") or "")
        print(f"[{index}/{len(rows)}] 重写 {stock_name}({code})", flush=True)
        analysis_context = _build_analysis_context(code, max_records=max(1, args.max_records))
        fact_context, fallback_facts = _build_fact_context(code, stock_name, max_facts=max(1, args.max_facts))
        profile, source = _generate_profile(
            llm_client=llm_client,
            stock_code=code,
            stock_name=stock_name,
            legacy_profile=row.get("macro_profile") or "",
            analysis_context=analysis_context,
            fact_context=fact_context,
            fallback_facts=fallback_facts,
        )
        if args.apply:
            agent_memory_db.save_long_term_profile(code, profile)
        results.append(
            {
                "stock_code": code,
                "stock_name": stock_name,
                "source": source,
                "applied": bool(args.apply),
                "profile_length": len(profile),
                "preview": profile[:180],
            }
        )

    remaining = len(_get_legacy_rows(stock_code=stock_code, limit=None)) if args.apply else len(rows)
    output = {
        "apply": bool(args.apply),
        "matched": len(rows),
        "updated": len(results) if args.apply else 0,
        "remaining_legacy_profiles": remaining,
        "backup_path": backup_path,
        "results": results,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0 if remaining == 0 or not args.apply else 1


if __name__ == "__main__":
    raise SystemExit(main())
