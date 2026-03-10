from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from analysis_repository import AnalysisRepository, analysis_repository
from investment_db_utils import DEFAULT_ACCOUNT_NAME


SCOPE_LABELS = {
    "research": "深度分析",
    "portfolio": "持仓分析",
}

SOURCE_LABELS = {
    "home_single_analysis": "单股深度分析",
    "home_batch_analysis": "批量深度分析",
    "portfolio_single_analysis": "单股持仓分析",
    "portfolio_batch_analysis": "批量持仓分析",
    "portfolio_scheduler": "定时持仓分析",
    "legacy_home_analysis": "历史深度分析",
    "legacy_portfolio_analysis": "历史持仓分析",
}


def _format_analysis_date(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return text


class AnalysisHistoryService:
    """Unified read-model for research and portfolio analysis history."""

    def __init__(self, repository: Optional[AnalysisRepository] = None):
        self.repository = repository or analysis_repository

    def _build_dedupe_key(self, record: Dict) -> tuple:
        return (
            record.get("analysis_scope") or "research",
            record.get("symbol") or "",
            record.get("stock_name") or "",
            record.get("period") or "",
            record.get("analysis_date") or "",
            record.get("rating") or "",
            str(record.get("summary") or "").strip(),
            self.repository._sort_json_text(record.get("final_decision") or {}),
        )

    def _build_view_model(self, record: Dict) -> Dict:
        normalized = dict(record)
        scope = str(normalized.get("analysis_scope") or "research").strip().lower()
        scope = scope if scope in SCOPE_LABELS else "research"
        normalized["analysis_scope"] = scope
        normalized["analysis_scope_label"] = SCOPE_LABELS[scope]
        normalized["analysis_source_label"] = SOURCE_LABELS.get(
            normalized.get("analysis_source"),
            "历史分析",
        )
        normalized["analysis_time_text"] = _format_analysis_date(normalized.get("analysis_date"))
        normalized["stock_name"] = (
            normalized.get("stock_name")
            or normalized.get("name")
            or normalized.get("symbol")
            or ""
        )
        normalized["account_name"] = normalized.get("account_name") or DEFAULT_ACCOUNT_NAME
        return normalized

    def _matches_search(self, record: Dict, search_term: str) -> bool:
        if not search_term:
            return True
        lowered = search_term.lower()
        haystacks = (
            str(record.get("symbol") or "").lower(),
            str(record.get("stock_name") or "").lower(),
            str(record.get("account_name") or "").lower(),
        )
        return any(lowered in item for item in haystacks)

    def list_scope_options(self) -> List[str]:
        return ["全部", "深度分析", "持仓分析"]

    def list_account_options(self) -> List[str]:
        records = self.list_records(scope="all")
        accounts = sorted({record.get("account_name") or DEFAULT_ACCOUNT_NAME for record in records})
        return ["全部账户"] + accounts

    def list_records(
        self,
        *,
        scope: str = "all",
        account_name: Optional[str] = None,
        search_term: str = "",
        limit: Optional[int] = None,
        full_report_only: bool = True,
    ) -> List[Dict]:
        normalized_scope = {
            "全部": "all",
            "深度分析": "research",
            "持仓分析": "portfolio",
        }.get(scope, scope)
        normalized_scope = normalized_scope if normalized_scope in {"all", "research", "portfolio"} else "all"
        normalized_account = None if account_name in (None, "", "全部账户") else account_name
        normalized_search = str(search_term or "").strip()

        records = self.repository.list_records(full_report_only=full_report_only, limit=limit)
        result: List[Dict] = []
        seen_keys = set()
        for record in records:
            item = self._build_view_model(record)
            if normalized_scope != "all" and item["analysis_scope"] != normalized_scope:
                continue
            if normalized_account and item["account_name"] != normalized_account:
                continue
            if not self._matches_search(item, normalized_search):
                continue
            dedupe_key = self._build_dedupe_key(item)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            result.append(item)
        return result

    def count_records(self, *, full_report_only: bool = True) -> int:
        return len(self.list_records(full_report_only=full_report_only))

    def get_record(self, record_id: int) -> Optional[Dict]:
        record = self.repository.get_record(record_id)
        return self._build_view_model(record) if record else None

    def delete_record(self, record_id: int) -> bool:
        return self.repository.delete_record(record_id)


analysis_history_service = AnalysisHistoryService()
