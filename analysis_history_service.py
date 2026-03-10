from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from analysis_repository import AnalysisRepository, analysis_repository
from asset_repository import AssetRepository, STATUS_PORTFOLIO, STATUS_PRIORITY, STATUS_RESEARCH, STATUS_WATCHLIST
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

ASSET_STATUS_LABELS = {
    STATUS_RESEARCH: "研究池",
    STATUS_WATCHLIST: "盯盘中",
    STATUS_PORTFOLIO: "在持仓",
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

    def __init__(
        self,
        repository: Optional[AnalysisRepository] = None,
        asset_store: Optional[AssetRepository] = None,
    ):
        self.repository = repository or analysis_repository
        self.asset_store = asset_store or AssetRepository(self.repository.db_path)

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

    def _normalize_asset_lookup_key(self, symbol: object, account_name: object) -> tuple[str, str]:
        return (
            str(symbol or "").strip().upper(),
            str(account_name or DEFAULT_ACCOUNT_NAME).strip() or DEFAULT_ACCOUNT_NAME,
        )

    def _build_asset_indexes(self) -> tuple[Dict[int, Dict], Dict[tuple[str, str], Dict]]:
        assets_by_id: Dict[int, Dict] = {}
        assets_by_symbol_account: Dict[tuple[str, str], Dict] = {}
        for asset in self.asset_store.list_assets(include_deleted=False):
            asset_id = asset.get("id")
            if asset_id not in (None, ""):
                try:
                    assets_by_id[int(asset_id)] = asset
                except (TypeError, ValueError):
                    pass
            key = self._normalize_asset_lookup_key(asset.get("symbol"), asset.get("account_name"))
            existing = assets_by_symbol_account.get(key)
            asset_sort_key = (
                STATUS_PRIORITY.get(str(asset.get("status") or STATUS_RESEARCH), -1),
                str(asset.get("updated_at") or ""),
                int(asset.get("id") or 0),
            )
            existing_sort_key = (
                STATUS_PRIORITY.get(str((existing or {}).get("status") or STATUS_RESEARCH), -1),
                str((existing or {}).get("updated_at") or ""),
                int((existing or {}).get("id") or 0),
            )
            if existing is None or asset_sort_key > existing_sort_key:
                assets_by_symbol_account[key] = asset
        return assets_by_id, assets_by_symbol_account

    def _resolve_linked_asset(
        self,
        record: Dict,
        *,
        assets_by_id: Dict[int, Dict],
        assets_by_symbol_account: Dict[tuple[str, str], Dict],
    ) -> Optional[Dict]:
        linked_asset_id = record.get("asset_id")
        if linked_asset_id in (None, ""):
            linked_asset_id = record.get("portfolio_stock_id")
        if linked_asset_id not in (None, ""):
            try:
                asset = assets_by_id.get(int(linked_asset_id))
                if asset:
                    return asset
            except (TypeError, ValueError):
                pass
        lookup_key = self._normalize_asset_lookup_key(
            record.get("symbol"),
            record.get("account_name") or DEFAULT_ACCOUNT_NAME,
        )
        return assets_by_symbol_account.get(lookup_key)

    def _apply_position_state(self, record: Dict, linked_asset: Optional[Dict]) -> Dict:
        normalized = dict(record)
        current_status = str((linked_asset or {}).get("status") or normalized.get("asset_status_snapshot") or "").strip().lower()
        is_in_portfolio = current_status == STATUS_PORTFOLIO
        normalized["linked_asset_id"] = (
            (linked_asset or {}).get("id")
            or normalized.get("asset_id")
            or normalized.get("portfolio_stock_id")
        )
        normalized["linked_asset_status"] = current_status
        normalized["linked_asset_status_label"] = ASSET_STATUS_LABELS.get(
            current_status,
            "未关联",
        )
        normalized["is_in_portfolio"] = is_in_portfolio
        normalized["portfolio_state_label"] = "在持仓" if is_in_portfolio else "未持仓"
        normalized["portfolio_action_label"] = "跳转持仓" if is_in_portfolio else "设为持仓"
        return normalized

    def _build_view_model(
        self,
        record: Dict,
        *,
        assets_by_id: Dict[int, Dict],
        assets_by_symbol_account: Dict[tuple[str, str], Dict],
    ) -> Dict:
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
        linked_asset = self._resolve_linked_asset(
            normalized,
            assets_by_id=assets_by_id,
            assets_by_symbol_account=assets_by_symbol_account,
        )
        return self._apply_position_state(normalized, linked_asset)

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

    def list_portfolio_state_options(self) -> List[str]:
        return ["全部", "在持仓", "未持仓"]

    def list_account_options(self) -> List[str]:
        records = self.list_records(scope="all", portfolio_state="全部")
        accounts = sorted({record.get("account_name") or DEFAULT_ACCOUNT_NAME for record in records})
        return ["全部账户"] + accounts

    def list_records(
        self,
        *,
        scope: str = "all",
        portfolio_state: str = "全部",
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
        normalized_portfolio_state = {
            "全部": "all",
            "在持仓": "portfolio",
            "未持仓": "non_portfolio",
        }.get(portfolio_state, portfolio_state)
        normalized_portfolio_state = (
            normalized_portfolio_state
            if normalized_portfolio_state in {"all", "portfolio", "non_portfolio"}
            else "all"
        )
        normalized_account = None if account_name in (None, "", "全部账户") else account_name
        normalized_search = str(search_term or "").strip()

        records = self.repository.list_records(full_report_only=full_report_only, limit=limit)
        assets_by_id, assets_by_symbol_account = self._build_asset_indexes()
        result: List[Dict] = []
        seen_keys = set()
        for record in records:
            item = self._build_view_model(
                record,
                assets_by_id=assets_by_id,
                assets_by_symbol_account=assets_by_symbol_account,
            )
            if normalized_scope != "all" and item["analysis_scope"] != normalized_scope:
                continue
            if normalized_portfolio_state == "portfolio" and not item.get("is_in_portfolio"):
                continue
            if normalized_portfolio_state == "non_portfolio" and item.get("is_in_portfolio"):
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
        if not record:
            return None
        assets_by_id, assets_by_symbol_account = self._build_asset_indexes()
        return self._build_view_model(
            record,
            assets_by_id=assets_by_id,
            assets_by_symbol_account=assets_by_symbol_account,
        )

    def delete_record(self, record_id: int) -> bool:
        return self.repository.delete_record(record_id)


analysis_history_service = AnalysisHistoryService()
