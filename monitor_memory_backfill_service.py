from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

import config
from analysis_repository import AnalysisRepository, analysis_repository
from asset_repository import (
    STATUS_FOCUS,
    STATUS_HOLDING,
    AssetRepository,
    asset_repository,
)
from investment_action_utils import build_execution_plan, normalize_condition_list, normalize_strategy_context
from llm_client import LLMClient
from model_routing import ModelTier
from monitoring_repository import MonitoringRepository
from prompt_registry import build_messages, load_prompt_template

if TYPE_CHECKING:
    from agent_memory_service import AgentMemoryService


logger = logging.getLogger(__name__)

ProgressReporter = Callable[..., None]


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


class MonitorRelatedExecutionMemoryBackfillService:
    """Force-rebuild execution memories for stocks currently tied to monitoring."""

    REQUIRED_MONITOR_OPTIMIZATION_VERSION = getattr(
        config,
        "SMART_MONITOR_REQUIRED_OPTIMIZATION_VERSION",
        "execution_conditions_v1",
    )
    EXECUTION_PLAN_SYSTEM_TEMPLATE = "stock_analysis/execution_plan_extract.system.txt"
    EXECUTION_PLAN_USER_TEMPLATE = "stock_analysis/execution_plan_extract.user.txt"

    def __init__(
        self,
        *,
        analysis_store: Optional[AnalysisRepository] = None,
        asset_store: Optional[AssetRepository] = None,
        monitoring_store: Optional[MonitoringRepository] = None,
        memory_service: Optional["AgentMemoryService"] = None,
        llm_client: Optional[LLMClient] = None,
    ) -> None:
        self.analysis_store = analysis_store or analysis_repository
        self.asset_store = asset_store or asset_repository
        self.monitoring_store = monitoring_store or MonitoringRepository()
        self._memory_service = memory_service
        self.llm_client = llm_client or LLMClient(model=getattr(config, "LIGHTWEIGHT_MODEL_NAME", None))

    @property
    def memory_service(self) -> "AgentMemoryService":
        if self._memory_service is None:
            from agent_memory_service import agent_memory_service

            self._memory_service = agent_memory_service
        return self._memory_service

    def verify_monitor_optimization_ready(self) -> Dict[str, Any]:
        """Backfill is only allowed after monitor/memory/prompt execution gates are active."""
        checks: List[Dict[str, Any]] = []

        def _add(name: str, passed: bool, detail: str = "") -> None:
            checks.append({"name": name, "passed": bool(passed), "detail": detail})

        current_version = str(getattr(config, "SMART_MONITOR_OPTIMIZATION_VERSION", "") or "")
        _add(
            "smart_monitor_optimization_version",
            current_version == self.REQUIRED_MONITOR_OPTIMIZATION_VERSION,
            f"current={current_version or 'unset'}, required={self.REQUIRED_MONITOR_OPTIMIZATION_VERSION}",
        )

        try:
            strategy_template = load_prompt_template("smart_monitor/sections/strategy_context.txt")
            _add(
                "strategy_context_prompt_conditions",
                all(
                    token in strategy_template
                    for token in (
                        "$entry_conditions_text",
                        "$exit_conditions_text",
                        "$hold_conditions_text",
                        "$invalidation_conditions_text",
                    )
                ),
                "strategy context must render structured execution conditions",
            )
        except Exception as exc:
            _add("strategy_context_prompt_conditions", False, str(exc))

        try:
            decision_template = load_prompt_template("smart_monitor/intraday_decision.system.txt")
            _add(
                "intraday_prompt_condition_gate",
                "结构化条件才是执行门槛" in decision_template,
                "intraday prompt must state structured conditions are execution gates",
            )
        except Exception as exc:
            _add("intraday_prompt_condition_gate", False, str(exc))

        try:
            memory_template = load_prompt_template("stock_analysis/memory_extract.user.txt")
            _add(
                "memory_prompt_execution_plan",
                "$execution_plan" in memory_template,
                "memory extraction must include the structured execution plan",
            )
        except Exception as exc:
            _add("memory_prompt_execution_plan", False, str(exc))

        try:
            final_decision_template = load_prompt_template("stock_analysis/final_decision.system.txt")
            _add(
                "final_decision_prompt_schema",
                all(
                    token in final_decision_template
                    for token in (
                        "entry_conditions",
                        "exit_conditions",
                        "hold_conditions",
                        "invalidation_conditions",
                        "execution_plan_summary",
                    )
                ),
                "deep analysis final decision must request execution condition fields",
            )
        except Exception as exc:
            _add("final_decision_prompt_schema", False, str(exc))

        failed = [item for item in checks if not item["passed"]]
        return {
            "ready": not failed,
            "required_version": self.REQUIRED_MONITOR_OPTIMIZATION_VERSION,
            "checks": checks,
            "failed_checks": failed,
        }

    @staticmethod
    def _normalize_symbol(value: object) -> str:
        return str(value or "").strip().upper()

    @staticmethod
    def _merge_target(targets: Dict[str, Dict[str, Any]], *, symbol: str, name: str = "", reason: str = "", asset_id: Any = None) -> None:
        normalized_symbol = MonitorRelatedExecutionMemoryBackfillService._normalize_symbol(symbol)
        if not normalized_symbol:
            return
        target = targets.setdefault(
            normalized_symbol,
            {
                "symbol": normalized_symbol,
                "name": name or normalized_symbol,
                "asset_ids": [],
                "reasons": [],
            },
        )
        if name and target.get("name") in ("", normalized_symbol):
            target["name"] = name
        if asset_id not in (None, "") and int(asset_id) not in target["asset_ids"]:
            target["asset_ids"].append(int(asset_id))
        if reason and reason not in target["reasons"]:
            target["reasons"].append(reason)

    def list_monitor_related_targets(self, *, stock_code: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        targets: Dict[str, Dict[str, Any]] = {}

        for status, reason in ((STATUS_HOLDING, "holding_asset"), (STATUS_FOCUS, "watchlist_asset")):
            for asset in self.asset_store.list_assets(status=status, include_deleted=False):
                self._merge_target(
                    targets,
                    symbol=asset.get("symbol"),
                    name=asset.get("name") or asset.get("symbol"),
                    reason=reason,
                    asset_id=asset.get("id"),
                )

        for item in self.monitoring_store.list_items(monitor_type="ai_task", enabled_only=True):
            self._merge_target(
                targets,
                symbol=item.get("symbol"),
                name=item.get("name") or item.get("symbol"),
                reason="enabled_ai_task",
                asset_id=item.get("asset_id"),
            )

        for item in self.monitoring_store.list_items(monitor_type="price_alert", managed_by_portfolio=True, enabled_only=True):
            self._merge_target(
                targets,
                symbol=item.get("symbol"),
                name=item.get("name") or item.get("symbol"),
                reason="managed_price_alert",
                asset_id=item.get("asset_id") or item.get("portfolio_stock_id"),
            )

        result = sorted(targets.values(), key=lambda item: item["symbol"])
        normalized_filter = self._normalize_symbol(stock_code)
        if normalized_filter:
            result = [item for item in result if item["symbol"] == normalized_filter]
        if limit is not None:
            result = result[: max(0, int(limit))]
        return result

    @staticmethod
    def _extract_json_object(text: str) -> Dict[str, Any]:
        decoder = json.JSONDecoder()
        for index, char in enumerate(str(text or "")):
            if char != "{":
                continue
            try:
                payload, _ = decoder.raw_decode(str(text)[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload
        raise ValueError("execution_plan_json_not_found")

    @staticmethod
    def _normalize_execution_plan_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = payload if isinstance(payload, dict) else {}
        return {
            "entry_conditions": normalize_condition_list(payload.get("entry_conditions")),
            "exit_conditions": normalize_condition_list(payload.get("exit_conditions")),
            "hold_conditions": normalize_condition_list(payload.get("hold_conditions")),
            "invalidation_conditions": normalize_condition_list(payload.get("invalidation_conditions")),
            "execution_plan_summary": str(payload.get("execution_plan_summary") or "").strip()[:240],
        }

    @staticmethod
    def _fallback_execution_plan(record: Dict[str, Any]) -> Dict[str, Any]:
        final_decision = record.get("final_decision") if isinstance(record.get("final_decision"), dict) else {}
        existing = build_execution_plan(final_decision)
        source_text = " ".join(
            str(value or "")
            for value in (
                final_decision.get("operation_advice"),
                final_decision.get("risk_warning"),
                final_decision.get("summary"),
                record.get("summary"),
                record.get("discussion_result"),
            )
        )
        if not existing["entry_conditions"]:
            entry_candidates = re.findall(r"[^。；;\n]*(?:回踩|企稳|放量|突破|站稳|低吸|建仓|买入|加仓)[^。；;\n]*", source_text)
            existing["entry_conditions"] = normalize_condition_list(entry_candidates, max_items=5)
        if not existing["exit_conditions"]:
            exit_candidates = re.findall(r"[^。；;\n]*(?:止盈|止损|减仓|清仓|跌破|失守|转弱|离场|卖出|兑现)[^。；;\n]*", source_text)
            existing["exit_conditions"] = normalize_condition_list(exit_candidates, max_items=5)
        if not existing["hold_conditions"]:
            hold_candidates = re.findall(r"[^。；;\n]*(?:持有|观望|等待|跟踪|未破|趋势未坏)[^。；;\n]*", source_text)
            existing["hold_conditions"] = normalize_condition_list(hold_candidates, max_items=5)
        if not existing["invalidation_conditions"]:
            invalid_candidates = re.findall(r"[^。；;\n]*(?:基线失效|逻辑失效|风险扩大|趋势破坏|结构破坏|明确利空)[^。；;\n]*", source_text)
            existing["invalidation_conditions"] = normalize_condition_list(invalid_candidates, max_items=5)
        if not existing["execution_plan_summary"]:
            existing["execution_plan_summary"] = str(final_decision.get("operation_advice") or record.get("summary") or "").strip()[:240]
        return existing

    def extract_execution_plan(self, record: Dict[str, Any]) -> Dict[str, Any]:
        final_decision = record.get("final_decision") if isinstance(record.get("final_decision"), dict) else {}
        messages = build_messages(
            self.EXECUTION_PLAN_SYSTEM_TEMPLATE,
            self.EXECUTION_PLAN_USER_TEMPLATE,
            stock_name=record.get("stock_name") or record.get("symbol") or "",
            stock_code=record.get("symbol") or "",
            analysis_date=record.get("analysis_date") or "",
            rating=record.get("rating") or final_decision.get("rating") or "",
            final_decision_json=_json_dumps(final_decision),
            summary=record.get("summary") or "",
            discussion_summary=str(record.get("discussion_result") or "")[:5000],
        )
        try:
            response = self.llm_client.call_api(
                messages,
                max_tokens=1200,
                sampling_profile="factual",
                tier=ModelTier.LIGHTWEIGHT,
            )
            return self._normalize_execution_plan_payload(self._extract_json_object(response))
        except Exception as exc:
            logger.warning("Execution-plan extraction fell back for record %s: %s", record.get("id"), exc)
            return self._fallback_execution_plan(record)

    @staticmethod
    def _record_has_execution_plan(record: Dict[str, Any]) -> bool:
        final_decision = record.get("final_decision") if isinstance(record.get("final_decision"), dict) else {}
        plan = build_execution_plan(final_decision)
        return any(plan.get(key) for key in ("entry_conditions", "exit_conditions", "hold_conditions", "invalidation_conditions"))

    def _enrich_record_execution_plan(self, record: Dict[str, Any], *, apply: bool, force: bool) -> Dict[str, Any]:
        if not force and self._record_has_execution_plan(record):
            return {"record_id": record.get("id"), "updated": False, "skipped": True, "reason": "execution_plan_exists"}

        plan = self.extract_execution_plan(record)
        final_decision = dict(record.get("final_decision") or {})
        final_decision.update(plan)
        final_decision["execution_plan"] = plan
        if apply:
            self.analysis_store.update_record_final_decision(int(record["id"]), final_decision)
        return {
            "record_id": record.get("id"),
            "updated": bool(apply),
            "skipped": False,
            "execution_plan": plan,
        }

    def _sync_monitor_strategy_context(self, symbol: str) -> int:
        latest_context = self.analysis_store.get_latest_strategy_context(symbol=symbol)
        if not latest_context:
            return 0
        normalized_context = normalize_strategy_context(latest_context)
        changed = 0
        for item in [
            *self.monitoring_store.list_items(monitor_type="ai_task", symbol=symbol),
            *self.monitoring_store.list_items(monitor_type="price_alert", symbol=symbol, managed_by_portfolio=True),
        ]:
            config_payload = dict(item.get("config") or {})
            config_payload["strategy_context"] = normalized_context
            if normalized_context.get("origin_analysis_id"):
                config_payload["origin_analysis_id"] = normalized_context.get("origin_analysis_id")
            if self.monitoring_store.update_item(int(item["id"]), {"config": config_payload}):
                changed += 1
        return changed

    def backfill_symbol(self, target: Dict[str, Any], *, apply: bool, force: bool, compress_after: bool) -> Dict[str, Any]:
        symbol = self._normalize_symbol(target.get("symbol"))
        records = sorted(
            self.analysis_store.list_records(symbol=symbol, full_report_only=True),
            key=lambda item: (str(item.get("analysis_date") or ""), int(item.get("id") or 0)),
        )
        record_results = []
        for record in records:
            record_results.append(self._enrich_record_execution_plan(record, apply=apply, force=force))

        memory_result: Dict[str, Any] = {}
        synced_items = 0
        if apply:
            memory_result = self.memory_service.backfill_from_analysis_history(
                symbol,
                clear_existing=True,
                compress_after=compress_after,
            )
            synced_items = self._sync_monitor_strategy_context(symbol)

        return {
            "symbol": symbol,
            "name": target.get("name") or symbol,
            "reasons": target.get("reasons") or [],
            "record_count": len(records),
            "records_updated": sum(1 for item in record_results if item.get("updated")),
            "records_skipped": sum(1 for item in record_results if item.get("skipped")),
            "memory": memory_result,
            "synced_monitor_items": synced_items,
            "record_results": record_results[:20],
        }

    def run(
        self,
        *,
        apply: bool = False,
        workers: Optional[int] = None,
        limit: Optional[int] = None,
        stock_code: Optional[str] = None,
        force: Optional[bool] = None,
        compress_after: Optional[bool] = None,
        progress: Optional[ProgressReporter] = None,
    ) -> Dict[str, Any]:
        force = bool(getattr(config, "MEMORY_BACKFILL_FORCE_OVERWRITE", True) if force is None else force)
        compress_after = bool(getattr(config, "MEMORY_BACKFILL_COMPRESS_AFTER", True) if compress_after is None else compress_after)
        worker_count = max(1, int(workers or getattr(config, "MEMORY_BACKFILL_WORKERS", 2) or 2))
        readiness = self.verify_monitor_optimization_ready()
        targets = self.list_monitor_related_targets(stock_code=stock_code, limit=limit)

        if not apply:
            return {
                "apply": False,
                "target_total": len(targets),
                "targets": targets,
                "force": force,
                "compress_after": compress_after,
                "optimization_readiness": readiness,
            }

        if not readiness["ready"]:
            failed_names = ", ".join(item["name"] for item in readiness["failed_checks"])
            raise RuntimeError(f"智能盯盘、记忆模式与Prompt优化未完成，禁止历史回填: {failed_names}")

        summary: Dict[str, Any] = {
            "apply": True,
            "target_total": len(targets),
            "success_total": 0,
            "failed_total": 0,
            "record_total": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "synced_monitor_items": 0,
            "force": force,
            "compress_after": compress_after,
            "optimization_readiness": readiness,
            "results": [],
            "failed": [],
        }
        if progress:
            progress(total=len(targets), current=0, message="开始回填盯盘相关历史报告")

        def _run_one(target: Dict[str, Any]) -> Dict[str, Any]:
            return self.backfill_symbol(target, apply=True, force=force, compress_after=compress_after)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(_run_one, target): target for target in targets}
            completed = 0
            for future in as_completed(future_map):
                target = future_map[future]
                completed += 1
                try:
                    result = future.result()
                    summary["success_total"] += 1
                    summary["record_total"] += int(result.get("record_count") or 0)
                    summary["records_updated"] += int(result.get("records_updated") or 0)
                    summary["records_skipped"] += int(result.get("records_skipped") or 0)
                    summary["synced_monitor_items"] += int(result.get("synced_monitor_items") or 0)
                    summary["results"].append(result)
                    message = f"已完成 {result.get('symbol')}，报告 {result.get('record_count', 0)} 条"
                except Exception as exc:
                    summary["failed_total"] += 1
                    summary["failed"].append({"symbol": target.get("symbol"), "error": str(exc)})
                    message = f"{target.get('symbol')} 回填失败: {exc}"
                    logger.exception("Monitor-related memory backfill failed for %s", target.get("symbol"))
                if progress:
                    progress(current=completed, total=len(targets), message=message)

        summary["message"] = (
            f"盯盘相关历史报告回填完成：成功 {summary['success_total']}，失败 {summary['failed_total']}，"
            f"更新报告 {summary['records_updated']} 条"
        )
        return summary


class _LazyMonitorRelatedExecutionMemoryBackfillService:
    def __init__(self) -> None:
        self._service: Optional[MonitorRelatedExecutionMemoryBackfillService] = None

    def _get(self) -> MonitorRelatedExecutionMemoryBackfillService:
        if self._service is None:
            self._service = MonitorRelatedExecutionMemoryBackfillService()
        return self._service

    def __getattr__(self, name: str) -> Any:
        return getattr(self._get(), name)


monitor_related_execution_memory_backfill_service = _LazyMonitorRelatedExecutionMemoryBackfillService()
