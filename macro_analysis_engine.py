"""
宏观分析板块 - 综合引擎
"""

from __future__ import annotations

import time
from typing import Any

from macro_analysis_agents import MacroAnalysisAgents
from macro_analysis_data import MacroAnalysisDataFetcher
from ui_shared import _split_analysis_report_sections


class MacroAnalysisEngine:
    """统筹宏观数据抓取、多智能体分析和结果组织"""

    def __init__(
        self,
        model: str | None = None,
        lightweight_model: str | None = None,
        reasoning_model: str | None = None,
    ) -> None:
        self.data_fetcher = MacroAnalysisDataFetcher()
        self.agents = MacroAnalysisAgents(
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )

    def run_full_analysis(self, progress_callback=None) -> dict[str, Any]:
        results: dict[str, Any] = {
            "success": False,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "raw_data": {},
            "agents_analysis": {},
            "sector_view": {},
            "stock_view": {},
            "candidate_stocks": [],
            "data_errors": [],
        }
        try:
            if progress_callback:
                progress_callback(5, "正在获取宏观数据...")
            raw_data = self.data_fetcher.fetch_all_data()
            results["raw_data"] = raw_data
            results["data_errors"] = raw_data.get("errors", [])
            context_text = self.data_fetcher.build_prompt_context(raw_data)

            if progress_callback:
                progress_callback(22, "宏观总量分析师正在研判...")
            macro_result = self.agents.macro_analyst_agent(context_text)

            if progress_callback:
                progress_callback(38, "政策流动性分析师正在研判...")
            policy_result = self.agents.policy_analyst_agent(context_text)

            if progress_callback:
                progress_callback(55, "行业映射分析师正在生成板块多空视图...")
            sector_result = self.agents.sector_mapper_agent(
                context_text=context_text,
                rule_view=raw_data.get("rule_based_sector_view", {}),
                sector_pool=list(self.data_fetcher.SECTOR_STOCK_POOLS.keys()),
            )
            sector_view = self._normalize_sector_view(
                sector_result.get("structured", {}),
                raw_data.get("rule_based_sector_view", {}),
            )

            bullish_sector_names = [item.get("sector", "") for item in sector_view.get("bullish_sectors", [])]
            candidate_stocks = self.data_fetcher.build_stock_candidates_for_sectors(bullish_sector_names)
            results["candidate_stocks"] = candidate_stocks

            if progress_callback:
                progress_callback(72, "优质标的分析师正在筛选候选股票...")
            stock_result = self.agents.stock_selector_agent(
                context_text=context_text,
                sector_view=sector_view,
                stock_candidates=candidate_stocks,
            )
            stock_view = self._normalize_stock_view(stock_result.get("structured", {}), candidate_stocks)

            if progress_callback:
                progress_callback(88, "首席策略官正在综合输出...")
            chief_result = self.agents.chief_strategist_agent(
                context_text=context_text,
                macro_report=macro_result.get("analysis", ""),
                policy_report=policy_result.get("analysis", ""),
                sector_view=sector_view,
                stock_view=stock_view,
            )

            results["agents_analysis"] = {
                "macro": macro_result,
                "policy": policy_result,
                "sector": sector_result,
                "stock": stock_result,
                "chief": chief_result,
            }
            results["sector_view"] = sector_view
            results["stock_view"] = stock_view
            results["success"] = True

            if progress_callback:
                progress_callback(100, "分析完成")
        except Exception as exc:
            results["error"] = str(exc)
        return results

    def _extract_chief_summary(self, results: dict[str, Any]) -> str:
        agents = results.get("agents_analysis", {})
        chief_analysis = agents.get("chief", {}).get("analysis", "")
        if not chief_analysis:
            return ""
        body, _reasoning = _split_analysis_report_sections(chief_analysis)
        normalized = str(body or chief_analysis).replace("\r\n", "\n").strip()
        for paragraph in normalized.split("\n\n"):
            text = paragraph.strip().lstrip("#*-> ")
            if len(text) >= 20:
                return text[:300]
        return normalized[:300]

    def _generate_report_summary(self, results: dict[str, Any]) -> str:
        chief_summary = self._extract_chief_summary(results)
        if chief_summary:
            return chief_summary
        agents = results.get("agents_analysis", {})
        available = [name for name, data in agents.items() if data.get("analysis")]
        if available:
            return f"宏观分析已完成，包含 {len(available)} 位分析师报告。"
        return "宏观分析报告"

    @staticmethod
    def _normalize_sector_view(ai_view: dict[str, Any], fallback_view: dict[str, Any]) -> dict[str, Any]:
        view = ai_view if isinstance(ai_view, dict) and ai_view else fallback_view
        return {
            "market_view": view.get("market_view", fallback_view.get("market_view", "结构性机会为主")),
            "bullish_sectors": view.get("bullish_sectors", fallback_view.get("bullish_sectors", [])),
            "bearish_sectors": view.get("bearish_sectors", fallback_view.get("bearish_sectors", [])),
            "watch_signals": view.get("watch_signals", fallback_view.get("watch_signals", [])),
        }

    @staticmethod
    def _normalize_stock_view(ai_view: dict[str, Any], candidate_stocks: list[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(ai_view, dict) or not ai_view:
            return {
                "recommended_stocks": candidate_stocks[:6],
                "watchlist": candidate_stocks[6:10],
            }

        candidate_map = {item["code"]: item for item in candidate_stocks}

        def merge_item(item: dict[str, Any]) -> dict[str, Any]:
            merged = candidate_map.get(item.get("code"), {}).copy()
            merged.update(item)
            return merged

        return {
            "recommended_stocks": [merge_item(item) for item in ai_view.get("recommended_stocks", [])],
            "watchlist": [merge_item(item) for item in ai_view.get("watchlist", [])],
        }
