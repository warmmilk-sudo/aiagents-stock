"""
智策综合研判引擎
整合各智能体分析，生成板块多空/轮动/热度预测
"""

import concurrent.futures
import json
import logging
import re
from typing import Any, Callable, Dict, Optional

from sector_strategy_agents import SectorStrategyAgents
from sector_strategy_db import SectorStrategyDatabase
from deepseek_client import DeepSeekClient
from model_routing import ModelTier
from prompt_registry import build_messages
from sector_strategy_normalization import (
    DEFAULT_INVESTMENT_HORIZON,
    build_sector_strategy_summary,
    derive_sector_strategy_investment_horizon,
    derive_sector_strategy_recommended_sectors,
    normalize_sector_strategy_predictions,
    normalize_sector_strategy_result,
)
from time_utils import local_now_str, local_today_str


class SectorStrategyEngine:
    """板块策略综合研判引擎"""
    
    def __init__(self, model=None, lightweight_model=None, reasoning_model=None):
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.agents = SectorStrategyAgents(
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        self.deepseek_client = DeepSeekClient(
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        self.database = SectorStrategyDatabase()
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
        print(f"[智策引擎] 初始化完成 (模型配置: {self.deepseek_client.model_selection})")

    def _report_stage_progress(
        self,
        progress_callback: Optional[Callable[[int, int, str], None]],
        current: int,
        total: int,
        message: str,
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(current, total, message)
    
    def run_comprehensive_analysis(
        self,
        data: Dict,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """
        运行综合分析流程
        
        Args:
            data: 包含市场数据的字典
            
        Returns:
            完整的分析结果
        """
        print("\n" + "=" * 60)
        print("🚀 智策综合分析系统启动")
        print("=" * 60)
        
        results = {
            "success": False,
            "timestamp": local_now_str(),
            "agents_analysis": {},
            "comprehensive_report": "",
            "final_predictions": {}
        }
        
        try:
            # 1. 运行四个AI智能体分析
            print("\n[阶段1] AI智能体分析集群工作中...")
            print("-" * 60)
            self._report_stage_progress(progress_callback, 25, 100, "AI 分析师团队正在分析板块与市场...")

            agent_tasks = {
                "macro": (
                    "宏观策略师",
                    lambda: self.agents.macro_strategist_agent(
                        market_data=data.get("market_overview", {}),
                        news_data=data.get("news", []),
                        analysis_date=str(data.get("timestamp") or local_now_str()),
                    ),
                ),
                "sector": (
                    "板块诊断师",
                    lambda: self.agents.sector_diagnostician_agent(
                        sectors_data=data.get("sectors", {}),
                        concepts_data=data.get("concepts", {}),
                        market_data=data.get("market_overview", {}),
                        analysis_date=str(data.get("timestamp") or local_now_str()),
                    ),
                ),
                "fund": (
                    "资金流向分析师",
                    lambda: self.agents.fund_flow_analyst_agent(
                        fund_flow_data=data.get("sector_fund_flow", {}),
                        north_flow_data=data.get("north_flow", {}),
                        sectors_data=data.get("sectors", {}),
                        analysis_date=str(data.get("timestamp") or local_now_str()),
                    ),
                ),
                "sentiment": (
                    "市场情绪解码员",
                    lambda: self.agents.market_sentiment_decoder_agent(
                        market_data=data.get("market_overview", {}),
                        sectors_data=data.get("sectors", {}),
                        concepts_data=data.get("concepts", {}),
                        analysis_date=str(data.get("timestamp") or local_now_str()),
                    ),
                ),
            }

            agents_results = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(agent_tasks)) as executor:
                future_map = {}
                for key, (label, task_fn) in agent_tasks.items():
                    print(f"- {label}...")
                    future_map[executor.submit(task_fn)] = key

                for future in concurrent.futures.as_completed(future_map):
                    key = future_map[future]
                    agents_results[key] = future.result()
            agents_results["_analysis_date"] = str(data.get("timestamp") or local_now_str())

            results["agents_analysis"] = agents_results
            print("\n✓ 所有智能体分析完成")
            
            # 2. 综合研判
            print("\n[阶段2] 综合研判引擎工作中...")
            print("-" * 60)
            self._report_stage_progress(progress_callback, 75, 100, "AI 团队正在进行综合讨论...")
            comprehensive_report = self._conduct_comprehensive_discussion(agents_results)
            results["comprehensive_report"] = comprehensive_report
            print("✓ 综合研判完成")
            
            # 3. 生成最终预测
            print("\n[阶段3] 生成最终预测...")
            print("-" * 60)
            self._report_stage_progress(progress_callback, 90, 100, "正在生成智策最终决策...")
            predictions = self._generate_final_predictions(comprehensive_report, agents_results, data)
            results["final_predictions"] = predictions
            print("✓ 预测生成完成")
            
            results["success"] = True
            
            # 4. 保存分析报告
            print("\n[阶段4] 保存分析报告...")
            print("-" * 60)
            try:
                report_id = self.save_analysis_report(results, data)
                results["report_id"] = report_id
                print(f"✓ 分析报告已保存 (ID: {report_id})")
                # 保存后读取报告详情并回传到结果，用于主页面动态渲染
                try:
                    saved_report = self.database.get_analysis_report(report_id)
                    if saved_report:
                        results["saved_report"] = saved_report
                except Exception as fetch_e:
                    self.logger.warning(f"[智策引擎] 获取保存报告详情失败: {fetch_e}")
            except Exception as e:
                print(f"⚠ 保存分析报告失败: {e}")
                self.logger.error(f"[智策引擎] 保存分析报告失败: {e}")
            
            print("\n" + "=" * 60)
            print("✓ 智策综合分析完成！")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n✗ 分析过程出错: {e}")
            import traceback
            traceback.print_exc()
            results["error"] = str(e)
        
        return results
    
    def _conduct_comprehensive_discussion(self, agents_results: Dict) -> str:
        """
        综合研判 - 整合各智能体的分析
        """
        print("  🤝 智能体团队正在综合讨论...")
        
        # 收集各分析师的报告
        macro_analysis = agents_results.get("macro", {}).get("analysis", "")
        sector_analysis = agents_results.get("sector", {}).get("analysis", "")
        fund_analysis = agents_results.get("fund", {}).get("analysis", "")
        sentiment_analysis = agents_results.get("sentiment", {}).get("analysis", "")
        
        analysis_date = str(agents_results.get("_analysis_date") or local_now_str())
        messages = build_messages(
            "sector_strategy/comprehensive_discussion.system.txt",
            "sector_strategy/comprehensive_discussion.user.txt",
            analysis_date=analysis_date,
            macro_analysis=macro_analysis,
            sector_analysis=sector_analysis,
            fund_analysis=fund_analysis,
            sentiment_analysis=sentiment_analysis,
        )
        
        report = self.deepseek_client.call_api(
            messages,
            max_tokens=5000,
            tier=ModelTier.REASONING,
        )
        report = self._enforce_text_time_freshness(
            text=report,
            reference_date=analysis_date,
            label="综合研判",
            context_text="\n\n".join(
                part for part in (macro_analysis, sector_analysis, fund_analysis, sentiment_analysis) if str(part or "").strip()
            ),
        )
        
        print("  ✓ 综合研判完成")
        return report
    
    def _generate_final_predictions(self, comprehensive_report: str, agents_results: Dict, raw_data: Dict) -> Dict:
        """
        生成最终预测 - 板块多空/轮动/热度
        """
        print("  📊 生成板块多空/轮动/热度预测...")
        
        # 提取板块列表用于预测
        sectors_list = []
        if raw_data.get("sectors"):
            sorted_sectors = sorted(raw_data["sectors"].items(), key=lambda x: abs(x[1]["change_pct"]), reverse=True)
            sectors_list = [name for name, _ in sorted_sectors[:30]]  # 取前30个活跃板块
        
        sectors_str = ", ".join(sectors_list) if sectors_list else "未知板块"
        analysis_date = str(raw_data.get("timestamp") or local_now_str())

        messages = build_messages(
            "sector_strategy/final_predictions.system.txt",
            "sector_strategy/final_predictions.user.txt",
            analysis_date=analysis_date,
            comprehensive_report=comprehensive_report,
            sectors_str=sectors_str,
        )
        
        response = self.deepseek_client.call_api(
            messages,
            temperature=0.3,
            max_tokens=6000,
            tier=ModelTier.REASONING,
        )
        if self._contains_stale_year_reference(str(response or ""), analysis_date):
            print("  ⚠ 最终预测出现过期年份引用，尝试修复输出")
            response = self._repair_prediction_response(
                raw_response=response,
                comprehensive_report=comprehensive_report,
                sectors_str=sectors_str,
                analysis_date=analysis_date,
            )

        parsed = self._parse_prediction_json(response)
        if self._prediction_payload_is_valid(parsed) and not self._payload_contains_stale_year_reference(parsed, analysis_date):
            print("  ✓ 预测报告生成成功（JSON格式）")
            return self._build_prediction_storage_payload(parsed)

        print("  ⚠ 初次预测结构不完整或存在过期年份，尝试修复输出")
        repaired_response = self._repair_prediction_response(
            raw_response=response,
            comprehensive_report=comprehensive_report,
            sectors_str=sectors_str,
            analysis_date=analysis_date,
        )
        repaired = self._parse_prediction_json(repaired_response)
        if self._prediction_payload_is_valid(repaired) and not self._payload_contains_stale_year_reference(repaired, analysis_date):
            print("  ✓ 修复后的预测报告解析成功")
            return self._build_prediction_storage_payload(repaired)

        print("  ⚠ 预测结构仍不完整，回退到默认结构")
        return self._build_prediction_storage_payload({}, raw_text=response)

    def _parse_prediction_json(self, response_text: Any) -> Dict[str, Any]:
        text = str(response_text or "").strip()
        if not text:
            return {}
        json_match = re.search(r"\{[\s\S]*\}", text)
        if not json_match:
            return {}
        try:
            parsed = json.loads(json_match.group(0))
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _prediction_payload_is_valid(self, payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict) or not payload:
            return False
        normalized = normalize_sector_strategy_predictions(payload)
        return not normalized.get("warnings", {}).get("missing_fields")

    def _extract_year_tokens(self, text: Any) -> set[str]:
        return set(re.findall(r"20\d{2}", str(text or "")))

    def _contains_stale_year_reference(self, text: Any, reference_date: str) -> bool:
        content = str(text or "").strip()
        if not content or not reference_date:
            return False
        allowed_years = self._extract_year_tokens(reference_date)
        if not allowed_years:
            return False
        return bool({year for year in self._extract_year_tokens(content) if year not in allowed_years})

    def _payload_contains_stale_year_reference(self, payload: Any, reference_date: str) -> bool:
        if isinstance(payload, dict):
            return any(self._payload_contains_stale_year_reference(value, reference_date) for value in payload.values())
        if isinstance(payload, list):
            return any(self._payload_contains_stale_year_reference(item, reference_date) for item in payload)
        if isinstance(payload, str):
            return self._contains_stale_year_reference(payload, reference_date)
        return False

    def _enforce_text_time_freshness(self, *, text: str, reference_date: str, label: str, context_text: str) -> str:
        if not self._contains_stale_year_reference(text, reference_date):
            return text
        print(f"  ⚠ {label}出现过期年份引用，按 {reference_date} 重新修正")
        messages = build_messages(
            "sector_strategy/repair_time_freshness_generic.system.txt",
            "sector_strategy/repair_time_freshness_generic.user.txt",
            agent_label=label,
            reference_date=reference_date,
            context_text=context_text or "暂无额外上下文",
            raw_analysis=text,
        )
        repaired = self.deepseek_client.call_api(
            messages,
            temperature=0.1,
            max_tokens=5000,
            tier=ModelTier.REASONING,
        )
        if self._contains_stale_year_reference(repaired, reference_date):
            print(f"  ⚠ {label}修正后仍含过期年份，保留原始输出")
            return text
        return repaired

    def _repair_prediction_response(self, raw_response: str, comprehensive_report: str, sectors_str: str, analysis_date: str) -> str:
        messages = build_messages(
            "sector_strategy/repair_prediction.system.txt",
            "sector_strategy/repair_prediction.user.txt",
            analysis_date=analysis_date,
            comprehensive_report=comprehensive_report,
            sectors_str=sectors_str,
            raw_response=raw_response,
        )
        return self.deepseek_client.call_api(
            messages,
            temperature=0.1,
            max_tokens=5000,
            tier=ModelTier.REASONING,
        )

    def _build_prediction_storage_payload(self, payload: Dict[str, Any], raw_text: str = "") -> Dict[str, Any]:
        normalized = normalize_sector_strategy_predictions(payload)
        storage_payload = {
            "long_short": normalized.get("long_short", {}),
            "rotation": normalized.get("rotation", {}),
            "heat": normalized.get("heat", {}),
            "summary": normalized.get("summary", {}),
            "confidence_score": normalized.get("confidence_score", 0),
            "risk_level": normalized.get("risk_level", "中等"),
            "market_outlook": normalized.get("market_outlook", "中性"),
        }
        warnings = normalized.get("warnings", {})
        if warnings.get("missing_fields") or warnings.get("language_warning") or warnings.get("parse_warning"):
            storage_payload["warnings"] = warnings
        fallback_text = raw_text.strip() or normalized.get("raw_fallback_text", "")
        if fallback_text:
            storage_payload["prediction_text"] = fallback_text
        return storage_payload
    
    def save_analysis_report(self, results: Dict, original_data: Dict) -> int:
        """
        保存分析报告到数据库
        
        Args:
            results: 分析结果
            original_data: 原始数据
            
        Returns:
            int: 报告ID
        """
        try:
            # 提取数据日期范围
            data_date_range = f"{local_today_str()} 数据分析"
            analysis_payload = dict(results)
            analysis_payload["data_summary"] = {
                "from_cache": bool(original_data.get("from_cache")),
                "cache_warning": str(original_data.get("cache_warning") or ""),
                "data_timestamp": str(original_data.get("timestamp") or ""),
                "market_overview": original_data.get("market_overview", {}) or {},
                "sectors": original_data.get("sectors", {}) or {},
                "concepts": original_data.get("concepts", {}) or {},
            }

            report_view = normalize_sector_strategy_result(analysis_payload)
            summary_data = build_sector_strategy_summary(report_view)
            recommended_sectors = derive_sector_strategy_recommended_sectors(report_view)
            summary = summary_data.get("headline", "智策板块分析报告")
            confidence_score = summary_data.get("confidence_score", 0)
            risk_level = summary_data.get("risk_level", "中等")
            investment_horizon = derive_sector_strategy_investment_horizon(report_view)
            market_outlook = summary_data.get("market_outlook", "中性")
            
            # 保存到数据库
            report_id = self.database.save_analysis_report(
                data_date_range=data_date_range,
                analysis_content=analysis_payload,
                recommended_sectors=recommended_sectors,
                summary=summary,
                confidence_score=confidence_score,
                risk_level=risk_level,
                investment_horizon=investment_horizon,
                market_outlook=market_outlook
            )
            
            return report_id
            
        except Exception as e:
            self.logger.error(f"[智策引擎] 保存分析报告失败: {e}")
            raise

    def _derive_report_value(self, results: Dict, derive_fn, default):
        try:
            report_view = normalize_sector_strategy_result(results)
            value = derive_fn(report_view)
        except Exception:
            return default
        return default if value is None else value
    
    def _generate_report_summary(self, results: Dict) -> str:
        """生成报告摘要"""
        return self._derive_report_value(
            results,
            lambda report_view: build_sector_strategy_summary(report_view).get("headline", "智策板块分析报告"),
            "智策板块分析报告",
        )
    
    def _extract_confidence_score(self, results: Dict) -> float:
        """提取置信度分数"""
        return self._derive_report_value(
            results,
            lambda report_view: float(build_sector_strategy_summary(report_view).get("confidence_score", 0)),
            0.0,
        )
    
    def _extract_risk_level(self, results: Dict) -> str:
        """提取风险等级"""
        return self._derive_report_value(
            results,
            lambda report_view: build_sector_strategy_summary(report_view).get("risk_level", "中等"),
            "中等",
        )
    
    def _extract_investment_horizon(self, results: Dict) -> str:
        """提取投资周期"""
        return self._derive_report_value(
            results,
            derive_sector_strategy_investment_horizon,
            DEFAULT_INVESTMENT_HORIZON,
        )
    
    def _extract_market_outlook(self, results: Dict) -> str:
        """提取市场展望"""
        return self._derive_report_value(
            results,
            lambda report_view: build_sector_strategy_summary(report_view).get("market_outlook", "中性"),
            "中性",
        )
    
    def get_historical_reports(self, limit=10):
        """获取历史报告"""
        return self.database.get_analysis_reports(limit)
    
    def get_report_detail(self, report_id):
        """获取报告详情"""
        return self.database.get_analysis_report(report_id)
    
    def delete_report(self, report_id):
        """删除报告"""
        return self.database.delete_analysis_report(report_id)


# 测试函数
if __name__ == "__main__":
    print("=" * 60)
    print("测试智策综合研判引擎")
    print("=" * 60)
    
    # 创建模拟数据
    test_data = {
        "success": True,
        "sectors": {
            "电子": {"change_pct": 2.5, "turnover": 3.5, "top_stock": "某某科技", "top_stock_change": 5.0, "up_count": 80, "down_count": 20},
            "计算机": {"change_pct": 1.8, "turnover": 4.0, "top_stock": "某某软件", "top_stock_change": 4.5, "up_count": 70, "down_count": 30}
        },
        "market_overview": {
            "sh_index": {"close": 3200, "change_pct": 0.5},
            "total_stocks": 5000,
            "up_count": 3000,
            "up_ratio": 60.0
        },
        "news": [
            {"title": "测试新闻", "content": "测试内容", "publish_time": "2024-01-15"}
        ],
        "sector_fund_flow": {
            "today": [
                {"sector": "电子", "main_net_inflow": 100000, "main_net_inflow_pct": 2.0, "change_pct": 2.5, "super_large_net_inflow": 50000}
            ]
        },
        "north_flow": {
            "date": "2024-01-15",
            "north_net_inflow": 50000
        }
    }
    
    engine = SectorStrategyEngine()
    
    print("\n开始综合分析...")
    # 注意：这只是测试框架，实际运行需要真实数据和API key
    # results = engine.run_comprehensive_analysis(test_data)
    # print(f"\n分析结果: {results.get('success')}")
