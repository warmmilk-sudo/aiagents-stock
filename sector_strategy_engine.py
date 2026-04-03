"""
智策综合研判引擎
整合各智能体分析，生成板块多空/轮动/热度预测
"""

import concurrent.futures
import json
import logging
import re
from typing import Any, Callable, Dict, Optional

import pandas as pd

from sector_strategy_agents import SectorStrategyAgents
from sector_strategy_db import SectorStrategyDatabase
from deepseek_client import DeepSeekClient
from model_routing import ModelTier
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
    
    def save_raw_data_with_fallback(self, data_type, data_df, data_date=None):
        """
        保存原始数据，支持失败回退机制
        
        Args:
            data_type: 数据类型
            data_df: 数据DataFrame
            data_date: 数据日期，默认为今天
            
        Returns:
            tuple: (success, version, message)
        """
        if data_date is None:
            data_date = local_today_str()
        
        try:
            is_empty = False
            if data_df is None:
                is_empty = True
            elif hasattr(data_df, 'empty'):
                is_empty = data_df.empty
            elif isinstance(data_df, (list, tuple, set, dict)):
                is_empty = len(data_df) == 0
            if is_empty:
                self.logger.warning(f"[智策引擎] {data_type}数据为空，跳过保存")
                return False, None, "数据为空"
            
            version = self.database.save_raw_data(data_date, data_type, data_df)
            return True, version, f"保存成功，版本: {version}"
            
        except Exception as e:
            self.logger.error(f"[智策引擎] 保存{data_type}数据失败: {e}")
            return False, None, str(e)
    
    def get_data_with_fallback(self, data_type, data_date=None):
        """
        获取数据，支持失败时回退到历史数据
        
        Args:
            data_type: 数据类型
            data_date: 数据日期，默认为今天
            
        Returns:
            tuple: (data_df, is_fallback, message)
        """
        if data_date is None:
            data_date = local_today_str()
        
        try:
            # 尝试获取指定日期的数据
            data_df = self.database.get_latest_data(data_type, data_date)
            
            if not data_df.empty:
                return data_df, False, f"获取{data_date}数据成功"
            
            # 如果指定日期没有数据，获取最新的历史数据
            self.logger.warning(f"[智策引擎] {data_date}的{data_type}数据不存在，尝试获取历史数据")
            data_df = self.database.get_latest_data(data_type)
            
            if not data_df.empty:
                fallback_date = data_df.iloc[0].get('data_date', '未知日期')
                return data_df, True, f"回退到{fallback_date}的历史数据"
            else:
                return pd.DataFrame(), True, "无可用的历史数据"
                
        except Exception as e:
            self.logger.error(f"[智策引擎] 获取{data_type}数据失败: {e}")
            return pd.DataFrame(), True, str(e)
    
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
                    ),
                ),
                "sector": (
                    "板块诊断师",
                    lambda: self.agents.sector_diagnostician_agent(
                        sectors_data=data.get("sectors", {}),
                        concepts_data=data.get("concepts", {}),
                        market_data=data.get("market_overview", {}),
                    ),
                ),
                "fund": (
                    "资金流向分析师",
                    lambda: self.agents.fund_flow_analyst_agent(
                        fund_flow_data=data.get("sector_fund_flow", {}),
                        north_flow_data=data.get("north_flow", {}),
                        sectors_data=data.get("sectors", {}),
                    ),
                ),
                "sentiment": (
                    "市场情绪解码员",
                    lambda: self.agents.market_sentiment_decoder_agent(
                        market_data=data.get("market_overview", {}),
                        sectors_data=data.get("sectors", {}),
                        concepts_data=data.get("concepts", {}),
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
        
        prompt = f"""
你是智策系统的首席策略官，现在需要综合四位专业分析师的报告，形成全面的市场和板块研判。

【宏观策略师报告】
{macro_analysis}

【板块诊断师报告】
{sector_analysis}

【资金流向分析师报告】
{fund_analysis}

【市场情绪解码员报告】
{sentiment_analysis}

请基于以上四位分析师的专业报告，进行深度综合研判：

1. **观点一致性分析**
   - 四位分析师的核心观点有哪些一致之处？
   - 在哪些方面存在分歧或不同看法？
   - 如何理解这些分歧的合理性？

2. **多维度交叉验证**
   - 宏观环境、板块基本面、资金流向、市场情绪是否形成共振？
   - 哪些板块得到了多维度的支持？
   - 哪些板块存在多维度的风险信号？

3. **关键矛盾识别**
   - 当前市场和板块的主要矛盾是什么？
   - 哪些因素可能成为决定性因素？
   - 如何平衡不同维度的分析结论？

4. **综合判断**
   - 基于四个维度的综合分析，对市场整体趋势的判断
   - 对板块轮动方向的判断
   - 对市场风险收益比的评估
   - 当前最值得把握的机会在哪里？

5. **策略权重建议**
   - 在当前环境下，四个分析维度的重要性权重（宏观/板块/资金/情绪）
   - 应该重点参考哪个维度的建议？
   - 需要警惕哪个维度的风险？

输出要求：
1. 最终答案必须使用简体中文。
2. 不要输出英文标题、英文标签、英文解释。
3. 结论要清晰分段，便于后续解析展示。

请给出专业、全面的综合研判报告，体现多维度分析的价值。
"""
        
        messages = [
            {"role": "system", "content": "你是智策系统的首席策略官，需要整合多维度分析，形成全面的投资策略。最终输出必须使用简体中文。"},
            {"role": "user", "content": prompt}
        ]
        
        report = self.deepseek_client.call_api(
            messages,
            max_tokens=5000,
            tier=ModelTier.REASONING,
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
        
        prompt = f"""
基于前期的深度分析和综合研判，现在需要生成最终的板块预测报告。

【综合研判结论】
{comprehensive_report}

【参考板块列表】
{sectors_str}

请生成以下三类预测，并以JSON格式输出：

1. **板块多空情况**
   - 看多板块（5-8个）：综合判断未来1-2周看涨的板块
   - 看空板块（3-5个）：综合判断未来1-2周看跌的板块
   - 中性板块（2-3个）：走势不明朗的板块
   
   对每个板块给出：
   - 板块名称
   - 多空判断（看多/看空/中性）
   - 推荐理由（100字以内）
   - 信心度（1-10分）
   - 风险提示

2. **板块轮动预测**
   - 当前强势板块（正在走强的2-3个板块）
   - 潜力接力板块（可能轮动到的3-5个板块）
   - 衰退板块（正在走弱的2-3个板块）
   
   对每个板块给出：
   - 板块名称
   - 轮动阶段（强势/潜力/衰退）
   - 轮动逻辑（150字以内）
   - 预计时间窗口
   - 操作建议

3. **板块热度排行**
   - 最热板块TOP5（综合资金、情绪、涨幅）
   - 升温板块TOP5（热度快速上升的板块）
   - 降温板块TOP3（热度快速下降的板块）
   
   对每个板块给出：
   - 板块名称
   - 热度评分（0-100分）
   - 热度变化趋势（升温/降温/稳定）
   - 持续性评估（强/中/弱）

请严格按照以下JSON格式输出：
{{
    "long_short": {{
        "bullish": [
            {{
                "sector": "板块名称",
                "direction": "看多",
                "reason": "推荐理由",
                "confidence": 8,
                "risk": "风险提示"
            }}
        ],
        "bearish": [...],
        "neutral": [...]
    }},
    "rotation": {{
        "current_strong": [
            {{
                "sector": "板块名称",
                "stage": "强势",
                "logic": "轮动逻辑",
                "time_window": "1-2周",
                "advice": "操作建议"
            }}
        ],
        "potential": [...],
        "declining": [...]
    }},
    "heat": {{
        "hottest": [
            {{
                "sector": "板块名称",
                "score": 95,
                "trend": "升温",
                "sustainability": "强"
            }}
        ],
        "heating": [...],
        "cooling": [...]
    }},
    "summary": {{
        "market_view": "市场整体看法",
        "key_opportunity": "核心机会",
        "major_risk": "主要风险",
        "strategy": "整体策略建议"
    }},
    "confidence_score": 78,
    "risk_level": "中等",
    "market_outlook": "中性"
}}

注意：
1. 所有板块名称必须从参考板块列表中选择
2. 分析要基于前期的多维度研判
3. 给出的建议要具体、可操作
4. 预测要客观、理性，避免过度乐观或悲观
5. 只输出一个 JSON 对象，不要输出 Markdown、代码块、额外说明文字
6. JSON key 必须严格使用上述英文 schema
7. 所有 value 必须使用简体中文，不能出现英文标题、英文解释
8. confidence_score 使用 0-100 的整数
"""
        
        messages = [
            {"role": "system", "content": "你是智策系统的预测引擎，需要输出严格可解析的 JSON。JSON key 使用英文，所有 value 必须使用简体中文。禁止输出代码块和额外说明。"},
            {"role": "user", "content": prompt}
        ]
        
        response = self.deepseek_client.call_api(
            messages,
            temperature=0.3,
            max_tokens=6000,
            tier=ModelTier.REASONING,
        )
        
        parsed = self._parse_prediction_json(response)
        if self._prediction_payload_is_valid(parsed):
            print("  ✓ 预测报告生成成功（JSON格式）")
            return self._build_prediction_storage_payload(parsed)

        print("  ⚠ 初次预测结构不完整，尝试修复输出")
        repaired_response = self._repair_prediction_response(
            raw_response=response,
            comprehensive_report=comprehensive_report,
            sectors_str=sectors_str,
        )
        repaired = self._parse_prediction_json(repaired_response)
        if self._prediction_payload_is_valid(repaired):
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

    def _repair_prediction_response(self, raw_response: str, comprehensive_report: str, sectors_str: str) -> str:
        repair_prompt = f"""
你需要把下面这段输出修复成一个严格合法的 JSON 对象。

【综合研判结论】
{comprehensive_report}

【参考板块列表】
{sectors_str}

【原始输出】
{raw_response}

要求：
1. 只输出一个 JSON 对象，不要输出 Markdown、代码块或任何解释。
2. JSON 顶层必须包含：long_short, rotation, heat, summary, confidence_score, risk_level, market_outlook。
3. long_short 必须包含：bullish, neutral, bearish。
4. rotation 必须包含：current_strong, potential, declining。
5. heat 必须包含：hottest, heating, cooling。
6. summary 必须包含：market_view, key_opportunity, major_risk, strategy。
7. JSON key 保持英文，所有 value 必须是简体中文。
8. confidence_score 使用 0-100 的整数。
9. 如果某个字段无法确定，请使用空数组或“暂无”补齐，不要省略字段。
"""
        messages = [
            {"role": "system", "content": "你是 JSON 修复助手，只输出严格合法的 JSON 对象。所有 value 必须使用简体中文。"},
            {"role": "user", "content": repair_prompt},
        ]
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
    
    def _generate_report_summary(self, results: Dict) -> str:
        """生成报告摘要"""
        try:
            return build_sector_strategy_summary(normalize_sector_strategy_result(results)).get("headline", "智策板块分析报告")
        except Exception:
            return "智策板块分析报告"
    
    def _extract_confidence_score(self, results: Dict) -> float:
        """提取置信度分数"""
        try:
            return float(build_sector_strategy_summary(normalize_sector_strategy_result(results)).get("confidence_score", 0))
        except Exception:
            return 0.0
    
    def _extract_risk_level(self, results: Dict) -> str:
        """提取风险等级"""
        try:
            return build_sector_strategy_summary(normalize_sector_strategy_result(results)).get("risk_level", "中等")
        except Exception:
            return "中等"
    
    def _extract_investment_horizon(self, results: Dict) -> str:
        """提取投资周期"""
        try:
            return derive_sector_strategy_investment_horizon(normalize_sector_strategy_result(results))
        except Exception:
            return DEFAULT_INVESTMENT_HORIZON
    
    def _extract_market_outlook(self, results: Dict) -> str:
        """提取市场展望"""
        try:
            return build_sector_strategy_summary(normalize_sector_strategy_result(results)).get("market_outlook", "中性")
        except Exception:
            return "中性"
    
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
