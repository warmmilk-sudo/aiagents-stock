import unittest

from sector_strategy_normalization import (
    _split_report_sections,
    build_sector_strategy_summary,
    derive_sector_strategy_recommended_sectors,
    normalize_sector_strategy_export_payload,
    normalize_sector_strategy_predictions,
    normalize_sector_strategy_result,
)


class SectorStrategyNormalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.complete_result = {
            "timestamp": "2026-03-15 10:00:00",
            "agents_analysis": {
                "macro": {
                    "agent_name": "宏观策略师",
                    "agent_role": "宏观与政策分析",
                    "focus_areas": ["宏观", "政策"],
                    "timestamp": "2026-03-15 10:01:00",
                    "analysis": "## 宏观判断\n政策边际改善，风险偏好回暖。",
                }
            },
            "comprehensive_report": "## 综合研判\n市场仍以结构性机会为主，优先关注景气主线。",
            "final_predictions": {
                "long_short": {
                    "bullish": [
                        {
                            "sector": "半导体",
                            "direction": "看多",
                            "reason": "产业链催化持续，景气度改善。",
                            "confidence": 8,
                            "risk": "高位波动可能加大。",
                        }
                    ],
                    "neutral": [
                        {
                            "sector": "医药",
                            "direction": "中性",
                            "reason": "短期缺乏新增催化。",
                            "confidence": 5,
                            "risk": "业绩兑现节奏偏慢。",
                        }
                    ],
                    "bearish": [
                        {
                            "sector": "煤炭",
                            "direction": "看空",
                            "reason": "资金回流成长方向。",
                            "confidence": 6,
                            "risk": "若资源品反弹则可能修复。",
                        }
                    ],
                },
                "rotation": {
                    "current_strong": [
                        {
                            "sector": "半导体",
                            "stage": "强势",
                            "logic": "业绩预期改善与政策共振。",
                            "time_window": "1-2周",
                            "advice": "回踩分批关注。",
                        }
                    ],
                    "potential": [
                        {
                            "sector": "算力",
                            "stage": "潜力",
                            "logic": "资金有从核心硬件向应用扩散迹象。",
                            "time_window": "1周左右",
                            "advice": "等待放量确认后跟进。",
                        }
                    ],
                    "declining": [
                        {
                            "sector": "煤炭",
                            "stage": "衰退",
                            "logic": "板块拥挤度较高，短线承接转弱。",
                            "time_window": "3-5日",
                            "advice": "降低追涨仓位。",
                        }
                    ],
                },
                "heat": {
                    "hottest": [
                        {
                            "sector": "半导体",
                            "score": 92,
                            "trend": "升温",
                            "sustainability": "强",
                        }
                    ],
                    "heating": [
                        {
                            "sector": "算力",
                            "score": 81,
                            "trend": "升温",
                            "sustainability": "中",
                        }
                    ],
                    "cooling": [
                        {
                            "sector": "煤炭",
                            "score": 45,
                            "trend": "降温",
                            "sustainability": "弱",
                        }
                    ],
                },
                "summary": {
                    "market_view": "市场延续结构性活跃。",
                    "key_opportunity": "科技成长仍是主线机会。",
                    "major_risk": "高位板块分化加剧。",
                    "strategy": "控制节奏，围绕景气板块低吸。",
                },
                "confidence_score": 78,
                "risk_level": "中等",
                "market_outlook": "偏积极",
            },
        }

    def test_normalize_complete_result_builds_report_view(self) -> None:
        report_view = normalize_sector_strategy_result(
            self.complete_result,
            data_summary={
                "from_cache": True,
                "cache_warning": "使用了最近一次有效市场快照。",
                "market_overview": {
                    "sh_index": {"close": 3350.12, "change_pct": 0.86},
                    "up_count": 3240,
                    "up_ratio": 64.8,
                },
                "sectors": {"半导体": {}, "算力": {}},
                "concepts": {"AI应用": {}},
            },
        )

        self.assertEqual(report_view["summary"]["headline"], "市场延续结构性活跃。；科技成长仍是主线机会。")
        self.assertEqual(report_view["summary"]["confidence_score"], 78)
        self.assertEqual(report_view["predictions"]["long_short"]["neutral"][0]["sector"], "医药")
        self.assertEqual(report_view["market_snapshot"]["sectors_count"], 2)
        self.assertEqual(report_view["raw_reports"]["macro"]["title"], "宏观策略师")
        self.assertEqual(report_view["warnings"]["missing_fields"], [])

    def test_missing_fields_are_backfilled(self) -> None:
        normalized = normalize_sector_strategy_predictions(
            {
                "long_short": {"bullish": [], "bearish": []},
                "rotation": {"current_strong": [], "potential": [], "declining": []},
                "heat": {"hottest": [], "heating": [], "cooling": []},
                "summary": {
                    "market_view": "市场震荡整理。",
                    "key_opportunity": "暂无明确主线。",
                    "major_risk": "成交量不足。",
                    "strategy": "耐心等待确认。",
                },
            }
        )

        self.assertEqual(normalized["risk_level"], "中等")
        self.assertEqual(normalized["market_outlook"], "中性")
        self.assertEqual(normalized["confidence_score"], 0)
        self.assertEqual(normalized["long_short"]["neutral"], [])
        self.assertIn("long_short.neutral", normalized["warnings"]["missing_fields"])
        self.assertIn("risk_level", normalized["warnings"]["missing_fields"])

    def test_prediction_text_sets_parse_warning(self) -> None:
        normalized = normalize_sector_strategy_predictions({"prediction_text": "plain english fallback"})
        self.assertTrue(normalized["warnings"]["parse_warning"])
        self.assertEqual(normalized["raw_fallback_text"], "plain english fallback")

    def test_old_record_with_analysis_content_parsed_is_supported(self) -> None:
        report_view = normalize_sector_strategy_result({"analysis_content_parsed": self.complete_result})
        summary = build_sector_strategy_summary(report_view)

        self.assertEqual(summary["headline"], "市场延续结构性活跃。；科技成长仍是主线机会。")
        self.assertEqual(summary["bullish"], ["半导体"])
        self.assertEqual(summary["bearish"], ["煤炭"])

    def test_embedded_data_summary_is_used_for_saved_report_snapshot(self) -> None:
        saved_report = {
            "analysis_content_parsed": {
                **self.complete_result,
                "data_summary": {
                    "from_cache": True,
                    "cache_warning": "基于缓存重建。",
                    "data_timestamp": "2026-03-16 15:00:00",
                    "market_overview": {
                        "sh_index": {"close": 3388.12, "change_pct": 1.26},
                        "up_count": 3123,
                        "up_ratio": 61.2,
                        "limit_up": 102,
                        "limit_down": 4,
                    },
                    "sectors": {"半导体": {}, "AI算力": {}, "机器人": {}},
                    "concepts": {"算力租赁": {}, "先进封装": {}},
                },
            }
        }

        report_view = normalize_sector_strategy_result(saved_report)

        self.assertEqual(report_view["market_snapshot"]["sectors_count"], 3)
        self.assertEqual(report_view["market_snapshot"]["concepts_count"], 2)
        self.assertEqual(report_view["market_snapshot"]["market_overview"]["up_count"], 3123)
        self.assertTrue(report_view["market_snapshot"]["from_cache"])

    def test_recommended_sectors_come_from_new_schema(self) -> None:
        report_view = normalize_sector_strategy_result(self.complete_result)
        recommended = derive_sector_strategy_recommended_sectors(report_view)

        self.assertEqual([item["sector_name"] for item in recommended[:3]], ["半导体", "算力"])
        self.assertEqual(recommended[0]["type"], "看多主线")
        self.assertEqual(recommended[1]["type"], "轮动潜力")

    def test_export_payload_removes_market_snapshot_details(self) -> None:
        export_payload = normalize_sector_strategy_export_payload(
            {
                **self.complete_result,
                "data_summary": {
                    "from_cache": True,
                    "cache_warning": "使用缓存快照。",
                    "data_timestamp": "2026-03-15 15:00:00",
                    "market_overview": {"up_count": 3200},
                    "sectors": {"半导体": {}},
                    "concepts": {"AI应用": {}},
                },
            }
        )

        self.assertNotIn("market_snapshot", export_payload["report_view"])
        self.assertEqual(
            export_payload["data_summary"],
            {
                "from_cache": True,
                "cache_warning": "使用缓存快照。",
                "data_timestamp": "2026-03-15 15:00:00",
            },
        )

    def test_split_report_sections_extracts_reasoning_before_report_body(self) -> None:
        body, reasoning = _split_report_sections(
            "【推理过程】\n先汇总宏观、板块、资金和情绪结论，再输出综合报告。\n"
            "【综合研判结论】\n"
            "市场延续结构性活跃，科技成长仍是主线。"
        )

        self.assertEqual(body, "【综合研判结论】\n市场延续结构性活跃，科技成长仍是主线。")
        self.assertEqual(reasoning, "先汇总宏观、板块、资金和情绪结论，再输出综合报告。")

    def test_split_report_sections_extracts_trailing_reasoning_and_removes_think_block(self) -> None:
        body, reasoning = _split_report_sections(
            "# 板块研判\n半导体与算力保持强势。\n\n"
            "思考过程：\n先比较景气度，再结合资金强弱判断主线延续性。\n"
            "<think>补充验证高位波动风险。</think>"
        )

        self.assertEqual(body, "# 板块研判\n半导体与算力保持强势。")
        self.assertEqual(reasoning, "补充验证高位波动风险。\n\n先比较景气度，再结合资金强弱判断主线延续性。")

    def test_split_report_sections_keeps_macro_intro_before_first_heading(self) -> None:
        body, reasoning = _split_report_sections(
            "【推理过程】\nThis is internal reasoning in English.\n\n"
            "以下为基于所给新闻线索形成的宏观策略分析报告。整体结论先行：\n\n"
            "**核心判断：当前宏观环境处于结构性修复阶段。**\n\n"
            "# 一、宏观环境评估\n"
            "这里开始是正文。"
        )

        self.assertTrue(body.startswith("以下为基于所给新闻线索形成的宏观策略分析报告。整体结论先行："))
        self.assertIn("**核心判断：当前宏观环境处于结构性修复阶段。**", body)
        self.assertEqual(reasoning, "This is internal reasoning in English.")

    def test_normalize_sector_strategy_reports_preserves_fund_and_sentiment_entries(self) -> None:
        report_view = normalize_sector_strategy_result(
            {
                "agents_analysis": {
                    "fund": {
                        "agent_name": "资金流向分析师",
                        "agent_role": "跟踪板块资金流向，分析主力行为和资金轮动",
                        "focus_areas": ["资金流向", "主力行为", "北向资金", "板块轮动", "量价配合"],
                        "analysis": "### 资金流向分析报告\n主力资金集中流入风电设备。",
                    },
                    "sentiment": {
                        "agent_name": "市场情绪解码员",
                        "agent_role": "量化市场情绪，识别恐慌贪婪信号，评估板块热度",
                        "focus_areas": ["市场情绪", "赚钱效应", "热点识别", "恐慌贪婪", "活跃度"],
                        "analysis": "# 市场情绪分析报告\n当前情绪谨慎偏弱。",
                    },
                },
                "final_predictions": self.complete_result["final_predictions"],
            }
        )

        self.assertEqual(report_view["raw_reports"]["fund"]["title"], "资金流向分析师")
        self.assertIn("主力资金集中流入风电设备", report_view["raw_reports"]["fund"]["body"])
        self.assertEqual(report_view["raw_reports"]["sentiment"]["title"], "市场情绪解码员")
        self.assertIn("当前情绪谨慎偏弱", report_view["raw_reports"]["sentiment"]["body"])


if __name__ == "__main__":
    unittest.main()
