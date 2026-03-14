import unittest

from sector_strategy_normalization import (
    build_sector_strategy_summary,
    derive_sector_strategy_recommended_sectors,
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

    def test_recommended_sectors_come_from_new_schema(self) -> None:
        report_view = normalize_sector_strategy_result(self.complete_result)
        recommended = derive_sector_strategy_recommended_sectors(report_view)

        self.assertEqual([item["sector_name"] for item in recommended[:3]], ["半导体", "算力"])
        self.assertEqual(recommended[0]["type"], "看多主线")
        self.assertEqual(recommended[1]["type"], "轮动潜力")


if __name__ == "__main__":
    unittest.main()
