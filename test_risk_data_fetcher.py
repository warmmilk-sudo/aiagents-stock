import sys
import types
import unittest

import pandas as pd

sys.modules.setdefault("pywencai_runtime", types.SimpleNamespace(setup_pywencai_runtime_env=lambda: None))
sys.modules.setdefault("pywencai", types.SimpleNamespace(get=lambda *args, **kwargs: None))

from risk_data_fetcher import RiskDataFetcher


class RiskDataFetcherPromptFormatTests(unittest.TestCase):
    def test_format_risk_data_for_ai_compacts_dataframe_rows(self):
        fetcher = RiskDataFetcher()

        formatted = fetcher.format_risk_data_for_ai(
            {
                "data_success": True,
                "lifting_ban": {
                    "has_data": True,
                    "summary": "发现 4 条解禁记录\n最近一条规模较大",
                    "data": pd.DataFrame(
                        [
                            {"解禁时间": "2026-05-01", "解禁股数": "1.2亿股", "解禁市值": "18亿元", "股东名称": "机构A", "url": "https://a/1", "id": 1},
                            {"解禁时间": "2026-06-01", "解禁股数": "0.8亿股", "解禁市值": "12亿元", "股东名称": "机构B", "url": "https://a/2", "id": 2},
                            {"解禁时间": "2026-07-01", "解禁股数": "0.5亿股", "解禁市值": "8亿元", "股东名称": "机构C", "url": "https://a/3", "id": 3},
                            {"解禁时间": "2026-08-01", "解禁股数": "0.3亿股", "解禁市值": "4亿元", "股东名称": "机构D", "url": "https://a/4", "id": 4},
                        ]
                    ),
                },
                "shareholder_reduction": {
                    "has_data": True,
                    "summary": "发现 2 条减持公告",
                    "data": pd.DataFrame(
                        [
                            {"公告日期": "2026-04-10", "股东名称": "股东甲", "减持比例": "1.5%", "减持股数": "600万股", "source_id": "abc"},
                            {"公告日期": "2026-04-15", "股东名称": "股东乙", "减持比例": "0.8%", "减持股数": "300万股", "source_id": "def"},
                        ]
                    ),
                },
                "important_events": {
                    "has_data": True,
                    "summary": "1 条重大事项待落地",
                    "data": pd.DataFrame(
                        [
                            {"日期": "2026-04-20", "事件标题": "筹划定增", "事件类型": "融资", "影响说明": "存在摊薄预期", "html_url": "https://e/1"}
                        ]
                    ),
                },
            }
        )

        self.assertIn("【限售解禁】", formatted)
        self.assertIn("记录数：4，仅保留前 3 条关键记录", formatted)
        self.assertIn("机构A", formatted)
        self.assertIn("机构C", formatted)
        self.assertNotIn("机构D", formatted)
        self.assertIn("【大股东减持】", formatted)
        self.assertIn("减持比例=1.5%", formatted)
        self.assertIn("【重要事件】", formatted)
        self.assertIn("事件标题=筹划定增", formatted)
        self.assertNotIn("https://", formatted)
        self.assertNotIn("source_id", formatted)
        self.assertNotIn("id=", formatted)

    def test_format_risk_data_for_ai_surfaces_checked_empty_sections(self):
        fetcher = RiskDataFetcher()

        formatted = fetcher.format_risk_data_for_ai(
            {
                "data_success": False,
                "lifting_ban": {
                    "has_data": False,
                    "checked": True,
                    "summary": "Tushare未发现未来限售解禁记录",
                },
                "shareholder_reduction": {
                    "has_data": False,
                    "checked": True,
                    "summary": "Tushare近180日未发现股东减持记录",
                },
            }
        )

        self.assertIn("【限售解禁】", formatted)
        self.assertIn("Tushare未发现未来限售解禁记录", formatted)
        self.assertIn("【大股东减持】", formatted)
        self.assertIn("Tushare近180日未发现股东减持记录", formatted)
        self.assertNotEqual("未获取到风险数据", formatted)

    def test_tushare_holder_trade_data_filters_to_reductions(self):
        fetcher = RiskDataFetcher()
        ann_date = pd.Timestamp.today().strftime("%Y%m%d")
        fetcher._call_tushare_risk_api = lambda api_name, symbol: pd.DataFrame(
            [
                {
                    "ann_date": ann_date,
                    "holder_name": "增持股东",
                    "holder_type": "高管",
                    "in_de": "IN",
                    "change_vol": 100,
                    "change_ratio": 0.2,
                    "avg_price": 15.5,
                    "after_share": 1000,
                    "after_ratio": 2.1,
                },
                {
                    "ann_date": ann_date,
                    "holder_name": "减持股东",
                    "holder_type": "大股东",
                    "in_de": "DE",
                    "change_vol": 200,
                    "change_ratio": 0.4,
                    "avg_price": 16.5,
                    "after_share": 800,
                    "after_ratio": 1.8,
                },
            ]
        )

        result = fetcher._get_tushare_holder_trade_data("603986.SH")

        self.assertTrue(result["has_data"])
        self.assertEqual("Tushare发现近180日 1 条股东减持记录", result["summary"])
        self.assertEqual(["减持股东"], result["data"]["股东名称"].tolist())
        self.assertEqual(["减持"], result["data"]["方式"].tolist())


if __name__ == "__main__":
    unittest.main()
