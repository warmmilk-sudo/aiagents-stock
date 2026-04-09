import unittest
from unittest.mock import Mock, patch

from stock_research_news_data import StockResearchNewsDataFetcher


class StockResearchNewsDataFetcherTests(unittest.TestCase):
    def test_rsshub_stock_research_platforms_are_reduced(self):
        self.assertEqual(
            StockResearchNewsDataFetcher.RSSHUB_STOCK_RESEARCH_PLATFORMS,
            ["caijing21_company", "caixin", "yicai_latest"],
        )

    @patch("stock_research_news_data.get_detail_fetcher")
    @patch("stock_research_news_data.NewsFlowDataFetcher")
    @patch("stock_research_news_data.QStockNewsDataFetcher")
    @patch("stock_research_news_data.CninfoAnnouncementFetcher")
    def test_get_stock_news_enriches_supported_rsshub_detail(
        self,
        cninfo_cls,
        pywencai_cls,
        rsshub_cls,
        get_detail_fetcher,
    ):
        detail_fetcher = Mock()
        detail_fetcher.supports_detail_url.return_value = True
        detail_fetcher.fetch_detail_text.return_value = "这是正文摘录。"
        get_detail_fetcher.return_value = detail_fetcher

        cninfo_cls.return_value.get_stock_announcements.return_value = {
            "announcement_data": {"stock_name": "贵州茅台", "count": 0, "items": []},
            "data_success": False,
        }
        pywencai_cls.return_value.get_stock_news.return_value = {"news_data": {"count": 0, "items": []}, "data_success": False}
        rsshub_cls.return_value.get_multi_platform_news.return_value = {
            "success_count": 1,
            "platforms_data": [
                {
                    "success": True,
                    "platform": "thepaper_featured",
                    "platform_name": "澎湃新闻",
                    "data": [
                        {
                            "title": "贵州茅台渠道改革继续推进",
                            "content": "摘要",
                            "publish_time": "Thu, 09 Apr 2026 10:02:00 GMT",
                            "source": "澎湃新闻",
                            "url": "https://www.thepaper.cn/newsDetail_forward_1",
                        }
                    ],
                }
            ],
        }

        fetcher = StockResearchNewsDataFetcher(max_items=5)
        result = fetcher.get_stock_news("600519")

        item = result["supplemental_news_data"]["items"][0]
        self.assertEqual(item["content"], "这是正文摘录。")
        self.assertEqual(item["content_origin"], "detail_page")
        detail_fetcher.fetch_detail_text.assert_called_once()

    @patch("stock_research_news_data.NewsFlowDataFetcher")
    @patch("stock_research_news_data.QStockNewsDataFetcher")
    @patch("stock_research_news_data.CninfoAnnouncementFetcher")
    def test_get_stock_news_combines_announcements_news_and_rsshub(
        self,
        cninfo_cls,
        pywencai_cls,
        rsshub_cls,
    ):
        cninfo_cls.return_value.get_stock_announcements.return_value = {
            "announcement_data": {
                "stock_name": "贵州茅台",
                "count": 1,
                "items": [
                    {
                        "title": "贵州茅台重大事项公告",
                        "publish_time": "2026-03-31 00:00:00",
                        "url": "https://example.com/ann.pdf",
                    }
                ],
                "query_time": "2026-04-09 18:00:00",
            },
            "data_success": True,
        }
        pywencai_cls.return_value.get_stock_news.return_value = {
            "news_data": {
                "count": 1,
                "items": [
                    {
                        "title": "贵州茅台宣布涨价",
                        "publish_time": "今天 09:42",
                        "source": "同花顺财经",
                        "content": "摘要",
                        "url": "https://example.com/news",
                    }
                ],
                "query_time": "2026-04-09 18:00:00",
            },
            "data_success": True,
        }
        rsshub_cls.return_value.get_multi_platform_news.return_value = {
            "success_count": 2,
            "platforms_data": [
                {
                    "success": True,
                    "platform": "caijing21_company",
                    "platform_name": "21财经公司",
                    "data": [
                        {
                            "title": "贵州茅台渠道改革继续推进",
                            "content": "贵州茅台正在推进渠道改革",
                            "publish_time": "Thu, 09 Apr 2026 10:02:00 GMT",
                            "source": "21财经公司",
                            "url": "https://example.com/rss",
                        }
                    ],
                }
            ],
        }

        fetcher = StockResearchNewsDataFetcher(max_items=5)
        result = fetcher.get_stock_news("600519")

        self.assertTrue(result["data_success"])
        self.assertEqual(result["stock_name"], "贵州茅台")
        self.assertEqual(result["source_breakdown"]["announcements"]["count"], 1)
        self.assertEqual(result["source_breakdown"]["news"]["count"], 1)
        self.assertEqual(result["source_breakdown"]["supplemental_news"]["count"], 1)

        formatted = fetcher.format_news_for_ai(result)
        self.assertIn("一级证据：法定公告（巨潮资讯）", formatted)
        self.assertIn("三级证据：个股新闻线索（pywencai，过滤后）", formatted)
        self.assertIn("二级证据：财经媒体正文/摘要（RSSHub）", formatted)
        self.assertIn("证据优先级说明", formatted)

    def test_filter_pywencai_news_removes_obvious_noise(self):
        fetcher = StockResearchNewsDataFetcher(max_items=5)

        filtered = fetcher._filter_pywencai_news(
            "000001",
            "平安银行",
            {
                "items": [
                    {
                        "title": "全国首批长期照护师证书颁发 南通王汝芳获“000001号”证书",
                        "publish_time": "01-18 13:03",
                        "source": "同花顺财经",
                        "content": "",
                        "url": "",
                    },
                    {
                        "title": "平安银行发布年度业绩快报",
                        "publish_time": "今天 18:00",
                        "source": "证券时报",
                        "content": "平安银行业绩表现稳健",
                        "url": "https://example.com/a",
                    },
                    {
                        "title": "平安银行000001",
                        "publish_time": "03-30 16:29",
                        "source": "CSDN博客",
                        "content": "个人记录",
                        "url": "https://example.com/b",
                    },
                ],
                "query_time": "2026-04-09 20:00:00",
                "count": 3,
            },
        )

        self.assertEqual(filtered["raw_count"], 3)
        self.assertEqual(filtered["count"], 1)
        self.assertEqual(filtered["items"][0]["title"], "平安银行发布年度业绩快报")


if __name__ == "__main__":
    unittest.main()
