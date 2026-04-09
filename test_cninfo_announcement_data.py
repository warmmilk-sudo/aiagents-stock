import unittest
from unittest.mock import Mock

from cninfo_announcement_data import CninfoAnnouncementFetcher


class CninfoAnnouncementFetcherTests(unittest.TestCase):
    def setUp(self):
        self.session = Mock()
        self.fetcher = CninfoAnnouncementFetcher(session=self.session)

    def test_get_stock_announcements_resolves_stock_and_normalizes_items(self):
        search_response = Mock()
        search_response.json.return_value = [
            {
                "code": "600519",
                "category": "A股",
                "orgId": "gssh0600519",
                "zwjc": "贵州茅台",
            }
        ]
        search_response.raise_for_status.return_value = None

        stock_page_response = Mock()
        stock_page_response.text = """
        <script>
            var stockCode = "600519";
            var orgId = "gssh0600519";
            var plate = "sse";
        </script>
        """
        stock_page_response.raise_for_status.return_value = None

        announcement_response = Mock()
        announcement_response.json.return_value = {
            "totalAnnouncement": 12,
            "announcements": [
                {
                    "secCode": "600519",
                    "secName": "贵州茅台",
                    "orgId": "gssh0600519",
                    "announcementId": "1225075835",
                    "announcementTitle": "贵州茅台关于回购股份实施进展的公告",
                    "announcementTime": 1775145600000,
                    "adjunctUrl": "finalpage/2026-04-03/1225075835.PDF",
                    "adjunctSize": 80,
                    "adjunctType": "PDF",
                    "announcementType": "01010503",
                    "columnId": "250401||251302",
                }
            ],
        }
        announcement_response.raise_for_status.return_value = None

        self.session.request.side_effect = [search_response, announcement_response]
        self.session.get.return_value = stock_page_response

        result = self.fetcher.get_stock_announcements("600519", limit=5)

        self.assertTrue(result["data_success"])
        data = result["announcement_data"]
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["stock_name"], "贵州茅台")
        item = data["items"][0]
        self.assertEqual(item["title"], "贵州茅台关于回购股份实施进展的公告")
        self.assertEqual(item["publish_time"], "2026-04-03 00:00:00")
        self.assertEqual(item["url"], "https://static.cninfo.com.cn/finalpage/2026-04-03/1225075835.PDF")
        self.assertEqual(item["source"], "巨潮资讯")

    def test_get_stock_announcements_rejects_non_a_share_symbols(self):
        result = self.fetcher.get_stock_announcements("AAPL")

        self.assertFalse(result["data_success"])
        self.assertEqual(result["error"], "公告数据仅支持中国A股股票")


if __name__ == "__main__":
    unittest.main()
