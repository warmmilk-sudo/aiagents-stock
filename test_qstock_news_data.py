import importlib
import sys
import types
import unittest

import pandas as pd


class _FakeDataSourceManager:
    tushare_available = True
    tushare_api = types.SimpleNamespace(
        stock_basic=lambda **kwargs: pd.DataFrame([{"name": "贵州茅台"}])
    )

    @staticmethod
    def _convert_to_ts_code(symbol):
        return f"{symbol}.SH"


class QStockNewsDataFetcherTests(unittest.TestCase):
    def setUp(self):
        for name in ["qstock_news_data", "data_source_manager", "pywencai_runtime", "pywencai"]:
            sys.modules.pop(name, None)

        sys.modules["pywencai_runtime"] = types.SimpleNamespace(
            setup_pywencai_runtime_env=lambda: None
        )
        sys.modules["pywencai"] = types.SimpleNamespace(
            get=lambda query, loop=True: pd.DataFrame(
                [
                    {
                        "news_list1": str(
                            [
                                {
                                    "date": {"value": "04-03 13:35", "key": "publish_time"},
                                    "source": {"value": "中财网", "key": "publish_source"},
                                    "title": {"value": "贵州茅台(600519):回购进展", "key": "title"},
                                    "content": {"value": "公司公告获得重要订单", "key": "summary"},
                                    "show_detail": "https://example.com/news/1",
                                }
                            ]
                        )
                    }
                ]
            )
        )
        sys.modules["data_source_manager"] = types.SimpleNamespace(
            data_source_manager=_FakeDataSourceManager()
        )
        self.module = importlib.import_module("qstock_news_data")

    def tearDown(self):
        for name in ["qstock_news_data", "data_source_manager", "pywencai_runtime", "pywencai"]:
            sys.modules.pop(name, None)

    def test_get_news_data_uses_pywencai_and_normalizes_items(self):
        fetcher = self.module.QStockNewsDataFetcher()

        result = fetcher._get_news_data("600519")

        self.assertIsNotNone(result)
        self.assertEqual(result["count"], 1)
        first = result["items"][0]
        self.assertEqual(first["title"], "贵州茅台(600519):回购进展")
        self.assertEqual(first["source"], "中财网")
        self.assertEqual(first["publish_time"], "04-03 13:35")
        self.assertEqual(first["url"], "https://example.com/news/1")


if __name__ == "__main__":
    unittest.main()
