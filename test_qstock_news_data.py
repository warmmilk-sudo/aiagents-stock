import importlib
import sys
import types
import unittest

class _GuardStub:
    def __init__(self):
        self.calls = []

    def call(self, func, *args, request_name=None, **kwargs):
        self.calls.append(request_name or getattr(func, "__name__", "unknown"))
        return func(*args, **kwargs)


class _FakeILoc:
    def __init__(self, rows):
        self._rows = rows

    def __getitem__(self, index):
        return self._rows[index]


class _FakeDataFrame:
    def __init__(self, rows=None):
        self._rows = list(rows or [])
        self.iloc = _FakeILoc(self._rows)
        self.columns = list(self._rows[0].keys()) if self._rows else []

    @property
    def empty(self):
        return len(self._rows) == 0

    def iterrows(self):
        for index, row in enumerate(self._rows):
            yield index, row

    def head(self, count):
        return _FakeDataFrame(self._rows[:count])

    def __len__(self):
        return len(self._rows)


class QStockNewsDataFetcherTests(unittest.TestCase):
    def setUp(self):
        for name in ["qstock_news_data", "akshare", "data_source_manager"]:
            sys.modules.pop(name, None)

        sys.modules["akshare"] = types.SimpleNamespace(
            stock_news_em=lambda symbol: _FakeDataFrame(
                [
                    {
                        "新闻标题": "公司签订大单",
                        "新闻内容": "公司公告获得重要订单",
                        "发布时间": "2026-04-03 10:00:00",
                        "文章来源": "东方财富",
                        "新闻链接": "https://example.com/news/1",
                    }
                ]
            ),
            stock_news_sina=lambda symbol: _FakeDataFrame(),
            stock_news_cls=lambda: _FakeDataFrame(),
            stock_zh_a_spot_em=lambda: _FakeDataFrame([{"代码": "600519", "名称": "贵州茅台"}]),
        )
        sys.modules["data_source_manager"] = types.SimpleNamespace(
            data_source_manager=types.SimpleNamespace(
                tushare_available=False,
            )
        )
        self.module = importlib.import_module("qstock_news_data")

    def tearDown(self):
        for name in ["qstock_news_data", "akshare", "data_source_manager"]:
            sys.modules.pop(name, None)

    def test_get_news_data_uses_guard_for_akshare_news_calls(self):
        fetcher = self.module.QStockNewsDataFetcher()
        guard = _GuardStub()
        fetcher.guard = guard

        result = fetcher._get_news_data("600519")

        self.assertIsNotNone(result)
        self.assertEqual(result["count"], 1)
        self.assertIn("stock_news_em", guard.calls)


if __name__ == "__main__":
    unittest.main()
