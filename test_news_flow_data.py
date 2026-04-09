from __future__ import annotations

import os
from unittest.mock import patch

from news_flow_data import NewsFlowDataFetcher


class _FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, text="", raise_error=None):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self._raise_error = raise_error

    def raise_for_status(self):
        if self._raise_error:
            raise self._raise_error
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def json(self):
        if self._json_data is None:
            raise RuntimeError("no json payload")
        return self._json_data


def test_get_platform_news_parses_rsshub_hot_feed():
    rss_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>微博热搜榜</title>
        <item>
          <title>AI算力走强</title>
          <link>https://example.com/1</link>
          <description><![CDATA[<p>服务器与GPU板块受关注</p>]]></description>
          <pubDate>Wed, 08 Apr 2026 19:00:00 GMT</pubDate>
        </item>
      </channel>
    </rss>
    """

    with patch.dict(os.environ, {"RSSHUB_BASE_URL": "http://127.0.0.1:1200"}, clear=False):
        fetcher = NewsFlowDataFetcher()

        with patch("news_flow_data.requests.get", return_value=_FakeResponse(text=rss_xml)):
            result = fetcher.get_platform_news("weibo")

    assert result["success"] is True
    assert result["count"] == 1
    assert result["platform_name"] == "微博热搜"
    assert result["data"][0]["title"] == "AI算力走强"
    assert result["data"][0]["content"] == "服务器与GPU板块受关注"


def test_get_platform_news_parses_rsshub_finance_feed():
    rss_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>财新网 - 最新文章</title>
        <item>
          <title>机器人板块再度活跃</title>
          <link>https://example.com/finance</link>
          <description><![CDATA[<p>A股机器人概念股盘中拉升。</p>]]></description>
          <pubDate>Wed, 08 Apr 2026 19:00:00 GMT</pubDate>
        </item>
      </channel>
    </rss>
    """

    with patch.dict(os.environ, {"RSSHUB_BASE_URL": "http://127.0.0.1:1200"}, clear=False):
        fetcher = NewsFlowDataFetcher()

        with patch("news_flow_data.requests.get", return_value=_FakeResponse(text=rss_xml)):
            result = fetcher.get_platform_news("caixin")

    assert result["success"] is True
    assert result["count"] == 1
    assert result["category"] == "finance"
    assert "机器人板块" in result["data"][0]["title"]


def test_get_multi_platform_news_falls_back_to_tianapi_when_hotspot_sources_all_fail():
    with patch.dict(os.environ, {"TIANAPI_KEY": "demo-key"}, clear=False):
        fetcher = NewsFlowDataFetcher()

        def fake_get(url, params=None, headers=None, timeout=None):
            if url.startswith("http://127.0.0.1:1200/"):
                return _FakeResponse(status_code=503, raise_error=RuntimeError("503"))
            if url.endswith("/networkhot/index"):
                return _FakeResponse(
                    json_data={
                        "code": 200,
                        "msg": "success",
                        "result": [
                            {"title": "算力链走强", "digest": "A股算力方向受到关注", "hotnum": 12345},
                            {"title": "机器人题材升温", "digest": "机器人方向活跃", "hotnum": 6789},
                        ],
                    }
                )
            raise AssertionError(f"unexpected url: {url}")

        with patch("news_flow_data.requests.get", side_effect=fake_get):
            result = fetcher.get_multi_platform_news(category="social")

    assert result["success"] is True
    assert result["success_count"] == 1
    fallback = [item for item in result["platforms_data"] if item.get("platform") == fetcher.TIANAPI_FALLBACK_PLATFORM]
    assert len(fallback) == 1
    assert fallback[0]["data"][0]["title"] == "算力链走强"


def test_calculate_flow_score_deduplicates_same_topic_across_platforms():
    fetcher = NewsFlowDataFetcher()
    platforms_data = [
        {
            "success": True,
            "platform": "zhihu",
            "platform_name": "知乎想法热榜",
            "category": "social",
            "weight": 7,
            "count": 1,
            "data": [{"title": "AI算力走强", "content": "", "rank": 1}],
        },
        {
            "success": True,
            "platform": "zhihu_hot",
            "platform_name": "知乎热榜",
            "category": "social",
            "weight": 8,
            "count": 1,
            "data": [{"title": "AI 算力走强", "content": "", "rank": 2}],
        },
    ]

    result = fetcher.calculate_flow_score(platforms_data)

    assert 8 < result["social_score"] < 15
    details = {item["platform"]: item for item in result["platform_details"]}
    assert details["zhihu_hot"]["effective_weight"] > details["zhihu"]["effective_weight"]
    assert details["zhihu"]["unique_count"] == 1
    assert details["zhihu_hot"]["unique_count"] == 1


def test_extract_stock_related_news_merges_duplicate_titles():
    fetcher = NewsFlowDataFetcher()
    platforms_data = [
        {
            "success": True,
            "platform": "weibo",
            "platform_name": "微博热搜",
            "category": "social",
            "weight": 10,
            "count": 1,
            "data": [{"title": "伊朗宣布停火协议生效", "content": "A股算力股大涨", "rank": 1}],
        },
        {
            "success": True,
            "platform": "zhihu_hot",
            "platform_name": "知乎热榜",
            "category": "social",
            "weight": 8,
            "count": 1,
            "data": [{"title": "伊朗称停火协议已经生效", "content": "算力板块继续上涨", "rank": 3}],
        },
    ]

    result = fetcher.extract_stock_related_news(platforms_data, keywords=["A股", "算力", "上涨"])

    assert len(result) == 1
    assert result[0]["cross_platform"] == 2
    assert set(result[0]["platforms"]) == {"weibo", "zhihu_hot"}
    assert "算力" in result[0]["matched_keywords"]


def test_get_platform_ranking_merges_duplicate_titles():
    fetcher = NewsFlowDataFetcher()
    platforms_data = [
        {
            "success": True,
            "platform": "bilibili",
            "platform_name": "哔哩哔哩排行榜",
            "category": "social",
            "weight": 6,
            "count": 1,
            "data": [{"title": "机器人产业爆发", "content": "", "rank": 1}],
        },
        {
            "success": True,
            "platform": "bilibili_popular",
            "platform_name": "哔哩哔哩综合热门",
            "category": "social",
            "weight": 6,
            "count": 1,
            "data": [{"title": "机器人 产业爆发", "content": "", "rank": 4}],
        },
    ]

    result = fetcher.get_platform_ranking(platforms_data)

    assert len(result) == 1
    assert result[0]["cross_platform"] == 2
    assert set(result[0]["platforms"]) == {"bilibili", "bilibili_popular"}


def test_get_multi_platform_news_social_priority_keeps_three_default_sources():
    fetcher = NewsFlowDataFetcher()

    with (
        patch.object(fetcher, "get_platform_news", side_effect=lambda platform: {"success": False, "platform": platform, "error": "mock"}),
        patch.object(fetcher, "_get_tianapi_fallback_news", return_value=None),
    ):
        result = fetcher.get_multi_platform_news(category="social")

    assert [item["platform"] for item in result["platforms_data"]] == ["weibo", "zhihu_hot", "bilibili_popular"]


def test_default_platforms_reduce_tech_sources_to_two():
    fetcher = NewsFlowDataFetcher()

    assert fetcher.CATEGORY_PLATFORM_PRIORITY["tech"] == ["kr36_newsflashes", "kr36_latest"]
    assert fetcher._get_default_platforms() == [
        "weibo",
        "zhihu_hot",
        "bilibili_popular",
        "wallstreetcn",
        "yicai_brief",
        "caixin",
        "thepaper_featured",
        "guancha_headline",
        "kr36_newsflashes",
        "kr36_latest",
    ]
