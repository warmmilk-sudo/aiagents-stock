from __future__ import annotations

from news_flow_engine import NewsFlowEngine


class _FakeFetcher:
    def get_multi_platform_news(self, platforms=None, category=None):
        return {
            "success": False,
            "success_count": 0,
            "failed_count": 2,
            "errors": [
                {"platform": "weibo", "error": "502"},
                {"platform": "eastmoney", "error": "502"},
            ],
            "platforms_data": [
                {"success": False, "platform": "weibo", "error": "502"},
                {"success": False, "platform": "eastmoney", "error": "502"},
            ],
        }

    def extract_stock_related_news(self, platforms_data, keywords=None):
        return [
            {
                "platform": "weibo",
                "platform_name": "微博热搜",
                "category": "social",
                "weight": 10,
                "title": "缓存新闻",
                "content": "缓存内容",
                "url": "",
                "source": "微博热搜",
                "publish_time": "2026-03-20 07:49:26",
                "matched_keywords": ["股"],
                "keyword_count": 1,
                "score": 100,
            }
        ] if platforms_data else []

    def get_hot_topics(self, platforms_data, top_n=20):
        return [
            {
                "topic": "缓存",
                "count": 1,
                "heat": 10,
                "cross_platform": 1,
                "sources": ["微博热搜"],
            }
        ] if platforms_data else []

    def calculate_flow_score(self, platforms_data):
        total = sum(item.get("count", 0) for item in platforms_data if item.get("success"))
        return {
            "total_score": total,
            "social_score": total,
            "news_score": 0,
            "finance_score": 0,
            "tech_score": 0,
            "level": "低",
            "analysis": "缓存分析",
            "platform_details": [],
        }


class _FakeDB:
    def __init__(self):
        self.saved_flow = None
        self.saved_sentiment = None

    def get_latest_snapshot(self):
        return {"id": 99}

    def get_snapshot_detail(self, snapshot_id):
        assert snapshot_id == 99
        return {
            "snapshot": {"fetch_time": "2026-03-20 07:49:26"},
            "platform_news": [
                {
                    "platform": "weibo",
                    "platform_name": "微博热搜",
                    "category": "social",
                    "weight": 10,
                    "title": "缓存新闻1",
                    "content": "内容1",
                    "url": "",
                    "source": "微博热搜",
                    "publish_time": "2026-03-20 07:49:26",
                    "rank": 1,
                },
                {
                    "platform": "eastmoney",
                    "platform_name": "东方财富",
                    "category": "finance",
                    "weight": 9,
                    "title": "缓存新闻2",
                    "content": "内容2",
                    "url": "",
                    "source": "东方财富",
                    "publish_time": "2026-03-20 07:49:26",
                    "rank": 1,
                },
            ],
        }

    def get_recent_scores(self, hours):
        return [{"total_score": 88}]

    def get_sentiment_history(self, limit):
        return []

    def save_flow_snapshot(self, flow_data, platforms_data, stock_news, hot_topics):
        self.saved_flow = {
            "flow_data": flow_data,
            "platforms_data": platforms_data,
            "stock_news": stock_news,
            "hot_topics": hot_topics,
        }
        return 123

    def save_sentiment_record(self, snapshot_id, sentiment_record):
        self.saved_sentiment = {
            "snapshot_id": snapshot_id,
            "sentiment_record": sentiment_record,
        }


def test_run_quick_analysis_falls_back_to_cached_news_data():
    engine = NewsFlowEngine()
    engine.fetcher = _FakeFetcher()
    engine.db = _FakeDB()
    engine.model = None
    engine.sentiment = None

    result = engine.run_quick_analysis(category="finance")

    assert result["success"] is True
    assert result["data_from_cache"] is True
    assert "缓存新闻数据" in result["data_warning"]
    assert result["snapshot_id"] == 123
    assert result["success_count"] == 2
    assert len(result["platforms_data"]) == 2
    assert len(result["hot_topics"]) == 1
    assert len(result["stock_news"]) == 1
    assert engine.db.saved_flow is not None
    assert "缓存新闻数据" in engine.db.saved_flow["flow_data"]["analysis"]
