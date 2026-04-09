"""
个股研究新闻聚合模块。

组合巨潮公告、pywencai 个股新闻和 RSSHub 财经媒体补充源。
"""

from __future__ import annotations

from datetime import datetime
import os
from typing import Any, Dict, List

from article_text_fetcher import get_detail_fetcher
from cninfo_announcement_data import CninfoAnnouncementFetcher
from news_flow_data import NewsFlowDataFetcher
from qstock_news_data import QStockNewsDataFetcher


class StockResearchNewsDataFetcher:
    """个股研究新闻聚合器。"""

    RSSHUB_STOCK_RESEARCH_PLATFORMS = [
        "caijing21_company",
        "caixin",
        "yicai_latest",
    ]

    def __init__(self, max_items: int = 10) -> None:
        self.max_items = max(1, int(max_items))
        self.cninfo_fetcher = CninfoAnnouncementFetcher(max_items=max_items)
        self.pywencai_fetcher = QStockNewsDataFetcher()
        self.rsshub_fetcher = NewsFlowDataFetcher()
        self.detail_fetcher = get_detail_fetcher()
        self.detail_item_limit = max(1, int(os.getenv("STOCK_RESEARCH_DETAIL_ITEM_LIMIT", "3")))

    @staticmethod
    def _dedupe_news_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped = []
        seen = set()
        for item in items:
            key = (item.get("title"), item.get("publish_time"), item.get("source"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _resolve_stock_name(self, symbol: str, announcement_result: Dict[str, Any]) -> str:
        announcement_data = announcement_result.get("announcement_data") or {}
        stock_name = str(announcement_data.get("stock_name") or "").strip()
        if stock_name:
            return stock_name

        try:
            return str(self.pywencai_fetcher._resolve_stock_name(symbol) or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _score_pywencai_news_item(symbol: str, stock_name: str, item: Dict[str, Any]) -> int:
        title = str(item.get("title") or "")
        content = str(item.get("content") or "")
        source = str(item.get("source") or "")
        publish_time = str(item.get("publish_time") or "")
        haystack = f"{title} {content}"

        score = 0
        if stock_name:
            if stock_name in title:
                score += 6
            if stock_name in content:
                score += 3

        wrapped_symbol_patterns = (
            f"({symbol})",
            f"（{symbol}）",
            f"{symbol}.SH",
            f"{symbol}.SZ",
        )
        if any(pattern in title for pattern in wrapped_symbol_patterns):
            score += 5
        elif symbol in title:
            score += 3

        if symbol in content:
            score += 2

        if any(keyword in haystack for keyword in ("公告", "评级", "业绩", "回购", "问询", "增资", "涨价", "并购", "重组", "订单")):
            score += 2

        if "今天" in publish_time:
            score += 3
        elif "昨天" in publish_time:
            score += 2
        elif publish_time:
            score += 1

        if any(noisy_source in source for noisy_source in ("博客",)):
            score -= 6

        return score

    def _filter_pywencai_news(self, symbol: str, stock_name: str, news_data: Dict[str, Any]) -> Dict[str, Any]:
        items = list((news_data or {}).get("items") or [])
        if not items:
            return {
                **(news_data or {}),
                "items": [],
                "count": 0,
                "raw_count": 0,
            }

        scored_items = []
        for item in items:
            score = self._score_pywencai_news_item(symbol, stock_name, item)
            if score < 5:
                continue
            enriched = dict(item)
            enriched["relevance_score"] = score
            scored_items.append(enriched)

        scored_items.sort(
            key=lambda item: (
                -int(item.get("relevance_score", 0)),
                str(item.get("publish_time") or ""),
            )
        )
        deduped = self._dedupe_news_items(scored_items)[: self.max_items]

        return {
            **(news_data or {}),
            "items": deduped,
            "count": len(deduped),
            "raw_count": len(items),
        }

    def _get_rsshub_stock_news(self, symbol: str, stock_name: str) -> Dict[str, Any]:
        result = self.rsshub_fetcher.get_multi_platform_news(platforms=self.RSSHUB_STOCK_RESEARCH_PLATFORMS)
        matched = []
        for platform_data in result.get("platforms_data", []):
            if not platform_data.get("success"):
                continue

            platform_name = platform_data.get("platform_name") or platform_data.get("platform")
            for row in platform_data.get("data", []):
                title = str(row.get("title") or "").strip()
                content = str(row.get("content") or "").strip()
                haystack = f"{title} {content}"
                if symbol not in haystack and (not stock_name or stock_name not in haystack):
                    continue
                matched.append(
                    {
                        "title": title,
                        "content": content,
                        "summary": content,
                        "publish_time": str(row.get("publish_time") or ""),
                        "source": str(row.get("source") or platform_name),
                        "url": str(row.get("url") or ""),
                        "platform": str(platform_data.get("platform") or ""),
                        "platform_name": str(platform_name),
                    }
                )

        deduped = self._dedupe_news_items(matched)[: self.max_items]
        self._enrich_detail_content(deduped)
        return {
            "items": deduped,
            "count": len(deduped),
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "rsshub_finance",
            "platforms": self.RSSHUB_STOCK_RESEARCH_PLATFORMS,
            "platform_success_count": result.get("success_count", 0),
        }

    def _enrich_detail_content(self, items: List[Dict[str, Any]]) -> None:
        for item in items[: self.detail_item_limit]:
            url = str(item.get("url") or "").strip()
            if not url or not self.detail_fetcher.supports_detail_url(url):
                continue
            detail_text = self.detail_fetcher.fetch_detail_text(url)
            if detail_text:
                item["content"] = detail_text
                item["content_origin"] = "detail_page"

    def get_stock_news(self, symbol: str) -> Dict[str, Any]:
        pywencai_result = self.pywencai_fetcher.get_stock_news(symbol)
        announcement_result = self.cninfo_fetcher.get_stock_announcements(symbol, limit=self.max_items)
        stock_name = self._resolve_stock_name(symbol, announcement_result)
        filtered_news_data = self._filter_pywencai_news(symbol, stock_name, pywencai_result.get("news_data") or {})
        rsshub_news = self._get_rsshub_stock_news(symbol, stock_name)

        has_news = bool(filtered_news_data.get("items"))
        has_announcements = bool((announcement_result.get("announcement_data") or {}).get("items"))
        has_supplemental = bool(rsshub_news.get("items"))

        errors = []
        if pywencai_result.get("error"):
            errors.append(f"pywencai: {pywencai_result['error']}")
        if announcement_result.get("error"):
            errors.append(f"巨潮: {announcement_result['error']}")

        return {
            "symbol": symbol,
            "stock_name": stock_name,
            "news_data": filtered_news_data,
            "announcement_data": announcement_result.get("announcement_data"),
            "supplemental_news_data": rsshub_news,
            "data_success": has_news or has_announcements or has_supplemental,
            "source": "cninfo_pywencai_rsshub",
            "source_breakdown": {
                "announcements": {
                    "source": "cninfo",
                    "count": (announcement_result.get("announcement_data") or {}).get("count", 0),
                },
                "news": {
                    "source": "pywencai",
                    "count": filtered_news_data.get("count", 0),
                    "raw_count": filtered_news_data.get("raw_count", 0),
                },
                "supplemental_news": {
                    "source": "rsshub",
                    "count": rsshub_news.get("count", 0),
                },
            },
            "error": "；".join(errors) if errors and not (has_news or has_announcements or has_supplemental) else None,
        }

    def format_news_for_ai(self, data: Dict[str, Any]) -> str:
        if not data or not data.get("data_success"):
            return "未能获取新闻公告数据"

        text_parts = []
        text_parts.append(
            """
【证据优先级说明】
1. 巨潮资讯公告正文/摘录：法定披露，优先级最高，用于确认事实。
2. 财经媒体正文摘录：来自详情页正文，用于补充背景、影响和风险。
3. 聚合新闻摘要：用于补充线索和市场关注点，可信度低于前两者。
若不同来源存在冲突，请优先相信巨潮资讯公告，其次相信正文媒体稿，再参考聚合摘要。
""".strip()
        )
        text_parts.append("")

        announcement_data = data.get("announcement_data") or {}
        if announcement_data.get("items"):
            text_parts.append(
                f"""
【一级证据：法定公告（巨潮资讯）】
股票：{announcement_data.get('stock_name', data.get('stock_name', data.get('symbol', 'N/A')))}
查询时间：{announcement_data.get('query_time', 'N/A')}
公告数量：{announcement_data.get('count', 0)}条

"""
            )
            for idx, item in enumerate(announcement_data.get("items", [])[: min(self.max_items, 5)], 1):
                text_parts.append(f"公告 {idx}:")
                for field in ["title", "publish_time", "url"]:
                    value = item.get(field)
                    if value:
                        text_parts.append(f"  {field}: {value}")
                content = item.get("content")
                if content:
                    value = str(content)
                    if len(value) > 1200:
                        value = value[:1200] + "..."
                    content_origin = str(item.get("content_origin") or "excerpt")
                    text_parts.append(f"  content_origin: {content_origin}")
                    text_parts.append(f"  content: {value}")
                text_parts.append("")

        news_data = data.get("news_data") or {}
        if news_data.get("items"):
            text_parts.append(
                f"""
【三级证据：个股新闻线索（pywencai，过滤后）】
查询时间：{news_data.get('query_time', 'N/A')}
新闻数量：{news_data.get('count', 0)}条（原始 {news_data.get('raw_count', news_data.get('count', 0))} 条）

"""
            )
            for idx, item in enumerate(news_data.get("items", [])[: min(self.max_items, 5)], 1):
                text_parts.append(f"新闻 {idx}:")
                for field in ["title", "publish_time", "source", "content", "url"]:
                    value = item.get(field)
                    if value:
                        if field == "content" and len(str(value)) > 500:
                            value = str(value)[:500] + "..."
                        text_parts.append(f"  {field}: {value}")
                text_parts.append("")

        supplemental_news_data = data.get("supplemental_news_data") or {}
        if supplemental_news_data.get("items"):
            text_parts.append(
                f"""
【二级证据：财经媒体正文/摘要（RSSHub）】
查询时间：{supplemental_news_data.get('query_time', 'N/A')}
命中数量：{supplemental_news_data.get('count', 0)}条

"""
            )
            for idx, item in enumerate(supplemental_news_data.get("items", [])[: min(self.max_items, 4)], 1):
                text_parts.append(f"补充 {idx}:")
                for field in ["title", "publish_time", "source", "url"]:
                    value = item.get(field)
                    if value:
                        text_parts.append(f"  {field}: {value}")
                content = item.get("content")
                if content:
                    value = str(content)
                    if len(value) > 1200:
                        value = value[:1200] + "..."
                    text_parts.append(f"  content_origin: {item.get('content_origin') or 'summary'}")
                    text_parts.append(f"  content: {value}")
                text_parts.append("")

        return "\n".join(text_parts) if text_parts else "未能获取新闻公告数据"
