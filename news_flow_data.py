"""
新闻流量数据获取模块
优先使用本地 RSSHub 获取中文热点与中文财经，仅在热点不可用时用 TianAPI 全网热搜榜兜底。
"""
from __future__ import annotations

import html
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

from article_text_fetcher import get_detail_fetcher

load_dotenv(Path(__file__).resolve().with_name(".env"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NewsFlowDataFetcher:
    """新闻流量数据获取器"""

    TIANAPI_FALLBACK_PLATFORM = "networkhot_fallback"
    PLATFORM_FAMILIES = {
        "weibo": "weibo",
        "zhihu": "zhihu",
        "zhihu_hot": "zhihu",
        "bilibili": "bilibili",
        "bilibili_popular": "bilibili",
        "kr36_newsflashes": "kr36",
        "kr36_latest": "kr36",
        "huxiu_article": "huxiu",
        "juejin_ai": "juejin",
        "juejin_aicoding": "juejin",
        "thepaper_featured": "thepaper",
        "guancha_headline": "guancha",
    }
    FAMILY_WEIGHT_FACTORS = (1.0, 0.72, 0.55, 0.45)
    CATEGORY_PLATFORM_PRIORITY = {
        "finance": [
            "wallstreetcn",
            "yicai_brief",
            "caixin",
        ],
        "social": [
            "weibo",
            "zhihu_hot",
            "bilibili_popular",
        ],
        "news": [
            "thepaper_featured",
            "guancha_headline",
        ],
        "tech": [
            "kr36_newsflashes",
            "kr36_latest",
        ],
    }

    def __init__(self):
        self.timeout = int(os.getenv("NEWS_FLOW_HTTP_TIMEOUT_SECONDS", "20"))
        self.request_pause_seconds = float(os.getenv("NEWS_FLOW_REQUEST_PAUSE_SECONDS", "0.15"))
        self.detail_item_limit = max(0, int(os.getenv("NEWS_FLOW_DETAIL_ITEM_LIMIT", "2")))
        self.rsshub_base_url = os.getenv("RSSHUB_BASE_URL", "http://127.0.0.1:1200").rstrip("/")
        self.tianapi_base_url = os.getenv("TIANAPI_BASE_URL", "https://apis.tianapi.com").rstrip("/")
        self.tianapi_key = os.getenv("TIANAPI_KEY", "").strip()
        self.detail_fetcher = get_detail_fetcher()
        self.default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            )
        }

        self.platforms = {
            "weibo": {
                "name": "微博热搜",
                "category": "social",
                "weight": 10,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/weibo/search/hot"},
                ],
            },
            "zhihu": {
                "name": "知乎想法热榜",
                "category": "social",
                "weight": 7,
                "influence": "medium",
                "sources": [
                    {"provider": "rsshub", "route": "/zhihu/pin/hotlist"},
                ],
            },
            "zhihu_hot": {
                "name": "知乎热榜",
                "category": "social",
                "weight": 8,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/zhihu/hot"},
                ],
            },
            "bilibili": {
                "name": "哔哩哔哩排行榜",
                "category": "social",
                "weight": 6,
                "influence": "medium",
                "sources": [
                    {"provider": "rsshub", "route": "/bilibili/ranking/all"},
                    {"provider": "rsshub", "route": "/bilibili/precious"},
                    {"provider": "rsshub", "route": "/bilibili/hot-search"},
                ],
            },
            "bilibili_popular": {
                "name": "哔哩哔哩综合热门",
                "category": "social",
                "weight": 6,
                "influence": "medium",
                "sources": [
                    {"provider": "rsshub", "route": "/bilibili/popular/all"},
                ],
            },
            "wallstreetcn": {
                "name": "华尔街见闻",
                "category": "finance",
                "weight": 9,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/wallstreetcn/news/global"},
                ],
            },
            "caixin": {
                "name": "财新网",
                "category": "finance",
                "weight": 9,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/caixin/latest"},
                ],
            },
            "yicai_latest": {
                "name": "第一财经最新",
                "category": "finance",
                "weight": 8,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/yicai/latest"},
                ],
            },
            "yicai_brief": {
                "name": "第一财经快讯",
                "category": "finance",
                "weight": 8,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/yicai/brief"},
                ],
            },
            "caijing21_finance": {
                "name": "21财经金融",
                "category": "finance",
                "weight": 8,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/21caijing/channel/金融/动态"},
                ],
            },
            "caijing21_securities": {
                "name": "21财经证券",
                "category": "finance",
                "weight": 8,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/21caijing/channel/证券/动态"},
                ],
            },
            "caijing21_global": {
                "name": "21财经全球",
                "category": "finance",
                "weight": 7,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/21caijing/channel/全球/动态"},
                ],
            },
            "caijing21_company": {
                "name": "21财经公司",
                "category": "finance",
                "weight": 7,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/21caijing/channel/公司/动态"},
                ],
            },
            "kr36_newsflashes": {
                "name": "36氪快讯",
                "category": "tech",
                "weight": 7,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/36kr/newsflashes"},
                ],
            },
            "kr36_latest": {
                "name": "36氪最新",
                "category": "tech",
                "weight": 7,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/36kr/news"},
                ],
            },
            "huxiu_article": {
                "name": "虎嗅",
                "category": "tech",
                "weight": 7,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/huxiu/article"},
                ],
            },
            "thepaper_featured": {
                "name": "澎湃新闻",
                "category": "news",
                "weight": 7,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/thepaper/featured"},
                ],
            },
            "guancha_headline": {
                "name": "观察者网",
                "category": "news",
                "weight": 6,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/guancha/headline"},
                ],
            },
            "juejin_ai": {
                "name": "掘金人工智能",
                "category": "tech",
                "weight": 7,
                "influence": "high",
                "sources": [
                    {"provider": "rsshub", "route": "/juejin/category/ai"},
                ],
            },
            "juejin_aicoding": {
                "name": "掘金AI编程",
                "category": "tech",
                "weight": 6,
                "influence": "medium",
                "sources": [
                    {"provider": "rsshub", "route": "/juejin/aicoding/AI编程/hot"},
                ],
            },
        }

        self.tianapi_fallback_info = {
            "name": "全网热搜榜",
            "category": "news",
            "weight": 8,
            "influence": "high",
            "sources": [
                {"provider": "tianapi", "endpoint": "/networkhot/index", "kind": "networkhot"},
            ],
        }

        self.category_weights = {
            "finance": 1.5,
            "social": 1.2,
            "news": 1.0,
            "tech": 0.8,
        }

        self.stop_words = {
            "的", "是", "在", "了", "和", "与", "等", "为", "将", "被",
            "有", "一", "个", "上", "下", "中", "大", "新", "年", "月", "日",
            "这", "那", "其", "之", "也", "要", "就", "不", "我", "你", "他",
            "来", "去", "到", "说", "会", "能", "都", "对", "着", "让",
            "从", "以", "及", "或", "如", "还", "没", "很", "更", "最",
        }

        self.noise_topic_words = {
            "一个", "一些", "一种", "哪些", "什么", "为何", "为什么", "如何", "怎么",
            "是否", "多少", "几个", "这个", "那个", "这些", "那些", "其中", "以及",
            "如果", "因为", "所以", "但是", "然后", "已经", "正在", "继续", "进行",
            "相关", "有关", "可能", "可以", "需要", "成为", "没有", "出现", "发布",
            "最新", "今日", "昨天", "今天", "明天", "目前", "此次", "其实", "真的",
            "注意", "问题", "情况", "内容", "消息", "表示", "介绍", "记者", "网友",
            "微博", "知乎", "热搜", "热榜", "热门", "新闻", "快讯", "视频", "公司",
            "财经", "观察者", "澎湃", "虎嗅", "掘金", "氪", "平台", "网友", "评论",
            "同比", "环比", "公告", "报道", "通过", "时间", "一季度", "二季度", "三季度", "四季度",
            "亿美元", "亿元", "万亿元",
        }
        self.noise_topic_pattern = re.compile(
            r"^(第?\d+|[\d.%+-]+|[一二三四五六七八九十百千万]+|[年月日天个只家位次条点分秒]+)$"
        )
        self.allowed_short_topic_words = {"AI", "GPU", "A股", "港股", "美股", "IPO", "CXO"}

    @staticmethod
    def _strip_html(text: str) -> str:
        value = html.unescape(str(text or ""))
        value = re.sub(r"<[^>]+>", " ", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value

    @staticmethod
    def _xml_local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    def _find_xml_text(self, element: ET.Element, names: tuple[str, ...]) -> str:
        target_names = set(names)
        for child in list(element):
            child_name = self._xml_local_name(child.tag)
            if child_name in target_names:
                href = child.attrib.get("href")
                if href:
                    return href.strip()
                if child.text:
                    return child.text.strip()
        return ""

    def _request(self, url: str, *, params: Optional[Dict] = None, expect_json: bool = True):
        response = requests.get(
            url,
            params=params,
            headers=self.default_headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json() if expect_json else response.text

    def _parse_rsshub_feed(self, feed_xml: str) -> List[Dict]:
        root = ET.fromstring(feed_xml)
        items = [node for node in root.iter() if self._xml_local_name(node.tag) in {"item", "entry"}]

        normalized = []
        for index, item in enumerate(items, start=1):
            title = self._find_xml_text(item, ("title",))
            if not title:
                continue
            link = self._find_xml_text(item, ("link", "id"))
            content = self._find_xml_text(item, ("description", "summary", "content"))
            publish_time = self._find_xml_text(item, ("pubDate", "published", "updated"))
            normalized.append(
                {
                    "title": self._strip_html(title),
                    "content": self._strip_html(content),
                    "url": link,
                    "source": "",
                    "publish_time": publish_time,
                    "rank": index,
                }
            )
        return normalized

    @staticmethod
    def _ensure_list(value) -> List[Dict]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            for nested in value.values():
                if isinstance(nested, list):
                    return [item for item in nested if isinstance(item, dict)]
            if value:
                return [value]
        return []

    def _normalize_tianapi_items(self, kind: str, items: List[Dict], platform_name: str) -> List[Dict]:
        normalized = []
        for index, item in enumerate(items, start=1):
            if kind == "networkhot":
                title = item.get("title") or ""
                content = item.get("digest") or ""
                heat_value = item.get("hotnum")
            else:
                title = item.get("title") or item.get("word") or item.get("keyword") or ""
                content = item.get("description") or item.get("digest") or ""
                heat_value = item.get("hotnum") or item.get("hotindex") or item.get("searchnum")

            title = self._strip_html(title)
            if not title:
                continue

            normalized.append(
                {
                    "title": title,
                    "content": self._strip_html(content),
                    "url": item.get("url") or "",
                    "source": platform_name,
                    "publish_time": "",
                    "rank": index,
                    "heat_value": heat_value,
                }
            )
        return normalized

    def _fetch_tianapi(self, source: Dict, platform_name: str) -> List[Dict]:
        if not self.tianapi_key:
            raise RuntimeError("未配置 TIANAPI_KEY")

        payload = self._request(
            f"{self.tianapi_base_url}{source['endpoint']}",
            params={"key": self.tianapi_key},
            expect_json=True,
        )
        if int(payload.get("code", 0)) != 200:
            raise RuntimeError(payload.get("msg") or "TianAPI 返回失败")

        items = self._ensure_list(payload.get("result"))
        return self._normalize_tianapi_items(source.get("kind", ""), items, platform_name)

    def _fetch_rsshub(self, source: Dict) -> List[Dict]:
        feed_xml = self._request(f"{self.rsshub_base_url}{source['route']}", expect_json=False)
        return self._parse_rsshub_feed(feed_xml)

    def _fetch_platform_items(self, platform: str, platform_info: Dict, source: Dict) -> List[Dict]:
        provider = source.get("provider")
        if provider == "rsshub":
            return self._fetch_rsshub(source)
        if provider == "tianapi":
            return self._fetch_tianapi(source, platform_info["name"])
        raise ValueError(f"不支持的新闻提供方: {provider}")

    def _build_result(self, platform: str, platform_info: Dict, items: List[Dict]) -> Dict:
        for index, item in enumerate(items, start=1):
            item["rank"] = item.get("rank") or index
            item["platform"] = platform
            item["source"] = item.get("source") or platform_info["name"]

        self._enrich_detail_text(items)

        return {
            "success": True,
            "platform": platform,
            "platform_name": platform_info["name"],
            "category": platform_info["category"],
            "weight": platform_info["weight"],
            "influence": platform_info.get("influence", "medium"),
            "data": items,
            "count": len(items),
            "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _enrich_detail_text(self, items: List[Dict]) -> None:
        if self.detail_item_limit <= 0:
            return

        for item in items[: self.detail_item_limit]:
            url = str(item.get("url") or "").strip()
            if not url or not self.detail_fetcher.supports_detail_url(url):
                continue
            detail_text = self.detail_fetcher.fetch_detail_text(url)
            if detail_text:
                item["content"] = detail_text
                item["content_origin"] = "detail_page"

    def _is_meaningful_topic_word(self, word: str) -> bool:
        token = str(word or "").strip()
        if token in self.allowed_short_topic_words:
            return True
        if len(token) < 2:
            return False
        if token in self.stop_words or token in self.noise_topic_words:
            return False
        if self.noise_topic_pattern.fullmatch(token):
            return False
        if re.fullmatch(r"[A-Za-z]{1,2}", token):
            return False
        if re.fullmatch(r"[\W_]+", token):
            return False
        return True

    def _get_default_platforms(self) -> List[str]:
        seen = set()
        ordered = []
        for category in ("social", "finance", "news", "tech"):
            for platform in self.CATEGORY_PLATFORM_PRIORITY.get(category, []):
                if platform in self.platforms and platform not in seen:
                    seen.add(platform)
                    ordered.append(platform)
        return ordered

    def _normalize_title_key(self, title: str) -> str:
        text = html.unescape(str(title or "")).strip().lower()
        if not text:
            return ""
        return re.sub(r"[\W_]+", "", text)

    def _title_ngrams(self, title: str) -> set[str]:
        if len(title) <= 2:
            return {title} if title else set()
        return {title[index : index + 2] for index in range(len(title) - 1)}

    def _title_token_set(self, title: str) -> set[str]:
        import jieba

        tokens = set()
        for token in jieba.cut(str(title or "")):
            normalized = str(token or "").strip()
            if self._is_meaningful_topic_word(normalized):
                tokens.add(normalized)
        return tokens

    def _title_token_list(self, title: str) -> List[str]:
        import jieba

        tokens = []
        for token in jieba.cut(str(title or "")):
            normalized = str(token or "").strip()
            if self._is_meaningful_topic_word(normalized):
                tokens.append(normalized)
        return tokens

    def _topic_phrase_candidates(self, title: str) -> List[Dict]:
        tokens = self._title_token_list(title)
        if not tokens:
            return []

        candidates = []
        seen = set()

        for index, token in enumerate(tokens):
            normalized = self._normalize_title_key(token)
            if normalized and normalized not in seen:
                seen.add(normalized)
                candidates.append({"topic": token, "multiplier": 1.0, "length": 1})

            for span, multiplier in ((2, 1.32), (3, 1.55)):
                phrase_tokens = tokens[index:index + span]
                if len(phrase_tokens) != span:
                    continue
                phrase = "".join(phrase_tokens)
                phrase_key = self._normalize_title_key(phrase)
                if (
                    not phrase_key
                    or phrase_key in seen
                    or len(phrase) < 4
                    or len(phrase) > 14
                ):
                    continue
                seen.add(phrase_key)
                candidates.append({"topic": phrase, "multiplier": multiplier, "length": span})

        return candidates

    def _titles_are_similar(self, left_title: str, right_title: str) -> bool:
        left = self._normalize_title_key(left_title)
        right = self._normalize_title_key(right_title)
        if not left or not right:
            return False
        if left == right:
            return True

        min_len = min(len(left), len(right))
        if min_len < 6:
            return False
        if min_len >= 8 and (left in right or right in left):
            return True

        ratio = SequenceMatcher(None, left, right).ratio()
        if ratio >= 0.82:
            return True

        left_ngrams = self._title_ngrams(left)
        right_ngrams = self._title_ngrams(right)
        if not left_ngrams or not right_ngrams:
            return False
        overlap = len(left_ngrams & right_ngrams)
        union = len(left_ngrams | right_ngrams)
        jaccard = overlap / union if union else 0.0
        same_prefix = left[:2] == right[:2]
        if same_prefix and jaccard >= 0.55 and overlap >= 3:
            return True
        if jaccard >= 0.72 and overlap >= 4:
            return True
        left_tokens = self._title_token_set(left_title)
        right_tokens = self._title_token_set(right_title)
        if left_tokens and right_tokens:
            token_overlap = left_tokens & right_tokens
            token_overlap_ratio = len(token_overlap) / max(1, min(len(left_tokens), len(right_tokens)))
            if len(token_overlap) >= 2 and token_overlap_ratio >= 0.6:
                return True
        return False

    def _build_platform_weight_context(self, platforms_data: List[Dict]) -> Dict[str, Dict]:
        family_platforms: Dict[str, List[Dict]] = {}
        for platform_data in platforms_data:
            if not platform_data.get("success"):
                continue
            platform = platform_data.get("platform")
            family = self.PLATFORM_FAMILIES.get(platform, platform)
            family_platforms.setdefault(family, []).append(platform_data)

        context: Dict[str, Dict] = {}
        for family, members in family_platforms.items():
            ranked_members = sorted(
                members,
                key=lambda item: (-float(item.get("weight", 0)), str(item.get("platform", ""))),
            )
            for index, member in enumerate(ranked_members):
                factor = self.FAMILY_WEIGHT_FACTORS[min(index, len(self.FAMILY_WEIGHT_FACTORS) - 1)]
                context[member["platform"]] = {
                    "family": family,
                    "family_rank": index + 1,
                    "effective_weight": round(float(member.get("weight", 0)) * factor, 2),
                }
        return context

    def _aggregate_entry_weight(self, effective_weights: List[float]) -> float:
        if not effective_weights:
            return 0.0
        ranked = sorted((float(weight) for weight in effective_weights), reverse=True)
        base = ranked[0]
        follow = sum(ranked[1:])
        bonus = min(follow * 0.35, base * 0.75)
        return round(base + bonus, 2)

    def _flatten_news_entries(self, platforms_data: List[Dict]) -> List[Dict]:
        weight_context = self._build_platform_weight_context(platforms_data)
        flattened = []
        for platform_data in platforms_data:
            if not platform_data.get("success"):
                continue

            platform = platform_data["platform"]
            platform_name = platform_data["platform_name"]
            category = platform_data["category"]
            context = weight_context.get(
                platform,
                {
                    "family": self.PLATFORM_FAMILIES.get(platform, platform),
                    "family_rank": 1,
                    "effective_weight": float(platform_data.get("weight", 0)),
                },
            )
            seen_keys = set()
            for news in platform_data.get("data", []):
                title = news.get("title") or ""
                key = self._normalize_title_key(title)
                if not title or not key or key in seen_keys:
                    continue
                seen_keys.add(key)
                flattened.append(
                    {
                        "key": key,
                        "title": title,
                        "content": news.get("content") or "",
                        "url": news.get("url") or "",
                        "publish_time": news.get("publish_time") or "",
                        "rank": int(news.get("rank", 99) or 99),
                        "platform": platform,
                        "platform_name": platform_name,
                        "category": category,
                        "source": news.get("source") or platform_name,
                        "weight": float(platform_data.get("weight", 0)),
                        "effective_weight": float(context["effective_weight"]),
                        "family": context["family"],
                    }
                )
        return flattened

    def _aggregate_news_entries(self, platforms_data: List[Dict]) -> List[Dict]:
        groups: Dict[str, Dict] = {}
        for entry in self._flatten_news_entries(platforms_data):
            matched_group_key = entry["key"] if entry["key"] in groups else None
            if matched_group_key is None:
                for group_key, group in groups.items():
                    if self._titles_are_similar(entry["title"], group["title"]):
                        matched_group_key = group_key
                        break

            group = groups.get(matched_group_key) if matched_group_key else None
            if group is None:
                groups[entry["key"]] = {
                    "key": entry["key"],
                    "entries": [entry],
                    "platforms": {entry["platform"]},
                    "platform_names": {entry["platform_name"]},
                    "sources": {entry["source"]},
                    "title": entry["title"],
                    "content": entry["content"],
                    "url": entry["url"],
                    "publish_time": entry["publish_time"],
                    "rank": entry["rank"],
                    "platform": entry["platform"],
                    "platform_name": entry["platform_name"],
                    "category": entry["category"],
                    "primary_weight": float(entry["effective_weight"]),
                }
                continue

            group["entries"].append(entry)
            group["platforms"].add(entry["platform"])
            group["platform_names"].add(entry["platform_name"])
            group["sources"].add(entry["source"])
            current_weight = float(entry["effective_weight"])
            current_rank = int(entry["rank"])
            should_replace_primary = current_weight > float(group["primary_weight"]) or (
                current_weight == float(group["primary_weight"]) and current_rank < int(group["rank"])
            )
            if should_replace_primary:
                group["title"] = entry["title"]
                group["content"] = entry["content"]
                group["url"] = entry["url"]
                group["publish_time"] = entry["publish_time"]
                group["rank"] = current_rank
                group["platform"] = entry["platform"]
                group["platform_name"] = entry["platform_name"]
                group["category"] = entry["category"]
                group["primary_weight"] = current_weight

        aggregated = []
        for group in groups.values():
            aggregate_weight = self._aggregate_entry_weight(
                [entry["effective_weight"] for entry in group["entries"]]
            )
            aggregated.append(
                {
                    "key": group["key"],
                    "title": group["title"],
                    "content": group["content"],
                    "url": group["url"],
                    "publish_time": group["publish_time"],
                    "rank": group["rank"],
                    "platform": group["platform"],
                    "platform_name": group["platform_name"],
                    "category": group["category"],
                    "platforms": sorted(group["platforms"]),
                    "platform_names": sorted(group["platform_names"]),
                    "sources": sorted(group["sources"]),
                    "cross_platform": len(group["platforms"]),
                    "aggregate_weight": aggregate_weight,
                    "entries": group["entries"],
                }
            )
        return aggregated

    def get_platform_news(self, platform: str) -> Dict:
        """获取单个平台的新闻数据"""
        platform_info = self.platforms.get(platform)
        if not platform_info:
            return {
                "success": False,
                "platform": platform,
                "error": "不支持的平台",
            }

        last_error = "无可用数据源"
        for source in platform_info.get("sources", []):
            provider = source.get("provider", "unknown")
            try:
                logger.info("正在获取 %s 平台数据（%s）...", platform, provider)
                items = self._fetch_platform_items(platform, platform_info, source)
                if items:
                    return self._build_result(platform, platform_info, items)
                last_error = f"{provider} 返回空数据"
                logger.warning("[%s] %s", platform, last_error)
            except requests.exceptions.Timeout:
                last_error = f"{provider} 请求超时（{self.timeout}秒）"
                logger.warning("[%s] %s", platform, last_error)
            except requests.exceptions.ConnectionError:
                last_error = f"{provider} 网络连接失败"
                logger.warning("[%s] %s", platform, last_error)
            except Exception as exc:
                last_error = f"{provider} 获取数据失败: {exc}"
                logger.warning("[%s] %s", platform, last_error)

        return {
            "success": False,
            "platform": platform,
            "error": last_error,
        }

    def _get_tianapi_fallback_news(self) -> Optional[Dict]:
        platform_info = self.tianapi_fallback_info
        last_error = ""
        for source in platform_info["sources"]:
            try:
                logger.info("正在获取 %s 兜底数据（%s）...", self.TIANAPI_FALLBACK_PLATFORM, source["provider"])
                items = self._fetch_tianapi(source, platform_info["name"])
                if items:
                    return self._build_result(self.TIANAPI_FALLBACK_PLATFORM, platform_info, items)
                last_error = "tianapi 返回空数据"
            except Exception as exc:
                last_error = str(exc)
                logger.warning("[%s] TianAPI 兜底失败: %s", self.TIANAPI_FALLBACK_PLATFORM, exc)

        if last_error:
            return {
                "success": False,
                "platform": self.TIANAPI_FALLBACK_PLATFORM,
                "error": last_error,
            }
        return None

    def get_multi_platform_news(self, platforms: List[str] = None, category: str = None) -> Dict:
        """获取多个平台的新闻数据"""
        if platforms is None:
            if category:
                preferred_platforms = self.CATEGORY_PLATFORM_PRIORITY.get(category)
                if preferred_platforms:
                    target_platforms = [code for code in preferred_platforms if code in self.platforms]
                else:
                    target_platforms = [
                        code for code, info in self.platforms.items()
                        if info.get("category") == category
                    ]
            else:
                target_platforms = self._get_default_platforms()
        else:
            target_platforms = platforms

        results = []
        success_count = 0
        failed_count = 0
        errors = []

        for index, platform in enumerate(target_platforms):
            result = self.get_platform_news(platform)
            results.append(result)

            if result["success"]:
                success_count += 1
            else:
                failed_count += 1
                errors.append(
                    {
                        "platform": platform,
                        "error": result.get("error", "未知错误"),
                    }
                )

            if index < len(target_platforms) - 1:
                time.sleep(self.request_pause_seconds)

        should_fallback_hotspots = (
            platforms is None
            and category in (None, "social", "news")
            and success_count == 0
        )
        if should_fallback_hotspots:
            fallback_result = self._get_tianapi_fallback_news()
            if fallback_result:
                results.append(fallback_result)
                if fallback_result["success"]:
                    success_count += 1
                else:
                    failed_count += 1
                    errors.append(
                        {
                            "platform": fallback_result["platform"],
                            "error": fallback_result.get("error", "未知错误"),
                        }
                    )

        return {
            "success": success_count > 0,
            "total_platforms": len(results),
            "success_count": success_count,
            "failed_count": failed_count,
            "errors": errors,
            "platforms_data": results,
            "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def extract_stock_related_news(self, platforms_data: List[Dict], keywords: List[str] = None) -> List[Dict]:
        """从新闻数据中提取股票相关的新闻"""
        if keywords is None:
            keywords = [
                "股", "股市", "股票", "A股", "港股", "美股", "创业板", "科创板", "北交所",
                "涨停", "跌停", "大涨", "暴涨", "飙升", "暴跌", "涨幅", "跌幅", "翻倍",
                "概念股", "龙头股", "妖股", "题材股", "白马股", "蓝筹股", "成长股",
                "上市", "IPO", "重组", "并购", "收购", "增发", "回购", "减持", "增持",
                "业绩", "财报", "利好", "利空", "预增", "预减", "盈利", "亏损",
                "牛市", "熊市", "反弹", "回调", "震荡", "突破", "新高",
                "主力", "游资", "北向资金", "外资", "机构", "资金流入", "资金流出",
                "板块", "行业", "赛道", "题材", "轮动", "热点",
                "芯片", "半导体", "光刻机", "封装", "存储",
                "新能源", "锂电", "光伏", "储能", "风电", "氢能",
                "AI", "人工智能", "大模型", "ChatGPT", "DeepSeek", "算力", "GPU",
                "机器人", "人形机器人", "工业机器人", "减速器", "伺服电机",
                "低空经济", "无人机", "eVTOL", "飞行汽车",
                "数据要素", "数字经济", "信创", "国产替代",
                "医药", "创新药", "CXO", "医疗器械", "中药",
                "消费", "白酒", "食品", "旅游", "免税",
                "军工", "国防", "航空", "航天", "船舶",
                "汽车", "新能源车", "智能驾驶", "无人驾驶", "充电桩",
                "地产", "房地产", "楼市", "房价",
                "金融", "银行", "保险", "券商", "证券",
                "政策", "利率", "降息", "降准", "货币政策", "财政政策",
                "国常会", "证监会", "央行", "发改委", "工信部",
            ]

        stock_related = []
        for entry in self._aggregate_news_entries(platforms_data):
            aggregated_text = " ".join(
                f"{nested_entry.get('title', '')} {nested_entry.get('content', '')}"
                for nested_entry in entry.get("entries", [])
            )
            matched_keywords = sorted({kw for kw in keywords if kw in aggregated_text})
            if not matched_keywords:
                continue

            rank_score = max(0, 100 - int(entry["rank"]) * 2)
            weight_score = float(entry["aggregate_weight"]) * 10
            keyword_score = len(matched_keywords) * 5
            cross_platform_bonus = max(0, int(entry["cross_platform"]) - 1) * 8
            stock_related.append(
                {
                    "platform": entry["platform"],
                    "platform_name": entry["platform_name"],
                    "platforms": entry["platforms"],
                    "platform_names": entry["platform_names"],
                    "category": entry["category"],
                    "weight": float(entry["aggregate_weight"]),
                    "effective_weight": float(entry["aggregate_weight"]),
                    "influence": "medium",
                    "rank": entry["rank"],
                    "title": entry["title"],
                    "content": entry["content"],
                    "url": entry["url"],
                    "source": " / ".join(entry["sources"]),
                    "publish_time": entry["publish_time"],
                    "matched_keywords": matched_keywords,
                    "keyword_count": len(matched_keywords),
                    "cross_platform": entry["cross_platform"],
                    "score": round(rank_score + weight_score + keyword_score + cross_platform_bonus, 2),
                }
            )
        stock_related.sort(
            key=lambda x: (
                -int(x.get("cross_platform", 1)),
                -float(x.get("score", 0)),
                int(x.get("rank", 99)),
            )
        )
        return stock_related

    def calculate_flow_score(self, platforms_data: List[Dict]) -> Dict:
        """计算流量得分"""
        scores = {"social": 0, "news": 0, "finance": 0, "tech": 0}
        platform_details = []
        aggregated_entries = self._aggregate_news_entries(platforms_data)
        platform_contribution: Dict[str, float] = {}
        platform_unique_count: Dict[str, int] = {}

        for entry in aggregated_entries:
            category = entry["category"]
            score = float(entry["aggregate_weight"])
            scores[category] = scores.get(category, 0) + score
            share = score / max(1, len(entry["platforms"]))
            for platform in entry["platforms"]:
                platform_contribution[platform] = platform_contribution.get(platform, 0) + share
                platform_unique_count[platform] = platform_unique_count.get(platform, 0) + 1

        weight_context = self._build_platform_weight_context(platforms_data)
        for platform_data in platforms_data:
            if not platform_data.get("success"):
                continue

            platform = platform_data["platform"]
            category = platform_data["category"]
            weight = platform_data["weight"]
            count = platform_data["count"]
            platform_name = platform_data["platform_name"]
            platform_details.append(
                {
                    "platform": platform,
                    "platform_name": platform_name,
                    "category": category,
                    "count": count,
                    "unique_count": int(platform_unique_count.get(platform, 0)),
                    "weight": weight,
                    "effective_weight": float(weight_context.get(platform, {}).get("effective_weight", weight)),
                    "score": round(float(platform_contribution.get(platform, 0)), 2),
                }
            )

        total_score = sum(scores.values())
        normalized_score = min(int(total_score / 50), 100) if total_score > 0 else 0
        dominant_categories = sorted(
            (
                (category, score)
                for category, score in scores.items()
                if score > 0
            ),
            key=lambda item: item[1],
            reverse=True,
        )[:2]
        category_labels = {
            "social": "社交",
            "news": "新闻",
            "finance": "财经",
            "tech": "科技",
        }
        category_hint = "、".join(category_labels.get(name, name) for name, _ in dominant_categories) or "全市场"

        if normalized_score >= 75:
            level = "极高"
            analysis = f"流量爆发，{category_hint}方向显著升温，短线热点密集扩散。建议：优先跟踪龙头与一线催化，但注意一致性过高后的追涨风险。"
        elif normalized_score >= 55:
            level = "高"
            analysis = f"流量较高，{category_hint}方向有较明确主线，资金活跃度较好。建议：围绕强势板块做跟踪，重视节奏与分化。"
        elif normalized_score >= 35:
            level = "中"
            analysis = f"流量处于常态，{category_hint}方向有一定热度但未形成全面共振。建议：保持观察，等待更明确的催化与扩散信号。"
        else:
            level = "低"
            analysis = f"流量偏低，当前未形成持续主线，{category_hint}方向也偏分散。建议：控制仓位，优先等待新催化出现。"

        return {
            "total_score": normalized_score,
            "social_score": scores.get("social", 0),
            "news_score": scores.get("news", 0),
            "finance_score": scores.get("finance", 0),
            "tech_score": scores.get("tech", 0),
            "level": level,
            "analysis": analysis,
            "platform_details": platform_details,
        }

    def get_hot_topics(self, platforms_data: List[Dict], top_n: int = 20) -> List[Dict]:
        """获取热门话题（基于去重标题的加权短语分析）"""
        aggregated_entries = self._aggregate_news_entries(platforms_data)
        topic_stats: Dict[str, Dict] = {}

        for entry in aggregated_entries:
            title = entry.get("title") or ""
            if not title:
                continue

            entry_weight = float(entry.get("aggregate_weight", 0))
            category_weight = float(self.category_weights.get(entry.get("category"), 1.0))
            cross_platform_bonus = 1 + max(0, int(entry.get("cross_platform", 1)) - 1) * 0.25
            topic_score = entry_weight * category_weight * cross_platform_bonus

            seen_tokens = set()
            for candidate in self._topic_phrase_candidates(title):
                topic = candidate["topic"]
                normalized = self._normalize_title_key(topic)
                if not normalized or normalized in seen_tokens:
                    continue
                seen_tokens.add(normalized)
                stats = topic_stats.setdefault(
                    topic,
                    {
                        "count": 0,
                        "score": 0.0,
                        "sources": set(),
                        "cross_platform": 0,
                        "length": 1,
                    },
                )
                stats["count"] += 1
                stats["score"] += topic_score * float(candidate.get("multiplier", 1.0))
                stats["sources"].update(entry.get("platform_names", []))
                stats["cross_platform"] = max(stats["cross_platform"], int(entry.get("cross_platform", 1)))
                stats["length"] = max(int(stats["length"]), int(candidate.get("length", 1)))

        ranked_topics = []
        for topic, stats in topic_stats.items():
            sources = sorted(stats["sources"])
            source_count = len(sources)
            if stats["count"] < 2 and stats["score"] < 14 and source_count < 2:
                continue
            ranked_topics.append(
                {
                    "topic": topic,
                    "count": int(stats["count"]),
                    "score": float(stats["score"]),
                    "cross_platform": max(source_count, int(stats["cross_platform"])),
                    "sources": sources[:5],
                    "length": int(stats.get("length", 1)),
                }
            )

        ranked_topics.sort(
            key=lambda item: (
                -float(item["score"]),
                -int(item["length"]),
                -int(item["count"]),
                -int(item["cross_platform"]),
                -len(str(item["topic"])),
            )
        )

        if not ranked_topics:
            return []

        selected_topics = []
        selected_keys: List[str] = []
        for item in ranked_topics:
            topic_key = self._normalize_title_key(item["topic"])
            if any(
                (topic_key in selected_key or selected_key in topic_key)
                and item["length"] <= selected_topics[index]["length"]
                and item["score"] <= selected_topics[index]["score"] * 1.1
                for index, selected_key in enumerate(selected_keys)
            ):
                continue
            selected_topics.append(item)
            selected_keys.append(topic_key)
            if len(selected_topics) >= top_n:
                break

        if not selected_topics:
            return []

        top_score = max(float(item["score"]) for item in selected_topics) or 1.0
        hot_topics = []
        for item in selected_topics:
            heat = max(20, min(100, int(item["score"] / top_score * 100)))
            hot_topics.append(
                {
                    "topic": item["topic"],
                    "count": item["count"],
                    "heat": heat,
                    "cross_platform": item["cross_platform"],
                    "sources": item["sources"],
                }
            )

        return hot_topics

    def get_platform_ranking(self, platforms_data: List[Dict]) -> List[Dict]:
        """获取跨平台热度排名"""
        all_news = []
        for entry in self._aggregate_news_entries(platforms_data):
            heat_score = round((100 - int(entry["rank"])) * float(entry["aggregate_weight"]), 2)
            all_news.append(
                {
                    "title": entry["title"],
                    "platform": entry["platform"],
                    "platform_name": entry["platform_name"],
                    "platforms": entry["platforms"],
                    "platform_names": entry["platform_names"],
                    "cross_platform": entry["cross_platform"],
                    "original_rank": entry["rank"],
                    "heat_score": heat_score,
                    "url": entry["url"] or "",
                    "content": entry["content"] or "",
                }
            )

        all_news.sort(key=lambda x: x["heat_score"], reverse=True)
        for i, news in enumerate(all_news):
            news["global_rank"] = i + 1
        return all_news

    def calculate_viral_coefficient(self, current_data: Dict, previous_data: Dict) -> Dict:
        """计算病毒系数K值"""
        current_score = current_data.get("total_score", 0)
        previous_score = previous_data.get("total_score", 0)

        if previous_score == 0:
            k_value = 1.0
            trend = "无历史数据"
            analysis = "首次采集，无法计算K值"
        else:
            k_value = round(current_score / previous_score, 2)
            if k_value > 1.5:
                trend = "指数型爆发"
                analysis = f"K值={k_value}，流量正在指数型增长！这是病毒式传播的特征，题材可能进入加速期。"
            elif k_value > 1.0:
                trend = "线性增长"
                analysis = f"K值={k_value}，流量稳步增长，题材正在发酵中。"
            elif k_value == 1.0:
                trend = "平稳"
                analysis = f"K值={k_value}，流量保持稳定，市场处于平衡状态。"
            else:
                trend = "衰减"
                analysis = f"K值={k_value}，流量正在衰减，题材热度下降，注意风险。"

        return {
            "k_value": k_value,
            "current_score": current_score,
            "previous_score": previous_score,
            "trend": trend,
            "analysis": analysis,
        }

    def detect_flow_type(self, history_scores: List[int], current_score: int) -> Dict:
        """识别流量类型（存量流量型 vs 增量流量型）"""
        if len(history_scores) < 2:
            return {
                "flow_type": "未知",
                "characteristics": ["历史数据不足"],
                "time_window": "无法判断",
                "operation": "继续观察",
            }

        initial_score = history_scores[0] if history_scores else 0
        growth_rates = []
        for i in range(1, len(history_scores)):
            if history_scores[i - 1] > 0:
                growth_rates.append((history_scores[i] - history_scores[i - 1]) / history_scores[i - 1])

        avg_growth = sum(growth_rates) / len(growth_rates) if growth_rates else 0
        if initial_score >= 500:
            flow_type = "存量流量型"
            characteristics = ["初始热度高", "流量快速到位", "可能与政策/大事件相关"]
            time_window = "时间窗口短（2-3天）"
            operation = "快进快出，密切关注热度变化"
        elif avg_growth > 0.2 and len([r for r in growth_rates if r > 0]) >= 2:
            flow_type = "增量流量型"
            characteristics = ["初始热度低", "逐步攀升", "具备病毒传播特征"]
            time_window = "时间窗口长（5-10天）"
            operation = "可以埋伏，等待加速"
        else:
            flow_type = "常规流量"
            characteristics = ["热度波动正常", "无明显趋势"]
            time_window = "无特定窗口"
            operation = "保持观望"

        return {
            "flow_type": flow_type,
            "characteristics": characteristics,
            "time_window": time_window,
            "operation": operation,
            "initial_score": initial_score,
            "current_score": current_score,
            "avg_growth": round(avg_growth * 100, 1),
        }

    def get_platform_list(self) -> List[Dict]:
        """获取所有支持的平台列表"""
        result = []
        for code, info in self.platforms.items():
            result.append(
                {
                    "code": code,
                    "name": info["name"],
                    "category": info["category"],
                    "weight": info["weight"],
                    "influence": info.get("influence", "medium"),
                }
            )
        result.sort(key=lambda x: x["weight"], reverse=True)
        return result

    def get_platforms_by_category(self) -> Dict[str, List[str]]:
        """按类别获取平台列表"""
        categories = {}
        for code, info in self.platforms.items():
            categories.setdefault(info["category"], []).append(code)
        return categories
