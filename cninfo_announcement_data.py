"""
巨潮资讯公告抓取模块。

使用巨潮资讯站内搜索解析股票元信息，再调用公告查询接口获取个股公告。
"""

from __future__ import annotations

import re
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote, urljoin

import requests

from article_text_fetcher import get_detail_fetcher


class CninfoAnnouncementFetcher:
    """巨潮资讯个股公告抓取器。"""

    BASE_URL = "https://www.cninfo.com.cn"
    SEARCH_URL = f"{BASE_URL}/new/information/topSearch/query"
    STOCK_PAGE_URL = f"{BASE_URL}/new/disclosure/stock"
    ANNOUNCEMENT_QUERY_URL = f"{BASE_URL}/new/hisAnnouncement/query"
    STATIC_FILE_BASE_URL = "https://static.cninfo.com.cn/"
    SOURCE_NAME = "巨潮资讯"
    CN_TZ = timezone(timedelta(hours=8))

    _PLATE_SHORT_MAP = {
        "sse": "sh",
        "szse": "sz",
    }
    _PLATE_COLUMN_MAP = {
        "sse": "sse",
        "szse": "szse",
    }

    def __init__(
        self,
        timeout: int = 15,
        max_items: int = 20,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.timeout = max(5, int(timeout))
        self.max_items = max(1, int(max_items))
        self.session = session or requests.Session()
        self.detail_fetcher = get_detail_fetcher()
        self.full_text_enabled = os.getenv("CNINFO_FULLTEXT_ENABLED", "true").strip().lower() not in {"0", "false", "off", "no"}
        self.full_text_item_limit = max(1, int(os.getenv("CNINFO_FULLTEXT_ITEM_LIMIT", "3")))
        self.default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.BASE_URL}/",
        }

    @staticmethod
    def _is_chinese_stock(symbol: str) -> bool:
        return symbol.isdigit() and len(symbol) == 6

    @staticmethod
    def _format_publish_time(timestamp_ms: Any) -> str:
        if timestamp_ms in (None, ""):
            return ""
        try:
            return datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=CninfoAnnouncementFetcher.CN_TZ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except Exception:
            return str(timestamp_ms)

    @classmethod
    def _build_pdf_url(cls, adjunct_url: str) -> str:
        value = str(adjunct_url or "").strip()
        if not value:
            return ""
        if value.startswith("http://") or value.startswith("https://"):
            return value
        return urljoin(cls.STATIC_FILE_BASE_URL, value.lstrip("/"))

    def _request_json(self, url: str, *, method: str = "GET", **kwargs) -> Any:
        headers = dict(self.default_headers)
        headers.update(kwargs.pop("headers", {}))
        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            timeout=self.timeout,
            **kwargs,
        )
        response.raise_for_status()
        return response.json()

    def _request_text(self, url: str, **kwargs) -> str:
        headers = dict(self.default_headers)
        headers.update(kwargs.pop("headers", {}))
        response = self.session.get(url, headers=headers, timeout=self.timeout, **kwargs)
        response.raise_for_status()
        return response.text

    def _resolve_stock_metadata(self, symbol: str) -> Optional[Dict[str, str]]:
        payload = self._request_json(
            self.SEARCH_URL,
            method="POST",
            data={"keyWord": symbol},
        )
        if not isinstance(payload, list):
            return None

        target = None
        for item in payload:
            if not isinstance(item, dict):
                continue
            if item.get("code") == symbol and item.get("category") == "A股":
                target = item
                break

        if not target:
            return None

        org_id = str(target.get("orgId") or "").strip()
        if not org_id:
            return None

        stock_page_url = f"{self.STOCK_PAGE_URL}?orgId={quote(org_id)}&stockCode={quote(symbol)}"
        stock_page = self._request_text(
            stock_page_url,
            headers={"Referer": stock_page_url, "X-Requested-With": ""},
        )
        plate_match = re.search(r'var\s+plate\s*=\s*"([^"]+)"', stock_page)
        plate = plate_match.group(1).strip() if plate_match else ""
        if plate not in self._PLATE_SHORT_MAP:
            return None

        return {
            "symbol": symbol,
            "name": str(target.get("zwjc") or symbol),
            "org_id": org_id,
            "plate": plate,
            "plate_short": self._PLATE_SHORT_MAP[plate],
            "column": self._PLATE_COLUMN_MAP[plate],
        }

    def _normalize_announcement_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        title = str(item.get("announcementTitle") or item.get("title") or "").strip()
        if not title:
            return None

        return {
            "title": title,
            "content": "",
            "publish_time": self._format_publish_time(item.get("announcementTime")),
            "source": self.SOURCE_NAME,
            "url": self._build_pdf_url(item.get("adjunctUrl")),
            "announcement_id": str(item.get("announcementId") or ""),
            "symbol": str(item.get("secCode") or ""),
            "name": str(item.get("secName") or ""),
            "org_id": str(item.get("orgId") or ""),
            "file_type": str(item.get("adjunctType") or ""),
            "file_size_kb": item.get("adjunctSize"),
            "announcement_type": str(item.get("announcementType") or ""),
            "raw_category": str(item.get("columnId") or ""),
        }

    def _enrich_announcement_full_text(self, items: list[Dict[str, Any]]) -> None:
        if not self.full_text_enabled:
            return

        for item in items[: self.full_text_item_limit]:
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            detail_text = self.detail_fetcher.fetch_detail_text(url)
            if detail_text:
                item["content"] = detail_text
                item["content_origin"] = "pdf_excerpt"

    def get_stock_announcements(
        self,
        symbol: str,
        *,
        limit: Optional[int] = None,
        category: str = "",
    ) -> Dict[str, Any]:
        result = {
            "symbol": symbol,
            "announcement_data": None,
            "data_success": False,
            "source": "cninfo_announcement",
        }

        if not self._is_chinese_stock(symbol):
            result["error"] = "公告数据仅支持中国A股股票"
            return result

        try:
            stock_meta = self._resolve_stock_metadata(symbol)
            if not stock_meta:
                result["error"] = "未找到股票对应的巨潮资讯信息"
                return result

            page_size = max(1, min(int(limit or self.max_items), 50))
            stock_page_url = (
                f"{self.STOCK_PAGE_URL}?orgId={quote(stock_meta['org_id'])}&stockCode={quote(symbol)}"
            )
            payload = self._request_json(
                self.ANNOUNCEMENT_QUERY_URL,
                method="POST",
                data={
                    "stock": f"{symbol},{stock_meta['org_id']}",
                    "tabName": "fulltext",
                    "pageSize": page_size,
                    "pageNum": 1,
                    "column": stock_meta["column"],
                    "category": category,
                    "plate": stock_meta["plate_short"],
                    "seDate": "",
                    "searchkey": "",
                    "secid": "",
                    "sortName": "",
                    "sortType": "",
                    "isHLtitle": "true",
                },
                headers={"Referer": stock_page_url},
            )

            announcements = []
            for item in payload.get("announcements", []) if isinstance(payload, dict) else []:
                if not isinstance(item, dict):
                    continue
                normalized = self._normalize_announcement_item(item)
                if normalized:
                    announcements.append(normalized)

            self._enrich_announcement_full_text(announcements)

            result["announcement_data"] = {
                "items": announcements,
                "count": len(announcements),
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "stock_name": stock_meta["name"],
                "org_id": stock_meta["org_id"],
                "plate": stock_meta["column"],
                "source": self.SOURCE_NAME,
                "total": int(payload.get("totalAnnouncement", len(announcements))) if isinstance(payload, dict) else len(announcements),
            }
            result["data_success"] = len(announcements) > 0
            if not result["data_success"]:
                result["error"] = "未获取到公告数据"
            return result
        except requests.RequestException as exc:
            result["error"] = f"巨潮资讯请求失败: {exc}"
            return result
        except Exception as exc:
            result["error"] = str(exc)
            return result

    def format_announcements_for_ai(self, data: Dict[str, Any]) -> str:
        if not data or not data.get("data_success"):
            return "未能获取公告数据"

        announcement_data = data.get("announcement_data") or {}
        items = announcement_data.get("items") or []
        if not items:
            return "未能获取公告数据"

        parts = [
            f"""
【最新公告】
数据源：{announcement_data.get('source', self.SOURCE_NAME)}
股票：{announcement_data.get('stock_name', data.get('symbol', 'N/A'))}
查询时间：{announcement_data.get('query_time', 'N/A')}
公告数量：{announcement_data.get('count', 0)}条

"""
        ]
        for idx, item in enumerate(items, 1):
            parts.append(f"公告 {idx}:")
            for field in ["title", "publish_time", "url", "file_type"]:
                value = item.get(field)
                if value:
                    parts.append(f"  {field}: {value}")
            parts.append("")
        return "\n".join(parts)


if __name__ == "__main__":
    fetcher = CninfoAnnouncementFetcher()
    sample = fetcher.get_stock_announcements("600519")
    print(sample.get("error") or f"获取到 {((sample.get('announcement_data') or {}).get('count'))} 条公告")
    print(fetcher.format_announcements_for_ai(sample))
