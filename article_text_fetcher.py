"""
正文抓取与裁剪模块。

为个股研究链路补充免费正文摘录：
- 巨潮 PDF：抽取前几页正文
- 澎湃 / 观察者 / 36氪 / 虎嗅 等详情页：抽取正文段落

所有正文都会经过严格裁剪，避免直接把超长原文塞给 AI。
"""

from __future__ import annotations

import io
import logging
import os
import re
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

try:
    from curl_cffi import requests as curl_requests
except Exception:  # pragma: no cover - optional dependency
    curl_requests = None

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None


class ArticleTextFetcher:
    """文章与 PDF 正文抓取器。"""

    DOMAIN_SELECTORS = {
        "www.thepaper.cn": [
            "article",
            "[class*='content']",
            "[class*='article']",
            ".news_txt",
        ],
        "m.thepaper.cn": [
            "article",
            "[class*='content']",
            "[class*='article']",
        ],
        "www.guancha.cn": [
            "article",
            ".content",
            ".all-txt",
            "[class*='content']",
            "[class*='article']",
        ],
        "www.36kr.com": [
            "article",
            "[class*='article']",
            "[class*='content']",
            "[class*='rich-text']",
        ],
        "36kr.com": [
            "article",
            "[class*='article']",
            "[class*='content']",
            "[class*='rich-text']",
        ],
        "www.huxiu.com": [
            "article",
            "[class*='article']",
            "[class*='content']",
        ],
        "m.huxiu.com": [
            "article",
            "[class*='article']",
            "[class*='content']",
        ],
    }

    SUPPORTED_DETAIL_HOSTS = frozenset(DOMAIN_SELECTORS.keys())
    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        ),
    }
    WAF_HOSTS = frozenset({"www.huxiu.com", "m.huxiu.com"})

    def __init__(
        self,
        *,
        timeout: int = 15,
        html_char_limit: int = 2400,
        pdf_char_limit: int = 2800,
        pdf_page_limit: int = 3,
    ) -> None:
        self.timeout = max(5, int(timeout))
        self.html_char_limit = max(600, int(html_char_limit))
        self.pdf_char_limit = max(800, int(pdf_char_limit))
        self.pdf_page_limit = max(1, int(pdf_page_limit))

    @classmethod
    def supports_detail_url(cls, url: str) -> bool:
        host = urlparse(str(url or "")).netloc.lower()
        return host in cls.SUPPORTED_DETAIL_HOSTS

    @staticmethod
    def _clean_text(text: str) -> str:
        value = re.sub(r"\s+", " ", str(text or "")).strip()
        value = re.sub(r"[ \t]+", " ", value)
        return value

    @staticmethod
    def _trim_text(text: str, char_limit: int) -> str:
        value = ArticleTextFetcher._clean_text(text)
        if len(value) <= char_limit:
            return value
        cutoff = value[:char_limit]
        last_punct = max(cutoff.rfind(mark) for mark in ("。", "！", "？", ".", "!", "?", "；", ";"))
        if last_punct >= max(120, char_limit // 3):
            cutoff = cutoff[: last_punct + 1]
        return cutoff.rstrip() + "..."

    @staticmethod
    def _is_waf_page(text: str) -> bool:
        body = str(text or "")
        return "_waf_" in body or "aliyun_waf_" in body or "waf-nc-h5" in body

    def _request_with_impersonation(self, url: str):
        if curl_requests is None:
            return None
        response = curl_requests.get(url, impersonate="chrome136", timeout=self.timeout, headers=self.DEFAULT_HEADERS)
        response.raise_for_status()
        return response

    def _request(self, url: str):
        response = requests.get(url, headers=self.DEFAULT_HEADERS, timeout=self.timeout)
        response.raise_for_status()
        host = urlparse(url).netloc.lower()
        if host in self.WAF_HOSTS and self._is_waf_page(getattr(response, "text", "")):
            fallback = self._request_with_impersonation(url)
            if fallback is not None:
                return fallback
        return response

    def _collect_candidate_blocks(self, soup: BeautifulSoup, selectors: Iterable[str]) -> list[str]:
        blocks: list[str] = []
        seen = set()
        for selector in selectors:
            for node in soup.select(selector):
                text = self._extract_text_from_node(node)
                normalized = self._clean_text(text)
                if len(normalized) < 50 or normalized in seen:
                    continue
                seen.add(normalized)
                blocks.append(normalized)
        return blocks

    def _extract_text_from_node(self, node) -> str:
        paragraphs = []
        for paragraph in node.select("p"):
            text = self._clean_text(paragraph.get_text(" ", strip=True))
            if len(text) >= 10:
                paragraphs.append(text)
        if paragraphs:
            return "\n".join(paragraphs)
        return self._clean_text(node.get_text(" ", strip=True))

    def _extract_html_text(self, html_text: str, url: str) -> str:
        soup = BeautifulSoup(html_text, "lxml")
        for selector in ("script", "style", "noscript", "header", "footer", "nav", "aside", "form"):
            for node in soup.select(selector):
                node.decompose()

        host = urlparse(url).netloc.lower()
        selectors = list(self.DOMAIN_SELECTORS.get(host, []))
        selectors.extend(["article", "main", "[class*='content']", "[class*='article']"])
        candidate_blocks = self._collect_candidate_blocks(soup, selectors)

        if not candidate_blocks:
            paragraphs = []
            for paragraph in soup.find_all("p"):
                text = self._clean_text(paragraph.get_text(" ", strip=True))
                if len(text) >= 10:
                    paragraphs.append(text)
            candidate_text = "\n".join(paragraphs)
        else:
            candidate_text = max(candidate_blocks, key=len)

        return self._trim_text(candidate_text, self.html_char_limit)

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        if PdfReader is None:
            return ""

        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages[: self.pdf_page_limit]:
            try:
                page_text = self._clean_text(page.extract_text() or "")
            except Exception:
                page_text = ""
            if page_text:
                parts.append(page_text)

        return self._trim_text("\n".join(parts), self.pdf_char_limit)

    def fetch_detail_text(self, url: str) -> str:
        value = str(url or "").strip()
        if not value:
            return ""
        try:
            response = self._request(value)
        except Exception as exc:
            logger.warning("正文抓取失败 %s: %s", value, exc)
            return ""

        content_type = str(response.headers.get("Content-Type") or "").lower()
        if value.lower().endswith(".pdf") or "application/pdf" in content_type:
            return self._extract_pdf_text(response.content)

        return self._extract_html_text(response.text, value)


def get_detail_fetcher() -> ArticleTextFetcher:
    return ArticleTextFetcher(
        timeout=int(os.getenv("ARTICLE_DETAIL_TIMEOUT_SECONDS", "15")),
        html_char_limit=int(os.getenv("ARTICLE_DETAIL_HTML_CHAR_LIMIT", "2400")),
        pdf_char_limit=int(os.getenv("ARTICLE_DETAIL_PDF_CHAR_LIMIT", "2800")),
        pdf_page_limit=int(os.getenv("ARTICLE_DETAIL_PDF_PAGE_LIMIT", "3")),
    )
