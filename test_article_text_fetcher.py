import unittest
from unittest.mock import Mock, patch

from article_text_fetcher import ArticleTextFetcher


class ArticleTextFetcherTests(unittest.TestCase):
    def test_supports_known_detail_hosts(self):
        self.assertTrue(ArticleTextFetcher.supports_detail_url("https://www.thepaper.cn/newsDetail_forward_1"))
        self.assertTrue(ArticleTextFetcher.supports_detail_url("https://www.guancha.cn/politics/2026_04_09_1.shtml"))
        self.assertTrue(ArticleTextFetcher.supports_detail_url("https://www.36kr.com/p/123"))
        self.assertTrue(ArticleTextFetcher.supports_detail_url("https://www.huxiu.com/article/123.html"))
        self.assertFalse(ArticleTextFetcher.supports_detail_url("https://example.com/article"))

    def test_extract_html_text_prefers_article_body(self):
        fetcher = ArticleTextFetcher(html_char_limit=500)
        html = """
        <html><body>
          <article>
            <p>第一段正文，描述公司基本情况和事件背景。</p>
            <p>第二段正文，继续补充影响和细节。</p>
            <p>第三段正文，提供进一步观察点。</p>
          </article>
          <div class="sidebar"><p>无关短句</p></div>
        </body></html>
        """
        text = fetcher._extract_html_text(html, "https://www.thepaper.cn/example")
        self.assertIn("第一段正文", text)
        self.assertIn("第二段正文", text)

    @patch("article_text_fetcher.curl_requests")
    @patch("article_text_fetcher.requests.get")
    def test_huxiu_waf_page_falls_back_to_impersonation(self, requests_get, curl_requests):
        fetcher = ArticleTextFetcher(html_char_limit=500)

        waf_response = Mock()
        waf_response.text = "<html>aliyun_waf_aa _waf_</html>"
        waf_response.raise_for_status.return_value = None

        real_response = Mock()
        real_response.text = (
            "<html><body><div class='article-body'>"
            "<p>虎嗅正文第一段，包含足够长度的内容用于提取。</p>"
            "<p>虎嗅正文第二段，继续补充更多细节与背景。</p>"
            "</div></body></html>"
        )
        real_response.raise_for_status.return_value = None

        requests_get.return_value = waf_response
        curl_requests.get.return_value = real_response

        text = fetcher.fetch_detail_text("https://www.huxiu.com/article/123.html")

        self.assertIn("虎嗅正文第一段", text)
        curl_requests.get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
