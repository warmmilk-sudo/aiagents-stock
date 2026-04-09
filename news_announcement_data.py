"""
新闻公告数据获取模块。

兼容旧接口，内部复用新的个股研究新闻聚合器。
"""

from stock_research_news_data import StockResearchNewsDataFetcher


class NewsAnnouncementDataFetcher:
    """新闻公告数据获取类"""
    
    def __init__(self):
        self.max_items = 20  # 最多获取的新闻/公告数量
        self.fetcher = StockResearchNewsDataFetcher(max_items=self.max_items)
    
    def get_news_and_announcements(self, symbol):
        """
        获取股票的新闻和公告数据
        
        Args:
            symbol: 股票代码（6位数字）
            
        Returns:
            dict: 包含新闻和公告数据的字典
        """
        print("📰📢 正在获取新闻公告聚合数据...")
        result = self.fetcher.get_stock_news(symbol)
        if result.get("data_success"):
            news_count = (result.get("news_data") or {}).get("count", 0)
            announcement_count = (result.get("announcement_data") or {}).get("count", 0)
            print(f"   ✓ 成功获取 {news_count} 条新闻、{announcement_count} 条公告")
            print("✅ 新闻公告数据获取完成")
        else:
            print("⚠️ 未能获取到新闻公告数据")
        return {
            "symbol": symbol,
            "news_data": result.get("news_data"),
            "announcement_data": result.get("announcement_data"),
            "supplemental_news_data": result.get("supplemental_news_data"),
            "data_success": result.get("data_success", False),
            "source": result.get("source"),
            "source_breakdown": result.get("source_breakdown"),
            "error": result.get("error"),
        }
    
    def _is_chinese_stock(self, symbol):
        """判断是否为中国股票"""
        return symbol.isdigit() and len(symbol) == 6
    
    def format_news_announcements_for_ai(self, data):
        """
        将新闻公告数据格式化为适合AI阅读的文本
        """
        return self.fetcher.format_news_for_ai(data)


# 测试函数
if __name__ == "__main__":
    print("测试新闻公告数据获取...")
    fetcher = NewsAnnouncementDataFetcher()
    
    # 测试平安银行
    symbol = "000001"
    print(f"\n正在获取 {symbol} 的新闻公告数据...\n")
    
    data = fetcher.get_news_and_announcements(symbol)
    
    if data.get("data_success"):
        print("\n" + "="*60)
        print("新闻公告数据获取成功！")
        print("="*60)
        
        formatted_text = fetcher.format_news_announcements_for_ai(data)
        print(formatted_text)
    else:
        print(f"\n获取失败: {data.get('error', '未知错误')}")
