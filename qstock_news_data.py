"""
新闻数据获取模块
Tushare优先获取股票最新新闻，AkShare兜底
"""

import pandas as pd
import sys
import io
import warnings
from datetime import datetime, timedelta
import akshare as ak
from data_source_manager import data_source_manager
from data_source_policy import policy

warnings.filterwarnings('ignore')

# 设置标准输出编码为UTF-8（仅在命令行环境，避免streamlit冲突）
def _setup_stdout_encoding():
    """仅在命令行环境设置标准输出编码"""
    if sys.platform == 'win32' and not hasattr(sys.stdout, '_original_stream'):
        try:
            # 检测是否在streamlit环境中
            import streamlit
            # 在streamlit中不修改stdout
            return
        except ImportError:
            # 不在streamlit环境，可以安全修改
            try:
                original_stdout = sys.stdout
                wrapped = io.TextIOWrapper(original_stdout.buffer, encoding='utf-8', errors='ignore')
                setattr(wrapped, '_original_stream', original_stdout)
                sys.stdout = wrapped
            except Exception:
                pass

_setup_stdout_encoding()


class QStockNewsDataFetcher:
    """新闻数据获取类（Tushare优先，AkShare兜底）"""
    
    def __init__(self):
        self.max_items = 30  # 最多获取的新闻数量
        self.available = True
        self.ts_pro = policy.tushare_api
        self.prefer_tushare = bool(self.ts_pro)
        source_text = "tushare-first" if self.prefer_tushare else "akshare-only/fallback"
        print(f"[OK] 新闻数据获取器初始化成功（{source_text}）")
    
    def get_stock_news(self, symbol):
        """
        获取股票的新闻数据
        
        Args:
            symbol: 股票代码（6位数字）
            
        Returns:
            dict: 包含新闻数据的字典
        """
        data = {
            "symbol": symbol,
            "news_data": None,
            "data_success": False,
            "source": "unknown",
            "source_chain": [],
            "error_detail": {}
        }
        
        if not self.available:
            data["error"] = "新闻数据模块不可用"
            return data
        
        # 只支持中国股票
        if not self._is_chinese_stock(symbol):
            data["error"] = "新闻数据仅支持中国A股股票"
            return data
        
        try:
            # 获取新闻数据
            print(f"[INFO] 正在获取 {symbol} 的最新新闻...")
            news_data = self._get_news_data(symbol)
            
            if news_data:
                data["news_data"] = news_data
                print(f"   [OK] 成功获取 {len(news_data.get('items', []))} 条新闻")
                data["data_success"] = True
                data["source"] = news_data.get("source", "unknown")
                data["source_chain"] = news_data.get("source_chain", [])
                data["error_detail"] = news_data.get("error_detail", {})
                print("[OK] 新闻数据获取完成")
            else:
                print("[WARN]️ 未能获取到新闻数据")
                
        except Exception as e:
            print(f"[ERR] 获取新闻数据失败: {e}")
            data["error"] = str(e)
            data["error_detail"] = {"unknown": str(e)}
        
        return data
    
    def _is_chinese_stock(self, symbol):
        """判断是否为中国股票"""
        return symbol.isdigit() and len(symbol) == 6
    
    def _get_news_data(self, symbol):
        """获取新闻数据（Tushare优先，AkShare兜底）"""
        source_chain = []
        errors = {}

        def _from_tushare():
            if not self.ts_pro:
                return []

            ts_code = data_source_manager._convert_to_ts_code(symbol)
            stock_name = None
            try:
                basic_df = self.ts_pro.stock_basic(ts_code=ts_code, fields='name')
                if basic_df is not None and not basic_df.empty:
                    stock_name = str(basic_df.iloc[0].get('name', '')).strip()
            except Exception:
                stock_name = None

            start_time = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d %H:%M:%S')
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df = self.ts_pro.news(start_date=start_time, end_date=end_time, src='sina')
            if df is None or df.empty:
                return []

            keywords = [symbol]
            if stock_name:
                keywords.append(stock_name)

            items = []
            for _, row in df.iterrows():
                title = str(row.get('title', '') or '')
                content = str(row.get('content', '') or '')
                joined = f"{title} {content}"
                if not any(k and k in joined for k in keywords):
                    continue
                items.append({
                    "source": "tushare.sina",
                    "title": title,
                    "content": content,
                    "date": str(row.get('datetime', '') or ''),
                    "url": str(row.get('url', '') or '')
                })
                if len(items) >= self.max_items:
                    break
            return items

        def _from_akshare():
            news_items = []

            try:
                df = ak.stock_news_em(symbol=symbol)
                if df is not None and not df.empty:
                    for _, row in df.head(self.max_items).iterrows():
                        item = {'source': '东方财富'}
                        for col in df.columns:
                            value = row.get(col)
                            if value is None or (isinstance(value, float) and pd.isna(value)):
                                continue
                            try:
                                item[col] = str(value)
                            except Exception:
                                item[col] = "无法解析"
                        if len(item) > 1:
                            news_items.append(item)
            except Exception as e:
                errors["akshare.news_em"] = str(e)

            if not news_items:
                try:
                    df_info = ak.stock_zh_a_spot_em()
                    stock_name = None
                    if df_info is not None and not df_info.empty:
                        match = df_info[df_info['代码'] == symbol]
                        if not match.empty:
                            stock_name = match.iloc[0]['名称']
                    if stock_name:
                        df = ak.stock_news_sina(symbol=stock_name)
                        if df is not None and not df.empty:
                            for _, row in df.head(self.max_items).iterrows():
                                item = {'source': '新浪财经'}
                                for col in df.columns:
                                    value = row.get(col)
                                    if value is None or (isinstance(value, float) and pd.isna(value)):
                                        continue
                                    try:
                                        item[col] = str(value)
                                    except Exception:
                                        item[col] = "无法解析"
                                if len(item) > 1:
                                    news_items.append(item)
                except Exception as e:
                    errors["akshare.news_sina"] = str(e)

            if not news_items or len(news_items) < 5:
                try:
                    df = ak.stock_news_cls()
                    if df is not None and not df.empty:
                        df_filtered = df[
                            df['内容'].str.contains(symbol, na=False) |
                            df['标题'].str.contains(symbol, na=False)
                        ]
                        for _, row in df_filtered.head(max(self.max_items - len(news_items), 0)).iterrows():
                            item = {'source': '财联社'}
                            for col in df_filtered.columns:
                                value = row.get(col)
                                if value is None or (isinstance(value, float) and pd.isna(value)):
                                    continue
                                try:
                                    item[col] = str(value)
                                except Exception:
                                    item[col] = "无法解析"
                            if len(item) > 1:
                                news_items.append(item)
                except Exception as e:
                    errors["akshare.news_cls"] = str(e)

            return news_items[:self.max_items]

        ordered = []
        if self.prefer_tushare:
            ordered.append(("tushare", _from_tushare))
        ordered.append(("akshare", _from_akshare))

        for src_name, src_func in ordered:
            source_chain.append(src_name)
            try:
                print(f"   使用 {src_name} 获取新闻...")
                news_items = src_func()
                if news_items:
                    return {
                        "items": news_items,
                        "count": len(news_items),
                        "query_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "date_range": "最近新闻",
                        "source": src_name,
                        "source_chain": source_chain,
                        "error_detail": errors
                    }
                errors[src_name] = "empty result"
            except Exception as e:
                errors[src_name] = f"{type(e).__name__}: {e}"

        print(f"   未找到股票 {symbol} 的新闻")
        return None
    
    def format_news_for_ai(self, data):
        """
        将新闻数据格式化为适合AI阅读的文本
        """
        if not data or not data.get("data_success"):
            return "未能获取新闻数据"
        
        text_parts = []
        
        # 新闻数据
        if data.get("news_data"):
            news_data = data["news_data"]
            source = data.get("source", "unknown")
            text_parts.append(f"""
【最新新闻 - {source}】
查询时间：{news_data.get('query_time', 'N/A')}
时间范围：{news_data.get('date_range', 'N/A')}
新闻数量：{news_data.get('count', 0)}条

""")
            
            for idx, item in enumerate(news_data.get('items', []), 1):
                text_parts.append(f"新闻 {idx}:")
                
                # 优先显示的字段
                priority_fields = ['title', 'date', 'time', 'source', 'content', 'url']
                
                # 先显示优先字段
                for field in priority_fields:
                    if field in item:
                        value = item[field]
                        # 限制content长度
                        if field == 'content' and len(str(value)) > 500:
                            value = str(value)[:500] + "..."
                        text_parts.append(f"  {field}: {value}")
                
                # 再显示其他字段
                for key, value in item.items():
                    if key not in priority_fields and key != 'source':
                        # 跳过过长的字段
                        if len(str(value)) > 300:
                            value = str(value)[:300] + "..."
                        text_parts.append(f"  {key}: {value}")
                
                text_parts.append("")  # 空行分隔
        
        return "\n".join(text_parts)


# 测试函数
if __name__ == "__main__":
    print("测试新闻数据获取（Tushare优先 + AkShare兜底）...")
    print("="*60)
    
    fetcher = QStockNewsDataFetcher()
    
    if not fetcher.available:
        print("[ERR] 新闻数据获取器不可用")
        sys.exit(1)
    
    # 测试股票
    test_symbols = ["000001", "600519"]  # 平安银行、贵州茅台
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"正在测试股票: {symbol}")
        print(f"{'='*60}\n")
        
        data = fetcher.get_stock_news(symbol)
        
        if data.get("data_success"):
            print("\n" + "="*60)
            print("新闻数据获取成功！")
            print("="*60)
            
            formatted_text = fetcher.format_news_for_ai(data)
            print(formatted_text)
        else:
            print(f"\n获取失败: {data.get('error', '未知错误')}")
        
        print("\n")


