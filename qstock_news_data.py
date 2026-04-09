"""
新闻数据获取模块
使用 pywencai 获取个股相关新闻，不再依赖 AkShare。
"""

import ast
import io
import sys
import warnings
from datetime import datetime

import pandas as pd
from pywencai_runtime import setup_pywencai_runtime_env

setup_pywencai_runtime_env()
import pywencai

from data_source_manager import data_source_manager

warnings.filterwarnings("ignore")


def _setup_stdout_encoding():
    """在Windows命令行环境设置标准输出编码。"""
    if sys.platform == "win32" and not hasattr(sys.stdout, "_original_stream"):
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="ignore")
        except Exception:
            pass


_setup_stdout_encoding()


class QStockNewsDataFetcher:
    """新闻数据获取类（pywencai 主源）"""

    def __init__(self):
        self.max_items = 30
        self.available = True
        print("✓ 新闻数据获取器初始化成功（pywencai 主源）")

    def get_stock_news(self, symbol):
        data = {
            "symbol": symbol,
            "news_data": None,
            "data_success": False,
            "source": "pywencai_news",
        }

        if not self.available:
            data["error"] = "新闻数据源不可用"
            return data

        if not self._is_chinese_stock(symbol):
            data["error"] = "新闻数据仅支持中国A股股票"
            return data

        try:
            print(f"📰 正在使用 pywencai 获取 {symbol} 的最新新闻...")
            news_data = self._get_news_data(symbol)
            if news_data:
                data["news_data"] = news_data
                data["data_success"] = True
                data["source"] = news_data.get("source", data["source"])
                print(f"   ✓ 成功获取 {len(news_data.get('items', []))} 条新闻")
                print("✅ 新闻数据获取完成")
            else:
                print("⚠️ 未能获取到新闻数据")
        except Exception as e:
            print(f"❌ 获取新闻数据失败: {e}")
            data["error"] = str(e)

        return data

    def _is_chinese_stock(self, symbol):
        return symbol.isdigit() and len(symbol) == 6

    def _resolve_stock_name(self, symbol):
        if data_source_manager.tushare_available:
            try:
                ts_code = data_source_manager._convert_to_ts_code(symbol)
                df = data_source_manager.tushare_api.stock_basic(
                    ts_code=ts_code,
                    fields="ts_code,name",
                )
                if df is not None and not df.empty:
                    stock_name = df.iloc[0]["name"]
                    print(f"   [Tushare] 找到股票名称: {stock_name}")
                    return stock_name
            except Exception as e:
                print(f"   [Tushare] 获取股票名称失败: {e}")
        return None

    @staticmethod
    def _normalize_news_entry(entry):
        if not isinstance(entry, dict):
            return None

        def _extract(value):
            if isinstance(value, dict):
                return value.get("value")
            return value

        title = _extract(entry.get("title"))
        content = _extract(entry.get("content")) or _extract(entry.get("summary"))
        publish_time = _extract(entry.get("date")) or _extract(entry.get("publish_time"))
        source = _extract(entry.get("source")) or _extract(entry.get("publish_source"))
        url = entry.get("show_detail") or entry.get("url") or ""
        if not title:
            return None

        return {
            "title": str(title),
            "content": str(content or ""),
            "publish_time": str(publish_time or ""),
            "source": str(source or "问财"),
            "url": str(url),
        }

    def _extract_news_items_from_frame(self, df_result):
        news_items = []

        for _, row in df_result.iterrows():
            for column in getattr(df_result, "columns", []):
                value = row.get(column)
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    continue

                candidate_entries = []
                if isinstance(value, list):
                    candidate_entries = value
                elif isinstance(value, str) and value.strip().startswith("["):
                    try:
                        parsed = ast.literal_eval(value)
                        if isinstance(parsed, list):
                            candidate_entries = parsed
                    except Exception:
                        continue

                for entry in candidate_entries:
                    normalized = self._normalize_news_entry(entry)
                    if normalized:
                        news_items.append(normalized)

        deduped = []
        seen = set()
        for item in news_items:
            key = (item["title"], item["publish_time"], item["source"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)

        return deduped[: self.max_items]

    def _query_news_by_keyword(self, keyword):
        result = pywencai.get(query=f"{keyword}新闻", loop=True)
        if result is None:
            return None

        if isinstance(result, dict):
            return pd.DataFrame([result])
        if isinstance(result, pd.DataFrame):
            return result
        return None

    def _get_news_data(self, symbol):
        stock_name = self._resolve_stock_name(symbol)
        query_candidates = [symbol]
        if stock_name:
            query_candidates.append(stock_name)

        for query_keyword in query_candidates:
            try:
                print(f"   使用问财查询: {query_keyword}新闻")
                df_result = self._query_news_by_keyword(query_keyword)
                if df_result is None or df_result.empty:
                    continue

                news_items = self._extract_news_items_from_frame(df_result)
                if news_items:
                    return {
                        "items": news_items,
                        "count": len(news_items),
                        "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "date_range": "最近新闻",
                        "source": "pywencai_news",
                    }
            except Exception as e:
                print(f"   使用问财查询 {query_keyword} 失败: {e}")

        return None

    def format_news_for_ai(self, data):
        if not data or not data.get("data_success"):
            return "未能获取新闻数据"

        text_parts = []
        if data.get("news_data"):
            news_data = data["news_data"]
            text_parts.append(
                f"""
【最新新闻】
数据源：{data.get('source', 'unknown')}
查询时间：{news_data.get('query_time', 'N/A')}
时间范围：{news_data.get('date_range', 'N/A')}
新闻数量：{news_data.get('count', 0)}条

"""
            )

            for idx, item in enumerate(news_data.get("items", []), 1):
                text_parts.append(f"新闻 {idx}:")
                for field in ["title", "publish_time", "source", "content", "url"]:
                    if field in item and item[field]:
                        value = item[field]
                        if field == "content" and len(str(value)) > 500:
                            value = str(value)[:500] + "..."
                        text_parts.append(f"  {field}: {value}")
                text_parts.append("")

        return "\n".join(text_parts)


if __name__ == "__main__":
    print("测试新闻数据获取（pywencai 主源）...")
    print("=" * 60)

    fetcher = QStockNewsDataFetcher()
    if not fetcher.available:
        print("❌ 新闻数据获取器不可用")
        sys.exit(1)

    for symbol in ["000001", "600519"]:
        print(f"\n{'=' * 60}")
        print(f"正在测试股票: {symbol}")
        print(f"{'=' * 60}\n")

        data = fetcher.get_stock_news(symbol)
        if data.get("data_success"):
            print("\n" + "=" * 60)
            print("新闻数据获取成功！")
            print("=" * 60)
            print(fetcher.format_news_for_ai(data))
        else:
            print(f"\n获取失败: {data.get('error', '未知错误')}")
        print("\n")
