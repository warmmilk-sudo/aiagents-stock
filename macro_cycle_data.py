"""
宏观周期分析 - 数据采集模块
采集宏观经济数据（GDP、CPI/PPI、PMI、利率、M2、市场指数、财经新闻）
用于康波周期和美林投资时钟分析
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import os
import time
import warnings

import pandas as pd
import yfinance as yf

from news_flow_data import NewsFlowDataFetcher
from tushare_utils import create_tushare_pro

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


class MacroCycleDataFetcher:
    """宏观经济数据采集器"""

    def __init__(self):
        print("[宏观周期] 数据采集器初始化...")
        self.max_retries = 3
        self._tushare_api = None
        self._tushare_url = None

    def _safe_request(self, func, *args, **kwargs):
        """安全请求，带重试"""
        for i in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if i < self.max_retries - 1:
                    time.sleep(1.5)
                else:
                    logger.warning(f"请求失败: {e}")
                    return None

    def _ensure_tushare_api(self):
        if self._tushare_api is not None:
            return self._tushare_api

        token = str(os.getenv("TUSHARE_TOKEN", "")).strip()
        if not token:
            logger.warning("未配置 TUSHARE_TOKEN")
            return None

        try:
            self._tushare_api, self._tushare_url = create_tushare_pro(token=token)
            if self._tushare_api is not None:
                logger.info("[宏观周期] Tushare初始化成功: %s", self._tushare_url)
        except Exception as exc:
            logger.warning("[宏观周期] Tushare初始化失败: %s", exc)
            self._tushare_api = None
        return self._tushare_api

    @staticmethod
    def _records_from_df(df: pd.DataFrame | None, limit: int) -> list[dict]:
        if df is None or df.empty:
            return []
        rows = []
        for _, row in df.tail(limit).iterrows():
            item = {}
            for col in df.columns:
                value = row.get(col)
                if pd.isna(value):
                    continue
                item[col] = str(value)
            if item:
                rows.append(item)
        return rows

    def get_all_macro_data(self) -> dict:
        """
        获取所有宏观经济数据
        Returns:
            dict: 包含多维度宏观数据的字典
        """
        print("\n[宏观周期] 开始采集宏观经济数据...")
        data = {
            "success": False,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "gdp": {},
            "cpi_ppi": {},
            "pmi": {},
            "money_supply": {},
            "interest_rate": {},
            "market_indices": {},
            "commodities": {},
            "real_estate": {},
            "employment": {},
            "news": [],
            "errors": [],
        }

        steps = [
            ("1/9 获取GDP数据...", "gdp", self._get_gdp_data, "GDP"),
            ("2/9 获取CPI/PPI数据...", "cpi_ppi", self._get_cpi_ppi_data, "CPI/PPI"),
            ("3/9 获取PMI数据...", "pmi", self._get_pmi_data, "PMI"),
            ("4/9 获取货币供应数据...", "money_supply", self._get_money_supply, "货币供应"),
            ("5/9 获取利率数据...", "interest_rate", self._get_interest_rate, "利率"),
            ("6/9 获取市场指数...", "market_indices", self._get_market_indices, "市场指数"),
            ("7/9 获取大宗商品数据...", "commodities", self._get_commodities_data, "大宗商品"),
            ("8/9 获取房地产数据...", "real_estate", self._get_real_estate_data, "房地产"),
            ("9/9 获取财经新闻...", "news", self._get_macro_news, "新闻"),
        ]

        for label, key, fetcher, error_label in steps:
            print(f"  {label}")
            try:
                payload = fetcher()
                if payload:
                    data[key] = payload
                    if key == "news":
                        print(f"    ✓ 获取{len(payload)}条新闻")
                    else:
                        print(f"    ✓ {error_label}数据获取成功")
            except Exception as exc:
                data["errors"].append(f"{error_label}: {exc}")
                print(f"    ✗ {error_label}获取失败: {exc}")

        valid_count = sum(
            1
            for k in ["gdp", "cpi_ppi", "pmi", "money_supply", "interest_rate", "market_indices", "commodities"]
            if data.get(k)
        )
        data["success"] = True
        if valid_count >= 3:
            print(f"\n[宏观周期] 数据采集完成，成功获取 {valid_count}/7 项核心数据")
        else:
            print(f"\n[宏观周期] 数据不足（仅 {valid_count}/7 项），分析可能不够准确")
        return data

    def _get_gdp_data(self) -> dict:
        pro = self._ensure_tushare_api()
        if not pro:
            return {}

        result = {}
        df = self._safe_request(pro.cn_gdp)
        yearly = self._records_from_df(df, 8)
        if yearly:
            result["yearly"] = yearly
            result["quarterly_growth"] = yearly[-8:]
        return result

    def _get_cpi_ppi_data(self) -> dict:
        pro = self._ensure_tushare_api()
        if not pro:
            return {}

        result = {}
        cpi_df = self._safe_request(pro.cn_cpi)
        ppi_df = self._safe_request(pro.cn_ppi)
        cpi_rows = self._records_from_df(cpi_df, 12)
        ppi_rows = self._records_from_df(ppi_df, 12)
        if cpi_rows:
            result["cpi_monthly"] = cpi_rows
        if ppi_rows:
            result["ppi_monthly"] = ppi_rows
        return result

    def _get_pmi_data(self) -> dict:
        pro = self._ensure_tushare_api()
        if not pro:
            return {}

        result = {}
        df = self._safe_request(pro.cn_pmi)
        rows = self._records_from_df(df, 12)
        if rows:
            result["manufacturing_pmi"] = rows
            result["caixin_pmi"] = rows[-6:]
        return result

    def _get_money_supply(self) -> dict:
        pro = self._ensure_tushare_api()
        if not pro:
            return {}

        df = self._safe_request(pro.cn_m)
        rows = self._records_from_df(df, 12)
        return {"m2_data": rows} if rows else {}

    def _get_interest_rate(self) -> dict:
        pro = self._ensure_tushare_api()
        if not pro:
            return {}

        result = {}
        shibor_df = self._safe_request(pro.shibor)
        hibor_df = self._safe_request(pro.hibor)
        libor_df = self._safe_request(pro.libor)
        shibor_rows = self._records_from_df(shibor_df, 12)
        hibor_rows = self._records_from_df(hibor_df, 12)
        libor_rows = self._records_from_df(libor_df, 12)
        if shibor_rows:
            result["shibor"] = shibor_rows
        if hibor_rows:
            result["hibor"] = hibor_rows
        if libor_rows:
            result["libor"] = libor_rows
        return result

    def _get_market_indices(self) -> dict:
        pro = self._ensure_tushare_api()
        if not pro:
            return {}

        result = {}
        index_map = {
            "sh_index": ("000001.SH", "上证指数"),
            "sz_index": ("399001.SZ", "深证成指"),
            "cyb_index": ("399006.SZ", "创业板指"),
        }
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=420)).strftime("%Y%m%d")

        for key, (ts_code, display_name) in index_map.items():
            df = self._safe_request(
                pro.index_daily,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields="trade_date,close,high,low,pct_chg",
            )
            if df is None or df.empty:
                continue
            df = df.sort_values("trade_date", ascending=True).reset_index(drop=True)
            latest = df.iloc[-1]
            first_60 = df.tail(60).iloc[0] if len(df) >= 60 else df.iloc[0]
            base_close = float(first_60["close"]) if float(first_60["close"]) else 0.0
            pct_60d = ((float(latest["close"]) / base_close) - 1) * 100 if base_close > 0 else 0.0
            window_52w = df.tail(250)
            result[key] = {
                "name": display_name,
                "close": round(float(latest["close"]), 2),
                "change_pct": round(float(latest.get("pct_chg", 0) or 0), 2),
                "pct_60d": round(pct_60d, 2),
                "high_52w": round(float(window_52w["high"].max()), 2) if not window_52w.empty else None,
                "low_52w": round(float(window_52w["low"].min()), 2) if not window_52w.empty else None,
            }

        return result

    def _get_commodities_data(self) -> dict:
        result = {}
        commodity_map = {
            "gold": ("GLD", "黄金ETF"),
            "crude_oil": ("USO", "原油ETF"),
            "copper": ("CPER", "铜ETF"),
        }

        for key, (ticker, name) in commodity_map.items():
            try:
                df = yf.Ticker(ticker).history(period="1y", interval="1d", auto_adjust=True)
                if df is None or df.empty:
                    continue
                latest = float(df["Close"].iloc[-1])
                first = float(df["Close"].iloc[0])
                ytd_pct = ((latest / first) - 1) * 100 if first > 0 else 0.0
                result[key] = {
                    "price": round(latest, 2),
                    "ytd_change_pct": round(ytd_pct, 2),
                    "name": name,
                }
            except Exception as exc:
                logger.warning("%s 数据获取失败: %s", ticker, exc)

        return result

    def _get_real_estate_data(self) -> dict:
        pro = self._ensure_tushare_api()
        if not pro:
            return {}

        df = self._safe_request(pro.cn_ppi)
        rows = self._records_from_df(df, 6)
        return {"data": rows} if rows else {}

    def _get_macro_news(self) -> list:
        try:
            fetcher = NewsFlowDataFetcher()
            result = fetcher.get_multi_platform_news(category="finance")
        except Exception as exc:
            logger.warning("宏观新闻获取失败: %s", exc)
            return []

        merged = []
        seen = set()
        for platform_data in result.get("platforms_data", []):
            if not platform_data.get("success"):
                continue
            for row in platform_data.get("data", []):
                title = str(row.get("title") or "").strip()
                if not title:
                    continue
                key = (title, str(row.get("publish_time") or ""))
                if key in seen:
                    continue
                seen.add(key)
                merged.append(
                    {
                        "title": title,
                        "publish_time": str(row.get("publish_time") or ""),
                        "content": str(row.get("content") or "")[:300],
                    }
                )
                if len(merged) >= 50:
                    return merged
        return merged

    def format_data_for_ai(self, data: dict) -> str:
        """将数据格式化为AI分析所需的文本"""
        parts = []
        parts.append("===== 宏观经济数据报告 =====")
        parts.append(f"数据采集时间: {data.get('timestamp', '未知')}")
        parts.append("")

        if data.get("gdp"):
            parts.append("【一、GDP数据】")
            gdp = data["gdp"]
            if gdp.get("yearly"):
                parts.append("近年GDP:")
                for item in gdp["yearly"][-4:]:
                    parts.append(f"  {item}")
            if gdp.get("quarterly_growth"):
                parts.append("季度GDP增速:")
                for item in gdp["quarterly_growth"][-8:]:
                    parts.append(f"  {item}")
            parts.append("")

        if data.get("cpi_ppi"):
            parts.append("【二、CPI/PPI通胀数据】")
            cp = data["cpi_ppi"]
            if cp.get("cpi_monthly"):
                parts.append("近12个月CPI:")
                for item in cp["cpi_monthly"]:
                    parts.append(f"  {item}")
            if cp.get("ppi_monthly"):
                parts.append("近12个月PPI:")
                for item in cp["ppi_monthly"]:
                    parts.append(f"  {item}")
            parts.append("")

        if data.get("pmi"):
            parts.append("【三、PMI景气指数】")
            pmi = data["pmi"]
            if pmi.get("manufacturing_pmi"):
                parts.append("制造业PMI（50为荣枯线）:")
                for item in pmi["manufacturing_pmi"]:
                    parts.append(f"  {item}")
            if pmi.get("caixin_pmi"):
                parts.append("财新PMI:")
                for item in pmi["caixin_pmi"]:
                    parts.append(f"  {item}")
            parts.append("")

        if data.get("money_supply"):
            parts.append("【四、货币供应量】")
            for item in data["money_supply"].get("m2_data", []):
                parts.append(f"  {item}")
            parts.append("")

        if data.get("interest_rate"):
            parts.append("【五、利率数据】")
            for key in ["shibor", "hibor", "libor"]:
                rows = data["interest_rate"].get(key)
                if rows:
                    parts.append(f"{key.upper()}:")
                    for item in rows:
                        parts.append(f"  {item}")
            parts.append("")

        if data.get("market_indices"):
            parts.append("【六、市场指数】")
            mi = data["market_indices"]
            for name, info in mi.items():
                label = {"sh_index": "上证指数", "sz_index": "深证成指", "cyb_index": "创业板指"}.get(name, name)
                parts.append(f"  {label}: {info['close']} (日涨跌: {info['change_pct']:+.2f}%, 60日涨跌: {info.get('pct_60d', 0):+.2f}%)")
                if info.get("high_52w"):
                    parts.append(f"    52周最高: {info['high_52w']}  52周最低: {info['low_52w']}")
            parts.append("")

        if data.get("commodities"):
            parts.append("【七、大宗商品】")
            for _, info in data["commodities"].items():
                parts.append(f"  {info['name']}: {info['price']} (年涨跌: {info['ytd_change_pct']:+.2f}%)")
            parts.append("")

        if data.get("real_estate"):
            parts.append("【八、房地产数据】")
            for item in data["real_estate"].get("data", [])[-4:]:
                parts.append(f"  {item}")
            parts.append("")

        if data.get("news"):
            parts.append("【九、近期宏观经济新闻】")
            for idx, news in enumerate(data["news"][:20], 1):
                parts.append(f"  {idx}. [{news.get('publish_time', '')}] {news.get('title', '')}")
                if news.get("content"):
                    parts.append(f"     {news['content'][:150]}")
            parts.append("")

        return "\n".join(parts)


if __name__ == "__main__":
    print("=" * 60)
    print("测试宏观周期数据采集")
    print("=" * 60)

    fetcher = MacroCycleDataFetcher()
    data = fetcher.get_all_macro_data()

    if data.get("success"):
        formatted = fetcher.format_data_for_ai(data)
        print(formatted[:5000])
        print(f"\n... (总长度: {len(formatted)} 字符)")
    else:
        print(f"数据采集失败: {data.get('errors')}")
