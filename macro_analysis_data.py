"""
宏观分析板块 - 数据采集与标准化
基于 Tushare、东方财富等可用数据源获取核心宏观数据，并补充 A 股市场快照与候选标的池。
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Optional
from urllib.parse import urljoin

import akshare as ak
import pandas as pd
import requests
import urllib3

from tushare_utils import create_tushare_pro


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MacroAnalysisDataFetcher:
    """宏观分析板块数据获取器"""

    REQUEST_HEADERS = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
    }
    EASTMONEY_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    STATS_RELEASE_INDEX_URL = "https://www.stats.gov.cn/sj/zxfb/"

    NBS_SERIES_CONFIG = {
        "gdp_yoy": {
            "dbcode": "hgjd",
            "group_code": "A0103",
            "series_code": "A010301",
            "label": "GDP当季同比",
            "unit": "%",
            "period": "LAST8",
            "transform": "index_minus_100",
        },
        "gdp_qoq": {
            "dbcode": "hgjd",
            "group_code": "A0104",
            "series_code": "A010401",
            "label": "GDP环比增长",
            "unit": "%",
            "period": "LAST8",
        },
        "industrial_yoy": {
            "dbcode": "hgyd",
            "group_code": "A0201",
            "series_code": "A020101",
            "label": "规上工业增加值同比",
            "unit": "%",
            "period": "LAST8",
        },
        "cpi_yoy": {
            "dbcode": "hgyd",
            "group_code": "A01010J",
            "series_code": "A01010J01",
            "label": "CPI同比",
            "unit": "%",
            "period": "LAST8",
            "transform": "index_minus_100",
        },
        "ppi_yoy": {
            "dbcode": "hgyd",
            "group_code": "A010801",
            "series_code": "A01080101",
            "label": "PPI同比",
            "unit": "%",
            "period": "LAST8",
            "transform": "index_minus_100",
        },
        "manufacturing_pmi": {
            "dbcode": "hgyd",
            "group_code": "A0B01",
            "series_code": "A0B0101",
            "label": "制造业PMI",
            "unit": "",
            "period": "LAST8",
        },
        "non_manufacturing_pmi": {
            "dbcode": "hgyd",
            "group_code": "A0B02",
            "series_code": "A0B0201",
            "label": "非制造业商务活动指数",
            "unit": "",
            "period": "LAST8",
        },
        "composite_pmi": {
            "dbcode": "hgyd",
            "group_code": "A0B03",
            "series_code": "A0B0301",
            "label": "综合PMI产出指数",
            "unit": "",
            "period": "LAST8",
        },
        "m2_yoy": {
            "dbcode": "hgyd",
            "group_code": "A0D01",
            "series_code": "A0D0102",
            "label": "M2同比",
            "unit": "%",
            "period": "LAST8",
        },
        "retail_sales_yoy": {
            "dbcode": "hgyd",
            "group_code": "A0701",
            "series_code": "A070104",
            "label": "社零累计同比",
            "unit": "%",
            "period": "LAST8",
        },
        "fixed_asset_yoy": {
            "dbcode": "hgyd",
            "group_code": "A0401",
            "series_code": "A040102",
            "label": "固定资产投资累计同比",
            "unit": "%",
            "period": "LAST8",
        },
        "real_estate_invest_yoy": {
            "dbcode": "hgyd",
            "group_code": "A0601",
            "series_code": "A060102",
            "label": "房地产开发投资累计同比",
            "unit": "%",
            "period": "LAST8",
        },
        "urban_unemployment": {
            "dbcode": "hgyd",
            "group_code": "A0E01",
            "series_code": "A0E0101",
            "label": "全国城镇调查失业率",
            "unit": "%",
            "period": "LAST8",
        },
    }

    A_SHARE_INDEX_CONFIG = {
        "上证指数": "sh000001",
        "深证成指": "sz399001",
        "创业板指": "sz399006",
        "沪深300": "sh000300",
    }

    SECTOR_STOCK_POOLS = {
        "银行": [{"code": "600036", "name": "招商银行"}, {"code": "601166", "name": "兴业银行"}, {"code": "600919", "name": "江苏银行"}],
        "券商": [{"code": "600030", "name": "中信证券"}, {"code": "300059", "name": "东方财富"}, {"code": "601688", "name": "华泰证券"}],
        "保险": [{"code": "601318", "name": "中国平安"}, {"code": "601628", "name": "中国人寿"}, {"code": "601601", "name": "中国太保"}],
        "公用事业": [{"code": "600900", "name": "长江电力"}, {"code": "600025", "name": "华能水电"}, {"code": "600674", "name": "川投能源"}],
        "电网设备": [{"code": "600406", "name": "国电南瑞"}, {"code": "000400", "name": "许继电气"}, {"code": "600312", "name": "平高电气"}],
        "半导体": [{"code": "002371", "name": "北方华创"}, {"code": "688981", "name": "中芯国际"}, {"code": "603986", "name": "兆易创新"}],
        "算力AI": [{"code": "300308", "name": "中际旭创"}, {"code": "601138", "name": "工业富联"}, {"code": "000977", "name": "浪潮信息"}],
        "软件信创": [{"code": "688111", "name": "金山办公"}, {"code": "600588", "name": "用友网络"}, {"code": "600536", "name": "中国软件"}],
        "消费电子": [{"code": "002475", "name": "立讯精密"}, {"code": "002241", "name": "歌尔股份"}, {"code": "300433", "name": "蓝思科技"}],
        "食品饮料": [{"code": "600519", "name": "贵州茅台"}, {"code": "600887", "name": "伊利股份"}, {"code": "603288", "name": "海天味业"}],
        "家电": [{"code": "000333", "name": "美的集团"}, {"code": "000651", "name": "格力电器"}, {"code": "600690", "name": "海尔智家"}],
        "创新药": [{"code": "600276", "name": "恒瑞医药"}, {"code": "688235", "name": "百济神州"}, {"code": "002422", "name": "科伦药业"}],
        "汽车整车": [{"code": "002594", "name": "比亚迪"}, {"code": "000625", "name": "长安汽车"}, {"code": "600066", "name": "宇通客车"}],
        "工程机械": [{"code": "600031", "name": "三一重工"}, {"code": "000425", "name": "徐工机械"}, {"code": "000157", "name": "中联重科"}],
        "有色金属": [{"code": "601899", "name": "紫金矿业"}, {"code": "603993", "name": "洛阳钼业"}, {"code": "601600", "name": "中国铝业"}],
        "黄金": [{"code": "600547", "name": "山东黄金"}, {"code": "600489", "name": "中金黄金"}, {"code": "600988", "name": "赤峰黄金"}],
        "石油石化": [{"code": "600938", "name": "中国海油"}, {"code": "601857", "name": "中国石油"}, {"code": "600028", "name": "中国石化"}],
        "煤炭": [{"code": "601088", "name": "中国神华"}, {"code": "601225", "name": "陕西煤业"}, {"code": "601898", "name": "中煤能源"}],
        "通信运营商": [{"code": "600941", "name": "中国移动"}, {"code": "601728", "name": "中国电信"}, {"code": "600050", "name": "中国联通"}],
        "旅游酒店": [{"code": "601888", "name": "中国中免"}, {"code": "600258", "name": "首旅酒店"}, {"code": "600754", "name": "锦江酒店"}],
        "房地产": [{"code": "600048", "name": "保利发展"}, {"code": "001979", "name": "招商蛇口"}, {"code": "000002", "name": "万科A"}],
        "建材家居": [{"code": "002271", "name": "东方雨虹"}, {"code": "000786", "name": "北新建材"}, {"code": "603833", "name": "欧派家居"}],
        "农业": [{"code": "002714", "name": "牧原股份"}, {"code": "002311", "name": "海大集团"}, {"code": "000998", "name": "隆平高科"}],
        "军工": [{"code": "600760", "name": "中航沈飞"}, {"code": "000768", "name": "中航西飞"}, {"code": "600893", "name": "航发动力"}],
    }

    SECTOR_ALIASES = {
        "高股息": ["银行", "保险", "公用事业", "煤炭", "通信运营商"],
        "红利": ["银行", "保险", "公用事业", "煤炭", "通信运营商"],
        "电力": ["公用事业"],
        "电网": ["电网设备"],
        "算力": ["算力AI"],
        "AI": ["算力AI"],
        "信创": ["软件信创"],
        "医药": ["创新药"],
        "消费": ["食品饮料", "家电", "旅游酒店"],
        "顺周期": ["有色金属", "工程机械", "石油石化", "煤炭"],
    }

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update(self.REQUEST_HEADERS)
        self._tushare_api = None
        self._tushare_url = None

    def fetch_all_data(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "success": False,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "macro_series": {},
            "macro_snapshot": {},
            "macro_tables": {},
            "market_indices": {},
            "news": [],
            "candidate_pools": self.sector_pools_for_prompt(),
            "rule_based_sector_view": {},
            "errors": [],
        }

        for key, series_config in self.NBS_SERIES_CONFIG.items():
            fetch_errors: list[str] = []
            series = self._fetch_tushare_series(key)
            if not series:
                fetch_errors.append("Tushare 无可用数据")
                series = self._fetch_stats_release_series(key)
            if not series:
                fetch_errors.append("统计局公开发布稿无可用数据")
                series = self._fetch_eastmoney_series(key)
            if not series:
                fetch_errors.append("东方财富无可用数据")
                series = self._fetch_akshare_series(key)
            if series:
                result["macro_series"][key] = series
                continue
            error_text = "；".join(fetch_errors) if fetch_errors else "暂无可用数据源"
            result["errors"].append(f"{series_config['label']}: {error_text}")
            self.logger.warning("宏观指标 %s 获取失败: %s", series_config["label"], error_text)

        result["macro_snapshot"] = self._build_macro_snapshot(result["macro_series"])
        result["macro_tables"] = self._build_macro_tables(result["macro_series"])
        result["rule_based_sector_view"] = self.build_rule_based_sector_view(result["macro_snapshot"])

        try:
            result["market_indices"] = self._fetch_market_indices()
        except Exception as exc:
            result["errors"].append(f"市场指数: {exc}")
            self.logger.warning("获取市场指数失败: %s", exc)

        try:
            result["news"] = self._fetch_macro_news()
        except Exception as exc:
            result["errors"].append(f"宏观新闻: {exc}")
            self.logger.warning("获取宏观新闻失败: %s", exc)

        result["success"] = bool(result["macro_snapshot"])
        return result

    def sector_pools_for_prompt(self) -> dict[str, list[dict[str, str]]]:
        return self.SECTOR_STOCK_POOLS

    def _ensure_tushare_api(self):
        if self._tushare_api is not None:
            return self._tushare_api
        token = str(os.getenv("TUSHARE_TOKEN", "")).strip()
        if not token:
            self.logger.warning("未配置 TUSHARE_TOKEN，无法回退到 Tushare 宏观口径")
            return None
        try:
            self._tushare_api, self._tushare_url = create_tushare_pro(token=token)
            if self._tushare_api is not None:
                self.logger.info("[宏观分析] Tushare 初始化成功: %s", self._tushare_url)
        except Exception as exc:
            self.logger.warning("[宏观分析] Tushare 初始化失败: %s", exc)
            self._tushare_api = None
        return self._tushare_api

    def _fetch_tushare_series(self, key: str) -> list[dict[str, Any]]:
        pro = self._ensure_tushare_api()
        if not pro:
            return []

        try:
            if key in {"gdp_yoy", "gdp_qoq"}:
                return self._map_tushare_gdp_series(pro, key)
            if key in {"cpi_yoy"}:
                return self._map_tushare_cpi_series(pro)
            if key in {"ppi_yoy"}:
                return self._map_tushare_ppi_series(pro)
            if key in {"manufacturing_pmi", "non_manufacturing_pmi", "composite_pmi"}:
                return self._map_tushare_pmi_series(pro, key)
            if key in {"m2_yoy"}:
                return self._map_tushare_money_supply_series(pro)
        except Exception as exc:
            self.logger.warning("Tushare 回退 %s 失败: %s", key, exc)
        return []

    def _fetch_stats_release_series(self, key: str) -> list[dict[str, Any]]:
        try:
            if key == "gdp_qoq":
                return self._map_stats_release_gdp_qoq_series()
            if key == "real_estate_invest_yoy":
                return self._map_stats_release_real_estate_series()
            if key == "urban_unemployment":
                return self._map_stats_release_unemployment_series()
        except Exception as exc:
            self.logger.warning("统计局公开稿回退 %s 失败: %s", key, exc)
        return []

    def _fetch_eastmoney_series(self, key: str) -> list[dict[str, Any]]:
        try:
            if key == "industrial_yoy":
                return self._map_eastmoney_industrial_yoy()
            if key == "retail_sales_yoy":
                return self._map_eastmoney_consumer_goods_retail()
            if key == "fixed_asset_yoy":
                return self._map_eastmoney_fixed_asset_investment()
        except Exception as exc:
            self.logger.warning("东方财富回退 %s 失败: %s", key, exc)
        return []

    def _fetch_akshare_series(self, key: str) -> list[dict[str, Any]]:
        try:
            if key == "manufacturing_pmi":
                return self._map_akshare_macro_report_series(ak.macro_china_pmi_yearly, key, "%")
            if key == "non_manufacturing_pmi":
                return self._map_akshare_macro_report_series(ak.macro_china_non_man_pmi, key, "%")
        except Exception as exc:
            self.logger.warning("AKShare 回退 %s 失败: %s", key, exc)
        return []

    def _map_stats_release_gdp_qoq_series(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for article in self._get_stats_release_articles(max_pages=6):
            if "国民经济" not in article["title"]:
                continue
            text = self._fetch_stats_release_text(article["url"])
            if not text:
                continue
            match = re.search(
                r"从环比看，(?P<period>一季度|上半年|前三季度|全年)国内生产总值增长(?P<value>-?\d+(?:\.\d+)?)%",
                text,
            )
            if not match:
                continue
            period = self._resolve_stats_period(article, match.group("period"))
            if not period:
                continue
            sequence_match = re.search(r"GDP环比增速分别为([0-9.%、和]+)。", text)
            sequence_values = self._extract_percent_sequence(sequence_match.group(1) if sequence_match else "")
            if sequence_values:
                sequence_rows = self._build_quarter_sequence_rows(
                    key="gdp_qoq",
                    current_period_code=period["code"],
                    current_period_label=period["label"],
                    values=sequence_values,
                )
                for row in sequence_rows:
                    if not any(item["period_code"] == row["period_code"] for item in rows):
                        rows.append(row)
            else:
                row = self._build_stats_release_row(
                    key="gdp_qoq",
                    period_code=period["code"],
                    period_label=period["label"],
                    value=float(match.group("value")),
                    unit="%",
                )
                if not any(item["period_code"] == row["period_code"] for item in rows):
                    rows.append(row)
            if len(rows) >= 8:
                break
        rows.sort(key=lambda item: item["period_code"], reverse=True)
        return rows[:8]

    def _map_stats_release_real_estate_series(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        pattern = re.compile(
            r"(?P<period>一季度|上半年|前三季度|全年|\d+—\d+月份).*?全国固定资产投资.*?房地产开发投资(?P<direction>增长|下降)(?P<value>\d+(?:\.\d+)?)%",
        )
        for article in self._get_stats_release_articles(max_pages=4):
            if "国民经济" not in article["title"]:
                continue
            text = self._fetch_stats_release_text(article["url"])
            if not text:
                continue
            match = pattern.search(text)
            if not match:
                continue
            period = self._resolve_stats_period(article, match.group("period"))
            if not period:
                continue
            value = float(match.group("value"))
            if match.group("direction") == "下降":
                value = -value
            row = self._build_stats_release_row(
                key="real_estate_invest_yoy",
                period_code=period["code"],
                period_label=period["label"],
                value=value,
                unit="%",
            )
            if not any(item["period_code"] == row["period_code"] for item in rows):
                rows.append(row)
            if len(rows) >= 8:
                break
        rows.sort(key=lambda item: item["period_code"], reverse=True)
        return rows[:8]

    def _map_stats_release_unemployment_series(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        pattern = re.compile(r"(?P<month>\d+月份)，全国城镇调查失业率为(?P<value>\d+(?:\.\d+)?)%")
        for article in self._get_stats_release_articles(max_pages=4):
            if "国民经济" not in article["title"]:
                continue
            text = self._fetch_stats_release_text(article["url"])
            if not text:
                continue
            match = pattern.search(text)
            if not match:
                continue
            period = self._resolve_stats_period(article, match.group("month"))
            if not period:
                continue
            row = self._build_stats_release_row(
                key="urban_unemployment",
                period_code=period["code"],
                period_label=period["label"],
                value=float(match.group("value")),
                unit="%",
            )
            if not any(item["period_code"] == row["period_code"] for item in rows):
                rows.append(row)
            if len(rows) >= 8:
                break
        rows.sort(key=lambda item: item["period_code"], reverse=True)
        return rows[:8]

    def _get_stats_release_articles(self, max_pages: int = 3) -> list[dict[str, Any]]:
        articles: list[dict[str, Any]] = []
        seen: set[str] = set()
        for page_index in range(max_pages):
            url = self.STATS_RELEASE_INDEX_URL if page_index == 0 else urljoin(self.STATS_RELEASE_INDEX_URL, f"index_{page_index}.html")
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or "utf-8"
            from bs4 import BeautifulSoup

            page = BeautifulSoup(response.text, "html.parser")
            for item in page.select(".list-content li"):
                anchor = item.find("a", href=True)
                if not anchor:
                    continue
                title = str(anchor.get("title") or anchor.get_text(" ", strip=True)).strip()
                href = str(anchor.get("href") or "").strip()
                if not title or not href:
                    continue
                article_url = urljoin(url, href)
                if article_url in seen:
                    continue
                seen.add(article_url)
                published_text = str(item.find("span").get_text(" ", strip=True) if item.find("span") else "").strip()
                articles.append(
                    {
                        "title": title,
                        "url": article_url,
                        "published_at": published_text,
                    }
                )
        return articles

    def _fetch_stats_release_text(self, url: str) -> str:
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or "utf-8"
        from bs4 import BeautifulSoup

        text = BeautifulSoup(response.text, "html.parser").get_text("\n")
        return re.sub(r"\s+", "", text.replace("\xa0", ""))

    def _resolve_stats_period(self, article: dict[str, str], raw_period: str) -> Optional[dict[str, str]]:
        raw = str(raw_period or "").strip()
        title = str(article.get("title") or "").strip()
        published_at = str(article.get("published_at") or "").strip()
        year_match = re.search(r"(\d{4})年", title)
        if year_match:
            year = int(year_match.group(1))
        else:
            try:
                year = int(published_at[:4])
            except Exception:
                year = datetime.now().year

        if raw == "一季度":
            return {"code": f"{year}-03", "label": f"{year}年一季度"}
        if raw == "上半年":
            return {"code": f"{year}-06", "label": f"{year}年上半年"}
        if raw == "前三季度":
            return {"code": f"{year}-09", "label": f"{year}年前三季度"}
        if raw == "全年":
            return {"code": f"{year}-12", "label": f"{year}年全年"}

        month_match = re.fullmatch(r"(\d+)月份", raw)
        if month_match:
            month = int(month_match.group(1))
            return {"code": f"{year}-{month:02d}", "label": f"{year}年{month:02d}月份"}

        range_match = re.fullmatch(r"(\d+)—(\d+)月份", raw)
        if range_match:
            end_month = int(range_match.group(2))
            return {"code": f"{year}-{end_month:02d}", "label": f"{year}年{range_match.group(1)}—{range_match.group(2)}月份"}
        return None

    def _build_stats_release_row(
        self,
        *,
        key: str,
        period_code: str,
        period_label: str,
        value: float,
        unit: str,
    ) -> dict[str, Any]:
        rounded = round(float(value), 2)
        return {
            "series_code": key,
            "series_label": self.NBS_SERIES_CONFIG[key]["label"],
            "period_code": period_code,
            "period_label": period_label,
            "value_raw": rounded,
            "value": rounded,
            "unit": unit,
        }

    @staticmethod
    def _extract_percent_sequence(text: str) -> list[float]:
        return [round(float(value), 2) for value in re.findall(r"-?\d+(?:\.\d+)?", str(text or ""))]

    def _build_quarter_sequence_rows(
        self,
        *,
        key: str,
        current_period_code: str,
        current_period_label: str,
        values: list[float],
    ) -> list[dict[str, Any]]:
        if not values:
            return []
        quarter_positions: list[tuple[int, int]] = []
        year = int(current_period_code[:4])
        month = int(current_period_code[-2:])
        quarter = max(1, min(4, ((month - 1) // 3) + 1))
        for offset in range(len(values) - 1, -1, -1):
            q = quarter - offset
            y = year
            while q <= 0:
                q += 4
                y -= 1
            quarter_positions.append((y, q))

        rows: list[dict[str, Any]] = []
        for (row_year, row_quarter), value in zip(quarter_positions, values, strict=False):
            month_end = row_quarter * 3
            if row_year == year and row_quarter == quarter:
                label = current_period_label
            else:
                label_map = {1: "一季度", 2: "上半年", 3: "前三季度", 4: "全年"}
                label = f"{row_year}年{label_map[row_quarter]}"
            rows.append(
                self._build_stats_release_row(
                    key=key,
                    period_code=f"{row_year}-{month_end:02d}",
                    period_label=label,
                    value=value,
                    unit="%",
                )
            )
        return rows

    def _map_tushare_gdp_series(self, pro, key: str) -> list[dict[str, Any]]:
        if key != "gdp_yoy":
            return []
        df = pro.cn_gdp()
        if df is None or df.empty:
            return []
        df = df.copy()
        date_key = self._find_first_column(df, ["quarter", "month", "trade_date"])
        value_key = self._find_first_column(df, ["gdp_yoy"])
        if not date_key or value_key not in df.columns:
            return []
        rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            value = self._to_float(row.get(value_key))
            if value is None:
                continue
            period_label = str(row.get(date_key, "")).strip()
            rows.append(
                {
                    "series_code": key,
                    "series_label": self.NBS_SERIES_CONFIG[key]["label"],
                    "period_code": period_label,
                    "period_label": period_label,
                    "value_raw": value,
                    "value": round(value, 2),
                    "unit": "%",
                }
            )
        rows.sort(key=lambda item: item["period_code"], reverse=True)
        return rows[:8]

    def _map_tushare_cpi_series(self, pro) -> list[dict[str, Any]]:
        df = pro.cn_cpi()
        if df is None or df.empty:
            return []
        df = df.copy()
        date_key = self._find_first_column(df, ["month", "trade_date"])
        value_key = self._find_first_column(df, ["nt_yoy", "cpi_yoy", "yoy"])
        if not date_key or not value_key:
            return []
        return self._map_simple_tushare_rows(df, "cpi_yoy", date_key, value_key, "%", limit=8)

    def _map_tushare_ppi_series(self, pro) -> list[dict[str, Any]]:
        df = pro.cn_ppi()
        if df is None or df.empty:
            return []
        df = df.copy()
        date_key = self._find_first_column(df, ["month", "trade_date"])
        value_key = self._find_first_column(df, ["ppi_yoy", "yoy"])
        if not date_key or not value_key:
            return []
        return self._map_simple_tushare_rows(df, "ppi_yoy", date_key, value_key, "%", limit=8)

    def _map_tushare_pmi_series(self, pro, key: str) -> list[dict[str, Any]]:
        df = pro.cn_pmi()
        if df is None or df.empty:
            return []
        df = df.copy()
        date_key = self._find_first_column(df, ["month", "trade_date"])
        value_key_map = {
            "manufacturing_pmi": ["pmi010000", "manufacturing", "pmi"],
            "non_manufacturing_pmi": ["pmi020100", "pmi020000", "non_manufacturing"],
            "composite_pmi": ["pmi030000", "composite"],
        }
        value_key = self._find_first_column(df, value_key_map[key])
        if not date_key or not value_key:
            return []
        return self._map_simple_tushare_rows(df, key, date_key, value_key, "", limit=8)

    def _map_tushare_money_supply_series(self, pro) -> list[dict[str, Any]]:
        df = pro.cn_m()
        if df is None or df.empty:
            return []
        df = df.copy()
        date_key = self._find_first_column(df, ["month", "trade_date"])
        value_key = self._find_first_column(df, ["m2_yoy", "m2同比", "m2"])
        if not date_key or not value_key:
            return []
        return self._map_simple_tushare_rows(df, "m2_yoy", date_key, value_key, "%", limit=8)

    def _map_simple_tushare_rows(
        self,
        df: pd.DataFrame,
        key: str,
        date_key: str,
        value_key: str,
        unit: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            value = self._to_float(row.get(value_key))
            if value is None:
                continue
            period_label = str(row.get(date_key, "")).strip()
            rows.append(
                {
                    "series_code": key,
                    "series_label": self.NBS_SERIES_CONFIG[key]["label"],
                    "period_code": period_label,
                    "period_label": period_label,
                    "value_raw": value,
                    "value": round(value, 2),
                    "unit": unit,
                }
            )
        rows.sort(key=lambda item: item["period_code"], reverse=True)
        return rows[:limit]

    @staticmethod
    def _find_first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
        lower_map = {str(column).lower(): column for column in df.columns}
        for candidate in candidates:
            matched = lower_map.get(candidate.lower())
            if matched is not None:
                return matched
        return None

    def _map_akshare_macro_report_series(self, fetcher, key: str, unit: str) -> list[dict[str, Any]]:
        df = fetcher()
        if df is None or df.empty:
            return []
        rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            value = self._to_float(row.get("今值"))
            if value is None:
                continue
            period_label = self._format_period_label(row.get("日期"))
            previous_value = self._to_float(row.get("前值"))
            rows.append(
                {
                    "series_code": key,
                    "series_label": self.NBS_SERIES_CONFIG[key]["label"],
                    "period_code": period_label,
                    "period_label": period_label,
                    "value_raw": value,
                    "value": round(value, 2),
                    "unit": unit,
                    "previous_value_hint": previous_value,
                }
            )
        rows.sort(key=lambda item: item["period_code"], reverse=True)
        return rows[:8]

    def _map_eastmoney_industrial_yoy(self) -> list[dict[str, Any]]:
        rows = self._fetch_eastmoney_table(
            report_name="RPT_ECONOMY_INDUS_GROW",
            columns="REPORT_DATE,TIME,BASE_SAME,BASE_ACCUMULATE",
        )
        return self._map_eastmoney_rows(
            rows=rows,
            key="industrial_yoy",
            period_field="TIME",
            value_field="BASE_SAME",
            unit="%",
        )

    def _map_eastmoney_consumer_goods_retail(self) -> list[dict[str, Any]]:
        rows = self._fetch_eastmoney_table(
            report_name="RPT_ECONOMY_TOTAL_RETAIL",
            columns="REPORT_DATE,TIME,RETAIL_TOTAL,RETAIL_TOTAL_SAME,RETAIL_TOTAL_SEQUENTIAL,RETAIL_TOTAL_ACCUMULATE,RETAIL_ACCUMULATE_SAME",
        )
        return self._map_eastmoney_rows(
            rows=rows,
            key="retail_sales_yoy",
            period_field="TIME",
            value_field="RETAIL_ACCUMULATE_SAME",
            unit="%",
        )

    def _map_eastmoney_fixed_asset_investment(self) -> list[dict[str, Any]]:
        rows = self._fetch_eastmoney_table(
            report_name="RPT_ECONOMY_ASSET_INVEST",
            columns="REPORT_DATE,TIME,BASE,BASE_SAME,BASE_SEQUENTIAL,BASE_ACCUMULATE",
        )
        return self._map_eastmoney_rows(
            rows=rows,
            key="fixed_asset_yoy",
            period_field="TIME",
            value_field="BASE_SAME",
            unit="%",
        )

    def _fetch_eastmoney_table(self, report_name: str, columns: str, page_size: int = 2000) -> list[dict[str, Any]]:
        response = self.session.get(
            self.EASTMONEY_URL,
            params={
                "columns": columns,
                "pageNumber": "1",
                "pageSize": str(page_size),
                "sortColumns": "REPORT_DATE",
                "sortTypes": "-1",
                "source": "WEB",
                "client": "WEB",
                "reportName": report_name,
                "p": "1",
                "pageNo": "1",
                "pageNum": "1",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        return list(((payload.get("result") or {}).get("data") or []))

    def _map_eastmoney_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        key: str,
        period_field: str,
        value_field: str,
        unit: str,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for row in rows:
            value = self._to_float(row.get(value_field))
            if value is None:
                continue
            period_label = self._format_period_label(row.get(period_field))
            if not period_label:
                continue
            result.append(
                {
                    "series_code": key,
                    "series_label": self.NBS_SERIES_CONFIG[key]["label"],
                    "period_code": period_label,
                    "period_label": period_label,
                    "value_raw": value,
                    "value": round(value, 2),
                    "unit": unit,
                }
            )
        result.sort(key=lambda item: item["period_code"], reverse=True)
        return result[:limit]

    @staticmethod
    def _format_period_label(value: Any) -> str:
        if value is None:
            return ""
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m")
        return str(value).strip()

    def _build_macro_snapshot(self, macro_series: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        snapshot: dict[str, Any] = {}
        for key, series in macro_series.items():
            if not series:
                continue
            latest = series[0]
            previous = series[1] if len(series) > 1 else None
            change = round(latest["value"] - previous["value"], 2) if previous else None
            snapshot[key] = {
                "label": self.NBS_SERIES_CONFIG[key]["label"],
                "value": latest["value"],
                "value_raw": latest["value_raw"],
                "unit": latest["unit"],
                "period_label": latest["period_label"],
                "previous_value": previous["value"] if previous else None,
                "previous_period_label": previous["period_label"] if previous else None,
                "change": change,
            }
        return snapshot

    def _build_macro_tables(self, macro_series: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
        tables: dict[str, list[dict[str, Any]]] = {}
        for key, series in macro_series.items():
            if not series:
                continue
            data_frame = pd.DataFrame(
                [
                    {
                        "期间": item["period_label"],
                        "数值": item["value"],
                        "原始值": item["value_raw"],
                        "单位": item["unit"] or "-",
                    }
                    for item in series
                ]
            )
            tables[key] = data_frame.to_dict(orient="records")
        return tables

    def _fetch_market_indices(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for label, symbol in self.A_SHARE_INDEX_CONFIG.items():
            data_frame = ak.stock_zh_index_daily(symbol=symbol)
            if data_frame is None or data_frame.empty:
                continue
            latest = data_frame.iloc[-1]
            previous = data_frame.iloc[-2] if len(data_frame) > 1 else latest
            previous_close = float(previous["close"]) if float(previous["close"]) != 0 else 0.0
            result[label] = {
                "close": round(float(latest["close"]), 2),
                "date": str(latest["date"]),
                "daily_change_pct": round(((float(latest["close"]) - previous_close) / previous_close) * 100, 2)
                if previous_close
                else 0.0,
                "pct_20d": self._calc_return(data_frame, 20, "close"),
                "pct_60d": self._calc_return(data_frame, 60, "close"),
            }
        return result

    @staticmethod
    def _calc_return(data_frame: pd.DataFrame, days: int, column: str) -> float:
        if len(data_frame) <= days:
            return 0.0
        latest = float(data_frame.iloc[-1][column])
        base = float(data_frame.iloc[-days - 1][column])
        if base == 0:
            return 0.0
        return round((latest - base) / base * 100, 2)

    def _fetch_macro_news(self, limit: int = 12) -> list[dict[str, str]]:
        data_frame = ak.stock_info_global_em()
        if data_frame is None or data_frame.empty:
            return []
        keywords = ["财政", "货币", "央行", "国常会", "国务院", "地产", "消费", "PMI", "CPI", "PPI", "失业率", "投资", "论坛"]
        rows: list[dict[str, str]] = []
        for _, row in data_frame.iterrows():
            title = str(row.get("标题", ""))
            summary = str(row.get("摘要", ""))
            if keywords and not any(word in title or word in summary for word in keywords):
                continue
            rows.append(
                {
                    "title": title,
                    "summary": summary[:180],
                    "publish_time": str(row.get("发布时间", "")),
                    "url": str(row.get("链接", "")),
                }
            )
            if len(rows) >= limit:
                break
        return rows

    def build_rule_based_sector_view(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        scores = {sector: 0 for sector in self.SECTOR_STOCK_POOLS}
        reasons = {sector: [] for sector in self.SECTOR_STOCK_POOLS}

        def value_of(key: str) -> Optional[float]:
            return snapshot.get(key, {}).get("value")

        manufacturing_pmi = value_of("manufacturing_pmi")
        non_manufacturing_pmi = value_of("non_manufacturing_pmi")
        cpi_yoy = value_of("cpi_yoy")
        ppi_yoy = value_of("ppi_yoy")
        m2_yoy = value_of("m2_yoy")
        retail_sales_yoy = value_of("retail_sales_yoy")
        fixed_asset_yoy = value_of("fixed_asset_yoy")
        real_estate_yoy = value_of("real_estate_invest_yoy")
        unemployment = value_of("urban_unemployment")
        industrial_yoy = value_of("industrial_yoy")

        if m2_yoy is not None and m2_yoy >= 7:
            for sector in ["银行", "券商", "保险", "公用事业", "通信运营商"]:
                scores[sector] += 2
                reasons[sector].append("流动性保持充裕")
        if cpi_yoy is not None and cpi_yoy <= 1:
            for sector in ["银行", "公用事业", "食品饮料", "家电"]:
                scores[sector] += 1
                reasons[sector].append("通胀温和为估值修复留出空间")
        if manufacturing_pmi is not None and manufacturing_pmi >= 50:
            for sector in ["工程机械", "有色金属", "半导体", "算力AI", "软件信创"]:
                scores[sector] += 2
                reasons[sector].append("制造业景气改善")
        elif manufacturing_pmi is not None:
            for sector in ["工程机械", "有色金属", "半导体"]:
                scores[sector] -= 1
                reasons[sector].append("制造业景气仍在荣枯线下")
        if non_manufacturing_pmi is not None and non_manufacturing_pmi >= 50:
            for sector in ["旅游酒店", "食品饮料", "家电", "汽车整车"]:
                scores[sector] += 1
                reasons[sector].append("服务消费活跃度改善")
        if retail_sales_yoy is not None and retail_sales_yoy >= 4:
            for sector in ["食品饮料", "家电", "旅游酒店", "汽车整车"]:
                scores[sector] += 2
                reasons[sector].append("消费数据偏强")
        if fixed_asset_yoy is not None and fixed_asset_yoy >= 3:
            for sector in ["工程机械", "电网设备", "有色金属"]:
                scores[sector] += 2
                reasons[sector].append("投资端仍有托底")
        if industrial_yoy is not None and industrial_yoy >= 5:
            for sector in ["工程机械", "有色金属", "军工", "半导体"]:
                scores[sector] += 1
                reasons[sector].append("工业生产维持扩张")
        if ppi_yoy is not None and ppi_yoy < 0:
            for sector in ["煤炭", "石油石化", "有色金属"]:
                scores[sector] -= 1
                reasons[sector].append("工业品价格仍承压")
        if real_estate_yoy is not None and real_estate_yoy < 0:
            for sector in ["房地产", "建材家居"]:
                scores[sector] -= 3
                reasons[sector].append("地产投资仍弱")
        if unemployment is not None and unemployment >= 5.3:
            for sector in ["旅游酒店"]:
                scores[sector] -= 1
                reasons[sector].append("就业压力抑制可选消费")

        bullish = sorted(
            [
                {
                    "sector": sector,
                    "score": score,
                    "logic": "；".join(reasons[sector][:3]) or "宏观环境相对受益",
                }
                for sector, score in scores.items()
                if score > 0
            ],
            key=lambda item: item["score"],
            reverse=True,
        )[:6]
        bearish = sorted(
            [
                {
                    "sector": sector,
                    "score": score,
                    "logic": "；".join(reasons[sector][:3]) or "宏观环境相对承压",
                }
                for sector, score in scores.items()
                if score < 0
            ],
            key=lambda item: item["score"],
        )[:4]
        return {
            "market_view": self._infer_market_view(snapshot),
            "bullish_sectors": bullish,
            "bearish_sectors": bearish,
            "watch_signals": self._build_watch_signals(snapshot),
        }

    def _infer_market_view(self, snapshot: dict[str, Any]) -> str:
        growth_score = 0
        if snapshot.get("gdp_yoy", {}).get("value", 0) >= 4.5:
            growth_score += 1
        if snapshot.get("manufacturing_pmi", {}).get("value", 0) >= 50:
            growth_score += 1
        if snapshot.get("retail_sales_yoy", {}).get("value", 0) >= 4:
            growth_score += 1
        if snapshot.get("real_estate_invest_yoy", {}).get("value", 0) < 0:
            growth_score -= 1
        if snapshot.get("urban_unemployment", {}).get("value", 0) >= 5.3:
            growth_score -= 1
        if growth_score >= 2:
            return "震荡偏多"
        if growth_score <= -1:
            return "震荡偏谨慎"
        return "结构性机会为主"

    def _build_watch_signals(self, snapshot: dict[str, Any]) -> list[str]:
        signals: list[str] = []
        for key in ["manufacturing_pmi", "retail_sales_yoy", "m2_yoy", "real_estate_invest_yoy"]:
            item = snapshot.get(key)
            if not item:
                continue
            if item.get("change") is not None:
                signals.append(
                    f"{item['label']} 最新 {item['period_label']} 为 {item['value']}{item['unit']}，较上一期变动 {item['change']:+.2f}{item['unit']}"
                )
            else:
                signals.append(f"{item['label']} 最新 {item['period_label']} 为 {item['value']}{item['unit']}")
        return signals

    def build_stock_candidates_for_sectors(self, sectors: list[str], limit_per_sector: int = 3, total_limit: int = 12) -> list[dict[str, Any]]:
        selected_sector_keys: list[str] = []
        for sector in sectors:
            for key in self._match_sector_keys(sector):
                if key not in selected_sector_keys:
                    selected_sector_keys.append(key)
        if not selected_sector_keys:
            selected_sector_keys = ["银行", "公用事业", "食品饮料", "半导体"]

        candidates: list[dict[str, Any]] = []
        for sector_key in selected_sector_keys:
            for stock in self.SECTOR_STOCK_POOLS.get(sector_key, [])[:limit_per_sector]:
                enriched = self._enrich_stock_snapshot(stock["code"], stock["name"], sector_key)
                if enriched:
                    candidates.append(enriched)
                if len(candidates) >= total_limit:
                    return candidates
        return candidates

    def _match_sector_keys(self, sector_name: str) -> list[str]:
        if sector_name in self.SECTOR_STOCK_POOLS:
            return [sector_name]
        matches = [key for key in self.SECTOR_STOCK_POOLS if sector_name in key or key in sector_name]
        if matches:
            return matches
        for alias, mapped in self.SECTOR_ALIASES.items():
            if alias in sector_name:
                return mapped
        return []

    def _enrich_stock_snapshot(self, code: str, fallback_name: str, sector_name: str) -> Optional[dict[str, Any]]:
        info_map: dict[str, str] = {}
        try:
            info_df = ak.stock_individual_info_em(symbol=code)
            if info_df is not None and not info_df.empty:
                info_map = {str(row["item"]).strip(): str(row["value"]).strip() for _, row in info_df.iterrows()}
        except Exception as exc:
            self.logger.warning("获取个股静态信息失败 %s: %s", code, exc)

        default_payload = {
            "code": code,
            "name": info_map.get("股票简称", fallback_name),
            "sector": sector_name,
            "industry": info_map.get("行业", sector_name),
            "price": None,
            "daily_change_pct": None,
            "change_amount": None,
            "turnover_rate": None,
            "volume": None,
            "pe_ratio": self._to_float(info_map.get("市盈率(动态)")),
            "pb_ratio": self._to_float(info_map.get("市净率")),
            "market_cap": self._to_float(info_map.get("总市值")),
            "recent_20d_return": None,
            "recent_60d_return": None,
            "listed_date": info_map.get("上市时间", ""),
        }

        try:
            start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
            end_date = datetime.now().strftime("%Y%m%d")
            hist_df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if hist_df is None or hist_df.empty:
                return default_payload
            latest = hist_df.iloc[-1]
            return {
                **default_payload,
                "price": round(float(latest["收盘"]), 2),
                "daily_change_pct": round(float(latest["涨跌幅"]), 2),
                "change_amount": round(float(latest["涨跌额"]), 2),
                "turnover_rate": self._to_float(latest.get("换手率")),
                "volume": self._to_float(latest.get("成交量")),
                "recent_20d_return": self._calc_return(hist_df, 20, "收盘"),
                "recent_60d_return": self._calc_return(hist_df, 60, "收盘"),
            }
        except Exception as exc:
            self.logger.warning("获取候选股数据失败 %s: %s", code, exc)
            return default_payload

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value in (None, "", "-", "--"):
            return None
        try:
            return round(float(str(value).replace(",", "")), 2)
        except Exception:
            return None

    def build_prompt_context(self, data: dict[str, Any]) -> str:
        snapshot = data.get("macro_snapshot", {})
        lines = ["===== 当前国内宏观数据快照（综合数据源） ====="]
        for key in self.NBS_SERIES_CONFIG:
            item = snapshot.get(key)
            if not item:
                continue
            change_str = f"，较上一期变动 {item['change']:+.2f}{item['unit']}" if item.get("change") is not None else ""
            lines.append(f"- {item['label']}: {item['value']}{item['unit']} ({item['period_label']}){change_str}")

        lines.append("")
        lines.append("===== A股指数快照 =====")
        for name, info in data.get("market_indices", {}).items():
            lines.append(
                f"- {name}: {info['close']}，日涨跌 {info['daily_change_pct']:+.2f}%，20日 {info['pct_20d']:+.2f}%，60日 {info['pct_60d']:+.2f}%"
            )

        if data.get("news"):
            lines.append("")
            lines.append("===== 宏观新闻样本 =====")
            for item in data["news"][:8]:
                lines.append(f"- {item['publish_time']} | {item['title']} | {item['summary']}")

        lines.append("")
        lines.append("===== 可选行业板块池（供AI输出时严格从中选择） =====")
        lines.append("、".join(self.SECTOR_STOCK_POOLS.keys()))
        return "\n".join(lines)
