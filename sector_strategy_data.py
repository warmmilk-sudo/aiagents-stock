"""
智策板块数据采集模块
优先使用 Tushare/RSSHub 等稳定数据源。
"""

import concurrent.futures
import json
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import warnings
import logging
import os
import threading
from dotenv import load_dotenv
from sector_strategy_db import SectorStrategyDatabase
from tushare_utils import create_tushare_pro

# 加载环境变量
load_dotenv()

warnings.filterwarnings('ignore')


class SectorStrategyDataFetcher:
    """板块策略数据获取类"""

    _CORE_DATA_KEYS = ("sectors", "concepts", "sector_fund_flow")
    _OPTIONAL_DATA_KEYS = ("market_overview", "north_flow", "news", "macro_data")
    
    def __init__(self):
        print("[智策] 板块数据获取器初始化...")
        self.max_fetch_workers = max(1, int(os.getenv("SECTOR_STRATEGY_FETCH_WORKERS", "3")))
        
        # 初始化数据库和日志
        self.database = SectorStrategyDatabase()
        self.logger = logging.getLogger(__name__)
        self._tushare_api = None
        self._tushare_url = None
        self._tushare_init_lock = threading.Lock()
        self._tushare_call_lock = threading.Lock()
        self._dc_index_cache = {}
        
        # 配置日志
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _fetch_data_source(self, step_label, fetch_func):
        try:
            return step_label, fetch_func(), None
        except Exception as exc:
            return step_label, None, exc

    @staticmethod
    def _to_float(value):
        try:
            if value is None:
                return None
            text = str(value).replace(",", "").replace("%", "").strip()
            if not text:
                return None
            return float(text)
        except (TypeError, ValueError):
            return None

    def _iter_frame_rows(self, df):
        if df is None or getattr(df, "empty", True):
            return []
        try:
            return [row for _, row in df.iterrows()]
        except Exception:
            return []

    @staticmethod
    def _new_data_payload(*, success=False):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return {
            "success": success,
            "timestamp": timestamp,
            "fetch_timestamp": timestamp,
            "source_trade_date": "",
            "source_trade_dates": {},
            "sectors": {},
            "concepts": {},
            "sector_fund_flow": {},
            "market_overview": {},
            "north_flow": {},
            "macro_data": {},
            "news": [],
        }

    def _read_cached_content(
        self,
        *,
        log_label,
        default_factory,
        content_type,
        cache_key=None,
        fetcher_name="get_latest_raw_data",
        log_on_hit=False,
    ):
        database = getattr(self, "database", None)
        default_value = default_factory()
        if database is None:
            return default_value

        fetcher = getattr(database, fetcher_name, None)
        if fetcher is None:
            return default_value

        try:
            cached_payload = fetcher(cache_key) if cache_key is not None else fetcher()
        except Exception as exc:
            self.logger.warning("[智策数据] 读取%s缓存失败: %s", log_label, exc)
            return default_value

        if not isinstance(cached_payload, dict):
            return default_value

        cached_content = cached_payload.get("data_content")
        if not isinstance(cached_content, content_type) or not cached_content:
            return default_value

        if log_on_hit:
            self.logger.warning("[智策数据] %s已回退到最近缓存快照", log_label)
        return cached_content

    @staticmethod
    def _format_trade_date(value):
        text = str(value or "").strip()
        if not text:
            return ""
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 8:
            return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        return text

    @classmethod
    def _first_trade_date_from_frame(cls, df):
        if df is None or getattr(df, "empty", True):
            return ""
        try:
            if "trade_date" not in getattr(df, "columns", []):
                return ""
            return cls._format_trade_date(df.iloc[0].get("trade_date"))
        except Exception:
            return ""

    @classmethod
    def _first_trade_date_from_boards(cls, boards):
        if not isinstance(boards, dict):
            return ""
        for item in boards.values():
            if isinstance(item, dict):
                trade_date = cls._format_trade_date(item.get("trade_date") or item.get("source_trade_date"))
                if trade_date:
                    return trade_date
        return ""

    @classmethod
    def _resolve_source_trade_date(cls, data):
        if not isinstance(data, dict):
            return datetime.now().strftime('%Y-%m-%d')

        source_dates = data.setdefault("source_trade_dates", {})
        if isinstance(data.get("sector_fund_flow"), dict):
            trade_date = cls._format_trade_date(
                data["sector_fund_flow"].get("trade_date") or data["sector_fund_flow"].get("source_trade_date")
            )
            if trade_date:
                source_dates["sector_fund_flow"] = trade_date
        for key in ("sectors", "concepts"):
            trade_date = cls._first_trade_date_from_boards(data.get(key))
            if trade_date:
                source_dates[key] = trade_date
        if isinstance(data.get("market_overview"), dict):
            trade_date = cls._format_trade_date(
                data["market_overview"].get("trade_date") or data["market_overview"].get("source_trade_date")
            )
            if trade_date:
                source_dates["market_overview"] = trade_date
        if isinstance(data.get("north_flow"), dict):
            trade_date = cls._format_trade_date(
                data["north_flow"].get("source_trade_date") or data["north_flow"].get("date")
            )
            if trade_date:
                source_dates["north_flow"] = trade_date

        for key in ("sector_fund_flow", "sectors", "concepts", "market_overview", "north_flow"):
            if source_dates.get(key):
                return source_dates[key]
        return datetime.now().strftime('%Y-%m-%d')

    @staticmethod
    def _row_has_key(row, key):
        try:
            if key in row:
                return True
        except Exception:
            pass
        try:
            return key in row.index
        except Exception:
            return False

    def _build_market_breadth_overview(self, rows):
        if not rows:
            return {}

        total_count = 0
        up_count = 0
        down_count = 0
        flat_count = 0
        limit_up = 0
        limit_down = 0

        for row in rows:
            change_pct = self._to_float(row.get('涨跌幅', row.get('pct_chg')))
            if change_pct is None:
                continue
            total_count += 1
            if change_pct > 0:
                up_count += 1
            elif change_pct < 0:
                down_count += 1
            else:
                flat_count += 1

            if change_pct >= 9.5:
                limit_up += 1
            elif change_pct <= -9.5:
                limit_down += 1

        if total_count <= 0:
            return {}

        return {
            "total_stocks": total_count,
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "up_ratio": round(up_count / total_count * 100, 2),
            "limit_up": limit_up,
            "limit_down": limit_down,
        }

    def _get_market_breadth_rows(self):
        return self._get_market_breadth_rows_from_tushare()

    def _get_market_breadth_rows_from_tushare(self):
        df = self._fetch_tushare_trade_data('daily', fields='ts_code,trade_date,pct_chg')
        return self._iter_frame_rows(df)

    def _get_market_index_overview(self):
        return self._get_market_index_overview_from_tushare()

    def _get_market_index_overview_from_tushare(self):
        index_map = {
            "000001.SH": ("sh_index", "上证指数"),
            "399001.SZ": ("sz_index", "深证成指"),
            "399006.SZ": ("cyb_index", "创业板指"),
        }
        overview = {}

        for ts_code, (target_key, display_name) in index_map.items():
            df = self._fetch_tushare_trade_data(
                'index_daily',
                ts_code=ts_code,
                fields='ts_code,trade_date,close,change,pct_chg',
            )
            rows = self._iter_frame_rows(df)
            if not rows:
                continue
            row = rows[0]
            overview[target_key] = {
                "code": ts_code.split(".")[0],
                "name": display_name,
                "close": self._clean_value(row.get('close')),
                "change_pct": self._clean_value(row.get('pct_chg')),
                "change": self._clean_value(row.get('change')),
                "trade_date": self._format_trade_date(row.get('trade_date')),
            }

        return overview

    def get_all_sector_data(self):
        """
        获取所有板块的综合数据
        
        Returns:
            dict: 包含多个维度的板块数据
        """
        print("[智策] 开始获取板块综合数据...")
        
        data = self._new_data_payload()

        try:
            fetch_specs = [
                ("sectors", "[1/7] 获取行业板块行情...", self._get_sector_performance, True),
                ("concepts", "[2/7] 获取概念板块行情...", self._get_concept_performance, True),
                ("sector_fund_flow", "[3/7] 获取行业资金流向...", self._get_sector_fund_flow, True),
                ("market_overview", "[4/7] 获取市场总体情况...", self._get_market_overview, False),
                ("north_flow", "[5/7] 获取北向资金流向...", self._get_north_money_flow, False),
                ("macro_data", "[6/7] 获取宏观指标快照...", self._get_macro_data, False),
                ("news", "[7/7] 获取财经新闻...", self._get_financial_news, False),
            ]

            for _, step_label, _, _ in fetch_specs:
                print(f"  {step_label}")

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(len(fetch_specs), max(1, int(getattr(self, "max_fetch_workers", 3) or 3)))
            ) as executor:
                future_map = {
                    executor.submit(self._fetch_data_source, step_label, fetch_func): (result_key, is_core)
                    for result_key, step_label, fetch_func, is_core in fetch_specs
                }

                fetch_errors = {}
                for future in concurrent.futures.as_completed(future_map):
                    result_key, is_core = future_map[future]
                    step_label, payload, error = future.result()
                    if error is not None:
                        print(f"    ✗ {step_label} 失败: {error}")
                        if is_core:
                            fetch_errors[result_key] = error
                        else:
                            self.logger.warning("[智策数据] 非核心步骤失败，继续执行: %s -> %s", result_key, error)
                        continue

                    if not payload:
                        continue

                    data[result_key] = payload
                    if result_key == "sectors":
                        print(f"    ✓ 成功获取 {len(payload)} 个行业板块数据")
                    elif result_key == "concepts":
                        print(f"    ✓ 成功获取 {len(payload)} 个概念板块数据")
                    elif result_key == "sector_fund_flow":
                        print("    ✓ 成功获取资金流向数据")
                    elif result_key == "market_overview":
                        print("    ✓ 成功获取市场概况")
                    elif result_key == "north_flow":
                        print("    ✓ 成功获取北向资金数据")
                    elif result_key == "macro_data":
                        snapshot_size = len(payload.get("macro_snapshot", {})) if isinstance(payload, dict) else 0
                        print(f"    ✓ 成功获取 {snapshot_size} 项宏观指标")
                    elif result_key == "news":
                        print(f"    ✓ 成功获取 {len(payload)} 条新闻")

            missing_core = [key for key in self._CORE_DATA_KEYS if not data.get(key)]
            if missing_core:
                missing_parts = []
                for key in missing_core:
                    error = fetch_errors.get(key)
                    missing_parts.append(f"{key}({error})" if error else key)
                raise RuntimeError(f"核心板块数据缺失: {', '.join(missing_parts)}")

            data["source_trade_date"] = self._resolve_source_trade_date(data)
            if data.get("news"):
                data["news"] = self._enhance_news_items(data.get("news", []), data)
            data["success"] = True
            print("[智策] ✓ 板块数据获取完成！")
            
            # 保存原始数据到数据库
            self._save_raw_data_to_db(data)
            
        except Exception as e:
            print(f"[智策] ✗ 数据获取出错: {e}")
            data["error"] = str(e)
        
        return data

    def _ensure_tushare_api(self):
        """按需初始化 Tushare 客户端。"""
        if self._tushare_api is not None:
            return self._tushare_api

        with self._tushare_init_lock:
            if self._tushare_api is not None:
                return self._tushare_api

            tushare_token = os.getenv('TUSHARE_TOKEN', '').strip()
            if not tushare_token:
                self.logger.info("[Tushare] 未配置 Token，跳过备用数据源")
                return None

            try:
                self._tushare_api, self._tushare_url = create_tushare_pro(
                    token=tushare_token,
                )
                if self._tushare_api:
                    self.logger.info(f"[Tushare] 初始化成功，地址: {self._tushare_url}")
            except Exception as e:
                self.logger.warning(f"[Tushare] 初始化失败: {e}")
                self._tushare_api = None

        return self._tushare_api

    def _call_tushare_api(self, api_name, **kwargs):
        pro = self._ensure_tushare_api()
        if not pro:
            return pd.DataFrame()

        method = getattr(pro, api_name, None)
        if method is None:
            self.logger.warning("[Tushare] 不支持接口: %s", api_name)
            return pd.DataFrame()

        with self._tushare_call_lock:
            return method(**kwargs)

    def _fetch_tushare_trade_data(self, api_name, *, max_days=7, **kwargs):
        """按最近交易日回退查询 Tushare 数据。"""
        last_error = None
        for offset in range(max_days):
            trade_date = (datetime.now() - timedelta(days=offset)).strftime('%Y%m%d')
            params = dict(kwargs)
            params.setdefault('trade_date', trade_date)
            try:
                df = self._call_tushare_api(api_name, **params)
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                last_error = e

        if last_error:
            self.logger.warning(f"[Tushare] {api_name} 获取失败: {last_error}")
        return pd.DataFrame()

    def _get_tushare_board_snapshot(self, idx_type):
        """获取最近可用的东财板块快照。"""
        if idx_type in self._dc_index_cache:
            return self._dc_index_cache[idx_type]

        df = self._fetch_tushare_trade_data(
            'dc_index',
            idx_type=idx_type,
        )
        self._dc_index_cache[idx_type] = df
        return df

    @staticmethod
    def _clean_value(value, default=None):
        if pd.isna(value):
            return default
        return value

    def _convert_tushare_board_snapshot(self, df):
        boards = {}
        if df is None or df.empty:
            return boards

        for _, row in df.iterrows():
            board_name = self._clean_value(row['name'])
            if not board_name:
                continue

            market_cap = self._clean_value(row['total_mv'])
            boards[board_name] = {
                "name": board_name,
                "change_pct": self._clean_value(row['pct_change']),
                "turnover": self._clean_value(row['turnover_rate']),
                "market_cap": market_cap,
                "total_market_cap": market_cap,
                "top_stock": self._clean_value(row['leading']),
                "top_stock_change": self._clean_value(row['leading_pct']),
                "up_count": self._clean_value(row['up_num']),
                "down_count": self._clean_value(row['down_num']),
                "ts_code": self._clean_value(row['ts_code']),
                "trade_date": self._format_trade_date(row.get('trade_date')),
            }

        return boards
    
    def _get_sector_performance(self):
        """获取行业板块表现"""
        tushare_df = self._get_tushare_board_snapshot("行业板块")
        if tushare_df is not None and not tushare_df.empty:
            print(f"    [Tushare] 行业板块数据获取成功，共 {len(tushare_df)} 条")
            return self._convert_tushare_board_snapshot(tushare_df)
        return self._read_cached_content(
            cache_key="sectors",
            log_label="行业板块数据",
            default_factory=dict,
            content_type=dict,
            log_on_hit=True,
        )
    
    def _get_concept_performance(self):
        """获取概念板块表现"""
        tushare_df = self._get_tushare_board_snapshot("概念板块")
        if tushare_df is not None and not tushare_df.empty:
            print(f"    [Tushare] 概念板块数据获取成功，共 {len(tushare_df)} 条")
            return self._convert_tushare_board_snapshot(tushare_df)
        return self._read_cached_content(
            cache_key="concepts",
            log_label="概念板块数据",
            default_factory=dict,
            content_type=dict,
            log_on_hit=True,
        )
    
    def _convert_fund_flow_frame(self, df, *, source_type, pct_map=None):
        items = []
        if df is None or df.empty:
            return items
        pct_map = pct_map or {}

        for _, row in df.iterrows():
            sector_name = self._clean_value(row.get('name'))
            if not sector_name:
                continue
            if sector_name in pct_map:
                change_pct = pct_map[sector_name]
            elif self._row_has_key(row, 'pct_change'):
                change_pct = self._clean_value(row.get('pct_change'))
            else:
                change_pct = None
            items.append({
                "sector": sector_name,
                "source_type": source_type,
                "content_type": self._clean_value(row.get('content_type')) or ("概念" if source_type == "concept" else "行业"),
                "ts_code": self._clean_value(row.get('ts_code')),
                "trade_date": self._format_trade_date(row.get('trade_date')),
                "main_net_inflow": self._clean_value(row.get('net_amount')),
                "main_net_inflow_pct": self._clean_value(row.get('net_amount_rate')),
                "super_large_net_inflow": self._clean_value(row.get('buy_elg_amount')),
                "super_large_net_inflow_pct": self._clean_value(row.get('buy_elg_amount_rate')),
                "large_net_inflow": self._clean_value(row.get('buy_lg_amount')),
                "large_net_inflow_pct": self._clean_value(row.get('buy_lg_amount_rate')),
                "medium_net_inflow": self._clean_value(row.get('buy_md_amount')),
                "medium_net_inflow_pct": self._clean_value(row.get('buy_md_amount_rate')),
                "small_net_inflow": self._clean_value(row.get('buy_sm_amount')),
                "small_net_inflow_pct": self._clean_value(row.get('buy_sm_amount_rate')),
                "change_pct": change_pct,
            })
        return items

    def _board_pct_map(self, idx_type):
        snapshot = self._get_tushare_board_snapshot(idx_type)
        if snapshot is None or snapshot.empty:
            return {}
        return {
            self._clean_value(row['name']): self._clean_value(row['pct_change'])
            for _, row in snapshot.iterrows()
        }

    def _get_sector_fund_flow(self):
        """获取行业与概念资金流向"""
        fund_flow = {
            "today": [],
            "industry": [],
            "concept": [],
            "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "trade_date": "",
        }

        fetch_plan = (
            ("行业", "industry", "行业板块"),
            ("概念", "concept", "概念板块"),
        )
        for content_type, source_type, board_snapshot_type in fetch_plan:
            tushare_df = self._fetch_tushare_trade_data(
                'moneyflow_ind_dc',
                content_type=content_type,
            )
            if tushare_df is None or tushare_df.empty:
                continue
            items = self._convert_fund_flow_frame(
                tushare_df,
                source_type=source_type,
                pct_map=self._board_pct_map(board_snapshot_type),
            )
            fund_flow[source_type] = items
            fund_flow["today"].extend(items)
            if not fund_flow["trade_date"]:
                fund_flow["trade_date"] = self._first_trade_date_from_frame(tushare_df)

        if not fund_flow["today"]:
            return self._read_cached_content(
                cache_key="fund_flow",
                log_label="板块资金流向",
                default_factory=dict,
                content_type=dict,
                log_on_hit=True,
            )

        fund_flow["today"].sort(key=lambda item: self._to_float(item.get("main_net_inflow")) or 0.0, reverse=True)
        print(
            "    [Tushare] 板块资金流向获取成功，行业 %s 条，概念 %s 条"
            % (len(fund_flow["industry"]), len(fund_flow["concept"]))
        )
        return fund_flow
    
    def _get_market_overview(self):
        """获取市场总体情况"""
        breadth_overview = self._build_market_breadth_overview(self._get_market_breadth_rows())
        index_overview = self._get_market_index_overview()
        overview = {}
        if breadth_overview:
            overview.update(breadth_overview)
        if index_overview:
            overview.update(index_overview)
            for item in index_overview.values():
                if isinstance(item, dict) and item.get("trade_date"):
                    overview["trade_date"] = item.get("trade_date")
                    break

        cached_overview = self._read_cached_content(
            cache_key="market_overview",
            log_label="市场概况",
            default_factory=dict,
            content_type=dict,
            log_on_hit=False,
        )
        if not overview:
            if cached_overview:
                self.logger.warning("[智策数据] 市场概况已回退到最近缓存快照")
            return cached_overview

        for key, value in cached_overview.items():
            overview.setdefault(key, value)
        return overview
    
    def _get_north_money_flow(self):
        """获取北向资金流向。"""
        if not self._ensure_tushare_api():
            return {}

        print("    [Tushare] 正在获取沪深港通资金流向...")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=20)

        try:
            df = self._call_tushare_api(
                'moneyflow_hsgt',
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d')
            )
        except Exception as exc:
            self.logger.warning("[智策数据] 北向资金获取失败: %s", exc)
            return {}

        if df is None or df.empty:
            return {}

        df = df.sort_values('trade_date', ascending=False)
        latest = df.iloc[0]
        latest_trade_date = self._format_trade_date(latest['trade_date'])
        north_flow = {
            "date": str(latest['trade_date']),
            "source_trade_date": latest_trade_date,
            "north_net_inflow": float(latest['north_money']),
            "hgt_net_inflow": float(latest['hgt']),
            "sgt_net_inflow": float(latest['sgt']),
            "north_total_amount": float(latest['north_money']),
            "history": [
                {
                    "date": str(row['trade_date']),
                    "source_trade_date": self._format_trade_date(row['trade_date']),
                    "net_inflow": float(row['north_money']),
                }
                for _, row in df.head(20).iterrows()
            ],
        }
        print("    [Tushare] 成功获取数据")
        return north_flow
    
    def _get_financial_news(self):
        """获取财经新闻"""
        try:
            from news_flow_data import NewsFlowDataFetcher

            fetcher = NewsFlowDataFetcher()
            result = fetcher.get_multi_platform_news(category="finance")
            news_list = []
            for platform_data in result.get("platforms_data", []):
                if not platform_data.get("success"):
                    continue
                for row in platform_data.get("data", [])[:40]:
                    title = self._clean_value(row.get("title"))
                    if not title:
                        continue
                    news_list.append(
                        {
                            "title": title,
                            "content": self._clean_value(row.get("content")),
                            "publish_time": str(self._clean_value(row.get("publish_time"), "")),
                            "source": self._clean_value(row.get("source")) or platform_data.get("platform_name"),
                            "url": self._clean_value(row.get("url"), ""),
                        }
                    )
            if news_list:
                print(f"    [RSSHub] 财经新闻获取成功，共 {len(news_list)} 条")
                return news_list[:150]
        except Exception as e:
            self.logger.warning("[智策数据] RSSHub财经新闻获取失败: %s", e)
        return self._read_cached_content(
            log_label="财经新闻",
            default_factory=list,
            content_type=list,
            fetcher_name="get_latest_news_data",
            log_on_hit=True,
        )

    def _macro_cache_is_fresh(self, cached_payload):
        if not isinstance(cached_payload, dict):
            return False
        max_age_hours = float(os.getenv("SECTOR_STRATEGY_MACRO_CACHE_HOURS", "72") or 72)
        timestamp_text = str(
            cached_payload.get("source_created_at")
            or cached_payload.get("timestamp")
            or ""
        ).strip()
        if not timestamp_text:
            return False
        try:
            parsed = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=None)
        except Exception:
            try:
                parsed = datetime.strptime(timestamp_text[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                return False
        return (datetime.now() - parsed) <= timedelta(hours=max_age_hours)

    def _fetch_full_macro_snapshot(self):
        from macro_analysis_data import MacroAnalysisDataFetcher

        macro_fetcher = MacroAnalysisDataFetcher()
        result = macro_fetcher.fetch_all_data()
        macro_snapshot = result.get("macro_snapshot") if isinstance(result, dict) else {}
        if not macro_snapshot:
            return {}
        return {
            "success": bool(result.get("success", True)),
            "timestamp": result.get("timestamp") or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "macro_analysis_fetch_all",
            "macro_series": result.get("macro_series", {}),
            "macro_snapshot": macro_snapshot,
            "macro_tables": result.get("macro_tables", {}),
            "rule_based_sector_view": result.get("rule_based_sector_view", {}),
            "errors": result.get("errors", []),
        }

    def _get_macro_data(self):
        """获取宏观指标快照，优先使用新鲜缓存，否则触发完整宏观采集。"""
        cached = self._load_latest_macro_analysis_snapshot()
        if cached and self._macro_cache_is_fresh(cached):
            return cached
        if not os.getenv("TUSHARE_TOKEN", "").strip():
            if cached:
                self.logger.warning("[智策数据] 未配置Tushare Token，宏观指标已回退到最近缓存快照")
                return cached
            return {}

        try:
            refreshed = self._fetch_full_macro_snapshot()
            if refreshed:
                return refreshed
        except Exception as exc:
            self.logger.warning("[智策数据] 宏观指标快照获取失败: %s", exc)
        if cached:
            self.logger.warning("[智策数据] 宏观指标已回退到最近缓存快照")
            return cached
        return {}

    def _load_latest_macro_analysis_snapshot(self):
        """从宏观分析库读取最近一次已采集的宏观快照。"""
        db_path = os.getenv("MACRO_ANALYSIS_DB_PATH", "macro_analysis.db")
        if not db_path or not os.path.exists(db_path):
            return {}

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT id, analysis_date, result_json, created_at
                FROM macro_analysis_reports
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            conn.close()
        except Exception as exc:
            self.logger.warning("[智策数据] 读取宏观分析缓存失败: %s", exc)
            return {}

        if not row:
            return {}

        try:
            result_payload = json.loads(row["result_json"] or "{}")
        except Exception as exc:
            self.logger.warning("[智策数据] 解析宏观分析缓存失败: %s", exc)
            return {}

        raw_data = result_payload.get("raw_data") if isinstance(result_payload, dict) else {}
        if not isinstance(raw_data, dict) or not raw_data.get("macro_snapshot"):
            return {}

        cached_payload = {
            "success": bool(raw_data.get("success", True)),
            "timestamp": raw_data.get("timestamp") or row["analysis_date"] or row["created_at"],
            "source": "macro_analysis_cache",
            "source_report_id": row["id"],
            "source_created_at": row["created_at"],
            "macro_series": raw_data.get("macro_series", {}),
            "macro_snapshot": raw_data.get("macro_snapshot", {}),
            "macro_tables": raw_data.get("macro_tables", {}),
            "rule_based_sector_view": raw_data.get("rule_based_sector_view", {}),
            "errors": raw_data.get("errors", []),
        }
        self.logger.info(
            "[智策数据] 已复用宏观分析缓存 #%s，指标数: %s",
            cached_payload["source_report_id"],
            len(cached_payload["macro_snapshot"]),
        )
        return cached_payload

    def _build_news_sector_aliases(self, data):
        aliases = {}
        for board_key in ("sectors", "concepts"):
            boards = data.get(board_key) if isinstance(data, dict) else {}
            if not isinstance(boards, dict):
                continue
            for name in boards.keys():
                text = str(name or "").strip()
                normalized = text.replace("概念", "").replace("板块", "").strip()
                for alias in {text, normalized}:
                    if len(alias) >= 2:
                        aliases[alias] = text
        return aliases

    @staticmethod
    def _score_news_sentiment(text):
        positive_keywords = (
            "增长", "上调", "突破", "利好", "回暖", "复苏", "扩张", "创新高",
            "签约", "中标", "增持", "降准", "降息", "补贴", "支持", "提振",
        )
        negative_keywords = (
            "下调", "下跌", "亏损", "减持", "风险", "调查", "处罚", "违约",
            "制裁", "衰退", "收缩", "暴跌", "暂停", "终止", "预警",
        )
        positive_count = sum(1 for keyword in positive_keywords if keyword in text)
        negative_count = sum(1 for keyword in negative_keywords if keyword in text)
        if positive_count == negative_count:
            return 0.0
        score = (positive_count - negative_count) / max(positive_count + negative_count, 1)
        return round(max(-1.0, min(1.0, score)), 2)

    @staticmethod
    def _score_news_importance(text, related_count):
        important_keywords = (
            "国务院", "央行", "证监会", "发改委", "财政部", "商务部", "统计局",
            "美国", "美联储", "关税", "AI", "芯片", "半导体", "新能源", "机器人",
            "重大", "突发", "首次", "创新高", "政策", "会议", "财报",
        )
        score = 30 + min(35, int(related_count or 0) * 8)
        score += min(35, sum(7 for keyword in important_keywords if keyword in text))
        return float(max(0, min(100, score)))

    def _enhance_news_items(self, news_list, data):
        if not isinstance(news_list, list) or not news_list:
            return []
        aliases = self._build_news_sector_aliases(data)
        enhanced = []
        for item in news_list:
            if not isinstance(item, dict):
                continue
            payload = dict(item)
            title = str(payload.get("title") or "")
            content = str(payload.get("content") or "")
            text = f"{title}\n{content}"
            related = []
            for alias, canonical in aliases.items():
                if alias and alias in text and canonical not in related:
                    related.append(canonical)
                if len(related) >= 8:
                    break
            payload["related_sectors"] = related
            payload["sentiment_score"] = self._score_news_sentiment(text)
            payload["importance_score"] = self._score_news_importance(text, len(related))
            enhanced.append(payload)
        enhanced.sort(key=lambda row: (self._to_float(row.get("importance_score")) or 0.0, str(row.get("publish_time") or "")), reverse=True)
        return enhanced
    
    def format_data_for_ai(self, data):
        """
        将数据格式化为适合AI分析的文本格式
        """
        if not data.get("success"):
            return "数据获取失败"
        
        text_parts = []

        def _fmt_num(value, digits=2):
            if value is None:
                return None
            try:
                return f"{float(value):.{digits}f}"
            except Exception:
                return None
        
        # 市场概况
        if data.get("market_overview"):
            market = data["market_overview"]
            block = ["【市场总体情况】"]
            timestamp = data.get("timestamp")
            if timestamp:
                block.append(f"时间: {timestamp}")
            block.append("")
            block.append("大盘指数:")

            sh = market.get("sh_index")
            if sh:
                close = _fmt_num(sh.get("close"))
                change_pct = _fmt_num(sh.get("change_pct"))
                if close is not None and change_pct is not None:
                    block.append(f"  上证指数: {close} ({float(change_pct):+.2f}%)")
            sz = market.get("sz_index")
            if sz:
                close = _fmt_num(sz.get("close"))
                change_pct = _fmt_num(sz.get("change_pct"))
                if close is not None and change_pct is not None:
                    block.append(f"  深证成指: {close} ({float(change_pct):+.2f}%)")
            cyb = market.get("cyb_index")
            if cyb:
                close = _fmt_num(cyb.get("close"))
                change_pct = _fmt_num(cyb.get("change_pct"))
                if close is not None and change_pct is not None:
                    block.append(f"  创业板指: {close} ({float(change_pct):+.2f}%)")
            
            if market.get("total_stocks"):
                stats = ["", "市场统计:"]
                total_stocks = market.get("total_stocks")
                if total_stocks is not None:
                    stats.append(f"  总股票数: {total_stocks}")
                up_count = market.get("up_count")
                up_ratio = market.get("up_ratio")
                if up_count is not None and up_ratio is not None:
                    stats.append(f"  上涨: {up_count} ({up_ratio:.1f}%)")
                down_count = market.get("down_count")
                if down_count is not None:
                    stats.append(f"  下跌: {down_count}")
                flat_count = market.get("flat_count")
                if flat_count is not None:
                    stats.append(f"  平盘: {flat_count}")
                limit_up = market.get("limit_up")
                if limit_up is not None:
                    stats.append(f"  涨停: {limit_up}")
                limit_down = market.get("limit_down")
                if limit_down is not None:
                    stats.append(f"  跌停: {limit_down}")
                block.extend(stats)
            text_parts.append("\n".join(block))
        
        # 北向资金
        if data.get("north_flow"):
            north = data["north_flow"]
            block = ["【北向资金流向】"]
            date = north.get("date")
            if date:
                block.append(f"日期: {date}")
            north_net_inflow = _fmt_num(north.get("north_net_inflow"))
            if north_net_inflow is not None:
                block.append(f"北向资金净流入: {north_net_inflow} 万元")
            hgt_net_inflow = _fmt_num(north.get("hgt_net_inflow"))
            if hgt_net_inflow is not None:
                block.append(f"  沪股通: {hgt_net_inflow} 万元")
            sgt_net_inflow = _fmt_num(north.get("sgt_net_inflow"))
            if sgt_net_inflow is not None:
                block.append(f"  深股通: {sgt_net_inflow} 万元")
            text_parts.append("\n".join(block))

        macro_text = self.format_macro_data_for_ai(data.get("macro_data", {}))
        if macro_text:
            text_parts.append(macro_text)
        
        # 行业板块表现（前20）
        if data.get("sectors"):
            sectors = data["sectors"]
            sorted_sectors = sorted(sectors.items(), key=lambda x: x[1]["change_pct"], reverse=True)
            
            text_parts.append(f"""
【行业板块表现 TOP20】
涨幅榜前10:
""")
            for name, info in sorted_sectors[:10]:
                change_pct = _fmt_num(info.get("change_pct"))
                top_stock = info.get("top_stock")
                top_stock_change = _fmt_num(info.get("top_stock_change"))
                if change_pct is not None and top_stock_change is not None:
                    text_parts.append(f"  {name}: {float(change_pct):+.2f}% | 领涨: {top_stock} ({float(top_stock_change):+.2f}%)")
            
            text_parts.append(f"""
跌幅榜前10:
""")
            for name, info in sorted_sectors[-10:]:
                change_pct = _fmt_num(info.get("change_pct"))
                top_stock = info.get("top_stock")
                top_stock_change = _fmt_num(info.get("top_stock_change"))
                if change_pct is not None and top_stock_change is not None:
                    text_parts.append(f"  {name}: {float(change_pct):+.2f}% | 领跌: {top_stock} ({float(top_stock_change):+.2f}%)")
        
        # 概念板块表现（前20）
        if data.get("concepts"):
            concepts = data["concepts"]
            sorted_concepts = sorted(concepts.items(), key=lambda x: x[1]["change_pct"], reverse=True)
            
            text_parts.append(f"""
【概念板块表现 TOP20】
涨幅榜前10:
""")
            for name, info in sorted_concepts[:10]:
                change_pct = _fmt_num(info.get("change_pct"))
                top_stock = info.get("top_stock")
                top_stock_change = _fmt_num(info.get("top_stock_change"))
                if change_pct is not None and top_stock_change is not None:
                    text_parts.append(f"  {name}: {float(change_pct):+.2f}% | 领涨: {top_stock} ({float(top_stock_change):+.2f}%)")
        
        # 板块资金流向（前15）
        if data.get("sector_fund_flow") and data["sector_fund_flow"].get("today"):
            flow = data["sector_fund_flow"]["today"]
            
            text_parts.append(f"""
【行业资金流向 TOP15】
主力资金净流入前15:
""")
            sorted_flow = sorted(flow, key=lambda x: self._to_float(x.get("main_net_inflow")) or 0.0, reverse=True)
            for item in sorted_flow[:15]:
                main_net_inflow = _fmt_num(item.get("main_net_inflow"))
                main_net_inflow_pct = _fmt_num(item.get("main_net_inflow_pct"))
                change_pct = _fmt_num(item.get("change_pct"))
                if main_net_inflow is not None and main_net_inflow_pct is not None and change_pct is not None:
                    source_label = "概念" if item.get("source_type") == "concept" else "行业"
                    text_parts.append(f"  [{source_label}] {item.get('sector')}: {float(main_net_inflow):.2f}万 ({float(main_net_inflow_pct):+.2f}%) | 涨跌: {float(change_pct):+.2f}%")
        
        # 重要新闻（前20条）
        if data.get("news"):
            text_parts.append(f"""
【重要财经新闻 TOP20】
""")
            for idx, news in enumerate(data["news"][:20], 1):
                text_parts.append(f"{idx}. [{news['publish_time']}] {news['title']}")
                if news.get('content') and len(news['content']) > 100:
                    text_parts.append(f"   {news['content'][:100]}...")
        
        return "\n".join(text_parts)

    @staticmethod
    def format_macro_data_for_ai(macro_data):
        """格式化宏观快照，避免报告误判为未提供 GDP/PMI 等指标。"""
        if not isinstance(macro_data, dict) or not macro_data.get("macro_snapshot"):
            return ""

        snapshot = macro_data.get("macro_snapshot", {})
        ordered_keys = (
            "gdp_yoy",
            "gdp_qoq",
            "industrial_yoy",
            "cpi_yoy",
            "ppi_yoy",
            "manufacturing_pmi",
            "non_manufacturing_pmi",
            "composite_pmi",
            "m2_yoy",
            "retail_sales_yoy",
            "fixed_asset_yoy",
            "real_estate_invest_yoy",
            "urban_unemployment",
        )
        source_label = {
            "macro_analysis_cache": "宏观分析模块最近快照",
            "macro_analysis_tushare": "宏观分析模块Tushare实时映射",
        }.get(macro_data.get("source"), str(macro_data.get("source") or "宏观分析模块"))

        lines = ["【宏观指标快照】", f"数据来源: {source_label}"]
        if macro_data.get("timestamp"):
            lines.append(f"采集时间: {macro_data['timestamp']}")

        for key in ordered_keys:
            item = snapshot.get(key)
            if not isinstance(item, dict):
                continue
            label = item.get("label") or key
            value = item.get("value")
            unit = item.get("unit") or ""
            if value in (None, ""):
                continue
            period_hint = "最新公布期"
            change = item.get("change")
            change_text = f"，较上一期变动 {change:+.2f}{unit}" if isinstance(change, (int, float)) else ""
            lines.append(f"- {label}: {value}{unit}（{period_hint}{change_text}）")

        rule_view = macro_data.get("rule_based_sector_view") or {}
        if isinstance(rule_view, dict):
            market_view = rule_view.get("market_view")
            if market_view:
                lines.append(f"- 宏观规则视图: {market_view}")
            bullish = rule_view.get("bullish_sectors") or []
            if bullish:
                names = [str(item.get("sector")) for item in bullish[:5] if isinstance(item, dict) and item.get("sector")]
                if names:
                    lines.append(f"- 宏观相对受益板块: {'、'.join(names)}")
            bearish = rule_view.get("bearish_sectors") or []
            if bearish:
                names = [str(item.get("sector")) for item in bearish[:4] if isinstance(item, dict) and item.get("sector")]
                if names:
                    lines.append(f"- 宏观相对承压板块: {'、'.join(names)}")

        return "\n".join(lines)
    
    def _save_raw_data_to_db(self, data):
        """保存原始数据到数据库"""
        try:
            if not data.get("success"):
                self.logger.warning("[智策数据] 数据获取失败，跳过保存")
                return

            data_date = self._resolve_source_trade_date(data)
            
            # 保存板块数据
            if data.get("sectors"):
                # 将字典转换为DataFrame并映射必要列
                sectors_df = pd.DataFrame([
                    {
                        '板块代码': v.get('ts_code') or v.get('code') or v.get('name') or k,
                        '板块名称': v.get('name') or k,
                        '涨跌幅': v.get('change_pct'),
                        '成交额': None,
                        '总市值': v.get('market_cap') or v.get('total_market_cap'),
                        '市盈率': v.get('pe_ratio'),
                        '市净率': v.get('pb_ratio'),
                        '最新价': None,
                        '成交量': None,
                        'turnover': v.get('turnover')
                    }
                    for k, v in data["sectors"].items()
                ])
                self.database.save_sector_raw_data(
                    data_date=data_date,
                    data_type="industry",
                    data_df=sectors_df
                )
                self.logger.info(f"[智策数据] 保存行业板块数据: {len(data['sectors'])} 个板块")
            
            # 保存概念板块数据
            if data.get("concepts"):
                concepts_df = pd.DataFrame([
                    {
                        '板块代码': v.get('ts_code') or v.get('code') or v.get('name') or k,
                        '板块名称': v.get('name') or k,
                        '涨跌幅': v.get('change_pct'),
                        '成交额': None,
                        '总市值': v.get('market_cap') or v.get('total_market_cap'),
                        '市盈率': v.get('pe_ratio'),
                        '市净率': v.get('pb_ratio'),
                        '最新价': None,
                        '成交量': None,
                        'turnover': v.get('turnover')
                    }
                    for k, v in data["concepts"].items()
                ])
                self.database.save_sector_raw_data(
                    data_date=data_date,
                    data_type="concept",
                    data_df=concepts_df
                )
                self.logger.info(f"[智策数据] 保存概念板块数据: {len(data['concepts'])} 个概念")
            
            # 保存资金流向数据
            if data.get("sector_fund_flow"):
                flow_today = data["sector_fund_flow"].get("today", [])
                fund_df = pd.DataFrame([
                    {
                        '板块代码': item.get('ts_code') or item.get('sector'),
                        '行业': item.get('sector'),
                        '板块类型': item.get('source_type') or item.get('content_type') or 'industry',
                        '主力净流入-净额': item.get('main_net_inflow'),
                        '主力净流入-净占比': item.get('main_net_inflow_pct'),
                        '超大单净流入-净额': item.get('super_large_net_inflow'),
                        '超大单净流入-净占比': item.get('super_large_net_inflow_pct'),
                        '大单净流入-净额': item.get('large_net_inflow'),
                        '大单净流入-净占比': item.get('large_net_inflow_pct')
                    }
                    for item in flow_today
                ])
                if not fund_df.empty:
                    self.database.save_sector_raw_data(
                        data_date=data_date,
                        data_type="fund_flow",
                        data_df=fund_df
                    )
                self.logger.info("[智策数据] 保存资金流向数据")
            
            # 保存市场概况数据
            if data.get("market_overview"):
                market = data["market_overview"]
                sh_index = market.get("sh_index")
                sz_index = market.get("sz_index")
                cyb_index = market.get("cyb_index")
                mo_df = pd.DataFrame([
                    {'代码': '000001', '名称': '上证指数', '最新价': sh_index.get('close') if sh_index else None, '涨跌幅': sh_index.get('change_pct') if sh_index else None, '成交量': sh_index.get('volume') if sh_index else None, '成交额': sh_index.get('turnover') if sh_index else None, '总市值': None, '市盈率': None, '市净率': None},
                    {'代码': '399001', '名称': '深证成指', '最新价': sz_index.get('close') if sz_index else None, '涨跌幅': sz_index.get('change_pct') if sz_index else None, '成交量': sz_index.get('volume') if sz_index else None, '成交额': sz_index.get('turnover') if sz_index else None, '总市值': None, '市盈率': None, '市净率': None},
                    {'代码': '399006', '名称': '创业板指', '最新价': cyb_index.get('close') if cyb_index else None, '涨跌幅': cyb_index.get('change_pct') if cyb_index else None, '成交量': cyb_index.get('volume') if cyb_index else None, '成交额': cyb_index.get('turnover') if cyb_index else None, '总市值': None, '市盈率': None, '市净率': None},
                    {'代码': '__MARKET_BREADTH__', '名称': '__MARKET_BREADTH__', '最新价': market.get('total_stocks'), '涨跌幅': market.get('up_ratio'), '成交量': market.get('up_count'), '成交额': market.get('down_count'), '总市值': market.get('flat_count'), '市盈率': market.get('limit_up'), '市净率': market.get('limit_down')},
                ])
                self.database.save_sector_raw_data(
                    data_date=data_date,
                    data_type="market_overview",
                    data_df=mo_df
                )
                self.logger.info("[智策数据] 保存市场概况数据")
            
            if data.get("north_flow") and hasattr(self.database, "save_north_flow_snapshot"):
                self.database.save_north_flow_snapshot(
                    north_flow=data["north_flow"],
                    data_date=self._format_trade_date(data["north_flow"].get("source_trade_date") or data_date),
                )
                self.logger.info("[智策数据] 保存北向资金快照")
            
            # 保存新闻数据
            if data.get("news"):
                self.database.save_news_data(
                    news_list=data["news"],
                    news_date=datetime.now().strftime('%Y-%m-%d'),
                    source="rsshub_cache"
                )
                self.logger.info(f"[智策数据] 保存财经新闻: {len(data['news'])} 条")
                
        except Exception as e:
            self.logger.error(f"[智策数据] 保存原始数据失败: {e}")
    
    def get_cached_data_with_fallback(self):
        """获取缓存数据，支持回退机制"""
        try:
            # 首先尝试获取最新数据
            print("[智策] 尝试获取最新数据...")
            fresh_data = self.get_all_sector_data()
            
            if fresh_data.get("success"):
                return fresh_data
            
            # 如果获取失败，回退到缓存数据
            print("[智策] 获取最新数据失败，尝试加载缓存数据...")
            cached_data = self._load_cached_data()
            
            if cached_data:
                print("[智策] ✓ 成功加载缓存数据")
                cached_data["from_cache"] = True
                cached_data["cache_warning"] = "当前显示为缓存数据（24小时内），可能不是最新信息"
                return cached_data
            else:
                print("[智策] ✗ 无可用缓存数据")
                failed_data = self._new_data_payload()
                failed_data["error"] = "无法获取数据且无可用缓存"
                return failed_data
                
        except Exception as e:
            self.logger.error(f"[智策数据] 获取数据失败: {e}")
            failed_data = self._new_data_payload()
            failed_data["error"] = str(e)
            return failed_data
    
    def _load_cached_data(self):
        """加载缓存数据"""
        cached_data = self._new_data_payload(success=True)
        cache_specs = (
            ("sectors", "sectors", dict, "行业板块数据", "get_latest_raw_data"),
            ("concepts", "concepts", dict, "概念板块数据", "get_latest_raw_data"),
            ("sector_fund_flow", "fund_flow", dict, "行业资金流向", "get_latest_raw_data"),
            ("market_overview", "market_overview", dict, "市场概况", "get_latest_raw_data"),
            ("north_flow", "north_flow", dict, "北向资金", "get_latest_raw_data"),
            ("news", None, list, "财经新闻", "get_latest_news_data"),
        )

        for result_key, cache_key, content_type, log_label, fetcher_name in cache_specs:
            cached_data[result_key] = self._read_cached_content(
                cache_key=cache_key,
                log_label=log_label,
                default_factory=content_type,
                content_type=content_type,
                fetcher_name=fetcher_name,
                log_on_hit=False,
            )

        cached_data["macro_data"] = self._get_macro_data()
        cached_data["source_trade_date"] = self._resolve_source_trade_date(cached_data)

        has_data = any(cached_data[key] for key in self._CORE_DATA_KEYS + self._OPTIONAL_DATA_KEYS)
        return cached_data if has_data else None


# 测试函数
if __name__ == "__main__":
    print("=" * 60)
    print("测试智策板块数据采集模块")
    print("=" * 60)
    
    fetcher = SectorStrategyDataFetcher()
    data = fetcher.get_all_sector_data()
    
    if data.get("success"):
        print("\n" + "=" * 60)
        print("数据采集成功！")
        print("=" * 60)
        
        formatted_text = fetcher.format_data_for_ai(data)
        print(formatted_text[:3000])  # 显示前3000字符
        print(f"\n... (总长度: {len(formatted_text)} 字符)")
    else:
        print(f"\n数据采集失败: {data.get('error', '未知错误')}")
