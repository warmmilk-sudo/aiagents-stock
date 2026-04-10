"""
智策板块数据采集模块
优先使用 Tushare/RSSHub 等稳定数据源。
"""

import concurrent.futures
import pandas as pd
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
    _OPTIONAL_DATA_KEYS = ("market_overview", "north_flow", "news")
    
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
        return {
            "success": success,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "sectors": {},
            "concepts": {},
            "sector_fund_flow": {},
            "market_overview": {},
            "north_flow": {},
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
                ("sectors", "[1/6] 获取行业板块行情...", self._get_sector_performance, True),
                ("concepts", "[2/6] 获取概念板块行情...", self._get_concept_performance, True),
                ("sector_fund_flow", "[3/6] 获取行业资金流向...", self._get_sector_fund_flow, True),
                ("market_overview", "[4/6] 获取市场总体情况...", self._get_market_overview, False),
                ("north_flow", "[5/6] 获取北向资金流向...", self._get_north_money_flow, False),
                ("news", "[6/6] 获取财经新闻...", self._get_financial_news, False),
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
                    elif result_key == "news":
                        print(f"    ✓ 成功获取 {len(payload)} 条新闻")

            missing_core = [key for key in self._CORE_DATA_KEYS if not data.get(key)]
            if missing_core:
                missing_parts = []
                for key in missing_core:
                    error = fetch_errors.get(key)
                    missing_parts.append(f"{key}({error})" if error else key)
                raise RuntimeError(f"核心板块数据缺失: {', '.join(missing_parts)}")

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

            boards[board_name] = {
                "name": board_name,
                "change_pct": self._clean_value(row['pct_change']),
                "turnover": self._clean_value(row['turnover_rate']),
                "total_market_cap": self._clean_value(row['total_mv']),
                "top_stock": self._clean_value(row['leading']),
                "top_stock_change": self._clean_value(row['leading_pct']),
                "up_count": self._clean_value(row['up_num']),
                "down_count": self._clean_value(row['down_num']),
                "ts_code": self._clean_value(row['ts_code']),
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
    
    def _get_sector_fund_flow(self):
        """获取行业资金流向"""
        tushare_df = self._fetch_tushare_trade_data(
            'moneyflow_ind_dc',
            content_type="行业",
        )
        if tushare_df is None or tushare_df.empty:
            return self._read_cached_content(
                cache_key="fund_flow",
                log_label="行业资金流向",
                default_factory=dict,
                content_type=dict,
                log_on_hit=True,
            )

        sector_snapshot = self._get_tushare_board_snapshot("行业板块")
        pct_map = {}
        if sector_snapshot is not None and not sector_snapshot.empty:
            pct_map = {
                self._clean_value(row['name']): self._clean_value(row['pct_change'])
                for _, row in sector_snapshot.iterrows()
            }

        fund_flow = {
            "today": [],
            "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        for _, row in tushare_df.head(50).iterrows():
            sector_name = self._clean_value(row['name'])
            if sector_name in pct_map:
                change_pct = pct_map[sector_name]
            elif 'pct_change' in row.index:
                change_pct = self._clean_value(row['pct_change'])
            else:
                change_pct = None
            fund_flow["today"].append({
                "sector": sector_name,
                "main_net_inflow": self._clean_value(row['net_amount']),
                "main_net_inflow_pct": self._clean_value(row['net_amount_rate']),
                "super_large_net_inflow": self._clean_value(row['buy_elg_amount']),
                "super_large_net_inflow_pct": self._clean_value(row['buy_elg_amount_rate']),
                "large_net_inflow": self._clean_value(row['buy_lg_amount']),
                "large_net_inflow_pct": self._clean_value(row['buy_lg_amount_rate']),
                "medium_net_inflow": self._clean_value(row['buy_md_amount']),
                "medium_net_inflow_pct": self._clean_value(row['buy_md_amount_rate']),
                "small_net_inflow": self._clean_value(row['buy_sm_amount']),
                "small_net_inflow_pct": self._clean_value(row['buy_sm_amount_rate']),
                "change_pct": change_pct,
            })

        print(f"    [Tushare] 行业资金流向获取成功，共 {len(fund_flow['today'])} 条")
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
        north_flow = {
            "date": str(latest['trade_date']),
            "north_net_inflow": float(latest['north_money']),
            "hgt_net_inflow": float(latest['hgt']),
            "sgt_net_inflow": float(latest['sgt']),
            "north_total_amount": float(latest['north_money']),
            "history": [
                {
                    "date": str(row['trade_date']),
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
            sorted_flow = sorted(flow, key=lambda x: x["main_net_inflow"], reverse=True)
            for item in sorted_flow[:15]:
                main_net_inflow = _fmt_num(item.get("main_net_inflow"))
                main_net_inflow_pct = _fmt_num(item.get("main_net_inflow_pct"))
                change_pct = _fmt_num(item.get("change_pct"))
                if main_net_inflow is not None and main_net_inflow_pct is not None and change_pct is not None:
                    text_parts.append(f"  {item.get('sector')}: {float(main_net_inflow):.2f}万 ({float(main_net_inflow_pct):+.2f}%) | 涨跌: {float(change_pct):+.2f}%")
        
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
    
    def _save_raw_data_to_db(self, data):
        """保存原始数据到数据库"""
        try:
            if not data.get("success"):
                self.logger.warning("[智策数据] 数据获取失败，跳过保存")
                return
            
            # 保存板块数据
            if data.get("sectors"):
                # 将字典转换为DataFrame并映射必要列
                sectors_df = pd.DataFrame([
                    {
                        '板块代码': v.get('ts_code') or v.get('code') or v.get('name') or k,
                        '板块名称': v.get('name') or k,
                        '涨跌幅': v.get('change_pct'),
                        '成交额': None,
                        '总市值': v.get('total_market_cap'),
                        '市盈率': v.get('pe_ratio'),
                        '市净率': v.get('pb_ratio'),
                        '最新价': None,
                        '成交量': None,
                        'turnover': v.get('turnover')
                    }
                    for k, v in data["sectors"].items()
                ])
                self.database.save_sector_raw_data(
                    data_date=datetime.now().strftime('%Y-%m-%d'),
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
                        '总市值': v.get('total_market_cap'),
                        '市盈率': v.get('pe_ratio'),
                        '市净率': v.get('pb_ratio'),
                        '最新价': None,
                        '成交量': None,
                        'turnover': v.get('turnover')
                    }
                    for k, v in data["concepts"].items()
                ])
                self.database.save_sector_raw_data(
                    data_date=datetime.now().strftime('%Y-%m-%d'),
                    data_type="concept",
                    data_df=concepts_df
                )
                self.logger.info(f"[智策数据] 保存概念板块数据: {len(data['concepts'])} 个概念")
            
            # 保存资金流向数据
            if data.get("sector_fund_flow"):
                flow_today = data["sector_fund_flow"].get("today", [])
                fund_df = pd.DataFrame([
                    {
                        '行业': item.get('sector'),
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
                        data_date=datetime.now().strftime('%Y-%m-%d'),
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
                    data_date=datetime.now().strftime('%Y-%m-%d'),
                    data_type="market_overview",
                    data_df=mo_df
                )
                self.logger.info("[智策数据] 保存市场概况数据")
            
            # 保存北向资金数据
            # 注：north_flow结构与原始表不一致，此处暂不保存以避免歧义
            
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
