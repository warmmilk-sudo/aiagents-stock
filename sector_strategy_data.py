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

    def _peek_cached_market_overview(self):
        database = getattr(self, "database", None)
        if database is None:
            return {}
        try:
            cached_payload = database.get_latest_raw_data("market_overview")
        except Exception:
            return {}

        if isinstance(cached_payload, dict):
            cached_content = cached_payload.get("data_content")
            if isinstance(cached_content, dict):
                return cached_content
        return {}

    def _get_market_breadth_rows(self):
        rows = self._get_market_breadth_rows_from_tushare()
        if rows:
            return rows
        return []

    def _get_market_breadth_rows_from_sina(self):
        return []

    def _get_market_breadth_rows_from_tushare(self):
        df = self._fetch_tushare_trade_data('daily', fields='ts_code,trade_date,pct_chg')
        rows = self._iter_frame_rows(df)
        if rows:
            self.logger.warning("[智策数据] 市场涨跌家数已切换到Tushare备用数据源")
        return rows

    def _extract_index_snapshot(self, rows):
        index_map = {
            "上证指数": ("sh_index", "000001"),
            "深证成指": ("sz_index", "399001"),
            "创业板指": ("cyb_index", "399006"),
        }
        overview = {}

        for row in rows:
            name = self._clean_value(row.get('名称'))
            if name not in index_map:
                continue
            target_key, code = index_map[name]
            overview[target_key] = {
                "code": code,
                "name": name,
                "close": self._clean_value(row.get('最新价')),
                "change_pct": self._clean_value(row.get('涨跌幅')),
                "change": self._clean_value(row.get('涨跌额')),
            }

        return overview

    def _get_market_index_overview(self):
        tushare_overview = self._get_market_index_overview_from_tushare()
        if tushare_overview:
            return tushare_overview
        return self._get_cached_market_overview() or {}

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

    def _get_cached_market_overview(self):
        try:
            cached_content = self._peek_cached_market_overview()
        except Exception as exc:
            self.logger.warning("[智策数据] 读取市场概况缓存失败: %s", exc)
            return {}

        if cached_content:
            self.logger.warning("[智策数据] 市场概况实时抓取失败，已回退到最近缓存快照")
        return cached_content

    def _get_cached_data_content(self, key, *, log_label):
        database = getattr(self, "database", None)
        if database is None:
            return {}
        try:
            cached_payload = database.get_latest_raw_data(key)
        except Exception as exc:
            self.logger.warning("[智策数据] 读取%s缓存失败: %s", log_label, exc)
            return {}

        if isinstance(cached_payload, dict):
            cached_content = cached_payload.get("data_content")
            if cached_content:
                self.logger.warning("[智策数据] %s已回退到最近缓存快照", log_label)
                return cached_content
        return {}

    def _get_cached_news_list(self):
        database = getattr(self, "database", None)
        if database is None:
            return []
        try:
            cached_payload = database.get_latest_news_data()
        except Exception as exc:
            self.logger.warning("[智策数据] 读取财经新闻缓存失败: %s", exc)
            return []

        if isinstance(cached_payload, dict):
            cached_content = cached_payload.get("data_content")
            if isinstance(cached_content, list) and cached_content:
                self.logger.warning("[智策数据] 财经新闻已回退到最近缓存快照")
                return cached_content
        return []
    
    def get_all_sector_data(self):
        """
        获取所有板块的综合数据
        
        Returns:
            dict: 包含多个维度的板块数据
        """
        print("[智策] 开始获取板块综合数据...")
        
        data = {
            "success": False,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "sectors": {},
            "concepts": {},
            "sector_fund_flow": {},
            "market_overview": {},
            "north_flow": {},
            "news": []
        }
        
        try:
            fetch_specs = {
                "sectors": ("[1/6] 获取行业板块行情...", self._get_sector_performance),
                "concepts": ("[2/6] 获取概念板块行情...", self._get_concept_performance),
                "sector_fund_flow": ("[3/6] 获取行业资金流向...", self._get_sector_fund_flow),
                "market_overview": ("[4/6] 获取市场总体情况...", self._get_market_overview),
                "north_flow": ("[5/6] 获取北向资金流向...", self._get_north_money_flow),
                "news": ("[6/6] 获取财经新闻...", self._get_financial_news),
            }

            for _, (step_label, _) in fetch_specs.items():
                print(f"  {step_label}")

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(len(fetch_specs), max(1, int(getattr(self, "max_fetch_workers", 3) or 3)))
            ) as executor:
                future_map = {
                    executor.submit(self._fetch_data_source, step_label, fetch_func): result_key
                    for result_key, (step_label, fetch_func) in fetch_specs.items()
                }

                non_critical_keys = {"market_overview", "north_flow", "news"}
                for future in concurrent.futures.as_completed(future_map):
                    result_key = future_map[future]
                    step_label, payload, error = future.result()
                    if error is not None:
                        print(f"    ✗ {step_label} 失败: {error}")
                        if result_key in non_critical_keys:
                            self.logger.warning("[智策数据] 非核心步骤失败，继续执行: %s -> %s", result_key, error)
                            continue
                        raise error

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

            missing_core = [
                key for key in ("sectors", "concepts", "sector_fund_flow")
                if not data.get(key)
            ]
            if missing_core:
                raise RuntimeError(f"核心板块数据缺失: {', '.join(missing_core)}")

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
        return self._get_cached_data_content("sectors", log_label="行业板块数据")
    
    def _get_concept_performance(self):
        """获取概念板块表现"""
        tushare_df = self._get_tushare_board_snapshot("概念板块")
        if tushare_df is not None and not tushare_df.empty:
            print(f"    [Tushare] 概念板块数据获取成功，共 {len(tushare_df)} 条")
            return self._convert_tushare_board_snapshot(tushare_df)
        return self._get_cached_data_content("concepts", log_label="概念板块数据")
    
    def _get_sector_fund_flow(self):
        """获取行业资金流向"""
        tushare_df = self._fetch_tushare_trade_data(
            'moneyflow_ind_dc',
            content_type="行业",
        )
        if tushare_df is None or tushare_df.empty:
            return self._get_cached_data_content("fund_flow", log_label="行业资金流向")

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
        try:
            overview = {}
            breadth_overview = self._build_market_breadth_overview(self._get_market_breadth_rows())
            overview.update(breadth_overview)
            overview.update(self._get_market_index_overview())
            cached_overview = self._get_cached_market_overview()
            if cached_overview:
                for key, value in cached_overview.items():
                    overview.setdefault(key, value)
            if not breadth_overview:
                overview.update(self._build_market_breadth_overview(self._get_market_breadth_rows_from_tushare()))
            if overview:
                return overview
            return cached_overview
        except Exception as e:
            print(f"    获取市场概况失败: {e}")
            cached_overview = self._get_cached_market_overview()
            if cached_overview:
                return cached_overview
            return {}
    
    def _get_north_money_flow(self):
        """获取北向资金流向。"""
        try:
            if self._ensure_tushare_api():
                print("    [Tushare] 正在获取沪深港通资金流向...")
                
                # 获取最近30天的数据
                end_date = datetime.now()
                start_date = end_date - timedelta(days=20)
                
                df = self._call_tushare_api(
                    'moneyflow_hsgt',
                    start_date=start_date.strftime('%Y%m%d'),
                    end_date=end_date.strftime('%Y%m%d')
                )
                
                if df is not None and not df.empty:
                    print("    [Tushare] 成功获取数据")
                    
                    # 按日期降序排列，获取最新数据
                    df = df.sort_values('trade_date', ascending=False)
                    latest = df.iloc[0]
                    
                    # 转换数据格式以匹配原有结构
                    north_flow = {
                        "date": str(latest['trade_date']),
                        "north_net_inflow": float(latest['north_money']),
                        "hgt_net_inflow": float(latest['hgt']),
                        "sgt_net_inflow": float(latest['sgt']),
                        "north_total_amount": float(latest['north_money'])  # Tushare没有总成交金额，使用净流入作为近似值
                    }
                    
                    # 获取历史趋势（最近20天）
                    history = []
                    for idx, row in df.head(20).iterrows():
                        history.append({
                            "date": str(row['trade_date']),
                            "net_inflow": float(row['north_money'])
                        })
                    north_flow["history"] = history
                    
                    return north_flow
                else:
                    print("    [Tushare] 未获取到数据")
            else:
                print("    [Tushare] 不可用")
        except Exception as e:
            print(f"    [Tushare] 获取北向资金失败: {e}")
        
        print("    [ERROR] 所有数据源均获取失败")
        return {}
    
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

        self.logger.warning("[智策数据] RSSHub财经新闻不可用，回退到本地缓存新闻")
        return self._get_cached_news_list()
    
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
                return {
                    "success": False,
                    "error": "无法获取数据且无可用缓存",
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
        except Exception as e:
            self.logger.error(f"[智策数据] 获取数据失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    def _load_cached_data(self):
        """加载缓存数据"""
        try:
            # 获取最近的各类数据
            cached_data = {
                "success": True,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "sectors": {},
                "concepts": {},
                "sector_fund_flow": {},
                "market_overview": {},
                "north_flow": {},
                "news": []
            }
            
            # 加载板块数据
            sectors_data = self.database.get_latest_raw_data("sectors")
            if sectors_data:
                cached_data["sectors"] = sectors_data.get("data_content", {})
            
            # 加载概念数据
            concepts_data = self.database.get_latest_raw_data("concepts")
            if concepts_data:
                cached_data["concepts"] = concepts_data.get("data_content", {})
            
            # 加载资金流向数据
            fund_flow_data = self.database.get_latest_raw_data("fund_flow")
            if fund_flow_data:
                cached_data["sector_fund_flow"] = fund_flow_data.get("data_content", {})
            
            # 加载市场概况数据
            market_data = self.database.get_latest_raw_data("market_overview")
            if market_data:
                cached_data["market_overview"] = market_data.get("data_content", {})
            
            # 加载北向资金数据
            north_data = self.database.get_latest_raw_data("north_flow")
            if north_data:
                cached_data["north_flow"] = north_data.get("data_content", {})
            
            # 加载新闻数据
            news_data = self.database.get_latest_news_data()
            if news_data:
                # 仅传递内容列表给下游分析，避免结构不一致
                cached_data["news"] = news_data.get("data_content", [])
            
            # 检查是否有有效数据
            has_data = any([
                cached_data["sectors"],
                cached_data["concepts"],
                cached_data["sector_fund_flow"],
                cached_data["market_overview"],
                cached_data["north_flow"],
                cached_data["news"]
            ])
            
            return cached_data if has_data else None
            
        except Exception as e:
            self.logger.error(f"[智策数据] 加载缓存数据失败: {e}")
            return None


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
