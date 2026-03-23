"""
智策板块数据采集模块
使用AKShare获取板块相关数据
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import warnings
import time
import logging
import os
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
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 2  # 重试延迟（秒）
        self.request_delay = 1  # 请求间隔（秒）
        
        # 初始化数据库和日志
        self.database = SectorStrategyDatabase()
        self.logger = logging.getLogger(__name__)
        self._tushare_api = None
        self._tushare_url = None
        self._dc_index_cache = {}
        
        # 配置日志
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _safe_request(self, func, *args, **kwargs):
        """安全的请求函数，包含重试机制"""
        for attempt in range(self.max_retries):
            try:
                result = func(*args, **kwargs)
                # 添加请求延迟，避免请求过快
                time.sleep(self.request_delay)
                return result
            except Exception as e:
                if attempt < self.max_retries - 1:
                    print(f"    请求失败，{self.retry_delay}秒后重试... (尝试 {attempt + 1}/{self.max_retries})")
                    time.sleep(self.retry_delay)
                else:
                    print(f"    请求失败，已达最大重试次数: {e}")
                    raise e
    
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
            # 1. 获取行业板块数据
            print("  [1/6] 获取行业板块行情...")
            sectors_data = self._get_sector_performance()
            if sectors_data:
                data["sectors"] = sectors_data
                print(f"    ✓ 成功获取 {len(sectors_data)} 个行业板块数据")
            
            # 2. 获取概念板块数据
            print("  [2/6] 获取概念板块行情...")
            concept_data = self._get_concept_performance()
            if concept_data:
                data["concepts"] = concept_data
                print(f"    ✓ 成功获取 {len(concept_data)} 个概念板块数据")
            
            # 3. 获取板块资金流向
            print("  [3/6] 获取行业资金流向...")
            fund_flow_data = self._get_sector_fund_flow()
            if fund_flow_data:
                data["sector_fund_flow"] = fund_flow_data
                print(f"    ✓ 成功获取资金流向数据")
            
            # 4. 获取市场总体情况
            print("  [4/6] 获取市场总体情况...")
            market_data = self._get_market_overview()
            if market_data:
                data["market_overview"] = market_data
                print(f"    ✓ 成功获取市场概况")
            
            # 5. 获取北向资金流向
            print("  [5/6] 获取北向资金流向...")
            north_flow = self._get_north_money_flow()
            if north_flow:
                data["north_flow"] = north_flow
                print(f"    ✓ 成功获取北向资金数据")
            
            # 6. 获取财经新闻
            print("  [6/6] 获取财经新闻...")
            news_data = self._get_financial_news()
            if news_data:
                data["news"] = news_data
                print(f"    ✓ 成功获取 {len(news_data)} 条新闻")

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

    def _fetch_tushare_trade_data(self, api_name, *, max_days=7, **kwargs):
        """按最近交易日回退查询 Tushare 数据。"""
        pro = self._ensure_tushare_api()
        if not pro:
            return pd.DataFrame()

        last_error = None
        for offset in range(max_days):
            trade_date = (datetime.now() - timedelta(days=offset)).strftime('%Y%m%d')
            params = dict(kwargs)
            params.setdefault('trade_date', trade_date)
            try:
                df = getattr(pro, api_name)(**params)
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
        try:
            # 获取行业板块实时行情（使用重试机制）
            df = self._safe_request(ak.stock_board_industry_name_em)
            
            if df is None or df.empty:
                return {}
            
            # 转换为字典格式
            sectors = {}
            for idx, row in df.iterrows():
                sector_name = row['板块名称']
                if sector_name:
                    sectors[sector_name] = {
                        "name": sector_name,
                        "change_pct": row['涨跌幅'],
                        "turnover": row['换手率'],
                        "total_market_cap": row['总市值'],
                        "top_stock": row['领涨股票'],
                        "top_stock_change": row['领涨股票涨跌幅'],
                        "up_count": row['上涨家数'],
                        "down_count": row['下跌家数']
                    }
            
            return sectors
            
        except Exception as e:
            print(f"    获取行业板块数据失败: {e}")
            tushare_df = self._get_tushare_board_snapshot("行业板块")
            if tushare_df is not None and not tushare_df.empty:
                print(f"    [Tushare] 行业板块数据获取成功，共 {len(tushare_df)} 条")
                return self._convert_tushare_board_snapshot(tushare_df)
            return {}
    
    def _get_concept_performance(self):
        """获取概念板块表现"""
        try:
            # 获取概念板块实时行情（使用重试机制）
            df = self._safe_request(ak.stock_board_concept_name_em)
            
            if df is None or df.empty:
                return {}
            
            # 转换为字典格式
            concepts = {}
            for idx, row in df.iterrows():
                concept_name = row['板块名称']
                if concept_name:
                    concepts[concept_name] = {
                        "name": concept_name,
                        "change_pct": row['涨跌幅'],
                        "turnover": row['换手率'],
                        "total_market_cap": row['总市值'],
                        "top_stock": row['领涨股票'],
                        "top_stock_change": row['领涨股票涨跌幅'],
                        "up_count": row['上涨家数'],
                        "down_count": row['下跌家数']
                    }
            
            return concepts
            
        except Exception as e:
            print(f"    获取概念板块数据失败: {e}")
            tushare_df = self._get_tushare_board_snapshot("概念板块")
            if tushare_df is not None and not tushare_df.empty:
                print(f"    [Tushare] 概念板块数据获取成功，共 {len(tushare_df)} 条")
                return self._convert_tushare_board_snapshot(tushare_df)
            return {}
    
    def _get_sector_fund_flow(self):
        """获取行业资金流向"""
        try:
            # 获取行业资金流向（使用重试机制）
            df = self._safe_request(ak.stock_sector_fund_flow_rank, indicator="今日")
            
            if df is None or df.empty:
                return {}
            
            # 转换为字典格式
            fund_flow = {
                "today": [],
                "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            for idx, row in df.head(50).iterrows():  # 取前50个
                fund_flow["today"].append({
                    "sector": row['名称'],
                    "main_net_inflow": row['今日主力净流入-净额'],
                    "main_net_inflow_pct": row['今日主力净流入-净占比'],
                    "super_large_net_inflow": row['今日超大单净流入-净额'],
                    "large_net_inflow": row['今日大单净流入-净额'],
                    "medium_net_inflow": row['今日中单净流入-净额'],
                    "small_net_inflow": row['今日小单净流入-净额'],
                    "change_pct": row['今日涨跌幅']
                })
            
            return fund_flow
            
        except Exception as e:
            print(f"    获取行业资金流向失败: {e}")
            tushare_df = self._fetch_tushare_trade_data(
                'moneyflow_ind_dc',
                content_type="行业",
            )
            if tushare_df is None or tushare_df.empty:
                return {}

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
            # 获取A股市场统计
            overview = {}
            
            # 涨跌家数
            try:
                df_stat = self._safe_request(ak.stock_zh_a_spot_em)
                if df_stat is not None and not df_stat.empty:
                    total_count = len(df_stat)
                    up_count = len(df_stat[df_stat['涨跌幅'] > 0])
                    down_count = len(df_stat[df_stat['涨跌幅'] < 0])
                    flat_count = total_count - up_count - down_count
                    
                    overview["total_stocks"] = total_count
                    overview["up_count"] = up_count
                    overview["down_count"] = down_count
                    overview["flat_count"] = flat_count
                    if total_count > 0:
                        overview["up_ratio"] = round(up_count / total_count * 100, 2)
                    
                    # 涨停跌停
                    limit_up = len(df_stat[df_stat['涨跌幅'] >= 9.5])
                    limit_down = len(df_stat[df_stat['涨跌幅'] <= -9.5])
                    overview["limit_up"] = limit_up
                    overview["limit_down"] = limit_down
            except:
                pass
            
            # 大盘指数
            try:
                # 上证指数
                df_sh = ak.stock_zh_index_spot_em(symbol="上证指数")
                if df_sh is not None and not df_sh.empty:
                    row = df_sh.iloc[0]
                    overview["sh_index"] = {
                        "code": "000001",
                        "name": "上证指数",
                        "close": row['最新价'],
                        "change_pct": row['涨跌幅'],
                        "change": row['涨跌额']
                    }
                
                # 深证成指
                df_sz = self._safe_request(ak.stock_zh_index_spot_em, symbol="深证成指")
                if df_sz is not None and not df_sz.empty:
                    row = df_sz.iloc[0]
                    overview["sz_index"] = {
                        "code": "399001",
                        "name": "深证成指",
                        "close": row['最新价'],
                        "change_pct": row['涨跌幅'],
                        "change": row['涨跌额']
                    }
                
                # 创业板指
                df_cyb = self._safe_request(ak.stock_zh_index_spot_em, symbol="创业板指")
                if df_cyb is not None and not df_cyb.empty:
                    row = df_cyb.iloc[0]
                    overview["cyb_index"] = {
                        "code": "399006",
                        "name": "创业板指",
                        "close": row['最新价'],
                        "change_pct": row['涨跌幅'],
                        "change": row['涨跌额']
                    }
            except:
                pass
            
            return overview
            
        except Exception as e:
            print(f"    获取市场概况失败: {e}")
            return {}
    
    def _get_north_money_flow(self):
        """获取北向资金流向（优先使用Tushare，失败时使用Akshare）"""
        # 优先使用Tushare获取沪深港通资金流向
        tushare_token = os.getenv('TUSHARE_TOKEN', '')
        try:
            # 初始化Tushare（如果尚未初始化）
            if self._tushare_api is None:
                if tushare_token:
                    try:
                        self._tushare_api, self._tushare_url = create_tushare_pro(
                            token=tushare_token,
                        )
                        if self._tushare_api:
                            print(f"    [Tushare] 初始化成功，地址: {self._tushare_url}")
                    except Exception as e:
                        print(f"    [Tushare] 初始化失败: {e}")
                        self._tushare_api = None
                else:
                    print("    [Tushare] 未配置Token")
            
            
            # 如果Tushare可用，获取数据
            if self._tushare_api:
                print("    [Tushare] 正在获取沪深港通资金流向...")
                
                # 获取最近30天的数据
                end_date = datetime.now()
                start_date = end_date - timedelta(days=20)
                
                df = self._tushare_api.moneyflow_hsgt(
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
        
        # Tushare失败，尝试使用Akshare
        try:
            print("    [Akshare] 正在获取沪深港通资金流向（备用数据源）...")
            df = self._safe_request(ak.stock_hsgt_fund_flow_summary_em)
            
            if df is not None and not df.empty:
                print("    [Akshare] 成功获取数据")
                
                # 获取最新数据
                latest = df.iloc[0]
                
                north_flow = {
                    "date": str(latest['日期']),
                    "north_net_inflow": latest['北向资金-成交净买额'],
                    "hgt_net_inflow": latest['沪股通-成交净买额'],
                    "sgt_net_inflow": latest['深股通-成交净买额'],
                    "north_total_amount": latest['北向资金-成交金额']
                }
                
                # 获取历史趋势（最近20天）
                history = []
                for idx, row in df.head(20).iterrows():
                    history.append({
                        "date": str(row['日期']),
                        "net_inflow": row['北向资金-成交净买额']
                    })
                north_flow["history"] = history
                
                return north_flow
            else:
                print("    [Akshare] 未获取到数据")
        except Exception as e:
            print(f"    [Akshare] 获取北向资金失败: {e}")
        
        # 所有数据源都失败
        print("    [ERROR] 所有数据源均获取失败")
        return {}
    
    def _get_financial_news(self):
        """获取财经新闻"""
        try:
            # 获取东方财富财经新闻（使用重试机制）
            df = self._safe_request(ak.stock_news_em, symbol="全球")
            
            if df is None or df.empty:
                return []
            
            news_list = []
            for idx, row in df.head(150).iterrows():  # 取前150条
                news_list.append({
                    "title": row['新闻标题'],
                    "content": row['新闻内容'],
                    "publish_time": str(row['发布时间']),
                    "source": row['文章来源'],
                    "url": row['新闻链接']
                })
            
            return news_list
            
        except Exception as e:
            print(f"    获取财经新闻失败: {e}")
            pro = self._ensure_tushare_api()
            if not pro:
                return []

            start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
            end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sources = ["新浪财经", "财联社", "同花顺", "第一财经"]
            merged = []
            seen = set()

            for src in sources:
                try:
                    df = pro.major_news(
                        src=src,
                        start_date=start_date,
                        end_date=end_date,
                        fields='title,content,pub_time,src',
                    )
                except Exception as src_error:
                    self.logger.warning(f"[Tushare] major_news({src}) 获取失败: {src_error}")
                    continue

                if df is None or df.empty:
                    continue

                for _, row in df.iterrows():
                    title = self._clean_value(row['title'])
                    pub_time = self._clean_value(row['pub_time'])
                    key = (title, pub_time)
                    if not title or key in seen:
                        continue
                    seen.add(key)
                    merged.append({
                        "title": title,
                        "content": self._clean_value(row['content']),
                        "publish_time": str(pub_time),
                        "source": self._clean_value(row['src']) or src,
                        "url": "",
                    })
                    if len(merged) >= 150:
                        print(f"    [Tushare] 财经新闻获取成功，共 {len(merged)} 条")
                        return merged

            if merged:
                print(f"    [Tushare] 财经新闻获取成功，共 {len(merged)} 条")
            return merged
    
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
                    source="akshare"
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
