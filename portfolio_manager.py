"""
持仓管理器模块

提供持仓股票管理和批量分析功能
"""

import time
import re
import math
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import config

# 导入必要的模块
from portfolio_db import portfolio_db
from monitor_db import monitor_db as realtime_monitor_db
from investment_lifecycle_service import InvestmentLifecycleService, investment_lifecycle_service
from smart_monitor_db import SmartMonitorDB


class PortfolioManager:
    """持仓管理器类"""

    DEFAULT_RISK_FREE_RATE = 0.015
    AGGREGATE_ACCOUNT_NAME = "全部账户"
    DEFAULT_ANALYSIS_AGENTS = ["technical", "fundamental", "fund_flow", "risk"]
    
    def __init__(self, model=None, lightweight_model=None, reasoning_model=None,
                 portfolio_store=None,
                 realtime_monitor_store=None, smart_monitor_store=None, lifecycle_service=None):
        """
        初始化持仓管理器
        
        Args:
            model: 强制所有任务统一使用同一个模型
        """
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.db = portfolio_store or portfolio_db
        self.realtime_monitor_db = realtime_monitor_store or realtime_monitor_db
        self.smart_monitor_db = smart_monitor_store or SmartMonitorDB()
        self.lifecycle_service = lifecycle_service or InvestmentLifecycleService(
            portfolio_store=self.db,
            realtime_monitor_store=self.realtime_monitor_db,
            analysis_store=getattr(self.db, "analysis_repository", None),
            monitoring_store=getattr(self.smart_monitor_db, "monitoring_repository", None),
        )
        self._integrations_reconcile_pending = True
        self._stock_data_fetcher = None
        self._basic_stock_info_cache: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def get_default_smart_monitor_check_interval() -> int:
        return max(60, int(getattr(config, "SMART_MONITOR_AI_INTERVAL_MINUTES", 60) or 60) * 60)

    @staticmethod
    def get_default_realtime_monitor_check_interval() -> int:
        return max(3, int(getattr(config, "SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES", 3) or 3))

    def _mark_integrations_reconcile_pending(self) -> None:
        self._integrations_reconcile_pending = True

    def _normalize_stock_code(self, code: str) -> str:
        """标准化股票代码，兼容 .SH/.SZ/.HK 等输入格式。"""
        normalized = (code or "").strip().upper()
        if not normalized:
            return ""

        normalized = normalized.replace(" ", "")

        if "." in normalized:
            base, suffix = normalized.rsplit(".", 1)
            if suffix in {"SH", "SZ", "BJ"} and base.isdigit() and len(base) == 6:
                return base
            if suffix == "HK" and base.isdigit():
                return base.zfill(5)
            if suffix in {"US", "NYSE", "NASDAQ", "AMEX"}:
                return base

        if normalized.startswith("HK") and normalized[2:].isdigit():
            return normalized[2:].zfill(5)

        if normalized.startswith("US:"):
            return normalized[3:]

        if normalized.isdigit() and 1 <= len(normalized) <= 5:
            return normalized.zfill(5)

        return normalized

    def _is_valid_stock_name(self, name: str, code: str) -> bool:
        """过滤数据源回退值，避免将占位名称写入持仓。"""
        if not name:
            return False

        invalid_names = {
            "",
            "N/A",
            "未知",
            f"股票{code}",
            f"港股{code}",
            f"美股{code}",
        }
        return name not in invalid_names and name.upper() != code.upper()

    def _is_a_share(self, code: str) -> bool:
        """判断是否为 A 股代码。"""
        return code.isdigit() and len(code) == 6

    def _is_hk_stock(self, code: str) -> bool:
        """判断是否为港股代码。"""
        return code.isdigit() and 1 <= len(code) <= 5

    def _resolve_stock_name(self, code: str) -> Optional[str]:
        """根据股票代码自动识别股票名称。"""
        normalized_code = self._normalize_stock_code(code)
        if not normalized_code:
            return None

        try:
            if self._is_a_share(normalized_code):
                from data_source_manager import data_source_manager

                stock_info = data_source_manager.get_stock_basic_info(normalized_code)
                name = str(stock_info.get("name") or "").strip()
                if self._is_valid_stock_name(name, normalized_code):
                    return name
            elif self._is_hk_stock(normalized_code):
                try:
                    import akshare as ak

                    realtime_df = ak.stock_hk_spot_em()
                    if realtime_df is not None and not realtime_df.empty:
                        matched = realtime_df[realtime_df["代码"] == normalized_code]
                        if not matched.empty:
                            name = str(matched.iloc[0].get("名称") or "").strip()
                            if self._is_valid_stock_name(name, normalized_code):
                                return name
                except Exception as e:
                    print(f"[WARN] 港股名称识别 Akshare 失败 ({normalized_code}): {e}")

                import yfinance as yf

                yahoo_symbol = f"{int(normalized_code):04d}.HK"
                ticker = yf.Ticker(yahoo_symbol)
                ticker_info = ticker.info or {}
                name = str(ticker_info.get("longName") or ticker_info.get("shortName") or "").strip()
                if self._is_valid_stock_name(name, normalized_code):
                    return name
            else:
                import yfinance as yf

                ticker = yf.Ticker(normalized_code)
                ticker_info = ticker.info or {}
                name = str(ticker_info.get("longName") or ticker_info.get("shortName") or "").strip()
                if self._is_valid_stock_name(name, normalized_code):
                    return name
        except Exception as e:
            print(f"[WARN] 自动识别股票名称失败 ({normalized_code}): {e}")

        return None
    
    # ==================== 持仓股票管理 ====================
    
    def add_stock(self, code: str, name: Optional[str], cost_price: Optional[float] = None,
                  quantity: Optional[int] = None, note: str = "",
                  auto_monitor: bool = True, account_name: str = "默认账户",
                  origin_analysis_id: Optional[int] = None) -> Tuple[bool, str, Optional[int]]:
        """
        添加持仓股票
        
        Args:
            code: 股票代码
            name: 股票名称
            cost_price: 持仓成本价
            quantity: 持仓数量
            note: 备注
            auto_monitor: 是否自动同步到监测
            
        Returns:
            (成功标志, 消息, 股票ID)
        """
        try:
            # 验证并标准化股票代码
            code = self._normalize_stock_code(code)
            if not code:
                return False, "股票代码不能为空", None

            provided_name = (name or "").strip()
            resolved_name = self._resolve_stock_name(code)
            final_name = resolved_name or provided_name
            if not final_name:
                return False, "无法根据股票代码自动识别股票名称，请检查代码格式后重试", None
            
            # 检查股票代码是否已存在
            existing = self.db.get_stock_by_code(code, account_name)
            if existing and existing.get("position_status", "active") == "active":
                return False, f"股票代码 {code} 已存在", None

            success, message, stock_id = self.lifecycle_service.create_position_from_analysis(
                symbol=code,
                stock_name=final_name,
                account_name=account_name,
                cost_price=cost_price,
                quantity=quantity,
                note=note,
                auto_monitor=auto_monitor,
                origin_analysis_id=origin_analysis_id,
            )
            if not success or stock_id is None:
                return False, message, None

            warnings: List[str] = []
            lifecycle_message = str(message or "").strip()
            if "（" in lifecycle_message and lifecycle_message.endswith("）"):
                warnings.append(lifecycle_message.split("（", 1)[1][:-1])

            self._mark_integrations_reconcile_pending()
            try:
                self.capture_daily_snapshot(account_name=account_name, source="manual")
            except Exception as exc:
                print(f"[WARN] 添加持仓后补写快照失败 ({code}): {exc}")
                warnings.append(f"快照补写失败: {exc}")

            success_message = f"添加持仓股票成功: {code} {final_name}"
            if warnings:
                success_message = f"{success_message}（{'；'.join(warnings)}）"
            return True, success_message, stock_id
            
        except Exception as e:
            return False, f"添加失败: {str(e)}", None
    
    def update_stock(self, stock_id: int, **kwargs) -> Tuple[bool, str]:
        """
        更新持仓股票信息
        
        Args:
            stock_id: 股票ID
            **kwargs: 要更新的字段
            
        Returns:
            (成功标志, 消息)
        """
        try:
            existing = self.db.get_stock(stock_id)
            if not existing:
                return False, f"未找到股票ID: {stock_id}"

            old_code = existing["code"]
            success = self.db.update_stock(stock_id, **kwargs)
            if success:
                self._mark_integrations_reconcile_pending()
                updated_stock = self.db.get_stock(stock_id)
                if old_code != updated_stock["code"]:
                    self.remove_managed_integrations_for_code(old_code)

                warning = ""
                try:
                    self.capture_daily_snapshot(
                        account_name=updated_stock.get("account_name", existing.get("account_name", "默认账户")),
                        source="manual",
                    )
                    self.lifecycle_service.sync_position(stock_id=stock_id)
                except Exception as e:
                    warning = f"（联动同步失败: {e}）"

                return True, f"更新成功{warning}"
            else:
                return False, f"未找到股票ID: {stock_id}"
        except Exception as e:
            return False, f"更新失败: {str(e)}"
    
    def delete_stock(self, stock_id: int) -> Tuple[bool, str]:
        """
        删除持仓股票（级联删除分析历史）
        
        Args:
            stock_id: 股票ID
            
        Returns:
            (成功标志, 消息)
        """
        try:
            existing = self.db.get_stock(stock_id)
            if not existing:
                return False, f"未找到股票ID: {stock_id}"

            success = self.db.delete_stock(stock_id)
            if success:
                self._mark_integrations_reconcile_pending()
                self.capture_daily_snapshot(
                    account_name=existing.get("account_name", "默认账户"),
                    source="manual",
                )
                warning = ""
                try:
                    self.lifecycle_service._delete_managed_items_for_position(existing)
                except Exception as e:
                    warning = f"（下游清理失败: {e}）"
                return True, f"删除成功{warning}"
            else:
                return False, f"未找到股票ID: {stock_id}"
        except Exception as e:
            return False, f"删除失败: {str(e)}"
    
    def _normalize_trade_type(self, trade_type: str) -> str:
        normalized = str(trade_type or "").strip().lower()
        if normalized in {"buy", "加仓", "买入", "建仓"}:
            return "buy"
        if normalized in {"sell", "减仓", "卖出"}:
            return "sell"
        if normalized in {"clear", "liquidate", "清仓", "清仓并降级"}:
            return "clear"
        return ""

    def seed_initial_trade(
        self,
        stock_id: int,
        trade_date: Optional[Any] = None,
        note: str = "",
    ) -> Tuple[bool, str]:
        """为新增持仓补写首笔建仓记录，不改变当前持仓数量。"""
        stock = self.db.get_stock(stock_id)
        if not stock:
            return False, f"未找到股票ID: {stock_id}"

        quantity = self._safe_int(stock.get("quantity"))
        cost_price = self._safe_float(stock.get("cost_price"))
        if quantity <= 0 or cost_price <= 0:
            return False, "当前持仓缺少成本价或数量，无法写入首笔建仓记录"

        summary = self.db.get_trade_summary_map([stock_id]).get(stock_id, {})
        if summary.get("trade_count", 0) > 0:
            return True, "已有交易记录，跳过首笔建仓记录补写"

        self.db.add_trade_history(
            stock_id=stock_id,
            trade_type="buy",
            trade_date=self._format_date_value(trade_date) or datetime.now().strftime("%Y-%m-%d"),
            price=cost_price,
            quantity=quantity,
            note=(note or "").strip(),
            trade_source="initial_position",
        )
        return True, "已补写首笔建仓记录"

    def record_trade(
        self,
        stock_id: int,
        trade_type: str,
        quantity: int,
        price: float,
        trade_date: Optional[Any] = None,
        note: str = "",
    ) -> Tuple[bool, str, Optional[Dict]]:
        """记录加仓/减仓/清仓交易，并同步更新当前持仓。"""
        normalized_trade_type = self._normalize_trade_type(trade_type)
        if normalized_trade_type not in {"buy", "sell", "clear"}:
            return False, "交易类型仅支持加仓/减仓/清仓", None

        stock = self.db.get_stock(stock_id)
        if not stock:
            return False, f"未找到股票ID: {stock_id}", None

        is_clear_trade = normalized_trade_type == "clear"
        current_quantity = self._safe_int(stock.get("quantity"))
        trade_quantity = current_quantity if is_clear_trade else self._safe_int(quantity)
        trade_price = self._safe_float(price)
        if trade_quantity <= 0:
            return False, ("当前没有可清仓的持仓数量" if is_clear_trade else "交易数量必须大于 0"), None
        if trade_price <= 0:
            return False, "成交价格必须大于 0", None

        formatted_trade_date = self._format_date_value(trade_date) or datetime.now().strftime("%Y-%m-%d")

        try:
            success, message, updated_stock = self.lifecycle_service.asset_service.record_manual_trade(
                asset_id=stock_id,
                trade_type=normalized_trade_type,
                quantity=trade_quantity,
                price=trade_price,
                trade_date=formatted_trade_date,
                note=((note or "").strip() or "清仓") if is_clear_trade else (note or "").strip(),
                trade_source="manual",
            )
            if not success:
                return False, message, None
            self._mark_integrations_reconcile_pending()

            warning = ""
            try:
                account_name = (updated_stock or stock).get("account_name", "默认账户")
                self.capture_daily_snapshot(account_name=account_name, source="manual")
                self.lifecycle_service.sync_position(stock_id=stock_id)
            except Exception as e:
                warning = f"（联动同步失败: {e}）"

            if normalized_trade_type == "buy":
                action_label = "加仓"
            elif normalized_trade_type == "clear":
                action_label = "清仓"
            else:
                action_label = "减仓"
            return True, f"{action_label}记录已保存{warning}", updated_stock
        except Exception as e:
            return False, f"保存交易记录失败: {e}", None

    def clear_position_to_watchlist(
        self,
        stock_id: int,
        *,
        price: float,
        trade_date: Optional[Any] = None,
        note: str = "",
    ) -> Tuple[bool, str, Optional[Dict]]:
        """按当前持仓数量登记清仓卖出，并把资产降级回盯盘。"""
        stock = self.db.get_stock(stock_id)
        if not stock:
            return False, f"未找到股票ID: {stock_id}", None

        quantity = self._safe_int(stock.get("quantity"))
        if quantity <= 0:
            return False, "当前没有可清仓的持仓数量", None

        return self.record_trade(
            stock_id=stock_id,
            trade_type="clear",
            quantity=quantity,
            price=price,
            trade_date=trade_date,
            note=(note or "").strip() or "清仓",
        )

    def get_trade_history(self, stock_id: int, limit: int = 20) -> List[Dict]:
        """获取指定持仓的交易流水。"""
        return self.db.get_trade_history(stock_id, limit=limit)

    def get_trade_records(self, account_name: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """获取账户范围内的交易流水。"""
        return self.db.get_trade_records(account_name=account_name, limit=limit)

    def get_trade_summary_map(self, stock_ids: List[int]) -> Dict[int, Dict]:
        """批量获取持仓交易摘要。"""
        if not stock_ids:
            return {}
        return self.db.get_trade_summary_map(stock_ids)

    def get_stock(self, stock_id: int) -> Optional[Dict]:
        """获取单只持仓股票信息"""
        return self.db.get_stock(stock_id)
    
    def get_all_stocks(self, auto_monitor_only: bool = False) -> List[Dict]:
        """获取所有持仓股票列表"""
        return self.db.get_all_stocks(auto_monitor_only)
    
    def search_stocks(self, keyword: str) -> List[Dict]:
        """搜索持仓股票"""
        return self.db.search_stocks(keyword)
    
    def get_stock_count(self) -> int:
        """获取持仓股票总数"""
        return self.db.get_stock_count()

    def _get_stock_data_fetcher(self):
        if self._stock_data_fetcher is None:
            from stock_data import StockDataFetcher

            self._stock_data_fetcher = StockDataFetcher()
        return self._stock_data_fetcher

    def _parse_date_value(self, value: Optional[Any]) -> Optional[pd.Timestamp]:
        if value in (None, ""):
            return None
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return pd.Timestamp(parsed).normalize()

    def _format_date_value(self, value: Optional[Any]) -> Optional[str]:
        parsed = self._parse_date_value(value)
        return parsed.strftime("%Y-%m-%d") if parsed is not None else None

    def _get_account_display_name(self, account_name: Optional[str]) -> str:
        return account_name or self.AGGREGATE_ACCOUNT_NAME

    def _filter_stocks_for_account(self, stocks: List[Dict], account_name: Optional[str] = None) -> List[Dict]:
        if not account_name or account_name == self.AGGREGATE_ACCOUNT_NAME:
            return list(stocks)
        return [stock for stock in stocks if stock.get("account_name", "默认账户") == account_name]

    def _safe_int(self, value: Any) -> int:
        try:
            return int(value) if value not in (None, "") else 0
        except (TypeError, ValueError):
            return 0

    def _safe_float(self, value: Any) -> float:
        try:
            return float(value) if value not in (None, "") else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _normalize_optional_text(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass

        text = str(value).strip()
        return "" if text in {"", "-", "--", "N/A", "NA", "未知", "未知行业", "null", "None", "nan"} else text

    def _extract_industry_from_payload(self, payload: Any) -> Optional[str]:
        if not isinstance(payload, dict):
            return None

        for key in ("industry", "所属同花顺行业", "所属行业", "所处行业", "行业", "sector"):
            candidate = self._normalize_optional_text(payload.get(key))
            if candidate:
                return candidate
        return None

    def _get_basic_stock_info(self, code: Optional[str]) -> Dict[str, Any]:
        normalized_code = self._normalize_stock_code(code or "")
        if not normalized_code:
            return {}

        cached = self._basic_stock_info_cache.get(normalized_code)
        if cached is not None:
            return cached

        info: Dict[str, Any] = {}
        try:
            from data_source_manager import data_source_manager

            raw_info = data_source_manager.get_stock_basic_info(normalized_code)
            if isinstance(raw_info, dict):
                info = raw_info
        except Exception as e:
            print(f"[WARN] 股票基础信息回补失败 ({normalized_code}): {e}")

        self._basic_stock_info_cache[normalized_code] = info
        return info

    def _get_latest_price_and_industry(self, stock: Dict) -> Tuple[float, str]:
        current_price = self._safe_float(stock.get("current_price"))
        industry = self._extract_industry_from_payload(stock) or "未知行业"

        stock_info = stock.get("stock_info")
        if isinstance(stock_info, dict):
            if current_price <= 0:
                current_price = self._extract_first_number(stock_info.get("current_price"), allow_zero=True) or 0.0
            industry = self._extract_industry_from_payload(stock_info) or industry

        if current_price <= 0 or industry == "未知行业":
            latest_analysis = self.get_latest_analysis(stock["id"])
            if latest_analysis:
                current_price = self._safe_float(latest_analysis.get("current_price"))
                latest_stock_info = latest_analysis.get("stock_info")
                if isinstance(latest_stock_info, dict):
                    if current_price <= 0:
                        current_price = (
                            self._extract_first_number(latest_stock_info.get("current_price"), allow_zero=True) or 0.0
                        )
                    industry = self._extract_industry_from_payload(latest_stock_info) or industry

        if industry == "未知行业":
            basic_info = self._get_basic_stock_info(stock.get("code") or stock.get("symbol"))
            industry = self._extract_industry_from_payload(basic_info) or industry

        cost_price = self._safe_float(stock.get("cost_price"))
        if current_price <= 0:
            current_price = cost_price

        return current_price, industry

    def _build_portfolio_snapshot_payload(self, stocks: List[Dict], account_name: Optional[str] = None) -> Dict:
        total_market_value = 0.0
        total_cost_value = 0.0
        holdings = []

        for stock in self._filter_stocks_for_account(stocks, account_name):
            quantity = self._safe_int(stock.get("quantity"))
            cost_price = self._safe_float(stock.get("cost_price"))
            if quantity <= 0:
                continue

            current_price, industry = self._get_latest_price_and_industry(stock)
            market_value = current_price * quantity
            cost_value = cost_price * quantity
            pnl = market_value - cost_value
            pnl_pct = (pnl / cost_value) if cost_value > 0 else 0.0

            total_market_value += market_value
            total_cost_value += cost_value
            holdings.append(
                {
                    "stock_id": stock.get("id"),
                    "account_name": stock.get("account_name", "默认账户"),
                    "code": stock.get("code"),
                    "name": stock.get("name"),
                    "quantity": quantity,
                    "cost_price": cost_price,
                    "current_price": current_price,
                    "market_value": market_value,
                    "cost_value": cost_value,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                    "industry": industry,
                }
            )

        holdings.sort(key=lambda item: item.get("market_value", 0), reverse=True)
        total_pnl = total_market_value - total_cost_value
        return {
            "account_name": self._get_account_display_name(account_name),
            "total_market_value": total_market_value,
            "total_cost_value": total_cost_value,
            "total_pnl": total_pnl,
            "holdings": holdings,
        }

    def _upsert_snapshot_for_account(
        self,
        account_name: str,
        stocks: List[Dict],
        snapshot_date: str,
        source: str,
    ) -> int:
        payload = self._build_portfolio_snapshot_payload(
            stocks,
            None if account_name == self.AGGREGATE_ACCOUNT_NAME else account_name,
        )
        return self.db.upsert_daily_snapshot(
            account_name=account_name,
            snapshot_date=snapshot_date,
            total_market_value=payload["total_market_value"],
            total_cost_value=payload["total_cost_value"],
            total_pnl=payload["total_pnl"],
            holdings=payload["holdings"],
            data_source=source,
        )

    def capture_daily_snapshot(self, account_name: Optional[str] = None, source: str = "manual") -> Dict:
        """采集当日持仓快照，并按日 upsert。"""
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        all_stocks = self.get_all_stocks()
        accounts = sorted({stock.get("account_name", "默认账户") for stock in all_stocks})
        captured_accounts: List[str] = []

        if account_name and account_name != self.AGGREGATE_ACCOUNT_NAME:
            self._upsert_snapshot_for_account(account_name, all_stocks, snapshot_date, source)
            captured_accounts.append(account_name)
            self._upsert_snapshot_for_account(self.AGGREGATE_ACCOUNT_NAME, all_stocks, snapshot_date, source)
            captured_accounts.append(self.AGGREGATE_ACCOUNT_NAME)
        else:
            for name in accounts:
                self._upsert_snapshot_for_account(name, all_stocks, snapshot_date, source)
                captured_accounts.append(name)
            self._upsert_snapshot_for_account(self.AGGREGATE_ACCOUNT_NAME, all_stocks, snapshot_date, source)
            captured_accounts.append(self.AGGREGATE_ACCOUNT_NAME)

        return {
            "snapshot_date": snapshot_date,
            "accounts": captured_accounts,
            "captured": len(captured_accounts),
        }

    def ensure_daily_snapshot(self, account_name: Optional[str] = None, source: str = "page_load") -> Dict:
        """确保今日快照存在，缺失时自动补采。"""
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        all_stocks = self.get_all_stocks()
        accounts = sorted({stock.get("account_name", "默认账户") for stock in all_stocks})
        required_accounts: List[str] = []

        if account_name and account_name != self.AGGREGATE_ACCOUNT_NAME:
            required_accounts.append(account_name)
        else:
            required_accounts.extend(accounts)
        required_accounts.append(self.AGGREGATE_ACCOUNT_NAME)

        missing = [
            account
            for account in required_accounts
            if not self.db.has_snapshot_for_date(account, snapshot_date)
        ]
        if missing:
            return self.capture_daily_snapshot(account_name=account_name, source=source)
        return {"snapshot_date": snapshot_date, "accounts": required_accounts, "captured": 0}

    def get_risk_free_rate_annual(self) -> float:
        raw_value = self.db.get_setting("risk_free_rate_annual", self.DEFAULT_RISK_FREE_RATE)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            value = self.DEFAULT_RISK_FREE_RATE
        return max(0.0, value)

    def set_risk_free_rate_annual(self, value: float) -> float:
        normalized = max(0.0, float(value))
        self.db.set_setting("risk_free_rate_annual", normalized)
        return normalized

    def _resolve_history_period(self, start_date: Optional[str], end_date: Optional[str]) -> str:
        start_ts = self._parse_date_value(start_date)
        end_ts = self._parse_date_value(end_date) or pd.Timestamp.now().normalize()
        if start_ts is None:
            return "1y"
        delta_days = max((end_ts - start_ts).days, 1)
        if delta_days <= 31:
            return "1mo"
        if delta_days <= 93:
            return "3mo"
        if delta_days <= 186:
            return "6mo"
        return "1y"

    def _normalize_price_series(self, frame: Any) -> pd.Series:
        if isinstance(frame, dict) or frame is None:
            return pd.Series(dtype=float)

        if isinstance(frame, pd.Series):
            series = pd.to_numeric(frame, errors="coerce")
            series.index = pd.to_datetime(series.index, errors="coerce")
            series = series[~series.index.isna()]
            series.index = pd.DatetimeIndex(series.index).normalize()
            return series.dropna().sort_index()

        if not isinstance(frame, pd.DataFrame) or frame.empty:
            return pd.Series(dtype=float)

        working = frame.copy()
        date_col = next((col for col in ["Date", "date", "日期"] if col in working.columns), None)
        if date_col:
            working[date_col] = pd.to_datetime(working[date_col], errors="coerce")
            working = working.dropna(subset=[date_col]).set_index(date_col)
        else:
            working.index = pd.to_datetime(working.index, errors="coerce")
            working = working[~working.index.isna()]

        close_col = next(
            (
                col
                for col in [
                    "Close",
                    "close",
                    "收盘",
                    "close_price",
                ]
                if col in working.columns
            ),
            None,
        )
        if close_col is None:
            return pd.Series(dtype=float)

        series = pd.to_numeric(working[close_col], errors="coerce").dropna()
        series.index = pd.DatetimeIndex(series.index).normalize()
        return series.sort_index()

    def _fetch_price_series(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.Series:
        fetcher = self._get_stock_data_fetcher()
        period = self._resolve_history_period(start_date, end_date)
        data = fetcher.get_stock_data(symbol, period=period, interval="1d")
        series = self._normalize_price_series(data)
        if series.empty:
            return series

        start_ts = self._parse_date_value(start_date)
        end_ts = self._parse_date_value(end_date)
        if start_ts is not None:
            series = series[series.index >= start_ts]
        if end_ts is not None:
            series = series[series.index <= end_ts]
        return series.sort_index()

    def _fetch_benchmark_price_series(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Tuple[pd.Series, str]:
        start_token = (self._format_date_value(start_date) or "").replace("-", "")
        end_token = (self._format_date_value(end_date) or datetime.now().strftime("%Y%m%d")).replace("-", "")

        try:
            import akshare as ak

            loaders = [
                lambda: ak.index_zh_a_hist(
                    symbol="000300",
                    period="daily",
                    start_date=start_token,
                    end_date=end_token,
                ),
                lambda: ak.stock_zh_index_daily_em(symbol="sh000300"),
                lambda: ak.stock_zh_index_daily(symbol="sh000300"),
            ]
            for loader in loaders:
                try:
                    series = self._normalize_price_series(loader())
                except Exception:
                    series = pd.Series(dtype=float)
                if not series.empty:
                    start_ts = self._parse_date_value(start_date)
                    end_ts = self._parse_date_value(end_date)
                    if start_ts is not None:
                        series = series[series.index >= start_ts]
                    if end_ts is not None:
                        series = series[series.index <= end_ts]
                    if not series.empty:
                        return series, "沪深300"
        except Exception as exc:
            print(f"[WARN] 获取沪深300指数日线失败，尝试 510300 ETF 回退: {exc}")

        fallback_series = self._fetch_price_series("510300", start_date=start_date, end_date=end_date)
        return fallback_series, "沪深300"

    def _load_snapshot_series(
        self,
        account_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        snapshot_account = self._get_account_display_name(account_name)
        rows = self.db.get_daily_snapshots(
            account_name=snapshot_account,
            start_date=self._format_date_value(start_date),
            end_date=self._format_date_value(end_date),
        )
        if not rows:
            return pd.DataFrame()

        frame = pd.DataFrame(rows)
        frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"], errors="coerce")
        frame = frame.dropna(subset=["snapshot_date"]).set_index("snapshot_date").sort_index()
        frame.index = frame.index.normalize()
        frame["source"] = "actual"
        frame["total_market_value"] = pd.to_numeric(frame["total_market_value"], errors="coerce").fillna(0.0)
        frame["total_cost_value"] = pd.to_numeric(frame["total_cost_value"], errors="coerce").fillna(0.0)
        frame["total_pnl"] = pd.to_numeric(frame["total_pnl"], errors="coerce").fillna(
            frame["total_market_value"] - frame["total_cost_value"]
        )
        return frame[["total_market_value", "total_cost_value", "total_pnl", "source"]]

    def _build_estimated_portfolio_series(
        self,
        stocks: List[Dict],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        market_value_series: List[pd.Series] = []
        total_cost_value = 0.0

        for stock in stocks:
            quantity = self._safe_int(stock.get("quantity"))
            if quantity <= 0:
                continue
            cost_price = self._safe_float(stock.get("cost_price"))
            total_cost_value += cost_price * quantity

            series = self._fetch_price_series(stock.get("code", ""), start_date=start_date, end_date=end_date)
            if series.empty:
                continue
            market_value_series.append((series * quantity).rename(stock.get("code", "")))

        if not market_value_series:
            return pd.DataFrame()

        market_frame = pd.concat(market_value_series, axis=1).sort_index().ffill()
        total_market_value = market_frame.sum(axis=1, min_count=1).dropna()
        if total_market_value.empty:
            return pd.DataFrame()

        estimated = pd.DataFrame(index=total_market_value.index)
        estimated["total_market_value"] = total_market_value
        estimated["total_cost_value"] = float(total_cost_value)
        estimated["total_pnl"] = estimated["total_market_value"] - estimated["total_cost_value"]
        estimated["source"] = "estimated"
        return estimated

    def build_portfolio_return_series(
        self,
        account_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        prefer_snapshots: bool = True,
    ) -> pd.DataFrame:
        """构建组合日度市值与盈亏时间序列。"""
        stocks = self._filter_stocks_for_account(self.get_all_stocks(), account_name)
        start_str = self._format_date_value(start_date)
        end_str = self._format_date_value(end_date)
        snapshot_series = self._load_snapshot_series(account_name, start_str, end_str) if prefer_snapshots else pd.DataFrame()
        estimated_series = self._build_estimated_portfolio_series(stocks, start_date=start_str, end_date=end_str)

        if snapshot_series.empty and estimated_series.empty:
            return pd.DataFrame()

        if snapshot_series.empty:
            combined = estimated_series.copy()
        elif estimated_series.empty:
            combined = snapshot_series.copy()
        else:
            combined = estimated_series.copy()
            combined = combined.reindex(combined.index.union(snapshot_series.index)).sort_index()
            for column in ["total_market_value", "total_cost_value", "total_pnl", "source"]:
                combined.loc[snapshot_series.index, column] = snapshot_series[column]

        combined = combined.sort_index()
        combined.index = pd.DatetimeIndex(combined.index).normalize()
        combined["total_market_value"] = pd.to_numeric(combined["total_market_value"], errors="coerce")
        combined["total_cost_value"] = pd.to_numeric(combined["total_cost_value"], errors="coerce")
        combined = combined.dropna(subset=["total_market_value"]).copy()
        combined["total_pnl"] = combined["total_market_value"] - combined["total_cost_value"]
        combined["daily_pnl_change"] = combined["total_market_value"].diff().fillna(0.0)
        combined["daily_return"] = combined["total_market_value"].pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)

        source_values = set(combined["source"].dropna().tolist())
        if source_values == {"actual"}:
            data_mode = "actual"
        elif source_values == {"estimated"}:
            data_mode = "estimated"
        else:
            data_mode = "mixed"

        combined.attrs["data_mode"] = data_mode
        combined.attrs["stock_count"] = len([stock for stock in stocks if self._safe_int(stock.get("quantity")) > 0])
        combined.attrs["available_days"] = len(combined.index)
        combined.attrs["contains_estimated"] = "estimated" in source_values
        return combined

    def _calculate_quantitative_risk_metrics(
        self,
        series: pd.DataFrame,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict:
        warnings: List[str] = []
        if series.empty or "total_market_value" not in series.columns:
            warnings.append("组合历史数据不足，暂无法计算量化风险指标。")
            return {
                "annual_volatility": None,
                "beta_hs300": None,
                "sharpe_ratio": None,
                "annualized_return": None,
                "risk_free_rate_annual": self.get_risk_free_rate_annual(),
                "benchmark_label": "沪深300",
                "metric_warnings": warnings,
                "data_coverage": {
                    "available_days": 0,
                    "stock_count": 0,
                    "data_mode": "estimated",
                    "contains_estimated": False,
                },
            }

        returns = pd.to_numeric(series["daily_return"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(returns) < 20:
            warnings.append("可用交易日不足 20 天，波动率/Beta/夏普比率仅供参考。")

        annual_volatility = None
        if len(returns) >= 2:
            annual_volatility = float(returns.std(ddof=0) * math.sqrt(252))
        else:
            warnings.append("历史收益序列不足 2 个交易日，无法计算年化波动率。")

        annualized_return = None
        start_value = self._safe_float(series["total_market_value"].iloc[0])
        end_value = self._safe_float(series["total_market_value"].iloc[-1])
        if start_value > 0 and len(returns) > 0:
            annualized_return = float((end_value / start_value) ** (252 / max(len(returns), 1)) - 1)
        else:
            warnings.append("组合起始市值无效，无法计算年化收益率。")

        risk_free_rate = self.get_risk_free_rate_annual()
        sharpe_ratio = None
        if annual_volatility and annual_volatility > 0 and annualized_return is not None:
            sharpe_ratio = float((annualized_return - risk_free_rate) / annual_volatility)
        elif annual_volatility == 0:
            warnings.append("组合波动率为 0，无法计算夏普比率。")

        beta_hs300 = None
        benchmark_series, benchmark_label = self._fetch_benchmark_price_series(start_date=start_date, end_date=end_date)
        if benchmark_series.empty:
            warnings.append("未获取到沪深300基准数据，无法计算 Beta。")
        else:
            benchmark_returns = benchmark_series.pct_change().replace([np.inf, -np.inf], np.nan)
            merged = pd.concat(
                [returns.rename("portfolio"), benchmark_returns.rename("benchmark")],
                axis=1,
            ).dropna()
            if len(merged) < 20:
                warnings.append("组合与基准重叠交易日不足 20 天，Beta 仅供参考。")
            benchmark_variance = merged["benchmark"].var(ddof=0) if not merged.empty else 0.0
            if benchmark_variance > 0:
                covariance = merged[["portfolio", "benchmark"]].cov(ddof=0).iloc[0, 1]
                beta_hs300 = float(covariance / benchmark_variance)
            elif not merged.empty:
                warnings.append("基准收益波动过低，无法计算 Beta。")

        return {
            "annual_volatility": annual_volatility,
            "beta_hs300": beta_hs300,
            "sharpe_ratio": sharpe_ratio,
            "annualized_return": annualized_return,
            "risk_free_rate_annual": risk_free_rate,
            "benchmark_label": benchmark_label,
            "metric_warnings": warnings,
            "data_coverage": {
                "available_days": len(series.index),
                "stock_count": int(series.attrs.get("stock_count", 0)),
                "data_mode": series.attrs.get("data_mode", "estimated"),
                "contains_estimated": bool(series.attrs.get("contains_estimated", False)),
            },
        }

    def build_pnl_calendar(
        self,
        account_name: Optional[str] = None,
        view: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict:
        """构建盈亏日历所需的日度或月度数据。"""
        series = self.build_portfolio_return_series(
            account_name=account_name,
            start_date=start_date,
            end_date=end_date,
            prefer_snapshots=True,
        )
        if series.empty:
            return {"status": "error", "message": "暂无足够的历史数据生成盈亏日历。"}

        daily = series.reset_index().rename(columns={"index": "date"})
        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
        daily = daily.dropna(subset=["date"]).copy()
        daily["date"] = daily["date"].dt.normalize()
        daily["year"] = daily["date"].dt.year
        daily["month"] = daily["date"].dt.month
        daily["day"] = daily["date"].dt.day
        daily["month_label"] = daily["date"].dt.strftime("%Y-%m")
        daily["source_label"] = daily["source"].map({"actual": "真实", "estimated": "估算"}).fillna("混合")

        if view == "monthly":
            monthly = (
                daily.groupby("month_label", as_index=False)
                .agg(
                    date=("date", "max"),
                    pnl=("daily_pnl_change", "sum"),
                    total_market_value=("total_market_value", "last"),
                    total_pnl=("total_pnl", "last"),
                    source=(
                        "source",
                        lambda values: "mixed" if len(set(values)) > 1 else next(iter(set(values)), "estimated"),
                    ),
                )
                .sort_values("date")
            )
            monthly["source_label"] = monthly["source"].map(
                {"actual": "真实", "estimated": "估算", "mixed": "混合"}
            ).fillna("混合")
            records = monthly.to_dict("records")
        else:
            records = daily.to_dict("records")

        return {
            "status": "success",
            "view": view,
            "records": records,
            "data_mode": series.attrs.get("data_mode", "estimated"),
            "available_days": len(daily.index),
        }

    def _resolve_review_period(self, period_type: str, reference: Optional[date] = None) -> Tuple[date, date]:
        today = reference or datetime.now().date()
        if period_type == "week":
            current_week_start = today - timedelta(days=today.weekday())
            end_date = current_week_start - timedelta(days=1)
            start_date = end_date - timedelta(days=6)
            return start_date, end_date
        if period_type == "quarter":
            current_quarter = (today.month - 1) // 3 + 1
            quarter_end_month = (current_quarter - 1) * 3
            if quarter_end_month == 0:
                year = today.year - 1
                quarter_end_month = 12
            else:
                year = today.year
            end_date = (pd.Timestamp(date(year, quarter_end_month, 1)) + pd.offsets.MonthEnd(0)).date()
            start_date = date(end_date.year, end_date.month - 2, 1)
            return start_date, end_date

        first_day_this_month = today.replace(day=1)
        end_date = first_day_this_month - timedelta(days=1)
        start_date = end_date.replace(day=1)
        return start_date, end_date

    def _calculate_stock_contributions(
        self,
        stocks: List[Dict],
        start_date: str,
        end_date: str,
    ) -> List[Dict]:
        contributions = []
        history_start = (pd.to_datetime(start_date) - timedelta(days=10)).strftime("%Y-%m-%d")
        for stock in stocks:
            quantity = self._safe_int(stock.get("quantity"))
            if quantity <= 0:
                continue
            series = self._fetch_price_series(stock.get("code", ""), start_date=history_start, end_date=end_date)
            if series.empty:
                continue
            period_series = series[series.index >= pd.to_datetime(start_date)]
            if period_series.empty:
                continue
            start_price = self._safe_float(period_series.iloc[0])
            end_price = self._safe_float(period_series.iloc[-1])
            pnl = (end_price - start_price) * quantity
            return_pct = ((end_price - start_price) / start_price) if start_price > 0 else 0.0
            contributions.append(
                {
                    "code": stock.get("code"),
                    "name": stock.get("name"),
                    "pnl": pnl,
                    "return_pct": return_pct,
                }
            )
        contributions.sort(key=lambda item: item["pnl"], reverse=True)
        return contributions

    def _build_review_markdown(
        self,
        account_label: str,
        period_type: str,
        start_date: str,
        end_date: str,
        summary: Dict,
    ) -> str:
        top_contributors = summary.get("top_contributors", [])
        worst_contributors = summary.get("worst_contributors", [])
        risk_metrics = summary.get("risk_metrics", {})
        concentration = summary.get("concentration_summary", {})

        lines = [
            f"# {account_label} {period_type.upper()} 投资复盘报告",
            "",
            f"- 周期: {start_date} 至 {end_date}",
            f"- 数据口径: {summary.get('data_mode_label', '估算')}",
            f"- 参考基准: {risk_metrics.get('benchmark_label', '沪深300')}",
            "",
            "## 组合表现",
            "",
            f"- 期初组合市值: ¥{summary.get('start_market_value', 0):,.2f}",
            f"- 期末组合市值: ¥{summary.get('end_market_value', 0):,.2f}",
            f"- 周期累计盈亏: ¥{summary.get('cumulative_pnl', 0):,.2f}",
            f"- 周期收益率: {summary.get('cumulative_return_pct', 0):.2f}%",
            f"- 盈利天数 / 亏损天数: {summary.get('winning_days', 0)} / {summary.get('losing_days', 0)}",
            f"- 胜率: {summary.get('win_rate_pct', 0):.2f}%",
            f"- 最大单日盈利: ¥{summary.get('best_day_pnl', 0):,.2f}",
            f"- 最大单日亏损: ¥{summary.get('worst_day_pnl', 0):,.2f}",
            "",
            "## 风险指标",
            "",
            f"- 年化波动率: {summary.get('annual_volatility_text', 'N/A')}",
            f"- Beta(沪深300): {summary.get('beta_text', 'N/A')}",
            f"- 夏普比率: {summary.get('sharpe_text', 'N/A')}",
            "",
            "## 持仓结构",
            "",
            f"- 期末持仓数量: {summary.get('position_count', 0)}",
            f"- 最大单票权重: {concentration.get('top_stock', 'N/A')}",
            f"- 最大行业权重: {concentration.get('top_industry', 'N/A')}",
            "",
            "## 最佳贡献",
            "",
        ]

        if top_contributors:
            for item in top_contributors:
                lines.append(
                    f"- {item.get('code')} {item.get('name')}: ¥{item.get('pnl', 0):,.2f} ({item.get('return_pct', 0) * 100:.2f}%)"
                )
        else:
            lines.append("- 暂无可计算的持仓贡献数据")

        lines.extend(["", "## 最差贡献", ""])
        if worst_contributors:
            for item in worst_contributors:
                lines.append(
                    f"- {item.get('code')} {item.get('name')}: ¥{item.get('pnl', 0):,.2f} ({item.get('return_pct', 0) * 100:.2f}%)"
                )
        else:
            lines.append("- 暂无可计算的持仓贡献数据")

        metric_warnings = risk_metrics.get("metric_warnings", [])
        if metric_warnings:
            lines.extend(["", "## 数据提示", ""])
            for warning in metric_warnings:
                lines.append(f"- {warning}")

        return "\n".join(lines).strip()

    def generate_review_report(
        self,
        account_name: Optional[str] = None,
        period_type: str = "month",
    ) -> Dict:
        """生成并保存周/月/季持仓复盘报告。"""
        start_dt, end_dt = self._resolve_review_period(period_type)
        account_label = self._get_account_display_name(account_name)
        history_start = (pd.Timestamp(start_dt) - timedelta(days=10)).strftime("%Y-%m-%d")
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        series = self.build_portfolio_return_series(
            account_name=account_name,
            start_date=history_start,
            end_date=end_str,
            prefer_snapshots=True,
        )
        if series.empty:
            return {"status": "error", "message": "暂无足够的组合历史数据生成复盘报告。"}

        window = series[(series.index >= pd.Timestamp(start_str)) & (series.index <= pd.Timestamp(end_str))].copy()
        if window.empty:
            return {"status": "error", "message": "所选周期内没有可用的组合数据。"}

        risk_metrics = self._calculate_quantitative_risk_metrics(window, start_date=start_str, end_date=end_str)
        cumulative_pnl = self._safe_float(window["total_market_value"].iloc[-1] - window["total_market_value"].iloc[0])
        cumulative_return = (
            cumulative_pnl / self._safe_float(window["total_market_value"].iloc[0])
            if self._safe_float(window["total_market_value"].iloc[0]) > 0
            else 0.0
        )
        daily_changes = pd.to_numeric(window["daily_pnl_change"], errors="coerce").fillna(0.0)
        winning_days = int((daily_changes > 0).sum())
        losing_days = int((daily_changes < 0).sum())
        total_trading_days = int(((daily_changes > 0) | (daily_changes < 0)).sum())
        win_rate = (winning_days / total_trading_days) if total_trading_days else 0.0

        stocks = self._filter_stocks_for_account(self.get_all_stocks(), account_name)
        concentration_data = self.calculate_portfolio_risk(account_name=account_name)
        stock_distribution = concentration_data.get("stock_distribution", [])
        industry_distribution = concentration_data.get("industry_distribution", [])
        contributions = self._calculate_stock_contributions(stocks, start_str, end_str)
        summary = {
            "start_market_value": self._safe_float(window["total_market_value"].iloc[0]),
            "end_market_value": self._safe_float(window["total_market_value"].iloc[-1]),
            "cumulative_pnl": cumulative_pnl,
            "cumulative_return_pct": cumulative_return * 100,
            "winning_days": winning_days,
            "losing_days": losing_days,
            "win_rate_pct": win_rate * 100,
            "best_day_pnl": self._safe_float(daily_changes.max()) if not daily_changes.empty else 0.0,
            "worst_day_pnl": self._safe_float(daily_changes.min()) if not daily_changes.empty else 0.0,
            "annual_volatility_text": (
                f"{risk_metrics['annual_volatility'] * 100:.2f}%"
                if risk_metrics.get("annual_volatility") is not None
                else "N/A"
            ),
            "beta_text": (
                f"{risk_metrics['beta_hs300']:.3f}"
                if risk_metrics.get("beta_hs300") is not None
                else "N/A"
            ),
            "sharpe_text": (
                f"{risk_metrics['sharpe_ratio']:.3f}"
                if risk_metrics.get("sharpe_ratio") is not None
                else "N/A"
            ),
            "position_count": len([stock for stock in stocks if self._safe_int(stock.get("quantity")) > 0]),
            "concentration_summary": {
                "top_stock": (
                    f"{stock_distribution[0]['name']} {stock_distribution[0]['weight'] * 100:.1f}%"
                    if stock_distribution
                    else "N/A"
                ),
                "top_industry": (
                    f"{industry_distribution[0]['industry']} {industry_distribution[0]['weight'] * 100:.1f}%"
                    if industry_distribution
                    else "N/A"
                ),
            },
            "top_contributors": contributions[:3],
            "worst_contributors": list(reversed(contributions[-3:])) if contributions else [],
            "data_mode_label": {
                "actual": "真实",
                "estimated": "估算",
                "mixed": "混合",
            }.get(window.attrs.get("data_mode", "estimated"), "估算"),
            "risk_metrics": risk_metrics,
        }
        markdown = self._build_review_markdown(account_label, period_type, start_str, end_str, summary)
        report_id = self.db.save_review_report(
            account_name=account_label,
            period_type=period_type,
            period_start=start_str,
            period_end=end_str,
            data_mode=window.attrs.get("data_mode", "estimated"),
            report_markdown=markdown,
            report_json=summary,
        )
        return {
            "status": "success",
            "report_id": report_id,
            "account_name": account_label,
            "period_type": period_type,
            "period_start": start_str,
            "period_end": end_str,
            "data_mode": window.attrs.get("data_mode", "estimated"),
            "report_markdown": markdown,
            "summary": summary,
        }

    def get_review_reports(
        self,
        account_name: Optional[str] = None,
        limit: int = 20,
        period_type: Optional[str] = None,
    ) -> List[Dict]:
        account_label = self._get_account_display_name(account_name) if account_name else None
        return self.db.get_review_reports(account_name=account_label, limit=limit, period_type=period_type)

    def _extract_first_number(self, value, allow_zero: bool = False) -> Optional[float]:
        """从数值或字符串中提取首个数字。"""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            result = float(value)
        else:
            numbers = re.findall(r'-?\d+\.?\d*', str(value).replace(',', ''))
            if not numbers:
                return None
            result = float(numbers[0])

        if not allow_zero and result == 0:
            return None
        return result

    def _extract_confidence(self, value) -> float:
        """统一解析信心度到 0-10。"""
        confidence = self._extract_first_number(value, allow_zero=True)
        if confidence is None:
            return 5.0
        return max(0.0, min(10.0, confidence))

    def _extract_entry_range(self, value) -> Tuple[Optional[float], Optional[float]]:
        """解析进场区间。支持字典和多种字符串格式。"""
        if isinstance(value, dict):
            entry_min = self._extract_first_number(value.get("min"))
            entry_max = self._extract_first_number(value.get("max"))
            if entry_min is not None and entry_max is not None:
                return (min(entry_min, entry_max), max(entry_min, entry_max))
            return (None, None)

        if not value:
            return (None, None)

        cleaned = str(value).replace("¥", "").replace("元", "").replace("$", "")
        numbers = re.findall(r'\d+\.?\d*', cleaned)
        if len(numbers) >= 2:
            entry_min = float(numbers[0])
            entry_max = float(numbers[1])
            return (min(entry_min, entry_max), max(entry_min, entry_max))

        return (None, None)

    def _sanitize_summary_text(self, value: str) -> str:
        if not value:
            return ""

        text = str(value).strip()
        text = re.sub(
            r"<p[^>]*portfolio-stock-card__summary[^>]*>",
            " ",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r"</p>", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"<think>.*?</think>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
        text = re.sub(r"【推理过程】.*", " ", text, flags=re.DOTALL)
        text = re.sub(r"推理过程[:：].*", " ", text, flags=re.DOTALL)
        text = re.sub(r"\s+", " ", text).strip()

        banned_markers = ("我现在需要", "首先，我得", "首先需要", "接下来需要", "JSON格式投资决策")
        if any(marker in text for marker in banned_markers):
            return ""
        return text[:240]

    def _build_fallback_summary(self, final_decision: Dict) -> str:
        rating = self._normalize_analysis_rating(final_decision.get("rating"), default="持有")
        parts = [f"评级: {rating}"]

        operation_advice = self._sanitize_summary_text(final_decision.get("operation_advice") or "")
        if operation_advice:
            parts.append(operation_advice)

        for label, key in (("进场区间", "entry_range"), ("止盈位", "take_profit"), ("止损位", "stop_loss")):
            value = final_decision.get(key)
            if value:
                parts.append(f"{label}: {value}")

        return "；".join(parts)[:240]

    def _extract_analysis_summary(self, final_decision: Dict) -> str:
        """鎻愬彇鍒嗘瀽鎽樿銆?"""
        for key in ("operation_advice", "advice", "summary"):
            cleaned = self._sanitize_summary_text(final_decision.get(key))
            if cleaned:
                return cleaned
        return self._build_fallback_summary(final_decision)

    def _normalize_analysis_rating(self, value, default: str = "持有") -> str:
        text = str(value or "").strip()
        if not text:
            return default

        normalized_aliases = {
            "买入": "买入",
            "强烈买入": "买入",
            "增持": "买入",
            "持有": "持有",
            "中性": "持有",
            "观望": "持有",
            "卖出": "卖出",
            "减持": "卖出",
            "鎸佹湁": "持有",
            "涔板叆": "买入",
            "鍗栧嚭": "卖出",
        }
        if text in normalized_aliases:
            return normalized_aliases[text]

        lowered = text.lower()
        if text in {"未知", "待分析", "N/A"} or lowered in {"unknown", "n/a", "na"}:
            return default
        if any(token in text for token in ("买入", "强烈买入", "增持")) or any(
            token in lowered for token in ("buy", "add")
        ):
            return "买入"
        if any(token in text for token in ("卖出", "减持")) or any(
            token in lowered for token in ("sell", "reduce")
        ):
            return "卖出"
        if any(token in text for token in ("持有", "中性", "观望")) or any(
            token in lowered for token in ("hold", "neutral")
        ):
            return "持有"
        return text

    def _extract_rating_from_text(self, value: str) -> str:
        text = self._sanitize_summary_text(value)
        if not text:
            return ""

        match = re.search(r"(?:投资)?评级\s*[:：]\s*([^\s；;，,。]+)", text)
        if match:
            return self._normalize_analysis_rating(match.group(1), default="")

        for token in ("买入", "持有", "卖出", "鎸佹湁", "涔板叆", "鍗栧嚭"):
            if token in text:
                return self._normalize_analysis_rating(token, default="")
        return ""

    def _sanitize_card_summary(self, value: str, *, rating: str = "") -> str:
        text = self._sanitize_summary_text(value)
        if not text:
            return ""

        text = re.sub(r"^(?:最新)?摘要\s*[:：]\s*", "", text, count=1)
        if rating:
            text = re.sub(
                rf"^(?:投资)?评级\s*[:：]\s*{re.escape(rating)}(?:[；;，,。]\s*|\s+)?",
                "",
                text,
                count=1,
            )
        text = re.sub(
            r"^(?:投资)?评级\s*[:：]\s*(?:买入|持有|卖出|鎸佹湁|涔板叆|鍗栧嚭)?(?:[；;，,。]\s*|\s+)?",
            "",
            text,
            count=1,
        )

        text = text.strip(" ；;，,。")
        return text[:160]

    def _resolve_stock_card_rating(self, latest_analysis: Optional[Dict]) -> str:
        if not latest_analysis:
            return "待分析"

        final_decision = latest_analysis.get("final_decision")
        if not isinstance(final_decision, dict):
            final_decision = {}

        candidates = [
            final_decision.get("rating"),
            latest_analysis.get("rating"),
            self._extract_rating_from_text(latest_analysis.get("summary", "")),
        ]
        for candidate in candidates:
            normalized = self._normalize_analysis_rating(candidate, default="")
            if normalized:
                return normalized
        return "待分析"

    def _format_currency_text(
        self,
        value: Optional[float],
        *,
        precision: int = 3,
        signed: bool = False,
    ) -> str:
        if value is None:
            return ""

        prefix = ""
        amount = value
        if signed:
            if value > 0:
                prefix = "+"
            elif value < 0:
                prefix = "-"
            amount = abs(value)

        return f"{prefix}¥{amount:,.{precision}f}"

    def _format_percent_text(self, value: Optional[float], *, signed: bool = False) -> str:
        if value is None:
            return ""

        prefix = ""
        amount = value
        if signed:
            if value > 0:
                prefix = "+"
            elif value < 0:
                prefix = "-"
            amount = abs(value)

        return f"{prefix}{amount:.2f}%"

    def _format_analysis_time_text(self, value) -> str:
        if not value:
            return ""

        text = str(value).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, fmt)
                return parsed.strftime("%Y-%m-%d %H:%M")
            except ValueError:
                continue
        return text

    def build_stock_card_view_model(
        self,
        stock: Dict,
        latest_analysis: Optional[Dict] = None,
    ) -> Dict[str, object]:
        latest_analysis = latest_analysis or {}
        final_decision = latest_analysis.get("final_decision")
        if not isinstance(final_decision, dict):
            final_decision = {}

        display_name = (stock.get("name") or stock.get("code") or "").strip()
        cost_price = self._extract_first_number(stock.get("cost_price"), allow_zero=True)
        quantity = stock.get("quantity")
        try:
            quantity_value = int(quantity) if quantity not in (None, "") else None
        except (TypeError, ValueError):
            quantity_value = None

        current_price = self._extract_first_number(latest_analysis.get("current_price"), allow_zero=True)
        if current_price is None:
            stock_info = latest_analysis.get("stock_info")
            if isinstance(stock_info, dict):
                current_price = self._extract_first_number(
                    stock_info.get("current_price"),
                    allow_zero=True,
                )

        rating = self._resolve_stock_card_rating(latest_analysis)
        summary_candidates = [
            latest_analysis.get("summary", ""),
            final_decision.get("operation_advice"),
            final_decision.get("advice"),
            final_decision.get("summary"),
        ]
        summary_text = ""
        for candidate in summary_candidates:
            summary_text = self._sanitize_card_summary(candidate, rating=rating)
            if summary_text:
                break

        pnl_amount = None
        pnl_percent = None
        if (
            latest_analysis
            and current_price is not None
            and cost_price is not None
            and quantity_value
        ):
            pnl_amount = (current_price - cost_price) * quantity_value
            if cost_price:
                pnl_percent = ((current_price - cost_price) / cost_price) * 100

        return {
            "display_name": display_name,
            "cost_text": self._format_currency_text(cost_price, precision=3) if cost_price is not None else "",
            "quantity_text": f"{quantity_value}股" if quantity_value is not None else "",
            "pnl_amount_text": self._format_currency_text(pnl_amount, precision=2, signed=True) if pnl_amount is not None else "",
            "pnl_percent_text": self._format_percent_text(pnl_percent, signed=True) if pnl_percent is not None else "",
            "rating": rating,
            "analysis_time_text": self._format_analysis_time_text(
                latest_analysis.get("analysis_time") or latest_analysis.get("analysis_date")
            ),
            "summary_text": summary_text,
            "note_text": str(stock.get("note") or "").strip(),
            "auto_monitor": bool(stock.get("auto_monitor", True)),
        }

    def _build_analysis_payload(
        self,
        stock_info: Dict,
        final_decision: Dict,
        agents_results: Optional[Dict] = None,
        discussion_result: Optional[str] = None,
        analysis_period: str = "1y",
        analysis_source: str = "portfolio_batch_analysis",
    ) -> Dict:
        """统一构建持仓分析历史落库数据。"""
        rating = self._normalize_analysis_rating(final_decision.get("rating"), default="持有")
        confidence = self._extract_confidence(final_decision.get("confidence_level", 5.0))
        current_price = self._extract_first_number(stock_info.get("current_price"), allow_zero=True) or 0.0
        target_price = self._extract_first_number(final_decision.get("target_price"))
        entry_min, entry_max = self._extract_entry_range(final_decision.get("entry_range"))
        take_profit = self._extract_first_number(final_decision.get("take_profit"))
        stop_loss = self._extract_first_number(final_decision.get("stop_loss"))
        summary = self._extract_analysis_summary(final_decision)

        return {
            "rating": rating,
            "confidence": confidence,
            "current_price": current_price,
            "target_price": target_price,
            "entry_min": entry_min,
            "entry_max": entry_max,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "summary": summary,
            "stock_info": stock_info,
            "agents_results": agents_results or {},
            "discussion_result": discussion_result or "",
            "final_decision": final_decision,
            "analysis_period": analysis_period,
            "analysis_source": analysis_source,
            "has_full_report": bool(stock_info or agents_results or discussion_result or final_decision),
        }

    def _build_smart_monitor_task_data(self, stock: Dict, existing_task: Optional[Dict] = None) -> Dict:
        """构建 AI 盯盘任务数据。"""
        quantity = stock.get("quantity") or 0
        cost_price = stock.get("cost_price") or 0
        has_position = 1 if quantity > 0 and cost_price > 0 else 0
        existing_task = existing_task or {}

        return {
            "task_name": existing_task.get("task_name") or f"{stock.get('name', stock['code'])}盯盘",
            "stock_code": stock["code"],
            "stock_name": stock.get("name"),
            "enabled": existing_task.get("enabled", 0),
            "check_interval": existing_task.get(
                "check_interval",
                self.get_default_smart_monitor_check_interval(),
            ),
            "trading_hours_only": existing_task.get("trading_hours_only", 1),
            "position_size_pct": existing_task.get("position_size_pct", 20),
            "stop_loss_pct": existing_task.get("stop_loss_pct", 5),
            "take_profit_pct": existing_task.get("take_profit_pct", 10),
            "notify_email": existing_task.get("notify_email"),
            "notify_webhook": existing_task.get("notify_webhook"),
            "has_position": has_position,
            "position_cost": float(cost_price) if cost_price else 0,
            "position_quantity": int(quantity) if quantity else 0,
            "position_date": existing_task.get("position_date") or datetime.now().strftime("%Y-%m-%d"),
            "managed_by_portfolio": 1,
        }

    def _build_realtime_monitor_payload(self, stock: Dict, latest_analysis: Dict) -> Optional[Dict]:
        """根据最新持仓分析构建实时监测配置。"""
        if not latest_analysis:
            return None

        entry_min = latest_analysis.get("entry_min")
        entry_max = latest_analysis.get("entry_max")
        take_profit = latest_analysis.get("take_profit")
        stop_loss = latest_analysis.get("stop_loss")

        if not all(v is not None for v in (entry_min, entry_max, take_profit, stop_loss)):
            return None

        existing = self.realtime_monitor_db.get_monitor_by_code(stock["code"])
        return {
            "code": stock["code"],
            "name": stock.get("name", stock["code"]),
            "rating": latest_analysis.get("rating", "持有"),
            "entry_min": float(entry_min),
            "entry_max": float(entry_max),
            "take_profit": float(take_profit),
            "stop_loss": float(stop_loss),
            "check_interval": (
                existing.get("check_interval", self.get_default_realtime_monitor_check_interval())
                if existing
                else self.get_default_realtime_monitor_check_interval()
            ),
            "notification_enabled": existing.get("notification_enabled", True) if existing else True,
            "trading_hours_only": existing.get("trading_hours_only", True) if existing else True,
            "managed_by_portfolio": True,
        }

    def _sync_stock_to_smart_monitor(self, stock: Dict) -> bool:
        """同步单只持仓到 AI 盯盘任务。"""
        if not stock:
            return False
        result = self.lifecycle_service.sync_position(stock_id=stock["id"])
        return bool(result.get("ai_tasks_upserted"))

    def sync_portfolio_to_smart_monitor(self) -> Dict[str, int]:
        """同步所有持仓到 AI 盯盘。"""
        synced = 0
        failed = 0
        for stock in self.get_all_stocks(auto_monitor_only=False):
            try:
                result = self.lifecycle_service.sync_position(stock_id=stock["id"])
                if result.get("ai_tasks_upserted"):
                    synced += 1
            except Exception as e:
                failed += 1
                print(f"[ERROR] 同步 AI盯盘任务失败 ({stock['code']}): {e}")
        return {"synced": synced, "failed": failed, "total": synced + failed}

    def sync_latest_analysis_to_realtime_monitor(self, codes: Optional[List[str]] = None) -> Dict[str, int]:
        """将最新持仓分析结果同步到实时监测。"""
        added = 0
        failed = 0
        stocks = self.db.get_all_stocks(auto_monitor_only=False)
        if codes:
            code_set = {self._normalize_stock_code(code) for code in codes if code}
            stocks = [stock for stock in stocks if stock["code"] in code_set]
        for stock in stocks:
            try:
                result = self.lifecycle_service.sync_position(stock_id=stock["id"])
                if result.get("price_alerts_upserted"):
                    added += int(result.get("price_alerts_upserted", 0))
            except Exception as e:
                failed += 1
                print(f"[ERROR] 同步实时监测失败 ({stock['code']}): {e}")
        return {"added": added, "updated": 0, "failed": failed, "total": added + failed}

    def remove_managed_integrations_for_code(self, code: str) -> Dict[str, int]:
        """移除指定股票的持仓托管下游记录。"""
        normalized_code = self._normalize_stock_code(code)
        smart_deleted = 0
        monitor_deleted = 0
        for stock in self.db.get_stocks_by_code(normalized_code):
            deleted = self.lifecycle_service._delete_managed_items_for_position(stock)
            smart_deleted += deleted["ai_task_deleted"]
            monitor_deleted += deleted["price_alert_deleted"]
        return {
            "smart_monitor_deleted": smart_deleted,
            "realtime_monitor_deleted": monitor_deleted,
        }

    def cleanup_managed_integrations(self) -> Dict[str, int]:
        """清理已经不再受持仓托管的下游记录。"""
        active_codes = {stock["code"] for stock in self.get_all_stocks(auto_monitor_only=False)}
        cleaned_smart = 0
        cleaned_monitor = 0

        for task in self.smart_monitor_db.get_monitor_tasks(enabled_only=False):
            if task.get("managed_by_portfolio") and task["stock_code"] not in active_codes:
                if self.smart_monitor_db.delete_monitor_task_by_code(
                    task["stock_code"],
                    managed_only=True,
                    account_name=task.get("account_name"),
                    portfolio_stock_id=task.get("portfolio_stock_id"),
                ):
                    cleaned_smart += 1

        for monitor in self.realtime_monitor_db.get_monitored_stocks():
            if monitor.get("managed_by_portfolio") and monitor["symbol"] not in active_codes:
                if self.realtime_monitor_db.remove_monitor_by_code(
                    monitor["symbol"],
                    managed_only=True,
                    account_name=monitor.get("account_name"),
                    portfolio_stock_id=monitor.get("portfolio_stock_id"),
                ):
                    cleaned_monitor += 1

        return {
            "smart_monitor_deleted": cleaned_smart,
            "realtime_monitor_deleted": cleaned_monitor,
        }

    def reconcile_portfolio_integrations(self) -> Dict[str, Dict[str, int]]:
        """执行持仓下游联动对账。"""
        if not self._integrations_reconcile_pending:
            return {
                "smart_monitor_sync": {"synced": 0, "failed": 0, "total": 0},
                "realtime_monitor_sync": {"added": 0, "updated": 0, "failed": 0, "total": 0},
                "cleanup": {"smart_monitor_deleted": 0, "realtime_monitor_deleted": 0},
            }

        smart_sync_result = {"synced": 0, "failed": 0, "total": 0}
        monitor_sync_result = {"added": 0, "updated": 0, "failed": 0, "total": 0}
        cleanup_result = {"smart_monitor_deleted": 0, "realtime_monitor_deleted": 0}

        try:
            smart_sync_result = self.sync_portfolio_to_smart_monitor()
        except Exception as e:
            smart_sync_result = {"synced": 0, "failed": 1, "total": 1, "error": str(e)}
            print(f"[ERROR] 持仓联动同步 AI盯盘失败: {e}")

        try:
            monitor_sync_result = self.sync_latest_analysis_to_realtime_monitor()
        except Exception as e:
            monitor_sync_result = {"added": 0, "updated": 0, "failed": 1, "total": 1, "error": str(e)}
            print(f"[ERROR] 持仓联动同步实时监测失败: {e}")

        try:
            cleanup_result = self.cleanup_managed_integrations()
        except Exception as e:
            cleanup_result = {"smart_monitor_deleted": 0, "realtime_monitor_deleted": 0, "error": str(e)}
            print(f"[ERROR] 持仓联动清理失败: {e}")
        finally:
            # Avoid repeated page-load error loops; later data mutations will reopen reconciliation.
            self._integrations_reconcile_pending = False

        return {
            "smart_monitor_sync": smart_sync_result,
            "realtime_monitor_sync": monitor_sync_result,
            "cleanup": cleanup_result,
        }

    def persist_analysis_results(
        self,
        analysis_results: Dict,
        sync_realtime_monitor: bool = True,
        analysis_source: str = "portfolio_batch_analysis",
        analysis_period: str = "1y",
    ) -> Dict:
        """保存持仓分析结果，并按需同步到实时监测。"""
        saved_ids = self.save_analysis_results(
            analysis_results,
            analysis_source=analysis_source,
            analysis_period=analysis_period,
        )
        sync_result = None

        if sync_realtime_monitor:
            codes = [item.get("code") for item in analysis_results.get("results", []) if item.get("code")]
            sync_result = self.sync_latest_analysis_to_realtime_monitor(codes=codes)

        affected_accounts = set()
        for item in analysis_results.get("results", []):
            code = item.get("code")
            if not code:
                continue
            for stock in self.db.get_stocks_by_code(code):
                affected_accounts.add(stock.get("account_name", "默认账户"))
        if affected_accounts:
            for account in affected_accounts:
                self.capture_daily_snapshot(account_name=account, source="analysis")

        return {"saved_ids": saved_ids, "sync_result": sync_result}

    def persist_single_analysis_result(
        self,
        code: str,
        analysis_result: Dict,
        *,
        sync_realtime_monitor: bool = True,
        analysis_source: str = "portfolio_batch_analysis",
        analysis_period: str = "1y",
    ) -> Dict:
        """Persist one completed analysis result immediately."""
        wrapped_results = {
            "success": bool(analysis_result.get("success", True)),
            "results": [
                {
                    "code": code,
                    "result": analysis_result,
                }
            ],
        }
        return self.persist_analysis_results(
            wrapped_results,
            sync_realtime_monitor=sync_realtime_monitor,
            analysis_source=analysis_source,
            analysis_period=analysis_period,
        )
    
    # ==================== 单只股票分析 ====================
    
    def analyze_single_stock(self, stock_code: str, period="1y",
                            selected_agents: List[str] = None,
                            model: str = None,
                            lightweight_model: str = None,
                            reasoning_model: str = None) -> Dict:
        """
        分析单只股票（复用app.py中的分析逻辑）
        
        Args:
            stock_code: 股票代码
            period: 数据周期
            selected_agents: 选中的分析师列表
            
        Returns:
            分析结果字典
        """
        stock_code = self._normalize_stock_code(stock_code)
        print(f"\n{'='*60}")
        print(f"开始分析股票: {stock_code}")
        print(f"{'='*60}\n")
        
        try:
            # 导入app.py中的分析函数
            from batch_analysis_service import analyze_single_stock_for_batch
            
            # 构建分析师配置
            if selected_agents is None:
                enabled_analysts_config = {
                    'technical': True,
                    'fundamental': True,
                    'fund_flow': True,
                    'risk': True,
                    'sentiment': False,
                    'news': False
                }
            else:
                enabled_analysts_config = {
                    'technical': 'technical' in selected_agents,
                    'fundamental': 'fundamental' in selected_agents,
                    'fund_flow': 'fund_flow' in selected_agents,
                    'risk': 'risk' in selected_agents,
                    'sentiment': 'sentiment' in selected_agents,
                    'news': 'news' in selected_agents
                }
            
            forced_model = model
            effective_lightweight_model = lightweight_model
            effective_reasoning_model = reasoning_model

            if forced_model is None:
                if effective_lightweight_model is None and effective_reasoning_model is None:
                    forced_model = self.model
                    if forced_model is None:
                        effective_lightweight_model = self.lightweight_model
                        effective_reasoning_model = self.reasoning_model
                else:
                    if effective_lightweight_model is None:
                        effective_lightweight_model = self.lightweight_model
                    if effective_reasoning_model is None:
                        effective_reasoning_model = self.reasoning_model

            # 调用首页的分析函数
            result = analyze_single_stock_for_batch(
                symbol=stock_code,
                period=period,
                enabled_analysts_config=enabled_analysts_config,
                selected_model=forced_model,
                selected_lightweight_model=effective_lightweight_model,
                selected_reasoning_model=effective_reasoning_model,
                save_to_global_history=False,
            )
            
            # 检查结果
            if not result.get("success", False):
                error_msg = result.get("error", "未知错误")
                print(f"\n[ERROR] 分析失败: {error_msg}")
                return {"success": False, "error": error_msg}
            
            print(f"\n{'='*60}")
            print(f"分析完成！")
            print(f"{'='*60}\n")
            
            return result
            
        except Exception as e:
            print(f"\n[ERROR] 分析失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    # ==================== 批量分析 ====================
    
    def batch_analyze_sequential(self, stock_codes: List[str], period="1y",
                                 selected_agents: List[str] = None,
                                 progress_callback=None,
                                 result_callback: Optional[Callable[[str, Dict], None]] = None,
                                 model: str = None,
                                 lightweight_model: str = None,
                                 reasoning_model: str = None) -> Dict:
        """
        顺序批量分析（逐只分析）
        
        Args:
            stock_codes: 股票代码列表
            period: 数据周期
            selected_agents: 选中的分析师列表
            progress_callback: 进度回调函数 callback(current, total, code, status)
            
        Returns:
            批量分析结果字典
        """
        print(f"\n{'='*60}")
        print(f"开始批量分析 (顺序模式): {len(stock_codes)}只股票")
        print(f"{'='*60}\n")
        
        start_time = time.time()
        results = []
        failed = []
        
        for i, code in enumerate(stock_codes, 1):
            print(f"\n--- 分析进度: {i}/{len(stock_codes)} ---")
            
            if progress_callback:
                progress_callback(i, len(stock_codes), code, "analyzing")
            
            try:
                result = self.analyze_single_stock(
                    code,
                    period,
                    selected_agents,
                    model=model,
                    lightweight_model=lightweight_model,
                    reasoning_model=reasoning_model,
                )
                
                if result.get("success"):
                    results.append({
                        "code": code,
                        "result": result
                    })
                    if result_callback:
                        result_callback(code, result)
                    if progress_callback:
                        progress_callback(i, len(stock_codes), code, "success")
                else:
                    failed.append({
                        "code": code,
                        "error": result.get("error", "未知错误")
                    })
                    if progress_callback:
                        progress_callback(i, len(stock_codes), code, "failed")
                    
            except Exception as e:
                print(f"[ERROR] 股票 {code} 分析失败: {str(e)}")
                failed.append({
                    "code": code,
                    "error": str(e)
                })
                if progress_callback:
                    progress_callback(i, len(stock_codes), code, "error")
        
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*60}")
        print(f"批量分析完成！")
        print(f"成功: {len(results)}只, 失败: {len(failed)}只, 耗时: {elapsed_time:.1f}秒")
        print(f"{'='*60}\n")
        
        return {
            "success": True,
            "mode": "sequential",
            "total": len(stock_codes),
            "succeeded": len(results),
            "failed": len(failed),
            "results": results,
            "failed_stocks": failed,
            "elapsed_time": elapsed_time
        }
    
    def batch_analyze_parallel(self, stock_codes: List[str], period="1y",
                               selected_agents: List[str] = None,
                               max_workers: int = 3,
                               progress_callback=None,
                               result_callback: Optional[Callable[[str, Dict], None]] = None,
                               model: str = None,
                               lightweight_model: str = None,
                               reasoning_model: str = None) -> Dict:
        """
        并行批量分析（多线程）
        
        Args:
            stock_codes: 股票代码列表
            period: 数据周期
            selected_agents: 选中的分析师列表
            max_workers: 最大并发数（默认3）
            progress_callback: 进度回调函数
            
        Returns:
            批量分析结果字典
        """
        print(f"\n{'='*60}")
        print(f"开始批量分析 (并行模式): {len(stock_codes)}只股票, 并发数: {max_workers}")
        print(f"{'='*60}\n")
        
        start_time = time.time()
        results = []
        failed = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_code = {
                executor.submit(
                    self.analyze_single_stock,
                    code,
                    period,
                    selected_agents,
                    model,
                    lightweight_model,
                    reasoning_model,
                ): code
                for code in stock_codes
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                completed += 1
                
                try:
                    result = future.result()
                    
                    if result.get("success"):
                        results.append({
                            "code": code,
                            "result": result
                        })
                        if result_callback:
                            result_callback(code, result)
                        print(f"\n[{completed}/{len(stock_codes)}] {code} 分析完成")
                        if progress_callback:
                            progress_callback(completed, len(stock_codes), code, "success")
                    else:
                        failed.append({
                            "code": code,
                            "error": result.get("error", "未知错误")
                        })
                        print(f"\n[{completed}/{len(stock_codes)}] {code} 分析失败: {result.get('error')}")
                        if progress_callback:
                            progress_callback(completed, len(stock_codes), code, "failed")
                        
                except Exception as e:
                    failed.append({
                        "code": code,
                        "error": str(e)
                    })
                    print(f"\n[{completed}/{len(stock_codes)}] {code} 分析异常: {str(e)}")
                    if progress_callback:
                        progress_callback(completed, len(stock_codes), code, "error")
        
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*60}")
        print(f"批量分析完成！")
        print(f"成功: {len(results)}只, 失败: {len(failed)}只, 耗时: {elapsed_time:.1f}秒")
        print(f"{'='*60}\n")
        
        return {
            "success": True,
            "mode": "parallel",
            "total": len(stock_codes),
            "succeeded": len(results),
            "failed": len(failed),
            "results": results,
            "failed_stocks": failed,
            "elapsed_time": elapsed_time
        }
    
    def batch_analyze_portfolio(self, mode="sequential", period="1y",
                                selected_agents: List[str] = None,
                                max_workers: int = 3,
                                progress_callback=None,
                                result_callback: Optional[Callable[[str, Dict], None]] = None,
                                model: str = None,
                                lightweight_model: str = None,
                                reasoning_model: str = None) -> Dict:
        """
        批量分析所有持仓股票
        
        Args:
            mode: 分析模式 ("sequential" 或 "parallel")
            period: 数据周期
            selected_agents: 选中的分析师列表
            max_workers: 并行模式下的最大并发数（默认3）
            progress_callback: 进度回调函数
            
        Returns:
            批量分析结果字典
        """
        # 获取所有持仓股票
        stocks = self.get_all_stocks()
        
        if not stocks:
            return {
                "success": False,
                "error": "没有持仓股票"
            }
        
        stock_codes = [stock['code'] for stock in stocks]
        
        # 根据模式选择分析方法
        if mode == "parallel":
            return self.batch_analyze_parallel(
                stock_codes,
                period,
                selected_agents,
                max_workers,
                progress_callback,
                result_callback,
                model=model,
                lightweight_model=lightweight_model,
                reasoning_model=reasoning_model,
            )
        else:
            return self.batch_analyze_sequential(
                stock_codes,
                period,
                selected_agents,
                progress_callback,
                result_callback,
                model=model,
                lightweight_model=lightweight_model,
                reasoning_model=reasoning_model,
            )
    
    # ==================== 分析结果保存 ====================
    
    def save_analysis_results(
        self,
        analysis_results: Dict,
        analysis_source: str = "portfolio_batch_analysis",
        analysis_period: str = "1y",
    ) -> List[int]:
        """
        保存批量分析结果到数据库
        
        Args:
            analysis_results: 批量分析结果字典
            
        Returns:
            保存的分析记录ID列表
        """
        saved_ids = []
        
        if not analysis_results.get("success"):
            print("[WARN] 分析未成功，跳过保存")
            return saved_ids
        
        for item in analysis_results.get("results", []):
            code = item.get("code")
            result = item.get("result", {})
            
            # 获取持仓股票ID
            stocks = self.db.get_stocks_by_code(code)
            if not stocks:
                print(f"[WARN] 未找到持仓股票: {code}，跳过保存")
                continue
            stock = stocks[0]
            if not stock:
                print(f"[WARN] 未找到持仓股票: {code}，跳过保存")
                continue
            
            stock_id = stock['id']
            
            # 提取分析结果关键信息
            final_decision = result.get("final_decision", {})
            stock_info = result.get("stock_info", {})
            payload = self._build_analysis_payload(
                stock_info,
                final_decision,
                agents_results=result.get("agents_results", {}),
                discussion_result=result.get("discussion_result", ""),
                analysis_period=analysis_period,
                analysis_source=analysis_source,
            )
            
            try:
                # 保存到数据库
                analysis_id = self.db.save_analysis(
                    stock_id,
                    payload["rating"],
                    payload["confidence"],
                    payload["current_price"],
                    payload["target_price"],
                    payload["entry_min"],
                    payload["entry_max"],
                    payload["take_profit"],
                    payload["stop_loss"],
                    payload["summary"],
                    stock_info=payload["stock_info"],
                    agents_results=payload["agents_results"],
                    discussion_result=payload["discussion_result"],
                    final_decision=payload["final_decision"],
                    analysis_period=payload["analysis_period"],
                    analysis_source=payload["analysis_source"],
                    has_full_report=payload["has_full_report"],
                )
                saved_ids.append(analysis_id)
                
            except Exception as e:
                print(f"[ERROR] 保存分析结果失败 ({code}): {str(e)}")
        
        print(f"\n[OK] 保存分析结果: {len(saved_ids)}条记录")
        return saved_ids
    
    # ==================== 分析历史查询 ====================
    
    def get_analysis_history(self, stock_id: int, limit: int = 10) -> List[Dict]:
        """获取股票分析历史"""
        return self.db.get_analysis_history(stock_id, limit)

    def delete_analysis_record(self, analysis_id: int) -> Tuple[bool, str]:
        """删除单条分析历史记录。"""
        try:
            deleted = self.db.delete_analysis_record(analysis_id)
            if deleted:
                return True, "分析历史记录已删除"
            return False, "未找到对应的分析历史记录"
        except Exception as e:
            return False, f"删除分析历史记录失败: {e}"
    
    def get_latest_analysis(self, stock_id: int) -> Optional[Dict]:
        """获取最新一次分析"""
        return self.db.get_latest_analysis(stock_id)
    
    def get_all_latest_analysis(self) -> List[Dict]:
        """获取所有持仓股票的最新分析"""
        return self.db.get_all_latest_analysis()
    
    def get_rating_changes(self, stock_id: int, days: int = 30) -> List[Tuple]:
        """获取评级变化"""
        return self.db.get_rating_changes(stock_id, days)


    
    # ==================== 风险评估 ====================
    
    def calculate_portfolio_risk(self, account_name: Optional[str] = None) -> Dict:
        """
        计算持仓风险指标评估，包括集中度与量化风险指标。
        
        Args:
            account_name: 指定账户名称进行过滤，若为None则计算所有账户。
            
        Returns:
            Dict: 包含风险指标评估结果的字典
        """
        stocks = self._filter_stocks_for_account(self.get_all_stocks(), account_name)
        if not stocks:
            return {"status": "error", "message": "没有持仓记录，无法评估风险"}

        snapshot = self._build_portfolio_snapshot_payload(stocks, account_name)
        total_market_value = snapshot["total_market_value"]
        total_cost_value = snapshot["total_cost_value"]
        stock_values = []
        industry_values: Dict[str, float] = {}

        if total_market_value == 0:
            return {"status": "error", "message": "持仓总市值为空，请更新价格或数量"}

        for holding in snapshot["holdings"]:
            stock_values.append(
                {
                    "code": holding["code"],
                    "name": holding["name"],
                    "market_value": holding["market_value"],
                    "cost_value": holding["cost_value"],
                    "pnl": holding["pnl"],
                    "pnl_pct": holding["pnl_pct"],
                    "industry": holding["industry"],
                    "weight": (holding["market_value"] / total_market_value) if total_market_value > 0 else 0.0,
                }
            )
            industry = holding["industry"] or "未知行业"
            industry_values[industry] = industry_values.get(industry, 0.0) + holding["market_value"]

        stock_values.sort(key=lambda x: x["weight"], reverse=True)
        
        industry_distribution = []
        for ind, val in industry_values.items():
            industry_distribution.append({
                "industry": ind,
                "market_value": val,
                "weight": val / total_market_value
            })
            
        industry_distribution.sort(key=lambda x: x["weight"], reverse=True)
        
        # 风险评估结果
        risk_warnings = []
        high_concentration = False
        
        if stock_values and stock_values[0]["weight"] > 0.3:
            risk_warnings.append(f"单票超载预警：{stock_values[0]['name']} 占比达到 {stock_values[0]['weight']*100:.1f}%，超过安全线(30%)。")
            high_concentration = True
            
        if industry_distribution and industry_distribution[0]["weight"] > 0.4:
            risk_warnings.append(f"行业集中度预警：{industry_distribution[0]['industry']} 占比达到 {industry_distribution[0]['weight']*100:.1f}%，超过安全线(40%)。")
            high_concentration = True
            
        if not risk_warnings:
            risk_warnings.append("仓位结构健康，未发现明显集中度风险。")
            
        total_pnl = total_market_value - total_cost_value
        total_pnl_pct = (total_pnl / total_cost_value) if total_cost_value > 0 else 0.0
        history_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        return_series = self.build_portfolio_return_series(
            account_name=account_name,
            start_date=history_start,
            end_date=datetime.now().strftime("%Y-%m-%d"),
            prefer_snapshots=True,
        )
        quant_metrics = self._calculate_quantitative_risk_metrics(
            return_series,
            start_date=history_start,
            end_date=datetime.now().strftime("%Y-%m-%d"),
        )

        return {
            "status": "success",
            "total_market_value": total_market_value,
            "total_cost_value": total_cost_value,
            "total_pnl": total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "stock_distribution": stock_values,
            "industry_distribution": industry_distribution,
            "high_concentration": high_concentration,
            "risk_warnings": risk_warnings,
            "annual_volatility": quant_metrics.get("annual_volatility"),
            "beta_hs300": quant_metrics.get("beta_hs300"),
            "sharpe_ratio": quant_metrics.get("sharpe_ratio"),
            "annualized_return": quant_metrics.get("annualized_return"),
            "risk_free_rate_annual": quant_metrics.get("risk_free_rate_annual"),
            "benchmark_label": quant_metrics.get("benchmark_label"),
            "metric_warnings": quant_metrics.get("metric_warnings", []),
            "data_coverage": quant_metrics.get("data_coverage", {}),
        }
# 创建全局实例
portfolio_manager = PortfolioManager()


if __name__ == "__main__":
    # 测试代码
    print("="*60)
    print("持仓管理器测试")
    print("="*60)
    
    manager = PortfolioManager()
    
    # 测试添加持仓
    success, msg, stock_id = manager.add_stock("000001", "平安银行", 12.5, 1000, "测试持仓")
    print(f"\n添加持仓: {msg}")
    
    # 测试获取所有持仓
    stocks = manager.get_all_stocks()
    print(f"\n持仓数量: {len(stocks)}")
    for stock in stocks:
        print(f"  {stock['code']} {stock['name']} - 成本:{stock['cost_price']}, 数量:{stock['quantity']}")
    
    print("\n[OK] 持仓管理器测试完成")

