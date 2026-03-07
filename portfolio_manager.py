"""
持仓管理器模块

提供持仓股票管理和批量分析功能
"""

import time
import re
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# 导入必要的模块
from portfolio_db import portfolio_db
from monitor_db import monitor_db as realtime_monitor_db
from smart_monitor_db import SmartMonitorDB


class PortfolioManager:
    """持仓管理器类"""

    DEFAULT_SMART_MONITOR_CHECK_INTERVAL = 300
    DEFAULT_REALTIME_MONITOR_CHECK_INTERVAL = 60
    
    def __init__(self, model=None, lightweight_model=None, reasoning_model=None,
                 portfolio_store=None,
                 realtime_monitor_store=None, smart_monitor_store=None):
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
                  auto_monitor: bool = True) -> Tuple[bool, str, Optional[int]]:
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
            existing = self.db.get_stock_by_code(code)
            if existing:
                return False, f"股票代码 {code} 已存在", None
            
            # 添加到数据库
            stock_id = self.db.add_stock(code, final_name, cost_price, quantity, note, auto_monitor)
            warning = ""
            if auto_monitor:
                try:
                    created_stock = self.db.get_stock(stock_id)
                    if created_stock:
                        self._sync_stock_to_smart_monitor(created_stock)
                except Exception as e:
                    warning = f"；AI盯盘同步失败: {e}"
            return True, f"添加持仓股票成功: {code} {final_name}{warning}", stock_id
            
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
                updated_stock = self.db.get_stock(stock_id)
                warning = ""

                if old_code != updated_stock["code"]:
                    self.remove_managed_integrations_for_code(old_code)

                try:
                    if updated_stock.get("auto_monitor"):
                        self._sync_stock_to_smart_monitor(updated_stock)
                    else:
                        self.remove_managed_integrations_for_code(updated_stock["code"])
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
                warning = ""
                try:
                    self.remove_managed_integrations_for_code(existing["code"])
                except Exception as e:
                    warning = f"（下游清理失败: {e}）"
                return True, f"删除成功{warning}"
            else:
                return False, f"未找到股票ID: {stock_id}"
        except Exception as e:
            return False, f"删除失败: {str(e)}"
    
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

    def _extract_analysis_summary(self, final_decision: Dict) -> str:
        """提取分析摘要。"""
        for key in ("operation_advice", "advice", "summary", "decision_text"):
            value = final_decision.get(key)
            if value:
                return str(value)[:500]
        return ""

    def _build_analysis_payload(self, stock_info: Dict, final_decision: Dict) -> Dict:
        """统一构建持仓分析历史落库数据。"""
        rating = str(final_decision.get("rating", "持有")).strip() or "持有"
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
        }

    def _sanitize_summary_text(self, value: str) -> str:
        if not value:
            return ""

        text = str(value).strip()
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
        rating = str(final_decision.get("rating", "持有")).strip() or "持有"
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

    def _build_analysis_payload(
        self,
        stock_info: Dict,
        final_decision: Dict,
        agents_results: Optional[Dict] = None,
        discussion_result: Optional[str] = None,
        analysis_period: str = "1y",
        analysis_source: str = "portfolio_batch_analysis",
    ) -> Dict:
        """缁熶竴鏋勫缓鎸佷粨鍒嗘瀽鍘嗗彶钀藉簱鏁版嵁銆?"""
        rating = str(final_decision.get("rating", "鎸佹湁")).strip() or "鎸佹湁"
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
            "check_interval": existing_task.get("check_interval", self.DEFAULT_SMART_MONITOR_CHECK_INTERVAL),
            "auto_trade": existing_task.get("auto_trade", 0),
            "trading_hours_only": existing_task.get("trading_hours_only", 1),
            "position_size_pct": existing_task.get("position_size_pct", 20),
            "stop_loss_pct": existing_task.get("stop_loss_pct", 5),
            "take_profit_pct": existing_task.get("take_profit_pct", 10),
            "qmt_account_id": existing_task.get("qmt_account_id"),
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
            "check_interval": existing.get("check_interval", self.DEFAULT_REALTIME_MONITOR_CHECK_INTERVAL) if existing else self.DEFAULT_REALTIME_MONITOR_CHECK_INTERVAL,
            "notification_enabled": existing.get("notification_enabled", True) if existing else True,
            "trading_hours_only": existing.get("trading_hours_only", True) if existing else True,
            "managed_by_portfolio": True,
        }

    def _sync_stock_to_smart_monitor(self, stock: Dict) -> bool:
        """同步单只持仓到 AI 盯盘任务。"""
        if not stock or not stock.get("auto_monitor"):
            return False

        existing_task = self.smart_monitor_db.get_monitor_task_by_code(stock["code"], managed_only=True)
        task_data = self._build_smart_monitor_task_data(stock, existing_task)
        self.smart_monitor_db.upsert_monitor_task(task_data)
        return True

    def sync_portfolio_to_smart_monitor(self) -> Dict[str, int]:
        """同步所有启用自动监测的持仓到 AI 盯盘。"""
        synced = 0
        failed = 0

        for stock in self.get_all_stocks(auto_monitor_only=True):
            try:
                if self._sync_stock_to_smart_monitor(stock):
                    synced += 1
            except Exception as e:
                failed += 1
                print(f"[ERROR] 同步 AI盯盘任务失败 ({stock['code']}): {e}")

        return {"synced": synced, "failed": failed, "total": synced + failed}

    def sync_latest_analysis_to_realtime_monitor(self, codes: Optional[List[str]] = None) -> Dict[str, int]:
        """将最新持仓分析结果同步到实时监测。"""
        monitors_to_sync = []
        stocks = self.get_all_stocks(auto_monitor_only=True)
        if codes:
            code_set = {self._normalize_stock_code(code) for code in codes}
            stocks = [stock for stock in stocks if stock["code"] in code_set]

        for stock in stocks:
            latest_analysis = self.db.get_latest_analysis(stock["id"])
            monitor_payload = self._build_realtime_monitor_payload(stock, latest_analysis)
            if monitor_payload:
                monitors_to_sync.append(monitor_payload)

        if not monitors_to_sync:
            return {"added": 0, "updated": 0, "failed": 0, "total": 0}

        return self.realtime_monitor_db.batch_add_or_update_monitors(monitors_to_sync)

    def remove_managed_integrations_for_code(self, code: str) -> Dict[str, int]:
        """移除指定股票的持仓托管下游记录。"""
        normalized_code = self._normalize_stock_code(code)
        smart_deleted = 1 if self.smart_monitor_db.delete_monitor_task_by_code(normalized_code, managed_only=True) else 0
        monitor_deleted = 1 if self.realtime_monitor_db.remove_monitor_by_code(normalized_code, managed_only=True) else 0
        return {
            "smart_monitor_deleted": smart_deleted,
            "realtime_monitor_deleted": monitor_deleted,
        }

    def cleanup_managed_integrations(self) -> Dict[str, int]:
        """清理已经不再受持仓托管的下游记录。"""
        active_codes = {stock["code"] for stock in self.get_all_stocks(auto_monitor_only=True)}
        cleaned_smart = 0
        cleaned_monitor = 0

        for task in self.smart_monitor_db.get_monitor_tasks(enabled_only=False):
            if task.get("managed_by_portfolio") and task["stock_code"] not in active_codes:
                if self.smart_monitor_db.delete_monitor_task_by_code(task["stock_code"], managed_only=True):
                    cleaned_smart += 1

        for monitor in self.realtime_monitor_db.get_monitored_stocks():
            if monitor.get("managed_by_portfolio") and monitor["symbol"] not in active_codes:
                if self.realtime_monitor_db.remove_monitor_by_code(monitor["symbol"], managed_only=True):
                    cleaned_monitor += 1

        return {
            "smart_monitor_deleted": cleaned_smart,
            "realtime_monitor_deleted": cleaned_monitor,
        }

    def reconcile_portfolio_integrations(self) -> Dict[str, Dict[str, int]]:
        """执行持仓下游联动对账。"""
        smart_sync_result = self.sync_portfolio_to_smart_monitor()
        monitor_sync_result = self.sync_latest_analysis_to_realtime_monitor()
        cleanup_result = self.cleanup_managed_integrations()

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

        return {"saved_ids": saved_ids, "sync_result": sync_result}
    
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
            from app import analyze_single_stock_for_batch
            
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
            stock = self.db.get_stock_by_code(code)
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
    
    def get_latest_analysis(self, stock_id: int) -> Optional[Dict]:
        """获取最新一次分析"""
        return self.db.get_latest_analysis(stock_id)
    
    def get_all_latest_analysis(self) -> List[Dict]:
        """获取所有持仓股票的最新分析"""
        return self.db.get_all_latest_analysis()
    
    def get_rating_changes(self, stock_id: int, days: int = 30) -> List[Tuple]:
        """获取评级变化"""
        return self.db.get_rating_changes(stock_id, days)


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

