"""
持仓管理器模块

提供持仓股票管理和批量分析功能
"""

import time
import re
from typing import List, Dict, Optional, Tuple, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# 导入必要的模块
from portfolio_db import portfolio_db
import config


class PortfolioManager:
    """持仓管理器类"""
    
    def __init__(self, model=None):
        """
        初始化持仓管理器
        
        Args:
            model: AI模型名称，默认从 .env 的 DEFAULT_MODEL_NAME 读取
        """
        self.model = model or config.DEFAULT_MODEL_NAME
        self.db = portfolio_db

    @staticmethod
    def normalize_stock_code(code: str) -> str:
        """
        统一股票代码格式，避免 A 股 .SH/.SZ 导致识别失败
        """
        if not code:
            return ""

        normalized = str(code).strip().upper()
        if "." in normalized:
            base, suffix = normalized.rsplit(".", 1)
            if suffix in {"SH", "SZ", "HK"}:
                return base.strip()
        return normalized

    @staticmethod
    def _extract_first_float(value) -> Optional[float]:
        """从任意值中提取第一个数字"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)

        numbers = re.findall(r"\d+\.?\d*", str(value))
        if not numbers:
            return None

        try:
            return float(numbers[0])
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_entry_range(entry_range) -> Tuple[Optional[float], Optional[float]]:
        """解析进场区间，兼容字符串/字典/自然语言"""
        if isinstance(entry_range, dict):
            min_val = PortfolioManager._extract_first_float(entry_range.get("min"))
            max_val = PortfolioManager._extract_first_float(entry_range.get("max"))
            if min_val is not None and max_val is not None and max_val > min_val:
                return min_val, max_val

        if entry_range is None:
            return None, None

        text = str(entry_range)
        numbers = re.findall(r"\d+\.?\d*", text)
        if len(numbers) >= 2:
            try:
                first = float(numbers[0])
                second = float(numbers[1])
                if second > first:
                    return first, second
            except (ValueError, TypeError):
                return None, None
        return None, None

    @staticmethod
    def _build_fallback_levels(current_price: float) -> Dict[str, float]:
        """基于当前价生成保守阈值"""
        return {
            "entry_min": round(current_price * 0.98, 2),
            "entry_max": round(current_price * 1.02, 2),
            "take_profit": round(current_price * 1.10, 2),
            "stop_loss": round(current_price * 0.95, 2)
        }

    @staticmethod
    def _extract_position_fields(stock: Dict) -> Tuple[bool, Optional[float], Optional[int]]:
        """从持仓数据中提取持仓快照字段"""
        cost_price = PortfolioManager._extract_first_float(stock.get("cost_price"))
        quantity_raw = stock.get("quantity")
        quantity = None
        try:
            quantity = int(quantity_raw) if quantity_raw is not None else None
        except (TypeError, ValueError):
            quantity = None

        has_position = bool(cost_price and cost_price > 0 and quantity and quantity > 0)
        return has_position, cost_price, quantity

    def _attach_portfolio_source_fields(self, payload: Dict[str, Any], stock: Dict):
        """将持仓来源与持仓字段注入同步载荷"""
        has_position, cost_price, quantity = self._extract_position_fields(stock)
        payload.update({
            "source_type": "portfolio",
            "source_label": "持仓",
            "portfolio_stock_id": stock.get("id"),
            "has_position": has_position,
            "position_cost": round(float(cost_price), 2) if has_position and cost_price else None,
            "position_quantity": quantity if has_position else None,
            "position_updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    def _build_monitor_payload_from_portfolio_stock(self, stock: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """
        为单只持仓构建实时监测同步载荷。
        优先使用最新分析结果，若缺失则以成本价/最新价兜底并标记 needs_review。
        """
        code = self.normalize_stock_code(stock.get("code", ""))
        if not code:
            return None, "股票代码为空"

        latest = self.db.get_latest_analysis(stock.get("id")) if stock.get("id") else None
        entry_min = self._extract_first_float((latest or {}).get("entry_min"))
        entry_max = self._extract_first_float((latest or {}).get("entry_max"))
        take_profit = self._extract_first_float((latest or {}).get("take_profit"))
        stop_loss = self._extract_first_float((latest or {}).get("stop_loss"))
        current_price = self._extract_first_float((latest or {}).get("current_price"))
        rating = (latest or {}).get("rating") or "持有"
        needs_review = False

        levels_complete = (
            entry_min is not None
            and entry_max is not None
            and take_profit is not None
            and stop_loss is not None
            and entry_max > entry_min
            and take_profit > 0
            and stop_loss > 0
        )

        if not levels_complete:
            anchor_price = self._extract_first_float(stock.get("cost_price")) or current_price
            if anchor_price is None or anchor_price <= 0:
                anchor_price = 10.0
            fallback = self._build_fallback_levels(anchor_price)
            entry_min = fallback["entry_min"]
            entry_max = fallback["entry_max"]
            take_profit = fallback["take_profit"]
            stop_loss = fallback["stop_loss"]
            current_price = anchor_price
            needs_review = True
        else:
            entry_min = round(float(entry_min), 2)
            entry_max = round(float(entry_max), 2)
            take_profit = round(float(take_profit), 2)
            stop_loss = round(float(stop_loss), 2)
            if current_price is not None and current_price > 0:
                current_price = round(float(current_price), 2)

        payload = {
            "code": code,
            "name": stock.get("name") or code,
            "rating": rating,
            "entry_min": entry_min,
            "entry_max": entry_max,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "needs_review": needs_review,
            "current_price": current_price,
        }
        self._attach_portfolio_source_fields(payload, stock)
        return payload, None

    def _build_monitor_payload(self, code: str, result: Dict, stock: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """
        构建同步到监测的标准数据。
        先尝试严格解析价格位，失败后按 current_price 兜底并标记 needs_review。
        """
        if not result.get("success"):
            return None, "分析未成功"

        final_decision = result.get("final_decision", {})
        if not isinstance(final_decision, dict):
            final_decision = {}
        stock_info = result.get("stock_info", {}) or {}

        rating = final_decision.get("rating", "持有")
        entry_min, entry_max = self._parse_entry_range(final_decision.get("entry_range"))
        take_profit = self._extract_first_float(final_decision.get("take_profit"))
        stop_loss = self._extract_first_float(final_decision.get("stop_loss"))
        current_price = self._extract_first_float(stock_info.get("current_price"))
        needs_review = False

        levels_complete = (
            entry_min is not None
            and entry_max is not None
            and take_profit is not None
            and stop_loss is not None
            and entry_max > entry_min
            and take_profit > 0
            and stop_loss > 0
        )

        if not levels_complete:
            anchor_price = current_price
            if anchor_price is None or anchor_price <= 0:
                anchor_price = self._extract_first_float(stock.get("cost_price"))
            if anchor_price is None or anchor_price <= 0:
                return None, "关键价格位无法解析且缺少有效 current_price/cost_price，无法兜底"
            fallback = self._build_fallback_levels(anchor_price)
            entry_min = fallback["entry_min"]
            entry_max = fallback["entry_max"]
            take_profit = fallback["take_profit"]
            stop_loss = fallback["stop_loss"]
            current_price = anchor_price
            needs_review = True
        else:
            entry_min = round(float(entry_min), 2)
            entry_max = round(float(entry_max), 2)
            take_profit = round(float(take_profit), 2)
            stop_loss = round(float(stop_loss), 2)
            if current_price is None:
                current_price = self._extract_first_float(stock_info.get("last_price"))

        payload = {
            "code": code,
            "name": stock_info.get("name") or stock.get("name") or code,
            "rating": rating,
            "entry_min": entry_min,
            "entry_max": entry_max,
            "take_profit": take_profit,
            "stop_loss": stop_loss,
            "needs_review": needs_review,
            "current_price": round(float(current_price), 2) if current_price else None
        }
        self._attach_portfolio_source_fields(payload, stock)
        return payload, None

    @staticmethod
    def _build_smart_task_payload(monitor_payload: Dict, include_runtime_defaults: bool = True) -> Dict:
        """将监测价格位映射为 AI 盯盘任务"""
        current_price = monitor_payload.get("current_price")
        stop_loss = monitor_payload.get("stop_loss")
        take_profit = monitor_payload.get("take_profit")
        has_position = bool(monitor_payload.get("has_position"))
        position_cost = PortfolioManager._extract_first_float(monitor_payload.get("position_cost")) or 0
        position_quantity_raw = monitor_payload.get("position_quantity")
        try:
            position_quantity = int(position_quantity_raw) if position_quantity_raw else 0
        except (TypeError, ValueError):
            position_quantity = 0

        stop_loss_pct = 5.0
        take_profit_pct = 10.0
        if (current_price is None or current_price <= 0) and has_position and position_cost > 0:
            current_price = position_cost
        if current_price and current_price > 0:
            if stop_loss is not None and stop_loss < current_price:
                stop_loss_pct = max(1.0, min(50.0, round((current_price - stop_loss) / current_price * 100, 2)))
            if take_profit is not None and take_profit > current_price:
                take_profit_pct = max(1.0, min(200.0, round((take_profit - current_price) / current_price * 100, 2)))

        name = monitor_payload.get("name") or monitor_payload.get("code")
        task_name = f"{name}盯盘"
        if monitor_payload.get("needs_review"):
            task_name = f"{task_name}[待确认]"

        payload = {
            "task_name": task_name,
            "stock_code": str(monitor_payload.get("code", "")).strip(),
            "stock_name": name,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct,
            "source_type": monitor_payload.get("source_type", "watch"),
            "source_label": monitor_payload.get("source_label", "关注"),
            "portfolio_stock_id": monitor_payload.get("portfolio_stock_id"),
            "has_position": 1 if has_position else 0,
            "position_cost": position_cost if has_position else 0,
            "position_quantity": position_quantity if has_position else 0,
            "position_date": datetime.now().strftime("%Y-%m-%d") if has_position else None,
        }

        if include_runtime_defaults:
            payload.update({
                "enabled": 0,  # 默认禁用待确认
                "auto_trade": 0,  # 默认不自动交易
                "check_interval": 300,
                "trading_hours_only": 1,
                "position_size_pct": 20,
            })

        return payload

    @staticmethod
    def _merge_with_existing_smart_task(payload: Dict, existing_task: Optional[Dict]) -> Dict:
        """保留既有任务运行态配置，避免分析同步覆盖用户设置"""
        if not existing_task:
            return payload

        runtime_fields = [
            "enabled",
            "auto_trade",
            "check_interval",
            "trading_hours_only",
            "position_size_pct",
            "notify_email",
            "notify_webhook",
            "qmt_account_id",
        ]
        for field in runtime_fields:
            value = existing_task.get(field)
            if value is not None:
                payload[field] = value
        return payload

    def sync_analysis_to_monitors(self, analysis_results: Dict) -> Dict[str, Dict[str, int]]:
        """
        将持仓分析结果统一同步到实时监测与AI盯盘。
        Returns:
            {
                "realtime_sync": {"added":0,"updated":0,"failed":0,"total":0},
                "smart_sync": {"added":0,"updated":0,"failed":0,"total":0},
                "added":0,"updated":0,"failed":0,"total":0,  # 兼容旧调用（对应 realtime）
                "skipped": 0
            }
        """
        sync_result = {
            "realtime_sync": {"added": 0, "updated": 0, "failed": 0, "total": 0},
            "smart_sync": {"added": 0, "updated": 0, "failed": 0, "total": 0},
            "added": 0,
            "updated": 0,
            "failed": 0,
            "total": 0,
            "skipped": 0,
            "failed_reasons": []
        }

        if not analysis_results.get("success"):
            return sync_result

        monitor_payloads = []
        smart_payloads = []
        skipped = 0
        smart_db = None

        for item in analysis_results.get("results", []):
            code = self.normalize_stock_code(item.get("code", ""))
            if not code:
                sync_result["failed_reasons"].append({
                    "code": item.get("code", ""),
                    "reason": "股票代码为空，无法同步"
                })
                skipped += 1
                continue

            stock = self.db.get_stock_by_code(code)
            if not stock:
                sync_result["failed_reasons"].append({
                    "code": code,
                    "reason": "持仓不存在"
                })
                skipped += 1
                continue

            result = item.get("result", {})
            payload, reason = self._build_monitor_payload(code, result, stock)
            if not payload:
                sync_result["failed_reasons"].append({
                    "code": code,
                    "reason": reason or "价格位解析失败"
                })
                skipped += 1
                continue

            monitor_payloads.append(payload)

            if smart_db is None:
                from smart_monitor_db import SmartMonitorDB
                smart_db = SmartMonitorDB()
            smart_payload = self._build_smart_task_payload(payload, include_runtime_defaults=True)
            existing_task = smart_db.get_monitor_task_by_stock_code(code)
            smart_payload = self._merge_with_existing_smart_task(smart_payload, existing_task)
            smart_payloads.append(smart_payload)

        if monitor_payloads:
            from monitor_db import monitor_db
            realtime_sync = monitor_db.batch_add_or_update_monitors(monitor_payloads)
            sync_result["realtime_sync"] = realtime_sync
            # 兼容旧通知结构：默认仍输出实时监测统计
            sync_result["added"] = realtime_sync.get("added", 0)
            sync_result["updated"] = realtime_sync.get("updated", 0)
            sync_result["failed"] = realtime_sync.get("failed", 0)
            sync_result["total"] = realtime_sync.get("total", 0)

        if smart_payloads:
            if smart_db is None:
                from smart_monitor_db import SmartMonitorDB
                smart_db = SmartMonitorDB()
            sync_result["smart_sync"] = smart_db.batch_add_or_update_tasks(smart_payloads)

        sync_result["skipped"] = skipped
        return sync_result

    def _delete_stock_from_downstream(self, code: str) -> Dict[str, bool]:
        """从实时监测与AI盯盘删除指定股票"""
        normalized_code = self.normalize_stock_code(code)
        if not normalized_code:
            return {"realtime_deleted": False, "smart_deleted": False}

        from monitor_db import monitor_db
        from smart_monitor_db import SmartMonitorDB

        realtime_deleted = monitor_db.remove_monitored_stock_by_symbol(normalized_code)
        smart_db = SmartMonitorDB()
        smart_deleted = smart_db.delete_monitor_task_by_stock_code(normalized_code)

        return {
            "realtime_deleted": realtime_deleted,
            "smart_deleted": smart_deleted,
        }

    def sync_portfolio_stock_realtime(self, stock_id: int) -> Dict[str, Any]:
        """
        新增/编辑持仓后实时同步单只股票到实时监测与AI盯盘。
        """
        result = {
            "success": False,
            "stock_id": stock_id,
            "code": "",
            "realtime_sync": {"added": 0, "updated": 0, "failed": 0, "total": 0},
            "smart_sync": {"added": 0, "updated": 0, "failed": 0, "total": 0},
            "reason": "",
        }

        stock = self.db.get_stock(stock_id)
        if not stock:
            result["reason"] = f"未找到持仓ID: {stock_id}"
            return result

        code = self.normalize_stock_code(stock.get("code", ""))
        result["code"] = code
        if not code:
            result["reason"] = "股票代码为空"
            return result

        monitor_payload, reason = self._build_monitor_payload_from_portfolio_stock(stock)
        if not monitor_payload:
            result["reason"] = reason or "无法构建监测同步参数"
            return result

        from monitor_db import monitor_db
        from smart_monitor_db import SmartMonitorDB

        realtime_sync = monitor_db.batch_add_or_update_monitors([monitor_payload])
        result["realtime_sync"] = realtime_sync

        smart_db = SmartMonitorDB()
        smart_payload = self._build_smart_task_payload(monitor_payload, include_runtime_defaults=True)
        existing_task = smart_db.get_monitor_task_by_stock_code(code)
        smart_payload = self._merge_with_existing_smart_task(smart_payload, existing_task)
        smart_sync = smart_db.batch_add_or_update_tasks([smart_payload])
        result["smart_sync"] = smart_sync

        result["success"] = True
        return result

    def reconcile_portfolio_sync_on_startup(self) -> Dict[str, Any]:
        """
        启动时执行一次全量对齐：
        - 持仓股票补齐到实时监测与AI盯盘（来源=持仓）
        - 非持仓来源条目保留并标记为关注（来源=watch）
        """
        summary = {
            "success": True,
            "portfolio_total": 0,
            "portfolio_synced": 0,
            "portfolio_failed": 0,
            "realtime_sync": {"added": 0, "updated": 0, "failed": 0, "total": 0},
            "smart_sync": {"added": 0, "updated": 0, "failed": 0, "total": 0},
            "monitor_orphans_marked": 0,
            "smart_orphans_marked": 0,
            "failed_reasons": [],
        }

        stocks = self.db.get_all_stocks()
        summary["portfolio_total"] = len(stocks)
        portfolio_codes: Set[str] = set()

        for stock in stocks:
            code = self.normalize_stock_code(stock.get("code", ""))
            if not code:
                summary["portfolio_failed"] += 1
                summary["failed_reasons"].append({"code": "", "reason": "持仓代码为空"})
                continue
            portfolio_codes.add(code)
            sync_item = self.sync_portfolio_stock_realtime(stock.get("id"))
            if sync_item.get("success"):
                summary["portfolio_synced"] += 1
                for key in ("added", "updated", "failed", "total"):
                    summary["realtime_sync"][key] += sync_item.get("realtime_sync", {}).get(key, 0)
                    summary["smart_sync"][key] += sync_item.get("smart_sync", {}).get(key, 0)
            else:
                summary["portfolio_failed"] += 1
                summary["failed_reasons"].append({
                    "code": code,
                    "reason": sync_item.get("reason", "未知错误"),
                })

        from monitor_db import monitor_db
        monitored_items = monitor_db.get_monitored_stocks()
        for item in monitored_items:
            symbol = self.normalize_stock_code(item.get("symbol", ""))
            if not symbol or symbol in portfolio_codes:
                continue
            if (
                item.get("source_type") == "watch"
                and item.get("source_label") == "关注"
                and not item.get("portfolio_stock_id")
            ):
                continue

            entry_range = item.get("entry_range") or {}
            entry_min = self._extract_first_float(entry_range.get("min"))
            entry_max = self._extract_first_float(entry_range.get("max"))
            take_profit = self._extract_first_float(item.get("take_profit"))
            stop_loss = self._extract_first_float(item.get("stop_loss"))

            levels_complete = (
                entry_min is not None
                and entry_max is not None
                and take_profit is not None
                and stop_loss is not None
                and entry_max > entry_min
                and take_profit > 0
                and stop_loss > 0
            )
            if not levels_complete:
                anchor = self._extract_first_float(item.get("current_price")) or self._extract_first_float(item.get("position_cost"))
                if anchor is None or anchor <= 0:
                    anchor = 10.0
                fallback = self._build_fallback_levels(anchor)
                entry_min = fallback["entry_min"]
                entry_max = fallback["entry_max"]
                take_profit = fallback["take_profit"]
                stop_loss = fallback["stop_loss"]

            monitor_db.batch_add_or_update_monitors([{
                "code": symbol,
                "name": item.get("name") or symbol,
                "rating": item.get("rating") or "持有",
                "entry_min": entry_min,
                "entry_max": entry_max,
                "take_profit": take_profit,
                "stop_loss": stop_loss,
                "check_interval": item.get("check_interval"),
                "notification_enabled": item.get("notification_enabled"),
                "trading_hours_only": item.get("trading_hours_only"),
                "source_type": "watch",
                "source_label": "关注",
                "portfolio_stock_id": None,
                "has_position": item.get("has_position", False),
                "position_cost": item.get("position_cost"),
                "position_quantity": item.get("position_quantity"),
                "position_updated_at": item.get("position_updated_at"),
            }])
            summary["monitor_orphans_marked"] += 1

        from smart_monitor_db import SmartMonitorDB
        smart_db = SmartMonitorDB()
        tasks = smart_db.get_monitor_tasks(enabled_only=False)
        for task in tasks:
            stock_code = self.normalize_stock_code(task.get("stock_code", ""))
            if not stock_code or stock_code in portfolio_codes:
                continue
            if (
                task.get("source_type") == "watch"
                and task.get("source_label") == "关注"
                and not task.get("portfolio_stock_id")
            ):
                continue
            smart_db.update_monitor_task(stock_code, {
                "source_type": "watch",
                "source_label": "关注",
                "portfolio_stock_id": None,
            })
            summary["smart_orphans_marked"] += 1

        return summary

    def delete_stock_by_code(self, code: str) -> Tuple[bool, str]:
        """按股票代码删除持仓（用于下游删除回写）"""
        normalized_code = self.normalize_stock_code(code)
        if not normalized_code:
            return False, "股票代码不能为空"

        stock = self.db.get_stock_by_code(normalized_code)
        if not stock:
            return False, f"未找到持仓代码: {normalized_code}"

        return self.delete_stock(stock.get("id"))
    
    # ==================== 持仓股票管理 ====================
    
    def add_stock(self, code: str, name: str, cost_price: Optional[float] = None,
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
            # 验证股票代码格式
            code = self.normalize_stock_code(code)
            if not code:
                return False, "股票代码不能为空", None
            
            # 检查股票代码是否已存在
            existing = self.db.get_stock_by_code(code)
            if existing:
                return False, f"股票代码 {code} 已存在", None
            
            # 添加到数据库
            stock_id = self.db.add_stock(code, name, cost_price, quantity, note, True)
            sync_result = self.sync_portfolio_stock_realtime(stock_id)
            if sync_result.get("success"):
                return True, f"添加持仓股票成功: {code} {name}", stock_id
            warning_reason = sync_result.get("reason")
            msg = f"添加持仓股票成功: {code} {name}"
            if warning_reason:
                msg = f"{msg}（监测同步待补充: {warning_reason}）"
            return True, msg, stock_id
            
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
            # 始终同步策略：忽略外部传入的auto_monitor开关
            kwargs["auto_monitor"] = True
            success = self.db.update_stock(stock_id, **kwargs)
            if success:
                sync_result = self.sync_portfolio_stock_realtime(stock_id)
                if sync_result.get("success"):
                    return True, "更新成功"
                reason = sync_result.get("reason")
                return True, f"更新成功（监测同步待补充: {reason}）" if reason else "更新成功"
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
            stock = self.db.get_stock(stock_id)
            stock_code = self.normalize_stock_code(stock.get("code", "")) if stock else ""
            success = self.db.delete_stock(stock_id)
            if success:
                if stock_code:
                    self._delete_stock_from_downstream(stock_code)
                return True, "删除成功"
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
    
    # ==================== 单只股票分析 ====================
    
    def analyze_single_stock(self, stock_code: str, period="1y", 
                            selected_agents: List[str] = None) -> Dict:
        """
        分析单只股票（复用app.py中的分析逻辑）
        
        Args:
            stock_code: 股票代码
            period: 数据周期
            selected_agents: 选中的分析师列表
            
        Returns:
            分析结果字典
        """
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
            
            # 调用首页的分析函数
            result = analyze_single_stock_for_batch(
                symbol=stock_code,
                period=period,
                enabled_analysts_config=enabled_analysts_config,
                selected_model=self.model
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
                                 progress_callback=None) -> Dict:
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
                result = self.analyze_single_stock(code, period, selected_agents)
                
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
                               progress_callback=None) -> Dict:
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
                executor.submit(self.analyze_single_stock, code, period, selected_agents): code
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
                                progress_callback=None) -> Dict:
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
            return self.batch_analyze_parallel(stock_codes, period, selected_agents, max_workers, progress_callback)
        else:
            return self.batch_analyze_sequential(stock_codes, period, selected_agents, progress_callback)
    
    # ==================== 分析结果保存 ====================
    
    def save_analysis_results(self, analysis_results: Dict) -> List[int]:
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
            code = self.normalize_stock_code(item.get("code", ""))
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
            
            # 使用正确的字段名
            rating = final_decision.get("rating", "持有")
            # 确保信心度为float类型，避免Arrow序列化错误
            confidence_raw = final_decision.get("confidence_level", 5.0)
            try:
                confidence = float(confidence_raw)
                # 确保信心度在合理范围内
                if confidence < 0:
                    confidence = 0.0
                elif confidence > 10:
                    confidence = 10.0
            except (ValueError, TypeError):
                # 如果转换失败，使用默认值
                confidence = 5.0
            current_price = stock_info.get("current_price", 0.0)
            target_price_str = final_decision.get("target_price", "")
            entry_range = final_decision.get("entry_range", "")
            take_profit_str = final_decision.get("take_profit", "")
            stop_loss_str = final_decision.get("stop_loss", "")
            
            # 解析目标价格
            import re
            target_price = None
            if target_price_str:
                try:
                    numbers = re.findall(r'\d+\.?\d*', str(target_price_str))
                    if numbers:
                        target_price = float(numbers[0])
                except Exception:
                    pass
            
            # 解析进场区间
            entry_min, entry_max = self._parse_entry_range(entry_range)
            
            # 解析止盈止损
            take_profit, stop_loss = None, None
            take_profit = self._extract_first_float(take_profit_str)
            
            stop_loss = self._extract_first_float(stop_loss_str)
            
            # 生成摘要（使用advice或summary字段）
            summary = final_decision.get("advice", final_decision.get("summary", ""))[:500]  # 限制长度
            
            try:
                # 保存到数据库
                analysis_id = self.db.save_analysis(
                    stock_id, rating, confidence, current_price, target_price,
                    entry_min, entry_max, take_profit, stop_loss, summary
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

