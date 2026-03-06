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
            if current_price is None or current_price <= 0:
                return None, "关键价格位无法解析且缺少有效 current_price，无法兜底"
            fallback = self._build_fallback_levels(current_price)
            entry_min = fallback["entry_min"]
            entry_max = fallback["entry_max"]
            take_profit = fallback["take_profit"]
            stop_loss = fallback["stop_loss"]
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
        return payload, None

    @staticmethod
    def _build_smart_task_payload(monitor_payload: Dict) -> Dict:
        """将监测价格位映射为 AI 盯盘任务"""
        current_price = monitor_payload.get("current_price")
        stop_loss = monitor_payload.get("stop_loss")
        take_profit = monitor_payload.get("take_profit")

        stop_loss_pct = 5.0
        take_profit_pct = 10.0
        if current_price and current_price > 0:
            if stop_loss is not None and stop_loss < current_price:
                stop_loss_pct = max(1.0, min(50.0, round((current_price - stop_loss) / current_price * 100, 2)))
            if take_profit is not None and take_profit > current_price:
                take_profit_pct = max(1.0, min(200.0, round((take_profit - current_price) / current_price * 100, 2)))

        name = monitor_payload.get("name") or monitor_payload.get("code")
        task_name = f"{name}盯盘"
        if monitor_payload.get("needs_review"):
            task_name = f"{task_name}[待确认]"

        return {
            "task_name": task_name,
            "stock_code": str(monitor_payload.get("code", "")).strip(),
            "stock_name": name,
            "enabled": 0,  # 默认禁用待确认
            "auto_trade": 0,  # 默认不自动交易
            "check_interval": 300,
            "trading_hours_only": 1,
            "position_size_pct": 20,
            "stop_loss_pct": stop_loss_pct,
            "take_profit_pct": take_profit_pct
        }

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
            if not stock or not stock.get("auto_monitor"):
                sync_result["failed_reasons"].append({
                    "code": code,
                    "reason": "未启用自动监测或持仓不存在"
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
            smart_payloads.append(self._build_smart_task_payload(payload))

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
            from smart_monitor_db import SmartMonitorDB
            smart_db = SmartMonitorDB()
            sync_result["smart_sync"] = smart_db.batch_add_or_update_tasks(smart_payloads)

        sync_result["skipped"] = skipped
        return sync_result
    
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
            stock_id = self.db.add_stock(code, name, cost_price, quantity, note, auto_monitor)
            return True, f"添加持仓股票成功: {code} {name}", stock_id
            
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
            success = self.db.update_stock(stock_id, **kwargs)
            if success:
                return True, "更新成功"
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
            success = self.db.delete_stock(stock_id)
            if success:
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

