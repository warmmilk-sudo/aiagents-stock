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
    
    def __init__(self, model=None, lightweight_model=None, reasoning_model=None):
        """
        初始化持仓管理器
        
        Args:
            model: 强制所有任务统一使用同一个模型
        """
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.db = portfolio_db

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
            return True, f"添加持仓股票成功: {code} {final_name}", stock_id
            
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
            target_price = None
            if target_price_str:
                try:
                    numbers = re.findall(r'\d+\.?\d*', str(target_price_str))
                    if numbers:
                        target_price = float(numbers[0])
                except:
                    pass
            
            # 解析进场区间
            entry_min, entry_max = None, None
            if entry_range and isinstance(entry_range, str) and "-" in entry_range:
                try:
                    parts = entry_range.split("-")
                    entry_min = float(parts[0].strip())
                    entry_max = float(parts[1].strip())
                except:
                    pass
            
            # 解析止盈止损
            take_profit, stop_loss = None, None
            if take_profit_str:
                try:
                    numbers = re.findall(r'\d+\.?\d*', str(take_profit_str))
                    if numbers:
                        take_profit = float(numbers[0])
                except:
                    pass
            
            if stop_loss_str:
                try:
                    numbers = re.findall(r'\d+\.?\d*', str(stop_loss_str))
                    if numbers:
                        stop_loss = float(numbers[0])
                except:
                    pass
            
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

