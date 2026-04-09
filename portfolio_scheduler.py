"""
持仓定时分析调度器模块

提供定时任务调度功能，在设定时间自动执行持仓批量分析
"""

import schedule
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional
import traceback

from portfolio_manager import portfolio_manager
from notification_service import NotificationService
from portfolio_analysis_tasks import (
    PORTFOLIO_ANALYSIS_GLOBAL_SESSION_ID,
    portfolio_analysis_task_manager,
)
from investment_db_utils import DEFAULT_ACCOUNT_NAME, normalize_account_name


@dataclass
class PortfolioAnalysisTaskConfig:
    """持仓分析任务的共享配置模型。"""

    analysis_mode: str = "sequential"
    max_workers: int = 1
    auto_monitor_sync: bool = True
    notification_enabled: bool = True
    selected_agents: Optional[List[str]] = field(
        default_factory=lambda: ["technical", "fundamental", "fund_flow", "risk"]
    )


@dataclass
class PortfolioAccountTaskConfig:
    """账户级定时分析启用配置。"""

    account_name: str
    enabled: bool = True


class PortfolioScheduler:
    """持仓分析定时调度器"""
    
    def __init__(self):
        """初始化调度器"""
        self.schedule_times = ["09:30"]  # 支持多个定时时间点
        self._is_running = False  # 使用私有属性
        self.thread = None
        self.last_run_time = None
        self.next_run_time = None
        self.notification_service = NotificationService()
        self.task_config = PortfolioAnalysisTaskConfig()
        self.account_task_configs: dict[str, PortfolioAccountTaskConfig] = {}

    @property
    def analysis_mode(self) -> str:
        return self.task_config.analysis_mode

    @analysis_mode.setter
    def analysis_mode(self, value: str) -> None:
        self.task_config.analysis_mode = "sequential"

    @property
    def auto_monitor_sync(self) -> bool:
        return self.task_config.auto_monitor_sync

    @auto_monitor_sync.setter
    def auto_monitor_sync(self, value: bool) -> None:
        self.task_config.auto_monitor_sync = bool(value)

    @property
    def notification_enabled(self) -> bool:
        return self.task_config.notification_enabled

    @notification_enabled.setter
    def notification_enabled(self, value: bool) -> None:
        self.task_config.notification_enabled = bool(value)

    @property
    def selected_agents(self) -> Optional[List[str]]:
        return list(self.task_config.selected_agents) if self.task_config.selected_agents else None

    @selected_agents.setter
    def selected_agents(self, value: Optional[List[str]]) -> None:
        self.task_config.selected_agents = list(value) if value else None

    @property
    def max_workers(self) -> int:
        return self.task_config.max_workers

    @max_workers.setter
    def max_workers(self, value: int) -> None:
        self.task_config.max_workers = 1
    
    # 兼容旧代码的属性
    @property
    def schedule_time(self) -> str:
        """获取第一个定时时间（向后兼容）"""
        return self.schedule_times[0] if self.schedule_times else "09:30"
    
    def is_running(self) -> bool:
        """
        检查调度器是否正在运行
        
        Returns:
            bool: True表示运行中，False表示已停止
        """
        return self._is_running
    
    def set_schedule_time(self, time_str: str):
        """
        设置定时分析时间（向后兼容，设置为单个时间）
        
        Args:
            time_str: 时间字符串，格式"HH:MM"（如"08:00"）
        """
        try:
            # 验证时间格式
            datetime.strptime(time_str, "%H:%M")
            self.schedule_times = [time_str]
            print(f"[OK] 设置定时分析时间: {time_str}")
            
            # 如果调度器正在运行，重新调度
            if self._is_running:
                self._reschedule()
                
        except ValueError:
            print(f"[ERROR] 无效的时间格式: {time_str}，应为 HH:MM")
    
    def add_schedule_time(self, time_str: str) -> bool:
        """
        添加一个定时分析时间点
        
        Args:
            time_str: 时间字符串，格式"HH:MM"
            
        Returns:
            是否添加成功
        """
        try:
            # 验证时间格式
            datetime.strptime(time_str, "%H:%M")
            
            # 检查是否已存在
            if time_str in self.schedule_times:
                print(f"[WARN] 定时时间 {time_str} 已存在")
                return False
            
            self.schedule_times.append(time_str)
            self.schedule_times.sort()  # 保持时间顺序
            print(f"[OK] 添加定时时间: {time_str}")
            
            # 如果调度器正在运行，重新调度
            if self._is_running:
                self._reschedule()
            
            return True
            
        except ValueError:
            print(f"[ERROR] 无效的时间格式: {time_str}，应为 HH:MM")
            return False
    
    def remove_schedule_time(self, time_str: str) -> bool:
        """
        删除一个定时分析时间点
        
        Args:
            time_str: 时间字符串
            
        Returns:
            是否删除成功
        """
        if time_str in self.schedule_times:
            self.schedule_times.remove(time_str)
            print(f"[OK] 删除定时时间: {time_str}")
            
            # 如果调度器正在运行，重新调度
            if self._is_running:
                self._reschedule()
            
            return True
        else:
            print(f"[WARN] 定时时间 {time_str} 不存在")
            return False
    
    def get_schedule_times(self) -> list:
        """
        获取所有定时分析时间点
        
        Returns:
            时间列表
        """
        return self.schedule_times.copy()
    
    def set_schedule_times(self, times: list):
        """
        批量设置定时分析时间点
        
        Args:
            times: 时间字符串列表
        """
        valid_times = []
        for time_str in times:
            try:
                datetime.strptime(time_str, "%H:%M")
                valid_times.append(time_str)
            except ValueError:
                print(f"[WARN] 跳过无效时间: {time_str}")
        
        if valid_times:
            self.schedule_times = sorted(valid_times)
            print(f"[OK] 设置定时时间: {', '.join(self.schedule_times)}")
            
            # 如果调度器正在运行，重新调度
            if self._is_running:
                self._reschedule()
        else:
            print(f"[ERROR] 没有有效的时间配置")
    
    def set_analysis_mode(self, mode: str):
        """
        设置分析模式
        
        Args:
            mode: "sequential" 或 "parallel"
        """
        self.task_config.analysis_mode = "sequential"
        if mode not in [None, "sequential", "parallel"]:
            print(f"[WARN] 忽略无效的分析模式: {mode}")
        print("[OK] 设置分析模式: sequential")
    
    def set_auto_monitor_sync(self, enabled: bool):
        """设置是否启用自动监测同步"""
        self.task_config.auto_monitor_sync = enabled
        print(f"[OK] 自动监测同步: {'启用' if enabled else '禁用'}")
    
    def set_notification_enabled(self, enabled: bool):
        """设置是否启用通知"""
        self.task_config.notification_enabled = enabled
        print(f"[OK] 通知推送: {'启用' if enabled else '禁用'}")
    
    def set_selected_agents(self, agents: Optional[list]):
        """设置参与分析的AI分析师"""
        self.task_config.selected_agents = list(agents) if agents else None
        if agents:
            print(f"[OK] 选择分析师: {', '.join(agents)}")
        else:
            print("[OK] 选择分析师: 全部")

    def get_task_config(self) -> PortfolioAnalysisTaskConfig:
        """获取当前共享分析任务配置。"""
        return PortfolioAnalysisTaskConfig(
            analysis_mode=self.analysis_mode,
            max_workers=self.max_workers,
            auto_monitor_sync=self.auto_monitor_sync,
            notification_enabled=self.notification_enabled,
            selected_agents=self.selected_agents,
        )

    def set_task_config(self, config: PortfolioAnalysisTaskConfig) -> None:
        """整体替换共享分析任务配置。"""
        self.set_analysis_mode(config.analysis_mode)
        self.max_workers = config.max_workers
        self.set_auto_monitor_sync(config.auto_monitor_sync)
        self.set_notification_enabled(config.notification_enabled)
        self.set_selected_agents(config.selected_agents)

    def get_account_task_configs(self) -> List[PortfolioAccountTaskConfig]:
        return [
            PortfolioAccountTaskConfig(
                account_name=config.account_name,
                enabled=config.enabled,
            )
            for config in sorted(self.account_task_configs.values(), key=lambda item: item.account_name)
        ]

    def set_account_task_configs(self, configs: Optional[List[dict]]) -> None:
        enabled = True
        for item in configs or []:
            enabled = bool((item or {}).get("enabled", True))
            break
        self.account_task_configs = {
            DEFAULT_ACCOUNT_NAME: PortfolioAccountTaskConfig(
                account_name=DEFAULT_ACCOUNT_NAME,
                enabled=enabled,
            )
        }

    def _resolve_enabled_accounts(self, available_accounts: List[str]) -> List[str]:
        if not self.account_task_configs:
            return list(available_accounts)
        enabled_accounts: List[str] = []
        for account_name in available_accounts:
            account_config = self.account_task_configs.get(account_name)
            if account_config is None or account_config.enabled:
                enabled_accounts.append(account_name)
        return enabled_accounts

    def _merge_sync_result(self, summary: Optional[dict], sync_result: Optional[dict]) -> Optional[dict]:
        if summary is None:
            return None
        if not sync_result:
            return summary
        for key in ("added", "updated", "failed", "total"):
            summary[key] = int(summary.get(key, 0)) + int(sync_result.get(key, 0) or 0)
        return summary

    def _build_progress_message(self, current: int, total: int, code: str, status: str) -> str:
        status_map = {
            "analyzing": "正在分析",
            "success": "已完成",
            "failed": "失败",
            "error": "异常",
        }
        base = status_map.get(status, "处理中")
        if total:
            return f"{base} {code} ({current}/{total})"
        return f"{base} {code}"

    def _collect_available_accounts(self) -> List[str]:
        stocks = portfolio_manager.get_all_stocks(account_name=DEFAULT_ACCOUNT_NAME)
        if not any(stock.get("code") for stock in stocks):
            return []
        return [DEFAULT_ACCOUNT_NAME]

    def _run_scheduled_analysis_task(self, report_progress, *, trigger: str) -> dict:
        config = self.get_task_config()
        available_accounts = self._collect_available_accounts()
        if not available_accounts:
            raise RuntimeError("没有持仓股票")
        target_accounts = self._resolve_enabled_accounts(available_accounts)
        if not target_accounts:
            raise RuntimeError("没有启用的定时分析账户")
        account_counts = {
            account_name: portfolio_manager.get_stock_count(account_name)
            for account_name in target_accounts
        }
        stock_count = sum(account_counts.values())
        trigger_label = "定时" if trigger == "scheduled" else "手动"
        task_label = f"{trigger_label}持仓分析"
        saved_ids: List[int] = []
        aggregated_sync_result = (
            {"added": 0, "updated": 0, "failed": 0, "total": 0}
            if config.auto_monitor_sync
            else None
        )
        aggregated_results: List[dict] = []
        aggregated_failed: List[dict] = []
        total_succeeded = 0
        total_failed = 0
        start_time = time.time()

        print("\n" + "=" * 60)
        print(f"{task_label}开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")

        report_progress(
            current=0,
            total=stock_count,
            step_status="analyzing",
            message=f"正在准备{task_label}任务...",
        )

        try:
            completed_offset = 0
            for account_name in target_accounts:
                account_stock_count = account_counts.get(account_name, 0)
                if account_stock_count <= 0:
                    continue
                account_label = DEFAULT_ACCOUNT_NAME

                def progress_callback(current, callback_total, code, status, *, offset=completed_offset, label=account_label):
                    absolute_current = min(stock_count, offset + int(current or 0))
                    report_progress(
                        current=absolute_current,
                        total=stock_count,
                        step_code=code,
                        step_status=status,
                        message=f"{label} | {self._build_progress_message(int(current or 0), int(callback_total or 0), code, status)}",
                    )

                def result_callback(code, single_result, *, target_account=account_name):
                    persistence_result = portfolio_manager.persist_single_analysis_result(
                        code,
                        single_result,
                        sync_realtime_monitor=config.auto_monitor_sync,
                        analysis_source="portfolio_scheduler",
                        analysis_period="1y",
                        account_name=target_account,
                    )
                    saved_ids.extend(persistence_result.get("saved_ids", []))
                    self._merge_sync_result(aggregated_sync_result, persistence_result.get("sync_result"))

                analysis_results = portfolio_manager.batch_analyze_portfolio(
                    mode=config.analysis_mode,
                    max_workers=config.max_workers,
                    selected_agents=config.selected_agents,
                    progress_callback=progress_callback,
                    result_callback=result_callback,
                    account_name=account_name,
                )
                if not analysis_results.get("success"):
                    error_msg = analysis_results.get("error", f"{account_label} 分析失败")
                    raise RuntimeError(error_msg)

                total_succeeded += int(analysis_results.get("succeeded", 0))
                total_failed += int(analysis_results.get("failed", 0))
                aggregated_results.extend(
                    [{**item, "account_name": account_name} for item in analysis_results.get("results", [])]
                )
                aggregated_failed.extend(
                    [{**item, "account_name": account_name} for item in analysis_results.get("failed_stocks", [])]
                )
                completed_offset += account_stock_count

            analysis_results = {
                "success": True,
                "mode": config.analysis_mode,
                "total": stock_count,
                "succeeded": total_succeeded,
                "failed": total_failed,
                "results": aggregated_results,
                "failed_stocks": aggregated_failed,
                "elapsed_time": time.time() - start_time,
                "accounts": [
                    {
                        "account_name": account_name,
                    }
                    for account_name in target_accounts
                    if account_counts.get(account_name, 0) > 0
                ],
            }

            if config.notification_enabled:
                self._send_notification(analysis_results, aggregated_sync_result)

            self.last_run_time = datetime.now()
            report_progress(
                current=stock_count,
                total=stock_count or 1,
                step_status="success",
                message=(
                    f"{task_label}完成：成功 {analysis_results.get('succeeded', 0)}，"
                    f"失败 {analysis_results.get('failed', 0)}，已写入 {len(saved_ids)} 条历史"
                ),
            )
            print("\n" + "=" * 60)
            print(f"{task_label}完成: {self.last_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60 + "\n")
            return {
                "task_type": "batch",
                "trigger": trigger,
                "analysis_source": "portfolio_scheduler",
                "analysis_result": analysis_results,
                "persistence_result": {
                    "saved_ids": list(saved_ids),
                    "sync_result": aggregated_sync_result,
                },
                "auto_sync": config.auto_monitor_sync,
                "send_notification": config.notification_enabled,
            }
        except Exception as exc:
            print(f"\n[ERROR] {task_label}执行异常: {exc}")
            traceback.print_exc()
            if config.notification_enabled:
                self._send_error_notification(str(exc))
            self.last_run_time = datetime.now()
            raise

    def _scheduled_job(self, trigger: str = "scheduled") -> Optional[str]:
        """定时任务执行入口：将分析任务提交到后台队列。"""
        if trigger == "scheduled" and datetime.now().weekday() >= 5:
            print("[INFO] 周末跳过持仓定时分析")
            self._update_next_run_time()
            return None

        active_task = portfolio_analysis_task_manager.get_active_task_any(task_type="batch")
        if active_task:
            print("[WARN] 已有持仓分析任务正在执行或排队，跳过新的提交")
            return None

        stock_count = portfolio_manager.get_stock_count()
        if stock_count == 0:
            print("[ERROR] 没有持仓股票，跳过定时分析")
            return None

        label = "定时持仓分析任务" if trigger == "scheduled" else "手动持仓分析任务"
        return portfolio_analysis_task_manager.start_task(
            PORTFOLIO_ANALYSIS_GLOBAL_SESSION_ID,
            task_type="batch",
            label=label,
            runner=lambda _task_id, report_progress: self._run_scheduled_analysis_task(
                report_progress,
                trigger=trigger,
            ),
            metadata={
                "trigger": trigger,
                "analysis_mode": self.analysis_mode,
                "max_workers": self.max_workers,
                "selected_agents": self.selected_agents,
                "account_configs": [
                    {
                        "account_name": config.account_name,
                        "enabled": config.enabled,
                    }
                    for config in self.get_account_task_configs()
                ],
            },
        )
    
    def _sync_to_monitor(self, analysis_results: dict) -> dict:
        """
        同步分析结果到监测列表
        
        Args:
            analysis_results: 批量分析结果
            
        Returns:
            同步结果统计
        """
        try:
            codes = [item.get("code") for item in analysis_results.get("results", []) if item.get("code")]
            return portfolio_manager.sync_latest_analysis_to_realtime_monitor(codes=codes)
            
        except Exception as e:
            print(f"[ERROR] 监测同步异常: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"added": 0, "updated": 0, "failed": 0, "total": 0}
    
    def _send_notification(self, analysis_results: dict, sync_result: Optional[dict]):
        """
        发送分析完成通知（使用新的notification_service方法）
        
        Args:
            analysis_results: 批量分析结果
            sync_result: 监测同步结果
        """
        try:
            from notification_service import notification_service
            
            # 使用新的专用通知方法
            success = notification_service.send_portfolio_analysis_notification(
                analysis_results, sync_result
            )
            
            if success:
                print("[OK] 持仓分析通知发送成功")
            else:
                print("[WARN] 持仓分析通知发送失败（可能未配置通知服务）")
            
        except Exception as e:
            print(f"[ERROR] 发送通知失败: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def _send_error_notification(self, error_msg: str):
        """发送错误通知"""
        try:
            content = f"""
持仓定时分析执行失败

错误信息：
{error_msg}

时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

请检查系统日志或手动运行分析。
"""
            
            if self.notification_service.email_enabled:
                self.notification_service.send_email("【持仓定时分析】执行失败", content)
            
            if self.notification_service.webhook_enabled:
                self.notification_service.send_webhook("【持仓定时分析】执行失败", content)
                
        except Exception as e:
            print(f"[ERROR] 发送错误通知失败: {str(e)}")
    
    def _generate_notification_content(self, analysis_results: dict, 
                                      sync_result: Optional[dict]) -> str:
        """
        生成通知内容
        
        Args:
            analysis_results: 批量分析结果
            sync_result: 监测同步结果
            
        Returns:
            通知内容文本
        """
        total = analysis_results.get("total", 0)
        succeeded = analysis_results.get("succeeded", 0)
        failed = analysis_results.get("failed", 0)
        mode = analysis_results.get("mode", "sequential")
        mode_label = "账户分组" if mode == "account_scoped" else ("顺序分析" if mode == "sequential" else "并行分析")
        elapsed_time = analysis_results.get("elapsed_time", 0)
        
        # 统计评级分布
        rating_stats = {"买入": 0, "持有": 0, "卖出": 0}
        rating_changes = []
        
        for item in analysis_results.get("results", []):
            code = item.get("code")
            result = item.get("result", {})
            final_decision = result.get("final_decision", {})
            rating = final_decision.get("investment_rating", "持有")
            
            rating_stats[rating] = rating_stats.get(rating, 0) + 1
            
            # 检查评级变化
            stock = portfolio_manager.db.get_stock_by_code(code)
            if stock:
                history = portfolio_manager.db.get_analysis_history(stock['id'], limit=2)
                if len(history) >= 2:
                    old_rating = history[1]['rating']
                    new_rating = history[0]['rating']
                    if old_rating != new_rating:
                        stock_info = result.get("stock_info", {})
                        name = stock_info.get("name", stock.get("name", code))
                        rating_changes.append(f"{code} {name}: {old_rating} → {new_rating}")
        
        # 构建通知内容
        content = f"""
持仓定时分析报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}

📊 分析完成：{total}只持仓股票
✅ 成功：{succeeded}只
❌ 失败：{failed}只
⏱ 耗时：{elapsed_time:.1f}秒
🔄 模式：{mode_label}

📈 投资评级分布：
• 买入：{rating_stats.get('买入', 0)}只
• 持有：{rating_stats.get('持有', 0)}只
• 卖出：{rating_stats.get('卖出', 0)}只
"""
        
        # 添加评级变化
        if rating_changes:
            content += "\n🔔 评级变化：\n"
            for change in rating_changes[:5]:  # 最多显示5个
                content += f"• {change}\n"
        
        # 添加监测同步结果
        if sync_result:
            content += f"""
🎯 监测同步：
• 新增：{sync_result.get('added', 0)}只
• 更新：{sync_result.get('updated', 0)}只
• 失败：{sync_result.get('failed', 0)}只
"""
        
        # 添加失败股票
        if failed > 0:
            failed_stocks = analysis_results.get("failed_stocks", [])
            content += "\n⚠️ 失败股票：\n"
            for stock in failed_stocks[:3]:  # 最多显示3个
                content += f"• {stock.get('code')}: {stock.get('error')}\n"
        
        content += "\n详细报告请登录系统查看。"
        
        return content
    
    def _register_weekday_jobs(self) -> None:
        for time_str in self.schedule_times:
            for weekday_job in (
                schedule.every().monday,
                schedule.every().tuesday,
                schedule.every().wednesday,
                schedule.every().thursday,
                schedule.every().friday,
            ):
                job = weekday_job.at(time_str).do(self._scheduled_job)
                job.tag('portfolio_analysis')

    def _reschedule(self):
        """重新调度任务（支持多个时间点）"""
        jobs_to_remove = [job for job in schedule.jobs if 'portfolio_analysis' in job.tags]
        for job in jobs_to_remove:
            schedule.cancel_job(job)

        self._register_weekday_jobs()
        self._update_next_run_time()
        print(f"[OK] 重新调度任务: 工作日 {', '.join(self.schedule_times)}")

    def _update_next_run_time(self):
        """更新下次运行时间"""
        jobs = [job for job in schedule.jobs if 'portfolio_analysis' in job.tags and job.next_run]
        if jobs:
            self.next_run_time = min(job.next_run for job in jobs)
        else:
            self.next_run_time = None
    
    def _run_schedule_loop(self):
        """调度循环（在后台线程中运行）"""
        print("[OK] 定时调度器线程启动")
        
        while self._is_running:
            schedule.run_pending()
            self._update_next_run_time()
            time.sleep(1)
        
        print("[OK] 定时调度器线程停止")
    
    def start(self) -> bool:
        """
        启动定时任务
        
        Returns:
            是否启动成功
        """
        if self._is_running:
            print("[WARN] 定时任务已在运行中")
            return False
        
        # 检查持仓数量
        stock_count = portfolio_manager.get_stock_count()
        if stock_count == 0:
            print("[ERROR] 没有持仓股票，无法启动定时任务")
            return False
        
        # 检查时间配置
        if not self.schedule_times:
            print("[ERROR] 没有配置定时时间")
            return False
        
        # 调度任务（为每个时间点创建任务）
        # 只清除持仓定时分析的任务，不影响智策和监测任务
        jobs_to_remove = [job for job in schedule.jobs if 'portfolio_analysis' in job.tags]
        for job in jobs_to_remove:
            schedule.cancel_job(job)
        print(f"[OK] 清除了 {len(jobs_to_remove)} 个旧的持仓任务")
        
        self._register_weekday_jobs()
        for time_str in self.schedule_times:
            print(f"[OK] 添加调度任务: 工作日 {time_str}")
        
        self._update_next_run_time()
        
        # 启动后台线程
        self._is_running = True
        self.thread = threading.Thread(target=self._run_schedule_loop, daemon=True)
        self.thread.start()
        
        print(f"\n[OK] 定时任务已启动")
        print(f"    调度时间: 工作日 {', '.join(self.schedule_times)}")
        print(f"    分析模式: {self.analysis_mode}")
        print(f"    持仓数量: {stock_count}只")
        if self.next_run_time:
            print(f"    下次运行: {self.next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
    
    def stop(self) -> bool:
        """
        停止定时任务
        
        Returns:
            是否停止成功
        """
        if not self._is_running:
            print("[WARN] 定时任务未运行")
            return False
        
        self._is_running = False
        
        # 只清除持仓定时分析的任务，不影响其他模块（智策、监测）
        try:
            jobs_to_remove = [job for job in schedule.jobs if 'portfolio_analysis' in job.tags]
            for job in jobs_to_remove:
                schedule.cancel_job(job)
            print(f"[OK] 清除了 {len(jobs_to_remove)} 个持仓任务")
        except Exception as e:
            print(f"[WARN] 清除任务时出错: {e}")
        
        # 等待线程结束（最多等待2秒）
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        
        self.thread = None
        self.next_run_time = None
        
        print("[OK] 定时任务已停止")
        return True
    
    def run_once(self) -> Optional[str]:
        """
        立即执行一次分析（不影响定时计划）
        
        Returns:
            已提交任务ID；若未提交成功则返回 None
        """
        # 检查持仓数量
        stock_count = portfolio_manager.get_stock_count()
        if stock_count == 0:
            print("[ERROR] 没有持仓股票")
            return None
        
        print("[OK] 立即执行持仓分析...")
        return self._scheduled_job(trigger="manual")
    
    def get_status(self) -> dict:
        """
        获取定时任务状态
        
        Returns:
            状态字典
        """
        return {
            "is_running": self._is_running,
            "schedule_time": self.schedule_time,
            "analysis_mode": self.analysis_mode,
            "max_workers": self.max_workers,
            "auto_monitor_sync": self.auto_monitor_sync,
            "notification_enabled": self.notification_enabled,
            "selected_agents": self.selected_agents,
            "last_run_time": self.last_run_time.strftime("%Y-%m-%d %H:%M:%S") if self.last_run_time else None,
            "next_run_time": self.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if self.next_run_time else None,
            "portfolio_count": portfolio_manager.get_stock_count(),
            "account_configs": [
                {
                    "account_name": config.account_name,
                    "enabled": config.enabled,
                }
                for config in self.get_account_task_configs()
            ],
        }
    
    def get_next_run_time(self) -> Optional[str]:
        """
        获取下次运行时间
        
        Returns:
            下次运行时间字符串，格式"HH:MM"，如果未设置则返回None
        """
        if self.next_run_time:
            return self.next_run_time.strftime("%H:%M")
        return None
    
    def update_config(
        self,
        schedule_time: str = None,
        analysis_mode: str = None,
        max_workers: int = None,
        auto_sync_monitor: bool = None,
        send_notification: bool = None,
        selected_agents: Optional[List[str]] = None,
        account_configs: Optional[List[dict]] = None,
    ):
        """
        更新调度器配置
        
        Args:
            schedule_time: 定时分析时间（格式"HH:MM"，可选，用于向后兼容）
            analysis_mode: 分析模式（"sequential"或"parallel"）
            max_workers: 兼容旧配置，固定忽略并保持顺序分析
            auto_sync_monitor: 是否自动同步到监测
            send_notification: 是否发送通知
        """
        if schedule_time is not None:
            self.set_schedule_time(schedule_time)
        
        if analysis_mode is not None:
            self.set_analysis_mode(analysis_mode)
        
        if max_workers is not None:
            self.max_workers = 1
            print("[OK] 并发线程数固定为: 1")
        
        if auto_sync_monitor is not None:
            self.set_auto_monitor_sync(auto_sync_monitor)
        
        if send_notification is not None:
            self.set_notification_enabled(send_notification)

        if selected_agents is not None:
            self.set_selected_agents(selected_agents)

        if account_configs is not None:
            self.set_account_task_configs(account_configs)
        
        print("[OK] 配置已更新")
    
    def start_scheduler(self) -> bool:
        """
        启动调度器（UI友好方法名）
        
        Returns:
            是否启动成功
        """
        return self.start()
    
    def stop_scheduler(self) -> bool:
        """
        停止调度器（UI友好方法名）
        
        Returns:
            是否停止成功
        """
        return self.stop()
    
    def run_analysis_now(self) -> Optional[str]:
        """
        立即执行一次分析（UI友好方法名）
        
        Returns:
            已提交任务ID；若未提交成功则返回 None
        """
        return self.run_once()


# 创建全局实例
portfolio_scheduler = PortfolioScheduler()


if __name__ == "__main__":
    # 测试代码
    print("="*60)
    print("持仓定时调度器测试")
    print("="*60)
    
    scheduler = PortfolioScheduler()
    
    # 设置配置
    scheduler.set_schedule_time("09:00")
    scheduler.set_analysis_mode("sequential")
    scheduler.set_auto_monitor_sync(True)
    scheduler.set_notification_enabled(False)  # 测试时禁用通知
    
    # 获取状态
    status = scheduler.get_status()
    print("\n调度器状态:")
    for key, value in status.items():
        print(f"  {key}: {value}")
    
    print("\n[OK] 调度器测试完成")
