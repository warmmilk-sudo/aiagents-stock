"""
智能盯盘 - 主引擎
整合DeepSeek AI决策、数据获取、待办生成、通知等功能
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Dict, List, Optional
from datetime import datetime
import threading

import config
from smart_monitor_deepseek import SmartMonitorDeepSeek
from smart_monitor_data import SmartMonitorDataFetcher
from smart_monitor_db import SmartMonitorDB
from notification_service import notification_service  # 复用主程序的通知服务
from config_manager import config_manager  # 复用主程序的配置管理器
from asset_repository import STATUS_PORTFOLIO
from investment_db_utils import DEFAULT_ACCOUNT_NAME
from investment_lifecycle_service import InvestmentLifecycleService, investment_lifecycle_service
from internal_events import event_bus, Events


class SmartMonitorEngine:
    """智能盯盘引擎"""

    DATA_FETCH_TIMEOUT_SECONDS = 10
    AI_DECISION_TIMEOUT_SECONDS = 25
    
    def __init__(self, deepseek_api_key: str = None, model: str = None,
                 lightweight_model: str = None, reasoning_model: str = None,
                 lifecycle_service: InvestmentLifecycleService = None):
        """
        初始化智能盯盘引擎
        
        Args:
            deepseek_api_key: DeepSeek API密钥（可选，从配置读取）
        """
        self.logger = logging.getLogger(__name__)
        
        # 从配置管理器读取配置
        config = config_manager.read_env()
        
        # DeepSeek API
        if deepseek_api_key is None:
            deepseek_api_key = config.get('DEEPSEEK_API_KEY', '')

        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        
        # 初始化各个模块
        self.deepseek = SmartMonitorDeepSeek(
            deepseek_api_key,
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        self.ai_decision_timeout_seconds = max(
            int(getattr(config, "SMART_MONITOR_AI_TIMEOUT_SECONDS", self.AI_DECISION_TIMEOUT_SECONDS) or self.AI_DECISION_TIMEOUT_SECONDS),
            int(getattr(self.deepseek, "http_timeout_seconds", self.AI_DECISION_TIMEOUT_SECONDS) or self.AI_DECISION_TIMEOUT_SECONDS) + 10,
        )
        self.data_fetcher = SmartMonitorDataFetcher()
        self.db = SmartMonitorDB()
        self.notification = notification_service  # 使用主程序的通知服务
        self.lifecycle_service = lifecycle_service or investment_lifecycle_service
        
        # 监控控制(保留字典为了停止特定监控时注销事件)
        self.monitoring_stocks = set()
        self.monitoring_contexts = {}
        
        # 注册事件总线监听
        event_bus.subscribe(Events.STOCK_ABNORMAL_FLUCTUATION, self._on_radar_event)
        
        self.logger.info("智能盯盘引擎初始化完成, 已订阅事件总线。")

    def set_model_overrides(self, model: str = None,
                            lightweight_model: str = None,
                            reasoning_model: str = None) -> None:
        """更新当前会话中后续分析使用的模型。"""
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.deepseek.set_model_overrides(
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )

    @staticmethod
    def _build_manual_account_info(
        asset: Optional[Dict],
        *,
        has_position: bool,
        position_cost: float,
        position_quantity: int,
    ) -> Dict:
        total_value = 0.0
        if has_position:
            total_value = round(position_cost * position_quantity, 3)

        account_info = {
            "available_cash": 0.0,
            "total_value": total_value,
            "positions_count": 1 if has_position else 0,
            "total_profit_loss": 0.0,
        }
        if has_position:
            account_info["current_position"] = {
                "quantity": position_quantity,
                "cost_price": position_cost,
                "status": (asset or {}).get("status"),
            }
        return account_info

    @staticmethod
    def _run_with_timeout(func, timeout_seconds: int, *args, **kwargs):
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError as exc:
            future.cancel()
            raise TimeoutError(f"operation_timed_out_after_{timeout_seconds}s") from exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def _map_action_to_rating(action: str, fallback: str = "持有") -> str:
        normalized = str(action or "").upper()
        if normalized == "BUY":
            return "买入"
        if normalized == "SELL":
            return "卖出"
        return fallback or "持有"

    @staticmethod
    def _normalize_monitor_levels(decision: Dict) -> Optional[Dict]:
        raw_levels = decision.get("monitor_levels")
        if not isinstance(raw_levels, dict):
            return None
        normalized: Dict[str, float] = {}
        for key in ("entry_min", "entry_max", "take_profit", "stop_loss"):
            value = raw_levels.get(key)
            if value in (None, ""):
                return None
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError):
                return None
        return normalized

    def _find_ai_task_item(
        self,
        stock_code: str,
        account_name: str,
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
    ) -> Optional[Dict]:
        return self.db.monitoring_repository.get_item_by_symbol(
            stock_code,
            monitor_type='ai_task',
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )

    def _sync_runtime_thresholds(
        self,
        *,
        stock_code: str,
        stock_name: str,
        decision: Dict,
        decision_id: int,
        account_name: str,
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
        strategy_context: Optional[Dict],
    ) -> bool:
        from monitor_db import monitor_db as realtime_monitor_db

        task_item = self._find_ai_task_item(stock_code, account_name, asset_id, portfolio_stock_id)
        monitor_levels = self._normalize_monitor_levels(decision)
        if not monitor_levels:
            if task_item:
                self.db.monitoring_repository.record_event(
                    item_id=task_item["id"],
                    event_type="threshold_sync_skipped",
                    message="AI 未返回完整 monitor_levels，跳过实时预警阈值回写",
                    notification_pending=False,
                    sent=True,
                    details={"decision_id": decision_id},
                )
            return False

        asset = self.db.asset_repository.get_asset(asset_id) if asset_id else None
        managed_by_portfolio = bool(asset and asset.get("status") == STATUS_PORTFOLIO)
        inherited_interval = 3
        inherited_notification = True
        inherited_trading_hours = True

        existing_alert = realtime_monitor_db.get_monitor_by_code(
            stock_code,
            managed_only=True if managed_by_portfolio else None,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        if existing_alert:
            inherited_interval = int(existing_alert.get("check_interval") or inherited_interval)
            inherited_notification = bool(existing_alert.get("notification_enabled", inherited_notification))
            inherited_trading_hours = bool(existing_alert.get("trading_hours_only", inherited_trading_hours))
            updated = realtime_monitor_db.apply_runtime_thresholds(
                existing_alert["id"],
                rating=self._map_action_to_rating(decision.get("action"), fallback=(strategy_context or {}).get("rating") or "持有"),
                monitor_levels=monitor_levels,
                origin_decision_id=decision_id,
            )
            if task_item:
                self.db.monitoring_repository.record_event(
                    item_id=task_item["id"],
                    event_type="threshold_sync",
                    message="已更新绑定价格预警的运行时阈值",
                    notification_pending=False,
                    sent=True,
                    details={"decision_id": decision_id, "price_alert_id": existing_alert["id"]},
                )
            return updated

        if task_item:
            inherited_interval = 3
            inherited_notification = bool(task_item.get("notification_enabled", inherited_notification))
            inherited_trading_hours = bool(task_item.get("trading_hours_only", inherited_trading_hours))

        price_alert_id = realtime_monitor_db.add_monitored_stock(
            symbol=stock_code,
            name=stock_name or stock_code,
            rating=self._map_action_to_rating(decision.get("action"), fallback=(strategy_context or {}).get("rating") or "持有"),
            entry_range={"min": monitor_levels["entry_min"], "max": monitor_levels["entry_max"]},
            take_profit=monitor_levels["take_profit"],
            stop_loss=monitor_levels["stop_loss"],
            check_interval=inherited_interval,
            notification_enabled=inherited_notification,
            trading_hours_only=inherited_trading_hours,
            managed_by_portfolio=managed_by_portfolio,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            origin_analysis_id=(strategy_context or {}).get("origin_analysis_id"),
        )
        updated = realtime_monitor_db.apply_runtime_thresholds(
            price_alert_id,
            rating=self._map_action_to_rating(decision.get("action"), fallback=(strategy_context or {}).get("rating") or "持有"),
            monitor_levels=monitor_levels,
            origin_decision_id=decision_id,
        )
        if task_item:
            self.db.monitoring_repository.record_event(
                item_id=task_item["id"],
                event_type="threshold_sync",
                message="已创建并写入绑定价格预警的运行时阈值",
                notification_pending=False,
                sent=True,
                details={"decision_id": decision_id, "price_alert_id": price_alert_id},
            )
        return updated
    
    def analyze_stock(self, stock_code: str, notify: bool = True, has_position: bool = False,
                      position_cost: float = 0, position_quantity: int = 0,
                      trading_hours_only: bool = True,
                      account_name: str = DEFAULT_ACCOUNT_NAME,
                      asset_id: Optional[int] = None,
                     portfolio_stock_id: Optional[int] = None,
                     strategy_context: Optional[Dict] = None) -> Dict:
        """
        分析单只股票并做出决策
        
        Args:
            stock_code: 股票代码
            notify: 是否发送通知
            has_position: 是否已持仓（可选）
            position_cost: 持仓成本（可选）
            position_quantity: 持仓数量（可选）
            trading_hours_only: 是否仅在交易时段分析（可选，默认True）
            
        Returns:
            分析结果
        """
        try:
            self.logger.info(f"[{stock_code}] 开始分析...")
            
            # 1. 检查交易时段
            session_info = self.deepseek.get_trading_session()
            self.logger.info(f"[{stock_code}] 当前时段: {session_info['session']}")
            
            # 如果启用了仅交易时段分析，且当前不在交易时段，则跳过分析
            if trading_hours_only and not session_info.get('can_trade', False):
                self.logger.info(f"[{stock_code}] 非交易时段，跳过分析")
                return {
                    'success': False,
                    'error': f"非交易时段（{session_info['session']}），跳过分析",
                    'session_info': session_info,
                    'skipped': True
                }
            
            # 2. 获取市场数据
            market_data = self._run_with_timeout(
                self.data_fetcher.get_comprehensive_data,
                self.DATA_FETCH_TIMEOUT_SECONDS,
                stock_code,
            )
            if not market_data:
                return {
                    'success': False,
                    'error': '获取市场数据失败'
                }
            
            task_context = self.db.get_monitor_task_by_code(
                stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
            ) or {}
            asset_id = asset_id or task_context.get("asset_id")
            account_name = task_context.get("account_name") or account_name
            asset = None
            if asset_id:
                asset = self.db.asset_repository.get_asset(asset_id)
            if not asset and stock_code:
                asset = self.db.asset_repository.get_asset_by_symbol(stock_code, account_name)
                if asset:
                    asset_id = asset.get("id")
            if asset:
                has_position = asset.get("status") == STATUS_PORTFOLIO and int(asset.get("quantity") or 0) > 0
                position_cost = float(asset.get("cost_price") or 0)
                position_quantity = int(asset.get("quantity") or 0)
            account_info = self._build_manual_account_info(
                asset,
                has_position=has_position,
                position_cost=position_cost,
                position_quantity=position_quantity,
            )
            if strategy_context is None:
                strategy_context = task_context.get("strategy_context") or {}
            if strategy_context is None or not strategy_context:
                strategy_context = self.db.analysis_repository.get_latest_strategy_context(
                    asset_id=asset_id,
                    symbol=stock_code,
                    account_name=account_name,
                ) or {}
            portfolio_stock_id = portfolio_stock_id or task_context.get("portfolio_stock_id") or (
                asset_id if asset and asset.get("status") == STATUS_PORTFOLIO else None
            )

            # 5. 调用DeepSeek AI决策
            ai_result = self._run_with_timeout(
                self.deepseek.analyze_stock_and_decide,
                self.ai_decision_timeout_seconds,
                stock_code=stock_code,
                market_data=market_data,
                account_info=account_info,
                has_position=has_position,
                position_cost=position_cost,
                position_quantity=position_quantity,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
                strategy_context=strategy_context,
            )
            
            if not ai_result['success']:
                return {
                    'success': False,
                    'error': 'AI决策失败',
                    'details': ai_result
                }
            
            decision = ai_result['decision']
            
            self.logger.info(f"[{stock_code}] AI决策: {decision['action']} "
                           f"(信心度: {decision['confidence']}%)")
            self.logger.info(f"[{stock_code}] 决策理由: {decision['reasoning'][:100]}...")
            
            # 6. 保存AI决策到数据库
            decision_id = self.db.save_ai_decision({
                'stock_code': stock_code,
                'stock_name': market_data.get('name'),
                'account_name': account_name,
                'asset_id': asset_id,
                'portfolio_stock_id': portfolio_stock_id,
                'origin_analysis_id': strategy_context.get('origin_analysis_id') if strategy_context else None,
                'trading_session': session_info['session'],
                'action': decision['action'],
                'confidence': decision['confidence'],
                'reasoning': decision['reasoning'],
                'position_size_pct': decision.get('position_size_pct'),
                'stop_loss_pct': decision.get('stop_loss_pct'),
                'take_profit_pct': decision.get('take_profit_pct'),
                'risk_level': decision.get('risk_level'),
                'key_price_levels': decision.get('key_price_levels', {}),
                'monitor_levels': decision.get('monitor_levels', {}),
                'market_data': market_data,
                'account_info': account_info,
                'execution_mode': 'manual_only',
                'action_status': 'pending' if str(decision.get('action', '')).upper() in {'BUY', 'SELL'} else 'suggested',
            })

            self._sync_runtime_thresholds(
                stock_code=stock_code,
                stock_name=market_data.get('name') or stock_code,
                decision=decision,
                decision_id=decision_id,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
                strategy_context=strategy_context,
            )

            # 7. 手工执行模式下只生成待处理动作
            execution_result = None
            pending_action = None
            if str(decision.get('action', '')).upper() in {'BUY', 'SELL'}:
                pending_action = self._create_pending_action(
                    stock_code=stock_code,
                    stock_name=market_data.get('name') or stock_code,
                    asset_id=asset_id,
                    decision_id=decision_id,
                    decision=decision,
                    market_data=market_data,
                    account_name=account_name,
                )
                execution_result = pending_action

            # 8. 发送通知
            if notify:
                self._send_notification(
                    stock_code=stock_code,
                    stock_name=market_data.get('name'),
                    decision=decision,
                    execution_result=execution_result,
                    market_data=market_data,
                    has_position=has_position,
                    position_cost=position_cost,
                    position_quantity=position_quantity,
                    session_info=session_info,
                    account_name=account_name,
                    asset_id=asset_id,
                    portfolio_stock_id=portfolio_stock_id,
                )
            
            return {
                'success': True,
                'stock_code': stock_code,
                'stock_name': market_data.get('name'),
                'session_info': session_info,
                'market_data': market_data,
                'decision': decision,
                'decision_id': decision_id,
                'execution_result': execution_result,
                'pending_action': pending_action,
                'account_name': account_name,
                'asset_id': asset_id,
                'portfolio_stock_id': portfolio_stock_id,
                'strategy_context': strategy_context or {},
            }
            
        except Exception as e:
            self.logger.error(f"[{stock_code}] 分析失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }

    def _create_pending_action(
        self,
        *,
        stock_code: str,
        stock_name: str,
        asset_id: Optional[int],
        decision_id: int,
        decision: Dict,
        market_data: Dict,
        account_name: str,
    ) -> Dict:
        resolved_asset_id = asset_id
        if resolved_asset_id is None:
            _, _, resolved_asset_id = self.db.asset_service.promote_to_watchlist(
                symbol=stock_code,
                stock_name=stock_name or stock_code,
                account_name=account_name,
                note="来自 AI 盯盘信号",
            )
        if resolved_asset_id is None:
            return {
                "success": False,
                "error": "asset_resolve_failed",
            }
        payload = {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "account_name": account_name,
            "current_price": market_data.get("current_price"),
            "decision": decision,
            "market_data": {
                "current_price": market_data.get("current_price"),
                "change_pct": market_data.get("change_pct"),
                "change_amount": market_data.get("change_amount"),
            },
        }
        pending_action_id = self.db.create_pending_action(
            asset_id=resolved_asset_id,
            action_type=str(decision.get("action", "")).lower(),
            origin_decision_id=decision_id,
            payload=payload,
        )
        return {
            "success": True,
            "mode": "manual_only",
            "pending_action_id": pending_action_id,
            "message": f"已创建待人工处理动作: {decision.get('action')}",
        }
    def _send_notification(
        self,
        stock_code: str,
        stock_name: str,
        decision: Dict,
        execution_result: Optional[Dict],
        market_data: Dict,
        has_position: Optional[bool] = None,
        position_cost: Optional[float] = None,
        position_quantity: Optional[int] = None,
        session_info: Optional[Dict] = None,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
    ):
        try:
            action = str(decision.get('action', '')).upper()
            if action not in ['BUY', 'SELL']:
                self.logger.info(f"[{stock_code}] 决策为 {action}，不发送通知")
                return

            task = self.db.get_monitor_task_by_code(
                stock_code,
                account_name=account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
            )
            task_config = task or {}
            if has_position is None:
                has_position = bool(task_config.get('has_position', 0))
            if position_cost is None:
                position_cost = float(task_config.get('position_cost', 0) or 0)
            if position_quantity is None:
                position_quantity = int(task_config.get('position_quantity', 0) or 0)
            if session_info is None:
                session_info = self.deepseek.get_trading_session()

            current_price = float(market_data.get('current_price', 0) or 0)
            profit_loss_pct = 'N/A'
            if has_position and position_cost:
                profit_loss_pct = f"{((current_price - position_cost) / position_cost * 100):+.2f}"

            action_text = {'BUY': '买入', 'SELL': '卖出'}.get(action, action)
            reasoning = decision.get('reasoning', '')
            reasoning_summary = reasoning[:150] + '...' if len(reasoning) > 150 else reasoning
            key_levels = decision.get('key_price_levels', {}) or {}

            message = f"{action_text}信号 - {stock_name}({stock_code})"
            content = (
                f"{action_text}信号\n"
                f"股票: {stock_name}({stock_code})\n"
                f"当前价格: {current_price:.2f}\n"
                f"涨跌幅: {market_data.get('change_pct', 0):+.2f}%\n"
                f"信心度: {decision.get('confidence', 'N/A')}%\n"
                f"风险等级: {decision.get('risk_level', 'N/A')}\n"
                f"支撑位: {key_levels.get('support', 'N/A')}\n"
                f"压力位: {key_levels.get('resistance', 'N/A')}\n"
                f"止盈: {decision.get('take_profit_pct', 'N/A')}%\n"
                f"止损: {decision.get('stop_loss_pct', 'N/A')}%\n"
                f"核心理由: {reasoning_summary}\n"
                f"触发时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            if execution_result and execution_result.get('pending_action_id'):
                content += f"\n已生成待人工处理动作 #{execution_result.get('pending_action_id')}"
            elif execution_result and not execution_result.get('success'):
                content += f"\n动作创建失败: {execution_result.get('error', '未知错误')}"

            notification_data = {
                'symbol': stock_code,
                'name': stock_name,
                'type': '智能盯盘',
                'message': message,
                'details': content,
                'triggered_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'current_price': f"{current_price:.2f}" if current_price else 'N/A',
                'change_pct': f"{market_data.get('change_pct', 0):+.2f}",
                'change_amount': f"{market_data.get('change_amount', 0):+.2f}",
                'volume': market_data.get('volume', 'N/A'),
                'turnover_rate': f"{market_data.get('turnover_rate', 0):.2f}",
                'position_status': '已持仓' if has_position else '未持仓',
                'position_cost': f"{position_cost:.2f}" if has_position and position_cost else 'N/A',
                'position_quantity': position_quantity if has_position else 0,
                'profit_loss_pct': profit_loss_pct,
                'trading_session': session_info.get('session', '未知'),
            }

            self.db.save_notification({
                'stock_code': stock_code,
                'notify_type': 'decision',
                'subject': f"智能盯盘 - {message}",
                'content': content,
                'status': 'pending',
            })

            task_item = self._find_ai_task_item(stock_code, account_name, asset_id, portfolio_stock_id)
            if task_item:
                self.db.monitoring_repository.record_event(
                    item_id=task_item['id'],
                    event_type=action.lower(),
                    message=message,
                    details=notification_data,
                    notification_pending=True,
                    sent=False,
                )
                self.logger.info(f"[{stock_code}] {action_text}通知已入队")
        except Exception as exc:
            self.logger.error(f"[{stock_code}] 发送通知失败: {exc}")

    def start_monitor(
        self,
        stock_code: str,
        check_interval: int = 300,
        notify: bool = True,
        has_position: bool = False,
        position_cost: float = 0,
        position_quantity: int = 0,
        trading_hours_only: bool = True,
        account_name: str = DEFAULT_ACCOUNT_NAME,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
        strategy_context: Optional[Dict] = None,
    ):
        self.logger.info(
            "[%s] start_monitor 已废弃，AI 执行统一由 MonitoringOrchestrator 调度",
            stock_code,
        )

    def stop_monitor(self, stock_code: str):
        self.logger.info("[%s] stop_monitor 已废弃，统一调度器不再使用旧事件总线路径", stock_code)

    def _on_radar_event(self, **kwargs):
        stock_code = kwargs.get('stock_code')
        if stock_code:
            self.logger.debug("[%s] 忽略旧雷达事件回调，统一调度器已接管执行", stock_code)


if __name__ == '__main__':
    # 测试代码
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    engine = SmartMonitorEngine(
        deepseek_api_key=os.getenv('DEEPSEEK_API_KEY'),
    )
    
    # 测试分析贵州茅台
    print("\n测试分析贵州茅台(600519)...")
    result = engine.analyze_stock('600519', notify=False)
    
    if result['success']:
        print(f"\n分析成功!")
        print(f"  决策: {result['decision']['action']}")
        print(f"  信心度: {result['decision']['confidence']}%")
        print(f"  理由: {result['decision']['reasoning'][:100]}...")
    else:
        print(f"\n分析失败: {result.get('error')}")

