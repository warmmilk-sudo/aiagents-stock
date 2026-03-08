import logging
from typing import Callable, Dict, List, Any
import threading

logger = logging.getLogger(__name__)

class EventBus:
    """
    轻量级内部事件总线 (Pub/Sub)
    用于解决模块间的网状依赖，解耦雷达层(Radar)与大脑层(Brain)。
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(EventBus, cls).__new__(cls)
                cls._instance._subscribers = {}
        return cls._instance

    def subscribe(self, event_type: str, callback: Callable[[Dict[str, Any]], None]):
        """
        订阅某种类型的事件
        Args:
            event_type: 事件类型，如 'stock_abnormal_fluctuation'
            callback: 收到事件时的回调函数，签名需接受一个 Dict 类型的 kwargs
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        if callback not in self._subscribers[event_type]:
            self._subscribers[event_type].append(callback)
            logger.info(f"Subscribed {callback.__name__} to {event_type}")

    def unsubscribe(self, event_type: str, callback: Callable):
        """取消订阅"""
        if event_type in self._subscribers and callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)

    def publish(self, event_type: str, **kwargs):
        """
        发布事件 (异步执行回调防阻塞)
        Args:
            event_type: 事件类型
            **kwargs: 随事件传递的数据参数
        """
        if event_type not in self._subscribers or not self._subscribers[event_type]:
            # 没有人订阅
            return
            
        logger.info(f"Publishing event [{event_type}] with payload keys: {list(kwargs.keys())}")
        
        # 将回调放入新线程执行，防止阻塞雷达层的扫描死循环
        for callback in self._subscribers[event_type]:
            threading.Thread(
                target=self._safe_execute_callback, 
                args=(callback, kwargs),
                name=f"EventBus-{event_type}-{callback.__name__}",
                daemon=True
            ).start()
            
    def _safe_execute_callback(self, callback: Callable, kwargs: Dict):
        try:
            callback(**kwargs)
        except Exception as e:
            logger.error(f"Error executing event callback {callback.__name__}: {e}", exc_info=True)

# 全局单例
event_bus = EventBus()

# --- 标准事件名称定义常量 ---
class Events:
    # 股票异动事件 (价格触发、技术指标触发)
    STOCK_ABNORMAL_FLUCTUATION = "stock_abnormal_fluctuation"
    # 突发新闻情绪事件
    NEWS_SENTIMENT_ALERT = "news_sentiment_alert"
