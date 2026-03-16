from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    password: str = ""


class AnalystConfig(BaseModel):
    technical: bool = True
    fundamental: bool = True
    fund_flow: bool = True
    risk: bool = True
    sentiment: bool = False
    news: bool = False


class AnalysisTaskRequest(BaseModel):
    stock_input: str = ""
    symbols: list[str] = Field(default_factory=list)
    period: Optional[str] = None
    batch_mode: Literal["顺序分析", "多线程并行"] = "顺序分析"
    max_workers: int = 3
    analysts: AnalystConfig = Field(default_factory=AnalystConfig)
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class PortfolioAnalysisTaskRequest(BaseModel):
    account_name: Optional[str] = None
    period: Optional[str] = None
    batch_mode: Literal["顺序分析", "多线程并行"] = "顺序分析"
    max_workers: int = 3
    analysts: AnalystConfig = Field(default_factory=AnalystConfig)


class MainForceSelectionTaskRequest(BaseModel):
    days_ago: Optional[int] = 90
    start_date: Optional[str] = None
    final_n: int = 5
    max_change: float = 30.0
    min_cap: float = 50.0
    max_cap: float = 5000.0
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class MainForceBatchTaskRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    analysis_mode: Literal["sequential", "parallel"] = "sequential"
    max_workers: int = 3
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class SectorStrategyTaskRequest(BaseModel):
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class LonghubangTaskRequest(BaseModel):
    date: Optional[str] = None
    days: int = 1
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class LonghubangBatchTaskRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    analysis_mode: Literal["sequential", "parallel"] = "sequential"
    max_workers: int = 3
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class SectorStrategySchedulerRequest(BaseModel):
    schedule_time: str = "09:00"
    enabled: bool = True


class MainForceExportRequest(BaseModel):
    result: dict = Field(default_factory=dict)
    context_snapshot: dict = Field(default_factory=dict)


class SectorStrategyExportRequest(BaseModel):
    result: dict = Field(default_factory=dict)


class LonghubangExportRequest(BaseModel):
    result: dict = Field(default_factory=dict)


class LowPriceBullSelectionTaskRequest(BaseModel):
    top_n: int = 5
    max_price: float = 10.0
    min_profit_growth: float = 100.0
    min_turnover_yi: float = 0.0
    max_turnover_yi: float = 0.0
    min_market_cap_yi: float = 0.0
    max_market_cap_yi: float = 0.0
    sort_by: str = "成交额升序"
    exclude_st: bool = True
    exclude_kcb: bool = True
    exclude_cyb: bool = True
    only_hs_a: bool = True
    filter_summary: str = ""


class LowPriceBullMonitorConfigRequest(BaseModel):
    scan_interval: int = 60


class LowPriceBullMonitorStockCreateRequest(BaseModel):
    stock_code: str
    stock_name: str
    buy_price: float = 0.0
    buy_date: Optional[str] = None


class LowPriceBullAlertResolveRequest(BaseModel):
    status: Literal["done", "ignored"] = "done"


class LowPriceBullSimulationRequest(BaseModel):
    stocks: list[dict] = Field(default_factory=list)


class SmallCapSelectionTaskRequest(BaseModel):
    top_n: int = 5
    max_market_cap_yi: float = 50.0
    min_revenue_growth: float = 10.0
    min_profit_growth: float = 100.0
    sort_by: str = "总市值升序"
    exclude_st: bool = True
    exclude_kcb: bool = True
    exclude_cyb: bool = True
    only_hs_a: bool = True
    filter_summary: str = ""


class ProfitGrowthSelectionTaskRequest(BaseModel):
    top_n: int = 5
    min_profit_growth: float = 10.0
    min_turnover_yi: float = 0.0
    max_turnover_yi: float = 0.0
    sort_by: str = "成交额升序"
    exclude_st: bool = True
    exclude_kcb: bool = True
    exclude_cyb: bool = True
    filter_summary: str = ""


class ProfitGrowthMonitorStockCreateRequest(BaseModel):
    stock_code: str
    stock_name: str
    buy_price: float = 0.0
    buy_date: Optional[str] = None


class ValueStockSelectionTaskRequest(BaseModel):
    top_n: int = 10
    max_pe: float = 20.0
    max_pb: float = 1.5
    min_dividend_yield: float = 1.0
    max_debt_ratio: float = 30.0
    min_float_cap_yi: float = 0.0
    max_float_cap_yi: float = 0.0
    sort_by: str = "流通市值升序"
    exclude_st: bool = True
    exclude_kcb: bool = True
    exclude_cyb: bool = True
    filter_summary: str = ""


class ValueStockSimulationRequest(BaseModel):
    stocks: list[dict] = Field(default_factory=list)


class MacroCycleTaskRequest(BaseModel):
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class MacroCycleExportRequest(BaseModel):
    result: dict = Field(default_factory=dict)


class NewsFlowTaskRequest(BaseModel):
    category: Optional[str] = None
    lightweight_model: Optional[str] = None
    reasoning_model: Optional[str] = None


class NewsFlowQuickAnalysisRequest(BaseModel):
    category: Optional[str] = None


class NewsFlowAlertConfigRequest(BaseModel):
    values: dict[str, str] = Field(default_factory=dict)


class NewsFlowSchedulerConfigRequest(BaseModel):
    task_enabled: dict[str, bool] = Field(default_factory=dict)
    task_intervals: dict[str, int] = Field(default_factory=dict)


class NewsFlowExportRequest(BaseModel):
    result: dict = Field(default_factory=dict)


class FollowupStatusRequest(BaseModel):
    note: str = ""


class PortfolioStockCreateRequest(BaseModel):
    code: str
    name: Optional[str] = None
    cost_price: Optional[float] = None
    quantity: Optional[int] = None
    note: str = ""
    auto_monitor: bool = True
    account_name: str = "默认账户"
    origin_analysis_id: Optional[int] = None
    buy_date: Optional[str] = None


class PortfolioStockUpdateRequest(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    cost_price: Optional[float] = None
    quantity: Optional[int] = None
    note: Optional[str] = None
    auto_monitor: Optional[bool] = None
    account_name: Optional[str] = None


class TradeRecordCreateRequest(BaseModel):
    trade_type: str
    quantity: int
    price: float
    trade_date: Optional[str] = None
    note: str = ""


class PortfolioSchedulerAccountConfigRequest(BaseModel):
    account_name: str
    enabled: bool = True


class PortfolioSchedulerConfigRequest(BaseModel):
    schedule_times: list[str] = Field(default_factory=list)
    analysis_mode: Optional[str] = None
    max_workers: Optional[int] = None
    auto_sync_monitor: Optional[bool] = None
    send_notification: Optional[bool] = None
    selected_agents: Optional[list[str]] = None
    account_configs: Optional[list[PortfolioSchedulerAccountConfigRequest]] = None


class PriceAlertCreateRequest(BaseModel):
    symbol: str
    name: str
    rating: str = "买入"
    entry_min: float
    entry_max: float
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    check_interval: Optional[int] = None
    notification_enabled: bool = True
    trading_hours_only: bool = True
    account_name: str = "默认账户"
    origin_analysis_id: Optional[int] = None


class PriceAlertUpdateRequest(BaseModel):
    rating: str = "买入"
    entry_min: float
    entry_max: float
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    check_interval: Optional[int] = None
    notification_enabled: bool = True
    trading_hours_only: Optional[bool] = None
    managed_by_portfolio: Optional[bool] = None


class SmartMonitorTaskRequest(BaseModel):
    stock_code: str
    stock_name: Optional[str] = None
    enabled: bool = True
    check_interval: Optional[int] = None
    trading_hours_only: bool = True
    position_size_pct: int = 20
    stop_loss_pct: int = 5
    take_profit_pct: int = 10
    account_name: str = "默认账户"
    managed_by_portfolio: bool = False
    asset_id: Optional[int] = None
    portfolio_stock_id: Optional[int] = None
    origin_analysis_id: Optional[int] = None
    task_name: Optional[str] = None
    notify_email: Optional[str] = None
    notify_webhook: Optional[str] = None
    position_date: Optional[str] = None


class SmartMonitorAnalyzeRequest(BaseModel):
    stock_code: str
    notify: bool = False
    trading_hours_only: bool = True
    account_name: str = "默认账户"
    asset_id: Optional[int] = None
    portfolio_stock_id: Optional[int] = None


class SmartMonitorRuntimeConfigRequest(BaseModel):
    intraday_decision_interval_minutes: int = 60
    realtime_monitor_interval_minutes: int = 3


class PendingActionResolveRequest(BaseModel):
    status: Literal["accepted", "rejected", "ignored", "done"]
    resolution_note: str = ""


class ConfigUpdateRequest(BaseModel):
    values: dict[str, str]


class DatabaseCleanupRequest(BaseModel):
    days: int = 7


class DatabaseRestoreRequest(BaseModel):
    backup_name: str
