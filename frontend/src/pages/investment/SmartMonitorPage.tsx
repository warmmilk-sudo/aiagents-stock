import { FormEvent, useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { ModuleCard } from "../../components/common/ModuleCard";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { SchedulerControl } from "../../components/common/SchedulerControl";
import { StatusBadge } from "../../components/common/StatusBadge";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { FormattedReport } from "../../components/research/FormattedReport";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import { DEFAULT_ACCOUNT_NAME, normalizeAccountName, SUPPORTED_ACCOUNT_NAMES } from "../../lib/accounts";
import { formatDateTime } from "../../lib/datetime";
import { decodeIntent } from "../../lib/intents";
import { useSmartMonitorStore, type SmartMonitorPageCache } from "../../stores/smartMonitorStore";
import styles from "../ConsolePage.module.scss";

interface SmartMonitorTask {
  id: number;
  task_name: string;
  stock_code: string;
  stock_name?: string;
  enabled: number;
  account_name?: string;
  total_position_pct?: number;
  asset_id?: number | null;
  portfolio_stock_id?: number | null;
  has_position?: number;
  asset_status?: string | null;
  strategy_context?: {
    origin_analysis_id?: number;
    analysis_scope?: string;
    analysis_date?: string;
    summary?: string;
    rating?: string;
    entry_min?: number;
    entry_max?: number;
    take_profit?: number;
    stop_loss?: number;
    entry_range?: unknown;
    final_decision?: {
      entry_min?: unknown;
      entry_max?: unknown;
      entry_range?: unknown;
      take_profit?: unknown;
      stop_loss?: unknown;
    };
  };
}

interface DecisionItem {
  id: number;
  stock_code: string;
  stock_name?: string;
  account_name?: string;
  asset_id?: number | null;
  portfolio_stock_id?: number | null;
  action?: string;
  decision_time?: string;
  reasoning?: string;
  monitor_levels?: {
    entry_min?: number;
    entry_max?: number;
    take_profit?: number;
    stop_loss?: number;
  };
}

interface SystemStatus {
  monitor_service?: {
    running?: boolean;
  };
  monitor_scheduler?: {
    scheduler_enabled?: boolean;
    scheduler_running?: boolean;
    is_trading_day?: boolean;
    is_trading_time?: boolean;
    next_trading_time?: string;
    auto_stop?: boolean;
    market?: string;
    monitor_service_running?: boolean;
  };
}

interface RuntimeConfig {
  intraday_decision_interval_minutes: number;
  realtime_monitor_interval_minutes: number;
}

interface MonitorConfig {
  position_size_pct: number;
  total_position_pct: number;
  stop_loss_pct: number;
  take_profit_pct: number;
  source?: string;
}

interface PriceAlertNotification {
  id: number;
  symbol: string;
  name?: string;
  message: string;
  triggered_at: string;
  account_name?: string;
}

interface MonitorIntentPayload {
  symbol?: string;
  stock_name?: string;
  account_name?: string;
  origin_analysis_id?: number;
  strategy_context?: Record<string, unknown>;
}

type ComposerPanel = "task" | null;
type NotificationTone = "danger" | "success" | "warning" | "info";
type ResultPanel = "decisions" | "notifications";
type SectionKey = "results" | "tasks" | "controls";

const sectionTabs: Array<{ key: SectionKey; label: string }> = [
  { key: "results", label: "监控结果" },
  { key: "tasks", label: "任务列表" },
  { key: "controls", label: "盯盘配置" },
];

const DEFAULT_TASK_RISK = {
  position_size_pct: "20",
  total_position_pct: "100",
  stop_loss_pct: "5",
  take_profit_pct: "10",
};

const defaultTaskForm = {
  stock_code: "",
  stock_name: "",
  account_name: DEFAULT_ACCOUNT_NAME,
  task_name: "",
  position_size_pct: DEFAULT_TASK_RISK.position_size_pct,
  total_position_pct: DEFAULT_TASK_RISK.total_position_pct,
  stop_loss_pct: DEFAULT_TASK_RISK.stop_loss_pct,
  take_profit_pct: DEFAULT_TASK_RISK.take_profit_pct,
  trading_hours_only: true,
  enabled: true,
  origin_analysis_id: undefined as number | undefined,
  strategy_context: {} as Record<string, unknown>,
};

const defaultRuntimeConfig: RuntimeConfig = {
  intraday_decision_interval_minutes: 60,
  realtime_monitor_interval_minutes: 3,
};

const notificationToneClass: Record<NotificationTone, string> = {
  danger: styles.noticeDanger,
  success: styles.noticeSuccess,
  warning: styles.noticeWarning,
  info: styles.noticeInfo,
};

function decisionBadge(
  value?: string,
  isHolding = false,
): { label: string; tone: "success" | "danger" | "warning" | "default" } {
  const normalized = String(value || "").toLowerCase();
  if (normalized === "buy") return { label: isHolding ? "加仓" : "买入", tone: "success" };
  if (normalized === "sell") return { label: isHolding ? "卖出" : "卖出信号", tone: "danger" };
  if (normalized === "hold") return { label: isHolding ? "持有" : "观望", tone: "warning" };
  return { label: value || "未知", tone: "default" };
}

const formatThresholdValue = (value?: unknown): string => {
  if (value == null) {
    return "N/A";
  }
  if (typeof value === "number") {
    return Number.isFinite(value) && value > 0
      ? value.toLocaleString("zh-CN", { maximumFractionDigits: 2 })
      : "N/A";
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? trimmed : "N/A";
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const min = record.min ?? record.low ?? record.start;
    const max = record.max ?? record.high ?? record.end;
    if (min != null || max != null) {
      const minText: string = formatThresholdValue(min);
      const maxText: string = formatThresholdValue(max);
      if (minText === "N/A" && maxText === "N/A") {
        return "N/A";
      }
      if (minText === maxText) {
        return minText;
      }
      return `${minText}-${maxText}`;
    }
    const text = record.text ?? record.value ?? record.range;
    if (typeof text === "string" || typeof text === "number") {
      return formatThresholdValue(text);
    }
  }
  return "N/A";
};

const formatThresholdSummary = (
  label: string,
  levels?: {
    entry_min?: unknown;
    entry_max?: unknown;
    take_profit?: unknown;
    stop_loss?: unknown;
    entry_range?: unknown;
    final_decision?: {
      entry_min?: unknown;
      entry_max?: unknown;
      entry_range?: unknown;
      take_profit?: unknown;
      stop_loss?: unknown;
    };
  } | null,
) => {
  if (!levels) {
    return `${label}：暂无关键指标`;
  }
  const fallback = levels.final_decision ?? {};
  const entryRange = formatThresholdValue(levels.entry_range ?? fallback.entry_range);
  const entryMin = formatThresholdValue(levels.entry_min ?? fallback.entry_min);
  const entryMax = formatThresholdValue(levels.entry_max ?? fallback.entry_max);
  const takeProfit = formatThresholdValue(levels.take_profit ?? fallback.take_profit);
  const stopLoss = formatThresholdValue(levels.stop_loss ?? fallback.stop_loss);
  if ([entryRange, entryMin, entryMax, takeProfit, stopLoss].every((value) => value === "N/A")) {
    return `${label}：暂无关键指标`;
  }
  const entryText =
    entryRange !== "N/A"
      ? entryRange
      : [entryMin, entryMax].every((value) => value === "N/A")
        ? "N/A"
        : `${entryMin}-${entryMax}`;
  return `${label}：入场${entryText} | 止盈:${takeProfit} | 止损:${stopLoss}`;
};

const taskPortfolioLabel = (task: SmartMonitorTask) =>
  Boolean(task.has_position) || task.asset_status === "portfolio" ? task.account_name || "在持仓" : "未持仓";

const matchesTaskDecision = (task: SmartMonitorTask, decision: DecisionItem) => {
  if (task.stock_code !== decision.stock_code) {
    return false;
  }
  if (task.asset_id != null && decision.asset_id != null) {
    return Number(task.asset_id) === Number(decision.asset_id);
  }
  if (task.portfolio_stock_id != null && decision.portfolio_stock_id != null) {
    return Number(task.portfolio_stock_id) === Number(decision.portfolio_stock_id);
  }
  return normalizeAccountName(task.account_name) === normalizeAccountName(decision.account_name);
};

const isDecisionHolding = (decision: DecisionItem, tasks: SmartMonitorTask[]) => {
  const matchedTask = tasks.find((task) => matchesTaskDecision(task, decision));
  if (matchedTask) {
    return Boolean(matchedTask.has_position) || matchedTask.asset_status === "portfolio";
  }
  return decision.portfolio_stock_id != null;
};

const isSellDecision = (decision: DecisionItem) => String(decision.action || "").toUpperCase() === "SELL";

const isVisibleDecision = (decision: DecisionItem, tasks: SmartMonitorTask[]) =>
  isDecisionHolding(decision, tasks) || !isSellDecision(decision);

const findLatestDecisionForTask = (task: SmartMonitorTask, decisions: DecisionItem[]) =>
  decisions.find((decision) => matchesTaskDecision(task, decision) && (Boolean(task.has_position) || task.asset_status === "portfolio" || !isSellDecision(decision)))
  || decisions.find((decision) => decision.stock_code === task.stock_code && !isSellDecision(decision));

const notificationMeta = (message: string): { tone: NotificationTone; label: string } => {
  if (/(止损|跌破|下破|失守|回撤)/.test(message)) return { tone: "danger", label: "风险预警" };
  if (/(止盈|突破|上破|目标价|盈利)/.test(message)) return { tone: "success", label: "收益信号" };
  if (/(买入|入场|区间|接近)/.test(message)) return { tone: "info", label: "关注提醒" };
  return { tone: "warning", label: "价格提醒" };
};

const PAGE_CACHE_TTL_MS = 20_000;

function stockDisplayName(name?: string, code?: string): string {
  if (name && code) {
    return `${name}（${code}）`;
  }
  return name || code || "未知标的";
}

function applySharedRiskDefaults(
  current: typeof defaultTaskForm,
  monitorConfig: MonitorConfig | null,
) {
  return {
    ...current,
    position_size_pct: String(monitorConfig?.position_size_pct ?? DEFAULT_TASK_RISK.position_size_pct),
    total_position_pct: String(monitorConfig?.total_position_pct ?? DEFAULT_TASK_RISK.total_position_pct),
    stop_loss_pct: String(monitorConfig?.stop_loss_pct ?? DEFAULT_TASK_RISK.stop_loss_pct),
    take_profit_pct: String(monitorConfig?.take_profit_pct ?? DEFAULT_TASK_RISK.take_profit_pct),
  };
}

export function SmartMonitorPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const enabledOnly = useSmartMonitorStore((state) => state.enabledOnly);
  const setEnabledOnly = useSmartMonitorStore((state) => state.setEnabledOnly);
  const [filterAccount, setFilterAccount] = useState<string>("all");
  const [filterHasPosition, setFilterHasPosition] = useState<string>("all");

  const cacheModeKey = `${enabledOnly ? "enabled" : "all"}-${filterAccount}-${filterHasPosition}`;
  const cachedPage = useSmartMonitorStore((state) => state.pageCacheByMode[cacheModeKey] ?? null);
  const setPageCache = useSmartMonitorStore((state) => state.setPageCache);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(() => (cachedPage?.systemStatus as SystemStatus | null) ?? null);
  const [tasks, setTasks] = useState<SmartMonitorTask[]>(() => (cachedPage?.tasks as SmartMonitorTask[]) ?? []);
  const [decisions, setDecisions] = useState<DecisionItem[]>(() => (cachedPage?.decisions as DecisionItem[]) ?? []);
  const [notifications, setNotifications] = useState<PriceAlertNotification[]>(() => (cachedPage?.notifications as PriceAlertNotification[]) ?? []);
  const [monitorConfig, setMonitorConfig] = useState<MonitorConfig | null>(() => (cachedPage?.monitorConfig as MonitorConfig | null) ?? null);
  const [runtimeConfig, setRuntimeConfig] = useState<RuntimeConfig>(() => (cachedPage?.runtimeConfig as RuntimeConfig | null) ?? defaultRuntimeConfig);
  const [taskForm, setTaskForm] = useState(() => applySharedRiskDefaults(defaultTaskForm, (cachedPage?.monitorConfig as MonitorConfig | null) ?? null));
  const [activePanel, setActivePanel] = useState<ComposerPanel>(null);
  const [activeResultPanel, setActiveResultPanel] = useState<ResultPanel>("decisions");
  const [section, setSection] = useState<SectionKey>("results");
  const [isRunningAllTasks, setIsRunningAllTasks] = useState(false);
  const [isSavingTask, setIsSavingTask] = useState(false);
  const [isSavingMonitorConfig, setIsSavingMonitorConfig] = useState(false);
  const [isTogglingService, setIsTogglingService] = useState(false);
  const [pendingRunTaskId, setPendingRunTaskId] = useState<number | null>(null);
  const [pendingToggleTaskId, setPendingToggleTaskId] = useState<number | null>(null);
  const [pendingDeleteTaskId, setPendingDeleteTaskId] = useState<number | null>(null);
  const [pendingNotificationId, setPendingNotificationId] = useState<number | null>(null);
  const { message, error, clear, showError, showMessage } = usePageFeedback();

  const applyPageCache = (cache: SmartMonitorPageCache | null) => {
    if (!cache) {
      return;
    }
    setSystemStatus((cache.systemStatus as SystemStatus | null) ?? null);
    setTasks(cache.tasks as SmartMonitorTask[]);
    setDecisions(cache.decisions as DecisionItem[]);
    setNotifications(cache.notifications as PriceAlertNotification[]);
    setMonitorConfig((cache.monitorConfig as MonitorConfig | null) ?? null);
    setRuntimeConfig((cache.runtimeConfig as RuntimeConfig | null) ?? defaultRuntimeConfig);
  };

  const setServiceEnabledOptimistically = (enabled: boolean) => {
    setSystemStatus((current) => ({
      ...(current ?? {}),
      monitor_service: {
        ...(current?.monitor_service ?? {}),
        running: enabled,
      },
      monitor_scheduler: {
        ...(current?.monitor_scheduler ?? {}),
        scheduler_enabled: enabled,
        monitor_service_running: enabled,
      },
    }));
  };

  const loadAll = async (force = false) => {
    if (!force && cachedPage && Date.now() - cachedPage.updatedAt < PAGE_CACHE_TTL_MS) {
      applyPageCache(cachedPage);
      return;
    }
    const queryParams: Record<string, string | boolean> = { enabled_only: enabledOnly };
    if (filterAccount !== "all") {
      queryParams.account_name = filterAccount;
    }
    if (filterHasPosition !== "all") {
      queryParams.has_position = filterHasPosition === "true";
    }

    const [statusData, taskData, decisionData, notificationData, monitorConfigData, runtimeData] = await Promise.all([
      apiFetch<SystemStatus>("/api/system/status"),
      apiFetch<SmartMonitorTask[]>(`/api/smart-monitor/tasks${buildQuery(queryParams)}`),
      apiFetch<DecisionItem[]>("/api/smart-monitor/decisions?limit=30"),
      apiFetch<PriceAlertNotification[]>("/api/price-alerts/notifications?limit=12"),
      apiFetch<MonitorConfig>("/api/smart-monitor/config"),
      apiFetch<RuntimeConfig>("/api/smart-monitor/runtime-config"),
    ]);
    setSystemStatus(statusData);
    setTasks(taskData);
    setDecisions(decisionData);
    setNotifications(notificationData);
    setMonitorConfig(monitorConfigData);
    setRuntimeConfig(runtimeData);
    setPageCache(cacheModeKey, {
      systemStatus: statusData,
      tasks: taskData,
      decisions: decisionData,
      notifications: notificationData,
      monitorConfig: monitorConfigData,
      runtimeConfig: runtimeData,
      updatedAt: Date.now(),
    });
  };

  useEffect(() => {
    if (cachedPage) {
      applyPageCache(cachedPage);
    }
    if (cachedPage && Date.now() - cachedPage.updatedAt < PAGE_CACHE_TTL_MS) {
      return;
    }
    void loadAll(Boolean(cachedPage));
  }, [cacheModeKey, cachedPage?.updatedAt]);

  useEffect(() => {
    if (!monitorConfig) {
      return;
    }
    setTaskForm((current) => {
      const isUsingFallbackRisk =
        current.position_size_pct === DEFAULT_TASK_RISK.position_size_pct
        && current.total_position_pct === DEFAULT_TASK_RISK.total_position_pct
        && current.stop_loss_pct === DEFAULT_TASK_RISK.stop_loss_pct
        && current.take_profit_pct === DEFAULT_TASK_RISK.take_profit_pct;
      if (!isUsingFallbackRisk) {
        return current;
      }
      return applySharedRiskDefaults(current, monitorConfig);
    });
  }, [monitorConfig]);

  useEffect(() => {
    const rawIntent = searchParams.get("intent");
    const intent = decodeIntent<MonitorIntentPayload>(rawIntent);
    if (!intent || !["watchlist", "smart_monitor", "ai_monitor", "price_alert"].includes(intent.type)) return;
    const payload = intent.payload || {};
    if (intent.type === "price_alert") {
      if (rawIntent) {
        navigate(`/investment/price-alerts?intent=${rawIntent}`, { replace: true });
      }
      return;
    } else {
      setTaskForm((current) => ({
        ...applySharedRiskDefaults(current, monitorConfig),
        stock_code: payload.symbol || "",
        stock_name: payload.stock_name || "",
        account_name: normalizeAccountName(payload.account_name) || DEFAULT_ACCOUNT_NAME,
        task_name: `${payload.stock_name || payload.symbol || ""}盯盘`,
        origin_analysis_id: payload.origin_analysis_id,
        strategy_context: payload.strategy_context || {},
      }));
      setActivePanel("task");
      setSection("tasks");
    }
    const nextParams = new URLSearchParams(searchParams);
    nextParams.delete("intent");
    setSearchParams(nextParams, { replace: true });
  }, [monitorConfig, navigate, searchParams, setSearchParams]);

  const submitTask = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    clear();
    setIsSavingTask(true);
    try {
      const savedTask = await apiFetch<{ task_id: number }>("/api/smart-monitor/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_code: taskForm.stock_code,
          stock_name: taskForm.stock_name || taskForm.stock_code,
          account_name: normalizeAccountName(taskForm.account_name) || DEFAULT_ACCOUNT_NAME,
          task_name: taskForm.task_name || `${taskForm.stock_name || taskForm.stock_code}盯盘`,
          position_size_pct: Number(taskForm.position_size_pct),
          total_position_pct: Number(taskForm.total_position_pct),
          stop_loss_pct: Number(taskForm.stop_loss_pct),
          take_profit_pct: Number(taskForm.take_profit_pct),
          trading_hours_only: taskForm.trading_hours_only,
          enabled: taskForm.enabled,
          origin_analysis_id: taskForm.origin_analysis_id ?? null,
          strategy_context: taskForm.strategy_context || {},
        }),
      });
      await apiFetch(`/api/smart-monitor/tasks/${savedTask.task_id}/run-once`, { method: "POST" });
      setTaskForm({
        ...applySharedRiskDefaults(defaultTaskForm, monitorConfig),
        account_name: normalizeAccountName(taskForm.account_name) || DEFAULT_ACCOUNT_NAME,
      });
      setActivePanel(null);
      showMessage(`盯盘任务已保存并完成一次盘中决策：${taskForm.stock_code}`);
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "保存任务失败");
    } finally {
      setIsSavingTask(false);
    }
  };

  const saveMonitorConfig = async () => {
    clear();
    setIsSavingMonitorConfig(true);
    try {
      const [nextConfig, nextRuntime] = await Promise.all([
        apiFetch<MonitorConfig>("/api/smart-monitor/config", {
          method: "PUT",
          body: JSON.stringify({
            position_size_pct: monitorConfig?.position_size_pct ?? Number(DEFAULT_TASK_RISK.position_size_pct),
            total_position_pct: monitorConfig?.total_position_pct ?? Number(DEFAULT_TASK_RISK.total_position_pct),
            stop_loss_pct: monitorConfig?.stop_loss_pct ?? Number(DEFAULT_TASK_RISK.stop_loss_pct),
            take_profit_pct: monitorConfig?.take_profit_pct ?? Number(DEFAULT_TASK_RISK.take_profit_pct),
          }),
        }),
        apiFetch<RuntimeConfig>("/api/smart-monitor/runtime-config", {
          method: "PUT",
          body: JSON.stringify(runtimeConfig),
        }),
      ]);
      setMonitorConfig(nextConfig);
      setRuntimeConfig(nextRuntime);
      setTaskForm((current) => applySharedRiskDefaults(current, nextConfig));
      showMessage("盯盘配置已更新");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "保存盯盘配置失败");
    } finally {
      setIsSavingMonitorConfig(false);
    }
  };

  const updateMonitorRiskValue = (key: keyof Omit<MonitorConfig, "source">, value: number) => {
    setMonitorConfig((current) => ({
      position_size_pct: current?.position_size_pct ?? Number(DEFAULT_TASK_RISK.position_size_pct),
      total_position_pct: current?.total_position_pct ?? Number(DEFAULT_TASK_RISK.total_position_pct),
      stop_loss_pct: current?.stop_loss_pct ?? Number(DEFAULT_TASK_RISK.stop_loss_pct),
      take_profit_pct: current?.take_profit_pct ?? Number(DEFAULT_TASK_RISK.take_profit_pct),
      source: current?.source,
      [key]: value,
    }));
  };

  const renderRiskSlider = (
    id: string,
    label: string,
    value: string,
    onChange: (nextValue: string) => void,
  ) => (
    <div className={styles.field}>
      <label htmlFor={id}>{label}：{value}%</label>
      <input
        id={id}
        className={styles.slider}
        max="100"
        min="0"
        onChange={(event) => onChange(event.target.value)}
        step="1"
        type="range"
        value={value}
      />
    </div>
  );

  const renderConfigSlider = (
    id: keyof Omit<MonitorConfig, "source">,
    label: string,
    minimum = 0,
    maximum = 100,
  ) => (
    <div className={styles.field}>
      <label htmlFor={String(id)}>{label}：{monitorConfig?.[id] ?? 0}%</label>
      <input
        id={String(id)}
        className={styles.slider}
        max={String(maximum)}
        min={String(minimum)}
        onChange={(event) => updateMonitorRiskValue(id, Number(event.target.value) || 0)}
        step="1"
        type="range"
        value={monitorConfig?.[id] ?? 0}
      />
    </div>
  );

  const toggleMonitorService = async (enabled: boolean) => {
    clear();
    if (isTogglingService) {
      return;
    }
    const previousStatus = systemStatus;
    setIsTogglingService(true);
    setServiceEnabledOptimistically(enabled);
    try {
      await apiFetch(enabled ? "/api/system/monitor-service/start" : "/api/system/monitor-service/stop", { method: "POST" });
      showMessage(enabled ? "监控服务已启动" : "监控服务已停止");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      setSystemStatus(previousStatus);
      showError(requestError instanceof ApiRequestError ? requestError.message : "操作失败");
    } finally {
      setIsTogglingService(false);
    }
  };

  const runTaskOnce = async (task: SmartMonitorTask) => {
    clear();
    if (pendingRunTaskId === task.id) {
      return;
    }
    setPendingRunTaskId(task.id);
    try {
      await apiFetch(`/api/smart-monitor/tasks/${task.id}/run-once`, { method: "POST" });
      showMessage(`已执行 ${task.stock_code} 的一次盘中决策`);
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "操作失败");
    } finally {
      setPendingRunTaskId((current) => current === task.id ? null : current);
    }
  };

  const toggleTaskEnabled = async (task: SmartMonitorTask) => {
    clear();
    if (pendingToggleTaskId === task.id) {
      return;
    }
    const previousEnabled = Boolean(task.enabled);
    setPendingToggleTaskId(task.id);
    setTasks((current) =>
      current.map((item) =>
        item.id === task.id
          ? { ...item, enabled: previousEnabled ? 0 : 1 }
          : item,
      ),
    );
    try {
      await apiFetch(`/api/smart-monitor/tasks/${task.id}/enable?enabled=${String(!previousEnabled)}`, { method: "POST" });
      showMessage(previousEnabled ? "任务已停用" : "任务已启用");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      setTasks((current) =>
        current.map((item) =>
          item.id === task.id
            ? { ...item, enabled: previousEnabled ? 1 : 0 }
            : item,
        ),
      );
      showError(requestError instanceof ApiRequestError ? requestError.message : "更新任务状态失败");
    } finally {
      setPendingToggleTaskId((current) => current === task.id ? null : current);
    }
  };

  const deleteTask = async (task: SmartMonitorTask) => {
    clear();
    if (pendingDeleteTaskId === task.id) {
      return;
    }
    const removedIndex = tasks.findIndex((item) => item.id === task.id);
    const removedTask = tasks[removedIndex];
    setPendingDeleteTaskId(task.id);
    setTasks((current) => current.filter((item) => item.id !== task.id));
    try {
      await apiFetch(`/api/smart-monitor/tasks/${task.id}`, { method: "DELETE" });
      showMessage("任务已删除");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      if (removedTask) {
        setTasks((current) => {
          if (current.some((item) => item.id === removedTask.id)) {
            return current;
          }
          const next = [...current];
          next.splice(Math.max(0, Math.min(removedIndex, next.length)), 0, removedTask);
          return next;
        });
      }
      showError(requestError instanceof ApiRequestError ? requestError.message : "删除任务失败");
    } finally {
      setPendingDeleteTaskId((current) => current === task.id ? null : current);
    }
  };

  const runAllTasksOnce = async () => {
    clear();
    setIsRunningAllTasks(true);
    try {
      const queryParams: Record<string, string | boolean> = { enabled_only: true };
      if (filterAccount !== "all") {
        queryParams.account_name = filterAccount;
      }
      if (filterHasPosition !== "all") {
        queryParams.has_position = filterHasPosition === "true";
      }
      const result = await apiFetch<{
        task_total: number;
        task_success: number;
        price_alert_total: number;
        price_alert_success: number;
      }>(`/api/smart-monitor/tasks/run-once${buildQuery(queryParams)}`, { method: "POST" });
      showMessage(
        `已执行 ${result.task_total} 个盯盘任务、${result.price_alert_total} 个价格监控；AI成功 ${result.task_success} 个，价格检查成功 ${result.price_alert_success} 个。`,
      );
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "批量盘中决策失败");
    } finally {
      setIsRunningAllTasks(false);
    }
  };

  const ignoreNotification = async (eventId: number) => {
    clear();
    if (pendingNotificationId === eventId) {
      return;
    }
    const removedIndex = notifications.findIndex((item) => item.id === eventId);
    const removedNotification = notifications[removedIndex];
    setPendingNotificationId(eventId);
    setNotifications((current) => current.filter((item) => item.id !== eventId));
    try {
      await apiFetch(`/api/price-alerts/notifications/${eventId}/ignore`, { method: "POST" });
      showMessage("预警通知已忽略");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      if (removedNotification) {
        setNotifications((current) => {
          if (current.some((item) => item.id === removedNotification.id)) {
            return current;
          }
          const next = [...current];
          next.splice(Math.max(0, Math.min(removedIndex, next.length)), 0, removedNotification);
          return next;
        });
      }
      showError(requestError instanceof ApiRequestError ? requestError.message : "忽略预警通知失败");
    } finally {
      setPendingNotificationId((current) => current === eventId ? null : current);
    }
  };

  const allAccountsList = Array.from(
    new Set([
      ...SUPPORTED_ACCOUNT_NAMES,
      ...(filterAccount !== "all" ? [normalizeAccountName(filterAccount) || DEFAULT_ACCOUNT_NAME] : []),
      ...tasks.map((task) => normalizeAccountName(task.account_name) || DEFAULT_ACCOUNT_NAME),
    ]),
  );
  const schedulerEnabled = Boolean(systemStatus?.monitor_scheduler?.scheduler_enabled ?? systemStatus?.monitor_service?.running);
  const serviceRunning = Boolean(systemStatus?.monitor_service?.running);
  const waitingTradingWindow = schedulerEnabled && !serviceRunning && !Boolean(systemStatus?.monitor_scheduler?.is_trading_time);
  const serviceStatusHint = waitingTradingWindow
    ? `当前非交易时段，已启用后会在 ${systemStatus?.monitor_scheduler?.next_trading_time || "下个交易窗口"} 自动执行。`
    : null;
  const runnableTaskCount = tasks.filter((item) => Boolean(item.enabled)).length;
  const latestDecisions = decisions.reduce<DecisionItem[]>((accumulator, item) => {
    const key = `${item.stock_code || ""}::${item.asset_id ?? ""}::${item.portfolio_stock_id ?? ""}::${normalizeAccountName(item.account_name) || DEFAULT_ACCOUNT_NAME}`;
    if (!accumulator.some((existing) => `${existing.stock_code || ""}::${existing.asset_id ?? ""}::${existing.portfolio_stock_id ?? ""}::${normalizeAccountName(existing.account_name) || DEFAULT_ACCOUNT_NAME}` === key)) {
      accumulator.push(item);
    }
    return accumulator;
  }, []);
  const visibleDecisions = latestDecisions.filter((item) => isVisibleDecision(item, tasks));
  const latestNotifications = notifications.reduce<PriceAlertNotification[]>((accumulator, item) => {
    const key = `${item.symbol || ""}::${normalizeAccountName(item.account_name) || DEFAULT_ACCOUNT_NAME}`;
    if (!accumulator.some((existing) => `${existing.symbol || ""}::${normalizeAccountName(existing.account_name) || DEFAULT_ACCOUNT_NAME}` === key)) {
      accumulator.push(item);
    }
    return accumulator;
  }, []);

  const renderTaskForm = () => (
    <form className={styles.moduleSection} onSubmit={submitTask}>
      <div className={styles.formGrid}>
        <div className={styles.field}><label htmlFor="task-code">股票代码</label><input id="task-code" onChange={(event) => setTaskForm((current) => ({ ...current, stock_code: event.target.value }))} value={taskForm.stock_code} /></div>
        <div className={styles.field}><label htmlFor="task-name">股票名称</label><input id="task-name" onChange={(event) => setTaskForm((current) => ({ ...current, stock_name: event.target.value }))} value={taskForm.stock_name} /></div>
        <div className={styles.field}>
          <label htmlFor="task-account">账户</label>
          <select
            id="task-account"
            onChange={(event) => setTaskForm((current) => ({ ...current, account_name: event.target.value || DEFAULT_ACCOUNT_NAME }))}
            value={taskForm.account_name}
          >
            {SUPPORTED_ACCOUNT_NAMES.map((account) => (
              <option key={account} value={account}>
                {account}
              </option>
            ))}
          </select>
        </div>
        <div className={styles.field}><label htmlFor="task-title">任务名称</label><input id="task-title" onChange={(event) => setTaskForm((current) => ({ ...current, task_name: event.target.value }))} value={taskForm.task_name} /></div>
        {renderRiskSlider("task-position", "单票仓位", taskForm.position_size_pct, (nextValue) => setTaskForm((current) => ({ ...current, position_size_pct: nextValue })))}
        {renderRiskSlider("task-total-position", "总仓位上限", taskForm.total_position_pct, (nextValue) => setTaskForm((current) => ({ ...current, total_position_pct: nextValue })))}
        {renderRiskSlider("task-stop", "止损", taskForm.stop_loss_pct, (nextValue) => setTaskForm((current) => ({ ...current, stop_loss_pct: nextValue })))}
        {renderRiskSlider("task-profit", "止盈", taskForm.take_profit_pct, (nextValue) => setTaskForm((current) => ({ ...current, take_profit_pct: nextValue })))}
      </div>
      <div className={styles.actions}>
        <button className={styles.primaryButton} disabled={isSavingTask} type="submit">
          {isSavingTask ? "保存中..." : "保存任务"}
        </button>
        <button className={styles.secondaryButton} onClick={() => setActivePanel(null)} type="button">取消</button>
      </div>
    </form>
  );

  const renderSharedMonitorConfig = () => (
    <div className={styles.moduleSection}>
      <h3 className={styles.mobileDuplicateHeading}>共享风控设置</h3>
      <div className={styles.noticeCard}>
        <div className={styles.noticeMeta}>
          <strong>全部账户共用</strong>
          <StatusBadge
            label={monitorConfig?.source === "shared_custom" ? "自定义" : "默认值"}
            tone={monitorConfig?.source === "shared_custom" ? "success" : "default"}
          />
        </div>
        <div className={styles.formGrid}>
          {renderConfigSlider("position_size_pct", "单票仓位")}
          {renderConfigSlider("total_position_pct", "总仓位上限")}
          {renderConfigSlider("stop_loss_pct", "止损")}
          {renderConfigSlider("take_profit_pct", "止盈")}
        </div>
      </div>
    </div>
  );

  return (
    <PageFrame
      title="智能盯盘"
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      sectionTabs={sectionTabs}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        {section === "results" ? (
          <ModuleCard
            title="监控结果"
            summary="最新决策、预警通知和运行提示统一收拢到同一模块。"
            hideTitleOnMobile
          >
            <div className={styles.moduleSection}>
              <label className={styles.switchField}>
                <span className={styles.switchLabel}>启用监控服务</span>
                <span className={styles.switchControl}>
                  <input
                    checked={schedulerEnabled}
                    disabled={isTogglingService || isSavingMonitorConfig}
                    onChange={(event) => void toggleMonitorService(event.target.checked)}
                    type="checkbox"
                  />
                  <span className={styles.switchTrack} aria-hidden="true">
                    <span className={styles.switchThumb} />
                  </span>
                </span>
              </label>
              {serviceStatusHint ? <p className={styles.helperText}>{serviceStatusHint}</p> : null}
            </div>

            <div className={styles.moduleSection}>
              <div className={styles.dualToggleGrid}>
                <button
                  className={activeResultPanel === "decisions" ? styles.primaryButton : styles.secondaryButton}
                  onClick={() => setActiveResultPanel("decisions")}
                  type="button"
                >
                  最新决策
                  {visibleDecisions.length ? ` (${visibleDecisions.length})` : ""}
                </button>
                <button
                  className={activeResultPanel === "notifications" ? styles.primaryButton : styles.secondaryButton}
                  onClick={() => setActiveResultPanel("notifications")}
                  type="button"
                >
                  价格预警通知
                  {latestNotifications.length ? ` (${latestNotifications.length})` : ""}
                </button>
              </div>
              <div className={styles.list}>
                {activeResultPanel === "decisions"
                  ? (
                    visibleDecisions.map((item) => {
                      const badge = decisionBadge(item.action, isDecisionHolding(item, tasks));
                      const toneClass =
                        badge.tone === "success"
                          ? styles.noticeSuccess
                          : badge.tone === "danger"
                            ? styles.noticeDanger
                            : badge.tone === "warning"
                              ? styles.noticeWarning
                              : styles.noticeInfo;
                      return (
                        <div className={`${styles.noticeCard} ${toneClass}`} key={item.id}>
                          <div className={styles.noticeMeta}>
                            <StatusBadge label={badge.label} tone={badge.tone} />
                          </div>
                          <strong>{stockDisplayName(item.stock_name, item.stock_code)}</strong>
                          <small className={styles.muted}>{formatDateTime(item.decision_time, "暂无时间")}</small>
                          <FormattedReport content={item.reasoning || "暂无盘中决策内容"} />
                        </div>
                      );
                    })
                  ) : (
                    latestNotifications.map((item) => {
                      const meta = notificationMeta(item.message);
                      return (
                        <div className={`${styles.noticeCard} ${notificationToneClass[meta.tone]}`} key={item.id}>
                          <div className={styles.noticeMeta}>
                            <StatusBadge label={meta.label} tone={meta.tone} />
                            <strong>{stockDisplayName(item.name, item.symbol)}</strong>
                          </div>
                          <div>{item.message}</div>
                          <small className={styles.muted}>
                            {formatDateTime(item.triggered_at, "暂无时间")} | {normalizeAccountName(item.account_name) || DEFAULT_ACCOUNT_NAME}
                          </small>
                          <div className={styles.actions}>
                            <button
                              className={styles.secondaryButton}
                              disabled={pendingNotificationId === item.id}
                              onClick={() => void ignoreNotification(item.id)}
                              type="button"
                            >
                              {pendingNotificationId === item.id ? "忽略中..." : "忽略"}
                            </button>
                          </div>
                        </div>
                      );
                    })
                  )}
                {activeResultPanel === "decisions" && !visibleDecisions.length ? <div className={styles.muted}>暂无盘中决策</div> : null}
                {activeResultPanel === "notifications" && !latestNotifications.length ? <div className={styles.noticeCard}><div>当前没有需要处理的预警通知。</div></div> : null}
              </div>
            </div>
          </ModuleCard>
        ) : null}

        {section === "tasks" ? (
          <ModuleCard
            title="监控任务"
            summary="盯盘任务和筛选条件统一保留在任务模块中，托管预警跟随任务启停。"
            hideTitleOnMobile
            toolbar={(
              <button
                className={activePanel === "task" ? styles.primaryButton : styles.secondaryButton}
                onClick={() => setActivePanel((current) => current === "task" ? null : "task")}
                type="button"
              >
                新增任务
              </button>
            )}
          >
            {activePanel === "task" ? renderTaskForm() : null}

            <div className={styles.moduleSection}>
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="smart-monitor-account-filter">账户</label>
                  <select
                    id="smart-monitor-account-filter"
                    value={filterAccount}
                    onChange={(event) => setFilterAccount(event.target.value)}
                  >
                    <option value="all">不限账户</option>
                    {allAccountsList.map((account) => (
                      <option key={account} value={account}>
                        {account}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
              <div className={styles.dualToggleGrid}>
                <label className={`${styles.switchField} ${styles.filterSwitchField}`}>
                  <span className={styles.switchBody}>
                    <span className={styles.switchLabel}>仅看启用</span>
                  </span>
                  <span className={styles.switchControl}>
                    <input checked={enabledOnly} onChange={(event) => setEnabledOnly(event.target.checked)} type="checkbox" />
                    <span className={styles.switchTrack} aria-hidden="true">
                      <span className={styles.switchThumb} />
                    </span>
                  </span>
                </label>
                <label className={`${styles.switchField} ${styles.filterSwitchField}`}>
                  <span className={styles.switchBody}>
                    <span className={styles.switchLabel}>仅看已持仓</span>
                  </span>
                  <span className={styles.switchControl}>
                    <input
                      checked={filterHasPosition === "true"}
                      onChange={(event) => setFilterHasPosition(event.target.checked ? "true" : "all")}
                      type="checkbox"
                    />
                    <span className={styles.switchTrack} aria-hidden="true">
                      <span className={styles.switchThumb} />
                    </span>
                  </span>
                </label>
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3 className={styles.mobileDuplicateHeading}>盯盘任务列表</h3>
              <div className={styles.actions}>
                <button
                  className={styles.primaryButton}
                  disabled={isRunningAllTasks || runnableTaskCount <= 0}
                  onClick={() => void runAllTasksOnce()}
                  type="button"
                >
                  {isRunningAllTasks ? "执行中..." : "全部盘中决策"}
                </button>
                <span className={styles.muted}>执行当前筛选范围内已启用任务，盘中决策和价格检查。</span>
              </div>
              <div className={styles.list}>
                {tasks.map((task) => {
                  const latestDecision = findLatestDecisionForTask(task, decisions);
                  return (
                    <div className={styles.smartMonitorTaskCard} key={task.id}>
                      <div className={styles.noticeMeta}>
                        <strong className={styles.smartMonitorTaskTitle}>
                          {`${stockDisplayName(task.stock_name, task.stock_code)} | ${taskPortfolioLabel(task)}`}
                        </strong>
                        <button
                          className={Boolean(task.enabled) ? styles.smartMonitorStateToggleEnabled : styles.smartMonitorStateToggleDisabled}
                          disabled={pendingToggleTaskId === task.id}
                          onClick={() => void toggleTaskEnabled(task)}
                          type="button"
                        >
                          {pendingToggleTaskId === task.id ? "处理中..." : Boolean(task.enabled) ? "启用" : "停用"}
                        </button>
                      </div>
                      <p className={styles.taskIndicatorText}>{formatThresholdSummary("分析基线", task.strategy_context)}</p>
                      <p className={styles.taskIndicatorText}>{formatThresholdSummary("盘中决策", latestDecision?.monitor_levels)}</p>
                      <div className={styles.actions}>
                        <button
                          className={styles.secondaryButton}
                          disabled={pendingRunTaskId === task.id}
                          onClick={() => void runTaskOnce(task)}
                          type="button"
                        >
                          {pendingRunTaskId === task.id ? "执行中..." : "立即分析"}
                        </button>
                        <button
                          className={styles.dangerButton}
                          disabled={pendingDeleteTaskId === task.id}
                          onClick={() => void deleteTask(task)}
                          type="button"
                        >
                          {pendingDeleteTaskId === task.id ? "删除中..." : "删除"}
                        </button>
                      </div>
                    </div>
                  );
                })}
                {!tasks.length ? <div className={styles.muted}>暂无智能盯盘任务</div> : null}
              </div>
            </div>
          </ModuleCard>
        ) : null}

        {section === "controls" ? (
          <ModuleCard hideTitleOnMobile title="盯盘配置" summary="共享风控、运行节奏和服务启停统一放在一个配置模块。">
            <SchedulerControl
              enabled={schedulerEnabled}
              label="启用监控服务"
              showToggle={false}
              busy={isSavingMonitorConfig || isTogglingService}
              onSave={() => void saveMonitorConfig()}
              onToggle={(next) =>
                void toggleMonitorService(next)
              }
              saveLabel="保存盯盘配置"
              scheduleFields={(
                <>
                  {renderSharedMonitorConfig()}
                  <p className={styles.helperText}>自动盯盘仅在周一至周五的交易时段运行；“全部盘中决策”会忽略交易时段限制，立即执行一次。</p>
                  <div className={styles.stack}>
                    <div className={styles.field}>
                      <label htmlFor="decision-interval">盘中决策间隔：{runtimeConfig.intraday_decision_interval_minutes} 分钟</label>
                      <input
                        id="decision-interval"
                        className={styles.slider}
                        max="120"
                        min="10"
                        onChange={(event) =>
                          setRuntimeConfig((current) => ({
                            ...current,
                            intraday_decision_interval_minutes: Number(event.target.value) || 60,
                          }))
                        }
                        step="10"
                        type="range"
                        value={runtimeConfig.intraday_decision_interval_minutes}
                      />
                    </div>
                    <div className={styles.field}>
                      <label htmlFor="realtime-interval">实时监测间隔：{runtimeConfig.realtime_monitor_interval_minutes} 分钟</label>
                      <input
                        id="realtime-interval"
                        className={styles.slider}
                        max="10"
                        min="1"
                        onChange={(event) =>
                          setRuntimeConfig((current) => ({
                            ...current,
                            realtime_monitor_interval_minutes: Number(event.target.value) || 3,
                          }))
                        }
                        step="1"
                        type="range"
                        value={runtimeConfig.realtime_monitor_interval_minutes}
                      />
                    </div>
                  </div>
                </>
              )}
              statusFields={(
                undefined
              )}
            />
          </ModuleCard>
        ) : null}
      </div>
    </PageFrame>
  );
}
