import { FormEvent, useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { ModuleCard } from "../../components/common/ModuleCard";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { SchedulerControl } from "../../components/common/SchedulerControl";
import { StatusBadge } from "../../components/common/StatusBadge";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { FormattedReport } from "../../components/research/FormattedReport";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
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
  asset_id?: number | null;
  portfolio_stock_id?: number | null;
  has_position?: number;
  asset_status?: string | null;
  strategy_context?: {
    entry_min?: number;
    entry_max?: number;
    take_profit?: number;
    stop_loss?: number;
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
}

interface RuntimeConfig {
  intraday_decision_interval_minutes: number;
  realtime_monitor_interval_minutes: number;
}

interface PriceAlert {
  id: number;
  symbol: string;
  name: string;
  rating: string;
  entry_range?: { min?: number; max?: number };
  take_profit?: number;
  stop_loss?: number;
  notification_enabled: boolean;
  account_name?: string;
}

interface PriceAlertNotification {
  id: number;
  symbol: string;
  name?: string;
  message: string;
  triggered_at: string;
  account_name?: string;
}

interface AlertFormState {
  symbol: string;
  name: string;
  entry_min: string;
  entry_max: string;
  take_profit: string;
  stop_loss: string;
}

interface MonitorIntentPayload {
  symbol?: string;
  stock_name?: string;
  account_name?: string;
  origin_analysis_id?: number;
  strategy_context?: Record<string, unknown>;
}

type ComposerPanel = "task" | "analysis" | "alert" | null;
type NotificationTone = "danger" | "success" | "warning" | "info";
type SectionKey = "results" | "tasks" | "controls";

const sectionTabs: Array<{ key: SectionKey; label: string }> = [
  { key: "results", label: "监控结果" },
  { key: "tasks", label: "任务列表" },
  { key: "controls", label: "运行控制" },
];

const defaultTaskForm = {
  stock_code: "",
  stock_name: "",
  account_name: "默认账户",
  task_name: "",
  position_size_pct: "20",
  stop_loss_pct: "5",
  take_profit_pct: "10",
  trading_hours_only: true,
  enabled: true,
  origin_analysis_id: undefined as number | undefined,
};

const defaultAlertForm: AlertFormState = {
  symbol: "",
  name: "",
  entry_min: "",
  entry_max: "",
  take_profit: "",
  stop_loss: "",
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

function decisionBadge(value?: string): { label: string; tone: "success" | "danger" | "warning" | "default" } {
  const normalized = String(value || "").toLowerCase();
  if (normalized === "buy") return { label: "买入", tone: "success" };
  if (normalized === "sell") return { label: "卖出", tone: "danger" };
  if (normalized === "hold") return { label: "观望", tone: "warning" };
  return { label: value || "未知", tone: "default" };
}

const formatNumber = (value?: number) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toLocaleString("zh-CN", { maximumFractionDigits: 2 }) : "-";
};

const formatLevelNumber = (value?: unknown) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toLocaleString("zh-CN", { maximumFractionDigits: 2 }) : "-";
};

const formatThresholdSummary = (
  label: string,
  levels?: {
    entry_min?: unknown;
    entry_max?: unknown;
    take_profit?: unknown;
    stop_loss?: unknown;
  } | null,
) => {
  if (!levels) {
    return `${label}：暂无关键指标`;
  }
  const entryMin = formatLevelNumber(levels.entry_min);
  const entryMax = formatLevelNumber(levels.entry_max);
  const takeProfit = formatLevelNumber(levels.take_profit);
  const stopLoss = formatLevelNumber(levels.stop_loss);
  if ([entryMin, entryMax, takeProfit, stopLoss].every((value) => value === "-")) {
    return `${label}：暂无关键指标`;
  }
  return `${label}：入场[${entryMin}-${entryMax}] | 止盈:${takeProfit} | 止损:${stopLoss}`;
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
  return (task.account_name || "默认账户") === (decision.account_name || "默认账户");
};

const findLatestDecisionForTask = (task: SmartMonitorTask, decisions: DecisionItem[]) =>
  decisions.find((decision) => matchesTaskDecision(task, decision))
  || decisions.find((decision) => decision.stock_code === task.stock_code);

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

export function SmartMonitorPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const enabledOnly = useSmartMonitorStore((state) => state.enabledOnly);
  const setEnabledOnly = useSmartMonitorStore((state) => state.setEnabledOnly);
  const cacheModeKey = enabledOnly ? "enabled" : "all";
  const cachedPage = useSmartMonitorStore((state) => state.pageCacheByMode[cacheModeKey] ?? null);
  const setPageCache = useSmartMonitorStore((state) => state.setPageCache);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(() => (cachedPage?.systemStatus as SystemStatus | null) ?? null);
  const [tasks, setTasks] = useState<SmartMonitorTask[]>(() => (cachedPage?.tasks as SmartMonitorTask[]) ?? []);
  const [decisions, setDecisions] = useState<DecisionItem[]>(() => (cachedPage?.decisions as DecisionItem[]) ?? []);
  const [alerts, setAlerts] = useState<PriceAlert[]>(() => (cachedPage?.alerts as PriceAlert[]) ?? []);
  const [notifications, setNotifications] = useState<PriceAlertNotification[]>(() => (cachedPage?.notifications as PriceAlertNotification[]) ?? []);
  const [runtimeConfig, setRuntimeConfig] = useState<RuntimeConfig>(() => (cachedPage?.runtimeConfig as RuntimeConfig | null) ?? defaultRuntimeConfig);
  const [taskForm, setTaskForm] = useState(defaultTaskForm);
  const [alertForm, setAlertForm] = useState<AlertFormState>(defaultAlertForm);
  const [analysisCode, setAnalysisCode] = useState("");
  const [analysisResult, setAnalysisResult] = useState<unknown>(null);
  const [activePanel, setActivePanel] = useState<ComposerPanel>(null);
  const [section, setSection] = useState<SectionKey>("results");
  const { message, error, clear, showError, showMessage } = usePageFeedback();

  const applyPageCache = (cache: SmartMonitorPageCache | null) => {
    if (!cache) {
      return;
    }
    setSystemStatus((cache.systemStatus as SystemStatus | null) ?? null);
    setTasks(cache.tasks as SmartMonitorTask[]);
    setDecisions(cache.decisions as DecisionItem[]);
    setAlerts(cache.alerts as PriceAlert[]);
    setNotifications(cache.notifications as PriceAlertNotification[]);
    setRuntimeConfig((cache.runtimeConfig as RuntimeConfig | null) ?? defaultRuntimeConfig);
  };

  const loadAll = async (force = false) => {
    if (!force && cachedPage && Date.now() - cachedPage.updatedAt < PAGE_CACHE_TTL_MS) {
      applyPageCache(cachedPage);
      return;
    }
    const [statusData, taskData, decisionData, alertData, notificationData, runtimeData] = await Promise.all([
      apiFetch<SystemStatus>("/api/system/status"),
      apiFetch<SmartMonitorTask[]>(`/api/smart-monitor/tasks${buildQuery({ enabled_only: enabledOnly })}`),
      apiFetch<DecisionItem[]>("/api/smart-monitor/decisions?limit=30"),
      apiFetch<PriceAlert[]>("/api/price-alerts"),
      apiFetch<PriceAlertNotification[]>("/api/price-alerts/notifications?limit=12"),
      apiFetch<RuntimeConfig>("/api/smart-monitor/runtime-config"),
    ]);
    setSystemStatus(statusData);
    setTasks(taskData);
    setDecisions(decisionData);
    setAlerts(alertData);
    setNotifications(notificationData);
    setRuntimeConfig(runtimeData);
    setPageCache(cacheModeKey, {
      systemStatus: statusData,
      tasks: taskData,
      decisions: decisionData,
      alerts: alertData,
      notifications: notificationData,
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
    const intent = decodeIntent<MonitorIntentPayload>(searchParams.get("intent"));
    if (!intent || !["watchlist", "smart_monitor", "ai_monitor", "price_alert"].includes(intent.type)) return;
    const payload = intent.payload || {};
    if (intent.type === "price_alert") {
      const context = payload.strategy_context || {};
      setAlertForm({
        symbol: String(payload.symbol || ""),
        name: String(payload.stock_name || ""),
        entry_min: String(context.entry_min ?? ""),
        entry_max: String(context.entry_max ?? ""),
        take_profit: String(context.take_profit ?? ""),
        stop_loss: String(context.stop_loss ?? ""),
      });
      setActivePanel("alert");
      setSection("tasks");
    } else {
      setTaskForm((current) => ({
        ...current,
        stock_code: payload.symbol || "",
        stock_name: payload.stock_name || "",
        account_name: payload.account_name || "默认账户",
        task_name: `${payload.stock_name || payload.symbol || ""}盯盘`,
        origin_analysis_id: payload.origin_analysis_id,
      }));
      setAnalysisCode(payload.symbol || "");
      setActivePanel("task");
      setSection("tasks");
    }
    const nextParams = new URLSearchParams(searchParams);
    nextParams.delete("intent");
    setSearchParams(nextParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const submitTask = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    clear();
    try {
      await apiFetch("/api/smart-monitor/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_code: taskForm.stock_code,
          stock_name: taskForm.stock_name || taskForm.stock_code,
          account_name: taskForm.account_name,
          task_name: taskForm.task_name || `${taskForm.stock_name || taskForm.stock_code}盯盘`,
          position_size_pct: Number(taskForm.position_size_pct),
          stop_loss_pct: Number(taskForm.stop_loss_pct),
          take_profit_pct: Number(taskForm.take_profit_pct),
          trading_hours_only: taskForm.trading_hours_only,
          enabled: taskForm.enabled,
          origin_analysis_id: taskForm.origin_analysis_id ?? null,
        }),
      });
      setTaskForm(defaultTaskForm);
      setActivePanel(null);
      showMessage(`盯盘任务已保存 ${taskForm.stock_code}`);
      await loadAll(true);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "保存任务失败");
    }
  };

  const submitAlert = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    clear();
    const entryMin = Number(alertForm.entry_min);
    const entryMax = Number(alertForm.entry_max);
    if (!alertForm.symbol.trim()) {
      showError("请先填写股票代码");
      return;
    }
    if (!Number.isFinite(entryMin) || !Number.isFinite(entryMax)) {
      showError("请输入有效的入场区间");
      return;
    }
    try {
      await apiFetch("/api/price-alerts", {
        method: "POST",
        body: JSON.stringify({
          symbol: alertForm.symbol.trim(),
          name: alertForm.name.trim() || alertForm.symbol.trim(),
          entry_min: entryMin,
          entry_max: entryMax,
          take_profit: alertForm.take_profit ? Number(alertForm.take_profit) : null,
          stop_loss: alertForm.stop_loss ? Number(alertForm.stop_loss) : null,
        }),
      });
      setAlertForm(defaultAlertForm);
      setActivePanel(null);
      showMessage("价格预警已创建");
      await loadAll(true);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "创建预警失败");
    }
  };

  const saveRuntimeConfig = async () => {
    clear();
    try {
      const next = await apiFetch<RuntimeConfig>("/api/smart-monitor/runtime-config", {
        method: "PUT",
        body: JSON.stringify(runtimeConfig),
      });
      setRuntimeConfig(next);
      showMessage("运行控制已更新");
      await loadAll(true);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "保存运行控制失败");
    }
  };

  const runMonitorCommand = async (path: string, successText: string, method: "POST" | "DELETE" = "POST") => {
    clear();
    try {
      await apiFetch(path, { method });
      showMessage(successText);
      await loadAll(true);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "操作失败");
    }
  };

  const runAnalysis = async () => {
    if (!analysisCode.trim()) {
      showError("请先输入股票代码");
      return;
    }
    clear();
    try {
      const result = await apiFetch<unknown>("/api/smart-monitor/analyze", {
        method: "POST",
        body: JSON.stringify({
          stock_code: analysisCode.trim(),
          account_name: taskForm.account_name || "默认账户",
          trading_hours_only: true,
          notify: false,
        }),
      });
      setAnalysisResult(result);
      showMessage(`已完成 ${analysisCode.trim()} 的即时分析`);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "手动分析失败");
    }
  };

  const ignoreNotification = async (eventId: number) => {
    clear();
    try {
      await apiFetch(`/api/price-alerts/notifications/${eventId}/ignore`, { method: "POST" });
      showMessage("预警通知已忽略");
      await loadAll(true);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "忽略预警通知失败");
    }
  };

  const toggleAlert = async (alert: PriceAlert) => {
    clear();
    try {
      await apiFetch(
        `/api/price-alerts/${alert.id}/notification?enabled=${String(!alert.notification_enabled)}`,
        { method: "POST" },
      );
      showMessage(alert.notification_enabled ? "已关闭通知" : "已开启通知");
      await loadAll(true);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "更新通知状态失败");
    }
  };

  const deleteAlert = async (alertId: number) => {
    clear();
    try {
      await apiFetch(`/api/price-alerts/${alertId}`, { method: "DELETE" });
      showMessage("价格预警已删除");
      await loadAll(true);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "删除预警失败");
    }
  };

  const renderTaskForm = () => (
    <form className={styles.moduleSection} onSubmit={submitTask}>
      <div className={styles.formGrid}>
        <div className={styles.field}><label htmlFor="task-code">股票代码</label><input id="task-code" onChange={(event) => setTaskForm((current) => ({ ...current, stock_code: event.target.value }))} value={taskForm.stock_code} /></div>
        <div className={styles.field}><label htmlFor="task-name">股票名称</label><input id="task-name" onChange={(event) => setTaskForm((current) => ({ ...current, stock_name: event.target.value }))} value={taskForm.stock_name} /></div>
        <div className={styles.field}><label htmlFor="task-account">账户</label><input id="task-account" onChange={(event) => setTaskForm((current) => ({ ...current, account_name: event.target.value }))} value={taskForm.account_name} /></div>
        <div className={styles.field}><label htmlFor="task-title">任务名称</label><input id="task-title" onChange={(event) => setTaskForm((current) => ({ ...current, task_name: event.target.value }))} value={taskForm.task_name} /></div>
        <div className={styles.field}><label htmlFor="task-position">仓位占比(%)</label><input id="task-position" onChange={(event) => setTaskForm((current) => ({ ...current, position_size_pct: event.target.value }))} value={taskForm.position_size_pct} /></div>
        <div className={styles.field}><label htmlFor="task-stop">止损(%)</label><input id="task-stop" onChange={(event) => setTaskForm((current) => ({ ...current, stop_loss_pct: event.target.value }))} value={taskForm.stop_loss_pct} /></div>
        <div className={styles.field}><label htmlFor="task-profit">止盈(%)</label><input id="task-profit" onChange={(event) => setTaskForm((current) => ({ ...current, take_profit_pct: event.target.value }))} value={taskForm.take_profit_pct} /></div>
      </div>
      <div className={styles.actions}>
        <button className={styles.primaryButton} type="submit">保存任务</button>
        <button className={styles.secondaryButton} onClick={() => setActivePanel(null)} type="button">取消</button>
      </div>
    </form>
  );

  const renderAlertForm = () => (
    <form className={styles.moduleSection} onSubmit={submitAlert}>
      <div className={styles.formGrid}>
        <div className={styles.field}><label htmlFor="alert-symbol">股票代码</label><input id="alert-symbol" onChange={(event) => setAlertForm((current) => ({ ...current, symbol: event.target.value }))} value={alertForm.symbol} /></div>
        <div className={styles.field}><label htmlFor="alert-name">股票名称</label><input id="alert-name" onChange={(event) => setAlertForm((current) => ({ ...current, name: event.target.value }))} value={alertForm.name} /></div>
        <div className={styles.field}><label htmlFor="alert-entry-min">入场下沿</label><input id="alert-entry-min" onChange={(event) => setAlertForm((current) => ({ ...current, entry_min: event.target.value }))} value={alertForm.entry_min} /></div>
        <div className={styles.field}><label htmlFor="alert-entry-max">入场上沿</label><input id="alert-entry-max" onChange={(event) => setAlertForm((current) => ({ ...current, entry_max: event.target.value }))} value={alertForm.entry_max} /></div>
        <div className={styles.field}><label htmlFor="alert-take-profit">止盈价</label><input id="alert-take-profit" onChange={(event) => setAlertForm((current) => ({ ...current, take_profit: event.target.value }))} value={alertForm.take_profit} /></div>
        <div className={styles.field}><label htmlFor="alert-stop-loss">止损价</label><input id="alert-stop-loss" onChange={(event) => setAlertForm((current) => ({ ...current, stop_loss: event.target.value }))} value={alertForm.stop_loss} /></div>
      </div>
      <div className={styles.actions}>
        <button className={styles.primaryButton} type="submit">保存预警</button>
        <button className={styles.secondaryButton} onClick={() => setActivePanel(null)} type="button">取消</button>
      </div>
    </form>
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
            summary="AI 即时分析、最新决策、预警通知和异常提示都收拢到同一模块。"
            toolbar={(
              <button
                className={activePanel === "analysis" ? styles.primaryButton : styles.secondaryButton}
                onClick={() => setActivePanel((current) => current === "analysis" ? null : "analysis")}
                type="button"
              >
                手动分析
              </button>
            )}
          >
            {!systemStatus?.monitor_service?.running ? (
              <div className={`${styles.noticeCard} ${styles.noticeWarning}`}>
                <div className={styles.noticeMeta}>
                  <StatusBadge label="服务提示" tone="warning" />
                </div>
                <div>当前监控服务未启动，自动盯盘不会执行。</div>
              </div>
            ) : null}

            {activePanel === "analysis" ? (
              <div className={styles.moduleSection}>
                <div className={styles.responsiveActionGrid}>
                  <input onChange={(event) => setAnalysisCode(event.target.value)} placeholder="股票代码" value={analysisCode} />
                  <button className={styles.primaryButton} onClick={() => void runAnalysis()} type="button">立即分析</button>
                </div>
                {analysisResult ? (
                  typeof analysisResult === "string" ? (
                    <FormattedReport content={analysisResult} />
                  ) : (
                    <pre className={styles.code}>{JSON.stringify(analysisResult, null, 2)}</pre>
                  )
                ) : (
                  <div className={styles.muted}>输入股票代码后可立即触发一次 AI 分析。</div>
                )}
              </div>
            ) : null}

            <div className={styles.moduleSection}>
              <h3>最新决策</h3>
              <div className={styles.list}>
                {decisions.map((item) => {
                  const badge = decisionBadge(item.action);
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
                })}
                {!decisions.length ? <div className={styles.muted}>暂无盘中决策</div> : null}
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>价格预警通知</h3>
              <div className={styles.list}>
                {notifications.map((item) => {
                  const meta = notificationMeta(item.message);
                  return (
                    <div className={`${styles.noticeCard} ${notificationToneClass[meta.tone]}`} key={item.id}>
                      <div className={styles.noticeMeta}>
                        <StatusBadge label={meta.label} tone={meta.tone} />
                        <strong>{stockDisplayName(item.name, item.symbol)}</strong>
                      </div>
                      <div>{item.message}</div>
                      <small className={styles.muted}>
                        {formatDateTime(item.triggered_at, "暂无时间")} | {item.account_name || "默认账户"}
                      </small>
                      <div className={styles.actions}>
                        <button className={styles.secondaryButton} onClick={() => void ignoreNotification(item.id)} type="button">
                          忽略
                        </button>
                      </div>
                    </div>
                  );
                })}
                {!notifications.length ? <div className={styles.noticeCard}><div>当前没有需要处理的预警通知。</div></div> : null}
              </div>
            </div>
          </ModuleCard>
        ) : null}

        {section === "tasks" ? (
          <ModuleCard
            title="监控任务"
            summary="盯盘任务、价格预警和新增入口统一保留在任务模块中。"
            toolbar={(
              <>
                <button
                  className={activePanel === "task" ? styles.primaryButton : styles.secondaryButton}
                  onClick={() => setActivePanel((current) => current === "task" ? null : "task")}
                  type="button"
                >
                  新增任务
                </button>
                <button
                  className={activePanel === "alert" ? styles.primaryButton : styles.secondaryButton}
                  onClick={() => setActivePanel((current) => current === "alert" ? null : "alert")}
                  type="button"
                >
                  新增预警
                </button>
                <button className={styles.secondaryButton} onClick={() => setEnabledOnly(!enabledOnly)} type="button">
                  {enabledOnly ? "显示全部" : "仅看启用"}
                </button>
              </>
            )}
          >
            {activePanel === "task" ? renderTaskForm() : null}
            {activePanel === "alert" ? renderAlertForm() : null}

            <div className={styles.moduleSection}>
              <h3>盯盘任务列表</h3>
              <div className={styles.list}>
                {tasks.map((task) => {
                  const latestDecision = findLatestDecisionForTask(task, decisions);
                  return (
                    <div className={styles.smartMonitorTaskCard} key={task.id}>
                      <div className={styles.noticeMeta}>
                        <strong className={styles.smartMonitorTaskTitle}>
                          {`${stockDisplayName(task.stock_name, task.stock_code)} | ${taskPortfolioLabel(task)}`}
                        </strong>
                        <StatusBadge label={Boolean(task.enabled) ? "启用" : "停用"} tone={Boolean(task.enabled) ? "success" : "default"} />
                      </div>
                      <p className={styles.taskIndicatorText}>{formatThresholdSummary("分析基线", task.strategy_context)}</p>
                      <p className={styles.taskIndicatorText}>{formatThresholdSummary("盘中决策", latestDecision?.monitor_levels)}</p>
                      <div className={styles.actions}>
                        <button className={styles.secondaryButton} onClick={() => void runMonitorCommand(`/api/smart-monitor/tasks/${task.id}/enable?enabled=${String(!task.enabled)}`, task.enabled ? "任务已停用" : "任务已启用")} type="button">{task.enabled ? "停用" : "启用"}</button>
                        <button className={styles.dangerButton} onClick={() => void runMonitorCommand(`/api/smart-monitor/tasks/${task.id}`, "任务已删除", "DELETE")} type="button">删除</button>
                      </div>
                    </div>
                  );
                })}
                {!tasks.length ? <div className={styles.muted}>暂无智能盯盘任务</div> : null}
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>当前预警规则</h3>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>入场区间</th>
                      <th>止盈 / 止损</th>
                      <th>通知状态</th>
                      <th>账户</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {alerts.map((alert) => (
                      <tr key={alert.id}>
                        <td>
                          <strong>{alert.symbol}</strong>
                          <div className={styles.muted}>{alert.name}</div>
                        </td>
                        <td>{formatNumber(alert.entry_range?.min)} - {formatNumber(alert.entry_range?.max)}</td>
                        <td>{formatNumber(alert.take_profit)} / {formatNumber(alert.stop_loss)}</td>
                        <td>{alert.notification_enabled ? "已开启" : "已关闭"}</td>
                        <td>{alert.account_name || "默认账户"}</td>
                        <td>
                          <div className={styles.actions}>
                            <button className={styles.secondaryButton} onClick={() => void toggleAlert(alert)} type="button">
                              {alert.notification_enabled ? "关闭通知" : "开启通知"}
                            </button>
                            <button className={styles.dangerButton} onClick={() => void deleteAlert(alert.id)} type="button">
                              删除
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!alerts.length ? (
                      <tr>
                        <td className={styles.muted} colSpan={6}>当前没有价格预警规则。</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </ModuleCard>
        ) : null}

        {section === "controls" ? (
          <ModuleCard title="运行控制" summary="服务启停、运行参数和批量控制合并为一个模块。">
            <SchedulerControl
              enabled={Boolean(systemStatus?.monitor_service?.running)}
              label="启用监控服务"
              onSave={() => void saveRuntimeConfig()}
              onToggle={(next) =>
                void runMonitorCommand(
                  next ? "/api/system/monitor-service/start" : "/api/system/monitor-service/stop",
                  next ? "监控服务已启动" : "监控服务已停止",
                )
              }
              saveLabel="保存运行控制"
              scheduleFields={(
                <>
                  <p className={styles.helperText}>自动盯盘仅在周一至周五的交易时段运行，周六周日不会自动执行。</p>
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
                <>
                  <div className={styles.compactGrid}>
                    <div className={styles.metric}>
                      <span className={styles.muted}>服务状态</span>
                      <strong>{systemStatus?.monitor_service?.running ? "运行中" : "已停止"}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>启用任务</span>
                      <strong>{tasks.filter((item) => Boolean(item.enabled)).length}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>价格预警</span>
                      <strong>{alerts.length}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>待处理通知</span>
                      <strong>{notifications.length}</strong>
                    </div>
                  </div>
                  <div className={styles.responsiveActionGrid}>
                    <button className={styles.secondaryButton} onClick={() => void runMonitorCommand("/api/smart-monitor/tasks/enable-all?enabled=true", "全部任务已启用")} type="button">全部启用</button>
                    <button className={styles.secondaryButton} onClick={() => void runMonitorCommand("/api/smart-monitor/tasks/enable-all?enabled=false", "全部任务已停用")} type="button">全部停用</button>
                  </div>
                </>
              )}
            />
          </ModuleCard>
        ) : null}
      </div>
    </PageFrame>
  );
}
