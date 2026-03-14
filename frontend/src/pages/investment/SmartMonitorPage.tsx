import { FormEvent, useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import { decodeIntent } from "../../lib/intents";
import { useSmartMonitorStore } from "../../stores/smartMonitorStore";
import styles from "../ConsolePage.module.scss";

interface SmartMonitorTask {
  id: number;
  task_name: string;
  stock_code: string;
  stock_name?: string;
  enabled: number;
  check_interval: number;
  position_size_pct: number;
  stop_loss_pct: number;
  take_profit_pct: number;
  account_name?: string;
}

interface DecisionItem {
  id: number;
  stock_code: string;
  stock_name?: string;
  action?: string;
  decision_time?: string;
  reasoning?: string;
  action_status?: string;
}

interface PendingAction {
  id: number;
  action_type?: string;
  status?: string;
  account_name?: string;
  payload?: Record<string, unknown>;
  created_at?: string;
}

interface SystemStatus {
  monitor_service?: {
    running?: boolean;
    total_items?: number;
    ai_tasks?: number;
    pending_notifications?: number;
  };
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
  message: string;
  triggered_at: string;
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
type NoticeTone = "default" | "success" | "warning" | "danger";
type NotificationTone = "danger" | "success" | "warning" | "info";
type SectionKey = "overview" | "pending" | "decisions" | "tasks" | "controls";

const sectionTabs: Array<{ key: SectionKey; label: string }> = [
  { key: "overview", label: "监控总览" },
  { key: "pending", label: "待办动作" },
  { key: "decisions", label: "AI决策" },
  { key: "tasks", label: "任务列表" },
  { key: "controls", label: "运行控制" },
];

const defaultTaskForm = {
  stock_code: "",
  stock_name: "",
  account_name: "默认账户",
  task_name: "",
  check_interval: "3600",
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

const noticeToneClass: Record<Exclude<NoticeTone, "default">, string> = {
  success: styles.noticeSuccess,
  warning: styles.noticeWarning,
  danger: styles.noticeDanger,
};

const notificationToneClass: Record<NotificationTone, string> = {
  danger: styles.noticeDanger,
  success: styles.noticeSuccess,
  warning: styles.noticeWarning,
  info: styles.noticeInfo,
};

const pendingMeta = (status?: string): { tone: NoticeTone; label: string } => {
  const normalized = String(status || "pending").toLowerCase();
  if (normalized === "accepted" || normalized === "done") return { tone: "success", label: "已处理" };
  if (normalized === "rejected") return { tone: "danger", label: "已拒绝" };
  if (normalized === "ignored") return { tone: "default", label: "已忽略" };
  return { tone: "warning", label: "待处理" };
};

const decisionMeta = (status?: string): { tone: NoticeTone; label: string } => {
  const normalized = String(status || "suggested").toLowerCase();
  if (normalized === "success" || normalized === "executed") return { tone: "success", label: "已执行" };
  if (normalized === "failed" || normalized === "rejected") return { tone: "danger", label: "执行失败" };
  if (normalized === "running" || normalized === "pending" || normalized === "suggested") return { tone: "warning", label: "待确认" };
  return { tone: "default", label: normalized || "状态未知" };
};

const decisionAction = (value?: string) =>
  ({ buy: "买入建议", sell: "卖出建议", hold: "继续观察" }[String(value || "").toLowerCase()] || value || "策略建议");

const readPayloadText = (payload: Record<string, unknown> | undefined, ...keys: string[]) => {
  for (const key of keys) {
    const value = payload?.[key];
    if (typeof value === "string" && value.trim()) return value.trim();
    if (typeof value === "number") return String(value);
  }
  return "";
};

const formatNumber = (value?: number) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toLocaleString("zh-CN", { maximumFractionDigits: 2 }) : "-";
};

const notificationMeta = (message: string): { tone: NotificationTone; label: string } => {
  if (/(止损|跌破|下破|失守|回撤)/.test(message)) return { tone: "danger", label: "风险预警" };
  if (/(止盈|突破|上破|目标价|盈利)/.test(message)) return { tone: "success", label: "收益信号" };
  if (/(买入|入场|区间|接近)/.test(message)) return { tone: "info", label: "关注提醒" };
  return { tone: "warning", label: "价格提醒" };
};

export function SmartMonitorPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const enabledOnly = useSmartMonitorStore((state) => state.enabledOnly);
  const setEnabledOnly = useSmartMonitorStore((state) => state.setEnabledOnly);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [tasks, setTasks] = useState<SmartMonitorTask[]>([]);
  const [decisions, setDecisions] = useState<DecisionItem[]>([]);
  const [pendingActions, setPendingActions] = useState<PendingAction[]>([]);
  const [alerts, setAlerts] = useState<PriceAlert[]>([]);
  const [notifications, setNotifications] = useState<PriceAlertNotification[]>([]);
  const [taskForm, setTaskForm] = useState(defaultTaskForm);
  const [alertForm, setAlertForm] = useState<AlertFormState>(defaultAlertForm);
  const [analysisCode, setAnalysisCode] = useState("");
  const [analysisResult, setAnalysisResult] = useState<unknown>(null);
  const [activePanel, setActivePanel] = useState<ComposerPanel>(null);
  const [section, setSection] = useState<SectionKey>("overview");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadAll = async () => {
    const [statusData, taskData, decisionData, pendingData, alertData, notificationData] = await Promise.all([
      apiFetch<SystemStatus>("/api/system/status"),
      apiFetch<SmartMonitorTask[]>(`/api/smart-monitor/tasks${buildQuery({ enabled_only: enabledOnly })}`),
      apiFetch<DecisionItem[]>("/api/smart-monitor/decisions?limit=30"),
      apiFetch<PendingAction[]>("/api/smart-monitor/pending-actions?limit=30"),
      apiFetch<PriceAlert[]>("/api/price-alerts"),
      apiFetch<PriceAlertNotification[]>("/api/price-alerts/notifications?limit=12"),
    ]);
    setSystemStatus(statusData);
    setTasks(taskData);
    setDecisions(decisionData);
    setPendingActions(pendingData);
    setAlerts(alertData);
    setNotifications(notificationData);
  };

  useEffect(() => {
    void loadAll();
  }, [enabledOnly]);

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
      setSection("overview");
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

  const withFeedback = (nextMessage = "", nextError = "") => {
    setMessage(nextMessage);
    setError(nextError);
  };

  const submitTask = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    withFeedback();
    try {
      await apiFetch("/api/smart-monitor/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_code: taskForm.stock_code,
          stock_name: taskForm.stock_name || taskForm.stock_code,
          account_name: taskForm.account_name,
          task_name: taskForm.task_name || `${taskForm.stock_name || taskForm.stock_code}盯盘`,
          check_interval: Number(taskForm.check_interval),
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
      withFeedback(`盯盘任务已保存 ${taskForm.stock_code}`);
      await loadAll();
    } catch (requestError) {
      withFeedback("", requestError instanceof ApiRequestError ? requestError.message : "保存任务失败");
    }
  };

  const submitAlert = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    withFeedback();
    const entryMin = Number(alertForm.entry_min);
    const entryMax = Number(alertForm.entry_max);
    if (!alertForm.symbol.trim()) return withFeedback("", "请先填写股票代码");
    if (!Number.isFinite(entryMin) || !Number.isFinite(entryMax)) return withFeedback("", "请输入有效的入场区间");
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
      withFeedback("价格预警已创建");
      await loadAll();
    } catch (requestError) {
      withFeedback("", requestError instanceof ApiRequestError ? requestError.message : "创建预警失败");
    }
  };

  const runMonitorCommand = async (path: string, successText: string, method: "POST" | "DELETE" = "POST") => {
    withFeedback();
    try {
      await apiFetch(path, { method });
      withFeedback(successText);
      await loadAll();
    } catch (requestError) {
      withFeedback("", requestError instanceof ApiRequestError ? requestError.message : "操作失败");
    }
  };

  const runAnalysis = async () => {
    if (!analysisCode.trim()) return withFeedback("", "请先输入股票代码");
    withFeedback();
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
      withFeedback(`已完成 ${analysisCode.trim()} 的即时分析`);
    } catch (requestError) {
      withFeedback("", requestError instanceof ApiRequestError ? requestError.message : "手动分析失败");
    }
  };

  const resolvePendingAction = async (actionId: number) => {
    withFeedback();
    try {
      await apiFetch(`/api/smart-monitor/pending-actions/${actionId}/resolve`, {
        method: "POST",
        body: JSON.stringify({ status: "ignored", resolution_note: "ignored" }),
      });
      withFeedback("待办动作已忽略");
      await loadAll();
    } catch (requestError) {
      withFeedback("", requestError instanceof ApiRequestError ? requestError.message : "更新待办动作失败");
    }
  };

  const openTaskEditor = (task: SmartMonitorTask) => {
    setTaskForm({
      stock_code: task.stock_code,
      stock_name: task.stock_name || "",
      account_name: task.account_name || "默认账户",
      task_name: task.task_name,
      check_interval: String(task.check_interval),
      position_size_pct: String(task.position_size_pct),
      stop_loss_pct: String(task.stop_loss_pct),
      take_profit_pct: String(task.take_profit_pct),
      trading_hours_only: true,
      enabled: Boolean(task.enabled),
      origin_analysis_id: undefined,
    });
    setAnalysisCode(task.stock_code);
    setActivePanel("task");
    setSection("tasks");
  };

  const activeTaskCount = tasks.filter((item) => Boolean(item.enabled)).length;
  const enabledAlerts = alerts.filter((item) => item.notification_enabled).length;
  const protectedAlerts = alerts.filter((item) => item.take_profit || item.stop_loss).length;
  const latestPendingActions = pendingActions.slice(0, 8);
  const latestDecisions = decisions.slice(0, 12);

  const renderTaskComposer = () =>
    activePanel !== "task" ? null : (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <h2>新增盯盘任务</h2>
          <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">收起</button>
        </div>
        <form className={styles.stack} onSubmit={submitTask}>
          <div className={styles.formGrid}>
            <div className={styles.field}><label htmlFor="task-code">股票代码</label><input id="task-code" onChange={(event) => setTaskForm((current) => ({ ...current, stock_code: event.target.value }))} value={taskForm.stock_code} /></div>
            <div className={styles.field}><label htmlFor="task-name">股票名称</label><input id="task-name" onChange={(event) => setTaskForm((current) => ({ ...current, stock_name: event.target.value }))} value={taskForm.stock_name} /></div>
            <div className={styles.field}><label htmlFor="task-account">账户</label><input id="task-account" onChange={(event) => setTaskForm((current) => ({ ...current, account_name: event.target.value }))} value={taskForm.account_name} /></div>
            <div className={styles.field}><label htmlFor="task-title">任务名称</label><input id="task-title" onChange={(event) => setTaskForm((current) => ({ ...current, task_name: event.target.value }))} value={taskForm.task_name} /></div>
            <div className={styles.field}><label htmlFor="task-interval">检查间隔(秒)</label><input id="task-interval" onChange={(event) => setTaskForm((current) => ({ ...current, check_interval: event.target.value }))} value={taskForm.check_interval} /></div>
            <div className={styles.field}><label htmlFor="task-position">仓位占比(%)</label><input id="task-position" onChange={(event) => setTaskForm((current) => ({ ...current, position_size_pct: event.target.value }))} value={taskForm.position_size_pct} /></div>
            <div className={styles.field}><label htmlFor="task-stop">止损(%)</label><input id="task-stop" onChange={(event) => setTaskForm((current) => ({ ...current, stop_loss_pct: event.target.value }))} value={taskForm.stop_loss_pct} /></div>
            <div className={styles.field}><label htmlFor="task-profit">止盈(%)</label><input id="task-profit" onChange={(event) => setTaskForm((current) => ({ ...current, take_profit_pct: event.target.value }))} value={taskForm.take_profit_pct} /></div>
          </div>
          <div className={styles.actions}>
            <button className={styles.primaryButton} type="submit">保存任务</button>
            <button className={styles.secondaryButton} onClick={() => setActivePanel(null)} type="button">取消</button>
          </div>
        </form>
      </section>
    );

  const renderAlertComposer = () =>
    activePanel !== "alert" ? null : (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <h2>新增预警</h2>
          <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">收起</button>
        </div>
        <form className={styles.stack} onSubmit={submitAlert}>
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
      </section>
    );

  const renderAnalysisComposer = () =>
    activePanel !== "analysis" ? null : (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <h2>手动分析</h2>
          <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">收起</button>
        </div>
        <div className={styles.actions}>
          <input onChange={(event) => setAnalysisCode(event.target.value)} placeholder="股票代码" value={analysisCode} />
          <button className={styles.primaryButton} onClick={() => void runAnalysis()} type="button">立即分析</button>
        </div>
        {analysisResult ? <pre className={styles.code}>{JSON.stringify(analysisResult, null, 2)}</pre> : null}
      </section>
    );

  const renderOverviewSection = () => (
    <>
      {renderAnalysisComposer()}
      {renderAlertComposer()}
      <section className={styles.card}>
        <div className={styles.compactGrid}>
          <div className={styles.metric}><span className={styles.muted}>监控总数</span><strong>{systemStatus?.monitor_service?.total_items ?? 0}</strong></div>
          <div className={styles.metric}><span className={styles.muted}>启用任务</span><strong>{activeTaskCount}</strong></div>
          <div className={styles.metric}><span className={styles.muted}>AI任务</span><strong>{systemStatus?.monitor_service?.ai_tasks ?? 0}</strong></div>
          <div className={styles.metric}><span className={styles.muted}>当前预警</span><strong>{alerts.length}</strong></div>
          <div className={styles.metric}><span className={styles.muted}>通知已开启</span><strong>{enabledAlerts}</strong></div>
          <div className={styles.metric}><span className={styles.muted}>带止盈止损</span><strong>{protectedAlerts}</strong></div>
        </div>
      </section>
      <section className={styles.card}>
        <div className={styles.cardHeader}><h2>预警通知</h2></div>
        <div className={styles.list}>
          {notifications.map((item) => {
            const meta = notificationMeta(item.message);
            return (
              <div className={`${styles.noticeCard} ${notificationToneClass[meta.tone]}`} key={item.id}>
                <div className={styles.noticeMeta}><StatusBadge label={meta.label} tone={meta.tone} /><strong>{item.symbol}</strong></div>
                <div>{item.message}</div>
                <small className={styles.muted}>{item.triggered_at}</small>
              </div>
            );
          })}
          {notifications.length === 0 ? <div className={styles.noticeCard}><div>最近还没有新的价格通知。</div></div> : null}
        </div>
      </section>
      <section className={styles.card}>
        <div className={styles.cardHeader}><h2>当前预警</h2></div>
        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead><tr><th>股票</th><th>入场区间</th><th>止盈 / 止损</th><th>通知</th><th>账户</th><th>操作</th></tr></thead>
            <tbody>
              {alerts.map((alert) => (
                <tr key={alert.id}>
                  <td><strong>{alert.symbol}</strong><div className={styles.muted}>{alert.name}</div></td>
                  <td>{formatNumber(alert.entry_range?.min)} - {formatNumber(alert.entry_range?.max)}</td>
                  <td>{formatNumber(alert.take_profit)} / {formatNumber(alert.stop_loss)}</td>
                  <td><StatusBadge label={alert.notification_enabled ? "开启" : "关闭"} tone={alert.notification_enabled ? "default" : "warning"} /></td>
                  <td>{alert.account_name ?? "默认账户"}</td>
                  <td><div className={styles.actions}><button className={styles.secondaryButton} onClick={() => void runMonitorCommand(`/api/price-alerts/${alert.id}/notification?enabled=${String(!alert.notification_enabled)}`, alert.notification_enabled ? "已关闭预警通知" : "已开启预警通知")} type="button">{alert.notification_enabled ? "关闭通知" : "开启通知"}</button><button className={styles.dangerButton} onClick={() => void runMonitorCommand(`/api/price-alerts/${alert.id}`, "价格预警已删除", "DELETE")} type="button">删除</button></div></td>
                </tr>
              ))}
              {alerts.length === 0 ? <tr><td className={styles.muted} colSpan={6}>暂无价格预警</td></tr> : null}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
  const renderPendingSection = () => (
    <section className={styles.card}>
      <div className={styles.cardHeader}><h2>待办动作</h2></div>
      <div className={styles.list}>
        {latestPendingActions.map((item) => {
          const meta = pendingMeta(item.status);
          const toneClass = meta.tone === "default" ? "" : noticeToneClass[meta.tone];
          const symbol = readPayloadText(item.payload, "stock_code", "symbol", "code", "ticker");
          const stockName = readPayloadText(item.payload, "stock_name", "name");
          return (
            <div className={`${styles.noticeCard} ${toneClass}`.trim()} key={item.id}>
              <div className={styles.noticeMeta}><StatusBadge label={meta.label} tone={meta.tone} /><strong>{item.action_type || "待办动作"}</strong></div>
              <p className={styles.muted}>股票: {stockName ? `${stockName}${symbol ? ` (${symbol})` : ""}` : symbol || "未指定"}</p>
              <p className={styles.muted}>账户: {item.account_name || "默认账户"} | 时间: {item.created_at || "暂无"}</p>
              <div className={styles.actions}>{String(item.status || "").toLowerCase() === "ignored" ? <span className={styles.muted}>已忽略</span> : <button className={styles.secondaryButton} onClick={() => void resolvePendingAction(item.id)} type="button">忽略</button>}</div>
            </div>
          );
        })}
        {latestPendingActions.length === 0 ? <div className={styles.noticeCard}><div>当前没有需要人工确认的动作。</div></div> : null}
      </div>
    </section>
  );
  const renderDecisionsSection = () => (
    <section className={styles.card}>
      <div className={styles.cardHeader}><h2>AI决策</h2></div>
      <div className={styles.list}>
        {latestDecisions.map((item) => {
          const meta = decisionMeta(item.action_status);
          return (
            <div className={styles.listItem} key={item.id}>
              <div className={styles.noticeMeta}><StatusBadge label={decisionAction(item.action)} tone="default" /><StatusBadge label={meta.label} tone={meta.tone} /></div>
              <strong>{item.stock_code}</strong>
              <p className={styles.muted}>{item.stock_name || item.stock_code} | {item.decision_time || "暂无时间"}</p>
              <p>{item.reasoning || "暂无推理说明"}</p>
            </div>
          );
        })}
        {latestDecisions.length === 0 ? <div className={styles.muted}>暂无 AI 决策</div> : null}
      </div>
    </section>
  );
  const renderTasksSection = () => (
    <>
      {renderTaskComposer()}
      <section className={styles.card}>
        <div className={styles.cardHeader}><h2>任务列表</h2></div>
        <div className={styles.list}>
          {tasks.map((task) => (
            <div className={styles.listItem} key={task.id}>
              <div className={styles.noticeMeta}><strong>{task.task_name}</strong><StatusBadge label={Boolean(task.enabled) ? "启用" : "停用"} tone={Boolean(task.enabled) ? "success" : "default"} /></div>
              <p className={styles.muted}>{task.stock_name || task.stock_code} | {task.account_name || "默认账户"} | 间隔 {task.check_interval}s</p>
              <div className={styles.actions}>
                <button className={styles.secondaryButton} onClick={() => openTaskEditor(task)} type="button">带入表单</button>
                <button className={styles.secondaryButton} onClick={() => void runMonitorCommand(`/api/smart-monitor/tasks/${task.id}/enable?enabled=${String(!task.enabled)}`, task.enabled ? "任务已停用" : "任务已启用")} type="button">{task.enabled ? "停用" : "启用"}</button>
                <button className={styles.dangerButton} onClick={() => void runMonitorCommand(`/api/smart-monitor/tasks/${task.id}`, "任务已删除", "DELETE")} type="button">删除</button>
              </div>
            </div>
          ))}
          {tasks.length === 0 ? <div className={styles.muted}>暂无智能盯盘任务</div> : null}
        </div>
      </section>
    </>
  );
  const renderControlsSection = () => (
    <section className={styles.card}>
      <div className={styles.cardHeader}><h2>运行控制</h2></div>
      <div className={styles.actions}>
        <button className={styles.secondaryButton} onClick={() => void runMonitorCommand("/api/system/monitor-service/start", "监控服务已启动")} type="button">启动服务</button>
        <button className={styles.secondaryButton} onClick={() => void runMonitorCommand("/api/system/monitor-service/stop", "监控服务已停止")} type="button">停止服务</button>
        <button className={styles.secondaryButton} onClick={() => void runMonitorCommand("/api/smart-monitor/tasks/enable-all?enabled=true", "全部任务已启用")} type="button">全部启用</button>
        <button className={styles.secondaryButton} onClick={() => void runMonitorCommand("/api/smart-monitor/tasks/enable-all?enabled=false", "全部任务已停用")} type="button">全部停用</button>
      </div>
    </section>
  );

  return (
    <PageFrame
      title="智能盯盘"
      actions={
        <>
          <StatusBadge label={systemStatus?.monitor_service?.running ? "监控服务运行中" : "监控服务未启动"} tone={systemStatus?.monitor_service?.running ? "success" : "warning"} />
          <StatusBadge label={`启用任务 ${activeTaskCount}/${tasks.length}`} tone="default" />
        </>
      }
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      sectionTabs={sectionTabs}
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <h2>页面操作</h2>
            <div className={styles.actions}>
              <button className={styles.secondaryButton} onClick={() => void loadAll()} type="button">刷新</button>
              <button className={section === "overview" && activePanel === "alert" ? styles.primaryButton : styles.secondaryButton} onClick={() => { setSection("overview"); setActivePanel((current) => current === "alert" ? null : "alert"); }} type="button">新增预警</button>
              <button className={section === "tasks" && activePanel === "task" ? styles.primaryButton : styles.secondaryButton} onClick={() => { setSection("tasks"); setActivePanel((current) => current === "task" ? null : "task"); }} type="button">新增任务</button>
              <button className={section === "overview" && activePanel === "analysis" ? styles.primaryButton : styles.secondaryButton} onClick={() => { setSection("overview"); setActivePanel((current) => current === "analysis" ? null : "analysis"); }} type="button">手动分析</button>
              <button className={styles.secondaryButton} onClick={() => setEnabledOnly(!enabledOnly)} type="button">{enabledOnly ? "显示全部" : "仅看启用"}</button>
            </div>
          </div>
          {message ? <p className={styles.successText}>{message}</p> : null}
          {error ? <p className={styles.dangerText}>{error}</p> : null}
        </section>
        {section === "overview" ? renderOverviewSection() : null}
        {section === "pending" ? renderPendingSection() : null}
        {section === "decisions" ? renderDecisionsSection() : null}
        {section === "tasks" ? renderTasksSection() : null}
        {section === "controls" ? renderControlsSection() : null}
      </div>
    </PageFrame>
  );
}
