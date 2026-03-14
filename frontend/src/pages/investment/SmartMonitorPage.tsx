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
  managed_by_portfolio?: number;
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

interface MonitorIntentPayload {
  symbol?: string;
  stock_name?: string;
  account_name?: string;
  origin_analysis_id?: number;
}

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

export function SmartMonitorPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const enabledOnly = useSmartMonitorStore((state) => state.enabledOnly);
  const setEnabledOnly = useSmartMonitorStore((state) => state.setEnabledOnly);

  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [tasks, setTasks] = useState<SmartMonitorTask[]>([]);
  const [decisions, setDecisions] = useState<DecisionItem[]>([]);
  const [pendingActions, setPendingActions] = useState<PendingAction[]>([]);
  const [taskForm, setTaskForm] = useState(defaultTaskForm);
  const [analysisCode, setAnalysisCode] = useState("");
  const [analysisResult, setAnalysisResult] = useState<unknown>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadAll = async () => {
    const [statusData, taskData, decisionData, pendingData] = await Promise.all([
      apiFetch<SystemStatus>("/api/system/status"),
      apiFetch<SmartMonitorTask[]>(`/api/smart-monitor/tasks${buildQuery({ enabled_only: enabledOnly })}`),
      apiFetch<DecisionItem[]>("/api/smart-monitor/decisions?limit=30"),
      apiFetch<PendingAction[]>("/api/smart-monitor/pending-actions?limit=30"),
    ]);
    setSystemStatus(statusData);
    setTasks(taskData);
    setDecisions(decisionData);
    setPendingActions(pendingData);
  };

  useEffect(() => {
    void loadAll();
  }, [enabledOnly]);

  useEffect(() => {
    const intent = decodeIntent<MonitorIntentPayload>(searchParams.get("intent"));
    if (!intent || !["watchlist", "smart_monitor", "ai_monitor"].includes(intent.type)) {
      return;
    }
    const payload = intent.payload || {};
    setTaskForm((current) => ({
      ...current,
      stock_code: payload.symbol || "",
      stock_name: payload.stock_name || "",
      account_name: payload.account_name || "默认账户",
      task_name: `${payload.stock_name || payload.symbol || ""}盯盘`,
      origin_analysis_id: payload.origin_analysis_id,
    }));
    setAnalysisCode(payload.symbol || "");
    searchParams.delete("intent");
    setSearchParams(searchParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const submitTask = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
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
      setMessage(`盯盘任务已保存: ${taskForm.stock_code}`);
      setTaskForm(defaultTaskForm);
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "保存任务失败");
    }
  };

  return (
    <PageFrame
      title="智能盯盘"
      summary="当前支持任务 CRUD、服务启停、手动分析、AI 决策和待办动作。"
      actions={
        <>
          <StatusBadge label={systemStatus?.monitor_service?.running ? "监测服务运行中" : "监测服务未启动"} tone={systemStatus?.monitor_service?.running ? "success" : "warning"} />
          <StatusBadge label={`任务 ${tasks.length}`} tone="info" />
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.secondaryButton} onClick={() => void apiFetch("/api/system/monitor-service/start", { method: "POST" }).then(loadAll)} type="button">
              启动服务
            </button>
            <button className={styles.secondaryButton} onClick={() => void apiFetch("/api/system/monitor-service/stop", { method: "POST" }).then(loadAll)} type="button">
              停止服务
            </button>
            <button className={styles.secondaryButton} onClick={() => setEnabledOnly(!enabledOnly)} type="button">
              {enabledOnly ? "显示全部" : "仅看启用"}
            </button>
            <button className={styles.secondaryButton} onClick={() => void apiFetch("/api/smart-monitor/tasks/enable-all?enabled=true", { method: "POST" }).then(loadAll)} type="button">
              全部启用
            </button>
            <button className={styles.secondaryButton} onClick={() => void apiFetch("/api/smart-monitor/tasks/enable-all?enabled=false", { method: "POST" }).then(loadAll)} type="button">
              全部停用
            </button>
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
          </div>
        </section>

        <section className={styles.card}>
          <div className={styles.compactGrid}>
            <div className={styles.metric}>
              <span className={styles.muted}>监控总数</span>
              <strong>{systemStatus?.monitor_service?.total_items ?? 0}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>AI 任务</span>
              <strong>{systemStatus?.monitor_service?.ai_tasks ?? 0}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>待通知</span>
              <strong>{systemStatus?.monitor_service?.pending_notifications ?? 0}</strong>
            </div>
          </div>
        </section>

        <section className={styles.card}>
          <h2>新增盯盘任务</h2>
          <form className={styles.stack} onSubmit={submitTask}>
            <div className={styles.formGrid}>
              <input placeholder="股票代码" value={taskForm.stock_code} onChange={(event) => setTaskForm((current) => ({ ...current, stock_code: event.target.value }))} />
              <input placeholder="股票名称" value={taskForm.stock_name} onChange={(event) => setTaskForm((current) => ({ ...current, stock_name: event.target.value }))} />
              <input placeholder="账户" value={taskForm.account_name} onChange={(event) => setTaskForm((current) => ({ ...current, account_name: event.target.value }))} />
              <input placeholder="任务名称" value={taskForm.task_name} onChange={(event) => setTaskForm((current) => ({ ...current, task_name: event.target.value }))} />
              <input placeholder="检查间隔(秒)" value={taskForm.check_interval} onChange={(event) => setTaskForm((current) => ({ ...current, check_interval: event.target.value }))} />
              <input placeholder="仓位%" value={taskForm.position_size_pct} onChange={(event) => setTaskForm((current) => ({ ...current, position_size_pct: event.target.value }))} />
              <input placeholder="止损%" value={taskForm.stop_loss_pct} onChange={(event) => setTaskForm((current) => ({ ...current, stop_loss_pct: event.target.value }))} />
              <input placeholder="止盈%" value={taskForm.take_profit_pct} onChange={(event) => setTaskForm((current) => ({ ...current, take_profit_pct: event.target.value }))} />
            </div>
            <div className={styles.actions}>
              <label className={styles.listItem}>
                <input checked={taskForm.trading_hours_only} onChange={(event) => setTaskForm((current) => ({ ...current, trading_hours_only: event.target.checked }))} type="checkbox" /> 仅交易时段
              </label>
              <label className={styles.listItem}>
                <input checked={taskForm.enabled} onChange={(event) => setTaskForm((current) => ({ ...current, enabled: event.target.checked }))} type="checkbox" /> 创建后启用
              </label>
              <button className={styles.primaryButton} type="submit">
                保存任务
              </button>
            </div>
          </form>
        </section>

        <section className={styles.card}>
          <h2>手动分析</h2>
          <div className={styles.actions}>
            <input placeholder="股票代码" value={analysisCode} onChange={(event) => setAnalysisCode(event.target.value)} />
            <button
              className={styles.primaryButton}
              onClick={() =>
                void apiFetch("/api/smart-monitor/analyze", {
                  method: "POST",
                  body: JSON.stringify({
                    stock_code: analysisCode,
                    account_name: taskForm.account_name || "默认账户",
                    trading_hours_only: true,
                    notify: false,
                  }),
                }).then(setAnalysisResult)
              }
              type="button"
            >
              立即分析
            </button>
          </div>
          {analysisResult ? <pre className={styles.code}>{JSON.stringify(analysisResult, null, 2)}</pre> : null}
        </section>

        <section className={styles.card}>
          <h2>任务列表</h2>
          <div className={styles.list}>
            {tasks.map((task) => (
              <div className={styles.listItem} key={task.id}>
                <strong>{task.task_name} · {task.stock_code}</strong>
                <p className={styles.muted}>
                  {task.stock_name || task.stock_code} | {task.account_name || "默认账户"} | {task.enabled ? "启用" : "停用"} | {task.check_interval}s
                </p>
                <div className={styles.actions}>
                  <button className={styles.secondaryButton} onClick={() => setTaskForm({ stock_code: task.stock_code, stock_name: task.stock_name || "", account_name: task.account_name || "默认账户", task_name: task.task_name, check_interval: String(task.check_interval), position_size_pct: String(task.position_size_pct), stop_loss_pct: String(task.stop_loss_pct), take_profit_pct: String(task.take_profit_pct), trading_hours_only: true, enabled: Boolean(task.enabled), origin_analysis_id: undefined })} type="button">
                    带入表单
                  </button>
                  <button className={styles.secondaryButton} onClick={() => void apiFetch(`/api/smart-monitor/tasks/${task.id}/enable?enabled=${String(!task.enabled)}`, { method: "POST" }).then(loadAll)} type="button">
                    {task.enabled ? "停用" : "启用"}
                  </button>
                  <button className={styles.dangerButton} onClick={() => void apiFetch(`/api/smart-monitor/tasks/${task.id}`, { method: "DELETE" }).then(loadAll)} type="button">
                    删除
                  </button>
                </div>
              </div>
            ))}
            {tasks.length === 0 ? <div className={styles.muted}>暂无智能盯盘任务</div> : null}
          </div>
        </section>

        <section className={styles.card}>
          <h2>AI 决策</h2>
          <div className={styles.list}>
            {decisions.map((item) => (
              <div className={styles.listItem} key={item.id}>
                <strong>{item.stock_code} · {item.action || "N/A"} · {item.action_status || "suggested"}</strong>
                <p className={styles.muted}>{item.stock_name || item.stock_code} | {item.decision_time || "暂无时间"}</p>
                <p>{item.reasoning || "暂无推理说明"}</p>
              </div>
            ))}
            {decisions.length === 0 ? <div className={styles.muted}>暂无 AI 决策</div> : null}
          </div>
        </section>

        <section className={styles.card}>
          <h2>待办动作</h2>
          <div className={styles.list}>
            {pendingActions.map((item) => (
              <div className={styles.listItem} key={item.id}>
                <strong>{item.action_type || "待办"} · {item.status || "pending"}</strong>
                <p className={styles.muted}>账户: {item.account_name || "默认账户"} | 创建时间: {item.created_at || "暂无"}</p>
                <pre className={styles.code}>{JSON.stringify(item.payload || {}, null, 2)}</pre>
                <div className={styles.actions}>
                  {["accepted", "rejected", "ignored", "done"].map((status) => (
                    <button key={status} className={styles.secondaryButton} onClick={() => void apiFetch(`/api/smart-monitor/pending-actions/${item.id}/resolve`, { method: "POST", body: JSON.stringify({ status, resolution_note: status }) }).then(loadAll)} type="button">
                      {status}
                    </button>
                  ))}
                </div>
              </div>
            ))}
            {pendingActions.length === 0 ? <div className={styles.muted}>暂无待办动作</div> : null}
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
