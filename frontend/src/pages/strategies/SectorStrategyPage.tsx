import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import {
  SectorReportDetailView,
  type SectorStrategyReportView,
  type SectorStrategySummaryView,
} from "../../components/research/SectorReportDetailView";
import { ApiRequestError, apiFetch, downloadApiFile } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import styles from "../ConsolePage.module.scss";

interface SectorTaskPayload {
  result?: Record<string, unknown>;
  report_view?: SectorStrategyReportView | null;
  lifecycle_snapshot?: Array<Record<string, unknown>> | null;
  data_summary?: Record<string, unknown> | null;
  message?: string;
}

interface TaskDetail<TPayload> {
  id: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
  error?: string;
  result?: TPayload | null;
}

interface SectorHistoryRecord {
  id: number;
  analysis_date?: string;
  created_at?: string;
  data_date_range?: string;
  summary?: string;
  summary_data?: SectorStrategySummaryView & {
    bullish?: string[];
    neutral?: string[];
    bearish?: string[];
  };
}

interface SectorHistoryDetail extends SectorHistoryRecord {
  analysis_content_parsed?: Record<string, unknown>;
  report_view?: SectorStrategyReportView | null;
  lifecycle_items?: Array<Record<string, unknown>> | null;
  lifecycle_summary?: Record<string, unknown> | null;
  daily_heat_panel?: {
    available?: boolean;
    board_date?: string;
    total_count?: number;
    items?: Array<Record<string, unknown>>;
  } | null;
}

interface SchedulerStatus {
  running: boolean;
  enabled?: boolean;
  schedule_time?: string;
  last_run_time?: string | null;
  next_run_time?: string | null;
}

interface LifecycleSnapshot {
  available?: boolean;
  analysis_id?: number;
  analysis_date?: string;
  summary?: Record<string, unknown> | null;
  items?: Array<Record<string, unknown>> | null;
  daily_heat_panel?: {
    available?: boolean;
    board_date?: string;
    total_count?: number;
    items?: Array<Record<string, unknown>>;
  } | null;
}

type LifecycleConfig = Record<string, number>;
interface LifecycleConfigSaveResponse {
  config: LifecycleConfig;
  rebuild_task_id?: string | null;
  rebuild_reused?: boolean;
}
type SectionKey = "overview" | "history" | "scheduler";

const sectionTabs = [
  { key: "overview", label: "分析总览" },
  { key: "history", label: "历史报告" },
  { key: "scheduler", label: "定时设置" },
];

const lifecycleConfigGroups = [
  {
    title: "启动期参数",
    fields: [
      { key: "startup_current_min", label: "3日当前热度下限", step: "0.1" },
      { key: "startup_change_3d_min", label: "3日变化下限", step: "0.1" },
      { key: "startup_slope_3d_min", label: "3日斜率下限", step: "0.1" },
      { key: "startup_current_vs_avg_3d_min", label: "3日高于均值下限", step: "0.1" },
      { key: "startup_acceleration_min", label: "3日加速度下限", step: "0.1" },
      { key: "startup_change_5d_min", label: "5日变化下限", step: "0.1" },
      { key: "startup_drawdown_5d_max", label: "5日回撤上限", step: "0.1" },
      { key: "startup_rising_5d_min", label: "5日上涨天数下限", step: "1" },
      { key: "startup_falling_5d_max", label: "5日下跌天数上限", step: "1" },
      { key: "startup_current_max", label: "当前热度上限", step: "0.1" },
    ],
  },
  {
    title: "爆发期参数",
    fields: [
      { key: "explosive_current_min", label: "10日当前热度下限", step: "0.1" },
      { key: "explosive_avg_10d_min", label: "10日均值下限", step: "0.1" },
      { key: "explosive_slope_10d_min", label: "10日斜率下限", step: "0.1" },
      { key: "explosive_drawdown_10d_max", label: "10日回撤上限", step: "0.1" },
      { key: "explosive_high_heat_days_min", label: "高热天数下限", step: "1" },
      { key: "explosive_rising_5d_min", label: "5日上涨天数下限", step: "1" },
      { key: "explosive_falling_5d_max", label: "5日下跌天数上限", step: "1" },
      { key: "explosive_current_vs_avg_5d_min", label: "5日高于均值下限", step: "0.1" },
    ],
  },
  {
    title: "衰退期参数",
    fields: [
      { key: "decay_peak_min", label: "长窗峰值下限", step: "0.1" },
      { key: "decay_drawdown_long_min", label: "长窗回撤下限", step: "0.1" },
      { key: "decay_change_5d_max", label: "5日变化上限", step: "0.1" },
      { key: "decay_change_3d_max", label: "3日变化上限", step: "0.1" },
      { key: "decay_falling_5d_min", label: "5日下跌天数下限", step: "1" },
      { key: "decay_current_below_avg_min", label: "低于长窗均值下限", step: "0.1" },
    ],
  },
];

function asText(value: unknown, fallback = "暂无"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function formatConfidence(value: unknown): string {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `${Math.round(numeric)}分` : "0分";
}

function taskProgressTone(task: TaskDetail<SectorTaskPayload> | null): "running" | "success" | "danger" {
  if (!task) {
    return "running";
  }
  if (task.status === "success") {
    return "success";
  }
  if (task.status === "failed" || task.status === "cancelled") {
    return "danger";
  }
  return "running";
}

export function SectorStrategyPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [task, setTask] = useState<TaskDetail<SectorTaskPayload> | null>(null);
  const [history, setHistory] = useState<SectorHistoryRecord[]>([]);
  const [historyDetails, setHistoryDetails] = useState<Record<number, SectorHistoryDetail>>({});
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [latestLifecycle, setLatestLifecycle] = useState<LifecycleSnapshot | null>(null);
  const [lifecycleConfig, setLifecycleConfig] = useState<LifecycleConfig>({});
  const [lifecycleRebuildTask, setLifecycleRebuildTask] = useState<TaskDetail<Record<string, unknown>> | null>(null);
  const [scheduleTime, setScheduleTime] = useState("09:00");
  const [section, setSection] = useState<SectionKey>("overview");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [isSubmittingAnalysis, setIsSubmittingAnalysis] = useState(false);
  const [isSavingScheduler, setIsSavingScheduler] = useState(false);
  const [isSavingLifecycleConfig, setIsSavingLifecycleConfig] = useState(false);
  const [isRebuildingLifecycle, setIsRebuildingLifecycle] = useState(false);
  const [isRunningOnce, setIsRunningOnce] = useState(false);
  const [deletingHistoryId, setDeletingHistoryId] = useState<number | null>(null);
  const syncedHistoryReportIdRef = useRef<number | null>(null);
  const syncedLifecycleRebuildTaskRef = useRef<string | null>(null);

  const detailView = searchParams.get("view") === "detail";
  const detailSource = searchParams.get("source");
  const detailReportId = Number(searchParams.get("reportId") || 0);
  const historyDetail = detailReportId ? (historyDetails[detailReportId] ?? null) : null;
  const latestReportView = task?.result?.report_view ?? null;

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail<SectorTaskPayload> | null>("/api/strategies/sector-strategy/tasks/latest");
    setTask(data);
  };

  const loadHistory = async () => {
    const data = await apiFetch<SectorHistoryRecord[]>("/api/strategies/sector-strategy/history");
    setHistory(data);
  };

  const loadScheduler = async () => {
    const data = await apiFetch<SchedulerStatus>("/api/strategies/sector-strategy/scheduler");
    setScheduler(data);
    setScheduleTime(data.schedule_time || "09:00");
  };

  const loadLifecycle = async () => {
    const data = await apiFetch<LifecycleSnapshot>("/api/strategies/sector-strategy/lifecycle/latest");
    setLatestLifecycle(data);
  };

  const loadLifecycleConfig = async () => {
    const data = await apiFetch<LifecycleConfig>("/api/strategies/sector-strategy/lifecycle-config");
    setLifecycleConfig(data);
  };

  const loadLatestLifecycleRebuildTask = async () => {
    const data = await apiFetch<TaskDetail<Record<string, unknown>> | null>("/api/strategies/sector-strategy/lifecycle/rebuild/tasks/latest");
    setLifecycleRebuildTask(data);
  };

  const loadLifecycleRebuildTask = async (taskId: string) => {
    const data = await apiFetch<TaskDetail<Record<string, unknown>>>(`/api/strategies/sector-strategy/lifecycle/rebuild/tasks/${taskId}`);
    setLifecycleRebuildTask(data);
    return data;
  };

  const setSchedulerRunningOptimistically = (running: boolean) => {
    setScheduler((current) =>
      current
        ? {
            ...current,
            running,
          }
        : current,
    );
  };

  const loadHistoryDetail = async (reportId: number) => {
    if (!reportId) {
      return;
    }
    const data = await apiFetch<SectorHistoryDetail>(`/api/strategies/sector-strategy/history/${reportId}`);
    setHistoryDetails((current) => ({ ...current, [reportId]: data }));
  };

  useEffect(() => {
    void Promise.all([loadTask(), loadHistory(), loadScheduler(), loadLifecycle(), loadLifecycleConfig(), loadLatestLifecycleRebuildTask()]);
    const taskTimer = window.setInterval(() => void loadTask(), 2000);
    const schedulerTimer = window.setInterval(() => void loadScheduler(), 10000);
    const lifecycleTimer = window.setInterval(() => void loadLifecycle(), 10000);
    const lifecycleRebuildTimer = window.setInterval(() => void loadLatestLifecycleRebuildTask(), 2000);
    return () => {
      window.clearInterval(taskTimer);
      window.clearInterval(schedulerTimer);
      window.clearInterval(lifecycleTimer);
      window.clearInterval(lifecycleRebuildTimer);
    };
  }, []);

  useEffect(() => {
    const reportId = Number(task?.result?.result?.report_id ?? 0);
    if (task?.status !== "success" || !reportId || syncedHistoryReportIdRef.current === reportId) {
      return;
    }
    syncedHistoryReportIdRef.current = reportId;
    void loadHistory().catch(() => undefined);
  }, [task?.status, task?.result?.result?.report_id]);

  useEffect(() => {
    if (!detailReportId || historyDetails[detailReportId]) {
      return;
    }
    void loadHistoryDetail(detailReportId);
  }, [detailReportId, historyDetails]);

  useEffect(() => {
    if (!detailView) {
      return;
    }
    if (detailReportId) {
      setSection("history");
      return;
    }
    if (detailSource === "latest") {
      setSection("overview");
    }
  }, [detailReportId, detailSource, detailView]);

  const latestSummary = latestReportView?.summary ?? null;
  const detailReportView = detailSource === "latest" ? latestReportView : historyDetail?.report_view ?? null;
  const detailLifecycleItems = detailSource === "latest" ? task?.result?.lifecycle_snapshot ?? [] : historyDetail?.lifecycle_items ?? [];
  const detailDailyHeatPanel = detailSource === "latest" ? latestLifecycle?.daily_heat_panel ?? null : historyDetail?.daily_heat_panel ?? null;
  const detailRawResult = detailSource === "latest" ? task?.result?.result ?? null : historyDetail?.analysis_content_parsed ?? null;

  const detailTitle = useMemo(() => {
    if (detailSource === "latest") {
      return latestSummary?.headline || "最新智策报告";
    }
    return historyDetail?.summary_data?.headline || historyDetail?.summary || "历史智策报告";
  }, [detailSource, historyDetail, latestSummary]);

  const closeDetail = () => {
    setSearchParams({});
  };

  const openLatestDetail = () => {
    setSection("overview");
    setSearchParams({ view: "detail", source: "latest" });
  };

  const openHistoryDetail = (reportId: number) => {
    setSection("history");
    setSearchParams({ view: "detail", reportId: String(reportId) });
  };

  const submitAnalysis = async () => {
    setMessage("");
    setError("");
    setIsSubmittingAnalysis(true);
    try {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/sector-strategy/tasks", {
        method: "POST",
        body: JSON.stringify({}),
      });
      closeDetail();
      setSection("overview");
      setMessage(`智策分析任务已提交: ${data.task_id}`);
      void loadTask().catch(() => undefined);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交智策分析失败");
    } finally {
      setIsSubmittingAnalysis(false);
    }
  };

  const saveScheduler = async (enabled: boolean) => {
    setMessage("");
    setError("");
    setIsSavingScheduler(true);
    const previousScheduler = scheduler;
    setSchedulerRunningOptimistically(enabled);
    try {
      const data = await apiFetch<SchedulerStatus>("/api/strategies/sector-strategy/scheduler", {
        method: "PUT",
        body: JSON.stringify({ enabled, schedule_time: scheduleTime }),
      });
      setScheduler(data);
      setMessage(enabled ? `智策定时任务已更新为每天 ${scheduleTime}` : "智策定时任务已停止");
    } catch (requestError) {
      setScheduler(previousScheduler);
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新智策定时任务失败");
    } finally {
      setIsSavingScheduler(false);
    }
  };

  const runOnce = async () => {
    setMessage("");
    setError("");
    setIsRunningOnce(true);
    try {
      await apiFetch("/api/strategies/sector-strategy/scheduler/run-once", { method: "POST" });
      setSection("overview");
      closeDetail();
      setMessage("已提交一次智策后台分析");
      void loadTask().catch(() => undefined);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交后台分析失败");
    } finally {
      setIsRunningOnce(false);
    }
  };

  const saveLifecycleConfig = async (autoRebuild: boolean) => {
    setMessage("");
    setError("");
    setIsSavingLifecycleConfig(true);
    try {
      const data = await apiFetch<LifecycleConfigSaveResponse>("/api/strategies/sector-strategy/lifecycle-config", {
        method: "PUT",
        body: JSON.stringify({ values: lifecycleConfig, auto_rebuild: autoRebuild }),
      });
      setLifecycleConfig(data.config ?? {});
      if (autoRebuild && data.rebuild_task_id) {
        setMessage(data.rebuild_reused ? "生命周期阈值已更新，已复用进行中的重建任务" : "生命周期阈值已更新，重建任务已提交");
        void loadLifecycleRebuildTask(data.rebuild_task_id).catch(() => undefined);
      } else {
        setMessage("生命周期阈值配置已更新");
      }
      void loadLifecycle().catch(() => undefined);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "保存生命周期阈值失败");
    } finally {
      setIsSavingLifecycleConfig(false);
    }
  };

  const rebuildLifecycle = async () => {
    setMessage("");
    setError("");
    setIsRebuildingLifecycle(true);
    try {
      const data = await apiFetch<{ task_id: string; reused?: boolean }>("/api/strategies/sector-strategy/lifecycle/rebuild", {
        method: "POST",
      });
      setMessage(data.reused ? "已存在进行中的生命周期重建任务，已复用" : "生命周期重建任务已提交");
      void loadLifecycleRebuildTask(data.task_id).catch(() => undefined);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "重建生命周期失败");
    } finally {
      setIsRebuildingLifecycle(false);
    }
  };

  useEffect(() => {
    if (!lifecycleRebuildTask) {
      return;
    }
    if (lifecycleRebuildTask.status !== "success" && lifecycleRebuildTask.status !== "failed" && lifecycleRebuildTask.status !== "cancelled") {
      return;
    }
    if (syncedLifecycleRebuildTaskRef.current === lifecycleRebuildTask.id) {
      return;
    }
    syncedLifecycleRebuildTaskRef.current = lifecycleRebuildTask.id;
    if (lifecycleRebuildTask.status === "success") {
      const result = lifecycleRebuildTask.result ?? {};
      const counts = (((result.result as Record<string, unknown> | undefined)?.latest_summary as Record<string, unknown> | undefined)?.counts as Record<string, number> | undefined) ?? {};
      setMessage(
        `生命周期重建完成: startup=${Number(counts.startup ?? 0)}, explosive=${Number(counts.explosive ?? 0)}, decay=${Number(counts.decay ?? 0)}`,
      );
      void loadLifecycle().catch(() => undefined);
      void loadHistory().catch(() => undefined);
    } else if (lifecycleRebuildTask.status === "failed") {
      setError(lifecycleRebuildTask.error || "生命周期重建失败");
    }
  }, [lifecycleRebuildTask]);

  const updateLifecycleField = (key: string, nextValue: string) => {
    setLifecycleConfig((current) => ({
      ...current,
      [key]: nextValue === "" ? 0 : Number(nextValue),
    }));
  };

  const deleteHistory = async (reportId: number) => {
    setMessage("");
    setError("");
    if (deletingHistoryId === reportId) {
      return;
    }
    const removedIndex = history.findIndex((item) => item.id === reportId);
    const removedRecord = history[removedIndex] ?? null;
    setDeletingHistoryId(reportId);
    setHistory((current) => current.filter((item) => item.id !== reportId));
    try {
      await apiFetch(`/api/strategies/sector-strategy/history/${reportId}`, { method: "DELETE" });
      setHistoryDetails((current) => {
        const next = { ...current };
        delete next[reportId];
        return next;
      });
      if (detailReportId === reportId) {
        closeDetail();
        setSection("history");
      }
      setMessage(`历史报告 #${reportId} 已删除`);
      void loadHistory().catch(() => undefined);
    } catch (requestError) {
      if (removedRecord) {
        setHistory((current) => {
          if (current.some((item) => item.id === removedRecord.id)) {
            return current;
          }
          const next = [...current];
          next.splice(Math.max(0, Math.min(removedIndex, next.length)), 0, removedRecord);
          return next;
        });
      }
      setError(requestError instanceof ApiRequestError ? requestError.message : "删除智策历史报告失败");
    } finally {
      setDeletingHistoryId((current) => current === reportId ? null : current);
    }
  };

  const exportCurrentResult = async (kind: "pdf" | "markdown") => {
    if (!detailRawResult) {
      return;
    }
    setMessage("");
    setError("");
    try {
      await downloadApiFile(`/api/exports/sector-strategy/${kind}`, {
        method: "POST",
        body: JSON.stringify({ result: detailRawResult }),
      });
      setMessage(kind === "pdf" ? "智策 PDF 已开始下载" : "智策 Markdown 已开始下载");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "导出智策报告失败");
    }
  };

  return (
    <PageFrame
      activeSectionKey={detailView ? undefined : section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      sectionTabs={detailView ? undefined : sectionTabs}
      summary="智策板块分析支持最新报告、历史报告与定时任务。"
      title="智策板块"
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />
        {detailView ? (
          <SectorReportDetailView
            backLabel={section === "history" ? "返回历史报告" : "返回分析总览"}
            dailyHeatPanel={detailDailyHeatPanel}
            lifecycleItems={detailLifecycleItems}
            onBack={closeDetail}
            onExport={(kind) => void exportCurrentResult(kind)}
            reportView={detailReportView}
            title={detailTitle}
          />
        ) : (
          <>
          {section === "overview" ? (
            <>
              <section className={styles.card}>
                <div className={styles.actions}>
                  <button className={styles.primaryButton} disabled={isSubmittingAnalysis} onClick={() => void submitAnalysis()} type="button">
                    {isSubmittingAnalysis ? "提交中..." : "开始智策分析"}
                  </button>
                  {latestReportView ? (
                    <button className={styles.secondaryButton} onClick={openLatestDetail} type="button">
                      查看最新报告
                    </button>
                  ) : null}
                </div>
              </section>

              <section className={styles.card}>
                <div className={styles.cardHeader}>
                  <div>
                    <h2>任务状态</h2>
                    <p className={styles.helperText}>{task?.message || "等待智策任务状态..."}</p>
                  </div>
                </div>
                <TaskProgressBar
                  current={task?.current ?? (task?.status === "success" ? task?.total ?? 100 : 0)}
                  total={task?.total ?? 100}
                  message={task?.message || "等待智策任务状态..."}
                  tone={taskProgressTone(task)}
                />
                {task?.error ? <div className={styles.dangerText}>{task.error}</div> : null}
              </section>

              <section className={styles.card}>
                <div className={styles.cardHeader}>
                  <div>
                    <h2>最新报告摘要</h2>
                    <p className={styles.helperText}>{formatDateTime(latestReportView?.meta?.timestamp, "暂无时间")}</p>
                  </div>
                  {latestReportView ? (
                    <button className={styles.secondaryButton} onClick={openLatestDetail} type="button">
                      查看最新报告
                    </button>
                  ) : null}
                </div>

                {latestSummary ? (
                  <div className={styles.historyRecordCard}>
                    <div className={styles.historyListBody}>
                      <strong className={styles.historyRecordTitle}>{latestSummary.headline || "智策板块分析报告"}</strong>
                      <div className={styles.historyListMetrics}>
                        <span className={styles.historyListMetric}>
                          风险等级：<strong>{asText(latestSummary.risk_level)}</strong>
                        </span>
                        <span className={styles.historyListMetric}>
                          市场展望：<strong>{asText(latestSummary.market_outlook)}</strong>
                        </span>
                        <span className={styles.historyListMetric}>
                          信心度：<strong>{formatConfidence(latestSummary.confidence_score)}</strong>
                        </span>
                      </div>
                      <div className={styles.strategySummaryGrid}>
                        <div className={styles.historySummaryCell}>
                          <span>市场观点</span>
                          <strong>{asText(latestSummary.market_view)}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>核心机会</span>
                          <strong>{asText(latestSummary.key_opportunity)}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>主要风险</span>
                          <strong>{asText(latestSummary.major_risk)}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>整体策略</span>
                          <strong>{asText(latestSummary.strategy)}</strong>
                        </div>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className={styles.muted}>暂无最新智策报告。</div>
                )}
              </section>

              <section className={styles.card}>
                <div className={styles.cardHeader}>
                  <div>
                    <h2>生命周期总览</h2>
                    <p className={styles.helperText}>{formatDateTime(latestLifecycle?.analysis_date, "暂无时间")}</p>
                  </div>
                </div>
                  <div className={styles.summaryMetricGrid}>
                  <div className={styles.historySummaryCell}>
                    <span>启动期</span>
                    <strong>{Number((((latestLifecycle?.summary as Record<string, unknown> | null)?.counts as Record<string, unknown> | undefined)?.startup) ?? 0)}</strong>
                  </div>
                  <div className={styles.historySummaryCell}>
                    <span>爆发期</span>
                    <strong>{Number((((latestLifecycle?.summary as Record<string, unknown> | null)?.counts as Record<string, unknown> | undefined)?.explosive) ?? 0)}</strong>
                  </div>
                  <div className={styles.historySummaryCell}>
                    <span>衰退期</span>
                    <strong>{Number((((latestLifecycle?.summary as Record<string, unknown> | null)?.counts as Record<string, unknown> | undefined)?.decay) ?? 0)}</strong>
                  </div>
                  </div>
                  <div className={styles.list} style={{ marginTop: 16 }}>
                    {(latestLifecycle?.daily_heat_panel?.items ?? []).slice(0, 8).map((item, index) => (
                      <div className={styles.listItem} key={`daily-heat-${index}`}>
                        <strong>{asText(item.sector_name)}</strong>
                        <div className={styles.muted}>
                          热度 {Number(item.heat_score ?? 0).toFixed(2)} | 涨跌幅 {Number(item.change_pct ?? 0).toFixed(2)}% | 来源 {asText(item.source_type)}
                        </div>
                      </div>
                    ))}
                    {!(latestLifecycle?.daily_heat_panel?.items ?? []).length ? <div className={styles.muted}>暂无当日热度面板</div> : null}
                  </div>
              </section>
            </>
          ) : null}

          {section === "history" ? (
            <section className={styles.card}>
              <h2 className={styles.mobileDuplicateHeading}>历史报告</h2>
              <div className={styles.list}>
                {history.map((item) => (
                  <div className={styles.historyRecordCard} key={item.id}>
                    <div className={styles.historyRecordTop}>
                      <div>
                        <strong className={styles.historyRecordTitle}>
                          {formatDateTime(item.analysis_date ?? item.created_at, "未知时间")}
                        </strong>
                        <p className={styles.historyMeta}>
                          {item.data_date_range || "暂无数据区间"}
                        </p>
                      </div>
                      <div className={`${styles.historyActionRow} ${styles.historyListActionRow}`}>
                        <button className={styles.secondaryButton} onClick={() => openHistoryDetail(item.id)} type="button">
                          查看报告
                        </button>
                        <button
                          className={styles.dangerButton}
                          disabled={deletingHistoryId === item.id}
                          onClick={() => void deleteHistory(item.id)}
                          type="button"
                        >
                          {deletingHistoryId === item.id ? "删除中..." : "删除"}
                        </button>
                      </div>
                    </div>

                    <div className={styles.historyListBody}>
                      <div className={styles.historyListMetrics}>
                        <span className={styles.historyListMetric}>
                          风险等级：<strong>{asText(item.summary_data?.risk_level)}</strong>
                        </span>
                        <span className={styles.historyListMetric}>
                          市场展望：<strong>{asText(item.summary_data?.market_outlook)}</strong>
                        </span>
                        <span className={styles.historyListMetric}>
                          信心度：<strong>{formatConfidence(item.summary_data?.confidence_score)}</strong>
                        </span>
                      </div>
                      <p className={styles.historyListSummary}>{item.summary_data?.market_view || item.summary || "暂无摘要"}</p>
                    </div>
                  </div>
                ))}
                {!history.length ? <div className={styles.muted}>暂无智策历史报告。</div> : null}
              </div>
            </section>
          ) : null}

          {section === "scheduler" ? (
            <section className={styles.card}>
              <h2 className={styles.mobileDuplicateHeading}>定时分析设置</h2>
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="scheduleTime">定时时间</label>
                  <input id="scheduleTime" onChange={(event) => setScheduleTime(event.target.value)} type="time" value={scheduleTime} />
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>当前状态</span>
                  <strong>{scheduler?.running ? "运行中" : "未运行"}</strong>
                  <div className={styles.muted}>下次运行: {formatDateTime(scheduler?.next_run_time, "-")}</div>
                  <div className={styles.muted}>上次运行: {formatDateTime(scheduler?.last_run_time, "-")}</div>
                </div>
              </div>
              <div className={styles.summaryMetricGrid} style={{ marginTop: 16 }}>
                <label className={styles.switchField}>
                  <span className={styles.switchLabel}>启用定时分析</span>
                  <span className={styles.switchControl}>
                    <input
                      checked={Boolean(scheduler?.running)}
                      disabled={isSavingScheduler}
                      onChange={(event) => void saveScheduler(event.target.checked)}
                      type="checkbox"
                    />
                    <span className={styles.switchTrack} aria-hidden="true">
                      <span className={styles.switchThumb} />
                    </span>
                  </span>
                </label>
                <button className={styles.secondaryButton} disabled={isRunningOnce} onClick={() => void runOnce()} type="button">
                  {isRunningOnce ? "提交中..." : "立即运行"}
                </button>
              </div>
              {lifecycleRebuildTask ? (
                <div style={{ marginTop: 16 }}>
                  <TaskProgressBar
                    current={lifecycleRebuildTask.current ?? 0}
                    message={lifecycleRebuildTask.message || "等待生命周期重建任务状态..."}
                    tone={
                      lifecycleRebuildTask.status === "success"
                        ? "success"
                        : lifecycleRebuildTask.status === "failed" || lifecycleRebuildTask.status === "cancelled"
                          ? "danger"
                          : "running"
                    }
                    total={lifecycleRebuildTask.total ?? 0}
                  />
                </div>
              ) : null}
              <div className={styles.cardHeader} style={{ marginTop: 24 }}>
                <div>
                  <h2>生命周期阈值</h2>
                  <p className={styles.helperText}>启动期、爆发期、衰退期判定参数会直接影响历史回放与智能选股候选。</p>
                </div>
                <button
                  className={styles.primaryButton}
                  disabled={isSavingLifecycleConfig}
                  onClick={() => void saveLifecycleConfig(true)}
                  type="button"
                >
                  {isSavingLifecycleConfig ? "保存并重建中..." : "保存并重建"}
                </button>
              </div>
              <div className={styles.actions} style={{ marginTop: 12 }}>
                <button
                  className={styles.secondaryButton}
                  disabled={isSavingLifecycleConfig}
                  onClick={() => void saveLifecycleConfig(false)}
                  type="button"
                >
                  仅保存阈值
                </button>
                <button
                  className={styles.secondaryButton}
                  disabled={isRebuildingLifecycle}
                  onClick={() => void rebuildLifecycle()}
                  type="button"
                >
                  {isRebuildingLifecycle ? "重建中..." : "仅重建生命周期"}
                </button>
              </div>
              <div className={styles.stack} style={{ marginTop: 16 }}>
                {lifecycleConfigGroups.map((group) => (
                  <section className={styles.card} key={group.title}>
                    <div className={styles.cardHeader}>
                      <div>
                        <h2>{group.title}</h2>
                      </div>
                    </div>
                    <div className={styles.formGrid}>
                      {group.fields.map((field) => (
                        <div className={styles.field} key={field.key}>
                          <label htmlFor={field.key}>{field.label}</label>
                          <input
                            id={field.key}
                            onChange={(event) => updateLifecycleField(field.key, event.target.value)}
                            step={field.step}
                            type="number"
                            value={String(lifecycleConfig[field.key] ?? 0)}
                          />
                        </div>
                      ))}
                    </div>
                  </section>
                ))}
              </div>
            </section>
          ) : null}
          </>
        )}
      </div>
    </PageFrame>
  );
}
