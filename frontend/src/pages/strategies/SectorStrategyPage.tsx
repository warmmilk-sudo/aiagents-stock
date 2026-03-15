import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import {
  SectorReportDetailView,
  type SectorStrategyReportView,
  type SectorStrategySummaryView,
} from "../../components/research/SectorReportDetailView";
import { ApiRequestError, apiFetch, apiFetchCached, downloadApiFile } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import styles from "../ConsolePage.module.scss";

interface SectorTaskPayload {
  result?: Record<string, unknown>;
  report_view?: SectorStrategyReportView | null;
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
}

interface SchedulerStatus {
  running: boolean;
  enabled?: boolean;
  schedule_time?: string;
  last_run_time?: string | null;
  next_run_time?: string | null;
}

type SectionKey = "overview" | "history" | "scheduler";

const sectionTabs = [
  { key: "overview", label: "分析总览" },
  { key: "history", label: "历史报告" },
  { key: "scheduler", label: "定时设置" },
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

export function SectorStrategyPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [task, setTask] = useState<TaskDetail<SectorTaskPayload> | null>(null);
  const [history, setHistory] = useState<SectorHistoryRecord[]>([]);
  const [historyDetails, setHistoryDetails] = useState<Record<number, SectorHistoryDetail>>({});
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [scheduleTime, setScheduleTime] = useState("09:00");
  const [section, setSection] = useState<SectionKey>("overview");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

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
    const data = await apiFetchCached<SectorHistoryRecord[]>("/api/strategies/sector-strategy/history");
    setHistory(data);
  };

  const loadScheduler = async () => {
    const data = await apiFetch<SchedulerStatus>("/api/strategies/sector-strategy/scheduler");
    setScheduler(data);
    setScheduleTime(data.schedule_time || "09:00");
  };

  const loadHistoryDetail = async (reportId: number) => {
    if (!reportId) {
      return;
    }
    const data = await apiFetchCached<SectorHistoryDetail>(`/api/strategies/sector-strategy/history/${reportId}`);
    setHistoryDetails((current) => ({ ...current, [reportId]: data }));
  };

  useEffect(() => {
    void Promise.all([loadTask(), loadHistory(), loadScheduler()]);
    const taskTimer = window.setInterval(() => void loadTask(), 2000);
    const schedulerTimer = window.setInterval(() => void loadScheduler(), 10000);
    return () => {
      window.clearInterval(taskTimer);
      window.clearInterval(schedulerTimer);
    };
  }, []);

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
    try {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/sector-strategy/tasks", {
        method: "POST",
        body: JSON.stringify({}),
      });
      closeDetail();
      setSection("overview");
      setMessage(`智策分析任务已提交: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交智策分析失败");
    }
  };

  const saveScheduler = async (enabled: boolean) => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<SchedulerStatus>("/api/strategies/sector-strategy/scheduler", {
        method: "PUT",
        body: JSON.stringify({ enabled, schedule_time: scheduleTime }),
      });
      setScheduler(data);
      setMessage(enabled ? `智策定时任务已更新为每天 ${scheduleTime}` : "智策定时任务已停止");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新智策定时任务失败");
    }
  };

  const runOnce = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/strategies/sector-strategy/scheduler/run-once", { method: "POST" });
      setSection("overview");
      closeDetail();
      setMessage("已提交一次智策后台分析");
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交后台分析失败");
    }
  };

  const deleteHistory = async (reportId: number) => {
    setMessage("");
    setError("");
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
      await loadHistory();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "删除智策历史报告失败");
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
      {detailView ? (
        <SectorReportDetailView
          backLabel={section === "history" ? "返回历史报告" : "返回分析总览"}
          onBack={closeDetail}
          onExport={(kind) => void exportCurrentResult(kind)}
          reportView={detailReportView}
          title={detailTitle}
        />
      ) : (
        <div className={styles.stack}>
          {section === "overview" ? (
            <>
              <section className={styles.card}>
                <div className={styles.actions}>
                  <button className={styles.primaryButton} onClick={() => void submitAnalysis()} type="button">
                    开始智策分析
                  </button>
                  {latestReportView ? (
                    <button className={styles.secondaryButton} onClick={openLatestDetail} type="button">
                      查看最新报告
                    </button>
                  ) : null}
                  {message ? <span className={styles.successText}>{message}</span> : null}
                  {error ? <span className={styles.dangerText}>{error}</span> : null}
                </div>
              </section>

              <section className={styles.card}>
                <div className={styles.cardHeader}>
                  <div>
                    <h2>任务状态</h2>
                    <p className={styles.helperText}>{task?.message || "等待智策任务状态..."}</p>
                  </div>
                  <div className={styles.historyMeta}>
                    进度: {task?.current ?? 0} / {task?.total ?? 0}
                  </div>
                </div>
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
            </>
          ) : null}

          {section === "history" ? (
            <section className={styles.card}>
              <h2>历史报告</h2>
              {message ? <p className={styles.successText}>{message}</p> : null}
              {error ? <p className={styles.dangerText}>{error}</p> : null}
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
                        <button className={styles.dangerButton} onClick={() => void deleteHistory(item.id)} type="button">
                          删除
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
              <h2>定时分析设置</h2>
              {message ? <p className={styles.successText}>{message}</p> : null}
              {error ? <p className={styles.dangerText}>{error}</p> : null}
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
                      onChange={(event) => void saveScheduler(event.target.checked)}
                      type="checkbox"
                    />
                    <span className={styles.switchTrack} aria-hidden="true">
                      <span className={styles.switchThumb} />
                    </span>
                  </span>
                </label>
                <button className={styles.secondaryButton} onClick={() => void runOnce()} type="button">
                  立即运行
                </button>
              </div>
            </section>
          ) : null}
        </div>
      )}
    </PageFrame>
  );
}
