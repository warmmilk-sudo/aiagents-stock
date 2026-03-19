import { useEffect, useMemo, useRef, useState } from "react";

import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from "chart.js";
import { Line } from "react-chartjs-2";

import { ModuleCard } from "../../components/common/ModuleCard";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { TopicBubbleCloud, filterMeaningfulTopics } from "../../components/strategies/TopicBubbleCloud";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { usePollingLoader } from "../../hooks/usePollingLoader";
import { asDisplayTextArray, asRecord, asRecordArray } from "../../lib/normalizers";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { asNumber, asText, integerText, numberText } from "../../lib/market";
import styles from "../ConsolePage.module.scss";

ChartJS.register(CategoryScale, Legend, LineElement, LinearScale, PointElement, Tooltip);

type Panel = "dashboard" | "analysis" | "trend" | "history";

interface TaskDetail<T> {
  id: string;
  status: string;
  message: string;
  current?: number;
  total?: number;
  error?: string;
  result?: T | null;
}

interface NewsFlowResult {
  fetch_time?: string;
  ai_analysis?: Record<string, unknown>;
  stock_news?: Array<Record<string, unknown>>;
  hot_topics?: Array<Record<string, unknown>>;
}

interface NewsFlowTaskPayload {
  result?: NewsFlowResult;
}

interface DashboardData {
  latest_snapshot?: Record<string, unknown> | null;
  latest_sentiment?: Record<string, unknown> | null;
  latest_ai_analysis?: Record<string, unknown> | null;
  flow_trend?: Record<string, unknown> | null;
  scheduler_status?: SchedulerStatus | null;
}

interface SchedulerStatus {
  running?: boolean;
  task_enabled?: Record<string, boolean>;
  task_intervals?: Record<string, number>;
  next_run_times?: Record<string, string | null>;
}

const panelOptions: Array<{ key: Panel; label: string }> = [
  { key: "dashboard", label: "仪表盘" },
  { key: "analysis", label: "实时监测" },
  { key: "trend", label: "趋势分析" },
  { key: "history", label: "历史记录" },
];

const categoryOptions = [
  { label: "全部平台", value: "" },
  { label: "财经平台", value: "finance" },
  { label: "社交媒体", value: "social" },
  { label: "新闻媒体", value: "news" },
  { label: "科技媒体", value: "tech" },
];

function extractRecommendedStocks(aiAnalysis: Record<string, unknown> | undefined): Array<Record<string, unknown>> {
  const directRecommendations = asRecordArray(aiAnalysis?.recommended_stocks);
  if (directRecommendations.length) {
    return directRecommendations;
  }
  return asRecordArray(asRecord(aiAnalysis?.stock_recommend)?.recommended_stocks);
}

function extractAiAnalysisSummary(aiAnalysis: Record<string, unknown> | undefined) {
  const investmentAdvice = asRecord(aiAnalysis?.investment_advice);
  const riskAssess = asRecord(aiAnalysis?.risk_assess);
  const sectorAnalysis = asRecord(aiAnalysis?.sector_analysis);
  return {
    summary: asText(aiAnalysis?.summary ?? investmentAdvice?.summary, "暂无摘要"),
    advice: asText(aiAnalysis?.advice ?? investmentAdvice?.advice, "观望"),
    confidence: integerText(aiAnalysis?.confidence ?? investmentAdvice?.confidence),
    riskLevel: asText(aiAnalysis?.risk_level ?? riskAssess?.risk_level, "N/A"),
    riskFactors: asDisplayTextArray(aiAnalysis?.risk_factors ?? riskAssess?.risk_factors),
    affectedSectors: asDisplayTextArray(
      aiAnalysis?.affected_sectors ?? sectorAnalysis?.benefited_sectors,
      ["name", "sector", "theme"],
    ),
    analysisTime: numberText(aiAnalysis?.analysis_time, 2),
    timestamp: asText(aiAnalysis?.fetch_time ?? aiAnalysis?.created_at, ""),
  };
}

export function NewsFlowPage() {
  const syncedSuccessTaskIdRef = useRef("");
  const [panel, setPanel] = useState<Panel>("dashboard");
  const [task, setTask] = useState<TaskDetail<NewsFlowTaskPayload> | null>(null);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [history, setHistory] = useState<Array<Record<string, unknown>>>([]);
  const [historyDetail, setHistoryDetail] = useState<Record<string, unknown> | null>(null);
  const [latestSnapshotDetail, setLatestSnapshotDetail] = useState<Record<string, unknown> | null>(null);
  const [trendData, setTrendData] = useState<Record<string, unknown> | null>(null);
  const [sentimentHistory, setSentimentHistory] = useState<Array<Record<string, unknown>>>([]);
  const [dailyStatistics, setDailyStatistics] = useState<Array<Record<string, unknown>>>([]);
  const [aiHistory, setAiHistory] = useState<Array<Record<string, unknown>>>([]);
  const [platforms, setPlatforms] = useState<Array<Record<string, unknown>>>([]);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [schedulerLogs, setSchedulerLogs] = useState<Array<Record<string, unknown>>>([]);
  const [searchKeyword, setSearchKeyword] = useState("");
  const [searchResults, setSearchResults] = useState<Array<Record<string, unknown>>>([]);
  const [category, setCategory] = useState("");
  const [trendDays, setTrendDays] = useState("7");
  const [taskEnabled, setTaskEnabled] = useState<Record<string, boolean>>({});
  const [taskIntervals, setTaskIntervals] = useState<Record<string, string>>({});
  const [platformsExpanded, setPlatformsExpanded] = useState(false);
  const [taskConfigExpanded, setTaskConfigExpanded] = useState(false);
  const [isSubmittingAnalysis, setIsSubmittingAnalysis] = useState(false);
  const [isRunningQuickAnalysis, setIsRunningQuickAnalysis] = useState(false);
  const [isSavingScheduler, setIsSavingScheduler] = useState(false);
  const [isTogglingScheduler, setIsTogglingScheduler] = useState(false);
  const { message, error, clear, showError, showMessage } = usePageFeedback();

  const currentResult = task?.status === "success" ? task.result?.result ?? null : null;
  const historySnapshot = asRecord(historyDetail?.snapshot);
  const historySentiment = asRecord(historyDetail?.sentiment);
  const historyAiAnalysis = asRecord(historyDetail?.ai_analysis);
  const historyHotTopics = filterMeaningfulTopics(asRecordArray(historyDetail?.hot_topics)).slice(0, 10);
  const historyRelatedNews = asRecordArray(historyDetail?.stock_news).slice(0, 8);
  const latestAiAnalysis =
    asRecord(latestSnapshotDetail?.ai_analysis) ??
    asRecord(dashboard?.latest_ai_analysis) ??
    asRecord(aiHistory[0]) ??
    asRecord(currentResult?.ai_analysis);
  const latestAiSummary = extractAiAnalysisSummary(latestAiAnalysis);
  const latestRecommendedStocks = extractRecommendedStocks(latestAiAnalysis).slice(0, 8);
  const latestHotTopics = filterMeaningfulTopics(
    asRecordArray(latestSnapshotDetail?.hot_topics ?? currentResult?.hot_topics),
  ).slice(0, 10);
  const latestHotNews = asRecordArray(latestSnapshotDetail?.stock_news ?? currentResult?.stock_news).slice(0, 8);
  const schedulerSummaryRows = [
    {
      label: "热点同步状态",
      value: taskEnabled.sync_hotspots ? "已启用" : "已停用",
      note: `每 ${taskIntervals.sync_hotspots ?? "30"} 分钟执行`,
    },
    {
      label: "深度分析状态",
      value: taskEnabled.deep_analysis ? "已启用" : "已停用",
      note: `每 ${taskIntervals.deep_analysis ?? "60"} 分钟执行`,
    },
    {
      label: "运行状态",
      value: scheduler?.running ? "运行中" : "已停止",
    },
    {
      label: "热点同步",
      value: formatDateTime(scheduler?.next_run_times?.sync_hotspots, "N/A"),
    },
    {
      label: "深度分析",
      value: formatDateTime(scheduler?.next_run_times?.deep_analysis, "N/A"),
    },
  ];

  const loadTask = async () => {
    setTask(await apiFetch<TaskDetail<NewsFlowTaskPayload> | null>("/api/strategies/news-flow/tasks/latest"));
  };

  const loadDashboard = async () => {
    const data = await apiFetch<DashboardData>("/api/strategies/news-flow/dashboard");
    setDashboard(data);
    if (!isSavingScheduler && !isTogglingScheduler) {
      setScheduler((data.scheduler_status as SchedulerStatus | null) ?? null);
    }
    setTrendData((data.flow_trend as Record<string, unknown> | null) ?? null);
  };

  const loadHistory = async () => {
    const [historyData, aiData] = await Promise.all([
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/history?limit=50"),
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/ai-history?limit=20"),
    ]);
    setHistory(historyData);
    setAiHistory(aiData);

    const latestSnapshotId = Number(historyData[0]?.id ?? 0);
    if (Number.isFinite(latestSnapshotId) && latestSnapshotId > 0) {
      try {
        setLatestSnapshotDetail(await apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/history/${latestSnapshotId}`));
      } catch {
        setLatestSnapshotDetail(null);
      }
      return;
    }
    setLatestSnapshotDetail(null);
  };

  const loadTrend = async () => {
    const [trend, sentiment, statistics] = await Promise.all([
      apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/trend${buildQuery({ days: trendDays })}`),
      apiFetch<Array<Record<string, unknown>>>(`/api/strategies/news-flow/sentiment-history${buildQuery({ limit: Number(trendDays) * 3 })}`),
      apiFetch<Array<Record<string, unknown>>>(`/api/strategies/news-flow/daily-statistics${buildQuery({ days: trendDays })}`),
    ]);
    setTrendData(trend);
    setSentimentHistory(sentiment);
    setDailyStatistics(statistics);
  };

  const loadSettings = async () => {
    const [platformData, schedulerData, logData] = await Promise.all([
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/platforms"),
      apiFetch<SchedulerStatus>("/api/strategies/news-flow/scheduler"),
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/scheduler/logs?days=7"),
    ]);
    setPlatforms(platformData);
    if (!isSavingScheduler && !isTogglingScheduler) {
      setScheduler(schedulerData);
      setTaskEnabled(schedulerData.task_enabled ?? {});
      setTaskIntervals(
        Object.fromEntries(
          Object.entries(schedulerData.task_intervals ?? {}).map(([key, value]) => [key, String(value)]),
        ),
      );
    }
    setSchedulerLogs(logData);
  };

  const setSchedulerRunningOptimistically = (running: boolean) => {
    setScheduler((current) =>
      current
        ? {
            ...current,
            running,
          }
        : {
            running,
          },
    );
  };

  usePollingLoader({ load: loadTask, intervalMs: 2000 });
  usePollingLoader({
    load: async () => {
      await Promise.all([loadDashboard(), loadSettings()]);
    },
    intervalMs: 10000,
  });

  useEffect(() => {
    void Promise.all([loadDashboard(), loadHistory(), loadTrend(), loadSettings()]);
  }, []);

  useEffect(() => {
    if (task?.status !== "success" || !task.id || syncedSuccessTaskIdRef.current === task.id) {
      return;
    }
    syncedSuccessTaskIdRef.current = task.id;
    void Promise.all([loadDashboard(), loadHistory(), loadTrend()]);
  }, [task?.id, task?.status]);

  useEffect(() => {
    void loadTrend();
  }, [trendDays]);

  const trendChartData = useMemo(
    () => ({
      labels: (trendData?.dates as string[] | undefined) ?? [],
      datasets: [
        {
          label: "平均得分",
          data: (trendData?.avg_scores as number[] | undefined) ?? [],
          borderColor: "#b54d2b",
          backgroundColor: "rgba(181,77,43,0.12)",
        },
        {
          label: "最高得分",
          data: (trendData?.max_scores as number[] | undefined) ?? [],
          borderColor: "#134074",
          backgroundColor: "rgba(19,64,116,0.08)",
        },
      ],
    }),
    [trendData],
  );

  const sentimentChartData = useMemo(
    () => ({
      labels: sentimentHistory.slice().reverse().map((item) => asText(item.fetch_time ?? item.created_at, "").slice(5, 16)),
      datasets: [
        {
          label: "情绪指数",
          data: sentimentHistory.slice().reverse().map((item) => asNumber(item.sentiment_index) ?? 50),
          borderColor: "#6a4c93",
          backgroundColor: "rgba(106,76,147,0.12)",
        },
        {
          label: "K值x20",
          data: sentimentHistory.slice().reverse().map((item) => (asNumber(item.viral_k) ?? 1) * 20),
          borderColor: "#2a9d8f",
          backgroundColor: "rgba(42,157,143,0.08)",
        },
      ],
    }),
    [sentimentHistory],
  );

  const submitAnalysis = async () => {
    clear();
    setIsSubmittingAnalysis(true);
    try {
      await apiFetch<{ task_id: string }>("/api/strategies/news-flow/tasks", {
        method: "POST",
        body: JSON.stringify({ category: category || null }),
      });
      setPanel("analysis");
      showMessage("新闻流量分析任务已提交，正在准备分析...");
      await loadTask().catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "提交新闻流量分析失败");
    } finally {
      setIsSubmittingAnalysis(false);
    }
  };

  const runQuickAnalysis = async () => {
    clear();
    setIsRunningQuickAnalysis(true);
    try {
      await apiFetch("/api/strategies/news-flow/quick-analysis", {
        method: "POST",
        body: JSON.stringify({ category: category || null }),
      });
      showMessage("热点同步已完成");
      await Promise.all([loadDashboard(), loadHistory(), loadTrend()]).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "热点同步失败");
    } finally {
      setIsRunningQuickAnalysis(false);
    }
  };

  const openHistoryDetail = async (snapshotId: number) => {
    clear();
    try {
      setHistoryDetail(await apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/history/${snapshotId}`));
      setPanel("history");
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "加载历史记录失败");
    }
  };

  const saveSchedulerConfig = async () => {
    clear();
    setIsSavingScheduler(true);
    try {
      const data = await apiFetch<SchedulerStatus>("/api/strategies/news-flow/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          task_enabled: taskEnabled,
          task_intervals: Object.fromEntries(
            Object.entries(taskIntervals).map(([key, value]) => [key, Number(value) || 5]),
          ),
        }),
      });
      setScheduler(data);
      setTaskEnabled(data.task_enabled ?? {});
      setTaskIntervals(
        Object.fromEntries(
          Object.entries(data.task_intervals ?? {}).map(([key, value]) => [key, String(value)]),
        ),
      );
      setTaskConfigExpanded(false);
      showMessage("定时任务配置已更新");
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "保存定时任务配置失败");
    } finally {
      setIsSavingScheduler(false);
    }
  };

  const toggleScheduler = async (running: boolean) => {
    clear();
    if (isTogglingScheduler) {
      return;
    }
    const previousScheduler = scheduler;
    setIsTogglingScheduler(true);
    setSchedulerRunningOptimistically(running);
    try {
      setScheduler(
        await apiFetch<SchedulerStatus>(
          running ? "/api/strategies/news-flow/scheduler/start" : "/api/strategies/news-flow/scheduler/stop",
          { method: "POST" },
        ),
      );
      showMessage(running ? "调度器已启动" : "调度器已停止");
    } catch (requestError) {
      setScheduler(previousScheduler);
      showError(requestError instanceof ApiRequestError ? requestError.message : "更新调度器状态失败");
    } finally {
      setIsTogglingScheduler(false);
    }
  };

  const searchStockNews = async () => {
    if (!searchKeyword.trim()) {
      setSearchResults([]);
      return;
    }
    clear();
    try {
      setSearchResults(
        await apiFetch<Array<Record<string, unknown>>>(
          `/api/strategies/news-flow/search-stock-news${buildQuery({
            keyword: searchKeyword.trim(),
            limit: 50,
          })}`,
        ),
      );
      showMessage("历史新闻检索已完成");
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "搜索相关新闻失败");
    }
  };

  return (
    <PageFrame
      title="新闻流量"
      sectionTabs={panelOptions}
      activeSectionKey={panel}
      onSectionChange={(nextSection) => setPanel(nextSection as Panel)}
    >
      <div className={`${styles.stack} ${styles.newsFlowPage}`}>
        <PageFeedback error={error} message={message} />

        {panel === "dashboard" ? (
          <ModuleCard hideTitleOnMobile title="仪表盘">
            <div className={styles.moduleSection}>
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="newsFlowCategory">平台类别</label>
                  <select id="newsFlowCategory" value={category} onChange={(event) => setCategory(event.target.value)}>
                    {categoryOptions.map((item) => (
                      <option key={item.label} value={item.value}>
                        {item.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
              <div className={styles.responsiveActionGrid}>
                <button className={styles.primaryButton} disabled={isRunningQuickAnalysis} onClick={() => void runQuickAnalysis()} type="button">
                  {isRunningQuickAnalysis ? "同步中..." : "热点同步"}
                </button>
              </div>
            </div>

            {task ? (
              <div className={styles.moduleSection}>
                <strong>AI 分析任务状态</strong>
                <div>{task.message || "等待新闻流量任务状态..."}</div>
                <div className={styles.muted}>进度: {task.current ?? 0} / {task.total ?? 0}</div>
                {task.error ? <div className={styles.dangerText}>{task.error}</div> : null}
              </div>
            ) : null}

            <div className={styles.moduleSection}>
              <div className={styles.summaryMetricGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>流量得分</span>
                  <strong>{integerText(dashboard?.latest_snapshot?.total_score)}</strong>
                  <div className={styles.muted}>{asText(dashboard?.latest_snapshot?.flow_level, "无数据")}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>情绪指数</span>
                  <strong>{integerText(dashboard?.latest_sentiment?.sentiment_index)}</strong>
                  <div className={styles.muted}>{asText(dashboard?.latest_sentiment?.sentiment_class, "中性")}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>流量阶段</span>
                  <strong>{asText(dashboard?.latest_sentiment?.flow_stage, "未知")}</strong>
                  <div className={styles.muted}>K值 {numberText(dashboard?.latest_sentiment?.viral_k)}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>AI 建议</span>
                  <strong>{asText(dashboard?.latest_ai_analysis?.advice, "观望")}</strong>
                  <div className={styles.muted}>置信度 {integerText(dashboard?.latest_ai_analysis?.confidence)}%</div>
                </div>
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>快照分析</h3>
              {dashboard?.latest_snapshot?.analysis ? (
                <div className={styles.listItem}>
                  <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>{asText(dashboard.latest_snapshot.analysis, "")}</div>
                </div>
              ) : (
                <div className={styles.muted}>暂无最新快照分析。</div>
              )}
            </div>

            <div className={styles.moduleSection}>
              <h3>热点话题</h3>
              <TopicBubbleCloud cloudKeyPrefix="dashboard" emptyText="暂无最新热点话题。" topics={latestHotTopics} />
            </div>

            <div className={styles.moduleSection}>
              <h3>热点新闻</h3>
              <div className={styles.list}>
                {latestHotNews.map((item, index) => (
                  <div className={styles.listItem} key={`dashboard-news-${index}`}>
                    <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      {(item.matched_keywords as string[] | undefined)?.join("、") || "无关键词"} | {formatDateTime(item.publish_time ?? item.fetch_time, "")}
                    </div>
                  </div>
                ))}
                {!latestHotNews.length ? <div className={styles.muted}>暂无最新热点新闻。</div> : null}
              </div>
            </div>
          </ModuleCard>
        ) : null}

        {panel === "analysis" ? (
          <ModuleCard hideTitleOnMobile title="实时监测">
            <div className={styles.moduleSection}>
              <div className={styles.cardHeader}>
                <div>
                  <h3>监测任务与调度状态</h3>
                </div>
                <button
                  className={styles.secondaryButton}
                  onClick={() => setTaskConfigExpanded((current) => !current)}
                  type="button"
                >
                  {taskConfigExpanded ? "收起配置" : "配置监测任务"}
                </button>
              </div>

              {taskConfigExpanded ? (
                <div className={styles.collapsibleSection}>
                  <div className={styles.list}>
                    {[
                      ["sync_hotspots", "热点同步"],
                      ["deep_analysis", "深度分析"],
                    ].map(([key, label]) => (
                      <div className={styles.listItem} key={key}>
                        <label className={styles.actionToggle}>
                          <input
                            checked={Boolean(taskEnabled[key])}
                            onChange={(event) =>
                              setTaskEnabled((current) => ({ ...current, [key]: event.target.checked }))
                            }
                            type="checkbox"
                          />
                          <span>{label}</span>
                        </label>
                        <div className={styles.field}>
                          <label htmlFor={`news-flow-interval-${key}`}>执行间隔（分钟）</label>
                          <input
                            id={`news-flow-interval-${key}`}
                            value={taskIntervals[key] ?? ""}
                            onChange={(event) =>
                              setTaskIntervals((current) => ({ ...current, [key]: event.target.value }))
                            }
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                  <div className={styles.responsiveActionGrid}>
                    <button className={styles.secondaryButton} disabled={isSavingScheduler} onClick={() => void saveSchedulerConfig()} type="button">
                      {isSavingScheduler ? "保存中..." : "保存任务配置"}
                    </button>
                  </div>
                </div>
              ) : null}

              <div className={styles.list}>
                {schedulerSummaryRows.map((item) => (
                  <div className={styles.listItem} key={item.label}>
                    <div className={styles.cardHeader}>
                      <span className={styles.muted}>{item.label}</span>
                      <strong>{item.value}</strong>
                    </div>
                    {item.note ? <div className={styles.muted}>{item.note}</div> : null}
                  </div>
                ))}
              </div>

              {task ? (
                <div className={styles.listItem}>
                  <strong>当前 AI 任务</strong>
                  <div>{task.message || "等待新闻流量任务状态..."}</div>
                  <div className={styles.muted}>进度: {task.current ?? 0} / {task.total ?? 0}</div>
                  {task.error ? <div className={styles.dangerText}>{task.error}</div> : null}
                </div>
              ) : null}

              <label className={styles.switchField}>
                <span className={styles.switchLabel}>启用调度器</span>
                <span className={styles.switchControl}>
                  <input
                    checked={Boolean(scheduler?.running)}
                    disabled={isSavingScheduler || isTogglingScheduler}
                    onChange={(event) => void toggleScheduler(event.target.checked)}
                    type="checkbox"
                  />
                  <span className={styles.switchTrack} aria-hidden="true">
                    <span className={styles.switchThumb} />
                  </span>
                </span>
              </label>
            </div>

            <div className={styles.moduleSection}>
              <div className={styles.responsiveActionGrid}>
                <button className={styles.primaryButton} disabled={isSubmittingAnalysis} onClick={() => void submitAnalysis()} type="button">
                  {isSubmittingAnalysis ? "提交中..." : "开始 AI 智能分析"}
                </button>
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>最新一次 AI 分析摘要</h3>
              {latestAiAnalysis ? (
                <div className={styles.listItem}>
                  <div>{latestAiSummary.summary}</div>
                  <div className={styles.muted} style={{ marginTop: 8 }}>
                    建议: {latestAiSummary.advice} | 风险等级: {latestAiSummary.riskLevel} | 置信度 {latestAiSummary.confidence}%
                  </div>
                  <div className={styles.muted} style={{ marginTop: 8 }}>
                    影响板块: {latestAiSummary.affectedSectors.join("、") || "N/A"}
                  </div>
                  {latestAiSummary.riskFactors.length ? (
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险因素: {latestAiSummary.riskFactors.join("、")}
                    </div>
                  ) : null}
                  {latestAiSummary.timestamp ? (
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      分析时间: {formatDateTime(latestAiSummary.timestamp, latestAiSummary.timestamp)}
                    </div>
                  ) : null}
                </div>
              ) : (
                <div className={styles.muted}>暂无最新 AI 分析结果，请先提交新闻流量分析任务。</div>
              )}
            </div>

            <div className={styles.moduleSection}>
              <h3>AI 选股推荐</h3>
              <div className={styles.list}>
                {latestRecommendedStocks.map((item, index) => (
                  <div className={styles.listItem} key={`stock-${index}`}>
                    <strong>{asText(item.code, "")} {asText(item.name, "")}</strong>
                    <div style={{ marginTop: 8 }}>
                      板块: {asText(item.sector, "N/A")} | 风险: {asText(item.risk_level, "N/A")} | 理由: {asText(item.reason, "暂无推荐理由")}
                    </div>
                  </div>
                ))}
                {!latestRecommendedStocks.length ? <div className={styles.muted}>当前最新 AI 分析未生成推荐股票。</div> : null}
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>热点新闻实时列表</h3>
              <div className={styles.list}>
                {latestHotNews.map((item, index) => (
                  <div className={styles.listItem} key={`analysis-news-${index}`}>
                    <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      {(item.matched_keywords as string[] | undefined)?.join("、") || "无关键词"} | {formatDateTime(item.publish_time ?? item.fetch_time, "")}
                    </div>
                  </div>
                ))}
                {!latestHotNews.length ? <div className={styles.muted}>暂无实时热点新闻。</div> : null}
              </div>
            </div>

            <div className={styles.moduleSection}>
              <div className={styles.cardHeader}>
                <h3>支持的平台</h3>
                <button className={styles.secondaryButton} onClick={() => setPlatformsExpanded((current) => !current)} type="button">
                  {platformsExpanded ? "收起" : "展开"}
                </button>
              </div>
              {platformsExpanded ? (
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>平台</th>
                        <th>类别</th>
                        <th>权重</th>
                      </tr>
                    </thead>
                    <tbody>
                      {platforms.map((item) => (
                        <tr key={asText(item.platform ?? item.name)}>
                          <td>{asText(item.name, "N/A")}</td>
                          <td>{asText(item.category, "N/A")}</td>
                          <td>{integerText(item.weight)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className={styles.muted}>平台列表默认折叠，点击右上角“展开”查看。</div>
              )}
            </div>
          </ModuleCard>
        ) : null}

        {panel === "trend" ? (
          <ModuleCard hideTitleOnMobile title="趋势分析">
            <div className={styles.moduleSection}>
              <div className={styles.stack}>
                <div className={styles.field}>
                  <label htmlFor="trendDays">分析天数</label>
                  <select id="trendDays" value={trendDays} onChange={(event) => setTrendDays(event.target.value)}>
                    <option value="3">3 天</option>
                    <option value="7">7 天</option>
                    <option value="14">14 天</option>
                    <option value="30">30 天</option>
                  </select>
                </div>
                <div className={styles.responsiveActionGrid}>
                  <button className={styles.secondaryButton} onClick={() => void loadTrend()} type="button">
                    刷新趋势
                  </button>
                </div>
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>流量趋势图</h3>
              <div className={styles.chartWrap}>
                <Line data={trendChartData} />
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>情绪趋势</h3>
              <div className={styles.chartWrap}>
                <Line data={sentimentChartData} />
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>每日统计</h3>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>日期</th>
                      <th>平均得分</th>
                      <th>最高得分</th>
                      <th>最低得分</th>
                      <th>采集次数</th>
                      <th>热门话题</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dailyStatistics.map((item, index) => (
                      <tr key={asText(item.date, String(index))}>
                        <td>{asText(item.date)}</td>
                        <td>{integerText(item.avg_score)}</td>
                        <td>{integerText(item.max_score)}</td>
                        <td>{integerText(item.min_score)}</td>
                        <td>{integerText(item.snapshot_count)}</td>
                        <td>{((item.top_topics as string[] | undefined) ?? []).slice(0, 3).join("、") || "N/A"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!dailyStatistics.length ? <div className={styles.muted}>暂无每日统计数据。</div> : null}
            </div>
          </ModuleCard>
        ) : null}

        {panel === "history" ? (
          historyDetail ? (
            <ModuleCard
              title="历史详情"
              summary="历史快照详情进入单独页面模式，顶部保留返回按钮。"
              toolbar={(
                <button className={styles.secondaryButton} onClick={() => setHistoryDetail(null)} type="button">
                  返回历史列表
                </button>
              )}
            >
              <div className={styles.moduleSection}>
                <div className={styles.compactGrid}>
                  <div className={styles.metric}>
                    <span className={styles.muted}>流量得分</span>
                    <strong>{integerText(historySnapshot?.total_score)}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>流量等级</span>
                    <strong>{asText(historySnapshot?.flow_level, "N/A")}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>情绪指数</span>
                    <strong>{integerText(historySentiment?.sentiment_index)}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>AI 建议</span>
                    <strong>{asText(historyAiAnalysis?.advice, "N/A")}</strong>
                  </div>
                </div>
              </div>

              <div className={styles.moduleSection}>
                {historySnapshot?.analysis ? (
                  <div className={styles.listItem}>
                    <strong>快照分析</strong>
                    <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>{asText(historySnapshot.analysis, "")}</div>
                  </div>
                ) : null}
                {historyAiAnalysis ? (
                  <div className={styles.listItem}>
                    <strong>AI 分析摘要</strong>
                    <div style={{ marginTop: 10 }}>{asText(historyAiAnalysis.summary, "暂无摘要")}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险等级: {asText(historyAiAnalysis.risk_level, "N/A")} | 置信度 {integerText(historyAiAnalysis.confidence)}%
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      影响板块: {asDisplayTextArray(historyAiAnalysis.affected_sectors, ["name", "sector", "theme"]).join("、") || "N/A"}
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险因素: {asDisplayTextArray(historyAiAnalysis.risk_factors).join("、") || "N/A"}
                    </div>
                  </div>
                ) : null}
              </div>

              <div className={styles.moduleSection}>
                <h3>热点话题</h3>
                <TopicBubbleCloud cloudKeyPrefix="history" emptyText="暂无热点话题详情。" topics={historyHotTopics} />
              </div>

              <div className={styles.moduleSection}>
                <h3>热点新闻</h3>
                <div className={styles.list}>
                  {historyRelatedNews.map((item, index) => (
                    <div className={styles.listItem} key={`history-news-${index}`}>
                      <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        {(item.matched_keywords as string[] | undefined)?.join("、") || "无关键词"} | {formatDateTime(item.publish_time ?? item.fetch_time, "")}
                      </div>
                    </div>
                  ))}
                  {!historyRelatedNews.length ? <div className={styles.muted}>暂无热点新闻详情。</div> : null}
                </div>
              </div>
            </ModuleCard>
          ) : (
            <ModuleCard hideTitleOnMobile title="历史记录">
              <div className={styles.moduleSection}>
                <h3>历史快照</h3>
                <div className={styles.list}>
                  {history.map((item) => (
                    <div className={styles.listItem} key={String(item.id)}>
                      <strong>{formatDateTime(item.fetch_time, "")} - 流量得分 {integerText(item.total_score)} ({asText(item.flow_level, "中")})</strong>
                      <div style={{ marginTop: 8 }}>{asText(item.analysis, "")}</div>
                      <div className={styles.responsiveActionGrid} style={{ marginTop: 12 }}>
                        <button className={styles.secondaryButton} onClick={() => void openHistoryDetail(Number(item.id))} type="button">
                          查看详情
                        </button>
                      </div>
                    </div>
                  ))}
                  {!history.length ? <div className={styles.muted}>暂无历史快照。</div> : null}
                </div>
              </div>

              <div className={styles.moduleSection}>
                <h3>AI 分析历史</h3>
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>时间</th>
                        <th>建议</th>
                        <th>置信度</th>
                        <th>风险</th>
                        <th>摘要</th>
                      </tr>
                    </thead>
                    <tbody>
                      {aiHistory.map((item) => (
                        <tr key={String(item.id ?? item.created_at)}>
                          <td>{formatDateTime(item.created_at ?? item.fetch_time, "")}</td>
                          <td>{asText(item.advice, "N/A")}</td>
                          <td>{integerText(item.confidence)}%</td>
                          <td>{asText(item.risk_level, "N/A")}</td>
                          <td>{asText(item.summary, "")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className={styles.moduleSection}>
                <h3>调度日志</h3>
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>时间</th>
                        <th>任务</th>
                        <th>状态</th>
                        <th>消息</th>
                      </tr>
                    </thead>
                    <tbody>
                      {schedulerLogs.map((item) => (
                        <tr key={String(item.id ?? item.executed_at)}>
                          <td>{asText(item.executed_at, "")}</td>
                          <td>{asText(item.task_name, "N/A")}</td>
                          <td>{asText(item.status, "N/A")}</td>
                          <td>{asText(item.message, "")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className={styles.moduleSection}>
                <h3>股票相关新闻检索</h3>
                <div className={styles.responsiveActionGrid}>
                  <input
                    className={styles.actionInput}
                    placeholder="输入股票名、代码或关键词"
                    value={searchKeyword}
                    onChange={(event) => setSearchKeyword(event.target.value)}
                  />
                  <button className={styles.primaryButton} onClick={() => void searchStockNews()} type="button">
                    搜索
                  </button>
                </div>
                <div className={styles.list} style={{ marginTop: 16 }}>
                  {searchResults.map((item, index) => (
                    <div className={styles.listItem} key={`search-${index}`}>
                      <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        {(item.matched_keywords as string[] | undefined)?.join("、") || "无关键词"} | {formatDateTime(item.fetch_time ?? item.publish_time, "")}
                      </div>
                    </div>
                  ))}
                  {!searchResults.length ? <div className={styles.muted}>输入关键词后可检索历史股票相关新闻。</div> : null}
                </div>
              </div>
            </ModuleCard>
          )
        ) : null}
      </div>
    </PageFrame>
  );
}
