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
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
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
  snapshot_id?: number;
  fetch_time?: string;
  ai_analysis?: Record<string, unknown>;
  stock_news?: Array<Record<string, unknown>>;
  hot_topics?: Array<Record<string, unknown>>;
  platforms_data?: Array<Record<string, unknown>>;
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

function extractAiReportPayload(aiAnalysis: Record<string, unknown> | undefined) {
  const rawPayload = asRecord(aiAnalysis?.raw_payload);
  const investmentAdvice = asRecord(aiAnalysis?.investment_advice ?? rawPayload?.investment_advice);
  const sectorAnalysis = asRecord(aiAnalysis?.sector_analysis ?? rawPayload?.sector_analysis);
  const riskAssess = asRecord(aiAnalysis?.risk_assess ?? rawPayload?.risk_assess);
  const stockRecommend = asRecord(aiAnalysis?.stock_recommend ?? rawPayload?.stock_recommend);
  const multiSector = asRecord(aiAnalysis?.multi_sector ?? rawPayload?.multi_sector);

  return {
    investmentAdvice,
    sectorAnalysis,
    riskAssess,
    stockRecommend,
    multiSector,
    recommendedStocks: extractRecommendedStocks(aiAnalysis),
    benefitedSectors: asDisplayTextArray(
      sectorAnalysis?.benefited_sectors ?? aiAnalysis?.affected_sectors,
      ["name", "sector", "theme"],
    ),
    damagedSectors: asDisplayTextArray(sectorAnalysis?.damaged_sectors, ["name", "sector", "theme"]),
    keyPoints: asDisplayTextArray(sectorAnalysis?.key_points),
    opportunities: asDisplayTextArray(riskAssess?.opportunities),
    riskFactors: asDisplayTextArray(aiAnalysis?.risk_factors ?? riskAssess?.risk_factors),
    actionPlan: asDisplayTextArray(investmentAdvice?.action_plan),
    multiSectorAnalyses: asRecordArray(multiSector?.sector_analyses),
  };
}

function renderSimpleList(items: string[], emptyText = "暂无内容") {
  if (!items.length) {
    return <div className={styles.muted}>{emptyText}</div>;
  }
  return (
    <ol className={styles.detailOrderedList}>
      {items.map((item, index) => (
        <li key={`${item}-${index}`}>{item}</li>
      ))}
    </ol>
  );
}

function buildTrendTopicTimeline(items: Array<Record<string, unknown>>) {
  return items
    .map((item) => {
      return {
        date: asText(item.date, ""),
        topics: (
          Array.isArray(item.top_topics)
            ? (item.top_topics as unknown[])
            : []
        )
          .map((topic) => asText(topic, ""))
          .filter(Boolean)
          .slice(0, 5),
      };
    })
    .filter((item) => item.date && item.topics.length > 0);
}

function renderNewsTitle(item: Record<string, unknown>) {
  const platformName = asText(item.platform_name, "平台");
  const title = asText(item.title, "");
  const url = asText(item.url, "");
  if (url) {
    return (
      <strong>
        [{platformName}]{" "}
        <a href={url} rel="noreferrer" target="_blank">
          {title}
        </a>
      </strong>
    );
  }
  return <strong>[{platformName}] {title}</strong>;
}

function renderNewsTime(item: Record<string, unknown>) {
  const timestamp = formatDateTime(item.publish_time ?? item.fetch_time, "");
  return timestamp ? <div className={styles.muted} style={{ marginTop: 8 }}>{timestamp}</div> : null;
}

function flattenPlatformNews(items: Array<Record<string, unknown>>): Array<Record<string, unknown>> {
  return items.flatMap((platformData) => {
    const platform = asRecord(platformData);
    const platformName = asText(platform.platform_name, "平台");
    const category = asText(platform.category, "");
    const weight = asNumber(platform.weight) ?? 0;
    return asRecordArray(platform.data).map((news) => ({
      ...news,
      platform_name: asText(news.platform_name ?? platformName, platformName),
      category: asText(news.category ?? category, category),
      weight: asNumber(news.weight) ?? weight,
      rank: asNumber(news.rank) ?? 99,
    }));
  });
}

function sortHotNews(items: Array<Record<string, unknown>>): Array<Record<string, unknown>> {
  return items.slice().sort((left, right) => {
    const leftWeight = asNumber(left.weight) ?? 0;
    const rightWeight = asNumber(right.weight) ?? 0;
    const leftRank = asNumber(left.rank) ?? 99;
    const rightRank = asNumber(right.rank) ?? 99;
    if (leftWeight !== rightWeight) {
      return rightWeight - leftWeight;
    }
    return leftRank - rightRank;
  });
}

function buildCategoryHotNews(items: Array<Record<string, unknown>>) {
  const categoryLabels: Array<{ key: string; label: string }> = [
    { key: "finance", label: "财经" },
    { key: "social", label: "社交" },
    { key: "news", label: "新闻" },
    { key: "tech", label: "科技" },
  ];

  return categoryLabels
    .map((category) => ({
      ...category,
      items: sortHotNews(items.filter((item) => asText(item.category, "") === category.key)),
    }))
    .filter((category) => category.items.length > 0);
}

function parseDateValue(value: unknown): Date | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = new Date(String(value).trim().replace(" ", "T"));
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function isRecentWithinHours(value: unknown, hours: number): boolean {
  const parsed = parseDateValue(value);
  if (!parsed) {
    return false;
  }
  return Date.now() - parsed.getTime() <= hours * 60 * 60 * 1000;
}

function getTaskProgressTone(task: TaskDetail<NewsFlowTaskPayload> | null): "running" | "success" | "danger" {
  if (!task) {
    return "running";
  }
  if (task.error || task.status === "failed" || task.status === "error") {
    return "danger";
  }
  if (task.status === "success") {
    return "success";
  }
  return "running";
}

function asNumberList(value: unknown): number[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item));
}

function hasKeys(record: Record<string, unknown> | null | undefined): record is Record<string, unknown> {
  return Object.keys(record ?? {}).length > 0;
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
  const historyAiReport = extractAiReportPayload(historyAiAnalysis);
  const historyAiSummary = extractAiAnalysisSummary(historyAiAnalysis);
  const historyHotTopics = filterMeaningfulTopics(asRecordArray(historyDetail?.hot_topics)).slice(0, 10);
  const historyPlatformNews = asRecordArray(historyDetail?.platform_news);
  const historyRelatedNews = buildCategoryHotNews(historyPlatformNews);
  const detailSnapshot = asRecord(latestSnapshotDetail?.snapshot);
  const dashboardSnapshot = asRecord(dashboard?.latest_snapshot);
  const latestSnapshot = (
    Number(detailSnapshot?.id ?? 0) >= Number(dashboardSnapshot?.id ?? 0)
      ? detailSnapshot
      : dashboardSnapshot
  );
  const latestSnapshotId = Math.max(
    Number(latestSnapshot?.id ?? 0),
    Number(currentResult?.snapshot_id ?? 0),
    Number(history[0]?.id ?? 0),
  );

  const detailSentiment = asRecord(latestSnapshotDetail?.sentiment);
  const dashboardSentiment = asRecord(dashboard?.latest_sentiment);
  const latestSentiment = (
    Number(detailSentiment?.snapshot_id ?? 0) === latestSnapshotId
      ? detailSentiment
      : Number(dashboardSentiment?.snapshot_id ?? 0) === latestSnapshotId
        ? dashboardSentiment
        : {}
  );

  const currentAiAnalysis = asRecord(currentResult?.ai_analysis);
  const detailAiAnalysis = asRecord(latestSnapshotDetail?.ai_analysis);
  const dashboardAiAnalysis = asRecord(dashboard?.latest_ai_analysis);
  const historyAiLatest = asRecord(aiHistory[0]);
  const candidateAiAnalysis =
    (Number(currentResult?.snapshot_id ?? 0) === latestSnapshotId && hasKeys(currentAiAnalysis))
      ? currentAiAnalysis
      : (Number(detailAiAnalysis?.snapshot_id ?? 0) === latestSnapshotId && hasKeys(detailAiAnalysis))
        ? detailAiAnalysis
        : (Number(dashboardAiAnalysis?.snapshot_id ?? 0) === latestSnapshotId && hasKeys(dashboardAiAnalysis))
          ? dashboardAiAnalysis
          : (Number(historyAiLatest?.snapshot_id ?? 0) === latestSnapshotId && hasKeys(historyAiLatest))
            ? historyAiLatest
            : undefined;
  const unifiedAiAnalysis = Number(candidateAiAnalysis?.snapshot_id ?? 0) === latestSnapshotId
    ? candidateAiAnalysis
    : undefined;
  const latestAiAnalysisTimestamp = asText(
    unifiedAiAnalysis?.created_at ??
    unifiedAiAnalysis?.fetch_time ??
    currentResult?.fetch_time ??
    latestSnapshot?.fetch_time,
    "",
  );
  const recentAiAnalysis = isRecentWithinHours(latestAiAnalysisTimestamp, 24) ? unifiedAiAnalysis : undefined;
  const latestAiSummary = extractAiAnalysisSummary(recentAiAnalysis);
  const latestRecommendedStocks = extractRecommendedStocks(recentAiAnalysis).slice(0, 8);
  const latestHotTopics = filterMeaningfulTopics(
    asRecordArray(latestSnapshotDetail?.hot_topics ?? currentResult?.hot_topics),
  ).slice(0, 10);
  const latestHotNews = buildCategoryHotNews(
    latestSnapshotDetail?.platform_news
      ? asRecordArray(latestSnapshotDetail.platform_news)
      : flattenPlatformNews(asRecordArray(currentResult?.platforms_data)),
  );
  const trendAvgScores = asNumberList(trendData?.avg_scores);
  const trendMaxScores = asNumberList(trendData?.max_scores);
  const latestTrendScore = trendAvgScores.length ? trendAvgScores[trendAvgScores.length - 1] : null;
  const averageTrendScore = trendAvgScores.length
    ? trendAvgScores.reduce((sum, value) => sum + value, 0) / trendAvgScores.length
    : null;
  const peakTrendScore = trendMaxScores.length ? Math.max(...trendMaxScores) : null;
  const latestTrendSentiment = sentimentHistory.length ? sentimentHistory[0] : null;
  const trendTopicTimeline = buildTrendTopicTimeline(dailyStatistics);
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

  const loadLatestSnapshotDetail = async (snapshotId: number) => {
    if (!Number.isFinite(snapshotId) || snapshotId <= 0) {
      setLatestSnapshotDetail(null);
      return;
    }
    try {
      setLatestSnapshotDetail(await apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/history/${snapshotId}`));
    } catch {
      setLatestSnapshotDetail(null);
    }
  };

  const loadHistory = async () => {
    const [historyData, aiData] = await Promise.all([
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/history?limit=50"),
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/ai-history?limit=20"),
    ]);
    setHistory(historyData);
    setAiHistory(aiData);
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
    if (latestSnapshotId > 0) {
      void loadLatestSnapshotDetail(latestSnapshotId);
    }
  }, [latestSnapshotId]);

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
      void loadTask().catch(() => undefined);
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
      await apiFetch<Record<string, unknown>>("/api/strategies/news-flow/quick-analysis", {
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
              <div className={styles.responsiveActionGrid}>
                <button className={styles.secondaryButton} disabled={isSubmittingAnalysis} onClick={() => void submitAnalysis()} type="button">
                  {isSubmittingAnalysis ? "提交中..." : "开始 AI 智能分析"}
                </button>
              </div>
            </div>

            {task ? (
              <div className={styles.moduleSection}>
                <strong>AI 分析任务状态</strong>
                <div style={{ marginTop: 12 }}>
                  <TaskProgressBar
                    current={task.current}
                    total={task.total}
                    message={task.message || "等待新闻流量任务状态..."}
                    tone={getTaskProgressTone(task)}
                    showCounter={false}
                  />
                </div>
                {task.error ? <div className={styles.dangerText}>{task.error}</div> : null}
              </div>
            ) : null}

            <div className={styles.moduleSection}>
              <div className={styles.summaryMetricGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>流量得分</span>
                  <strong>{integerText(latestSnapshot?.total_score)}</strong>
                  <div className={styles.muted}>{asText(latestSnapshot?.flow_level, "无数据")}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>情绪指数</span>
                  <strong>{integerText(latestSentiment?.sentiment_index)}</strong>
                  <div className={styles.muted}>{asText(latestSentiment?.sentiment_class, "中性")}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>流量阶段</span>
                  <strong>{asText(latestSentiment?.flow_stage, "未知")}</strong>
                  <div className={styles.muted}>K值 {numberText(latestSentiment?.viral_k)}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>AI 建议</span>
                  <strong>{asText(recentAiAnalysis?.advice, "无近期分析")}</strong>
                  <div className={styles.muted}>
                    {recentAiAnalysis ? `置信度 ${integerText(recentAiAnalysis?.confidence)}%` : "仅展示 24 小时内结果"}
                  </div>
                </div>
              </div>
            </div>

            {recentAiAnalysis ? (
              <div className={styles.moduleSection}>
                <h3>最近一次 AI 分析摘要</h3>
                <div className={styles.listItem}>
                  <div>{latestAiSummary.summary}</div>
                  <div className={styles.muted} style={{ marginTop: 8 }}>
                    建议: {latestAiSummary.advice} | 风险等级: {latestAiSummary.riskLevel} | 置信度 {latestAiSummary.confidence}%
                  </div>
                  {latestAiSummary.affectedSectors.length ? (
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      影响板块: {latestAiSummary.affectedSectors.join("、")}
                    </div>
                  ) : null}
                  {latestAiSummary.riskFactors.length ? (
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险因素: {latestAiSummary.riskFactors.join("、")}
                    </div>
                  ) : null}
                  <div className={styles.muted} style={{ marginTop: 8 }}>
                    分析时间: {formatDateTime(latestAiAnalysisTimestamp, latestAiAnalysisTimestamp)}
                  </div>
                </div>
              </div>
            ) : null}

            {recentAiAnalysis && latestRecommendedStocks.length ? (
              <div className={styles.moduleSection}>
                <h3>AI 选股推荐</h3>
                <div className={styles.list}>
                  {latestRecommendedStocks.map((item, index) => (
                    <div className={styles.listItem} key={`dashboard-stock-${index}`}>
                      <strong>{asText(item.code, "")} {asText(item.name, "")}</strong>
                      <div style={{ marginTop: 8 }}>
                        板块: {asText(item.sector, "N/A")} | 风险: {asText(item.risk_level, "N/A")} | 理由: {asText(item.reason, "暂无推荐理由")}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            <div className={styles.moduleSection}>
              <h3>快照分析</h3>
              {latestSnapshot?.analysis ? (
                <div className={styles.listItem}>
                  <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>{asText(latestSnapshot.analysis, "")}</div>
                </div>
              ) : (
                <div className={styles.muted}>暂无最新快照分析。</div>
              )}
            </div>

            <div className={styles.moduleSection}>
              <h3>热点话题</h3>
              <TopicBubbleCloud cloudKeyPrefix="dashboard" emptyText="暂无最新热点话题。" topics={latestHotTopics} />
            </div>

          </ModuleCard>
        ) : null}

        {panel === "analysis" ? (
          <ModuleCard hideTitleOnMobile title="实时监测">
            <div className={styles.moduleSection}>
              <div className={styles.cardHeader}>
                <h3>监测任务与调度状态</h3>
              </div>

              <div className={styles.responsiveActionGrid}>
                <button
                  className={styles.secondaryButton}
                  onClick={() => setTaskConfigExpanded((current) => !current)}
                  type="button"
                >
                  {taskConfigExpanded ? "收起配置" : "配置监测任务"}
                </button>
                <label className={styles.switchField} style={{ margin: 0 }}>
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

              <div className={styles.listItem}>
                <div className={styles.cardHeader}>
                  <span className={styles.muted}>调度器</span>
                  <strong>{scheduler?.running ? "运行中" : "已停止"}</strong>
                </div>
                <div className={styles.muted} style={{ marginTop: 8 }}>
                  热点同步: {taskEnabled.sync_hotspots
                    ? `${taskIntervals.sync_hotspots ?? "30"} 分钟一次，下一次 ${formatDateTime(scheduler?.next_run_times?.sync_hotspots, "待定")}`
                    : "未启用"}
                </div>
                <div className={styles.muted} style={{ marginTop: 6 }}>
                  深度分析: {taskEnabled.deep_analysis
                    ? `${taskIntervals.deep_analysis ?? "60"} 分钟一次，下一次 ${formatDateTime(scheduler?.next_run_times?.deep_analysis, "待定")}`
                    : "未启用"}
                </div>
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>热点新闻实时列表</h3>
              <div className={styles.list}>
                {latestHotNews.map((group) => (
                  <div className={styles.listItem} key={`analysis-news-${group.key}`}>
                    <strong>{group.label}</strong>
                    <div className={styles.list} style={{ marginTop: 12, maxHeight: 480, overflowY: "auto" }}>
                      {group.items.map((item, index) => (
                        <div className={styles.listItem} key={`analysis-news-${group.key}-${index}`}>
                          {renderNewsTitle(item)}
                          {renderNewsTime(item)}
                        </div>
                      ))}
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
              <div className={styles.summaryMetricGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>趋势判断</span>
                  <strong>{asText(trendData?.trend, "无数据")}</strong>
                  <div className={styles.muted}>{asText(trendData?.analysis, "暂无趋势解读")}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>区间均分</span>
                  <strong>{latestTrendScore === null ? "N/A" : integerText(latestTrendScore)}</strong>
                  <div className={styles.muted}>
                    {averageTrendScore === null ? "暂无均值" : `近${trendDays}天均值 ${averageTrendScore.toFixed(0)}`}
                  </div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>区间高点</span>
                  <strong>{peakTrendScore === null ? "N/A" : integerText(peakTrendScore)}</strong>
                  <div className={styles.muted}>反映近阶段热点冲高水平</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>最新情绪</span>
                  <strong>{integerText(latestTrendSentiment?.sentiment_index)}</strong>
                  <div className={styles.muted}>
                    {latestTrendSentiment
                      ? `${asText(latestTrendSentiment.sentiment_class, "中性")} / ${asText(latestTrendSentiment.flow_stage, "未知")}`
                      : "暂无情绪记录"}
                  </div>
                </div>
              </div>
            </div>

            <div className={styles.moduleSection}>
              <div className={`${styles.noticeCard} ${styles.noticeInfo}`}>
                <strong>趋势解读</strong>
                <div>{asText(trendData?.analysis, "暂无趋势解读。")}</div>
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

            <div className={styles.moduleSection}>
              <h3>情绪历史</h3>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>时间</th>
                      <th>情绪指数</th>
                      <th>情绪分类</th>
                      <th>流量阶段</th>
                      <th>K值</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sentimentHistory.map((item, index) => (
                      <tr key={asText(item.id, `${index}`)}>
                        <td>{formatDateTime(item.fetch_time ?? item.created_at, "")}</td>
                        <td>{integerText(item.sentiment_index)}</td>
                        <td>{asText(item.sentiment_class, "中性")}</td>
                        <td>{asText(item.flow_stage, "未知")}</td>
                        <td>{numberText(item.viral_k)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!sentimentHistory.length ? <div className={styles.muted}>暂无情绪历史。</div> : null}
            </div>

            <div className={styles.moduleSection}>
              <h3>主题演变</h3>
              <div className={styles.strategyEntryList}>
                {trendTopicTimeline.map((item) => (
                  <div className={styles.strategyEntryItem} key={item.date}>
                    <strong>{item.date}</strong>
                    <div>{item.topics.join("、")}</div>
                  </div>
                ))}
              </div>
              {!trendTopicTimeline.length ? <div className={styles.muted}>暂无可追踪的主题演变。</div> : null}
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
                {hasKeys(historyAiAnalysis) ? (
                  <div className={styles.listItem}>
                    <strong>AI 分析摘要</strong>
                    <div style={{ marginTop: 10 }}>{historyAiSummary.summary}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险等级: {historyAiSummary.riskLevel} | 置信度 {historyAiSummary.confidence}%
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      影响板块: {historyAiSummary.affectedSectors.join("、") || "N/A"}
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险因素: {historyAiSummary.riskFactors.join("、") || "N/A"}
                    </div>
                  </div>
                ) : null}
              </div>

              {hasKeys(historyAiAnalysis) ? (
                <div className={styles.moduleSection}>
                  <h3>完整 AI 报告</h3>
                  <div className={styles.historyDetailPanel}>
                    <div className={styles.historyDetailPanelBody}>
                      <div className={styles.summaryMetricGrid}>
                        <div className={styles.metric}>
                          <span className={styles.muted}>最终建议</span>
                          <strong>{historyAiSummary.advice}</strong>
                          <div className={styles.muted}>{historyAiSummary.summary}</div>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>风险等级</span>
                          <strong>{historyAiSummary.riskLevel}</strong>
                          <div className={styles.muted}>
                            风险分 {integerText(historyAiReport.riskAssess?.risk_score)}
                          </div>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>置信度</span>
                          <strong>{historyAiSummary.confidence}%</strong>
                          <div className={styles.muted}>
                            分析耗时 {numberText(historyAiAnalysis.analysis_time, 2)} 秒
                          </div>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>分析时间</span>
                          <strong>{formatDateTime(historyAiAnalysis.created_at ?? historyAiAnalysis.fetch_time, "N/A")}</strong>
                          <div className={styles.muted}>与当前快照绑定展示</div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <details className={styles.historyDetailPanel} open>
                    <summary className={styles.historyDetailSummary}>投资建议</summary>
                    <div className={styles.historyDetailPanelBody}>
                      <div className={styles.strategyEntryList}>
                        <div className={styles.strategyEntryItem}>
                          <strong>建议摘要</strong>
                          <div>{asText(historyAiReport.investmentAdvice?.summary ?? historyAiAnalysis.summary, "暂无摘要")}</div>
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>仓位与时机</strong>
                          <div>仓位建议：{asText(historyAiReport.investmentAdvice?.position_suggestion, "暂无")}</div>
                          <div>执行时机：{asText(historyAiReport.investmentAdvice?.timing, "暂无")}</div>
                          <div>关键提示：{asText(historyAiReport.investmentAdvice?.key_message, "暂无")}</div>
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>行动计划</strong>
                          {renderSimpleList(historyAiReport.actionPlan, "暂无行动计划")}
                        </div>
                      </div>
                    </div>
                  </details>

                  <details className={styles.historyDetailPanel} open>
                    <summary className={styles.historyDetailSummary}>板块影响</summary>
                    <div className={styles.historyDetailPanelBody}>
                      <div className={styles.strategySummaryGrid}>
                        <div className={styles.strategyEntryItem}>
                          <strong>受益板块</strong>
                          {renderSimpleList(historyAiReport.benefitedSectors, "暂无明显受益板块")}
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>承压板块</strong>
                          {renderSimpleList(historyAiReport.damagedSectors, "暂无明显承压板块")}
                        </div>
                      </div>
                      <div className={styles.strategyEntryList} style={{ marginTop: 12 }}>
                        <div className={styles.strategyEntryItem}>
                          <strong>机会评估</strong>
                          <div>{asText(historyAiReport.sectorAnalysis?.opportunity_assessment, "暂无机会评估")}</div>
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>交易建议</strong>
                          <div>{asText(historyAiReport.sectorAnalysis?.trading_suggestion, "暂无交易建议")}</div>
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>关键观察点</strong>
                          {renderSimpleList(historyAiReport.keyPoints, "暂无关键观察点")}
                        </div>
                      </div>
                    </div>
                  </details>

                  <details className={styles.historyDetailPanel} open>
                    <summary className={styles.historyDetailSummary}>个股推荐</summary>
                    <div className={styles.historyDetailPanelBody}>
                      <div className={styles.strategyEntryList}>
                        {historyAiReport.recommendedStocks.map((item, index) => (
                          <div className={styles.strategyEntryItem} key={`history-ai-stock-${index}`}>
                            <strong>{asText(item.code, "")} {asText(item.name, "未命名标的")}</strong>
                            <div>所属板块：{asText(item.sector, "N/A")}</div>
                            <div>策略建议：{asText(item.strategy, asText(historyAiReport.stockRecommend?.overall_strategy, "N/A"))}</div>
                            <div>风险等级：{asText(item.risk_level, "N/A")}</div>
                            <div>推荐理由：{asText(item.reason, "暂无")}</div>
                            <div>催化因素：{asText(item.catalyst, "暂无")}</div>
                          </div>
                        ))}
                      </div>
                      {!historyAiReport.recommendedStocks.length ? (
                        <div className={styles.muted}>暂无个股推荐。</div>
                      ) : null}
                    </div>
                  </details>

                  <details className={styles.historyDetailPanel} open>
                    <summary className={styles.historyDetailSummary}>风险评估</summary>
                    <div className={styles.historyDetailPanelBody}>
                      <div className={styles.strategyEntryList}>
                        <div className={styles.strategyEntryItem}>
                          <strong>风险判断</strong>
                          <div>{asText(historyAiReport.riskAssess?.analysis, "暂无风险分析")}</div>
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>核心预警</strong>
                          <div>{asText(historyAiReport.riskAssess?.key_warning, "暂无核心预警")}</div>
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>主要风险因素</strong>
                          {renderSimpleList(historyAiReport.riskFactors, "暂无风险因素")}
                        </div>
                        <div className={styles.strategyEntryItem}>
                          <strong>潜在机会</strong>
                          {renderSimpleList(historyAiReport.opportunities, "暂无潜在机会")}
                        </div>
                      </div>
                    </div>
                  </details>

                  <details className={styles.historyDetailPanel}>
                    <summary className={styles.historyDetailSummary}>多板块深度分析</summary>
                    <div className={styles.historyDetailPanelBody}>
                      <div className={styles.noticeCard}>
                        <strong>综合结论</strong>
                        <div>{asText(historyAiReport.multiSector?.summary, "暂无多板块综合结论")}</div>
                      </div>
                      <div className={styles.strategyEntryList} style={{ marginTop: 12 }}>
                        {historyAiReport.multiSectorAnalyses.map((item, index) => (
                          <div className={styles.strategyEntryItem} key={`history-sector-${index}`}>
                            <strong>{asText(item.sector_name, `板块 ${index + 1}`)}</strong>
                            <div>短线展望：{asText(item.short_term_outlook, "暂无")}</div>
                            <div>热度级别：{asText(item.heat_level, "暂无")}</div>
                            <div>核心逻辑：{asText(item.core_logic, "暂无")}</div>
                            <div>催化因素：{asText(item.catalyst, "暂无")}</div>
                            <div>风险提示：{asText(item.risk_warning, "暂无")}</div>
                          </div>
                        ))}
                      </div>
                      {!historyAiReport.multiSectorAnalyses.length ? (
                        <div className={styles.muted} style={{ marginTop: 12 }}>暂无多板块深度分析。</div>
                      ) : null}
                    </div>
                  </details>
                </div>
              ) : null}

              <div className={styles.moduleSection}>
                <h3>热点话题</h3>
                <TopicBubbleCloud cloudKeyPrefix="history" emptyText="暂无热点话题详情。" topics={historyHotTopics} />
              </div>

              <div className={styles.moduleSection}>
                <h3>热点新闻</h3>
                <div className={styles.list}>
                  {historyRelatedNews.map((group) => (
                    <div className={styles.listItem} key={`history-news-${group.key}`}>
                      <strong>{group.label}</strong>
                      <div className={styles.list} style={{ marginTop: 12, maxHeight: 480, overflowY: "auto" }}>
                        {group.items.map((item, index) => (
                          <div className={styles.listItem} key={`history-news-${group.key}-${index}`}>
                            {renderNewsTitle(item)}
                            {renderNewsTime(item)}
                          </div>
                        ))}
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
                      {renderNewsTitle(item)}
                      {renderNewsTime(item)}
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
