import { useEffect, useMemo, useState } from "react";

import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from "chart.js";
import { Bar, Line } from "react-chartjs-2";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch, buildQuery, downloadApiFile } from "../../lib/api";
import { asNumber, asText, integerText, numberText } from "../../lib/market";
import styles from "../ConsolePage.module.scss";


ChartJS.register(BarElement, CategoryScale, Legend, LineElement, LinearScale, PointElement, Tooltip);

type Panel = "dashboard" | "analysis" | "alerts" | "trend" | "history" | "settings";

interface TaskDetail<T> {
  id: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
  error?: string;
  result?: T | null;
}

interface NewsFlowResult {
  fetch_time?: string;
  duration?: number;
  flow_data?: Record<string, unknown>;
  model_data?: Record<string, unknown>;
  sentiment_data?: Record<string, unknown>;
  ai_analysis?: Record<string, unknown>;
  trading_signals?: Record<string, unknown>;
  stock_news?: Array<Record<string, unknown>>;
  hot_topics?: Array<Record<string, unknown>>;
}

interface NewsFlowTaskPayload {
  result?: NewsFlowResult;
  message?: string;
}

interface DashboardData {
  latest_snapshot?: Record<string, unknown> | null;
  latest_sentiment?: Record<string, unknown> | null;
  latest_ai_analysis?: Record<string, unknown> | null;
  recent_alerts?: Array<Record<string, unknown>>;
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
  { key: "alerts", label: "预警中心" },
  { key: "trend", label: "趋势分析" },
  { key: "history", label: "历史记录" },
  { key: "settings", label: "设置" },
];

const categoryOptions = [
  { label: "全部平台", value: "" },
  { label: "财经平台", value: "finance" },
  { label: "社交媒体", value: "social" },
  { label: "新闻媒体", value: "news" },
  { label: "科技媒体", value: "tech" },
];

const alertTypeOptions = [
  { label: "全部", value: "" },
  { label: "热度飙升", value: "heat_surge" },
  { label: "流量高潮", value: "flow_peak" },
  { label: "情绪极值", value: "sentiment_extreme" },
  { label: "病毒传播", value: "viral_spread" },
  { label: "流量退潮", value: "flow_decline" },
];

export function NewsFlowPage() {
  const [panel, setPanel] = useState<Panel>("dashboard");
  const [task, setTask] = useState<TaskDetail<NewsFlowTaskPayload> | null>(null);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [alerts, setAlerts] = useState<Array<Record<string, unknown>>>([]);
  const [history, setHistory] = useState<Array<Record<string, unknown>>>([]);
  const [historyDetail, setHistoryDetail] = useState<Record<string, unknown> | null>(null);
  const [trendData, setTrendData] = useState<Record<string, unknown> | null>(null);
  const [sentimentHistory, setSentimentHistory] = useState<Array<Record<string, unknown>>>([]);
  const [dailyStatistics, setDailyStatistics] = useState<Array<Record<string, unknown>>>([]);
  const [aiHistory, setAiHistory] = useState<Array<Record<string, unknown>>>([]);
  const [platforms, setPlatforms] = useState<Array<Record<string, unknown>>>([]);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [schedulerLogs, setSchedulerLogs] = useState<Array<Record<string, unknown>>>([]);
  const [alertConfig, setAlertConfig] = useState<Record<string, string>>({});
  const [searchKeyword, setSearchKeyword] = useState("");
  const [searchResults, setSearchResults] = useState<Array<Record<string, unknown>>>([]);
  const [category, setCategory] = useState("");
  const [alertDays, setAlertDays] = useState("7");
  const [alertType, setAlertType] = useState("");
  const [trendDays, setTrendDays] = useState("7");
  const [taskEnabled, setTaskEnabled] = useState<Record<string, boolean>>({});
  const [taskIntervals, setTaskIntervals] = useState<Record<string, string>>({});
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const currentResult = task?.status === "success" ? task.result?.result ?? null : null;
  const historySnapshot = (historyDetail?.snapshot as Record<string, unknown> | undefined) ?? undefined;
  const historySentiment = (historyDetail?.sentiment as Record<string, unknown> | undefined) ?? undefined;
  const historyAiAnalysis = (historyDetail?.ai_analysis as Record<string, unknown> | undefined) ?? undefined;

  const loadTask = async () => {
    setTask(await apiFetch<TaskDetail<NewsFlowTaskPayload> | null>("/api/strategies/news-flow/tasks/latest"));
  };

  const loadDashboard = async () => {
    const data = await apiFetch<DashboardData>("/api/strategies/news-flow/dashboard");
    setDashboard(data);
    setScheduler((data.scheduler_status as SchedulerStatus | null) ?? null);
    setTrendData((data.flow_trend as Record<string, unknown> | null) ?? null);
  };

  const loadAlerts = async () => {
    setAlerts(await apiFetch<Array<Record<string, unknown>>>(`/api/strategies/news-flow/alerts${buildQuery({ days: alertDays, alert_type: alertType })}`));
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
    const [platformData, schedulerData, configData, logData] = await Promise.all([
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/platforms"),
      apiFetch<SchedulerStatus>("/api/strategies/news-flow/scheduler"),
      apiFetch<Record<string, string>>("/api/strategies/news-flow/alert-config"),
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/scheduler/logs?days=7"),
    ]);
    setPlatforms(platformData);
    setScheduler(schedulerData);
    setAlertConfig(configData);
    setSchedulerLogs(logData);
    setTaskEnabled(schedulerData.task_enabled ?? {});
    setTaskIntervals(Object.fromEntries(Object.entries(schedulerData.task_intervals ?? {}).map(([key, value]) => [key, String(value)])));
  };

  useEffect(() => {
    void Promise.all([loadTask(), loadDashboard(), loadAlerts(), loadHistory(), loadTrend(), loadSettings()]);
    const taskTimer = window.setInterval(() => void loadTask(), 2000);
    const statusTimer = window.setInterval(() => void Promise.all([loadDashboard(), loadSettings()]), 10000);
    return () => {
      window.clearInterval(taskTimer);
      window.clearInterval(statusTimer);
    };
  }, []);

  const topicChartData = useMemo(() => {
    const topics = ((currentResult?.hot_topics as Array<Record<string, unknown>> | undefined) ?? []).slice(0, 10);
    return {
      labels: topics.map((item) => asText(item.topic, "N/A")),
      datasets: [{ label: "热度", data: topics.map((item) => asNumber(item.heat) ?? 0), backgroundColor: "rgba(211, 107, 45, 0.75)" }],
    };
  }, [currentResult]);

  const trendChartData = useMemo(() => ({
    labels: (trendData?.dates as string[] | undefined) ?? [],
    datasets: [
      { label: "平均得分", data: (trendData?.avg_scores as number[] | undefined) ?? [], borderColor: "#b54d2b", backgroundColor: "rgba(181,77,43,0.12)" },
      { label: "最高得分", data: (trendData?.max_scores as number[] | undefined) ?? [], borderColor: "#134074", backgroundColor: "rgba(19,64,116,0.08)" },
    ],
  }), [trendData]);

  const sentimentChartData = useMemo(() => ({
    labels: sentimentHistory.slice().reverse().map((item) => asText(item.fetch_time ?? item.created_at, "").slice(5, 16)),
    datasets: [
      { label: "情绪指数", data: sentimentHistory.slice().reverse().map((item) => asNumber(item.sentiment_index) ?? 50), borderColor: "#6a4c93", backgroundColor: "rgba(106,76,147,0.12)" },
      { label: "K值x20", data: sentimentHistory.slice().reverse().map((item) => (asNumber(item.viral_k) ?? 1) * 20), borderColor: "#2a9d8f", backgroundColor: "rgba(42,157,143,0.08)" },
    ],
  }), [sentimentHistory]);

  const submitAnalysis = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/news-flow/tasks", { method: "POST", body: JSON.stringify({ category: category || null }) });
      setPanel("analysis");
      setMessage(`新闻流量分析任务已提交: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交新闻流量分析失败");
    }
  };

  const runQuickAnalysis = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/strategies/news-flow/quick-analysis", { method: "POST", body: JSON.stringify({ category: category || null }) });
      setMessage("热点同步已完成");
      await Promise.all([loadDashboard(), loadHistory(), loadTrend()]);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "热点同步失败");
    }
  };

  const runAlertCheck = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/strategies/news-flow/alerts/check", { method: "POST" });
      setMessage("预警检查已完成");
      await Promise.all([loadAlerts(), loadDashboard(), loadSettings()]);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "预警检查失败");
    }
  };

  const openHistoryDetail = async (snapshotId: number) => {
    setMessage("");
    setError("");
    try {
      setHistoryDetail(await apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/history/${snapshotId}`));
      setPanel("history");
      setMessage(`已加载历史记录 #${snapshotId}`);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加载历史记录失败");
    }
  };

  const saveAlertConfig = async () => {
    setMessage("");
    setError("");
    try {
      setAlertConfig(await apiFetch<Record<string, string>>("/api/strategies/news-flow/alert-config", { method: "PUT", body: JSON.stringify({ values: alertConfig }) }));
      setMessage("预警配置已保存");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "保存预警配置失败");
    }
  };

  const saveSchedulerConfig = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<SchedulerStatus>("/api/strategies/news-flow/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          task_enabled: taskEnabled,
          task_intervals: Object.fromEntries(Object.entries(taskIntervals).map(([key, value]) => [key, Number(value) || 5])),
        }),
      });
      setScheduler(data);
      setMessage("定时任务配置已更新");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "保存定时任务配置失败");
    }
  };

  const toggleScheduler = async (running: boolean) => {
    setMessage("");
    setError("");
    try {
      setScheduler(await apiFetch<SchedulerStatus>(running ? "/api/strategies/news-flow/scheduler/start" : "/api/strategies/news-flow/scheduler/stop", { method: "POST" }));
      setMessage(running ? "调度器已启动" : "调度器已停止");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新调度器状态失败");
    }
  };

  const runSchedulerTask = async (taskType: string) => {
    setMessage("");
    setError("");
    try {
      await apiFetch(`/api/strategies/news-flow/scheduler/run-now${buildQuery({ task_type: taskType })}`, { method: "POST" });
      setMessage("定时任务已触发");
      await Promise.all([loadDashboard(), loadSettings(), loadHistory()]);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "触发定时任务失败");
    }
  };

  const searchStockNews = async () => {
    if (!searchKeyword.trim()) {
      setSearchResults([]);
      return;
    }
    setMessage("");
    setError("");
    try {
      setSearchResults(await apiFetch<Array<Record<string, unknown>>>(`/api/strategies/news-flow/search-stock-news${buildQuery({ keyword: searchKeyword.trim(), limit: 50 })}`));
      setMessage("历史新闻检索已完成");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "搜索相关新闻失败");
    }
  };

  const exportCurrentPdf = async () => {
    if (!currentResult) {
      return;
    }
    setMessage("");
    setError("");
    try {
      await downloadApiFile("/api/exports/news-flow/pdf", { method: "POST", body: JSON.stringify({ result: currentResult }) });
      setMessage("新闻流量 PDF 已开始下载");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "导出新闻流量报告失败");
    }
  };

  return (
    <PageFrame
      title="新闻流量"
      summary="覆盖仪表盘、实时分析、预警、趋势、历史和调度设置。"
      sectionTabs={panelOptions}
      activeSectionKey={panel}
      onSectionChange={(nextSection) => setPanel(nextSection as Panel)}
      actions={
        <>
          <StatusBadge label={scheduler?.running ? "调度器运行中" : "调度器已停止"} tone={scheduler?.running ? "success" : "warning"} />
          <StatusBadge
            label={task ? `AI 分析 ${task.status} ${Math.round((task.progress ?? 0) * 100)}%` : "AI 分析空闲"}
            tone={task?.status === "success" ? "success" : task?.status === "failed" ? "danger" : task ? "warning" : "default"}
          />
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
          </div>
        </section>

        {task ? (
          <section className={styles.card}>
            <h2>AI 分析任务状态</h2>
            <p>{task.message || "等待新闻流量任务状态..."}</p>
            <p className={styles.muted}>进度: {task.current ?? 0} / {task.total ?? 0}</p>
            {task.error ? <p className={styles.dangerText}>{task.error}</p> : null}
          </section>
        ) : null}

        {panel === "dashboard" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <div className={`${styles.field} ${styles.actionField}`}>
                  <label htmlFor="dashboardCategory">平台类别</label>
                  <select id="dashboardCategory" value={category} onChange={(event) => setCategory(event.target.value)}>
                    {categoryOptions.map((item) => (
                      <option key={item.label} value={item.value}>
                        {item.label}
                      </option>
                    ))}
                  </select>
                </div>
                <button className={styles.primaryButton} onClick={() => void runQuickAnalysis()} type="button">
                  热点同步
                </button>
                <button className={styles.secondaryButton} onClick={() => void runAlertCheck()} type="button">
                  预警检查
                </button>
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.compactGrid}>
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
            </section>

            <section className={styles.card}>
              <h2>流量趋势</h2>
              <Line data={trendChartData} />
              <div className={styles.muted} style={{ marginTop: 12 }}>
                趋势方向: {asText(trendData?.trend, "无数据")} | {asText(trendData?.analysis, "")}
              </div>
            </section>

            <section className={styles.card}>
              <h2>最近预警</h2>
              <div className={styles.list}>
                {(dashboard?.recent_alerts ?? []).slice(0, 5).map((item) => (
                  <div className={styles.listItem} key={String(item.id ?? item.title)}>
                    <strong>[{asText(item.alert_level, "info")}] {asText(item.title)}</strong>
                    <div style={{ marginTop: 8 }}>{asText(item.content, "")}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>{asText(item.created_at, "")}</div>
                  </div>
                ))}
                {!(dashboard?.recent_alerts ?? []).length ? <div className={styles.muted}>暂无预警。</div> : null}
              </div>
            </section>
          </>
        ) : null}

        {panel === "analysis" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <div className={`${styles.field} ${styles.actionField}`}>
                  <label htmlFor="analysisCategory">平台类别</label>
                  <select id="analysisCategory" value={category} onChange={(event) => setCategory(event.target.value)}>
                    {categoryOptions.map((item) => (
                      <option key={item.label} value={item.value}>
                        {item.label}
                      </option>
                    ))}
                  </select>
                </div>
                <button className={styles.primaryButton} onClick={() => void submitAnalysis()} type="button">
                  开始 AI 智能分析
                </button>
                {currentResult ? (
                  <button className={styles.secondaryButton} onClick={() => void exportCurrentPdf()} type="button">
                    导出 PDF
                  </button>
                ) : null}
              </div>
            </section>

            {currentResult ? (
              <>
                <section className={styles.card}>
                  <div className={styles.compactGrid}>
                    <div className={styles.metric}>
                      <span className={styles.muted}>流量得分</span>
                      <strong>{integerText(currentResult.flow_data?.total_score)}</strong>
                      <div className={styles.muted}>{asText(currentResult.flow_data?.level, "中")}</div>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>情绪指数</span>
                      <strong>{integerText(currentResult.sentiment_data?.sentiment_index)}</strong>
                      <div className={styles.muted}>{asText(currentResult.sentiment_data?.sentiment_class, "中性")}</div>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>交易信号</span>
                      <strong>{asText(currentResult.trading_signals?.overall_signal, "观望")}</strong>
                      <div className={styles.muted}>置信度 {integerText(currentResult.trading_signals?.confidence)}%</div>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>分析耗时</span>
                      <strong>{numberText(currentResult.duration, 1)} 秒</strong>
                      <div className={styles.muted}>{asText(currentResult.fetch_time, "-")}</div>
                    </div>
                  </div>
                  {currentResult.trading_signals?.key_message ? (
                    <div className={styles.listItem} style={{ marginTop: 16 }}>
                      <strong>核心提示</strong>
                      <div style={{ marginTop: 10 }}>{asText(currentResult.trading_signals?.key_message, "")}</div>
                    </div>
                  ) : null}
                </section>

                <section className={styles.card}>
                  <h2>流量模型与情绪</h2>
                  <div className={styles.compactGrid}>
                    <div className={styles.listItem}>
                      <strong>流量模型</strong>
                      <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
                        接盘潜力: {numberText(currentResult.model_data?.potential_volume)} 亿元
                        {"\n"}转化率: {numberText((asNumber(currentResult.model_data?.conversion_rate) ?? 0) * 100, 2)}%
                        {"\n"}流量类型: {asText(currentResult.model_data?.flow_type, "未知")}
                        {"\n"}K值: {numberText(currentResult.model_data?.k_value)} ({asText(currentResult.model_data?.trend, "稳定")})
                      </div>
                    </div>
                    <div className={styles.listItem}>
                      <strong>情绪分析</strong>
                      <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
                        情绪分类: {asText(currentResult.sentiment_data?.sentiment_class, "中性")}
                        {"\n"}流量阶段: {asText(currentResult.sentiment_data?.flow_stage, "未知")}
                        {"\n"}风险等级: {asText(currentResult.sentiment_data?.risk_level, "中等")}
                        {"\n"}建议: {asText(currentResult.sentiment_data?.advice, "")}
                      </div>
                    </div>
                  </div>
                </section>

                <section className={styles.card}>
                  <h2>热点话题 TOP10</h2>
                  <Bar data={topicChartData} />
                </section>

                <section className={styles.card}>
                  <h2>AI 选股推荐与新闻</h2>
                  <div className={styles.list}>
                    {((currentResult.ai_analysis?.recommended_stocks as Array<Record<string, unknown>> | undefined) ?? []).slice(0, 8).map((item, index) => (
                      <div className={styles.listItem} key={`stock-${index}`}>
                        <strong>{asText(item.code, "")} {asText(item.name, "")}</strong>
                        <div style={{ marginTop: 8 }}>
                          板块: {asText(item.sector, "N/A")} | 风险: {asText(item.risk_level, "N/A")} | 理由: {asText(item.reason, "")}
                        </div>
                      </div>
                    ))}
                    {(currentResult.stock_news ?? []).slice(0, 5).map((item, index) => (
                      <div className={styles.listItem} key={`news-${index}`}>
                        <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                        <div className={styles.muted} style={{ marginTop: 8 }}>
                          {((item.matched_keywords as string[] | undefined) ?? []).join("、") || "无关键词"} | {asText(item.publish_time, "")}
                        </div>
                      </div>
                    ))}
                    {!(((currentResult.stock_news?.length ?? 0) || ((currentResult.ai_analysis?.recommended_stocks as Array<Record<string, unknown>> | undefined)?.length ?? 0))) ? (
                      <div className={styles.muted}>暂无 AI 结果。</div>
                    ) : null}
                  </div>
                </section>
              </>
            ) : (
              <section className={styles.card}>
                <div className={styles.muted}>暂无 AI 分析结果，请先提交新闻流量分析任务。</div>
              </section>
            )}
          </>
        ) : null}

        {panel === "alerts" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <div className={styles.field}>
                  <label htmlFor="alertDays">时间范围</label>
                  <select id="alertDays" value={alertDays} onChange={(event) => setAlertDays(event.target.value)}>
                    <option value="1">1 天</option>
                    <option value="3">3 天</option>
                    <option value="7">7 天</option>
                    <option value="30">30 天</option>
                  </select>
                </div>
                <div className={styles.field}>
                  <label htmlFor="alertType">预警类型</label>
                  <select id="alertType" value={alertType} onChange={(event) => setAlertType(event.target.value)}>
                    {alertTypeOptions.map((item) => (
                      <option key={item.label} value={item.value}>
                        {item.label}
                      </option>
                    ))}
                  </select>
                </div>
                <button className={styles.primaryButton} onClick={() => void loadAlerts()} type="button">
                  刷新预警
                </button>
                <button className={styles.secondaryButton} onClick={() => void runAlertCheck()} type="button">
                  立即检查
                </button>
              </div>
            </section>

            <section className={styles.card}>
              <h2>预警列表</h2>
              <div className={styles.list}>
                {alerts.map((item) => (
                  <div className={styles.listItem} key={String(item.id ?? item.title)}>
                    <strong>[{asText(item.alert_level, "info")}] {asText(item.title)}</strong>
                    <div style={{ marginTop: 10 }}>{asText(item.content, "")}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      类型: {asText(item.alert_type, "")} | 相关话题: {((item.related_topics as string[] | undefined) ?? []).join("、") || "N/A"} | 时间 {asText(item.created_at, "")}
                    </div>
                  </div>
                ))}
                {!alerts.length ? <div className={styles.muted}>暂无预警记录。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>预警阈值配置</h2>
              <div className={styles.formGrid}>
                {Object.entries(alertConfig).map(([key, value]) => (
                  <div className={styles.field} key={key}>
                    <label htmlFor={key}>{key}</label>
                    <input id={key} value={value} onChange={(event) => setAlertConfig((current) => ({ ...current, [key]: event.target.value }))} />
                  </div>
                ))}
              </div>
              <div className={styles.actions} style={{ marginTop: 16 }}>
                <button className={styles.primaryButton} onClick={() => void saveAlertConfig()} type="button">
                  保存配置
                </button>
              </div>
            </section>
          </>
        ) : null}

        {panel === "trend" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <div className={styles.field}>
                  <label htmlFor="trendDays">分析天数</label>
                  <select id="trendDays" value={trendDays} onChange={(event) => setTrendDays(event.target.value)}>
                    <option value="3">3 天</option>
                    <option value="7">7 天</option>
                    <option value="14">14 天</option>
                    <option value="30">30 天</option>
                  </select>
                </div>
                <button className={styles.primaryButton} onClick={() => void loadTrend()} type="button">
                  刷新趋势
                </button>
              </div>
            </section>

            <section className={styles.card}>
              <h2>流量趋势图</h2>
              <Line data={trendChartData} />
            </section>

            <section className={styles.card}>
              <h2>情绪趋势</h2>
              <Line data={sentimentChartData} />
            </section>

            <section className={styles.card}>
              <h2>每日统计</h2>
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
                    {dailyStatistics.map((item) => (
                      <tr key={asText(item.date, Math.random().toString())}>
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
            </section>
          </>
        ) : null}

        {panel === "history" ? (
          <>
            <section className={styles.card}>
              <h2>历史快照</h2>
              <div className={styles.list}>
                {history.map((item) => (
                  <div className={styles.listItem} key={String(item.id)}>
                    <strong>{asText(item.fetch_time, "")} - 流量得分 {integerText(item.total_score)} ({asText(item.flow_level, "中")})</strong>
                    <div style={{ marginTop: 8 }}>{asText(item.analysis, "")}</div>
                    <div className={styles.actions} style={{ marginTop: 12 }}>
                      <button className={styles.secondaryButton} onClick={() => void openHistoryDetail(Number(item.id))} type="button">
                        查看详情
                      </button>
                    </div>
                  </div>
                ))}
                {!history.length ? <div className={styles.muted}>暂无历史快照。</div> : null}
              </div>
            </section>

            {historyDetail ? (
              <section className={styles.card}>
                <h2>历史详情</h2>
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
              </section>
            ) : null}

            <section className={styles.card}>
              <h2>AI 分析历史</h2>
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
                        <td>{asText(item.created_at ?? item.fetch_time, "").slice(0, 16)}</td>
                        <td>{asText(item.advice, "N/A")}</td>
                        <td>{integerText(item.confidence)}%</td>
                        <td>{asText(item.risk_level, "N/A")}</td>
                        <td>{asText(item.summary, "")}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className={styles.card}>
              <h2>股票相关新闻检索</h2>
              <div className={styles.actions}>
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
                      {((item.matched_keywords as string[] | undefined) ?? []).join("、") || "无关键词"} | {asText(item.fetch_time ?? item.publish_time, "")}
                    </div>
                  </div>
                ))}
                {!searchResults.length ? <div className={styles.muted}>输入关键词后可检索历史股票相关新闻。</div> : null}
              </div>
            </section>
          </>
        ) : null}

        {panel === "settings" ? (
          <>
            <section className={styles.card}>
              <h2>调度器状态</h2>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>运行状态</span>
                  <strong>{scheduler?.running ? "运行中" : "已停止"}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>热点同步</span>
                  <strong>{asText(scheduler?.next_run_times?.sync_hotspots, "N/A").slice(0, 16)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>预警生成</span>
                  <strong>{asText(scheduler?.next_run_times?.generate_alerts, "N/A").slice(0, 16)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>深度分析</span>
                  <strong>{asText(scheduler?.next_run_times?.deep_analysis, "N/A").slice(0, 16)}</strong>
                </div>
              </div>
              <div className={styles.actions} style={{ marginTop: 16 }}>
                <button className={styles.primaryButton} disabled={Boolean(scheduler?.running)} onClick={() => void toggleScheduler(true)} type="button">
                  启动调度器
                </button>
                <button className={styles.secondaryButton} disabled={!scheduler?.running} onClick={() => void toggleScheduler(false)} type="button">
                  停止调度器
                </button>
              </div>
            </section>

            <section className={styles.card}>
              <h2>任务配置</h2>
              <div className={styles.list}>
                {[
                  ["sync_hotspots", "热点同步"],
                  ["generate_alerts", "预警生成"],
                  ["deep_analysis", "深度分析"],
                ].map(([key, label]) => (
                  <div className={styles.listItem} key={key}>
                    <div className={styles.actions}>
                      <label className={`${styles.listItem} ${styles.actionToggle}`}>
                        <input checked={Boolean(taskEnabled[key])} onChange={(event) => setTaskEnabled((current) => ({ ...current, [key]: event.target.checked }))} type="checkbox" /> {label}
                      </label>
                      <input
                        className={styles.shortInput}
                        value={taskIntervals[key] ?? ""}
                        onChange={(event) => setTaskIntervals((current) => ({ ...current, [key]: event.target.value }))}
                      />
                      <span className={styles.muted}>分钟</span>
                      <button className={styles.secondaryButton} onClick={() => void runSchedulerTask(key)} type="button">
                        立即执行
                      </button>
                    </div>
                  </div>
                ))}
              </div>
              <div className={styles.actions} style={{ marginTop: 16 }}>
                <button className={styles.primaryButton} onClick={() => void saveSchedulerConfig()} type="button">
                  保存任务配置
                </button>
              </div>
            </section>

            <section className={styles.card}>
              <h2>支持的平台</h2>
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
            </section>

            <section className={styles.card}>
              <h2>调度日志</h2>
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
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
