import { useEffect, useMemo, useState } from "react";

import {
  ArcElement,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  RadialLinearScale,
  Tooltip,
} from "chart.js";
import { Bar, Pie, Radar } from "react-chartjs-2";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { AnalysisActionButtons, type ActionPayload } from "../../components/research/AnalysisActionButtons";
import { ApiRequestError, apiFetch, downloadApiFile } from "../../lib/api";
import styles from "../ConsolePage.module.scss";


ChartJS.register(
  ArcElement,
  BarElement,
  CategoryScale,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  RadialLinearScale,
  Tooltip,
);

type Panel = "analysis" | "history" | "statistics";

const sectionTabs = [
  { key: "analysis", label: "龙虎榜分析" },
  { key: "history", label: "历史报告" },
  { key: "statistics", label: "数据统计" },
];

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

interface LonghubangResult {
  success?: boolean;
  timestamp?: string;
  report_id?: number;
  error?: string;
  data_info?: {
    total_records?: number;
    total_stocks?: number;
    total_youzi?: number;
    data_source?: string;
    update_hint?: string;
    summary?: {
      total_buy_amount?: number;
      total_sell_amount?: number;
      total_net_inflow?: number;
      top_youzi?: Record<string, number>;
      top_stocks?: Array<{ code?: string; name?: string; net_inflow?: number }>;
      hot_concepts?: Record<string, number>;
    };
  };
  final_report?: { summary?: string };
  recommended_stocks?: Array<{
    rank?: number;
    code?: string;
    name?: string;
    net_inflow?: number;
    reason?: string;
    confidence?: string;
    hold_period?: string;
  }>;
  agents_analysis?: Record<string, { analysis?: string; agent_role?: string; timestamp?: string }>;
  scoring_ranking?: Array<{
    排名?: number;
    股票名称?: string;
    股票代码?: string;
    综合评分?: number;
    资金含金量?: number;
    净买入额?: number;
    卖出压力?: number;
    机构共振?: number;
    加分项?: number;
    净流入?: number;
  }>;
}

interface LonghubangTaskPayload {
  result?: LonghubangResult;
  message?: string;
}

interface HistoryRecord {
  id: number;
  analysis_date?: string;
  data_date_range?: string;
  summary?: string;
  summary_data?: {
    recommended_count?: number;
    total_records?: number;
    total_stocks?: number;
    total_youzi?: number;
    top_concepts?: string[];
  };
  result_payload?: LonghubangResult;
}

interface StatisticsPayload {
  stats?: {
    total_records?: number;
    total_stocks?: number;
    total_youzi?: number;
    total_reports?: number;
    date_range?: { start?: string; end?: string };
  };
  window_days?: number;
  top_youzi?: Array<{ youzi_name?: string; trade_count?: number; total_net_inflow?: number }>;
  top_stocks?: Array<{ stock_code?: string; stock_name?: string; youzi_count?: number; total_net_inflow?: number }>;
}

interface BatchItem {
  symbol?: string;
  success?: boolean;
  error?: string;
  record_id?: number;
  stock_info?: Record<string, unknown>;
  final_decision?: Record<string, unknown>;
}

interface BatchResult {
  results?: BatchItem[];
  total?: number;
  success?: number;
  failed?: number;
  elapsed_time?: number;
  analysis_mode?: string;
  analysis_date?: string;
}

const SOURCE_TEXT = "数据来源：lhb-api.ws4.cn 龙虎榜接口（/v1/youzi/all），交易日 17:30 后通常可获取当天数据。";
const AGENTS = [
  ["youzi", "游资行为分析师"],
  ["stock", "个股潜力分析师"],
  ["theme", "题材追踪分析师"],
  ["risk", "风险控制专家"],
  ["chief", "首席策略师综合研判"],
] as const;

function localDateText(value = new Date()): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function asText(value: unknown, fallback = "N/A"): string {
  return value === null || value === undefined || value === "" ? fallback : String(value);
}

function asNumber(value: unknown): number | null {
  const result = Number(String(value ?? "").replace(/,/g, "").trim());
  return Number.isFinite(result) ? result : null;
}

function numberText(value: unknown, digits = 2): string {
  const result = asNumber(value);
  return result === null ? "N/A" : result.toFixed(digits);
}

function currencyText(value: unknown): string {
  const result = asNumber(value);
  return result === null ? "N/A" : result.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
}

function integerText(value: unknown): string {
  const result = asNumber(value);
  return result === null ? "N/A" : String(Math.round(result));
}

function normalizeSymbol(value: unknown): string {
  const text = asText(value, "").trim().toUpperCase();
  if (!text) {
    return "";
  }
  return text.includes(".") ? text.split(".")[0] : text;
}

function parseEntryMin(entryRange: unknown): number | undefined {
  const text = asText(entryRange, "");
  if (!text.includes("-")) {
    return undefined;
  }
  const result = asNumber(text.split("-")[0]);
  return result ?? undefined;
}

function dateHint(dateText: string): { tone: "warning" | "info"; message: string } | null {
  if (!dateText) {
    return null;
  }
  const target = new Date(`${dateText}T00:00:00`);
  if (Number.isNaN(target.getTime())) {
    return null;
  }
  if ([0, 6].includes(target.getDay())) {
    return { tone: "info", message: "所选日期是非交易日，龙虎榜通常没有当日数据。" };
  }
  const now = new Date();
  if (dateText === localDateText(now) && (now.getHours() < 17 || (now.getHours() === 17 && now.getMinutes() < 30))) {
    return { tone: "warning", message: "今日龙虎榜数据通常在交易日 17:30 后更新，当前时段可能还拿不到当天数据。" };
  }
  return null;
}

function batchActionPayload(item: BatchItem): ActionPayload | null {
  const stockInfo = item.stock_info ?? {};
  const finalDecision = item.final_decision ?? {};
  const symbol = normalizeSymbol(item.symbol ?? stockInfo.symbol ?? stockInfo["股票代码"]);
  if (!symbol) {
    return null;
  }
  return {
    symbol,
    stock_name: asText(stockInfo.name ?? stockInfo["股票名称"] ?? stockInfo["股票简称"], symbol),
    account_name: "默认账户",
    origin_analysis_id: item.record_id,
    default_cost_price: parseEntryMin(finalDecision.entry_range),
    default_note: asText(finalDecision.advice, ""),
    strategy_context: { source: "longhubang_batch" },
  };
}

export function LonghubangPage() {
  const [panel, setPanel] = useState<Panel>("analysis");
  const [analysisMode, setAnalysisMode] = useState<"specified" | "recent">("specified");
  const [selectedDate, setSelectedDate] = useState(localDateText());
  const [days, setDays] = useState("1");
  const [batchCount, setBatchCount] = useState("3");
  const [batchMode, setBatchMode] = useState<"sequential" | "parallel">("sequential");
  const [maxWorkers, setMaxWorkers] = useState("3");
  const [task, setTask] = useState<TaskDetail<LonghubangTaskPayload> | null>(null);
  const [batchTask, setBatchTask] = useState<TaskDetail<BatchResult> | null>(null);
  const [history, setHistory] = useState<HistoryRecord[]>([]);
  const [selectedReport, setSelectedReport] = useState<HistoryRecord | null>(null);
  const [loadedResult, setLoadedResult] = useState<LonghubangResult | null>(null);
  const [loadedReportId, setLoadedReportId] = useState<number | null>(null);
  const [dismissedTaskId, setDismissedTaskId] = useState("");
  const [dismissedBatchTaskId, setDismissedBatchTaskId] = useState("");
  const [statistics, setStatistics] = useState<StatisticsPayload | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadTask = async () => setTask(await apiFetch<TaskDetail<LonghubangTaskPayload> | null>("/api/strategies/longhubang/tasks/latest"));
  const loadBatchTask = async () =>
    setBatchTask(await apiFetch<TaskDetail<BatchResult> | null>("/api/strategies/longhubang/batch-tasks/latest"));
  const loadHistory = async () => setHistory(await apiFetch<HistoryRecord[]>("/api/strategies/longhubang/history"));
  const loadStatistics = async () =>
    setStatistics(await apiFetch<StatisticsPayload>("/api/strategies/longhubang/statistics?days=30"));

  useEffect(() => {
    void Promise.all([loadTask(), loadBatchTask(), loadHistory(), loadStatistics()]);
    const taskTimer = window.setInterval(() => {
      void loadTask();
      void loadBatchTask();
    }, 2000);
    const dataTimer = window.setInterval(() => {
      void loadHistory();
      void loadStatistics();
    }, 10000);
    return () => {
      window.clearInterval(taskTimer);
      window.clearInterval(dataTimer);
    };
  }, []);

  const visibleTaskResult = task?.status === "success" && task.id !== dismissedTaskId ? task.result?.result ?? null : null;
  const visibleBatchResult = batchTask?.status === "success" && batchTask.id !== dismissedBatchTaskId ? batchTask.result ?? null : null;
  const currentResult = loadedResult ?? visibleTaskResult;
  const currentHint = analysisMode === "specified" ? dateHint(selectedDate) : null;

  const scoringRows = useMemo(
    () =>
      (currentResult?.scoring_ranking ?? []).map((item) => ({
        ...item,
        排名: asNumber(item.排名) ?? 0,
        综合评分: asNumber(item.综合评分) ?? 0,
        资金含金量: asNumber(item.资金含金量) ?? 0,
        净买入额: asNumber(item.净买入额) ?? 0,
        卖出压力: asNumber(item.卖出压力) ?? 0,
        机构共振: asNumber(item.机构共振) ?? 0,
        加分项: asNumber(item.加分项) ?? 0,
        净流入: asNumber(item.净流入) ?? 0,
      })),
    [currentResult],
  );
  const batchSymbols = useMemo(
    () => scoringRows.slice(0, Math.max(1, Number(batchCount) || 3)).map((item) => normalizeSymbol(item.股票代码)).filter(Boolean),
    [batchCount, scoringRows],
  );
  const summary = currentResult?.data_info?.summary;
  const topStocks = (summary?.top_stocks ?? []).slice(0, 10);
  const topYouzi = Object.entries(summary?.top_youzi ?? {}).slice(0, 10);
  const hotConcepts = Object.entries(summary?.hot_concepts ?? {}).slice(0, 15);

  const scoreBarData = useMemo(
    () => ({
      labels: scoringRows.slice(0, 10).map((item) => asText(item.股票名称)),
      datasets: [{ label: "综合评分", data: scoringRows.slice(0, 10).map((item) => item.综合评分 ?? 0), backgroundColor: "#c46b3d", borderRadius: 10 }],
    }),
    [scoringRows],
  );
  const radarData = useMemo(
    () => ({
      labels: ["资金含金量", "净买入额", "卖出压力", "机构共振", "加分项"],
      datasets: scoringRows.slice(0, 5).map((item, index) => ({
        label: asText(item.股票名称, `TOP${index + 1}`),
        data: [((item.资金含金量 ?? 0) / 30) * 100, ((item.净买入额 ?? 0) / 25) * 100, ((item.卖出压力 ?? 0) / 20) * 100, ((item.机构共振 ?? 0) / 15) * 100, ((item.加分项 ?? 0) / 10) * 100],
        backgroundColor: `rgba(${80 + index * 25}, ${110 + index * 15}, ${140 + index * 10}, 0.15)`,
        borderColor: `rgba(${80 + index * 25}, ${110 + index * 15}, ${140 + index * 10}, 0.95)`,
      })),
    }),
    [scoringRows],
  );
  const topStocksChart = useMemo(
    () => ({
      labels: topStocks.map((item) => asText(item.name)),
      datasets: [{ label: "净流入金额", data: topStocks.map((item) => asNumber(item.net_inflow) ?? 0), backgroundColor: "#3b7ea1", borderRadius: 10 }],
    }),
    [topStocks],
  );
  const hotConceptChart = useMemo(
    () => ({
      labels: hotConcepts.map(([name]) => name),
      datasets: [{ label: "出现次数", data: hotConcepts.map(([, count]) => asNumber(count) ?? 0), backgroundColor: ["#c46b3d", "#3b7ea1", "#5f8b4c", "#d2a03d", "#8b5d7d", "#7d6b5d", "#557f8d", "#9b6e3f", "#4f6f6c", "#b35a4c", "#6b8e23", "#708090", "#c08497", "#8d99ae", "#52796f"] }],
    }),
    [hotConcepts],
  );

  const withRequest = async (runner: () => Promise<void>, fallback: string) => {
    setMessage("");
    setError("");
    try {
      await runner();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : fallback);
    }
  };

  const submitAnalysis = async () =>
    withRequest(async () => {
      if (analysisMode === "specified" && currentHint?.tone === "warning") {
        throw new ApiRequestError(400, currentHint.message);
      }
      const data = await apiFetch<{ task_id: string }>("/api/strategies/longhubang/tasks", {
        method: "POST",
        body: JSON.stringify({ date: analysisMode === "specified" ? selectedDate : null, days: analysisMode === "recent" ? Math.max(1, Math.min(10, Number(days) || 1)) : 1 }),
      });
      setLoadedResult(null);
      setLoadedReportId(null);
      setDismissedTaskId("");
      setPanel("analysis");
      setMessage(`龙虎榜分析任务已提交: ${data.task_id}`);
      await loadTask();
    }, "提交龙虎榜分析失败");

  const submitBatch = async () =>
    withRequest(async () => {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/longhubang/batch-tasks", {
        method: "POST",
        body: JSON.stringify({ symbols: batchSymbols, analysis_mode: batchMode, max_workers: Math.max(1, Math.min(5, Number(maxWorkers) || 1)) }),
      });
      setDismissedBatchTaskId("");
      setMessage(`龙虎榜 TOP 批量分析任务已提交: ${data.task_id}`);
      await loadBatchTask();
    }, "提交龙虎榜批量分析失败");

  const openHistory = async (reportId: number, loadIntoAnalysis = false) =>
    withRequest(async () => {
      const data = await apiFetch<HistoryRecord>(`/api/strategies/longhubang/history/${reportId}`);
      setSelectedReport(data);
      if (loadIntoAnalysis) {
        setLoadedResult(data.result_payload ?? null);
        setLoadedReportId(reportId);
        setDismissedTaskId("");
        setPanel("analysis");
        setMessage(`历史报告 #${reportId} 已加载到分析页`);
      } else {
        setPanel("history");
        setMessage(`已加载龙虎榜历史报告 #${reportId}`);
      }
    }, "加载龙虎榜历史报告失败");

  const deleteHistory = async (reportId: number) =>
    withRequest(async () => {
      await apiFetch(`/api/strategies/longhubang/history/${reportId}`, { method: "DELETE" });
      if (selectedReport?.id === reportId) {
        setSelectedReport(null);
      }
      if (loadedReportId === reportId) {
        setLoadedResult(null);
        setLoadedReportId(null);
      }
      setMessage(`龙虎榜历史报告 #${reportId} 已删除`);
      await loadHistory();
    }, "删除龙虎榜历史报告失败");

  const exportResult = async (result: LonghubangResult | null, kind: "pdf" | "markdown") =>
    withRequest(async () => {
      if (!result) {
        return;
      }
      await downloadApiFile(`/api/exports/longhubang/${kind}`, { method: "POST", body: JSON.stringify({ result }) });
      setMessage(kind === "pdf" ? "智瞰龙虎 PDF 已开始下载" : "智瞰龙虎 Markdown 已开始下载");
    }, "导出智瞰龙虎报告失败");

  return (
    <PageFrame
      title="智瞰龙虎"
      summary="当前覆盖龙虎榜分析、TOP 批量深度分析、历史报告、统计与导出。"
      sectionTabs={sectionTabs}
      activeSectionKey={panel}
      onSectionChange={(nextSection) => setPanel(nextSection as Panel)}
      actions={
        <>
          <StatusBadge label={task ? `分析 ${task.status} ${Math.round((task.progress ?? 0) * 100)}%` : "分析空闲"} tone={task?.status === "success" ? "success" : task?.status === "failed" ? "danger" : task?.status === "running" ? "warning" : "default"} />
          <StatusBadge label={batchTask ? `批量 ${batchTask.status} ${Math.round((batchTask.progress ?? 0) * 100)}%` : "批量空闲"} tone={batchTask?.status === "success" ? "success" : batchTask?.status === "failed" ? "danger" : batchTask?.status === "running" ? "warning" : "default"} />
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <p className={styles.muted} style={{ marginTop: 12 }}>{SOURCE_TEXT}</p>
          {message ? <p className={styles.successText}>{message}</p> : null}
          {error ? <p className={styles.dangerText}>{error}</p> : null}
        </section>

        {panel === "analysis" ? (
          <>
            <section className={styles.card}>
              <h2>分析任务</h2>
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="analysisMode">分析模式</label>
                  <select id="analysisMode" value={analysisMode} onChange={(event) => setAnalysisMode(event.target.value as "specified" | "recent")}>
                    <option value="specified">指定日期</option>
                    <option value="recent">最近 N 天</option>
                  </select>
                </div>
                {analysisMode === "specified" ? (
                  <div className={styles.field}>
                    <label htmlFor="selectedDate">选择日期</label>
                    <input id="selectedDate" type="date" value={selectedDate} onChange={(event) => setSelectedDate(event.target.value)} />
                  </div>
                ) : (
                  <div className={styles.field}>
                    <label htmlFor="days">最近天数</label>
                    <input id="days" value={days} onChange={(event) => setDays(event.target.value)} />
                  </div>
                )}
              </div>
              {currentHint ? <p className={currentHint.tone === "warning" ? styles.dangerText : styles.muted}>{currentHint.message}</p> : null}
              <div className={styles.actions}>
                <button className={styles.primaryButton} onClick={() => void submitAnalysis()} type="button">开始分析</button>
                <button className={styles.secondaryButton} onClick={() => { setLoadedResult(null); setLoadedReportId(null); if (task?.status === "success") setDismissedTaskId(task.id); setMessage("当前分析结果已清除。"); }} type="button">清除结果</button>
              </div>
              {task ? <div className={styles.listItem} style={{ marginTop: 16 }}><strong>任务状态</strong><div style={{ marginTop: 8 }}>{task.message || "等待任务执行..."}</div><div className={styles.muted}>进度: {task.current ?? 0} / {task.total ?? 0}</div>{task.error ? <div className={styles.dangerText}>{task.error}</div> : null}</div> : null}
            </section>

            {currentResult ? (
              <>
                <section className={styles.card}>
                  <div className={styles.actions}>
                    <h2>{loadedReportId ? `已加载历史报告 #${loadedReportId}` : "当前分析结果"}</h2>
                    <button className={styles.secondaryButton} onClick={() => void exportResult(currentResult, "pdf")} type="button">导出 PDF</button>
                    <button className={styles.secondaryButton} onClick={() => void exportResult(currentResult, "markdown")} type="button">导出 Markdown</button>
                  </div>
                  <div className={styles.compactGrid}>
                    <div className={styles.metric}><span className={styles.muted}>龙虎榜记录</span><strong>{currentResult.data_info?.total_records ?? 0}</strong></div>
                    <div className={styles.metric}><span className={styles.muted}>涉及股票</span><strong>{currentResult.data_info?.total_stocks ?? 0}</strong></div>
                    <div className={styles.metric}><span className={styles.muted}>涉及游资</span><strong>{currentResult.data_info?.total_youzi ?? 0}</strong></div>
                    <div className={styles.metric}><span className={styles.muted}>推荐股票</span><strong>{currentResult.recommended_stocks?.length ?? 0}</strong></div>
                  </div>
                  <p className={styles.muted} style={{ marginTop: 12 }}>
                    分析时间: {asText(currentResult.timestamp, "N/A")}
                    {currentResult.data_info?.data_source ? ` | 数据来源: ${currentResult.data_info.data_source}` : ""}
                    {currentResult.data_info?.update_hint ? ` | ${currentResult.data_info.update_hint}` : ""}
                  </p>
                  {currentResult.final_report?.summary ? <p>{currentResult.final_report.summary}</p> : null}
                </section>

                {scoringRows.length ? (
                  <section className={styles.card}>
                    <div className={styles.actions}>
                      <h2>AI 智能评分排名</h2>
                      <select value={batchCount} onChange={(event) => setBatchCount(event.target.value)}>
                        <option value="3">TOP 3</option>
                        <option value="5">TOP 5</option>
                        <option value="10">TOP 10</option>
                      </select>
                      <select value={batchMode} onChange={(event) => setBatchMode(event.target.value as "sequential" | "parallel")}>
                        <option value="sequential">顺序分析</option>
                        <option value="parallel">并行分析</option>
                      </select>
                      <input
                        className={styles.shortInput}
                        disabled={batchMode !== "parallel"}
                        value={maxWorkers}
                        onChange={(event) => setMaxWorkers(event.target.value)}
                      />
                      <button className={styles.primaryButton} onClick={() => void submitBatch()} type="button">开始批量分析</button>
                    </div>
                    <p className={styles.muted}>将按 AI 评分排序对前 {batchSymbols.length} 只股票执行完整深度分析：{batchSymbols.join(", ") || "暂无"}</p>
                    <div className={styles.grid}>
                      <div className={`${styles.card} ${styles.span6}`}>
                        <h3>TOP10 综合评分</h3>
                        <Bar data={scoreBarData} options={{ responsive: true, plugins: { legend: { display: false } } }} />
                      </div>
                      <div className={`${styles.card} ${styles.span6}`}>
                        <h3>TOP5 五维评分</h3>
                        <Radar data={radarData} options={{ responsive: true, scales: { r: { beginAtZero: true, max: 100 } } }} />
                      </div>
                    </div>
                    <div className={styles.tableWrap} style={{ marginTop: 16 }}>
                      <table className={styles.table}>
                        <thead>
                          <tr><th>排名</th><th>股票</th><th>综合评分</th><th>资金含金量</th><th>净买入额</th><th>卖出压力</th><th>机构共振</th><th>加分项</th><th>净流入</th></tr>
                        </thead>
                        <tbody>
                          {scoringRows.map((item, index) => (
                            <tr key={`${normalizeSymbol(item.股票代码) || "score"}-${index}`}>
                              <td>{integerText(item.排名)}</td>
                              <td>{asText(item.股票名称)} ({asText(item.股票代码)})</td>
                              <td>{numberText(item.综合评分, 1)}</td>
                              <td>{numberText(item.资金含金量, 1)}</td>
                              <td>{numberText(item.净买入额, 1)}</td>
                              <td>{numberText(item.卖出压力, 1)}</td>
                              <td>{numberText(item.机构共振, 1)}</td>
                              <td>{numberText(item.加分项, 1)}</td>
                              <td>{currencyText(item.净流入)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </section>
                ) : null}

                {(currentResult.recommended_stocks?.length ?? 0) > 0 ? (
                  <section className={styles.card}>
                    <h2>AI 推荐股票</h2>
                    <div className={styles.list}>
                      {currentResult.recommended_stocks?.map((item, index) => (
                        <div className={styles.listItem} key={`${normalizeSymbol(item.code) || "recommended"}-${index}`}>
                          <strong>第 {item.rank ?? index + 1} 名 · {asText(item.name)} ({asText(item.code)})</strong>
                          <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                            <div><div className={styles.muted}>净流入金额</div><div>{currencyText(item.net_inflow)}</div></div>
                            <div><div className={styles.muted}>确定性</div><div>{asText(item.confidence)}</div></div>
                            <div><div className={styles.muted}>持有周期</div><div>{asText(item.hold_period)}</div></div>
                          </div>
                          <div style={{ marginTop: 12, whiteSpace: "pre-wrap" }}>{asText(item.reason, "暂无推荐理由")}</div>
                        </div>
                      ))}
                    </div>
                  </section>
                ) : null}

                {Object.keys(currentResult.agents_analysis ?? {}).length ? (
                  <section className={styles.card}>
                    <h2>AI 分析师团队报告</h2>
                    <div className={styles.list}>
                      {AGENTS.map(([key, title]) => {
                        const report = currentResult.agents_analysis?.[key];
                        return report ? (
                          <details className={styles.listItem} key={key}>
                            <summary>{title}</summary>
                            <div style={{ marginTop: 12, whiteSpace: "pre-wrap" }}>{asText(report.analysis, "暂无分析")}</div>
                            <div className={styles.muted} style={{ marginTop: 8 }}>{asText(report.agent_role, "")}{report.timestamp ? ` | ${report.timestamp}` : ""}</div>
                          </details>
                        ) : null;
                      })}
                    </div>
                  </section>
                ) : null}

                <section className={styles.card}>
                  <h2>数据详情与可视化</h2>
                  <div className={styles.compactGrid}>
                    <div className={styles.metric}><span className={styles.muted}>总买入金额</span><strong>{currencyText(summary?.total_buy_amount)}</strong></div>
                    <div className={styles.metric}><span className={styles.muted}>总卖出金额</span><strong>{currencyText(summary?.total_sell_amount)}</strong></div>
                    <div className={styles.metric}><span className={styles.muted}>净流入金额</span><strong>{currencyText(summary?.total_net_inflow)}</strong></div>
                  </div>
                  <div className={styles.grid} style={{ marginTop: 18 }}>
                    <div className={`${styles.card} ${styles.span6}`}><h3>TOP10 股票资金净流入</h3>{topStocks.length ? <Bar data={topStocksChart} options={{ responsive: true, plugins: { legend: { display: false } } }} /> : <div className={styles.muted}>暂无净流入图表数据</div>}</div>
                    <div className={`${styles.card} ${styles.span6}`}><h3>热门概念分布</h3>{hotConcepts.length ? <Pie data={hotConceptChart} options={{ responsive: true }} /> : <div className={styles.muted}>暂无热门概念图表数据</div>}</div>
                  </div>
                  <div className={styles.grid} style={{ marginTop: 18 }}>
                    <div className={`${styles.card} ${styles.span6}`}>
                      <h3>活跃游资 TOP10</h3>
                      <div className={styles.tableWrap}>
                        <table className={styles.table}><thead><tr><th>排名</th><th>游资名称</th><th>净流入金额</th></tr></thead><tbody>{topYouzi.map(([name, amount], index) => <tr key={`${name}-${index}`}><td>{index + 1}</td><td>{name}</td><td>{currencyText(amount)}</td></tr>)}</tbody></table>
                      </div>
                    </div>
                    <div className={`${styles.card} ${styles.span6}`}>
                      <h3>资金净流入 TOP20 股票</h3>
                      <div className={styles.tableWrap}>
                        <table className={styles.table}><thead><tr><th>股票代码</th><th>股票名称</th><th>净流入金额</th></tr></thead><tbody>{(summary?.top_stocks ?? []).slice(0, 20).map((item, index) => <tr key={`${normalizeSymbol(item.code) || "stock"}-${index}`}><td>{asText(item.code)}</td><td>{asText(item.name)}</td><td>{currencyText(item.net_inflow)}</td></tr>)}</tbody></table>
                      </div>
                    </div>
                  </div>
                </section>
              </>
            ) : <section className={styles.card}><div className={styles.muted}>当前还没有龙虎榜分析结果。</div></section>}

            {batchTask ? <section className={styles.card}><div className={styles.actions}><h2>批量分析任务状态</h2><button className={styles.secondaryButton} onClick={() => { if (batchTask?.status === "success") setDismissedBatchTaskId(batchTask.id); setMessage("当前批量分析结果已清除。"); }} type="button">清除批量结果</button></div><p>{batchTask.message || "等待批量分析任务..."}</p><p className={styles.muted}>进度: {batchTask.current ?? 0} / {batchTask.total ?? 0}</p>{batchTask.error ? <p className={styles.dangerText}>{batchTask.error}</p> : null}</section> : null}

            {visibleBatchResult ? (
              <section className={styles.card}>
                <h2>TOP 股票批量深度分析结果</h2>
                <div className={styles.compactGrid}>
                  <div className={styles.metric}><span className={styles.muted}>总计</span><strong>{visibleBatchResult.total ?? 0}</strong></div>
                  <div className={styles.metric}><span className={styles.muted}>成功</span><strong>{visibleBatchResult.success ?? 0}</strong></div>
                  <div className={styles.metric}><span className={styles.muted}>失败</span><strong>{visibleBatchResult.failed ?? 0}</strong></div>
                  <div className={styles.metric}><span className={styles.muted}>耗时(秒)</span><strong>{numberText(visibleBatchResult.elapsed_time)}</strong></div>
                </div>
                <p className={styles.muted}>生成时间: {asText(visibleBatchResult.analysis_date, "-")} | 分析模式: {asText(visibleBatchResult.analysis_mode, "-")}</p>
                <div className={styles.list}>
                  {(visibleBatchResult.results ?? []).filter((item) => item.success).map((item, index) => {
                    const stockInfo = item.stock_info ?? {};
                    const finalDecision = item.final_decision ?? {};
                    return (
                      <div className={styles.listItem} key={`${normalizeSymbol(item.symbol) || "batch"}-${index}`}>
                        <strong>{asText(item.symbol)} - {asText(stockInfo.name ?? stockInfo["股票名称"], asText(item.symbol))}</strong>
                        <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                          <div><div className={styles.muted}>评级</div><div>{asText(finalDecision.rating)}</div></div>
                          <div><div className={styles.muted}>信心度</div><div>{asText(finalDecision.confidence_level)}</div></div>
                          <div><div className={styles.muted}>进场区间</div><div>{asText(finalDecision.entry_range)}</div></div>
                          <div><div className={styles.muted}>目标价</div><div>{asText(finalDecision.target_price)}</div></div>
                          <div><div className={styles.muted}>止盈 / 止损</div><div>{asText(finalDecision.take_profit)} / {asText(finalDecision.stop_loss)}</div></div>
                        </div>
                        {finalDecision.advice ? <div style={{ marginTop: 12 }}>{asText(finalDecision.advice)}</div> : null}
                        <div style={{ marginTop: 12 }}><AnalysisActionButtons actionPayload={batchActionPayload(item)} recordId={item.record_id} /></div>
                      </div>
                    );
                  })}
                </div>
                {(visibleBatchResult.results ?? []).some((item) => !item.success) ? <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>股票</th><th>失败原因</th></tr></thead><tbody>{(visibleBatchResult.results ?? []).filter((item) => !item.success).map((item, index) => <tr key={`${normalizeSymbol(item.symbol) || "failed"}-${index}`}><td>{asText(item.symbol)}</td><td>{asText(item.error)}</td></tr>)}</tbody></table></div> : null}
              </section>
            ) : null}
          </>
        ) : null}

        {panel === "history" ? (
          <>
            {selectedReport ? (
              <section className={styles.card}>
                <div className={styles.actions}>
                  <h2>历史报告详情 #{selectedReport.id}</h2>
                  <button className={styles.secondaryButton} onClick={() => setSelectedReport(null)} type="button">收起详情</button>
                </div>
                <p className={styles.muted}>分析时间: {asText(selectedReport.analysis_date)} | 数据范围: {asText(selectedReport.data_date_range)}</p>
                <p>{asText(selectedReport.summary, "暂无摘要")}</p>
                <div className={styles.compactGrid}>
                  <div className={styles.metric}><span className={styles.muted}>推荐股票</span><strong>{selectedReport.summary_data?.recommended_count ?? 0}</strong></div>
                  <div className={styles.metric}><span className={styles.muted}>龙虎榜记录</span><strong>{selectedReport.summary_data?.total_records ?? 0}</strong></div>
                  <div className={styles.metric}><span className={styles.muted}>涉及股票</span><strong>{selectedReport.summary_data?.total_stocks ?? 0}</strong></div>
                  <div className={styles.metric}><span className={styles.muted}>涉及游资</span><strong>{selectedReport.summary_data?.total_youzi ?? 0}</strong></div>
                </div>
                {selectedReport.summary_data?.top_concepts?.length ? <p className={styles.muted}>热门概念: {selectedReport.summary_data.top_concepts.join("、")}</p> : null}
                <div className={styles.actions}>
                  <button className={styles.secondaryButton} onClick={() => void exportResult(selectedReport.result_payload ?? null, "pdf")} type="button">导出 PDF</button>
                  <button className={styles.secondaryButton} onClick={() => void exportResult(selectedReport.result_payload ?? null, "markdown")} type="button">导出 Markdown</button>
                  <button className={styles.primaryButton} onClick={() => void openHistory(selectedReport.id, true)} type="button">加载到分析页</button>
                  <button className={styles.dangerButton} onClick={() => void deleteHistory(selectedReport.id)} type="button">删除报告</button>
                </div>
              </section>
            ) : null}

            <section className={styles.card}>
              <h2>历史分析报告</h2>
              <div className={styles.list}>
                {history.map((item) => (
                  <div className={styles.listItem} key={item.id}>
                    <strong>报告 #{item.id} | {asText(item.analysis_date, "未知时间")}</strong>
                    <p className={styles.muted}>数据范围: {asText(item.data_date_range, "未知")}</p>
                    <p>{asText(item.summary, "暂无摘要")}</p>
                    <div className={styles.actions}>
                      <button className={styles.secondaryButton} onClick={() => void openHistory(item.id)} type="button">查看详情</button>
                      <button className={styles.primaryButton} onClick={() => void openHistory(item.id, true)} type="button">加载到分析页</button>
                      <button className={styles.dangerButton} onClick={() => void deleteHistory(item.id)} type="button">删除</button>
                    </div>
                  </div>
                ))}
                {!history.length ? <div className={styles.muted}>暂无龙虎榜历史报告。</div> : null}
              </div>
            </section>
          </>
        ) : null}

        {panel === "statistics" ? (
          <section className={styles.card}>
            <h2>数据统计</h2>
            <div className={styles.compactGrid}>
              <div className={styles.metric}><span className={styles.muted}>总记录数</span><strong>{statistics?.stats?.total_records ?? 0}</strong></div>
              <div className={styles.metric}><span className={styles.muted}>股票总数</span><strong>{statistics?.stats?.total_stocks ?? 0}</strong></div>
              <div className={styles.metric}><span className={styles.muted}>游资总数</span><strong>{statistics?.stats?.total_youzi ?? 0}</strong></div>
              <div className={styles.metric}><span className={styles.muted}>分析报告</span><strong>{statistics?.stats?.total_reports ?? 0}</strong></div>
            </div>
            <p className={styles.muted}>数据日期范围: {asText(statistics?.stats?.date_range?.start, "N/A")} 至 {asText(statistics?.stats?.date_range?.end, "N/A")}</p>
            <div className={styles.grid}>
              <div className={`${styles.card} ${styles.span6}`}>
                <h3>近 {statistics?.window_days ?? 30} 天活跃游资排名</h3>
                <div className={styles.tableWrap}>
                  <table className={styles.table}><thead><tr><th>游资名称</th><th>交易次数</th><th>总净流入(元)</th></tr></thead><tbody>{(statistics?.top_youzi ?? []).map((item, index) => <tr key={`${asText(item.youzi_name)}-${index}`}><td>{asText(item.youzi_name)}</td><td>{integerText(item.trade_count)}</td><td>{currencyText(item.total_net_inflow)}</td></tr>)}</tbody></table>
                </div>
              </div>
              <div className={`${styles.card} ${styles.span6}`}>
                <h3>近 {statistics?.window_days ?? 30} 天热门股票排名</h3>
                <div className={styles.tableWrap}>
                  <table className={styles.table}><thead><tr><th>股票代码</th><th>股票名称</th><th>游资数量</th><th>总净流入(元)</th></tr></thead><tbody>{(statistics?.top_stocks ?? []).map((item, index) => <tr key={`${normalizeSymbol(item.stock_code) || "stats-stock"}-${index}`}><td>{asText(item.stock_code)}</td><td>{asText(item.stock_name)}</td><td>{integerText(item.youzi_count)}</td><td>{currencyText(item.total_net_inflow)}</td></tr>)}</tbody></table>
                </div>
              </div>
            </div>
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
