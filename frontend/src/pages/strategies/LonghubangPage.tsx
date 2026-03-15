import { useEffect, useMemo, useState } from "react";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { AnalysisActionButtons, type ActionPayload } from "../../components/research/AnalysisActionButtons";
import { LonghubangReportDetailView } from "../../components/research/LonghubangReportDetailView";
import { ApiRequestError, apiFetch, apiFetchCached, downloadApiFile } from "../../lib/api";
import styles from "../ConsolePage.module.scss";

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
  const [dismissedTaskId, setDismissedTaskId] = useState("");
  const [dismissedBatchTaskId, setDismissedBatchTaskId] = useState("");
  const [statistics, setStatistics] = useState<StatisticsPayload | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadTask = async () => setTask(await apiFetch<TaskDetail<LonghubangTaskPayload> | null>("/api/strategies/longhubang/tasks/latest"));
  const loadBatchTask = async () =>
    setBatchTask(await apiFetch<TaskDetail<BatchResult> | null>("/api/strategies/longhubang/batch-tasks/latest"));
  const loadHistory = async () => setHistory(await apiFetchCached<HistoryRecord[]>("/api/strategies/longhubang/history"));
  const loadStatistics = async () =>
    setStatistics(await apiFetchCached<StatisticsPayload>("/api/strategies/longhubang/statistics?days=30"));

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
  const currentResult = visibleTaskResult;
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

  const openHistory = async (reportId: number) =>
    withRequest(async () => {
      const data = await apiFetchCached<HistoryRecord>(`/api/strategies/longhubang/history/${reportId}`);
      setSelectedReport(data);
      setPanel("history");
    }, "加载龙虎榜历史报告失败");

  const deleteHistory = async (reportId: number) =>
    withRequest(async () => {
      await apiFetch(`/api/strategies/longhubang/history/${reportId}`, { method: "DELETE" });
      if (selectedReport?.id === reportId) {
        setSelectedReport(null);
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
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

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
                <button className={styles.secondaryButton} onClick={() => { if (task?.status === "success") setDismissedTaskId(task.id); setMessage("当前分析结果已清除。"); }} type="button">清除结果</button>
              </div>
              {task ? <div className={styles.listItem} style={{ marginTop: 16 }}><strong>任务状态</strong><div style={{ marginTop: 8 }}>{task.message || "等待任务执行..."}</div><div className={styles.muted}>进度: {task.current ?? 0} / {task.total ?? 0}</div>{task.error ? <div className={styles.dangerText}>{task.error}</div> : null}</div> : null}
            </section>

            {currentResult ? (
              <>
                <LonghubangReportDetailView
                  onExport={(kind) => void exportResult(currentResult, kind)}
                  result={currentResult}
                />

                {scoringRows.length ? (
                  <section className={styles.card}>
                    <div className={styles.actions}>
                      <h2>TOP 股票批量分析</h2>
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
                  </section>
                ) : null}
              </>
            ) : null}

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
              <>
                <section className={styles.card}>
                  <div className={styles.actions}>
                    <button className={styles.secondaryButton} onClick={() => setSelectedReport(null)} type="button">返回历史列表</button>
                  </div>
                  <p className={styles.helperText}>
                    分析时间 {asText(selectedReport.analysis_date)} | 数据范围 {asText(selectedReport.data_date_range)}
                  </p>
                </section>

                {selectedReport.result_payload ? (
                  <LonghubangReportDetailView
                    onExport={(kind) => void exportResult(selectedReport.result_payload ?? null, kind)}
                    result={selectedReport.result_payload}
                  />
                ) : (
                  <section className={styles.card}>
                    <div className={styles.muted}>该历史报告缺少完整结果数据。</div>
                  </section>
                )}
              </>
            ) : null}

            <section className={styles.card}>
              <h2>历史分析报告</h2>
              <div className={styles.list}>
                {history.map((item) => (
                  <div className={styles.listItem} key={item.id}>
                    <strong>{asText(item.analysis_date, "未知时间")}</strong>
                    <p className={styles.muted}>数据范围: {asText(item.data_date_range, "未知")}</p>
                    <p>{asText(item.summary, "暂无摘要")}</p>
                    <div className={styles.actions}>
                      <button className={styles.secondaryButton} onClick={() => void openHistory(item.id)} type="button">查看详情</button>
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
