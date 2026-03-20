import { FormEvent, useEffect, useMemo, useState } from "react";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { AnalysisActionButtons, type ActionPayload } from "../../components/research/AnalysisActionButtons";
import { ApiRequestError, apiFetch, apiFetchCached, downloadApiFile } from "../../lib/api";
import styles from "../ConsolePage.module.scss";


interface MainForceRecommendation {
  rank?: number;
  symbol?: string;
  name?: string;
  reasons?: string[];
  highlights?: string;
  position?: string;
  investment_period?: string;
  risks?: string;
  stock_data?: Record<string, unknown>;
}

interface MainForceSelectionResult {
  success: boolean;
  total_stocks?: number;
  filtered_stocks?: number;
  final_recommendations?: MainForceRecommendation[];
  error?: string;
}

interface MainForceSelectionContext {
  raw_stocks?: Array<Record<string, unknown>>;
  fund_flow_analysis?: string;
  industry_analysis?: string;
  fundamental_analysis?: string;
}

interface MainForceSelectionTaskPayload {
  result?: MainForceSelectionResult;
  context_snapshot?: MainForceSelectionContext;
  message?: string;
}

interface MainForceBatchItem {
  symbol?: string;
  success?: boolean;
  error?: string;
  record_id?: number;
  stock_info?: Record<string, unknown>;
  final_decision?: Record<string, unknown>;
}

interface MainForceBatchResults {
  results?: MainForceBatchItem[];
  total?: number;
  success?: number;
  failed?: number;
  elapsed_time?: number;
  analysis_mode?: string;
  analysis_date?: string;
  history_record_id?: number;
}

interface MainForceHistoryRecord {
  id: number;
  analysis_date?: string;
  summary?: string;
  batch_results?: MainForceBatchResults;
}

interface MainForceHistoryStats {
  total_records?: number;
  total_stocks_analyzed?: number;
  total_success?: number;
  total_failed?: number;
  average_time?: number;
  success_rate?: number;
}

interface MainForceHistoryResponse {
  stats: MainForceHistoryStats;
  records: MainForceHistoryRecord[];
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

type SectionKey = "selection" | "candidates" | "batch" | "history";

const sectionTabs = [
  { key: "selection", label: "主筛选" },
  { key: "candidates", label: "候选明细" },
  { key: "batch", label: "批量深度分析" },
  { key: "history", label: "批量历史" },
];

const dateOptions = [
  { label: "最近 3 个月", value: "90" },
  { label: "最近 6 个月", value: "180" },
  { label: "最近 1 年", value: "365" },
  { label: "自定义日期", value: "custom" },
];

const mainFundPatterns = ["区间主力资金流向", "区间主力资金净流入", "主力资金流向", "主力资金净流入", "主力净流入", "主力资金"];
const rangePatterns = ["区间涨跌幅:前复权", "区间涨跌幅", "涨跌幅:前复权", "涨跌幅"];
const industryPatterns = ["所属同花顺行业", "所属行业", "行业"];

function asText(value: unknown, fallback = "N/A"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function asNumber(value: unknown): number | null {
  const normalized = String(value ?? "").replace(/,/g, "").trim();
  const result = Number(normalized);
  return Number.isFinite(result) ? result : null;
}

function numberText(value: unknown, digits = 2): string {
  const result = asNumber(value);
  return result === null ? "N/A" : result.toFixed(digits);
}

function findKey(record: Record<string, unknown> | undefined, patterns: string[]): string | null {
  if (!record) {
    return null;
  }
  const keys = Object.keys(record);
  for (const pattern of patterns) {
    const matched = keys.find((key) => key.includes(pattern));
    if (matched) {
      return matched;
    }
  }
  return null;
}

function normalizeSymbol(value: unknown): string {
  const raw = asText(value, "").trim().toUpperCase();
  if (!raw) {
    return "";
  }
  return raw.includes(".") ? raw.split(".")[0] : raw;
}

function parseEntryMin(entryRange: unknown): number | undefined {
  const text = asText(entryRange, "");
  if (!text.includes("-")) {
    return undefined;
  }
  const head = text.split("-")[0];
  const value = asNumber(head);
  return value ?? undefined;
}

function batchActionPayload(item: MainForceBatchItem): ActionPayload | null {
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
    default_note: asText(finalDecision.operation_advice ?? finalDecision.advice, ""),
    strategy_context: {
      source: "main_force_batch",
    },
  };
}

function downloadCsv(rows: Array<Record<string, unknown>>, filename: string) {
  if (!rows.length) {
    return;
  }
  const headers = Array.from(new Set(rows.flatMap((item) => Object.keys(item))));
  const escapeCell = (value: unknown) => {
    const text = String(value ?? "");
    return `"${text.replace(/"/g, '""')}"`;
  };
  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => escapeCell(row[header])).join(",")),
  ];
  const blob = new Blob(["\ufeff", lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

function renderTextBlock(title: string, value: string | undefined) {
  if (!value) {
    return null;
  }
  return (
    <div className={styles.listItem}>
      <strong>{title}</strong>
      <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>{value}</div>
    </div>
  );
}

export function MainForcePage() {
  const [dateMode, setDateMode] = useState("90");
  const [customDate, setCustomDate] = useState("");
  const [finalN, setFinalN] = useState("5");
  const [maxChange, setMaxChange] = useState("30");
  const [minCap, setMinCap] = useState("50");
  const [maxCap, setMaxCap] = useState("5000");
  const [batchCount, setBatchCount] = useState("20");
  const [batchMode, setBatchMode] = useState<"sequential" | "parallel">("sequential");
  const [maxWorkers, setMaxWorkers] = useState("3");
  const [selectionTask, setSelectionTask] = useState<TaskDetail<MainForceSelectionTaskPayload> | null>(null);
  const [batchTask, setBatchTask] = useState<TaskDetail<MainForceBatchResults> | null>(null);
  const [history, setHistory] = useState<MainForceHistoryResponse | null>(null);
  const [loadedHistoryRecord, setLoadedHistoryRecord] = useState<MainForceHistoryRecord | null>(null);
  const [section, setSection] = useState<SectionKey>("selection");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadSelectionTask = async () => {
    const data = await apiFetch<TaskDetail<MainForceSelectionTaskPayload> | null>("/api/selectors/main-force/tasks/latest");
    setSelectionTask(data);
  };

  const loadBatchTask = async () => {
    const data = await apiFetch<TaskDetail<MainForceBatchResults> | null>("/api/selectors/main-force/batch-tasks/latest");
    setBatchTask(data);
    if (data?.status === "success" && data.result) {
      setLoadedHistoryRecord(null);
    }
  };

  const loadHistory = async () => {
    const data = await apiFetchCached<MainForceHistoryResponse>("/api/selectors/main-force/history");
    setHistory(data);
  };

  useEffect(() => {
    void Promise.all([loadSelectionTask(), loadBatchTask(), loadHistory()]);
    const timer = window.setInterval(() => {
      void loadSelectionTask();
      void loadBatchTask();
    }, 2000);
    return () => window.clearInterval(timer);
  }, []);

  const selectionResult = selectionTask?.status === "success" ? selectionTask.result?.result ?? null : null;
  const selectionContext = selectionTask?.status === "success" ? selectionTask.result?.context_snapshot ?? null : null;
  const rawStocks = selectionContext?.raw_stocks ?? [];

  const candidateMeta = useMemo(() => {
    const firstRow = rawStocks[0];
    const codeKey = findKey(firstRow, ["股票代码"]) ?? "股票代码";
    const nameKey = findKey(firstRow, ["股票简称", "股票名称"]) ?? "股票简称";
    const industryKey = findKey(firstRow, industryPatterns);
    const mainFundKey = findKey(firstRow, mainFundPatterns);
    const rangeKey = findKey(firstRow, rangePatterns);
    const extraKeys = ["总市值", "市盈率", "市净率"]
      .map((pattern) => findKey(firstRow, [pattern]))
      .filter((item): item is string => Boolean(item));
    return {
      codeKey,
      nameKey,
      industryKey,
      mainFundKey,
      rangeKey,
      displayKeys: [codeKey, nameKey, industryKey, mainFundKey, rangeKey, ...extraKeys].filter(
        (item, index, array): item is string => Boolean(item) && array.indexOf(item) === index,
      ),
    };
  }, [rawStocks]);

  const topCandidateSymbols = useMemo(() => {
    if (!rawStocks.length) {
      return [];
    }
    const mainFundKey = candidateMeta.mainFundKey;
    const rows = [...rawStocks];
    if (mainFundKey) {
      rows.sort((left, right) => (asNumber(right[mainFundKey]) ?? 0) - (asNumber(left[mainFundKey]) ?? 0));
    }
    return rows
      .slice(0, Number(batchCount) || 20)
      .map((row) => normalizeSymbol(row[candidateMeta.codeKey]))
      .filter(Boolean);
  }, [batchCount, candidateMeta.codeKey, candidateMeta.mainFundKey, rawStocks]);

  const activeBatchResults = loadedHistoryRecord?.batch_results ?? (batchTask?.status === "success" ? batchTask.result ?? null : null);

  const submitSelection = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
    try {
      const payload = {
        days_ago: dateMode === "custom" ? null : Number(dateMode),
        start_date: dateMode === "custom" ? customDate || null : null,
        final_n: Number(finalN) || 5,
        max_change: Number(maxChange) || 30,
        min_cap: Number(minCap) || 50,
        max_cap: Number(maxCap) || 5000,
      };
      const data = await apiFetch<{ task_id: string }>("/api/selectors/main-force/tasks", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setSection("selection");
      setMessage(`主力选股任务已提交: ${data.task_id}`);
      await loadSelectionTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交主力选股任务失败");
    }
  };

  const submitBatch = async () => {
    setMessage("");
    setError("");
    if (!topCandidateSymbols.length) {
      setError("当前没有可用于批量分析的候选股票");
      return;
    }
    try {
      const data = await apiFetch<{ task_id: string }>("/api/selectors/main-force/batch-tasks", {
        method: "POST",
        body: JSON.stringify({
          symbols: topCandidateSymbols,
          analysis_mode: batchMode,
          max_workers: Number(maxWorkers) || 1,
        }),
      });
      setLoadedHistoryRecord(null);
      setSection("batch");
      setMessage(`主力 TOP 批量分析任务已提交: ${data.task_id}`);
      void loadBatchTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交批量分析任务失败");
    }
  };

  const loadHistoryRecord = async (recordId: number) => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetchCached<MainForceHistoryRecord>(`/api/selectors/main-force/history/${recordId}`);
      setLoadedHistoryRecord(data);
      setSection("batch");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加载历史记录失败");
    }
  };

  const deleteHistoryRecord = async (recordId: number) => {
    setMessage("");
    setError("");
    try {
      await apiFetch(`/api/selectors/main-force/history/${recordId}`, { method: "DELETE" });
      if (loadedHistoryRecord?.id === recordId) {
        setLoadedHistoryRecord(null);
      }
      setMessage(`历史记录 #${recordId} 已删除`);
      void loadHistory();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "删除历史记录失败");
    }
  };

  const exportSelection = async (kind: "pdf" | "markdown") => {
    if (!selectionResult) {
      return;
    }
    setMessage("");
    setError("");
    try {
      await downloadApiFile(`/api/exports/main-force/${kind}`, {
        method: "POST",
        body: JSON.stringify({
          result: selectionResult,
          context_snapshot: selectionContext ?? {},
        }),
      });
      setMessage(kind === "pdf" ? "主力选股 PDF 已开始下载" : "主力选股 Markdown 已开始下载");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "导出主力选股报告失败");
    }
  };

  return (
    <PageFrame
      title="主力选股"
      summary="当前支持主筛选、TOP 批量深度分析和批量历史管理。"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />
        {section === "selection" ? (
          <>
            <section className={styles.card}>
              <form className={styles.stack} onSubmit={submitSelection}>
                <div className={styles.formGrid}>
                  <div className={styles.field}>
                    <label htmlFor="dateMode">选择时间区间</label>
                    <select id="dateMode" value={dateMode} onChange={(event) => setDateMode(event.target.value)}>
                      {dateOptions.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="finalN">最终精选数量</label>
                    <input id="finalN" value={finalN} onChange={(event) => setFinalN(event.target.value)} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxChange">最大涨跌幅(%)</label>
                    <input id="maxChange" value={maxChange} onChange={(event) => setMaxChange(event.target.value)} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="minCap">最小市值(亿)</label>
                    <input id="minCap" value={minCap} onChange={(event) => setMinCap(event.target.value)} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxCap">最大市值(亿)</label>
                    <input id="maxCap" value={maxCap} onChange={(event) => setMaxCap(event.target.value)} />
                  </div>
                  {dateMode === "custom" ? (
                    <div className={styles.field}>
                      <label htmlFor="customDate">开始日期</label>
                      <input id="customDate" type="date" value={customDate} onChange={(event) => setCustomDate(event.target.value)} />
                    </div>
                  ) : null}
                </div>
                <div className={styles.actions}>
                  <button className={styles.primaryButton} type="submit">
                    开始主力选股
                  </button>
                  <button className={styles.secondaryButton} onClick={() => void loadHistory()} type="button">
                    刷新历史
                  </button>
                </div>
              </form>
            </section>

            {selectionTask ? (
              <section className={styles.card}>
                <h2>筛选任务状态</h2>
                <p>{selectionTask.message || "等待主力选股任务..."}</p>
                <p className={styles.muted}>
                  进度: {selectionTask.current ?? 0} / {selectionTask.total ?? 0}
                </p>
                {selectionTask.error ? <p className={styles.dangerText}>{selectionTask.error}</p> : null}
              </section>
            ) : null}

            {selectionResult?.success ? (
              <>
                <section className={styles.card}>
                  <div className={styles.compactGrid}>
                    <div className={styles.metric}>
                      <span className={styles.muted}>获取股票数</span>
                      <strong>{selectionResult.total_stocks ?? 0}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>筛选后</span>
                      <strong>{selectionResult.filtered_stocks ?? 0}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>最终推荐</span>
                      <strong>{selectionResult.final_recommendations?.length ?? 0}</strong>
                    </div>
                  </div>
                </section>

                <section className={styles.card}>
                  <h2>精选推荐</h2>
                  <div className={styles.list}>
                    {(selectionResult.final_recommendations ?? []).map((item, index) => {
                      const stockData = item.stock_data ?? {};
                      const industryKey = findKey(stockData, industryPatterns);
                      const mainFundKey = findKey(stockData, mainFundPatterns);
                      const rangeKey = findKey(stockData, rangePatterns);
                      return (
                        <div className={styles.listItem} key={`${item.symbol ?? "unknown"}-${index}`}>
                          <strong>
                            第 {item.rank ?? index + 1} 名 · {asText(item.symbol)} - {asText(item.name)}
                          </strong>
                          <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                            <div>
                              <div className={styles.muted}>推荐理由</div>
                              <div>{(item.reasons ?? []).length ? item.reasons?.join("；") : "暂无"}</div>
                            </div>
                            <div>
                              <div className={styles.muted}>建议仓位 / 周期</div>
                              <div>
                                {asText(item.position)} / {asText(item.investment_period)}
                              </div>
                            </div>
                            <div>
                              <div className={styles.muted}>亮点</div>
                              <div>{asText(item.highlights)}</div>
                            </div>
                            <div>
                              <div className={styles.muted}>风险提示</div>
                              <div>{asText(item.risks)}</div>
                            </div>
                          </div>
                          <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                            <div className={styles.metric}>
                              <span className={styles.muted}>所属行业</span>
                              <strong>{industryKey ? asText(stockData[industryKey]) : "N/A"}</strong>
                            </div>
                            <div className={styles.metric}>
                              <span className={styles.muted}>主力资金净流入</span>
                              <strong>{mainFundKey ? asText(stockData[mainFundKey]) : "N/A"}</strong>
                            </div>
                            <div className={styles.metric}>
                              <span className={styles.muted}>区间涨跌幅</span>
                              <strong>{rangeKey ? asText(stockData[rangeKey]) : "N/A"}</strong>
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </section>
              </>
            ) : null}
          </>
        ) : null}

        {section === "candidates" ? (
          <section className={styles.card}>
            <div className={styles.actions}>
              <h2 className={styles.mobileDuplicateHeading}>候选股票列表</h2>
              <button className={styles.secondaryButton} onClick={() => void exportSelection("pdf")} type="button">
                导出 PDF
              </button>
              <button className={styles.secondaryButton} onClick={() => void exportSelection("markdown")} type="button">
                导出 Markdown
              </button>
              <button
                className={styles.secondaryButton}
                onClick={() => downloadCsv(rawStocks, `main_force_candidates_${new Date().toISOString().slice(0, 10)}.csv`)}
                type="button"
              >
                下载候选列表 CSV
              </button>
            </div>
            <div className={styles.tableWrap}>
              <table className={styles.table}>
                <thead>
                  <tr>
                    {candidateMeta.displayKeys.map((key) => (
                      <th key={key}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rawStocks.map((row, index) => (
                    <tr key={`${normalizeSymbol(row[candidateMeta.codeKey]) || "row"}-${index}`}>
                      {candidateMeta.displayKeys.map((key) => (
                        <td key={key}>{asText(row[key])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className={styles.muted}>共 {rawStocks.length} 只候选股票。</p>
          </section>
        ) : null}

        {section === "batch" ? (
          <>
            <section className={styles.card}>
              <h2 className={styles.mobileDuplicateHeading}>TOP 股票批量深度分析</h2>
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="batchCount">分析数量</label>
                  <select id="batchCount" value={batchCount} onChange={(event) => setBatchCount(event.target.value)}>
                    <option value="10">10</option>
                    <option value="20">20</option>
                    <option value="30">30</option>
                    <option value="50">50</option>
                  </select>
                </div>
                <div className={styles.field}>
                  <label htmlFor="batchMode">分析模式</label>
                  <select
                    id="batchMode"
                    value={batchMode}
                    onChange={(event) => setBatchMode(event.target.value as "sequential" | "parallel")}
                  >
                    <option value="sequential">顺序分析（稳定）</option>
                    <option value="parallel">并行分析（快速）</option>
                  </select>
                </div>
                <div className={styles.field}>
                  <label htmlFor="maxWorkers">并行线程数</label>
                  <input
                    disabled={batchMode !== "parallel"}
                    id="maxWorkers"
                    value={maxWorkers}
                    onChange={(event) => setMaxWorkers(event.target.value)}
                  />
                </div>
              </div>
              <p className={styles.muted}>
                将按主力资金净流入排序后，分析前 {topCandidateSymbols.length} 只股票：{topCandidateSymbols.slice(0, 10).join(", ")}
                {topCandidateSymbols.length > 10 ? "..." : ""}
              </p>
              <div className={styles.actions}>
                <button className={styles.primaryButton} onClick={() => void submitBatch()} type="button">
                  开始批量分析
                </button>
              </div>
            </section>

            {selectionContext ? (
              <section className={styles.card}>
                <h2>AI 团队分析报告</h2>
                <div className={styles.stack}>
                  {renderTextBlock("资金流向分析", selectionContext.fund_flow_analysis)}
                  {renderTextBlock("行业板块分析", selectionContext.industry_analysis)}
                  {renderTextBlock("财务基本面分析", selectionContext.fundamental_analysis)}
                </div>
              </section>
            ) : null}
          </>
        ) : null}

        {section === "batch" && batchTask ? (
          <section className={styles.card}>
            <h2>批量分析任务状态</h2>
            <p>{batchTask.message || "等待批量分析任务..."}</p>
            <p className={styles.muted}>
              进度: {batchTask.current ?? 0} / {batchTask.total ?? 0}
            </p>
            {batchTask.error ? <p className={styles.dangerText}>{batchTask.error}</p> : null}
          </section>
        ) : null}

        {section === "batch" && activeBatchResults ? (
          <section className={styles.card}>
            <div className={styles.actions}>
              <h2>{loadedHistoryRecord ? `批量历史 #${loadedHistoryRecord.id}` : "当前批量分析结果"}</h2>
              {loadedHistoryRecord ? (
                <button className={styles.secondaryButton} onClick={() => setLoadedHistoryRecord(null)} type="button">
                  返回最新结果
                </button>
              ) : null}
            </div>
            <div className={styles.compactGrid}>
              <div className={styles.metric}>
                <span className={styles.muted}>总计分析</span>
                <strong>{activeBatchResults.total ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>成功分析</span>
                <strong>{activeBatchResults.success ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>失败分析</span>
                <strong>{activeBatchResults.failed ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>总耗时(分钟)</span>
                <strong>{numberText((activeBatchResults.elapsed_time ?? 0) / 60)}</strong>
              </div>
            </div>
            <p className={styles.muted}>
              生成时间: {asText(activeBatchResults.analysis_date, "-")} | 分析模式: {asText(activeBatchResults.analysis_mode, "-")}
            </p>

            <div className={styles.list}>
              {(activeBatchResults.results ?? [])
                .filter((item) => item.success)
                .map((item, index) => {
                  const finalDecision = item.final_decision ?? {};
                  const stockInfo = item.stock_info ?? {};
                  return (
                    <div className={styles.listItem} key={`${item.symbol ?? "batch"}-${index}`}>
                      <strong>
                        {asText(item.symbol)} - {asText(stockInfo.name ?? stockInfo["股票名称"], asText(item.symbol))}
                      </strong>
                      <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                        <div>
                          <div className={styles.muted}>评级</div>
                          <div>{asText(finalDecision.rating ?? finalDecision.investment_rating)}</div>
                        </div>
                        <div>
                          <div className={styles.muted}>信心度</div>
                          <div>{asText(finalDecision.confidence_level)}</div>
                        </div>
                        <div>
                          <div className={styles.muted}>进场区间</div>
                          <div>{asText(finalDecision.entry_range)}</div>
                        </div>
                        <div>
                          <div className={styles.muted}>目标价</div>
                          <div>{asText(finalDecision.target_price)}</div>
                        </div>
                        <div>
                          <div className={styles.muted}>止盈 / 止损</div>
                          <div>
                            {asText(finalDecision.take_profit)} / {asText(finalDecision.stop_loss)}
                          </div>
                        </div>
                      </div>
                      <div style={{ marginTop: 12 }}>
                        <AnalysisActionButtons actionPayload={batchActionPayload(item)} recordId={item.record_id} />
                      </div>
                    </div>
                  );
                })}
            </div>

            {(activeBatchResults.results ?? []).some((item) => !item.success) ? (
              <>
                <h3 style={{ marginTop: 18 }}>失败项</h3>
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>股票</th>
                        <th>失败原因</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(activeBatchResults.results ?? [])
                        .filter((item) => !item.success)
                        .map((item, index) => (
                          <tr key={`${item.symbol ?? "failed"}-${index}`}>
                            <td>{asText(item.symbol)}</td>
                            <td>{asText(item.error)}</td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : null}
          </section>
        ) : null}

        {section === "history" ? (
          <section className={styles.card}>
            <h2 className={styles.mobileDuplicateHeading}>批量分析历史</h2>
            <div className={styles.compactGrid}>
              <div className={styles.metric}>
                <span className={styles.muted}>总记录数</span>
                <strong>{history?.stats.total_records ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>分析股票总数</span>
                <strong>{history?.stats.total_stocks_analyzed ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>成功率</span>
                <strong>{numberText(history?.stats.success_rate)}%</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>平均耗时(秒)</span>
                <strong>{numberText(history?.stats.average_time)}</strong>
              </div>
            </div>
            <div className={styles.list} style={{ marginTop: 18 }}>
              {(history?.records ?? []).map((item) => (
                <div className={styles.listItem} key={item.id}>
                  <strong>{asText(item.analysis_date, "未知时间")}</strong>
                  <div style={{ marginTop: 8 }}>{asText(item.summary, "无摘要")}</div>
                  <div className={styles.actions} style={{ marginTop: 12 }}>
                    <button className={styles.secondaryButton} onClick={() => void loadHistoryRecord(item.id)} type="button">
                      加载到当前结果
                    </button>
                    <button className={styles.dangerButton} onClick={() => void deleteHistoryRecord(item.id)} type="button">
                      删除
                    </button>
                  </div>
                </div>
              ))}
              {!(history?.records.length ?? 0) ? <div className={styles.muted}>暂无批量分析历史记录。</div> : null}
            </div>
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
