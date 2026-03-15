import { FormEvent, useEffect, useMemo, useState } from "react";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch } from "../../lib/api";
import styles from "../ConsolePage.module.scss";


interface TaskPayload {
  stocks?: Array<Record<string, unknown>>;
  message?: string;
  filter_summary?: string;
  selected_time?: string;
}

interface TaskDetail {
  id: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
  error?: string;
  result?: TaskPayload | null;
}

interface MonitorStatus {
  running: boolean;
  scan_interval: number;
  holding_days_limit: number;
  monitored_count: number;
  pending_alerts: number;
}

interface MonitoredStock {
  id?: number;
  stock_code: string;
  stock_name: string;
  buy_price: number;
  buy_date: string;
  holding_days: number;
  add_time?: string;
}

interface AlertItem {
  id: number;
  stock_code: string;
  stock_name: string;
  alert_type: string;
  alert_reason: string;
  current_price?: number;
  ma5?: number;
  ma20?: number;
  holding_days?: number;
  alert_time?: string;
  is_sent?: number;
}

interface SimulationResult {
  buy_results: Array<{
    success: boolean;
    message: string;
    trade?: Record<string, unknown> | null;
  }>;
  positions: Array<Record<string, unknown>>;
  summary: {
    initial_capital?: number;
    available_cash?: number;
    position_value?: number;
    total_value?: number;
    total_profit?: number;
    total_profit_pct?: number;
    positions_count?: number;
    trade_count?: number;
  };
  trade_history: Array<Record<string, unknown>>;
}

type SectionKey = "results" | "data" | "simulation" | "monitor";

const sectionTabs = [
  { key: "results", label: "选股结果" },
  { key: "data", label: "完整数据" },
  { key: "simulation", label: "模拟执行" },
  { key: "monitor", label: "监控中心" },
];

const defaultForm = {
  top_n: "5",
  max_price: "10",
  min_profit_growth: "100",
  min_turnover_yi: "0",
  max_turnover_yi: "0",
  min_market_cap_yi: "0",
  max_market_cap_yi: "0",
  sort_by: "成交额升序",
  exclude_st: true,
  exclude_kcb: true,
  exclude_cyb: true,
  only_hs_a: true,
};

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
  const number = asNumber(value);
  return number === null ? "N/A" : number.toFixed(digits);
}

function findKey(record: Record<string, unknown> | undefined, patterns: string[]): string | null {
  if (!record) {
    return null;
  }
  for (const pattern of patterns) {
    const match = Object.keys(record).find((key) => key.includes(pattern));
    if (match) {
      return match;
    }
  }
  return null;
}

function buildFilterSummary(form: typeof defaultForm): string {
  const parts = [
    `股价≤${Number(form.max_price).toFixed(2)}元`,
    `净利增速≥${Number(form.min_profit_growth).toFixed(0)}%`,
    `排序: ${form.sort_by}`,
  ];
  if (Number(form.min_turnover_yi) > 0) {
    parts.push(`成交额≥${Number(form.min_turnover_yi).toFixed(2)}亿`);
  }
  if (Number(form.max_turnover_yi) > 0) {
    parts.push(`成交额≤${Number(form.max_turnover_yi).toFixed(2)}亿`);
  }
  if (Number(form.min_market_cap_yi) > 0) {
    parts.push(`总市值≥${Number(form.min_market_cap_yi).toFixed(2)}亿`);
  }
  if (Number(form.max_market_cap_yi) > 0) {
    parts.push(`总市值≤${Number(form.max_market_cap_yi).toFixed(2)}亿`);
  }
  if (form.exclude_st) {
    parts.push("剔除ST");
  }
  if (form.exclude_kcb) {
    parts.push("剔除科创板");
  }
  if (form.exclude_cyb) {
    parts.push("剔除创业板");
  }
  if (form.only_hs_a) {
    parts.push("仅沪深A股");
  }
  return parts.join("，");
}

function downloadCsv(rows: Array<Record<string, unknown>>, filename: string) {
  if (!rows.length) {
    return;
  }
  const headers = Array.from(new Set(rows.flatMap((item) => Object.keys(item))));
  const lines = [
    headers.join(","),
    ...rows.map((row) =>
      headers
        .map((header) => `"${String(row[header] ?? "").replace(/"/g, '""')}"`)
        .join(","),
    ),
  ];
  const blob = new Blob(["\ufeff", lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

export function LowPriceBullPage() {
  const [form, setForm] = useState(defaultForm);
  const [scanInterval, setScanInterval] = useState("60");
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [monitorStatus, setMonitorStatus] = useState<MonitorStatus | null>(null);
  const [monitoredStocks, setMonitoredStocks] = useState<MonitoredStock[]>([]);
  const [pendingAlerts, setPendingAlerts] = useState<AlertItem[]>([]);
  const [alertHistory, setAlertHistory] = useState<AlertItem[]>([]);
  const [simulation, setSimulation] = useState<SimulationResult | null>(null);
  const [section, setSection] = useState<SectionKey>("results");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail | null>("/api/selectors/low-price-bull/tasks/latest");
    setTask(data);
  };

  const loadMonitorData = async () => {
    const [status, stocks, alerts, history] = await Promise.all([
      apiFetch<MonitorStatus>("/api/selectors/low-price-bull/monitor/status"),
      apiFetch<MonitoredStock[]>("/api/selectors/low-price-bull/monitor/stocks"),
      apiFetch<AlertItem[]>("/api/selectors/low-price-bull/monitor/alerts"),
      apiFetch<AlertItem[]>("/api/selectors/low-price-bull/monitor/alerts/history?limit=50"),
    ]);
    setMonitorStatus(status);
    setMonitoredStocks(stocks);
    setPendingAlerts(alerts);
    setAlertHistory(history);
    setScanInterval(String(status.scan_interval || 60));
  };

  useEffect(() => {
    void Promise.all([loadTask(), loadMonitorData()]);
    const taskTimer = window.setInterval(() => void loadTask(), 2000);
    const monitorTimer = window.setInterval(() => void loadMonitorData(), 10000);
    return () => {
      window.clearInterval(taskTimer);
      window.clearInterval(monitorTimer);
    };
  }, []);

  const filterSummary = useMemo(() => buildFilterSummary(form), [form]);
  const stocks = task?.status === "success" ? task.result?.stocks ?? [] : [];
  const firstRow = stocks[0];
  const displayKeys = useMemo(() => {
    const keys = [
      findKey(firstRow, ["股票代码"]),
      findKey(firstRow, ["股票简称", "股票名称"]),
      findKey(firstRow, ["股价", "最新价"]),
      findKey(firstRow, ["净利润增长率", "净利润同比增长率"]),
      findKey(firstRow, ["成交额"]),
      findKey(firstRow, ["总市值"]),
      findKey(firstRow, ["市盈率"]),
      findKey(firstRow, ["市净率"]),
      findKey(firstRow, ["所属行业", "所属同花顺行业"]),
    ];
    return keys.filter((item, index, array): item is string => Boolean(item) && array.indexOf(item) === index);
  }, [firstRow]);

  const submitSelection = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/selectors/low-price-bull/tasks", {
        method: "POST",
        body: JSON.stringify({
          top_n: Number(form.top_n) || 5,
          max_price: Number(form.max_price) || 10,
          min_profit_growth: Number(form.min_profit_growth) || 100,
          min_turnover_yi: Number(form.min_turnover_yi) || 0,
          max_turnover_yi: Number(form.max_turnover_yi) || 0,
          min_market_cap_yi: Number(form.min_market_cap_yi) || 0,
          max_market_cap_yi: Number(form.max_market_cap_yi) || 0,
          sort_by: form.sort_by,
          exclude_st: form.exclude_st,
          exclude_kcb: form.exclude_kcb,
          exclude_cyb: form.exclude_cyb,
          only_hs_a: form.only_hs_a,
          filter_summary: filterSummary,
        }),
      });
      setSimulation(null);
      setSection("results");
      setMessage(`低价擒牛选股任务已提交: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交低价擒牛任务失败");
    }
  };

  const runSimulation = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<SimulationResult>("/api/selectors/low-price-bull/simulation", {
        method: "POST",
        body: JSON.stringify({ stocks }),
      });
      setSimulation(data);
      setSection("simulation");
      setMessage("策略模拟已完成");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "策略模拟失败");
    }
  };

  const addToMonitor = async (stock: Record<string, unknown>) => {
    const code = String(stock["股票代码"] ?? "").split(".")[0];
    const name = asText(stock["股票简称"] ?? stock["股票名称"], code);
    const price = asNumber(stock["股价"] ?? stock["最新价"]) ?? 0;
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/selectors/low-price-bull/monitor/stocks", {
        method: "POST",
        body: JSON.stringify({
          stock_code: code,
          stock_name: name,
          buy_price: price,
        }),
      });
      setSection("monitor");
      setMessage(`已加入策略监控: ${code}`);
      await loadMonitorData();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加入监控失败");
    }
  };

  const removeFromMonitor = async (stockCode: string) => {
    setMessage("");
    setError("");
    try {
      await apiFetch(`/api/selectors/low-price-bull/monitor/stocks/${stockCode}`, { method: "DELETE" });
      setSection("monitor");
      setMessage(`已移出监控: ${stockCode}`);
      await loadMonitorData();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "移出监控失败");
    }
  };

  const saveMonitorConfig = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<MonitorStatus>("/api/selectors/low-price-bull/monitor/config", {
        method: "PUT",
        body: JSON.stringify({ scan_interval: Number(scanInterval) || 60 }),
      });
      setMonitorStatus(data);
      setSection("monitor");
      setMessage("监控配置已更新");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新监控配置失败");
    }
  };

  const toggleMonitor = async (running: boolean) => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<MonitorStatus>(
        running ? "/api/selectors/low-price-bull/monitor/start" : "/api/selectors/low-price-bull/monitor/stop",
        { method: "POST" },
      );
      setMonitorStatus(data);
      setSection("monitor");
      setMessage(running ? "监控服务已启动" : "监控服务已停止");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新监控服务状态失败");
    }
  };

  const resolveAlert = async (alertId: number, status: "done" | "ignored") => {
    setMessage("");
    setError("");
    try {
      await apiFetch(`/api/selectors/low-price-bull/monitor/alerts/${alertId}/resolve`, {
        method: "POST",
        body: JSON.stringify({ status }),
      });
      setSection("monitor");
      setMessage(status === "done" ? "已处理提醒并移出监控列表" : "已忽略提醒");
      await loadMonitorData();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "处理提醒失败");
    }
  };

  const cleanupHistory = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/selectors/low-price-bull/monitor/alerts/cleanup?days=30", { method: "POST" });
      setSection("monitor");
      setMessage("已清理 30 天前提醒记录");
      await loadMonitorData();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "清理历史提醒失败");
    }
  };

  const monitoredCodes = useMemo(() => new Set(monitoredStocks.map((item) => item.stock_code)), [monitoredStocks]);

  return (
    <PageFrame
      title="低价擒牛"
      summary="当前覆盖选股、策略模拟和策略监控工作台。"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />
        {section === "results" ? (
          <>
            <section className={styles.card}>
              <form className={styles.stack} onSubmit={submitSelection}>
                <div className={styles.formGrid}>
                  <div className={styles.field}>
                    <label htmlFor="topN">结果数量</label>
                    <input id="topN" value={form.top_n} onChange={(event) => setForm((current) => ({ ...current, top_n: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxPrice">最高股价(元)</label>
                    <input id="maxPrice" value={form.max_price} onChange={(event) => setForm((current) => ({ ...current, max_price: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="growth">最低净利增速(%)</label>
                    <input id="growth" value={form.min_profit_growth} onChange={(event) => setForm((current) => ({ ...current, min_profit_growth: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="sortBy">排序方式</label>
                    <select id="sortBy" value={form.sort_by} onChange={(event) => setForm((current) => ({ ...current, sort_by: event.target.value }))}>
                      <option value="成交额升序">成交额升序</option>
                      <option value="成交额降序">成交额降序</option>
                      <option value="净利润增长率降序">净利润增长率降序</option>
                      <option value="股价升序">股价升序</option>
                      <option value="总市值升序">总市值升序</option>
                    </select>
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="minTurnover">最低成交额(亿)</label>
                    <input id="minTurnover" value={form.min_turnover_yi} onChange={(event) => setForm((current) => ({ ...current, min_turnover_yi: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxTurnover">最高成交额(亿)</label>
                    <input id="maxTurnover" value={form.max_turnover_yi} onChange={(event) => setForm((current) => ({ ...current, max_turnover_yi: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="minCap">最低总市值(亿)</label>
                    <input id="minCap" value={form.min_market_cap_yi} onChange={(event) => setForm((current) => ({ ...current, min_market_cap_yi: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxCap">最高总市值(亿)</label>
                    <input id="maxCap" value={form.max_market_cap_yi} onChange={(event) => setForm((current) => ({ ...current, max_market_cap_yi: event.target.value }))} />
                  </div>
                </div>
                <div className={styles.compactGrid}>
                  <label className={styles.listItem}>
                    <input checked={form.exclude_st} onChange={(event) => setForm((current) => ({ ...current, exclude_st: event.target.checked }))} type="checkbox" /> 剔除 ST
                  </label>
                  <label className={styles.listItem}>
                    <input checked={form.exclude_kcb} onChange={(event) => setForm((current) => ({ ...current, exclude_kcb: event.target.checked }))} type="checkbox" /> 剔除科创板
                  </label>
                  <label className={styles.listItem}>
                    <input checked={form.exclude_cyb} onChange={(event) => setForm((current) => ({ ...current, exclude_cyb: event.target.checked }))} type="checkbox" /> 剔除创业板
                  </label>
                  <label className={styles.listItem}>
                    <input checked={form.only_hs_a} onChange={(event) => setForm((current) => ({ ...current, only_hs_a: event.target.checked }))} type="checkbox" /> 仅沪深 A 股
                  </label>
                </div>
                <p className={styles.muted}>当前筛选: {filterSummary}</p>
                <div className={styles.actions}>
                  <button className={styles.primaryButton} type="submit">
                    开始低价擒牛选股
                  </button>
                  <button
                    className={styles.secondaryButton}
                    onClick={() => downloadCsv(stocks, `low_price_bull_${new Date().toISOString().slice(0, 10)}.csv`)}
                    type="button"
                  >
                    下载 CSV
                  </button>
                  {!!stocks.length ? (
                    <button className={styles.secondaryButton} onClick={() => void runSimulation()} type="button">
                      开始策略模拟
                    </button>
                  ) : null}
                </div>
              </form>
            </section>

            {task ? (
              <section className={styles.card}>
                <h2>选股任务状态</h2>
                <p>{task.message || "等待低价擒牛任务..."}</p>
                <p className={styles.muted}>
                  进度: {task.current ?? 0} / {task.total ?? 0}
                </p>
                {task.error ? <p className={styles.dangerText}>{task.error}</p> : null}
              </section>
            ) : null}

            {stocks.length ? (
              <>
            <section className={styles.card}>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>筛选数量</span>
                  <strong>{stocks.length}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>平均净利增长率</span>
                  <strong>
                    {numberText(
                      stocks.reduce((sum, row) => sum + (asNumber(row["净利润增长率"] ?? row["净利润同比增长率"]) ?? 0), 0) /
                        Math.max(stocks.length, 1),
                    )}
                    %
                  </strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>平均股价</span>
                  <strong>
                    {numberText(
                      stocks.reduce((sum, row) => sum + (asNumber(row["股价"] ?? row["最新价"]) ?? 0), 0) /
                        Math.max(stocks.length, 1),
                    )}
                    元
                  </strong>
                </div>
              </div>
              <p className={styles.muted}>
                选股时间: {asText(task?.result?.selected_time, "-")} | 条件: {asText(task?.result?.filter_summary, "-")}
              </p>
            </section>

            <section className={styles.card}>
              <h2>精选股票列表</h2>
              <div className={styles.list}>
                {stocks.map((stock, index) => {
                  const code = String(stock["股票代码"] ?? "").split(".")[0];
                  const price = asNumber(stock["股价"] ?? stock["最新价"]);
                  const growth = asNumber(stock["净利润增长率"] ?? stock["净利润同比增长率"]);
                  const industryKey = findKey(stock, ["所属行业", "所属同花顺行业"]);
                  return (
                    <div className={styles.listItem} key={`${code}-${index}`}>
                      <strong>
                        第 {index + 1} 名 · {code} - {asText(stock["股票简称"] ?? stock["股票名称"], code)}
                      </strong>
                      <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                        <div className={styles.metric}>
                          <span className={styles.muted}>当前价格</span>
                          <strong>{price === null ? "N/A" : `${price.toFixed(2)}元`}</strong>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>净利增速</span>
                          <strong>{growth === null ? "N/A" : `${growth.toFixed(2)}%`}</strong>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>成交额</span>
                          <strong>{asText(stock["成交额"])}</strong>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>所属行业</span>
                          <strong>{industryKey ? asText(stock[industryKey]) : "N/A"}</strong>
                        </div>
                      </div>
                      <div className={styles.actions} style={{ marginTop: 12 }}>
                        {monitoredCodes.has(code) ? (
                          <button className={styles.dangerButton} onClick={() => void removeFromMonitor(code)} type="button">
                            移出监控
                          </button>
                        ) : (
                          <button className={styles.primaryButton} onClick={() => void addToMonitor(stock)} type="button">
                            加入策略监控
                          </button>
                        )}
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

        {section === "data" && stocks.length ? (
          <section className={styles.card}>
            <div className={styles.actions}>
              <h2>完整数据表格</h2>
              <button className={styles.secondaryButton} onClick={() => void runSimulation()} type="button">
                开始策略模拟
              </button>
            </div>
            <div className={styles.tableWrap}>
              <table className={styles.table}>
                <thead>
                  <tr>
                    {displayKeys.map((key) => (
                      <th key={key}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {stocks.map((row, index) => (
                    <tr key={`${String(row["股票代码"] ?? "row")}-${index}`}>
                      {displayKeys.map((key) => (
                        <td key={key}>{asText(row[key])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}

        {section === "simulation" ? (
          simulation ? (
            <section className={styles.card}>
              <h2>策略模拟执行</h2>
              <div className={styles.list}>
                {simulation.buy_results.map((item, index) => (
                  <div className={styles.listItem} key={`buy-${index}`}>
                    {item.success ? item.message : `失败: ${item.message}`}
                  </div>
                ))}
              </div>
              <div className={styles.compactGrid} style={{ marginTop: 16 }}>
                <div className={styles.metric}>
                  <span className={styles.muted}>初始资金</span>
                  <strong>{numberText(simulation.summary.initial_capital)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>可用资金</span>
                  <strong>{numberText(simulation.summary.available_cash)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>持仓市值</span>
                  <strong>{numberText(simulation.summary.position_value)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>总资产</span>
                  <strong>{numberText(simulation.summary.total_value)}</strong>
                </div>
              </div>
            </section>
          ) : (
            <section className={styles.card}>
              <h2>策略模拟执行</h2>
              <p className={styles.muted}>
                {stocks.length ? "可直接启动策略模拟，查看买入结果与资金变化。" : "请先完成一次选股，再进入策略模拟。"}
              </p>
              {stocks.length ? (
                <div className={styles.actions}>
                  <button className={styles.primaryButton} onClick={() => void runSimulation()} type="button">
                    开始策略模拟
                  </button>
                </div>
              ) : null}
            </section>
          )
        ) : null}

        {section === "monitor" ? (
          <>
            <section className={styles.card}>
              <h2>策略监控中心</h2>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>服务状态</span>
                  <strong>{monitorStatus?.running ? "运行中" : "已停止"}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>监控股票</span>
                  <strong>{monitorStatus?.monitored_count ?? 0} 只</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>待处理提醒</span>
                  <strong>{monitorStatus?.pending_alerts ?? 0} 条</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>扫描间隔</span>
                  <strong>{monitorStatus?.scan_interval ?? 60} 秒</strong>
                </div>
              </div>
              <div className={styles.actions} style={{ marginTop: 16 }}>
                <button className={styles.primaryButton} disabled={Boolean(monitorStatus?.running)} onClick={() => void toggleMonitor(true)} type="button">
                  启动监控服务
                </button>
                <button className={styles.secondaryButton} disabled={!monitorStatus?.running} onClick={() => void toggleMonitor(false)} type="button">
                  停止监控服务
                </button>
                <input
                  className={styles.shortInput}
                  value={scanInterval}
                  onChange={(event) => setScanInterval(event.target.value)}
                />
                <button className={styles.secondaryButton} onClick={() => void saveMonitorConfig()} type="button">
                  保存扫描间隔
                </button>
              </div>
            </section>

            <section className={styles.card}>
              <h2>监控列表</h2>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>买入价格 / 日期</th>
                      <th>持有天数</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {monitoredStocks.map((item) => (
                      <tr key={item.stock_code}>
                        <td>
                          <strong>{item.stock_name}</strong>
                          <div className={styles.muted}>{item.stock_code}</div>
                        </td>
                        <td>
                          {numberText(item.buy_price)} / {asText(item.buy_date)}
                        </td>
                        <td>{item.holding_days}</td>
                        <td>
                          <button className={styles.dangerButton} onClick={() => void removeFromMonitor(item.stock_code)} type="button">
                            移除
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!monitoredStocks.length ? <div className={styles.muted}>暂无监控中的股票。</div> : null}
            </section>

            <section className={styles.card}>
              <h2>卖出提醒</h2>
              <div className={styles.list}>
                {pendingAlerts.map((alert) => (
                  <div className={styles.listItem} key={alert.id}>
                    <strong>
                      {alert.stock_code} {alert.stock_name} - {alert.alert_reason}
                    </strong>
                    <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                      <div>
                        <div className={styles.muted}>提醒类型</div>
                        <div>{alert.alert_type}</div>
                      </div>
                      <div>
                        <div className={styles.muted}>当前价格</div>
                        <div>{numberText(alert.current_price)}</div>
                      </div>
                      <div>
                        <div className={styles.muted}>MA5 / MA20</div>
                        <div>
                          {numberText(alert.ma5)} / {numberText(alert.ma20)}
                        </div>
                      </div>
                      <div>
                        <div className={styles.muted}>持有天数</div>
                        <div>{asText(alert.holding_days)}</div>
                      </div>
                    </div>
                    <div className={styles.actions} style={{ marginTop: 12 }}>
                      <button className={styles.primaryButton} onClick={() => void resolveAlert(alert.id, "done")} type="button">
                        已处理
                      </button>
                      <button className={styles.secondaryButton} onClick={() => void resolveAlert(alert.id, "ignored")} type="button">
                        忽略
                      </button>
                    </div>
                  </div>
                ))}
                {!pendingAlerts.length ? <div className={styles.muted}>暂无待处理的卖出提醒。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.actions}>
                <h2>历史提醒记录</h2>
                <button className={styles.secondaryButton} onClick={() => void cleanupHistory()} type="button">
                  清理 30 天前记录
                </button>
              </div>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>提醒类型</th>
                      <th>提醒原因</th>
                      <th>提醒时间</th>
                      <th>已发送</th>
                    </tr>
                  </thead>
                  <tbody>
                    {alertHistory.map((item) => (
                      <tr key={item.id}>
                        <td>
                          {item.stock_code} {item.stock_name}
                        </td>
                        <td>{item.alert_type}</td>
                        <td>{item.alert_reason}</td>
                        <td>{asText(item.alert_time)}</td>
                        <td>{item.is_sent ? "是" : "否"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!alertHistory.length ? <div className={styles.muted}>暂无历史提醒记录。</div> : null}
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
