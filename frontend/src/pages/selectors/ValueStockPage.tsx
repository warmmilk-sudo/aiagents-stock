import { FormEvent, useEffect, useMemo, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch } from "../../lib/api";
import { asNumber, asText, downloadCsvRows, numberText } from "../../lib/market";
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

interface SimulationResult {
  buy_results: Array<{
    success: boolean;
    message: string;
    trade?: Record<string, unknown> | null;
  }>;
  sell_checks: Array<{
    code?: string;
    name?: string;
    should_sell?: boolean;
    reason?: string;
    rsi?: number | null;
  }>;
  positions: Array<Record<string, unknown>>;
  summary: {
    initial_capital?: number;
    available_cash?: number;
    position_value?: number;
    total_assets?: number;
    total_profit?: number;
    total_return?: number;
    holding_count?: number;
    total_trades?: number;
    win_rate?: number;
  };
  trade_history: Array<Record<string, unknown>>;
}

type SectionKey = "results" | "data" | "simulation";

const sectionTabs = [
  { key: "results", label: "选股结果" },
  { key: "data", label: "完整数据" },
  { key: "simulation", label: "策略模拟" },
];

const defaultForm = {
  top_n: "10",
  max_pe: "20",
  max_pb: "1.5",
  min_dividend_yield: "1",
  max_debt_ratio: "30",
  min_float_cap_yi: "0",
  max_float_cap_yi: "0",
  sort_by: "流通市值升序",
  exclude_st: true,
  exclude_kcb: true,
  exclude_cyb: true,
};

function findColumn(record: Record<string, unknown> | undefined, patterns: string[]): string | null {
  if (!record) {
    return null;
  }
  for (const pattern of patterns) {
    const matched = Object.keys(record).find((key) => key.includes(pattern));
    if (matched) {
      return matched;
    }
  }
  return null;
}

function buildFilterSummary(form: typeof defaultForm): string {
  const parts = [
    `PE≤${Number(form.max_pe).toFixed(1)}`,
    `PB≤${Number(form.max_pb).toFixed(1)}`,
    `股息率≥${Number(form.min_dividend_yield).toFixed(1)}%`,
    `资产负债率≤${Number(form.max_debt_ratio).toFixed(1)}%`,
    `排序: ${form.sort_by}`,
  ];
  if (Number(form.min_float_cap_yi) > 0) {
    parts.push(`流通市值≥${Number(form.min_float_cap_yi).toFixed(0)}亿`);
  }
  if (Number(form.max_float_cap_yi) > 0) {
    parts.push(`流通市值≤${Number(form.max_float_cap_yi).toFixed(0)}亿`);
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
  return parts.join("，");
}

export function ValueStockPage() {
  const [form, setForm] = useState(defaultForm);
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [simulation, setSimulation] = useState<SimulationResult | null>(null);
  const [section, setSection] = useState<SectionKey>("results");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail | null>("/api/selectors/value-stock/tasks/latest");
    setTask(data);
  };

  useEffect(() => {
    void loadTask();
    const taskTimer = window.setInterval(() => void loadTask(), 2000);
    return () => window.clearInterval(taskTimer);
  }, []);

  const filterSummary = useMemo(() => buildFilterSummary(form), [form]);
  const stocks = task?.status === "success" ? task.result?.stocks ?? [] : [];
  const firstRow = stocks[0];
  const peKey = findColumn(firstRow, ["市盈率"]);
  const pbKey = findColumn(firstRow, ["市净率"]);
  const dividendKey = findColumn(firstRow, ["股息率"]);
  const debtKey = findColumn(firstRow, ["资产负债率"]);
  const capKey = findColumn(firstRow, ["流通市值"]);
  const industryKey = findColumn(firstRow, ["所属行业", "所属同花顺行业"]);
  const priceKey = findColumn(firstRow, ["最新价", "股价"]);
  const displayKeys = useMemo(
    () =>
      ["股票代码", "股票简称", priceKey, peKey, pbKey, dividendKey, debtKey, capKey, industryKey].filter(
        (item, index, array): item is string => Boolean(item) && array.indexOf(item) === index,
      ),
    [priceKey, peKey, pbKey, dividendKey, debtKey, capKey, industryKey],
  );

  const submitSelection = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/selectors/value-stock/tasks", {
        method: "POST",
        body: JSON.stringify({
          top_n: Number(form.top_n) || 10,
          max_pe: Number(form.max_pe) || 20,
          max_pb: Number(form.max_pb) || 1.5,
          min_dividend_yield: Number(form.min_dividend_yield) || 1,
          max_debt_ratio: Number(form.max_debt_ratio) || 30,
          min_float_cap_yi: Number(form.min_float_cap_yi) || 0,
          max_float_cap_yi: Number(form.max_float_cap_yi) || 0,
          sort_by: form.sort_by,
          exclude_st: form.exclude_st,
          exclude_kcb: form.exclude_kcb,
          exclude_cyb: form.exclude_cyb,
          filter_summary: filterSummary,
        }),
      });
      setSimulation(null);
      setSection("results");
      setMessage(`低估值选股任务已提交: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交低估值任务失败");
    }
  };

  const runSimulation = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<SimulationResult>("/api/selectors/value-stock/simulation", {
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

  return (
    <PageFrame
      title="低估值"
      summary="覆盖低估值选股、完整数据表格、CSV 导出和价值策略模拟。"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
    >
      <div className={styles.stack}>
        {section === "results" ? (
          <>
            <section className={styles.card}>
              <form className={styles.stack} onSubmit={submitSelection}>
                <div className={styles.formGrid}>
                  <div className={styles.field}>
                    <label htmlFor="topN">筛选数量</label>
                    <input id="topN" value={form.top_n} onChange={(event) => setForm((current) => ({ ...current, top_n: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxPe">最高 PE</label>
                    <input id="maxPe" value={form.max_pe} onChange={(event) => setForm((current) => ({ ...current, max_pe: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxPb">最高 PB</label>
                    <input id="maxPb" value={form.max_pb} onChange={(event) => setForm((current) => ({ ...current, max_pb: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="dividendYield">最低股息率(%)</label>
                    <input
                      id="dividendYield"
                      value={form.min_dividend_yield}
                      onChange={(event) => setForm((current) => ({ ...current, min_dividend_yield: event.target.value }))}
                    />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="debtRatio">最高资产负债率(%)</label>
                    <input
                      id="debtRatio"
                      value={form.max_debt_ratio}
                      onChange={(event) => setForm((current) => ({ ...current, max_debt_ratio: event.target.value }))}
                    />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="minCap">最低流通市值(亿)</label>
                    <input
                      id="minCap"
                      value={form.min_float_cap_yi}
                      onChange={(event) => setForm((current) => ({ ...current, min_float_cap_yi: event.target.value }))}
                    />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="maxCap">最高流通市值(亿)</label>
                    <input
                      id="maxCap"
                      value={form.max_float_cap_yi}
                      onChange={(event) => setForm((current) => ({ ...current, max_float_cap_yi: event.target.value }))}
                    />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="sortBy">排序方式</label>
                    <select id="sortBy" value={form.sort_by} onChange={(event) => setForm((current) => ({ ...current, sort_by: event.target.value }))}>
                      <option value="流通市值升序">流通市值升序</option>
                      <option value="PE升序">PE升序</option>
                      <option value="PB升序">PB升序</option>
                      <option value="股息率降序">股息率降序</option>
                      <option value="资产负债率升序">资产负债率升序</option>
                    </select>
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
                </div>
                <p className={styles.muted}>当前筛选: {filterSummary}</p>
                <div className={styles.actions}>
                  <button className={styles.primaryButton} type="submit">
                    开始低估值选股
                  </button>
                  <button
                    className={styles.secondaryButton}
                    onClick={() => downloadCsvRows(stocks, `value_stock_${new Date().toISOString().slice(0, 10)}.csv`)}
                    type="button"
                  >
                    下载 CSV
                  </button>
                  {!!stocks.length ? (
                    <button className={styles.secondaryButton} onClick={() => void runSimulation()} type="button">
                      开始策略模拟
                    </button>
                  ) : null}
                  {message ? <span className={styles.successText}>{message}</span> : null}
                  {error ? <span className={styles.dangerText}>{error}</span> : null}
                </div>
              </form>
            </section>

            {task ? (
              <section className={styles.card}>
                <h2>选股任务状态</h2>
                <p>{task.message || "等待低估值任务..."}</p>
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
                      <span className={styles.muted}>平均 PE</span>
                      <strong>
                        {numberText(stocks.reduce((sum, row) => sum + (asNumber(peKey ? row[peKey] : null) ?? 0), 0) / Math.max(stocks.length, 1), 1)}
                      </strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>平均 PB</span>
                      <strong>
                        {numberText(stocks.reduce((sum, row) => sum + (asNumber(pbKey ? row[pbKey] : null) ?? 0), 0) / Math.max(stocks.length, 1))}
                      </strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>平均股息率</span>
                      <strong>
                        {numberText(stocks.reduce((sum, row) => sum + (asNumber(dividendKey ? row[dividendKey] : null) ?? 0), 0) / Math.max(stocks.length, 1))}
                        %
                      </strong>
                    </div>
                  </div>
                  <p className={styles.muted}>
                    选股时间: {asText(task?.result?.selected_time, "-")} | 条件: {asText(task?.result?.filter_summary, "-")}
                  </p>
                </section>

                <section className={styles.card}>
                  <h2>精选低估值股票</h2>
                  <div className={styles.list}>
                    {stocks.map((stock, index) => (
                      <div className={styles.listItem} key={`${String(stock["股票代码"] ?? index)}`}>
                        <strong>
                          【第{index + 1}名】{asText(stock["股票代码"])} - {asText(stock["股票简称"] ?? stock["股票名称"])}
                        </strong>
                        <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                          <div className={styles.metric}>
                            <span className={styles.muted}>PE</span>
                            <strong>{numberText(peKey ? stock[peKey] : null, 1)}</strong>
                          </div>
                          <div className={styles.metric}>
                            <span className={styles.muted}>PB</span>
                            <strong>{numberText(pbKey ? stock[pbKey] : null)}</strong>
                          </div>
                          <div className={styles.metric}>
                            <span className={styles.muted}>股息率</span>
                            <strong>{numberText(dividendKey ? stock[dividendKey] : null)}%</strong>
                          </div>
                          <div className={styles.metric}>
                            <span className={styles.muted}>资产负债率</span>
                            <strong>{numberText(debtKey ? stock[debtKey] : null)}%</strong>
                          </div>
                        </div>
                        <div className={styles.muted} style={{ marginTop: 10 }}>
                          当前价格 {numberText(priceKey ? stock[priceKey] : null)} | 流通市值 {numberText((asNumber(capKey ? stock[capKey] : null) ?? 0) / 100000000)} 亿 | 行业{" "}
                          {industryKey ? asText(stock[industryKey]) : "N/A"}
                        </div>
                      </div>
                    ))}
                  </div>
                </section>
              </>
            ) : null}
          </>
        ) : null}

        {section === "data" && stocks.length ? (
          <section className={styles.card}>
            <h2>完整数据表格</h2>
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
          <section className={styles.card}>
            <h2>策略模拟结果</h2>
            {simulation ? (
              <>
                <div className={styles.list}>
                  {simulation.buy_results.map((item, index) => (
                    <div className={styles.listItem} key={`buy-${index}`}>
                      {item.success ? item.message : `失败: ${item.message}`}
                    </div>
                  ))}
                  {simulation.sell_checks.map((item) => (
                    <div className={styles.listItem} key={`rsi-${item.code}`}>
                      {asText(item.code)} {asText(item.name, "")} | {item.should_sell ? "触发卖出" : "继续持有"} | {asText(item.reason)} | RSI{" "}
                      {item.rsi ?? "N/A"}
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
                    <strong>{numberText(simulation.summary.total_assets)}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>累计收益率</span>
                    <strong>{numberText(simulation.summary.total_return)}%</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>胜率</span>
                    <strong>{numberText(simulation.summary.win_rate)}%</strong>
                  </div>
                </div>
              </>
            ) : (
              <div className={styles.muted}>先在选股结果页完成筛选，再运行策略模拟。</div>
            )}
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
