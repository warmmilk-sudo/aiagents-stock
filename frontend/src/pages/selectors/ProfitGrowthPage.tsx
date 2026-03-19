import { FormEvent, useEffect, useMemo, useState } from "react";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch } from "../../lib/api";
import { asNumber, asText, downloadCsvRows, findKey, numberText } from "../../lib/market";
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
  monitored_count: number;
  pending_alerts: number;
  removed_count: number;
}

interface MonitoredStock {
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
  holding_days?: number;
  alert_time?: string;
}

interface RemovedItem {
  id?: number;
  stock_code: string;
  stock_name: string;
  remove_time?: string;
  remove_reason?: string;
}

type SectionKey = "results" | "data" | "monitor";

const sectionTabs = [
  { key: "results", label: "选股结果" },
  { key: "data", label: "完整数据" },
  { key: "monitor", label: "监控中心" },
];

const defaultForm = {
  top_n: "5",
  min_profit_growth: "10",
  min_turnover_yi: "0",
  max_turnover_yi: "0",
  sort_by: "成交额升序",
  exclude_st: true,
  exclude_kcb: true,
  exclude_cyb: true,
};

function buildFilterSummary(form: typeof defaultForm): string {
  const parts = [`净利增长≥${Number(form.min_profit_growth).toFixed(0)}%`, `排序: ${form.sort_by}`];
  if (Number(form.min_turnover_yi) > 0) {
    parts.push(`成交额≥${Number(form.min_turnover_yi).toFixed(0)}亿`);
  }
  if (Number(form.max_turnover_yi) > 0) {
    parts.push(`成交额≤${Number(form.max_turnover_yi).toFixed(0)}亿`);
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

export function ProfitGrowthPage() {
  const [form, setForm] = useState(defaultForm);
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [monitorStatus, setMonitorStatus] = useState<MonitorStatus | null>(null);
  const [monitoredStocks, setMonitoredStocks] = useState<MonitoredStock[]>([]);
  const [pendingAlerts, setPendingAlerts] = useState<AlertItem[]>([]);
  const [alertHistory, setAlertHistory] = useState<AlertItem[]>([]);
  const [removedStocks, setRemovedStocks] = useState<RemovedItem[]>([]);
  const [section, setSection] = useState<SectionKey>("results");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [isSubmittingSelection, setIsSubmittingSelection] = useState(false);
  const [isSendingNotification, setIsSendingNotification] = useState(false);
  const [pendingMonitorCode, setPendingMonitorCode] = useState("");

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail | null>("/api/selectors/profit-growth/tasks/latest");
    setTask(data);
  };

  const loadMonitorData = async () => {
    const [status, stocks, alerts, history, removed] = await Promise.all([
      apiFetch<MonitorStatus>("/api/selectors/profit-growth/monitor/status"),
      apiFetch<MonitoredStock[]>("/api/selectors/profit-growth/monitor/stocks"),
      apiFetch<AlertItem[]>("/api/selectors/profit-growth/monitor/alerts"),
      apiFetch<AlertItem[]>("/api/selectors/profit-growth/monitor/alerts/history?limit=50"),
      apiFetch<RemovedItem[]>("/api/selectors/profit-growth/monitor/removed?limit=50"),
    ]);
    setMonitorStatus(status);
    setMonitoredStocks(stocks);
    setPendingAlerts(alerts);
    setAlertHistory(history);
    setRemovedStocks(removed);
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
      findKey(firstRow, ["净利润增长率", "净利润同比增长率"]),
      findKey(firstRow, ["成交额"]),
      findKey(firstRow, ["股价", "最新价"]),
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
    setIsSubmittingSelection(true);
    try {
      const data = await apiFetch<{ task_id: string }>("/api/selectors/profit-growth/tasks", {
        method: "POST",
        body: JSON.stringify({
          top_n: Number(form.top_n) || 5,
          min_profit_growth: Number(form.min_profit_growth) || 10,
          min_turnover_yi: Number(form.min_turnover_yi) || 0,
          max_turnover_yi: Number(form.max_turnover_yi) || 0,
          sort_by: form.sort_by,
          exclude_st: form.exclude_st,
          exclude_kcb: form.exclude_kcb,
          exclude_cyb: form.exclude_cyb,
          filter_summary: filterSummary,
        }),
      });
      setSection("results");
      setMessage(`净利增长选股任务已提交: ${data.task_id}`);
      await loadTask().catch(() => undefined);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交净利增长任务失败");
    } finally {
      setIsSubmittingSelection(false);
    }
  };

  const notifyWebhook = async () => {
    setMessage("");
    setError("");
    setIsSendingNotification(true);
    try {
      await apiFetch("/api/selectors/profit-growth/notify", {
        method: "POST",
        body: JSON.stringify({ stocks, filter_summary: filterSummary }),
      });
      setSection("results");
      setMessage("钉钉通知已发送");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "发送通知失败");
    } finally {
      setIsSendingNotification(false);
    }
  };

  const addToMonitor = async (stock: Record<string, unknown>) => {
    const code = String(stock["股票代码"] ?? "").split(".")[0];
    const name = asText(stock["股票简称"] ?? stock["股票名称"], code);
    const price = asNumber(stock["股价"] ?? stock["最新价"]) ?? 0;
    setMessage("");
    setError("");
    if (pendingMonitorCode === code) {
      return;
    }
    const optimisticStock: MonitoredStock = {
      stock_code: code,
      stock_name: name,
      buy_price: price,
      buy_date: new Date().toISOString().slice(0, 10),
      holding_days: 0,
      add_time: new Date().toISOString(),
    };
    setPendingMonitorCode(code);
    setMonitoredStocks((current) =>
      current.some((item) => item.stock_code === code) ? current : [optimisticStock, ...current],
    );
    setMonitorStatus((current) =>
      current
        ? {
            ...current,
            monitored_count: Number(current.monitored_count ?? 0) + 1,
          }
        : current,
    );
    try {
      await apiFetch("/api/selectors/profit-growth/monitor/stocks", {
        method: "POST",
        body: JSON.stringify({ stock_code: code, stock_name: name, buy_price: price }),
      });
      setSection("monitor");
      setMessage(`已加入策略监控: ${code}`);
      await loadMonitorData().catch(() => undefined);
    } catch (requestError) {
      setMonitoredStocks((current) => current.filter((item) => item.stock_code !== code));
      setMonitorStatus((current) =>
        current
          ? {
              ...current,
              monitored_count: Math.max(0, Number(current.monitored_count ?? 0) - 1),
            }
          : current,
      );
      setError(requestError instanceof ApiRequestError ? requestError.message : "加入监控失败");
    } finally {
      setPendingMonitorCode((current) => (current === code ? "" : current));
    }
  };

  const removeFromMonitor = async (stockCode: string) => {
    setMessage("");
    setError("");
    if (pendingMonitorCode === stockCode) {
      return;
    }
    const removedIndex = monitoredStocks.findIndex((item) => item.stock_code === stockCode);
    const removedStock = monitoredStocks[removedIndex] ?? null;
    setPendingMonitorCode(stockCode);
    setMonitoredStocks((current) => current.filter((item) => item.stock_code !== stockCode));
    setMonitorStatus((current) =>
      current
        ? {
            ...current,
            monitored_count: Math.max(0, Number(current.monitored_count ?? 0) - 1),
          }
        : current,
    );
    try {
      await apiFetch(`/api/selectors/profit-growth/monitor/stocks/${stockCode}`, { method: "DELETE" });
      setSection("monitor");
      setMessage(`已移出监控: ${stockCode}`);
      await loadMonitorData().catch(() => undefined);
    } catch (requestError) {
      if (removedStock) {
        setMonitoredStocks((current) => {
          if (current.some((item) => item.stock_code === removedStock.stock_code)) {
            return current;
          }
          const next = [...current];
          next.splice(Math.max(0, Math.min(removedIndex, next.length)), 0, removedStock);
          return next;
        });
      }
      setMonitorStatus((current) =>
        current
          ? {
              ...current,
              monitored_count: Number(current.monitored_count ?? 0) + 1,
            }
          : current,
      );
      setError(requestError instanceof ApiRequestError ? requestError.message : "移出监控失败");
    } finally {
      setPendingMonitorCode((current) => (current === stockCode ? "" : current));
    }
  };

  const monitoredCodes = useMemo(() => new Set(monitoredStocks.map((item) => item.stock_code)), [monitoredStocks]);

  return (
    <PageFrame
      title="净利增长"
      summary="覆盖净利增长选股、策略监控、卖出提醒和移除历史。"
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
                    <label htmlFor="topN">筛选数量</label>
                    <input id="topN" value={form.top_n} onChange={(event) => setForm((current) => ({ ...current, top_n: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="profitGrowth">最低净利增速(%)</label>
                    <input id="profitGrowth" value={form.min_profit_growth} onChange={(event) => setForm((current) => ({ ...current, min_profit_growth: event.target.value }))} />
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
                    <label htmlFor="sortBy">排序方式</label>
                    <select id="sortBy" value={form.sort_by} onChange={(event) => setForm((current) => ({ ...current, sort_by: event.target.value }))}>
                      <option value="成交额升序">成交额升序</option>
                      <option value="成交额降序">成交额降序</option>
                      <option value="净利润增长率降序">净利润增长率降序</option>
                      <option value="股价升序">股价升序</option>
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
                  <button className={styles.primaryButton} disabled={isSubmittingSelection} type="submit">
                    {isSubmittingSelection ? "提交中..." : "开始净利增长选股"}
                  </button>
                  <button
                    className={styles.secondaryButton}
                    onClick={() => downloadCsvRows(stocks, `profit_growth_${new Date().toISOString().slice(0, 10)}.csv`)}
                    type="button"
                  >
                    下载 CSV
                  </button>
                  {!!stocks.length ? (
                    <button className={styles.secondaryButton} disabled={isSendingNotification} onClick={() => void notifyWebhook()} type="button">
                      {isSendingNotification ? "发送中..." : "发送钉钉通知"}
                    </button>
                  ) : null}
                </div>
              </form>
            </section>

            {task ? (
              <section className={styles.card}>
                <h2>选股任务状态</h2>
                <p>{task.message || "等待净利增长任务..."}</p>
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
                          stocks.reduce((sum, row) => sum + (asNumber(row["净利润增长率"] ?? row["净利润同比增长率"]) ?? 0), 0) / Math.max(stocks.length, 1),
                        )}
                        %
                      </strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>平均成交额</span>
                      <strong>
                        {numberText(
                          stocks.reduce((sum, row) => sum + (asNumber(row["成交额"]) ?? 0), 0) / Math.max(stocks.length, 1) / 100000000,
                        )}
                        亿
                      </strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>平均股价</span>
                      <strong>
                        {numberText(
                          stocks.reduce((sum, row) => sum + (asNumber(row["股价"] ?? row["最新价"]) ?? 0), 0) / Math.max(stocks.length, 1),
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
                      const industryKey = findKey(stock, ["所属行业", "所属同花顺行业"]);
                      return (
                        <div className={styles.listItem} key={`${code}-${index}`}>
                          <strong>
                            第 {index + 1} 名 · {code} - {asText(stock["股票简称"] ?? stock["股票名称"], code)}
                          </strong>
                          <div className={styles.compactGrid} style={{ marginTop: 12 }}>
                            <div className={styles.metric}>
                              <span className={styles.muted}>净利增速</span>
                              <strong>{numberText(stock["净利润增长率"] ?? stock["净利润同比增长率"])}%</strong>
                            </div>
                            <div className={styles.metric}>
                              <span className={styles.muted}>成交额</span>
                              <strong>{numberText((asNumber(stock["成交额"]) ?? 0) / 100000000)}亿</strong>
                            </div>
                            <div className={styles.metric}>
                              <span className={styles.muted}>当前价格</span>
                              <strong>{numberText(stock["股价"] ?? stock["最新价"])}元</strong>
                            </div>
                            <div className={styles.metric}>
                              <span className={styles.muted}>所属行业</span>
                              <strong>{industryKey ? asText(stock[industryKey]) : "N/A"}</strong>
                            </div>
                          </div>
                          <div className={styles.actions} style={{ marginTop: 12 }}>
                            {monitoredCodes.has(code) ? (
                              <button className={styles.dangerButton} disabled={pendingMonitorCode === code} onClick={() => void removeFromMonitor(code)} type="button">
                                {pendingMonitorCode === code ? "处理中..." : "移出监控"}
                              </button>
                            ) : (
                              <button className={styles.primaryButton} disabled={pendingMonitorCode === code} onClick={() => void addToMonitor(stock)} type="button">
                                {pendingMonitorCode === code ? "处理中..." : "加入策略监控"}
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
            <h2 className={styles.mobileDuplicateHeading}>完整数据表格</h2>
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

        {section === "monitor" ? (
          <>
            <section className={styles.card}>
              <h2 className={styles.mobileDuplicateHeading}>策略监控中心</h2>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>监控股票</span>
                  <strong>{monitorStatus?.monitored_count ?? 0} 只</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>待处理提醒</span>
                  <strong>{monitorStatus?.pending_alerts ?? 0} 条</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>移除历史</span>
                  <strong>{monitorStatus?.removed_count ?? 0} 条</strong>
                </div>
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
                      <th>持股天数</th>
                      <th>加入时间</th>
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
                        <td>{asText(item.add_time, "-")}</td>
                        <td>
                          <button className={styles.dangerButton} disabled={pendingMonitorCode === item.stock_code} onClick={() => void removeFromMonitor(item.stock_code)} type="button">
                            {pendingMonitorCode === item.stock_code ? "处理中..." : "移除"}
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
                      {alert.stock_code} {alert.stock_name}
                    </strong>
                    <div style={{ marginTop: 10 }}>{asText(alert.alert_type)} - {asText(alert.alert_reason)}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      当前价格 {numberText(alert.current_price)} | 持有天数 {asText(alert.holding_days)} | 时间 {asText(alert.alert_time)}
                    </div>
                  </div>
                ))}
                {!pendingAlerts.length ? <div className={styles.muted}>暂无新的卖出提醒。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>提醒历史</h2>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>提醒类型</th>
                      <th>提醒原因</th>
                      <th>提醒时间</th>
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
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!alertHistory.length ? <div className={styles.muted}>暂无提醒历史。</div> : null}
            </section>

            <section className={styles.card}>
              <h2>移除历史</h2>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>移除时间</th>
                      <th>移除原因</th>
                    </tr>
                  </thead>
                  <tbody>
                    {removedStocks.map((item) => (
                      <tr key={`${item.stock_code}-${item.remove_time}`}>
                        <td>
                          {item.stock_code} {item.stock_name}
                        </td>
                        <td>{asText(item.remove_time)}</td>
                        <td>{asText(item.remove_reason)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!removedStocks.length ? <div className={styles.muted}>暂无移除历史。</div> : null}
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
