import { FormEvent, useEffect, useMemo, useState } from "react";

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
  running: boolean;
  scan_interval: number;
  monitored_count: number;
  pending_alerts: number;
}

interface MonitoredStock {
  stock_code: string;
  stock_name: string;
  buy_price: number;
  buy_date: string;
  holding_days: number;
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

type SectionKey = "results" | "data" | "monitor";

const sectionTabs = [
  { key: "results", label: "选股结果" },
  { key: "data", label: "完整数据" },
  { key: "monitor", label: "监控中心" },
];

const defaultForm = {
  top_n: "5",
  max_market_cap_yi: "50",
  min_revenue_growth: "10",
  min_profit_growth: "100",
  sort_by: "总市值升序",
  exclude_st: true,
  exclude_kcb: true,
  exclude_cyb: true,
  only_hs_a: true,
};

function buildFilterSummary(form: typeof defaultForm): string {
  const parts = [
    `总市值≤${Number(form.max_market_cap_yi).toFixed(0)}亿`,
    `营收增长≥${Number(form.min_revenue_growth).toFixed(0)}%`,
    `净利增长≥${Number(form.min_profit_growth).toFixed(0)}%`,
    `排序: ${form.sort_by}`,
  ];
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

export function SmallCapPage() {
  const [form, setForm] = useState(defaultForm);
  const [scanInterval, setScanInterval] = useState("60");
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [monitorStatus, setMonitorStatus] = useState<MonitorStatus | null>(null);
  const [monitoredStocks, setMonitoredStocks] = useState<MonitoredStock[]>([]);
  const [pendingAlerts, setPendingAlerts] = useState<AlertItem[]>([]);
  const [alertHistory, setAlertHistory] = useState<AlertItem[]>([]);
  const [section, setSection] = useState<SectionKey>("results");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail | null>("/api/selectors/small-cap/tasks/latest");
    setTask(data);
  };

  const loadMonitorData = async () => {
    const [status, stocks, alerts, history] = await Promise.all([
      apiFetch<MonitorStatus>("/api/selectors/small-cap/monitor/status"),
      apiFetch<MonitoredStock[]>("/api/selectors/small-cap/monitor/stocks"),
      apiFetch<AlertItem[]>("/api/selectors/small-cap/monitor/alerts"),
      apiFetch<AlertItem[]>("/api/selectors/small-cap/monitor/alerts/history?limit=50"),
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
      findKey(firstRow, ["总市值"]),
      findKey(firstRow, ["营收增长率", "营业收入增长率"]),
      findKey(firstRow, ["净利润增长率", "净利润同比增长率"]),
      findKey(firstRow, ["股价", "最新价"]),
      findKey(firstRow, ["所属行业", "所属同花顺行业"]),
    ];
    return keys.filter((item, index, array): item is string => Boolean(item) && array.indexOf(item) === index);
  }, [firstRow]);

  const submitSelection = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/selectors/small-cap/tasks", {
        method: "POST",
        body: JSON.stringify({
          top_n: Number(form.top_n) || 5,
          max_market_cap_yi: Number(form.max_market_cap_yi) || 50,
          min_revenue_growth: Number(form.min_revenue_growth) || 10,
          min_profit_growth: Number(form.min_profit_growth) || 100,
          sort_by: form.sort_by,
          exclude_st: form.exclude_st,
          exclude_kcb: form.exclude_kcb,
          exclude_cyb: form.exclude_cyb,
          only_hs_a: form.only_hs_a,
          filter_summary: filterSummary,
        }),
      });
      setSection("results");
      setMessage(`小市值选股任务已提交: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交小市值任务失败");
    }
  };

  const notifyWebhook = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/selectors/small-cap/notify", {
        method: "POST",
        body: JSON.stringify({ stocks, filter_summary: filterSummary }),
      });
      setSection("results");
      setMessage("钉钉通知已发送");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "发送通知失败");
    }
  };

  const addToMonitor = async (stock: Record<string, unknown>) => {
    const code = String(stock["股票代码"] ?? "").split(".")[0];
    const name = asText(stock["股票简称"] ?? stock["股票名称"], code);
    const price = asNumber(stock["股价"] ?? stock["最新价"]) ?? 0;
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/selectors/small-cap/monitor/stocks", {
        method: "POST",
        body: JSON.stringify({ stock_code: code, stock_name: name, buy_price: price }),
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
      await apiFetch(`/api/selectors/small-cap/monitor/stocks/${stockCode}`, { method: "DELETE" });
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
      const data = await apiFetch<MonitorStatus>("/api/selectors/small-cap/monitor/config", {
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
        running ? "/api/selectors/small-cap/monitor/start" : "/api/selectors/small-cap/monitor/stop",
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
      await apiFetch(`/api/selectors/small-cap/monitor/alerts/${alertId}/resolve`, {
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
      await apiFetch("/api/selectors/small-cap/monitor/alerts/cleanup?days=30", { method: "POST" });
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
      title="小市值"
      summary="覆盖小市值选股、钉钉通知和复用原策略监控面板的全部核心交互。"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      actions={
        <>
          <StatusBadge label={`结果 ${stocks.length}`} tone="default" />
          <StatusBadge label={monitorStatus?.running ? "监控运行中" : "监控空闲"} tone={monitorStatus?.running ? "success" : "default"} />
          <StatusBadge
            label={task ? `选股 ${task.status} ${Math.round((task.progress ?? 0) * 100)}%` : "选股空闲"}
            tone={task?.status === "success" ? "success" : task?.status === "failed" ? "danger" : task ? "warning" : "default"}
          />
        </>
      }
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
                    <label htmlFor="maxCap">最高总市值(亿)</label>
                    <input id="maxCap" value={form.max_market_cap_yi} onChange={(event) => setForm((current) => ({ ...current, max_market_cap_yi: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="revenueGrowth">最低营收增速(%)</label>
                    <input id="revenueGrowth" value={form.min_revenue_growth} onChange={(event) => setForm((current) => ({ ...current, min_revenue_growth: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="profitGrowth">最低净利增速(%)</label>
                    <input id="profitGrowth" value={form.min_profit_growth} onChange={(event) => setForm((current) => ({ ...current, min_profit_growth: event.target.value }))} />
                  </div>
                  <div className={styles.field}>
                    <label htmlFor="sortBy">排序方式</label>
                    <select id="sortBy" value={form.sort_by} onChange={(event) => setForm((current) => ({ ...current, sort_by: event.target.value }))}>
                      <option value="总市值升序">总市值升序</option>
                      <option value="营收增长率降序">营收增长率降序</option>
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
                  <label className={styles.listItem}>
                    <input checked={form.only_hs_a} onChange={(event) => setForm((current) => ({ ...current, only_hs_a: event.target.checked }))} type="checkbox" /> 仅沪深 A 股
                  </label>
                </div>
                <p className={styles.muted}>当前筛选: {filterSummary}</p>
                <div className={styles.actions}>
                  <button className={styles.primaryButton} type="submit">
                    开始小市值选股
                  </button>
                  <button
                    className={styles.secondaryButton}
                    onClick={() => downloadCsvRows(stocks, `small_cap_${new Date().toISOString().slice(0, 10)}.csv`)}
                    type="button"
                  >
                    下载 CSV
                  </button>
                  {!!stocks.length ? (
                    <button className={styles.secondaryButton} onClick={() => void notifyWebhook()} type="button">
                      发送钉钉通知
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
                <p>{task.message || "等待小市值任务..."}</p>
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
                  <span className={styles.muted}>平均总市值</span>
                  <strong>
                    {numberText(
                      stocks.reduce((sum, row) => sum + (asNumber(row["总市值"]) ?? 0), 0) / Math.max(stocks.length, 1) / 100000000,
                    )}
                    亿
                  </strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>平均营收增长率</span>
                  <strong>
                    {numberText(
                      stocks.reduce((sum, row) => sum + (asNumber(row["营收增长率"] ?? row["营业收入增长率"]) ?? 0), 0) / Math.max(stocks.length, 1),
                    )}
                    %
                  </strong>
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
                          <span className={styles.muted}>总市值</span>
                          <strong>{numberText((asNumber(stock["总市值"]) ?? 0) / 100000000)}亿</strong>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>营收增速</span>
                          <strong>{numberText(stock["营收增长率"] ?? stock["营业收入增长率"])}%</strong>
                        </div>
                        <div className={styles.metric}>
                          <span className={styles.muted}>净利增速</span>
                          <strong>{numberText(stock["净利润增长率"] ?? stock["净利润同比增长率"])}%</strong>
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
