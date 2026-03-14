import { FormEvent, useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import { decodeIntent } from "../../lib/intents";
import { usePortfolioStore } from "../../stores/portfolioStore";
import styles from "../ConsolePage.module.scss";


interface PortfolioStock {
  id: number;
  code: string;
  name: string;
  account_name?: string;
  cost_price?: number;
  quantity?: number;
  analysis_record_id?: number;
  analysis_time?: string;
}

interface PortfolioTrade {
  id: number;
  stock_code: string;
  stock_name: string;
  trade_type: string;
  quantity: number;
  price: number;
  trade_time?: string;
  note?: string;
}

interface PortfolioRisk {
  status: string;
  message?: string;
  total_market_value?: number;
  total_cost_value?: number;
  total_pnl?: number;
  total_pnl_pct?: number;
  risk_warnings?: string[];
}

interface PortfolioReview {
  id: number;
  period_type?: string;
  period_start?: string;
  period_end?: string;
  report_markdown?: string;
}

interface SchedulerStatus {
  is_running: boolean;
  schedule_times?: string[];
  analysis_mode?: string;
  max_workers?: number;
}

interface PositionIntentPayload {
  symbol?: string;
  stock_name?: string;
  account_name?: string;
  origin_analysis_id?: number;
  default_cost_price?: number;
  default_note?: string;
}

type ComposerPanel = "position" | "trade" | "scheduler" | "review" | null;
type SectionKey = "overview" | "holdings" | "trades" | "scheduler" | "review";

const sectionTabs = [
  { key: "overview", label: "总览" },
  { key: "holdings", label: "持仓列表" },
  { key: "trades", label: "最近交易" },
  { key: "scheduler", label: "定时分析" },
  { key: "review", label: "复盘报告" },
];

const UI = {
  title: "持仓分析",
  allAccounts: "全部账户",
  defaultAccount: "默认账户",
  refresh: "刷新",
  addPosition: "新增持仓",
  addTrade: "登记交易",
  schedule: "定时分析",
  review: "复盘报告",
  close: "收起",
  riskWarnings: "风险提醒",
  noWarnings: "当前没有风险提醒。",
  noHoldings: "当前没有持仓记录。",
  noTrades: "最近还没有交易记录。",
  noReviews: "还没有生成复盘报告。",
  schedulerRunning: "定时分析运行中",
  schedulerIdle: "定时分析空闲",
};

const defaultPositionForm = {
  code: "",
  name: "",
  account_name: UI.defaultAccount,
  cost_price: "",
  quantity: "",
  note: "",
  buy_date: "",
  auto_monitor: true,
  origin_analysis_id: undefined as number | undefined,
};

const defaultTradeForm = {
  stock_id: "",
  trade_type: "buy" as "buy" | "sell" | "clear",
  quantity: "",
  price: "",
  trade_date: "",
  note: "",
};

function numberText(value: unknown, digits = 2) {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? numeric.toLocaleString("zh-CN", { maximumFractionDigits: digits })
    : "N/A";
}

function percentText(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? `${(numeric * 100).toLocaleString("zh-CN", { maximumFractionDigits: 2 })}%`
    : "N/A";
}

function tradeTypeText(value: string) {
  if (value === "buy") {
    return "买入";
  }
  if (value === "sell") {
    return "卖出";
  }
  if (value === "clear") {
    return "清仓";
  }
  return value || "未知";
}

export function PortfolioPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedAccount = usePortfolioStore((state) => state.selectedAccount);
  const setSelectedAccount = usePortfolioStore((state) => state.setSelectedAccount);

  const [stocks, setStocks] = useState<PortfolioStock[]>([]);
  const [trades, setTrades] = useState<PortfolioTrade[]>([]);
  const [risk, setRisk] = useState<PortfolioRisk | null>(null);
  const [reviews, setReviews] = useState<PortfolioReview[]>([]);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [positionForm, setPositionForm] = useState(defaultPositionForm);
  const [tradeForm, setTradeForm] = useState(defaultTradeForm);
  const [schedulerTimes, setSchedulerTimes] = useState("09:30");
  const [reviewPeriod, setReviewPeriod] = useState<"week" | "month" | "quarter">("month");
  const [activePanel, setActivePanel] = useState<ComposerPanel>(null);
  const [section, setSection] = useState<SectionKey>("overview");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadAll = async () => {
    const accountParam = selectedAccount === UI.allAccounts ? "" : selectedAccount;
    const [stockData, tradeData, riskData, reviewData, schedulerData] = await Promise.all([
      apiFetch<PortfolioStock[]>(`/api/portfolio/stocks${buildQuery({ account_name: accountParam })}`),
      apiFetch<PortfolioTrade[]>(`/api/portfolio/trades${buildQuery({ account_name: accountParam, limit: 30 })}`),
      apiFetch<PortfolioRisk>(`/api/portfolio/risk${buildQuery({ account_name: accountParam })}`),
      apiFetch<PortfolioReview[]>(`/api/portfolio/reviews${buildQuery({ account_name: accountParam })}`),
      apiFetch<SchedulerStatus>("/api/portfolio/scheduler"),
    ]);
    setStocks(stockData);
    setTrades(tradeData);
    setRisk(riskData);
    setReviews(reviewData);
    setScheduler(schedulerData);
    setSchedulerTimes((schedulerData.schedule_times ?? []).join(", ") || "09:30");
    if (!tradeForm.stock_id && stockData[0]) {
      setTradeForm((current) => ({ ...current, stock_id: String(stockData[0].id) }));
    }
  };

  useEffect(() => {
    void loadAll();
  }, [selectedAccount]);

  useEffect(() => {
    const intent = decodeIntent<PositionIntentPayload>(searchParams.get("intent"));
    if (!intent || intent.type !== "portfolio") {
      return;
    }

    const payload = intent.payload || {};
    setPositionForm((current) => ({
      ...current,
      code: payload.symbol || "",
      name: payload.stock_name || "",
      account_name: payload.account_name || UI.defaultAccount,
      cost_price: payload.default_cost_price !== undefined ? String(payload.default_cost_price) : "",
      note: payload.default_note || "",
      origin_analysis_id: payload.origin_analysis_id,
    }));
    setActivePanel("position");
    setSection("holdings");

    const nextParams = new URLSearchParams(searchParams);
    nextParams.delete("intent");
    setSearchParams(nextParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const accountOptions = useMemo(() => {
    const values = new Set<string>([UI.allAccounts, UI.defaultAccount]);
    stocks.forEach((item) => values.add(item.account_name || UI.defaultAccount));
    return Array.from(values);
  }, [stocks]);

  const analyzedHoldings = stocks.filter((item) => item.analysis_record_id).length;
  const riskWarnings = risk?.risk_warnings ?? [];
  const latestTrades = trades.slice(0, 12);
  const latestReviews = reviews.slice(0, 6);

  const openPanel = (panel: Exclude<ComposerPanel, null>, nextSection: SectionKey) => {
    setSection(nextSection);
    setActivePanel((current) => (current === panel ? null : panel));
  };

  const submitPosition = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/portfolio/stocks", {
        method: "POST",
        body: JSON.stringify({
          code: positionForm.code,
          name: positionForm.name || positionForm.code,
          account_name: positionForm.account_name,
          cost_price: positionForm.cost_price ? Number(positionForm.cost_price) : null,
          quantity: positionForm.quantity ? Number(positionForm.quantity) : null,
          note: positionForm.note,
          buy_date: positionForm.buy_date || null,
          auto_monitor: positionForm.auto_monitor,
          origin_analysis_id: positionForm.origin_analysis_id ?? null,
        }),
      });
      setMessage(`持仓已新增：${positionForm.code}`);
      setPositionForm(defaultPositionForm);
      setActivePanel(null);
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "新增持仓失败");
    }
  };

  const submitTrade = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!tradeForm.stock_id) {
      setError("请先选择对应持仓。");
      return;
    }

    setMessage("");
    setError("");
    try {
      await apiFetch(`/api/portfolio/stocks/${tradeForm.stock_id}/trades`, {
        method: "POST",
        body: JSON.stringify({
          trade_type: tradeForm.trade_type,
          quantity: tradeForm.quantity ? Number(tradeForm.quantity) : 0,
          price: tradeForm.price ? Number(tradeForm.price) : 0,
          trade_date: tradeForm.trade_date || null,
          note: tradeForm.note,
        }),
      });
      setMessage("交易记录已保存。");
      setTradeForm((current) => ({ ...defaultTradeForm, stock_id: current.stock_id }));
      setActivePanel(null);
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "登记交易失败");
    }
  };

  const saveScheduler = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/portfolio/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          schedule_times: schedulerTimes.split(",").map((item) => item.trim()).filter(Boolean),
          analysis_mode: scheduler?.analysis_mode || "sequential",
          max_workers: scheduler?.max_workers || 3,
        }),
      });
      setMessage("定时分析配置已更新。");
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "保存定时分析失败");
    }
  };

  const toggleScheduler = async (running: boolean) => {
    setMessage("");
    setError("");
    try {
      await apiFetch(running ? "/api/portfolio/scheduler/start" : "/api/portfolio/scheduler/stop", {
        method: "POST",
      });
      setMessage(running ? "定时分析已启动。" : "定时分析已停止。");
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新定时分析状态失败");
    }
  };

  const runSchedulerNow = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/portfolio/scheduler/run-once", { method: "POST" });
      setMessage("已触发一次立即分析。");
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "立即执行失败");
    }
  };

  const generateReview = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/portfolio/reviews", {
        method: "POST",
        body: JSON.stringify({
          account_name: selectedAccount === UI.allAccounts ? null : selectedAccount,
          period_type: reviewPeriod,
        }),
      });
      setMessage("复盘报告已生成。");
      setActivePanel(null);
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "生成复盘报告失败");
    }
  };

  const renderComposer = () => {
    if (activePanel === "position") {
      return (
        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <div>
              <h2>新增持仓</h2>
            </div>
            <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">
              {UI.close}
            </button>
          </div>
          <form className={styles.stack} onSubmit={submitPosition}>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="position-code">股票代码</label>
                <input id="position-code" value={positionForm.code} onChange={(event) => setPositionForm((current) => ({ ...current, code: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="position-name">股票名称</label>
                <input id="position-name" value={positionForm.name} onChange={(event) => setPositionForm((current) => ({ ...current, name: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="position-account">账户</label>
                <input id="position-account" value={positionForm.account_name} onChange={(event) => setPositionForm((current) => ({ ...current, account_name: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="position-cost">成本价</label>
                <input id="position-cost" value={positionForm.cost_price} onChange={(event) => setPositionForm((current) => ({ ...current, cost_price: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="position-quantity">数量</label>
                <input id="position-quantity" value={positionForm.quantity} onChange={(event) => setPositionForm((current) => ({ ...current, quantity: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="position-date">买入日期</label>
                <input id="position-date" type="date" value={positionForm.buy_date} onChange={(event) => setPositionForm((current) => ({ ...current, buy_date: event.target.value }))} />
              </div>
            </div>
            <div className={styles.field}>
              <label htmlFor="position-note">备注</label>
              <textarea id="position-note" rows={3} value={positionForm.note} onChange={(event) => setPositionForm((current) => ({ ...current, note: event.target.value }))} />
            </div>
            <label className={styles.listItem}>
              <input checked={positionForm.auto_monitor} onChange={(event) => setPositionForm((current) => ({ ...current, auto_monitor: event.target.checked }))} type="checkbox" />{" "}
              新增后自动加入监控联动
            </label>
            <div className={styles.actions}>
              <button className={styles.primaryButton} type="submit">保存持仓</button>
              <button className={styles.secondaryButton} onClick={() => setActivePanel(null)} type="button">取消</button>
            </div>
          </form>
        </section>
      );
    }

    if (activePanel === "trade") {
      return (
        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <div>
              <h2>交易登记</h2>
            </div>
            <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">
              {UI.close}
            </button>
          </div>
          <form className={styles.stack} onSubmit={submitTrade}>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="trade-stock">选择持仓</label>
                <select id="trade-stock" value={tradeForm.stock_id} onChange={(event) => setTradeForm((current) => ({ ...current, stock_id: event.target.value }))}>
                  <option value="">请选择持仓</option>
                  {stocks.map((stock) => (
                    <option key={stock.id} value={stock.id}>
                      {stock.name} ({stock.code})
                    </option>
                  ))}
                </select>
              </div>
              <div className={styles.field}>
                <label htmlFor="trade-type">交易类型</label>
                <select id="trade-type" value={tradeForm.trade_type} onChange={(event) => setTradeForm((current) => ({ ...current, trade_type: event.target.value as "buy" | "sell" | "clear" }))}>
                  <option value="buy">加仓</option>
                  <option value="sell">减仓</option>
                  <option value="clear">清仓</option>
                </select>
              </div>
              <div className={styles.field}>
                <label htmlFor="trade-quantity">数量</label>
                <input id="trade-quantity" value={tradeForm.quantity} onChange={(event) => setTradeForm((current) => ({ ...current, quantity: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="trade-price">价格</label>
                <input id="trade-price" value={tradeForm.price} onChange={(event) => setTradeForm((current) => ({ ...current, price: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="trade-date">交易日期</label>
                <input id="trade-date" type="date" value={tradeForm.trade_date} onChange={(event) => setTradeForm((current) => ({ ...current, trade_date: event.target.value }))} />
              </div>
              <div className={styles.field}>
                <label htmlFor="trade-note">备注</label>
                <input id="trade-note" value={tradeForm.note} onChange={(event) => setTradeForm((current) => ({ ...current, note: event.target.value }))} />
              </div>
            </div>
            <div className={styles.actions}>
              <button className={styles.primaryButton} type="submit">保存交易</button>
              <button className={styles.secondaryButton} onClick={() => setActivePanel(null)} type="button">取消</button>
            </div>
          </form>
        </section>
      );
    }

    if (activePanel === "scheduler") {
      return (
        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <div>
              <h2>定时分析配置</h2>
            </div>
            <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">
              {UI.close}
            </button>
          </div>
          <div className={styles.actions}>
            <input value={schedulerTimes} onChange={(event) => setSchedulerTimes(event.target.value)} />
            <button className={styles.secondaryButton} onClick={() => void saveScheduler()} type="button">保存调度</button>
            <button className={styles.secondaryButton} onClick={() => void toggleScheduler(true)} type="button">启动</button>
            <button className={styles.secondaryButton} onClick={() => void toggleScheduler(false)} type="button">停止</button>
            <button className={styles.primaryButton} onClick={() => void runSchedulerNow()} type="button">立即执行</button>
          </div>
        </section>
      );
    }

    if (activePanel === "review") {
      return (
        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <div>
              <h2>生成复盘报告</h2>
            </div>
            <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">
              {UI.close}
            </button>
          </div>
          <div className={styles.actions}>
            <select value={reviewPeriod} onChange={(event) => setReviewPeriod(event.target.value as "week" | "month" | "quarter")}>
              <option value="week">周度</option>
              <option value="month">月度</option>
              <option value="quarter">季度</option>
            </select>
            <button className={styles.primaryButton} onClick={() => void generateReview()} type="button">生成复盘</button>
          </div>
        </section>
      );
    }

    return null;
  };

  return (
    <PageFrame
      title={UI.title}
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      actions={
        <>
          <StatusBadge label={scheduler?.is_running ? UI.schedulerRunning : UI.schedulerIdle} tone={scheduler?.is_running ? "success" : "default"} />
          <StatusBadge label={`持仓 ${stocks.length}`} tone="default" />
          <StatusBadge label={`风险提醒 ${riskWarnings.length}`} tone={riskWarnings.length ? "warning" : "default"} />
        </>
      }
    >
      <div className={styles.stack}>
        {section === "overview" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <select value={selectedAccount} onChange={(event) => setSelectedAccount(event.target.value)}>
                  {accountOptions.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
                <button className={styles.secondaryButton} onClick={() => void loadAll()} type="button">{UI.refresh}</button>
                {message ? <span className={styles.successText}>{message}</span> : null}
                {error ? <span className={styles.dangerText}>{error}</span> : null}
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>总市值</span>
                  <strong>{numberText(risk?.total_market_value)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>总成本</span>
                  <strong>{numberText(risk?.total_cost_value)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>浮动盈亏</span>
                  <strong>{numberText(risk?.total_pnl)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>收益率</span>
                  <strong>{percentText(risk?.total_pnl_pct)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>已联动分析</span>
                  <strong>{analyzedHoldings} / {stocks.length || 0}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>最近交易</span>
                  <strong>{latestTrades.length}</strong>
                </div>
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.cardHeader}>
                <div>
                  <h2>{UI.riskWarnings}</h2>
                </div>
              </div>
              <div className={styles.list}>
                {riskWarnings.map((item, index) => (
                  <div className={`${styles.noticeCard} ${styles.noticeWarning}`} key={`${item}-${index}`}>
                    <div className={styles.noticeMeta}>
                      <StatusBadge label="风险提醒" tone="warning" />
                    </div>
                    <div>{item}</div>
                  </div>
                ))}
                {risk?.status === "error" && risk.message ? (
                  <div className={`${styles.noticeCard} ${styles.noticeDanger}`}>
                    <div className={styles.noticeMeta}>
                      <StatusBadge label="系统异常" tone="danger" />
                    </div>
                    <div>{risk.message}</div>
                  </div>
                ) : null}
                {!riskWarnings.length && risk?.status !== "error" ? (
                  <div className={styles.noticeCard}>
                    <div className={styles.noticeMeta}>
                      <StatusBadge label="状态稳定" tone="default" />
                    </div>
                    <div>{UI.noWarnings}</div>
                  </div>
                ) : null}
              </div>
            </section>
          </>
        ) : null}

        {section === "holdings" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <button className={section === "holdings" && activePanel === "position" ? styles.primaryButton : styles.secondaryButton} onClick={() => openPanel("position", "holdings")} type="button">{UI.addPosition}</button>
                {message ? <span className={styles.successText}>{message}</span> : null}
                {error ? <span className={styles.dangerText}>{error}</span> : null}
              </div>
            </section>

            {activePanel === "position" ? renderComposer() : null}

            <section className={styles.card}>
              <div className={styles.cardHeader}>
                <div>
                  <h2>持仓列表</h2>
                </div>
              </div>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>账户</th>
                      <th>成本 / 数量</th>
                      <th>最近分析</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stocks.map((stock) => (
                      <tr key={stock.id}>
                        <td>
                          <strong>{stock.name}</strong>
                          <div className={styles.muted}>{stock.code}</div>
                        </td>
                        <td>{stock.account_name || UI.defaultAccount}</td>
                        <td>{numberText(stock.cost_price)} / {stock.quantity ?? "N/A"}</td>
                        <td>
                          <div>{stock.analysis_time || "暂无"}</div>
                          {stock.analysis_record_id ? (
                            <div style={{ marginTop: 8 }}>
                              <button className={styles.secondaryButton} onClick={() => navigate(`/research/history?recordId=${stock.analysis_record_id}`)} type="button">
                                查看分析历史
                              </button>
                            </div>
                          ) : null}
                        </td>
                      </tr>
                    ))}
                    {!stocks.length ? (
                      <tr>
                        <td className={styles.muted} colSpan={4}>{UI.noHoldings}</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        ) : null}

        {section === "trades" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <button className={section === "trades" && activePanel === "trade" ? styles.primaryButton : styles.secondaryButton} onClick={() => openPanel("trade", "trades")} type="button">{UI.addTrade}</button>
                {message ? <span className={styles.successText}>{message}</span> : null}
                {error ? <span className={styles.dangerText}>{error}</span> : null}
              </div>
            </section>

            {activePanel === "trade" ? renderComposer() : null}

            <section className={styles.card}>
              <div className={styles.cardHeader}>
                <div>
                  <h2>最近交易</h2>
                </div>
              </div>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>时间</th>
                      <th>股票</th>
                      <th>类型</th>
                      <th>数量</th>
                      <th>价格</th>
                    </tr>
                  </thead>
                  <tbody>
                    {latestTrades.map((trade) => (
                      <tr key={trade.id}>
                        <td>{trade.trade_time || "暂无"}</td>
                        <td>{trade.stock_name} ({trade.stock_code})</td>
                        <td>{tradeTypeText(trade.trade_type)}</td>
                        <td>{trade.quantity}</td>
                        <td>{numberText(trade.price)}</td>
                      </tr>
                    ))}
                    {!latestTrades.length ? (
                      <tr>
                        <td className={styles.muted} colSpan={5}>{UI.noTrades}</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        ) : null}

        {section === "scheduler" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <button className={section === "scheduler" && activePanel === "scheduler" ? styles.primaryButton : styles.secondaryButton} onClick={() => openPanel("scheduler", "scheduler")} type="button">{UI.schedule}</button>
                {message ? <span className={styles.successText}>{message}</span> : null}
                {error ? <span className={styles.dangerText}>{error}</span> : null}
              </div>
            </section>

            {activePanel === "scheduler" ? renderComposer() : null}

            <section className={styles.card}>
              <div className={styles.cardHeader}>
                <div>
                  <h2>定时分析状态</h2>
                </div>
              </div>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>运行状态</span>
                  <strong>{scheduler?.is_running ? "运行中" : "未启动"}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>执行时间</span>
                  <strong>{schedulerTimes || "未配置"}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>分析模式</span>
                  <strong>{scheduler?.analysis_mode || "sequential"}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>最大并发</span>
                  <strong>{scheduler?.max_workers ?? 3}</strong>
                </div>
              </div>
            </section>
          </>
        ) : null}

        {section === "review" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <button className={section === "review" && activePanel === "review" ? styles.primaryButton : styles.secondaryButton} onClick={() => openPanel("review", "review")} type="button">{UI.review}</button>
                {message ? <span className={styles.successText}>{message}</span> : null}
                {error ? <span className={styles.dangerText}>{error}</span> : null}
              </div>
            </section>

            {activePanel === "review" ? renderComposer() : null}

            <section className={styles.card}>
              <div className={styles.cardHeader}>
                <div>
                  <h2>复盘报告</h2>
                </div>
              </div>
              <div className={styles.list}>
                {latestReviews.map((review) => (
                  <details className={styles.listItem} key={review.id}>
                    <summary>{review.period_type || "周期"} | {review.period_start || "-"} ~ {review.period_end || "-"}</summary>
                    <pre className={styles.code}>{review.report_markdown || "暂无正文"}</pre>
                  </details>
                ))}
                {!latestReviews.length ? <div className={styles.muted}>{UI.noReviews}</div> : null}
              </div>
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
