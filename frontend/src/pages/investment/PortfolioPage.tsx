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

const defaultPositionForm = {
  code: "",
  name: "",
  account_name: "默认账户",
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

const numberText = (value: unknown, digits = 2) => {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(digits) : "N/A";
};

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
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadAll = async () => {
    const accountParam = selectedAccount === "全部账户" ? "" : selectedAccount;
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
    setSchedulerTimes((schedulerData.schedule_times ?? []).join(",") || "09:30");
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
      account_name: payload.account_name || "默认账户",
      cost_price: payload.default_cost_price !== undefined ? String(payload.default_cost_price) : "",
      note: payload.default_note || "",
      origin_analysis_id: payload.origin_analysis_id,
    }));
    searchParams.delete("intent");
    setSearchParams(searchParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const accountOptions = useMemo(() => {
    const values = new Set<string>(["全部账户", "默认账户"]);
    stocks.forEach((item) => values.add(item.account_name || "默认账户"));
    return Array.from(values);
  }, [stocks]);

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
      setMessage(`持仓已新增: ${positionForm.code}`);
      setPositionForm(defaultPositionForm);
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "新增持仓失败");
    }
  };

  const submitTrade = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!tradeForm.stock_id) {
      setError("请选择持仓");
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
      setMessage("交易记录已保存");
      setTradeForm((current) => ({ ...defaultTradeForm, stock_id: current.stock_id }));
      await loadAll();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "记录交易失败");
    }
  };

  return (
    <PageFrame
      title="持仓分析"
      summary="当前支持持仓新增、交易登记、风险概览、定时分析和复盘报告。"
      actions={
        <>
          <StatusBadge label={scheduler?.is_running ? "定时分析运行中" : "定时分析空闲"} tone={scheduler?.is_running ? "success" : "default"} />
          <StatusBadge label={`持仓 ${stocks.length}`} tone="info" />
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <select value={selectedAccount} onChange={(event) => setSelectedAccount(event.target.value)}>
              {accountOptions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
            <button className={styles.secondaryButton} onClick={() => void loadAll()} type="button">
              刷新
            </button>
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
              <strong>{numberText((Number(risk?.total_pnl_pct) || 0) * 100)}%</strong>
            </div>
          </div>
          <div className={styles.list}>
            {(risk?.risk_warnings ?? []).map((item, index) => (
              <div className={styles.listItem} key={`${item}-${index}`}>
                {item}
              </div>
            ))}
            {risk?.status === "error" && risk.message ? <div className={styles.dangerText}>{risk.message}</div> : null}
          </div>
        </section>

        <section className={styles.card}>
          <h2>新增持仓</h2>
          <form className={styles.stack} onSubmit={submitPosition}>
            <div className={styles.formGrid}>
              <input placeholder="股票代码" value={positionForm.code} onChange={(event) => setPositionForm((current) => ({ ...current, code: event.target.value }))} />
              <input placeholder="股票名称" value={positionForm.name} onChange={(event) => setPositionForm((current) => ({ ...current, name: event.target.value }))} />
              <input placeholder="账户" value={positionForm.account_name} onChange={(event) => setPositionForm((current) => ({ ...current, account_name: event.target.value }))} />
              <input placeholder="成本价" value={positionForm.cost_price} onChange={(event) => setPositionForm((current) => ({ ...current, cost_price: event.target.value }))} />
              <input placeholder="数量" value={positionForm.quantity} onChange={(event) => setPositionForm((current) => ({ ...current, quantity: event.target.value }))} />
              <input type="date" value={positionForm.buy_date} onChange={(event) => setPositionForm((current) => ({ ...current, buy_date: event.target.value }))} />
            </div>
            <textarea rows={3} placeholder="备注" value={positionForm.note} onChange={(event) => setPositionForm((current) => ({ ...current, note: event.target.value }))} />
            <label className={styles.listItem}>
              <input checked={positionForm.auto_monitor} onChange={(event) => setPositionForm((current) => ({ ...current, auto_monitor: event.target.checked }))} type="checkbox" /> 自动联动监测
            </label>
            <button className={styles.primaryButton} type="submit">
              保存持仓
            </button>
          </form>
        </section>

        <section className={styles.card}>
          <h2>交易登记</h2>
          <form className={styles.stack} onSubmit={submitTrade}>
            <div className={styles.formGrid}>
              <select value={tradeForm.stock_id} onChange={(event) => setTradeForm((current) => ({ ...current, stock_id: event.target.value }))}>
                <option value="">请选择持仓</option>
                {stocks.map((stock) => (
                  <option key={stock.id} value={stock.id}>
                    {stock.name} ({stock.code})
                  </option>
                ))}
              </select>
              <select value={tradeForm.trade_type} onChange={(event) => setTradeForm((current) => ({ ...current, trade_type: event.target.value as "buy" | "sell" | "clear" }))}>
                <option value="buy">加仓</option>
                <option value="sell">减仓</option>
                <option value="clear">清仓</option>
              </select>
              <input placeholder="数量" value={tradeForm.quantity} onChange={(event) => setTradeForm((current) => ({ ...current, quantity: event.target.value }))} />
              <input placeholder="价格" value={tradeForm.price} onChange={(event) => setTradeForm((current) => ({ ...current, price: event.target.value }))} />
              <input type="date" value={tradeForm.trade_date} onChange={(event) => setTradeForm((current) => ({ ...current, trade_date: event.target.value }))} />
              <input placeholder="备注" value={tradeForm.note} onChange={(event) => setTradeForm((current) => ({ ...current, note: event.target.value }))} />
            </div>
            <button className={styles.primaryButton} type="submit">
              保存交易
            </button>
          </form>
        </section>

        <section className={styles.card}>
          <h2>持仓列表</h2>
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>股票</th>
                  <th>账户</th>
                  <th>成本 / 数量</th>
                  <th>最新分析</th>
                </tr>
              </thead>
              <tbody>
                {stocks.map((stock) => (
                  <tr key={stock.id}>
                    <td>
                      <strong>{stock.name}</strong>
                      <div className={styles.muted}>{stock.code}</div>
                    </td>
                    <td>{stock.account_name || "默认账户"}</td>
                    <td>
                      {numberText(stock.cost_price)} / {stock.quantity ?? "N/A"}
                    </td>
                    <td>
                      {stock.analysis_time || "暂无"}
                      {stock.analysis_record_id ? (
                        <div>
                          <button className={styles.secondaryButton} onClick={() => navigate(`/research/history?recordId=${stock.analysis_record_id}`)} type="button">
                            分析历史
                          </button>
                        </div>
                      ) : null}
                    </td>
                  </tr>
                ))}
                {stocks.length === 0 ? (
                  <tr>
                    <td className={styles.muted} colSpan={4}>
                      暂无持仓
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>

        <section className={styles.card}>
          <h2>最近交易</h2>
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
                {trades.map((trade) => (
                  <tr key={trade.id}>
                    <td>{trade.trade_time || "暂无"}</td>
                    <td>{trade.stock_name} ({trade.stock_code})</td>
                    <td>{trade.trade_type}</td>
                    <td>{trade.quantity}</td>
                    <td>{numberText(trade.price)}</td>
                  </tr>
                ))}
                {trades.length === 0 ? (
                  <tr>
                    <td className={styles.muted} colSpan={5}>
                      暂无交易记录
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>

        <section className={styles.card}>
          <h2>定时分析</h2>
          <div className={styles.actions}>
            <input value={schedulerTimes} onChange={(event) => setSchedulerTimes(event.target.value)} />
            <button className={styles.secondaryButton} onClick={() => void apiFetch("/api/portfolio/scheduler", { method: "PUT", body: JSON.stringify({ schedule_times: schedulerTimes.split(",").map((item) => item.trim()).filter(Boolean), analysis_mode: scheduler?.analysis_mode || "sequential", max_workers: scheduler?.max_workers || 3 }) }).then(loadAll)} type="button">
              保存调度
            </button>
            <button className={styles.secondaryButton} onClick={() => void apiFetch("/api/portfolio/scheduler/start", { method: "POST" }).then(loadAll)} type="button">
              启动
            </button>
            <button className={styles.secondaryButton} onClick={() => void apiFetch("/api/portfolio/scheduler/stop", { method: "POST" }).then(loadAll)} type="button">
              停止
            </button>
            <button className={styles.primaryButton} onClick={() => void apiFetch("/api/portfolio/scheduler/run-once", { method: "POST" }).then(loadAll)} type="button">
              立即执行
            </button>
          </div>
        </section>

        <section className={styles.card}>
          <h2>复盘报告</h2>
          <div className={styles.actions}>
            <select value={reviewPeriod} onChange={(event) => setReviewPeriod(event.target.value as "week" | "month" | "quarter")}>
              <option value="week">周度</option>
              <option value="month">月度</option>
              <option value="quarter">季度</option>
            </select>
            <button className={styles.primaryButton} onClick={() => void apiFetch("/api/portfolio/reviews", { method: "POST", body: JSON.stringify({ account_name: selectedAccount === "全部账户" ? null : selectedAccount, period_type: reviewPeriod }) }).then(loadAll)} type="button">
              生成复盘
            </button>
          </div>
          <div className={styles.list}>
            {reviews.map((review) => (
              <details className={styles.listItem} key={review.id}>
                <summary>{review.period_type} | {review.period_start} ~ {review.period_end}</summary>
                <pre className={styles.code}>{review.report_markdown || "暂无正文"}</pre>
              </details>
            ))}
            {reviews.length === 0 ? <div className={styles.muted}>暂无复盘报告</div> : null}
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
