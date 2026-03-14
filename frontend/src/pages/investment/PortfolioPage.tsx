import { FormEvent, useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { ArcElement, Chart as ChartJS, Legend, Tooltip } from "chart.js";
import { Doughnut } from "react-chartjs-2";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch, apiFetchCached, buildQuery } from "../../lib/api";
import { formatDateOnly } from "../../lib/datetime";
import { decodeIntent } from "../../lib/intents";
import { usePortfolioStore, type PortfolioPageCache } from "../../stores/portfolioStore";
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

interface PortfolioStockDistribution {
  code?: string;
  name?: string;
  market_value?: number;
  cost_value?: number;
  pnl?: number;
  pnl_pct?: number;
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

interface PortfolioTradePage {
  items: PortfolioTrade[];
  total: number;
  page: number;
  page_size: number;
}

interface PortfolioRisk {
  status: string;
  message?: string;
  total_market_value?: number;
  total_cost_value?: number;
  total_pnl?: number;
  total_pnl_pct?: number;
  risk_warnings?: string[];
  stock_distribution?: PortfolioStockDistribution[];
  industry_distribution?: Array<{
    industry?: string;
    market_value?: number;
    weight?: number;
  }>;
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

type ComposerPanel = "position" | "trade" | "scheduler" | null;
type SectionKey = "overview" | "holdings" | "trades" | "scheduler";

const sectionTabs = [
  { key: "overview", label: "总览" },
  { key: "holdings", label: "持仓列表" },
  { key: "trades", label: "最近交易" },
  { key: "scheduler", label: "定时分析" },
];

const UI = {
  title: "持仓分析",
  allAccounts: "全部账户",
  defaultAccount: "ly",
  refresh: "刷新",
  addPosition: "新增持仓",
  addTrade: "登记交易",
  schedule: "定时分析",
  close: "收起",
  riskWarnings: "风险提醒",
  noWarnings: "当前没有风险提醒。",
  noHoldings: "当前没有持仓记录。",
  noTrades: "最近还没有交易记录。",
  schedulerRunning: "定时分析运行中",
  schedulerIdle: "定时分析空闲",
};

const TRADE_PAGE_SIZE = 20;
const PAGE_CACHE_TTL_MS = 30_000;
const PIE_COLORS = ["#c65d4b", "#db7c57", "#d6a45f", "#7f9b6d", "#4f7c82", "#6f6d9b", "#9a5f7c", "#8b7d64"];

ChartJS.register(ArcElement, Legend, Tooltip);

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

function schedulerModeLabel(value?: string) {
  return value === "parallel" ? "并行分析" : "顺序分析";
}

function resolvePnlTone(value: unknown, stylesMap: Record<string, string>) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return stylesMap.muted;
  }
  return numeric > 0 ? stylesMap.dangerText : numeric < 0 ? stylesMap.successText : stylesMap.muted;
}

export function PortfolioPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedAccount = usePortfolioStore((state) => state.selectedAccount);
  const knownAccounts = usePortfolioStore((state) => state.knownAccounts);
  const setSelectedAccount = usePortfolioStore((state) => state.setSelectedAccount);
  const setKnownAccounts = usePortfolioStore((state) => state.setKnownAccounts);
  const cachedPage = usePortfolioStore((state) => state.pageCacheByAccount[selectedAccount] ?? null);
  const allAccountsCache = usePortfolioStore((state) => state.pageCacheByAccount[UI.allAccounts] ?? null);
  const setPageCache = usePortfolioStore((state) => state.setPageCache);

  const [stocks, setStocks] = useState<PortfolioStock[]>(() => (cachedPage?.stocks as PortfolioStock[]) ?? []);
  const [trades, setTrades] = useState<PortfolioTrade[]>(() => (cachedPage?.trades as PortfolioTrade[]) ?? []);
  const [tradePage, setTradePage] = useState(() => cachedPage?.tradePage ?? 1);
  const [tradeTotal, setTradeTotal] = useState(() => cachedPage?.tradeTotal ?? 0);
  const [risk, setRisk] = useState<PortfolioRisk | null>(() => (cachedPage?.risk as PortfolioRisk | null) ?? null);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(() => (cachedPage?.scheduler as SchedulerStatus | null) ?? null);
  const [positionForm, setPositionForm] = useState(defaultPositionForm);
  const [tradeForm, setTradeForm] = useState(defaultTradeForm);
  const [schedulerTimes, setSchedulerTimes] = useState(() => cachedPage?.schedulerTimes ?? "09:30");
  const [schedulerMode, setSchedulerMode] = useState<"sequential" | "parallel">(
    () => (cachedPage?.scheduler as SchedulerStatus | null)?.analysis_mode === "parallel" ? "parallel" : "sequential",
  );
  const [schedulerMaxWorkers, setSchedulerMaxWorkers] = useState(
    () => (cachedPage?.scheduler as SchedulerStatus | null)?.max_workers ?? 3,
  );
  const [activePanel, setActivePanel] = useState<ComposerPanel>(null);
  const [section, setSection] = useState<SectionKey>("overview");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const applyPageCache = (cache: PortfolioPageCache | null) => {
    if (!cache) {
      return;
    }
    setStocks(cache.stocks as PortfolioStock[]);
    setTrades(cache.trades as PortfolioTrade[]);
    setTradePage(cache.tradePage || 1);
    setTradeTotal(cache.tradeTotal || 0);
    setRisk((cache.risk as PortfolioRisk | null) ?? null);
    const cachedScheduler = (cache.scheduler as SchedulerStatus | null) ?? null;
    setScheduler(cachedScheduler);
    setSchedulerTimes(cache.schedulerTimes || "09:30");
    setSchedulerMode(cachedScheduler?.analysis_mode === "parallel" ? "parallel" : "sequential");
    setSchedulerMaxWorkers(cachedScheduler?.max_workers ?? 3);
  };

  const loadAll = async (force = false, targetTradePage = tradePage) => {
    if (
      !force &&
      cachedPage &&
      cachedPage.tradePage === targetTradePage &&
      Date.now() - cachedPage.updatedAt < PAGE_CACHE_TTL_MS
    ) {
      applyPageCache(cachedPage);
      return;
    }
    const accountParam = selectedAccount === UI.allAccounts ? "" : selectedAccount;
    const [stockData, tradeData, riskData, schedulerData] = await Promise.all([
      apiFetchCached<PortfolioStock[]>(`/api/portfolio/stocks${buildQuery({ account_name: accountParam })}`),
      apiFetch<PortfolioTradePage>(
        `/api/portfolio/trades${buildQuery({
          account_name: accountParam,
          page: targetTradePage,
          page_size: TRADE_PAGE_SIZE,
        })}`,
      ),
      apiFetchCached<PortfolioRisk>(`/api/portfolio/risk${buildQuery({ account_name: accountParam })}`),
      apiFetch<SchedulerStatus>("/api/portfolio/scheduler"),
    ]);
    setStocks(stockData);
    setTrades(tradeData.items);
    setTradePage(tradeData.page);
    setTradeTotal(tradeData.total);
    setRisk(riskData);
    setScheduler(schedulerData);
    const nextSchedulerTimes = (schedulerData.schedule_times ?? []).join(", ") || "09:30";
    setSchedulerTimes(nextSchedulerTimes);
    setSchedulerMode(schedulerData.analysis_mode === "parallel" ? "parallel" : "sequential");
    setSchedulerMaxWorkers(schedulerData.max_workers ?? 3);
    setKnownAccounts(
      stockData
        .map((item) => item.account_name || UI.defaultAccount)
        .concat(selectedAccount === UI.allAccounts ? [] : [selectedAccount]),
    );
    setPageCache(selectedAccount, {
      stocks: stockData,
      trades: tradeData.items,
      tradePage: tradeData.page,
      tradeTotal: tradeData.total,
      tradePageSize: tradeData.page_size,
      risk: riskData,
      scheduler: schedulerData,
      schedulerTimes: nextSchedulerTimes,
      updatedAt: Date.now(),
    });
    if (!tradeForm.stock_id && stockData[0]) {
      setTradeForm((current) => ({ ...current, stock_id: String(stockData[0].id) }));
    }
  };

  useEffect(() => {
    if (cachedPage && cachedPage.tradePage === tradePage) {
      applyPageCache(cachedPage);
    }
    if (
      cachedPage &&
      cachedPage.tradePage === tradePage &&
      Date.now() - cachedPage.updatedAt < PAGE_CACHE_TTL_MS
    ) {
      return;
    }
    void loadAll(Boolean(cachedPage && cachedPage.tradePage === tradePage), tradePage);
  }, [selectedAccount, tradePage, cachedPage?.updatedAt]);

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
    const values = new Set<string>([UI.allAccounts, UI.defaultAccount, selectedAccount, ...knownAccounts]);
    const cachedAllStocks = (allAccountsCache?.stocks as PortfolioStock[] | undefined) ?? [];
    cachedAllStocks.forEach((item) => values.add(item.account_name || UI.defaultAccount));
    stocks.forEach((item) => values.add(item.account_name || UI.defaultAccount));
    return Array.from(values);
  }, [allAccountsCache?.updatedAt, knownAccounts, selectedAccount, stocks]);

  const riskWarnings = risk?.risk_warnings ?? [];
  const tradePageCount = Math.max(1, Math.ceil(tradeTotal / TRADE_PAGE_SIZE));
  const holdingMetrics = useMemo(() => {
    const distribution = risk?.stock_distribution ?? [];
    return new Map(
      distribution.map((item) => [
        `${item.code || ""}::${item.name || ""}`,
        {
          pnl: item.pnl,
          pnlPct: item.pnl_pct,
        },
      ]),
    );
  }, [risk?.stock_distribution]);
  const renderAccountSelect = (id: string) => (
    <select
      id={id}
      value={selectedAccount}
      onChange={(event) => {
        setTradePage(1);
        setSelectedAccount(event.target.value);
      }}
    >
      {accountOptions.map((item) => (
        <option key={item} value={item}>
          {item}
        </option>
      ))}
    </select>
  );
  const industryPieData = useMemo(() => {
    const items = (risk?.industry_distribution ?? [])
      .filter((item) => Number(item.market_value) > 0)
      .slice(0, 8);
    return {
      labels: items.map((item) => item.industry || "未知行业"),
      datasets: [
        {
          data: items.map((item) => Number(item.market_value) || 0),
          backgroundColor: PIE_COLORS.slice(0, items.length),
          borderWidth: 0,
        },
      ],
    };
  }, [risk?.industry_distribution]);
  const stockPieData = useMemo(() => {
    const items = (risk?.stock_distribution ?? [])
      .filter((item) => Number(item.market_value) > 0)
      .slice(0, 8);
    return {
      labels: items.map((item) => `${item.name || item.code || "未知"}（${item.code || "--"}）`),
      datasets: [
        {
          data: items.map((item) => Number(item.market_value) || 0),
          backgroundColor: PIE_COLORS.slice(0, items.length),
          borderWidth: 0,
        },
      ],
    };
  }, [risk?.stock_distribution]);

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
      await loadAll(true, tradePage);
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
      await loadAll(true, tradePage);
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
          analysis_mode: schedulerMode,
          max_workers: schedulerMaxWorkers,
        }),
      });
      setMessage("定时分析配置已更新。");
      await loadAll(true, tradePage);
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
      await loadAll(true, tradePage);
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
      await loadAll(true, tradePage);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "立即执行失败");
    }
  };

  const renderComposer = () => {
    if (activePanel === "position") {
      return (
        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <p className={styles.helperText}>新增持仓后，可继续登记交易并关联后续分析历史。</p>
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
                <input
                  id="position-account"
                  list="portfolio-account-options"
                  value={positionForm.account_name}
                  onChange={(event) => setPositionForm((current) => ({ ...current, account_name: event.target.value }))}
                  placeholder="输入新账号或选择已有账号"
                />
                <datalist id="portfolio-account-options">
                  {accountOptions
                    .filter((item) => item !== UI.allAccounts)
                    .map((item) => (
                      <option key={item} value={item} />
                    ))}
                </datalist>
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
            <p className={styles.helperText}>登记后的流水会出现在完整交易列表中，并支持分页查看。</p>
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
            <p className={styles.helperText}>可以直接修改执行时间、分析模式和并发线程数。</p>
            <button className={styles.tertiaryButton} onClick={() => setActivePanel(null)} type="button">
              {UI.close}
            </button>
          </div>
          <div className={styles.formGrid}>
            <div className={styles.field}>
              <label htmlFor="scheduler-times">执行时间</label>
              <input id="scheduler-times" value={schedulerTimes} onChange={(event) => setSchedulerTimes(event.target.value)} />
            </div>
            <div className={styles.field}>
              <label htmlFor="scheduler-mode">分析模式</label>
              <select
                id="scheduler-mode"
                onChange={(event) => setSchedulerMode(event.target.value as "sequential" | "parallel")}
                value={schedulerMode}
              >
                <option value="sequential">顺序分析</option>
                <option value="parallel">并行分析</option>
              </select>
            </div>
            {schedulerMode === "parallel" ? (
              <div className={styles.field}>
                <label htmlFor="scheduler-workers">并发线程数</label>
                <select
                  id="scheduler-workers"
                  onChange={(event) => setSchedulerMaxWorkers(Number(event.target.value) || 3)}
                  value={schedulerMaxWorkers}
                >
                  {[1, 2, 3, 4, 5].map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </select>
              </div>
            ) : null}
          </div>
          <div className={styles.actions} style={{ marginTop: 14 }}>
            <button className={styles.secondaryButton} onClick={() => void saveScheduler()} type="button">保存调度</button>
            <button className={styles.secondaryButton} onClick={() => void toggleScheduler(true)} type="button">启动</button>
            <button className={styles.secondaryButton} onClick={() => void toggleScheduler(false)} type="button">停止</button>
            <button className={styles.primaryButton} onClick={() => void runSchedulerNow()} type="button">立即执行</button>
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
              <div className={styles.stack}>
                <div className={styles.field}>
                  <label htmlFor="portfolio-account-overview">账户</label>
                  {renderAccountSelect("portfolio-account-overview")}
                </div>
                <div className={styles.actions}>
                  <button className={styles.secondaryButton} onClick={() => void loadAll(true, tradePage)} type="button">{UI.refresh}</button>
                </div>
                {message ? <span className={styles.successText}>{message}</span> : null}
                {error ? <span className={styles.dangerText}>{error}</span> : null}
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

            <section className={styles.card}>
              <h2>行业分布</h2>
              {industryPieData.labels.length ? (
                <div className={styles.chartRingWrap}>
                  <Doughnut
                    data={industryPieData}
                    options={{
                      responsive: true,
                      maintainAspectRatio: false,
                      cutout: "58%",
                      plugins: { legend: { position: "bottom" } },
                    }}
                  />
                </div>
              ) : (
                <div className={styles.muted}>暂无行业分布数据</div>
              )}
            </section>
            <section className={styles.card}>
              <h2>个股分布</h2>
              {stockPieData.labels.length ? (
                <div className={styles.chartRingWrap}>
                  <Doughnut
                    data={stockPieData}
                    options={{
                      responsive: true,
                      maintainAspectRatio: false,
                      cutout: "58%",
                      plugins: { legend: { position: "bottom" } },
                    }}
                  />
                </div>
              ) : (
                <div className={styles.muted}>暂无个股分布数据</div>
              )}
            </section>
          </>
        ) : null}

        {section === "holdings" ? (
          <>
            <section className={styles.card}>
              <div className={styles.stack}>
                <div className={styles.field}>
                  <label htmlFor="portfolio-account-holdings">账户</label>
                  {renderAccountSelect("portfolio-account-holdings")}
                </div>
                <div className={styles.summaryMetricGrid}>
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
                    <strong className={resolvePnlTone(risk?.total_pnl, styles)}>{numberText(risk?.total_pnl)}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>收益率</span>
                    <strong className={resolvePnlTone(risk?.total_pnl, styles)}>{percentText(risk?.total_pnl_pct)}</strong>
                  </div>
                </div>
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.actions}>
                <button className={section === "holdings" && activePanel === "position" ? styles.primaryButton : styles.secondaryButton} onClick={() => openPanel("position", "holdings")} type="button">{UI.addPosition}</button>
                {message ? <span className={styles.successText}>{message}</span> : null}
                {error ? <span className={styles.dangerText}>{error}</span> : null}
              </div>
            </section>

            {activePanel === "position" ? renderComposer() : null}

            <section className={styles.card}>
              <div className={styles.tableWrap}>
                <table className={`${styles.table} ${styles.tableCompact} ${styles.holdingsTableCompact}`}>
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>成本 / 数量</th>
                      <th className={styles.numericCell}>盈亏 / 收益率</th>
                      <th>分析历史</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stocks.map((stock) => {
                      const metrics = holdingMetrics.get(`${stock.code || ""}::${stock.name || ""}`);
                      const pnlClassName = resolvePnlTone(metrics?.pnl, styles);

                      return (
                      <tr key={stock.id}>
                        <td>
                          <strong>{stock.name}</strong>
                          <div className={styles.muted}>{stock.code}</div>
                        </td>
                        <td>{numberText(stock.cost_price)} / {stock.quantity ?? "N/A"}</td>
                        <td className={`${styles.numericCell} ${pnlClassName}`}>
                          <div>{numberText(metrics?.pnl)}</div>
                          <div>{percentText(metrics?.pnlPct)}</div>
                        </td>
                        <td className={styles.holdingHistoryCell}>
                          {stock.analysis_record_id ? (
                            <button
                              className={styles.holdingHistoryButton}
                              onClick={() => navigate(`/research/history?recordId=${stock.analysis_record_id}`)}
                              type="button"
                            >
                              查看
                            </button>
                          ) : (
                            <span className={styles.muted}>暂无</span>
                          )}
                        </td>
                      </tr>
                    )})}
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
              <div className={styles.tableWrap}>
                <table className={`${styles.table} ${styles.tableCompact} ${styles.tradeTableCompact}`}>
                  <thead>
                    <tr>
                      <th>时间</th>
                      <th>股票</th>
                      <th className={styles.desktopOnly}>类型</th>
                      <th className={styles.desktopOnly}>数量</th>
                      <th className={styles.mobileOnly}>交易</th>
                      <th className={styles.numericCell}>价格</th>
                    </tr>
                  </thead>
                  <tbody>
                    {trades.map((trade) => (
                      <tr key={trade.id}>
                        <td>{formatDateOnly(trade.trade_time, "暂无")}</td>
                        <td>{`${trade.stock_name}（${trade.stock_code}）`}</td>
                        <td className={styles.desktopOnly}>{tradeTypeText(trade.trade_type)}</td>
                        <td className={styles.desktopOnly}>{trade.quantity}</td>
                        <td className={styles.mobileOnly}>{`${tradeTypeText(trade.trade_type)} ${trade.quantity}股`}</td>
                        <td className={styles.numericCell}>{numberText(trade.price)}</td>
                      </tr>
                    ))}
                    {!trades.length ? (
                      <tr>
                        <td className={styles.muted} colSpan={6}>{UI.noTrades}</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
              <div className={styles.pagination}>
                <button className={styles.secondaryButton} disabled={tradePage <= 1} onClick={() => setTradePage((current) => Math.max(1, current - 1))} type="button">
                  上一页
                </button>
                <span className={styles.muted}>第 {tradePage} / {tradePageCount} 页</span>
                <button className={styles.secondaryButton} disabled={tradePage >= tradePageCount} onClick={() => setTradePage((current) => Math.min(tradePageCount, current + 1))} type="button">
                  下一页
                </button>
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
                  <strong>{schedulerModeLabel(scheduler?.analysis_mode)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>最大并发</span>
                  <strong>{scheduler?.max_workers ?? 3}</strong>
                </div>
              </div>
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
