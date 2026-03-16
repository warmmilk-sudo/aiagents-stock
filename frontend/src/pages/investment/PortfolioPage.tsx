import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { ArcElement, Chart as ChartJS, Legend, Tooltip } from "chart.js";
import { Doughnut } from "react-chartjs-2";

import { AnalystSelector } from "../../components/common/AnalystSelector";
import { ModuleCard } from "../../components/common/ModuleCard";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { SchedulerControl } from "../../components/common/SchedulerControl";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import { ANALYST_OPTIONS, analystKeysToConfig, normalizeAnalystKeys, type AnalystKey } from "../../constants/analysts";
import { DEFAULT_SCHEDULER_TIME, DEFAULT_SCHEDULER_WORKERS, schedulerModeLabel } from "../../constants/scheduler";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { usePollingLoader } from "../../hooks/usePollingLoader";
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
  note?: string;
  analysis_record_id?: number;
}

interface PortfolioStockDistribution {
  code?: string;
  name?: string;
  market_value?: number;
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
  }>;
}

interface SchedulerStatus {
  is_running: boolean;
  schedule_times?: string[];
  analysis_mode?: string;
  max_workers?: number;
  selected_agents?: string[];
  account_configs?: SchedulerAccountConfig[];
}

interface SchedulerAccountConfig {
  account_name: string;
  enabled: boolean;
}

interface PositionIntentPayload {
  symbol?: string;
  stock_name?: string;
  account_name?: string;
  origin_analysis_id?: number;
  default_cost_price?: number;
  default_note?: string;
}

interface AnalysisTaskRow {
  symbol?: string;
  code?: string;
  success?: boolean;
  error?: string;
}

interface AnalysisTaskSummary {
  id: string;
  label?: string;
  status: string;
  message: string;
  current?: number;
  total?: number;
  error?: string;
  metadata?: Record<string, unknown>;
  result?: {
    mode?: string;
    symbol?: string;
    success_count?: number;
    failed_count?: number;
    saved_count?: number;
    results?: AnalysisTaskRow[];
    trigger?: string;
    analysis_result?: {
      total?: number;
      succeeded?: number;
      failed?: number;
      accounts?: Array<{
        account_name: string;
      }>;
      failed_stocks?: AnalysisTaskRow[];
    };
    persistence_result?: {
      saved_ids?: number[];
    };
  } | null;
}

type EditorPanel = "position" | "editPosition" | "trade" | null;
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
  deletePosition: "删除",
  noWarnings: "当前没有风险提醒。",
  noHoldings: "当前没有持仓记录。",
  noTrades: "最近还没有交易记录。",
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

function resolvePnlTone(value: unknown, stylesMap: Record<string, string>) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return stylesMap.muted;
  }
  return numeric > 0 ? stylesMap.dangerText : numeric < 0 ? stylesMap.successText : stylesMap.muted;
}

function normalizeSchedulerMode(value?: string): "sequential" | "parallel" {
  return value === "parallel" ? "parallel" : "sequential";
}

function normalizeSchedulerAccountConfigs(
  accountNames: string[],
  configs: SchedulerAccountConfig[] | undefined,
) {
  const configMap = new Map(
    (configs ?? [])
      .filter((item) => item?.account_name)
      .map((item) => [
        item.account_name,
        {
          account_name: item.account_name,
          enabled: item.enabled !== false,
        },
      ]),
  );
  return accountNames.map((accountName) => {
    const config = configMap.get(accountName);
    return config ?? {
      account_name: accountName,
      enabled: true,
    };
  });
}

function sameSchedulerAccountConfigs(left: SchedulerAccountConfig[], right: SchedulerAccountConfig[]) {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((item, index) =>
    item.account_name === right[index]?.account_name
    && item.enabled === right[index]?.enabled,
  );
}

function isPendingTaskStatus(status?: string | null) {
  return status === "queued" || status === "running";
}

function taskProgressTone(task: AnalysisTaskSummary | null): "running" | "success" | "danger" {
  if (!task || isPendingTaskStatus(task.status)) {
    return "running";
  }
  if (task.status === "success") {
    return "success";
  }
  return "danger";
}

function taskStatusMeta(task: AnalysisTaskSummary | null): { label: string; tone: "default" | "success" | "warning" | "danger" } {
  if (!task) {
    return { label: "未开始", tone: "default" };
  }
  if (task.status === "success") {
    return { label: "已完成", tone: "success" };
  }
  if (task.status === "failed" || task.status === "cancelled") {
    return { label: "失败", tone: "danger" };
  }
  if (task.status === "queued") {
    return { label: "排队中", tone: "warning" };
  }
  return { label: "进行中", tone: "warning" };
}

function taskCounterLabel(task: AnalysisTaskSummary | null) {
  const current = Number(task?.current ?? 0);
  const total = Number(task?.total ?? 0);
  if (total > 0) {
    return `${Math.max(0, current)}/${total}`;
  }
  return "进行中";
}

function summarizeHoldingsTask(task: AnalysisTaskSummary | null) {
  const result = task?.result;
  if (!result) {
    return null;
  }
  if (result.mode === "single") {
    return {
      total: 1,
      success: task?.status === "success" ? 1 : 0,
      failed: task?.status === "success" ? 0 : 1,
      saved: task?.status === "success" ? 1 : 0,
      failedSymbols: [] as string[],
    };
  }
  const rows = Array.isArray(result.results) ? result.results : [];
  const failedSymbols = rows
    .filter((item) => item && item.success === false)
    .map((item) => item.symbol || item.code || "")
    .filter(Boolean)
    .slice(0, 5);
  return {
    total: rows.length || Number(task?.total ?? 0),
    success: Number(result.success_count ?? 0),
    failed: Number(result.failed_count ?? 0),
    saved: Number(result.saved_count ?? 0),
    failedSymbols,
  };
}

function summarizeSchedulerTask(task: AnalysisTaskSummary | null) {
  const analysisResult = task?.result?.analysis_result;
  if (!analysisResult) {
    return null;
  }
  const failedSymbols = (analysisResult.failed_stocks ?? [])
    .map((item) => item.symbol || item.code || "")
    .filter(Boolean)
    .slice(0, 5);
  return {
    total: Number(analysisResult.total ?? task?.total ?? 0),
    success: Number(analysisResult.succeeded ?? 0),
    failed: Number(analysisResult.failed ?? 0),
    saved: Number(task?.result?.persistence_result?.saved_ids?.length ?? 0),
    accounts: analysisResult.accounts ?? [],
    trigger: task?.result?.trigger === "scheduled" ? "定时触发" : "手动触发",
    failedSymbols,
  };
}

export function PortfolioPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedAccount = usePortfolioStore((state) => state.selectedAccount);
  const knownAccounts = usePortfolioStore((state) => state.knownAccounts);
  const setSelectedAccount = usePortfolioStore((state) => state.setSelectedAccount);
  const setKnownAccounts = usePortfolioStore((state) => state.setKnownAccounts);
  const holdingsAnalysisTaskId = usePortfolioStore((state) => state.holdingsAnalysisTaskId);
  const schedulerTaskId = usePortfolioStore((state) => state.schedulerTaskId);
  const setHoldingsAnalysisTaskId = usePortfolioStore((state) => state.setHoldingsAnalysisTaskId);
  const setSchedulerTaskId = usePortfolioStore((state) => state.setSchedulerTaskId);
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
  const [schedulerTimes, setSchedulerTimes] = useState(() => cachedPage?.schedulerTimes ?? DEFAULT_SCHEDULER_TIME);
  const [schedulerMode, setSchedulerMode] = useState<"sequential" | "parallel">(
    () => (cachedPage?.scheduler as SchedulerStatus | null)?.analysis_mode === "parallel" ? "parallel" : "sequential",
  );
  const [schedulerMaxWorkers, setSchedulerMaxWorkers] = useState(
    () => (cachedPage?.scheduler as SchedulerStatus | null)?.max_workers ?? DEFAULT_SCHEDULER_WORKERS,
  );
  const [schedulerAnalysts, setSchedulerAnalysts] = useState<AnalystKey[]>(
    () => normalizeAnalystKeys((cachedPage?.scheduler as SchedulerStatus | null)?.selected_agents),
  );
  const [schedulerAccountConfigs, setSchedulerAccountConfigs] = useState<SchedulerAccountConfig[]>(
    () => ((cachedPage?.scheduler as SchedulerStatus | null)?.account_configs ?? []) as SchedulerAccountConfig[],
  );
  const [activeEditor, setActiveEditor] = useState<EditorPanel>(null);
  const [editingStockId, setEditingStockId] = useState<number | null>(null);
  const [isSubmittingHoldingsAnalysis, setIsSubmittingHoldingsAnalysis] = useState(false);
  const [isSubmittingSchedulerRun, setIsSubmittingSchedulerRun] = useState(false);
  const [holdingsAnalysisTask, setHoldingsAnalysisTask] = useState<AnalysisTaskSummary | null>(null);
  const [schedulerTask, setSchedulerTask] = useState<AnalysisTaskSummary | null>(null);
  const [section, setSection] = useState<SectionKey>("overview");
  const { message, error, clear, showError, showMessage } = usePageFeedback();
  const holdingsTerminalTaskRef = useRef<string>("");
  const schedulerTerminalTaskRef = useRef<string>("");

  const applySchedulerState = (schedulerData: SchedulerStatus | null) => {
    setScheduler(schedulerData);
    setSchedulerTimes((schedulerData?.schedule_times ?? [])[0] || DEFAULT_SCHEDULER_TIME);
    setSchedulerMode(schedulerData?.analysis_mode === "parallel" ? "parallel" : "sequential");
    setSchedulerMaxWorkers(schedulerData?.max_workers ?? DEFAULT_SCHEDULER_WORKERS);
    setSchedulerAnalysts(normalizeAnalystKeys(schedulerData?.selected_agents));
    setSchedulerAccountConfigs((schedulerData?.account_configs ?? []) as SchedulerAccountConfig[]);
  };

  const applyPageCache = (cache: PortfolioPageCache | null) => {
    if (!cache) {
      return;
    }
    setStocks(cache.stocks as PortfolioStock[]);
    setTrades(cache.trades as PortfolioTrade[]);
    setTradePage(cache.tradePage || 1);
    setTradeTotal(cache.tradeTotal || 0);
    setRisk((cache.risk as PortfolioRisk | null) ?? null);
    applySchedulerState((cache.scheduler as SchedulerStatus | null) ?? null);
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
    const fetchStocks = force ? apiFetch<PortfolioStock[]> : apiFetchCached<PortfolioStock[]>;
    const fetchRisk = force ? apiFetch<PortfolioRisk> : apiFetchCached<PortfolioRisk>;
    const [stockData, tradeData, riskData, schedulerData] = await Promise.all([
      fetchStocks(`/api/portfolio/stocks${buildQuery({ account_name: accountParam })}`),
      apiFetch<PortfolioTradePage>(
        `/api/portfolio/trades${buildQuery({
          account_name: accountParam,
          page: targetTradePage,
          page_size: TRADE_PAGE_SIZE,
        })}`,
      ),
      fetchRisk(`/api/portfolio/risk${buildQuery({ account_name: accountParam })}`),
      apiFetch<SchedulerStatus>("/api/portfolio/scheduler"),
    ]);

    setStocks(stockData);
    setTrades(tradeData.items);
    setTradePage(tradeData.page);
    setTradeTotal(tradeData.total);
    setRisk(riskData);
    applySchedulerState(schedulerData);
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
      schedulerTimes: (schedulerData?.schedule_times ?? [])[0] || DEFAULT_SCHEDULER_TIME,
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
    setActiveEditor("position");
    setSection("holdings");

    const nextParams = new URLSearchParams(searchParams);
    nextParams.delete("intent");
    setSearchParams(nextParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const clearHoldingsAnalysisTask = () => {
    setHoldingsAnalysisTaskId(null);
    setHoldingsAnalysisTask(null);
    holdingsTerminalTaskRef.current = "";
  };

  const clearSchedulerTask = () => {
    setSchedulerTaskId(null);
    setSchedulerTask(null);
    schedulerTerminalTaskRef.current = "";
  };

  const loadHoldingsAnalysisTask = async (taskIdOverride?: string | null) => {
    const taskId = taskIdOverride ?? holdingsAnalysisTaskId;
    if (!taskId) {
      setHoldingsAnalysisTask(null);
      return;
    }
    try {
      const task = await apiFetch<AnalysisTaskSummary>(`/api/tasks/${taskId}`);
      setHoldingsAnalysisTask(task);
    } catch (requestError) {
      if (requestError instanceof ApiRequestError && requestError.status === 404) {
        clearHoldingsAnalysisTask();
      }
    }
  };

  const loadSchedulerTask = async (taskIdOverride?: string | null) => {
    try {
      const taskId = taskIdOverride ?? schedulerTaskId;
      if (taskId) {
        const task = await apiFetch<AnalysisTaskSummary>(`/api/portfolio/scheduler/tasks/${taskId}`);
        setSchedulerTask(task);
        return;
      }

      const activeTask = await apiFetch<AnalysisTaskSummary | null>("/api/portfolio/scheduler/tasks/active");
      if (activeTask) {
        setSchedulerTask(activeTask);
        setSchedulerTaskId(activeTask.id);
        return;
      }
      setSchedulerTask(null);
    } catch (requestError) {
      if (requestError instanceof ApiRequestError && requestError.status === 404) {
        clearSchedulerTask();
      }
    }
  };

  const accountOptions = useMemo(() => {
    const values = new Set<string>([UI.allAccounts, UI.defaultAccount, selectedAccount, ...knownAccounts]);
    const cachedAllStocks = (allAccountsCache?.stocks as PortfolioStock[] | undefined) ?? [];
    cachedAllStocks.forEach((item) => values.add(item.account_name || UI.defaultAccount));
    stocks.forEach((item) => values.add(item.account_name || UI.defaultAccount));
    return Array.from(values);
  }, [allAccountsCache?.updatedAt, knownAccounts, selectedAccount, stocks]);

  const schedulerAccountOptions = useMemo(
    () => accountOptions.filter((item) => item !== UI.allAccounts),
    [accountOptions],
  );

  useEffect(() => {
    setSchedulerAccountConfigs((current) => {
      const next = normalizeSchedulerAccountConfigs(schedulerAccountOptions, current);
      return sameSchedulerAccountConfigs(current, next) ? current : next;
    });
  }, [schedulerAccountOptions]);

  usePollingLoader({
    load: loadHoldingsAnalysisTask,
    intervalMs: 2000,
    enabled: Boolean(holdingsAnalysisTaskId && isPendingTaskStatus(holdingsAnalysisTask?.status || "running")),
    immediate: true,
    dependencies: [holdingsAnalysisTaskId, holdingsAnalysisTask?.status],
  });

  usePollingLoader({
    load: loadSchedulerTask,
    intervalMs: 2500,
    enabled:
      Boolean(schedulerTaskId)
      || section === "scheduler"
      || isPendingTaskStatus(schedulerTask?.status || ""),
    immediate: true,
    dependencies: [schedulerTaskId, schedulerTask?.status, section],
  });

  useEffect(() => {
    if (!holdingsAnalysisTask || isPendingTaskStatus(holdingsAnalysisTask.status)) {
      return;
    }
    const terminalKey = `${holdingsAnalysisTask.id}:${holdingsAnalysisTask.status}`;
    if (holdingsTerminalTaskRef.current === terminalKey) {
      return;
    }
    holdingsTerminalTaskRef.current = terminalKey;
    void loadAll(true, tradePage);
  }, [holdingsAnalysisTask?.id, holdingsAnalysisTask?.status, tradePage]);

  useEffect(() => {
    if (!schedulerTask || isPendingTaskStatus(schedulerTask.status)) {
      return;
    }
    const terminalKey = `${schedulerTask.id}:${schedulerTask.status}`;
    if (schedulerTerminalTaskRef.current === terminalKey) {
      return;
    }
    schedulerTerminalTaskRef.current = terminalKey;
    void loadAll(true, tradePage);
  }, [schedulerTask?.id, schedulerTask?.status, tradePage]);

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

  const visibleHoldingSymbols = useMemo(
    () =>
      Array.from(
        new Set(
          stocks
            .map((item) => String(item.code || "").trim().toUpperCase())
            .filter(Boolean),
        ),
      ),
    [stocks],
  );

  const holdingsAnalysisBusy =
    isSubmittingHoldingsAnalysis
    || Boolean(holdingsAnalysisTaskId && !holdingsAnalysisTask)
    || isPendingTaskStatus(holdingsAnalysisTask?.status);
  const schedulerRunBusy =
    isSubmittingSchedulerRun
    || Boolean(schedulerTaskId && !schedulerTask)
    || isPendingTaskStatus(schedulerTask?.status);
  const holdingsTaskStatus = taskStatusMeta(holdingsAnalysisTask);
  const schedulerTaskStatus = taskStatusMeta(schedulerTask);
  const holdingsTaskSummary = summarizeHoldingsTask(holdingsAnalysisTask);
  const schedulerTaskSummary = summarizeSchedulerTask(schedulerTask);
  const holdingsRunLabel = isSubmittingHoldingsAnalysis
    ? "提交中..."
    : holdingsAnalysisBusy
      ? `分析中 ${taskCounterLabel(holdingsAnalysisTask)}`
      : `立即分析${selectedAccount === UI.allAccounts ? "全部账户" : "当前账户"}`;
  const schedulerRunLabel = isSubmittingSchedulerRun
    ? "提交中..."
    : schedulerRunBusy
      ? `执行中 ${taskCounterLabel(schedulerTask)}`
      : "立即执行";
  const enabledSchedulerAccounts = useMemo(
    () => schedulerAccountConfigs.filter((item) => item.enabled),
    [schedulerAccountConfigs],
  );

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

  const renderHoldingsAnalysisTask = () => {
    if (!holdingsAnalysisTaskId && !holdingsAnalysisTask) {
      return null;
    }
    return (
      <div className={styles.moduleSection}>
        <div className={styles.noticeMeta}>
          <div>
            <strong>持仓批量分析进度</strong>
            <div className={styles.muted}>{holdingsAnalysisTask?.label || "等待任务状态..."}</div>
          </div>
          <StatusBadge label={holdingsTaskStatus.label} tone={holdingsTaskStatus.tone} />
        </div>
        <TaskProgressBar
          current={holdingsAnalysisTask?.current ?? 0}
          message={holdingsAnalysisTask?.message || "等待持仓分析任务状态..."}
          tone={taskProgressTone(holdingsAnalysisTask)}
          total={holdingsTaskSummary?.total || holdingsAnalysisTask?.total || 0}
        />
        {holdingsTaskSummary ? (
          <div className={styles.summaryMetricGrid}>
            <div className={styles.metric}>
              <span className={styles.muted}>分析总数</span>
              <strong>{holdingsTaskSummary.total}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>成功</span>
              <strong>{holdingsTaskSummary.success}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>失败</span>
              <strong>{holdingsTaskSummary.failed}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>已写入历史</span>
              <strong>{holdingsTaskSummary.saved}</strong>
            </div>
          </div>
        ) : null}
        {holdingsAnalysisTask?.metadata ? (
          <p className={styles.helperText}>
            执行方式：{String(holdingsAnalysisTask.metadata.batch_mode || "顺序分析")}
            {Number(holdingsAnalysisTask.metadata.max_workers || 1) > 1 ? ` | 并发 ${Number(holdingsAnalysisTask.metadata.max_workers)}` : ""}
          </p>
        ) : null}
        {holdingsTaskSummary?.failedSymbols?.length ? (
          <p className={styles.dangerText}>失败股票：{holdingsTaskSummary.failedSymbols.join("、")}</p>
        ) : null}
        {holdingsAnalysisTask?.error ? <p className={styles.dangerText}>{holdingsAnalysisTask.error}</p> : null}
        <div className={styles.actions}>
          <button className={styles.secondaryButton} onClick={() => void loadHoldingsAnalysisTask()} type="button">
            刷新状态
          </button>
          {!isPendingTaskStatus(holdingsAnalysisTask?.status) ? (
            <button className={styles.secondaryButton} onClick={clearHoldingsAnalysisTask} type="button">
              清除状态
            </button>
          ) : null}
        </div>
      </div>
    );
  };

  const renderSchedulerTask = () => {
    if (!schedulerTaskId && !schedulerTask) {
      return null;
    }
    return (
      <div className={styles.moduleSection}>
        <div className={styles.noticeMeta}>
          <div>
            <strong>持仓分析任务进度</strong>
            <div className={styles.muted}>{schedulerTask?.label || "等待任务状态..."}</div>
          </div>
          <StatusBadge label={schedulerTaskStatus.label} tone={schedulerTaskStatus.tone} />
        </div>
        <TaskProgressBar
          current={schedulerTask?.current ?? 0}
          message={schedulerTask?.message || "等待持仓分析任务状态..."}
          tone={taskProgressTone(schedulerTask)}
          total={schedulerTaskSummary?.total || schedulerTask?.total || 0}
        />
        {schedulerTaskSummary ? (
          <div className={styles.summaryMetricGrid}>
            <div className={styles.metric}>
              <span className={styles.muted}>触发方式</span>
              <strong>{schedulerTaskSummary.trigger}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>分析总数</span>
              <strong>{schedulerTaskSummary.total}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>成功</span>
              <strong>{schedulerTaskSummary.success}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>失败</span>
              <strong>{schedulerTaskSummary.failed}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>已写入历史</span>
              <strong>{schedulerTaskSummary.saved}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>账户执行</span>
              <strong>{schedulerTaskSummary.accounts.length} 个账户</strong>
            </div>
          </div>
        ) : null}
        {schedulerTaskSummary?.accounts?.length ? (
          <p className={styles.helperText}>
            执行账户：{schedulerTaskSummary.accounts.map((item) => item.account_name).join("、")}
          </p>
        ) : null}
        {schedulerTaskSummary?.failedSymbols?.length ? (
          <p className={styles.dangerText}>失败股票：{schedulerTaskSummary.failedSymbols.join("、")}</p>
        ) : null}
        {schedulerTask?.error ? <p className={styles.dangerText}>{schedulerTask.error}</p> : null}
        <div className={styles.actions}>
          <button className={styles.secondaryButton} onClick={() => void loadSchedulerTask()} type="button">
            刷新状态
          </button>
          {!isPendingTaskStatus(schedulerTask?.status) ? (
            <button className={styles.secondaryButton} onClick={clearSchedulerTask} type="button">
              清除状态
            </button>
          ) : null}
        </div>
      </div>
    );
  };

  const resetPositionEditor = () => {
    setPositionForm(defaultPositionForm);
    setEditingStockId(null);
    setActiveEditor(null);
  };

  const openEditPosition = (stock: PortfolioStock) => {
    setPositionForm({
      code: stock.code || "",
      name: stock.name || "",
      account_name: stock.account_name || UI.defaultAccount,
      cost_price: stock.cost_price !== undefined ? String(stock.cost_price) : "",
      quantity: stock.quantity !== undefined ? String(stock.quantity) : "",
      note: stock.note || "",
      buy_date: "",
      auto_monitor: false,
      origin_analysis_id: stock.analysis_record_id,
    });
    setEditingStockId(stock.id);
    setActiveEditor("editPosition");
  };

  const runSelectedAccountAnalysis = async () => {
    if (!visibleHoldingSymbols.length) {
      showError(selectedAccount === UI.allAccounts ? "当前没有可分析的持仓股。" : `账户 ${selectedAccount} 当前没有可分析的持仓股。`);
      return;
    }

    clear();
    setIsSubmittingHoldingsAnalysis(true);
    try {
      const taskData = await apiFetch<{ task_id: string }>("/api/portfolio/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          account_name: selectedAccount === UI.allAccounts ? null : selectedAccount,
          batch_mode: normalizeSchedulerMode(scheduler?.analysis_mode) === "parallel" ? "多线程并行" : "顺序分析",
          max_workers: scheduler?.max_workers ?? DEFAULT_SCHEDULER_WORKERS,
          analysts: analystKeysToConfig(normalizeAnalystKeys(scheduler?.selected_agents)),
        }),
      });
      setHoldingsAnalysisTaskId(taskData.task_id);
      holdingsTerminalTaskRef.current = "";
      await loadHoldingsAnalysisTask(taskData.task_id);
      showMessage(
        `${selectedAccount === UI.allAccounts ? "全部账户" : `${selectedAccount} 账户`}的持仓批量分析任务已提交，共 ${visibleHoldingSymbols.length} 只股票。`,
      );
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "提交持仓批量分析失败");
    } finally {
      setIsSubmittingHoldingsAnalysis(false);
    }
  };

  const submitPosition = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    clear();
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
      setPositionForm(defaultPositionForm);
      setEditingStockId(null);
      setActiveEditor(null);
      showMessage(`持仓已新增：${positionForm.code}`);
      await loadAll(true, tradePage);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "新增持仓失败");
    }
  };

  const submitTrade = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!tradeForm.stock_id) {
      showError("请先选择对应持仓。");
      return;
    }

    clear();
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
      setTradeForm((current) => ({ ...defaultTradeForm, stock_id: current.stock_id }));
      setActiveEditor(null);
      showMessage("交易记录已保存。");
      await loadAll(true, tradePage);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "登记交易失败");
    }
  };

  const submitEditPosition = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!editingStockId) {
      showError("无法找到该持仓记录");
      return;
    }
    if (!positionForm.code.trim()) {
      showError("股票代码不能为空");
      return;
    }

    clear();
    try {
      await apiFetch(`/api/portfolio/stocks/${editingStockId}`, {
        method: "PATCH",
        body: JSON.stringify({
          code: positionForm.code.trim(),
          name: positionForm.name.trim() || positionForm.code.trim(),
          account_name: positionForm.account_name.trim() || UI.defaultAccount,
          cost_price: positionForm.cost_price ? Number(positionForm.cost_price) : null,
          quantity: positionForm.quantity ? Number(positionForm.quantity) : null,
          note: positionForm.note,
        }),
      });
      resetPositionEditor();
      showMessage(`持仓已更新：${positionForm.code.trim()}`);
      await loadAll(true, tradePage);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "修改持仓失败");
    }
  };

  const deletePositionAction = async (stockId: number) => {
    if (!window.confirm("确定要删除该持仓吗？交易记录将保留。")) {
      return;
    }
    clear();
    try {
      await apiFetch(`/api/portfolio/stocks/${stockId}`, {
        method: "DELETE",
      });
      if (editingStockId === stockId) {
        resetPositionEditor();
      }
      showMessage("持仓已删除。");
      await loadAll(true, tradePage);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "删除持仓失败");
    }
  };

  const updateSchedulerAccountConfig = (
    accountName: string,
    enabled: boolean,
  ) => {
    setSchedulerAccountConfigs((current) =>
      current.map((item) =>
        item.account_name === accountName
          ? {
            ...item,
            enabled,
          }
          : item,
      ),
    );
  };

  const saveScheduler = async () => {
    clear();
    if (!schedulerAnalysts.length) {
      showError("请至少选择一位分析师。");
      return;
    }
    try {
      const nextScheduler = await apiFetch<SchedulerStatus>("/api/portfolio/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          schedule_times: [schedulerTimes.trim() || DEFAULT_SCHEDULER_TIME],
          analysis_mode: schedulerMode,
          max_workers: schedulerMaxWorkers,
          selected_agents: schedulerAnalysts,
          account_configs: schedulerAccountConfigs.map((item) => ({
            account_name: item.account_name,
            enabled: item.enabled,
          })),
        }),
      });
      applySchedulerState(nextScheduler);
      showMessage("定时分析配置已更新。");
      await loadAll(true, tradePage);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "保存定时分析失败");
    }
  };

  const toggleScheduler = async (running: boolean) => {
    clear();
    try {
      const nextScheduler = await apiFetch<SchedulerStatus>(
        running ? "/api/portfolio/scheduler/start" : "/api/portfolio/scheduler/stop",
        { method: "POST" },
      );
      applySchedulerState(nextScheduler);
      showMessage(running ? "定时分析已启动。" : "定时分析已停止。");
      await loadAll(true, tradePage);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "更新定时分析状态失败");
    }
  };

  const runSchedulerNow = async () => {
    clear();
    setIsSubmittingSchedulerRun(true);
    try {
      const result = await apiFetch<{ task_id: string; task?: AnalysisTaskSummary | null }>("/api/portfolio/scheduler/run-once", { method: "POST" });
      setSchedulerTaskId(result.task_id);
      schedulerTerminalTaskRef.current = "";
      setSchedulerTask(result.task ?? null);
      await loadSchedulerTask(result.task_id);
      showMessage("已触发一次立即分析。");
      await loadAll(true, tradePage);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "立即执行失败");
    } finally {
      setIsSubmittingSchedulerRun(false);
    }
  };

  const renderPositionForm = (isEdit = false) => (
    <form className={styles.moduleSection} onSubmit={isEdit ? submitEditPosition : submitPosition}>
      {isEdit ? <p className={styles.helperText}>当前在表格内联编辑持仓，删除入口已收进当前表单。</p> : null}
      <div className={styles.formGrid}>
        <div className={styles.field}>
          <label htmlFor="position-code">股票代码</label>
          <input id="position-code" onChange={(event) => setPositionForm((current) => ({ ...current, code: event.target.value }))} value={positionForm.code} />
        </div>
        <div className={styles.field}>
          <label htmlFor="position-name">股票名称</label>
          <input id="position-name" onChange={(event) => setPositionForm((current) => ({ ...current, name: event.target.value }))} value={positionForm.name} />
        </div>
        <div className={styles.field}>
          <label htmlFor="position-account">账户</label>
          <input
            id="position-account"
            list="portfolio-account-options"
            onChange={(event) => setPositionForm((current) => ({ ...current, account_name: event.target.value }))}
            placeholder="输入新账号或选择已有账号"
            value={positionForm.account_name}
            disabled={isEdit}
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
          <input id="position-cost" onChange={(event) => setPositionForm((current) => ({ ...current, cost_price: event.target.value }))} value={positionForm.cost_price} />
        </div>
        <div className={styles.field}>
          <label htmlFor="position-quantity">数量</label>
          <input id="position-quantity" onChange={(event) => setPositionForm((current) => ({ ...current, quantity: event.target.value }))} value={positionForm.quantity} />
        </div>
        {!isEdit ? (
          <div className={styles.field}>
            <label htmlFor="position-date">买入日期</label>
            <input id="position-date" onChange={(event) => setPositionForm((current) => ({ ...current, buy_date: event.target.value }))} type="date" value={positionForm.buy_date} />
          </div>
        ) : null}
      </div>
      <div className={styles.field}>
        <label htmlFor="position-note">备注</label>
        <textarea id="position-note" onChange={(event) => setPositionForm((current) => ({ ...current, note: event.target.value }))} rows={3} value={positionForm.note} />
      </div>
      {!isEdit ? (
        <label className={styles.switchField}>
          <span className={styles.switchBody}>
            <span className={styles.switchLabel}>新增后默认启用智能盯盘</span>
            <span className={styles.switchDescription}>
              会同步创建托管盯盘任务和价格阈值，后续统一在智能盯盘列表中启用或停用，不再单独展示当前预警规则。关闭时任务仍会保留，但初始为停用状态。
            </span>
          </span>
          <span className={styles.switchControl}>
            <input
              checked={positionForm.auto_monitor}
              onChange={(event) => setPositionForm((current) => ({ ...current, auto_monitor: event.target.checked }))}
              type="checkbox"
            />
            <span aria-hidden="true" className={styles.switchTrack}>
              <span className={styles.switchThumb} />
            </span>
          </span>
        </label>
      ) : null}
      <div className={styles.actions}>
        <button className={styles.primaryButton} type="submit">{isEdit ? "保存修改" : "保存持仓"}</button>
        {isEdit && editingStockId ? (
          <button className={styles.dangerButton} onClick={() => void deletePositionAction(editingStockId)} type="button">
            {UI.deletePosition}
          </button>
        ) : null}
        <button className={styles.secondaryButton} onClick={resetPositionEditor} type="button">取消</button>
      </div>
    </form>
  );

  const renderTradeForm = () => (
    <form className={styles.moduleSection} onSubmit={submitTrade}>
      <div className={styles.formGrid}>
        <div className={styles.field}>
          <label htmlFor="trade-stock">选择持仓</label>
          <select id="trade-stock" onChange={(event) => setTradeForm((current) => ({ ...current, stock_id: event.target.value }))} value={tradeForm.stock_id}>
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
          <select
            id="trade-type"
            onChange={(event) => setTradeForm((current) => ({ ...current, trade_type: event.target.value as "buy" | "sell" | "clear" }))}
            value={tradeForm.trade_type}
          >
            <option value="buy">加仓</option>
            <option value="sell">减仓</option>
            <option value="clear">清仓</option>
          </select>
        </div>
        <div className={styles.field}>
          <label htmlFor="trade-quantity">数量</label>
          <input id="trade-quantity" onChange={(event) => setTradeForm((current) => ({ ...current, quantity: event.target.value }))} value={tradeForm.quantity} />
        </div>
        <div className={styles.field}>
          <label htmlFor="trade-price">价格</label>
          <input id="trade-price" onChange={(event) => setTradeForm((current) => ({ ...current, price: event.target.value }))} value={tradeForm.price} />
        </div>
        <div className={styles.field}>
          <label htmlFor="trade-date">交易日期</label>
          <input id="trade-date" onChange={(event) => setTradeForm((current) => ({ ...current, trade_date: event.target.value }))} type="date" value={tradeForm.trade_date} />
        </div>
        <div className={styles.field}>
          <label htmlFor="trade-note">备注</label>
          <input id="trade-note" onChange={(event) => setTradeForm((current) => ({ ...current, note: event.target.value }))} value={tradeForm.note} />
        </div>
      </div>
      <div className={styles.actions}>
        <button className={styles.primaryButton} type="submit">保存交易</button>
        <button className={styles.secondaryButton} onClick={() => setActiveEditor(null)} type="button">取消</button>
      </div>
    </form>
  );

  return (
    <PageFrame
      title={UI.title}
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        {section === "overview" ? (
          <ModuleCard
            title="持仓总览"
            summary="账户、风险提醒和持仓分布收敛到一个总览模块。"
            hideTitleOnMobile
            toolbar={(
              <button className={styles.secondaryButton} onClick={() => void loadAll(true, tradePage)} type="button">
                {UI.refresh}
              </button>
            )}
          >
            <div className={styles.moduleSection}>
              <div className={styles.field}>
                <label htmlFor="portfolio-account-overview">账户</label>
                {renderAccountSelect("portfolio-account-overview")}
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
                  <strong className={resolvePnlTone(risk?.total_pnl_pct, styles)}>{percentText(risk?.total_pnl_pct)}</strong>
                </div>
              </div>
            </div>

            <div className={styles.moduleSection}>
              <h3>风险提醒</h3>
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
            </div>

            <div className={styles.moduleSection}>
              <h3>行业分布</h3>
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
            </div>

            <div className={styles.moduleSection}>
              <h3>个股分布</h3>
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
            </div>
          </ModuleCard>
        ) : null}

        {section === "holdings" ? (
          <>
            <ModuleCard
              title="持仓操作"
              summary="新增持仓和账户上下文放在同一模块内。"
              toolbar={(
                <button
                  className={activeEditor === "position" ? styles.primaryButton : styles.secondaryButton}
                  onClick={() => {
                    if (activeEditor === "position") {
                      resetPositionEditor();
                      return;
                    }
                    setPositionForm(defaultPositionForm);
                    setEditingStockId(null);
                    setActiveEditor("position");
                  }}
                  type="button"
                >
                  {UI.addPosition}
                </button>
              )}
            >
              <div className={styles.moduleSection}>
                <div className={styles.responsiveActionGrid}>
                  <div className={styles.field}>
                    <label htmlFor="portfolio-account-holdings">账户</label>
                    {renderAccountSelect("portfolio-account-holdings")}
                  </div>
                  <button
                    className={styles.secondaryButton}
                    disabled={holdingsAnalysisBusy || !visibleHoldingSymbols.length}
                    onClick={() => void runSelectedAccountAnalysis()}
                    type="button"
                  >
                    {holdingsRunLabel}
                  </button>
                </div>
                <p className={styles.helperText}>
                  按已保存的定时分析配置批量提交当前账户持仓，当前共 {visibleHoldingSymbols.length} 只股票。
                </p>
                {renderHoldingsAnalysisTask()}
              </div>
              {activeEditor === "position" ? renderPositionForm(false) : null}
            </ModuleCard>

            <ModuleCard title="持仓数据" summary="列表与分析历史入口统一保留在数据模块内。">
              {activeEditor === "editPosition" ? renderPositionForm(true) : null}
              <div className={styles.moduleSection}>
                <div className={styles.tableWrap}>
                  <table className={`${styles.table} ${styles.tableCompact} ${styles.holdingsTableCompact}`}>
                    <thead>
                      <tr>
                        <th>股票</th>
                        <th>成本 / 数量</th>
                        <th>盈亏 / 收益率</th>
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
                              <button
                                className={styles.holdingSymbolButton}
                                onClick={() => openEditPosition(stock)}
                                title="点击修改持仓"
                                type="button"
                              >
                                <strong>{stock.name}</strong>
                                <span className={styles.holdingSymbolCode}>{stock.code}</span>
                              </button>
                            </td>
                            <td className={styles.holdingMetricCell}>
                              <div>{numberText(stock.cost_price)}</div>
                              <div>{stock.quantity ?? "N/A"}</div>
                            </td>
                            <td className={`${styles.holdingMetricCell} ${pnlClassName}`}>
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
                        );
                      })}
                      {!stocks.length ? (
                        <tr>
                          <td className={styles.muted} colSpan={4}>{UI.noHoldings}</td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
              </div>
            </ModuleCard>
          </>
        ) : null}

        {section === "trades" ? (
          <>
            <ModuleCard
              title="交易操作"
              summary="登记交易表单作为当前模块内的展开区，不再拆成额外卡片。"
              toolbar={(
                <button
                  className={activeEditor === "trade" ? styles.primaryButton : styles.secondaryButton}
                  onClick={() => setActiveEditor((current) => (current === "trade" ? null : "trade"))}
                  type="button"
                >
                  {UI.addTrade}
                </button>
              )}
            >
              {activeEditor === "trade" ? renderTradeForm() : (
                <div className={styles.moduleSection}>
                  <p className={styles.helperText}>登记后的流水会出现在完整交易列表中</p>
                </div>
              )}
            </ModuleCard>

            <ModuleCard title="交易列表" summary="交易记录和分页统一放在同一模块内。">
              <div className={styles.moduleSection}>
                <div className={styles.field}>
                  <label htmlFor="portfolio-account-trades">账户</label>
                  {renderAccountSelect("portfolio-account-trades")}
                </div>
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
              </div>
            </ModuleCard>
          </>
        ) : null}

        {section === "scheduler" ? (
          <ModuleCard hideTitleOnMobile title="定时分析" summary="执行时间、模式、分析师配置和运行状态统一收口到一个模块。">
            <SchedulerControl
              enabled={Boolean(scheduler?.is_running)}
              label="启用定时分析"
              onRunOnce={() => void runSchedulerNow()}
              runOnceDisabled={schedulerRunBusy}
              runOnceLabel={schedulerRunLabel}
              onSave={() => void saveScheduler()}
              onToggle={(next) => void toggleScheduler(next)}
              scheduleFields={(
                <>
                  <div className={styles.formGrid}>
                    <div className={styles.field}>
                      <label htmlFor="scheduler-times">执行时间</label>
                      <input
                        id="scheduler-times"
                        onChange={(event) => setSchedulerTimes(event.target.value)}
                        step="60"
                        type="time"
                        value={schedulerTimes}
                      />
                    </div>
                    <div className={styles.field}>
                      <label htmlFor="scheduler-mode">默认分析模式</label>
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
                        <label htmlFor="scheduler-workers">默认并发线程数</label>
                        <select
                          id="scheduler-workers"
                          onChange={(event) => setSchedulerMaxWorkers(Number(event.target.value) || DEFAULT_SCHEDULER_WORKERS)}
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
                  <div className={styles.field}>
                    <label>分析师配置</label>
                    <AnalystSelector
                      onChange={(next) => setSchedulerAnalysts(normalizeAnalystKeys(next))}
                      value={schedulerAnalysts}
                    />
                  </div>
                  <div className={styles.stack}>
                    <div>
                      <strong>分析账户</strong>
                      <p className={styles.helperText}>选择哪些账户参与定时分析。关闭后，该账户不会被定时任务和“立即执行”纳入。</p>
                    </div>
                    {schedulerAccountConfigs.map((config) => (
                      <label className={styles.switchField} key={config.account_name}>
                        <span className={styles.switchBody}>
                          <span className={styles.switchLabel}>{config.account_name}</span>
                          <span className={styles.switchDescription}>
                            {config.enabled ? "已纳入定时分析范围" : "已排除，不参与定时分析"}
                          </span>
                        </span>
                        <span className={styles.switchControl}>
                          <input
                            checked={config.enabled}
                            onChange={(event) => updateSchedulerAccountConfig(config.account_name, event.target.checked)}
                            type="checkbox"
                          />
                          <span aria-hidden="true" className={styles.switchTrack}>
                            <span className={styles.switchThumb} />
                          </span>
                        </span>
                      </label>
                    ))}
                  </div>
                </>
              )}
              statusFields={(
                <>
                  {renderSchedulerTask()}
                  <div className={styles.compactGrid}>
                    <div className={styles.metric}>
                      <span className={styles.muted}>运行状态</span>
                      <strong>{scheduler?.is_running ? "运行中" : "未启动"}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>执行时间</span>
                      <strong>{schedulerTimes || "未配置"}</strong>
                      <div className={styles.muted}>仅周一至周五执行</div>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>默认模式</span>
                      <strong>{schedulerModeLabel(scheduler?.analysis_mode)}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>默认并发</span>
                      <strong>{scheduler?.max_workers ?? DEFAULT_SCHEDULER_WORKERS}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>分析师配置</span>
                      <strong>
                        {normalizeAnalystKeys(scheduler?.selected_agents)
                          .map((item) => ANALYST_OPTIONS.find((option) => option.key === item)?.label || item)
                          .join("、")}
                      </strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>已启用账户</span>
                      <strong>{enabledSchedulerAccounts.length} 个账户</strong>
                      <div className={styles.muted}>
                        {enabledSchedulerAccounts.map((item) => item.account_name).join("、") || "未选择账户"}
                      </div>
                    </div>
                  </div>
                </>
              )}
            />
          </ModuleCard>
        ) : null}
      </div>
    </PageFrame>
  );
}
