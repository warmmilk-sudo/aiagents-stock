import { FormEvent, Fragment, lazy, memo, Suspense, useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
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
import {
  ALL_ACCOUNT_NAME,
  DEFAULT_ACCOUNT_NAME,
  normalizeAccountName,
  SUPPORTED_ACCOUNT_NAMES,
  supportedAccountOptions,
} from "../../lib/accounts";
import { formatDateTime } from "../../lib/datetime";
import { decodeIntent } from "../../lib/intents";
import { usePortfolioStore, type PortfolioPageCache } from "../../stores/portfolioStore";
import { useSmartMonitorStore } from "../../stores/smartMonitorStore";
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
  last_trade_at?: string;
}

interface PortfolioStockDistribution {
  account_name?: string;
  code?: string;
  name?: string;
  market_value?: number;
  cost_value?: number;
  pnl?: number;
  pnl_pct?: number;
  weight?: number;
  asset_weight?: number;
}

interface HoldingMetricSummary {
  pnl?: number;
  pnlPct?: number;
  marketValue?: number;
  assetWeight?: number;
  investedWeight?: number;
}

interface PortfolioRisk {
  status: string;
  message?: string;
  total_assets?: number;
  configured_total_assets?: number;
  total_market_value?: number;
  total_cost_value?: number;
  total_pnl?: number;
  total_pnl_pct?: number;
  available_cash?: number;
  position_usage_pct?: number;
  total_assets_configured?: boolean;
  position_size_limit_pct?: number;
  total_position_limit_pct?: number;
  risk_warnings?: string[];
  stock_distribution?: PortfolioStockDistribution[];
  industry_distribution?: Array<{
    industry?: string;
    market_value?: number;
    weight?: number;
    asset_weight?: number;
  }>;
}

interface SchedulerStatus {
  is_running: boolean;
  schedule_times?: string[];
  analysis_mode?: string;
  max_workers?: number;
  selected_agents?: string[];
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

interface PortfolioAnalysisHistoryItem {
  id: number;
  symbol?: string;
  stock_name?: string;
  analysis_time_text?: string;
  portfolio_state_label?: string;
  analysis_source_label?: string;
  summary?: string;
}

type EditorPanel = "position" | "editPosition" | "trade" | null;
type SectionKey = "overview" | "holdings" | "scheduler";
type HoldingActionPanel = "edit" | "trade" | null;

const sectionTabs = [
  { key: "overview", label: "总览" },
  { key: "holdings", label: "持仓列表" },
  { key: "scheduler", label: "定时分析" },
];

const UI = {
  title: "持仓分析",
  allAccounts: ALL_ACCOUNT_NAME,
  defaultAccount: DEFAULT_ACCOUNT_NAME,
  addPosition: "新增持仓",
  deletePosition: "删除",
  noWarnings: "当前没有风险提醒。",
  noHoldings: "当前没有持仓记录。",
};

const PAGE_CACHE_TTL_MS = 30_000;
const PIE_COLORS = ["#c65d4b", "#db7c57", "#d6a45f", "#7f9b6d", "#4f7c82", "#6f6d9b", "#9a5f7c", "#8b7d64"];
const PIE_CHART_OPTIONS = {
  responsive: true,
  maintainAspectRatio: false,
  cutout: "58%",
  plugins: { legend: { position: "bottom" as const } },
};

const LazyDoughnutChart = lazy(() =>
  import("../../components/common/DoughnutChart").then((module) => ({ default: module.DoughnutChart })),
);

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

const numberFormatters = new Map<number, Intl.NumberFormat>();

function getNumberFormatter(digits = 2) {
  const cachedFormatter = numberFormatters.get(digits);
  if (cachedFormatter) {
    return cachedFormatter;
  }

  const formatter = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: digits });
  numberFormatters.set(digits, formatter);
  return formatter;
}

function numberText(value: unknown, digits = 2) {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? getNumberFormatter(digits).format(numeric)
    : "N/A";
}

function percentText(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? `${getNumberFormatter(2).format(numeric * 100)}%`
    : "N/A";
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

function normalizeDateInput(value?: string | null) {
  if (!value) {
    return "";
  }
  return String(value).slice(0, 10);
}

function buildHoldingMetricKey(accountName?: string, code?: string, name?: string) {
  return `${normalizeAccountName(accountName) || UI.defaultAccount}::${code || ""}::${name || ""}`;
}

interface PortfolioDoughnutChartProps {
  data: {
    labels: string[];
    datasets: Array<{
      data: number[];
      backgroundColor: string[];
      borderWidth: number;
    }>;
  };
  emptyText: string;
}

const PortfolioDoughnutChart = memo(function PortfolioDoughnutChart({ data, emptyText }: PortfolioDoughnutChartProps) {
  if (!data.labels.length) {
    return <div className={styles.muted}>{emptyText}</div>;
  }

  return (
    <div className={styles.chartRingWrap}>
      <Suspense fallback={<div className={styles.muted}>图表加载中...</div>}>
        <LazyDoughnutChart data={data} options={PIE_CHART_OPTIONS} />
      </Suspense>
    </div>
  );
});

interface PortfolioHoldingRowProps {
  stock: PortfolioStock;
  selectedAccount: string;
  metrics?: HoldingMetricSummary;
  isMenuOpen: boolean;
  activeHoldingPanel: HoldingActionPanel;
  isEditingRow: boolean;
  isTradingRow: boolean;
  isHistoryLoading: boolean;
  inlineEditForm: ReactNode;
  inlineTradeForm: ReactNode;
  onToggleHoldingMenu: (stockId: number) => void;
  onOpenEditPosition: (stock: PortfolioStock) => void;
  onOpenTradeEditor: (stock: PortfolioStock) => void;
  onOpenDeepAnalysis: (stock: PortfolioStock) => void;
  onOpenHistoryAnalysis: (stock: PortfolioStock) => void;
}

function areHoldingRowPropsEqual(prev: PortfolioHoldingRowProps, next: PortfolioHoldingRowProps) {
  if (
    prev.stock !== next.stock
    || prev.selectedAccount !== next.selectedAccount
    || prev.metrics !== next.metrics
    || prev.isMenuOpen !== next.isMenuOpen
    || prev.isEditingRow !== next.isEditingRow
    || prev.isTradingRow !== next.isTradingRow
    || prev.isHistoryLoading !== next.isHistoryLoading
    || prev.inlineEditForm !== next.inlineEditForm
    || prev.inlineTradeForm !== next.inlineTradeForm
    || prev.onToggleHoldingMenu !== next.onToggleHoldingMenu
    || prev.onOpenEditPosition !== next.onOpenEditPosition
    || prev.onOpenTradeEditor !== next.onOpenTradeEditor
    || prev.onOpenDeepAnalysis !== next.onOpenDeepAnalysis
    || prev.onOpenHistoryAnalysis !== next.onOpenHistoryAnalysis
  ) {
    return false;
  }

  if (prev.isMenuOpen && prev.activeHoldingPanel !== next.activeHoldingPanel) {
    return false;
  }

  return true;
}

const PortfolioHoldingRow = memo(function PortfolioHoldingRow({
  stock,
  selectedAccount,
  metrics,
  isMenuOpen,
  activeHoldingPanel,
  isEditingRow,
  isTradingRow,
  isHistoryLoading,
  inlineEditForm,
  inlineTradeForm,
  onToggleHoldingMenu,
  onOpenEditPosition,
  onOpenTradeEditor,
  onOpenDeepAnalysis,
  onOpenHistoryAnalysis,
}: PortfolioHoldingRowProps) {
  const pnlClassName = resolvePnlTone(metrics?.pnl, styles);

  return (
    <Fragment>
      <tr
        className={`${styles.holdingRow} ${isMenuOpen ? styles.holdingRowActive : ""}`}
        onClick={() => onToggleHoldingMenu(stock.id)}
      >
        <td>
          <div className={styles.holdingSymbolCell}>
            <strong>{stock.name}</strong>
            <span className={styles.holdingSymbolCode}>{stock.code}</span>
            {selectedAccount === UI.allAccounts ? <span className={styles.muted}>{normalizeAccountName(stock.account_name) || UI.defaultAccount}</span> : null}
          </div>
        </td>
        <td className={styles.holdingMetricCell}>
          <div>{numberText(stock.cost_price)}</div>
          <div>{stock.quantity ?? "N/A"}</div>
        </td>
        <td className={styles.holdingMetricCell}>
          <div>{numberText(metrics?.marketValue)}</div>
          <div>{percentText(metrics?.assetWeight)}</div>
        </td>
        <td className={`${styles.holdingMetricCell} ${pnlClassName}`}>
          <div>{numberText(metrics?.pnl)}</div>
          <div>{percentText(metrics?.pnlPct)}</div>
        </td>
      </tr>
      {isMenuOpen ? (
        <tr className={styles.holdingActionMenuRow}>
          <td colSpan={4}>
            <div className={styles.holdingActionPanel} onClick={(event) => event.stopPropagation()}>
              <div className={styles.holdingActionMenu}>
                <button
                  className={`${styles.holdingActionMenuButton} ${activeHoldingPanel === "edit" ? styles.holdingActionMenuButtonActive : ""}`}
                  onClick={() => onOpenEditPosition(stock)}
                  type="button"
                >
                  修改
                </button>
                <button
                  className={`${styles.holdingActionMenuButton} ${activeHoldingPanel === "trade" ? styles.holdingActionMenuButtonActive : ""}`}
                  onClick={() => onOpenTradeEditor(stock)}
                  type="button"
                >
                  买入 / 卖出
                </button>
                <button className={styles.holdingActionMenuButton} onClick={() => onOpenDeepAnalysis(stock)} type="button">深度分析</button>
                <button
                  className={styles.holdingActionMenuButton}
                  disabled={isHistoryLoading}
                  onClick={() => void onOpenHistoryAnalysis(stock)}
                  type="button"
                >
                  {isHistoryLoading ? "读取中..." : "查看历史分析"}
                </button>
              </div>
              <div className={styles.holdingActionContent}>
                {isEditingRow ? inlineEditForm : null}
                {isTradingRow ? inlineTradeForm : null}
              </div>
            </div>
          </td>
        </tr>
      ) : null}
    </Fragment>
  );
}, areHoldingRowPropsEqual);

interface PortfolioHoldingsTableProps {
  stocks: PortfolioStock[];
  selectedAccount: string;
  holdingMetrics: Map<string, HoldingMetricSummary>;
  activeHoldingMenuId: number | null;
  activeHoldingPanel: HoldingActionPanel;
  editingStockId: number | null;
  tradeStockId: string;
  historyLoadingStockId: number | null;
  inlineEditForm: ReactNode;
  inlineTradeForm: ReactNode;
  onToggleHoldingMenu: (stockId: number) => void;
  onOpenEditPosition: (stock: PortfolioStock) => void;
  onOpenTradeEditor: (stock: PortfolioStock) => void;
  onOpenDeepAnalysis: (stock: PortfolioStock) => void;
  onOpenHistoryAnalysis: (stock: PortfolioStock) => void;
}

const PortfolioHoldingsTable = memo(function PortfolioHoldingsTable({
  stocks,
  selectedAccount,
  holdingMetrics,
  activeHoldingMenuId,
  activeHoldingPanel,
  editingStockId,
  tradeStockId,
  historyLoadingStockId,
  inlineEditForm,
  inlineTradeForm,
  onToggleHoldingMenu,
  onOpenEditPosition,
  onOpenTradeEditor,
  onOpenDeepAnalysis,
  onOpenHistoryAnalysis,
}: PortfolioHoldingsTableProps) {
  return (
    <div className={styles.tableWrap}>
      <table className={`${styles.table} ${styles.tableCompact} ${styles.holdingsTableCompact}`}>
        <thead>
          <tr>
            <th>股票</th>
            <th>成本 / 数量</th>
            <th>市值 / 占比</th>
            <th>盈亏 / 收益率</th>
          </tr>
        </thead>
        <tbody>
          {stocks.map((stock) => {
            const isMenuOpen = activeHoldingMenuId === stock.id;
            const isEditingRow = activeHoldingPanel === "edit" && editingStockId === stock.id;
            const isTradingRow = activeHoldingPanel === "trade" && String(stock.id) === tradeStockId;

            return (
              <PortfolioHoldingRow
                key={stock.id}
                stock={stock}
                selectedAccount={selectedAccount}
                metrics={holdingMetrics.get(buildHoldingMetricKey(stock.account_name, stock.code, stock.name))}
                isMenuOpen={isMenuOpen}
                activeHoldingPanel={activeHoldingPanel}
                isEditingRow={isEditingRow}
                isTradingRow={isTradingRow}
                isHistoryLoading={historyLoadingStockId === stock.id}
                inlineEditForm={isEditingRow ? inlineEditForm : null}
                inlineTradeForm={isTradingRow ? inlineTradeForm : null}
                onToggleHoldingMenu={onToggleHoldingMenu}
                onOpenEditPosition={onOpenEditPosition}
                onOpenTradeEditor={onOpenTradeEditor}
                onOpenDeepAnalysis={onOpenDeepAnalysis}
                onOpenHistoryAnalysis={onOpenHistoryAnalysis}
              />
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
  );
});

export function PortfolioPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedAccount = usePortfolioStore((state) => state.selectedAccount);
  const setSelectedAccount = usePortfolioStore((state) => state.setSelectedAccount);
  const setKnownAccounts = usePortfolioStore((state) => state.setKnownAccounts);
  const holdingsAnalysisTaskId = usePortfolioStore((state) => state.holdingsAnalysisTaskId);
  const schedulerTaskId = usePortfolioStore((state) => state.schedulerTaskId);
  const setHoldingsAnalysisTaskId = usePortfolioStore((state) => state.setHoldingsAnalysisTaskId);
  const setSchedulerTaskId = usePortfolioStore((state) => state.setSchedulerTaskId);
  const cachedPage = usePortfolioStore((state) => state.pageCacheByAccount[selectedAccount] ?? null);
  const setPageCache = usePortfolioStore((state) => state.setPageCache);
  const clearSmartMonitorPageCache = useSmartMonitorStore((state) => state.clearPageCache);

  const [stocks, setStocks] = useState<PortfolioStock[]>(() => (cachedPage?.stocks as PortfolioStock[]) ?? []);
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
  const [activeEditor, setActiveEditor] = useState<EditorPanel>(null);
  const [editingStockId, setEditingStockId] = useState<number | null>(null);
  const [activeHoldingMenuId, setActiveHoldingMenuId] = useState<number | null>(null);
  const [activeHoldingPanel, setActiveHoldingPanel] = useState<HoldingActionPanel>(null);
  const [historyLoadingStockId, setHistoryLoadingStockId] = useState<number | null>(null);
  const [isSubmittingHoldingsAnalysis, setIsSubmittingHoldingsAnalysis] = useState(false);
  const [isSubmittingPosition, setIsSubmittingPosition] = useState(false);
  const [isSubmittingTrade, setIsSubmittingTrade] = useState(false);
  const [isUpdatingPosition, setIsUpdatingPosition] = useState(false);
  const [deletingStockId, setDeletingStockId] = useState<number | null>(null);
  const [isSavingScheduler, setIsSavingScheduler] = useState(false);
  const [isTogglingScheduler, setIsTogglingScheduler] = useState(false);
  const [isRefreshingPage, setIsRefreshingPage] = useState(false);
  const [holdingsAnalysisTask, setHoldingsAnalysisTask] = useState<AnalysisTaskSummary | null>(null);
  const [schedulerTask, setSchedulerTask] = useState<AnalysisTaskSummary | null>(null);
  const [section, setSection] = useState<SectionKey>("overview");
  const { message, error, clear, showError, showMessage } = usePageFeedback();
  const holdingsTerminalTaskRef = useRef<string>("");
  const schedulerTerminalTaskRef = useRef<string>("");
  const selectedAccountRef = useRef(selectedAccount);
  const pageLoadRequestRef = useRef(0);
  selectedAccountRef.current = selectedAccount;

  const applySchedulerState = (schedulerData: SchedulerStatus | null) => {
    setScheduler(schedulerData);
    setSchedulerTimes((schedulerData?.schedule_times ?? [])[0] || DEFAULT_SCHEDULER_TIME);
    setSchedulerMode(schedulerData?.analysis_mode === "parallel" ? "parallel" : "sequential");
    setSchedulerMaxWorkers(schedulerData?.max_workers ?? DEFAULT_SCHEDULER_WORKERS);
    setSchedulerAnalysts(normalizeAnalystKeys(schedulerData?.selected_agents));
  };

  const setSchedulerRunningOptimistically = (running: boolean) => {
    setScheduler((current) =>
      current
        ? {
          ...current,
          is_running: running,
        }
        : {
          is_running: running,
        },
    );
  };

  const applyPageCache = (cache: PortfolioPageCache | null) => {
    if (!cache) {
      return;
    }
    setStocks(cache.stocks as PortfolioStock[]);
    setRisk((cache.risk as PortfolioRisk | null) ?? null);
    applySchedulerState((cache.scheduler as SchedulerStatus | null) ?? null);
  };

  const loadAll = async (force = false, options?: { background?: boolean }) => {
    if (!force && cachedPage && Date.now() - cachedPage.updatedAt < PAGE_CACHE_TTL_MS) {
      applyPageCache(cachedPage);
      return;
    }
    const accountKey = selectedAccount;
    const requestId = pageLoadRequestRef.current + 1;
    pageLoadRequestRef.current = requestId;
    selectedAccountRef.current = accountKey;
    setIsRefreshingPage(true);
    try {
      const accountParam = accountKey === UI.allAccounts ? "" : accountKey;
      const useFreshRequest = force || options?.background;
      const fetchStocks = useFreshRequest ? apiFetch<PortfolioStock[]> : apiFetchCached<PortfolioStock[]>;
      const fetchRisk = useFreshRequest ? apiFetch<PortfolioRisk> : apiFetchCached<PortfolioRisk>;
      const [stockData, riskData, schedulerData] = await Promise.all([
        fetchStocks(`/api/portfolio/stocks${buildQuery({ account_name: accountParam })}`),
        fetchRisk(`/api/portfolio/risk${buildQuery({ account_name: accountParam })}`),
        apiFetch<SchedulerStatus>("/api/portfolio/scheduler"),
      ]);

      if (pageLoadRequestRef.current !== requestId || selectedAccountRef.current !== accountKey) {
        return;
      }

      setStocks(stockData);
      setRisk(riskData);
      applySchedulerState(schedulerData);
      setKnownAccounts(
        stockData
          .map((item) => normalizeAccountName(item.account_name) || UI.defaultAccount)
          .concat(accountKey === UI.allAccounts ? [] : [accountKey]),
      );
      setPageCache(accountKey, {
        stocks: stockData,
        risk: riskData,
        scheduler: schedulerData,
        schedulerTimes: (schedulerData?.schedule_times ?? [])[0] || DEFAULT_SCHEDULER_TIME,
        updatedAt: Date.now(),
      });
      setTradeForm((current) => {
        if (current.stock_id || !stockData[0]) {
          return current;
        }
        return { ...current, stock_id: String(stockData[0].id) };
      });
    } finally {
      if (pageLoadRequestRef.current === requestId && selectedAccountRef.current === accountKey) {
        setIsRefreshingPage(false);
      }
    }
  };

  useEffect(() => {
    if (cachedPage) {
      applyPageCache(cachedPage);
    } else {
      setStocks([]);
      setRisk(null);
    }
    void loadAll(Boolean(cachedPage), { background: Boolean(cachedPage) }).catch(() => undefined);
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
      account_name: normalizeAccountName(payload.account_name) || UI.defaultAccount,
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
    return supportedAccountOptions(true);
  }, []);

  const schedulerAccountOptions = useMemo(() => [...SUPPORTED_ACCOUNT_NAMES], []);
  const schedulerTaskPending = isPendingTaskStatus(schedulerTask?.status || "");
  const schedulerPollingIntervalMs = schedulerTaskId || schedulerTaskPending
    ? 2500
    : section === "scheduler"
      ? 15000
      : null;

  usePollingLoader({
    load: loadHoldingsAnalysisTask,
    intervalMs: 2000,
    enabled: Boolean(holdingsAnalysisTaskId && isPendingTaskStatus(holdingsAnalysisTask?.status || "running")),
    immediate: true,
    dependencies: [holdingsAnalysisTaskId, holdingsAnalysisTask?.status],
  });

  usePollingLoader({
    load: loadSchedulerTask,
    intervalMs: schedulerPollingIntervalMs,
    enabled:
      Boolean(schedulerTaskId)
      || section === "scheduler"
      || schedulerTaskPending,
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
    void loadAll(true).catch(() => undefined);
    clearSmartMonitorPageCache();
  }, [clearSmartMonitorPageCache, holdingsAnalysisTask?.id, holdingsAnalysisTask?.status]);

  useEffect(() => {
    if (!schedulerTask || isPendingTaskStatus(schedulerTask.status)) {
      return;
    }
    const terminalKey = `${schedulerTask.id}:${schedulerTask.status}`;
    if (schedulerTerminalTaskRef.current === terminalKey) {
      return;
    }
    schedulerTerminalTaskRef.current = terminalKey;
    void loadAll(true).catch(() => undefined);
    clearSmartMonitorPageCache();
  }, [clearSmartMonitorPageCache, schedulerTask?.id, schedulerTask?.status]);

  const cachedUpdatedAtText = cachedPage?.updatedAt
    ? formatDateTime(cachedPage.updatedAt, "暂无缓存")
    : "暂无缓存";
  const pageDataStatus = isRefreshingPage
    ? { label: cachedPage ? "更新中" : "加载中", tone: "warning" as const }
    : cachedPage
      ? { label: "缓存可用", tone: "default" as const }
      : { label: "实时数据", tone: "default" as const };
  const handleManualRefresh = () => {
    clear();
    void loadAll(true).catch((requestError) => {
      showError(requestError instanceof ApiRequestError ? requestError.message : "刷新持仓数据失败");
    });
  };

  const riskWarnings = risk?.risk_warnings ?? [];
  const holdingMetrics = useMemo(() => {
    const distribution = risk?.stock_distribution ?? [];
    return new Map<string, HoldingMetricSummary>(
      distribution.map((item) => [
        buildHoldingMetricKey(item.account_name, item.code, item.name),
        {
          pnl: item.pnl,
          pnlPct: item.pnl_pct,
          marketValue: item.market_value,
          assetWeight: item.asset_weight,
          investedWeight: item.weight,
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
  const holdingsTaskStatus = taskStatusMeta(holdingsAnalysisTask);
  const schedulerTaskStatus = taskStatusMeta(schedulerTask);
  const holdingsTaskSummary = summarizeHoldingsTask(holdingsAnalysisTask);
  const schedulerTaskSummary = summarizeSchedulerTask(schedulerTask);
  const holdingsRunLabel = isSubmittingHoldingsAnalysis
    ? "提交中..."
    : holdingsAnalysisBusy
      ? `分析中 ${taskCounterLabel(holdingsAnalysisTask)}`
      : `深度分析${selectedAccount === UI.allAccounts ? "全部账户" : "当前账户"}`;
  const schedulerCoverageAccounts = useMemo(() => {
    const holdingAccounts = stocks
      .map((item) => normalizeAccountName(item.account_name))
      .filter((item) => Boolean(item) && item !== UI.allAccounts) as string[];
    return Array.from(new Set([...(holdingAccounts.length ? holdingAccounts : []), ...SUPPORTED_ACCOUNT_NAMES]));
  }, [stocks]);
  const schedulerAnalystLabels = useMemo(
    () =>
      normalizeAnalystKeys(scheduler?.selected_agents)
        .map((item) => ANALYST_OPTIONS.find((option) => option.key === item)?.label || item),
    [scheduler?.selected_agents],
  );

  const renderAccountSelect = (id: string) => (
    <select
      id={id}
      value={selectedAccount}
      onChange={(event) => setSelectedAccount(event.target.value)}
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

  const closeHoldingPanel = useCallback(() => {
    setActiveHoldingMenuId(null);
    setActiveHoldingPanel(null);
    setEditingStockId(null);
    setActiveEditor((current) => current === "position" ? current : null);
  }, []);

  const openEditPosition = useCallback((stock: PortfolioStock) => {
    setPositionForm({
      code: stock.code || "",
      name: stock.name || "",
      account_name: stock.account_name || UI.defaultAccount,
      cost_price: stock.cost_price !== undefined ? String(stock.cost_price) : "",
      quantity: stock.quantity !== undefined ? String(stock.quantity) : "",
      note: stock.note || "",
      buy_date: normalizeDateInput(stock.last_trade_at),
      auto_monitor: false,
      origin_analysis_id: stock.analysis_record_id,
    });
    setEditingStockId(stock.id);
    setActiveEditor("editPosition");
    setActiveHoldingMenuId(stock.id);
    setActiveHoldingPanel("edit");
  }, []);

  const openTradeEditor = useCallback((stock: PortfolioStock) => {
    setTradeForm({
      ...defaultTradeForm,
      stock_id: String(stock.id),
      trade_type: "buy",
    });
    setActiveEditor("trade");
    setActiveHoldingMenuId(stock.id);
    setActiveHoldingPanel("trade");
  }, []);

  const openDeepAnalysis = useCallback((stock: PortfolioStock) => {
    closeHoldingPanel();
    navigate(`/research/deep-analysis?symbol=${encodeURIComponent(stock.code)}`);
  }, [closeHoldingPanel, navigate]);

  const openHistoryAnalysis = useCallback(async (stock: PortfolioStock) => {
    setActiveEditor((current) => current === "position" ? null : current);
    setHistoryLoadingStockId(stock.id);
    try {
      const items = await apiFetch<PortfolioAnalysisHistoryItem[]>(`/api/portfolio/stocks/${stock.id}/history?limit=1`);
      const latestRecord = items[0];
      if (!latestRecord?.id) {
        showMessage(`${stock.code} 暂无持仓分析历史`);
        return;
      }
      navigate(`/research/history?recordId=${latestRecord.id}`);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "读取历史分析失败");
    } finally {
      setHistoryLoadingStockId(null);
    }
  }, [navigate, showError, showMessage]);

  const toggleHoldingMenu = useCallback((stockId: number) => {
    setActiveHoldingMenuId((current) => {
      if (current === stockId) {
        setActiveHoldingPanel(null);
        setEditingStockId(null);
        setActiveEditor((currentEditor) => currentEditor === "position" ? currentEditor : null);
        return null;
      }
      setActiveEditor((currentEditor) => currentEditor === "position" ? null : currentEditor);
      setActiveHoldingPanel(null);
      return stockId;
    });
  }, []);

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
      void loadHoldingsAnalysisTask(taskData.task_id).catch(() => undefined);
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
    setIsSubmittingPosition(true);
    const nextPosition = {
      id: -Date.now(),
      code: positionForm.code.trim(),
      name: positionForm.name.trim() || positionForm.code.trim(),
      account_name: normalizeAccountName(positionForm.account_name) || UI.defaultAccount,
      cost_price: positionForm.cost_price ? Number(positionForm.cost_price) : undefined,
      quantity: positionForm.quantity ? Number(positionForm.quantity) : undefined,
      note: positionForm.note,
      analysis_record_id: positionForm.origin_analysis_id,
      last_trade_at: positionForm.buy_date || undefined,
    } satisfies PortfolioStock;
    setStocks((current) => [nextPosition, ...current]);
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
      closeHoldingPanel();
      showMessage(`持仓已新增：${positionForm.code}`);
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      setStocks((current) => current.filter((item) => item.id !== nextPosition.id));
      showError(requestError instanceof ApiRequestError ? requestError.message : "新增持仓失败");
    } finally {
      setIsSubmittingPosition(false);
    }
  };

  const submitTrade = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!tradeForm.stock_id) {
      showError("请先选择对应持仓。");
      return;
    }

    clear();
    setIsSubmittingTrade(true);
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
      closeHoldingPanel();
      showMessage("交易记录已保存。");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "登记交易失败");
    } finally {
      setIsSubmittingTrade(false);
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
    setIsUpdatingPosition(true);
    const nextCode = positionForm.code.trim();
    const nextName = positionForm.name.trim() || nextCode;
    const nextAccount = normalizeAccountName(positionForm.account_name) || UI.defaultAccount;
    const nextCostPrice = positionForm.cost_price ? Number(positionForm.cost_price) : undefined;
    const nextQuantity = positionForm.quantity ? Number(positionForm.quantity) : undefined;
    const nextBuyDate = positionForm.buy_date || undefined;
    const previousStock = stocks.find((item) => item.id === editingStockId) ?? null;
    setStocks((current) =>
      current.map((stock) =>
        stock.id === editingStockId
          ? {
            ...stock,
            code: nextCode,
            name: nextName,
            account_name: nextAccount,
            cost_price: nextCostPrice,
            quantity: nextQuantity,
            note: positionForm.note,
            last_trade_at: nextBuyDate ?? stock.last_trade_at,
          }
          : stock,
      ),
    );
    try {
      await apiFetch(`/api/portfolio/stocks/${editingStockId}`, {
        method: "PATCH",
        body: JSON.stringify({
          code: nextCode,
          name: nextName,
          account_name: nextAccount,
          cost_price: nextCostPrice ?? null,
          quantity: nextQuantity ?? null,
          note: positionForm.note,
          buy_date: nextBuyDate ?? null,
        }),
      });
      resetPositionEditor();
      closeHoldingPanel();
      showMessage(`持仓已更新：${nextCode}`);
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      if (previousStock) {
        setStocks((current) =>
          current.map((stock) =>
            stock.id === previousStock.id
              ? previousStock
              : stock,
          ),
        );
      }
      showError(requestError instanceof ApiRequestError ? requestError.message : "修改持仓失败");
    } finally {
      setIsUpdatingPosition(false);
    }
  };

  const deletePositionAction = async (stockId: number) => {
    if (!window.confirm("确定要删除该持仓吗？交易记录将保留。")) {
      return;
    }
    clear();
    setDeletingStockId(stockId);
    const removedIndex = stocks.findIndex((item) => item.id === stockId);
    const removedStock = stocks[removedIndex] ?? null;
    setStocks((current) => current.filter((item) => item.id !== stockId));
    if (editingStockId === stockId) {
      resetPositionEditor();
    }
    if (activeHoldingMenuId === stockId) {
      closeHoldingPanel();
    }
    try {
      await apiFetch(`/api/portfolio/stocks/${stockId}`, {
        method: "DELETE",
      });
      showMessage("持仓已删除。");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      if (removedStock) {
        setStocks((current) => {
          if (current.some((item) => item.id === removedStock.id)) {
            return current;
          }
          const next = [...current];
          next.splice(Math.max(0, Math.min(removedIndex, next.length)), 0, removedStock);
          return next;
        });
      }
      showError(requestError instanceof ApiRequestError ? requestError.message : "删除持仓失败");
    } finally {
      setDeletingStockId((current) => current === stockId ? null : current);
    }
  };

  const saveScheduler = async () => {
    clear();
    if (!schedulerAnalysts.length) {
      showError("请至少选择一位分析师。");
      return;
    }
    setIsSavingScheduler(true);
    try {
      const nextScheduler = await apiFetch<SchedulerStatus>("/api/portfolio/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          schedule_times: [schedulerTimes.trim() || DEFAULT_SCHEDULER_TIME],
          analysis_mode: schedulerMode,
          max_workers: schedulerMaxWorkers,
          selected_agents: schedulerAnalysts,
          account_configs: schedulerAccountOptions.map((accountName) => ({
            account_name: accountName,
            enabled: true,
          })),
        }),
      });
      applySchedulerState(nextScheduler);
      showMessage("定时分析配置已更新。");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "保存定时分析失败");
    } finally {
      setIsSavingScheduler(false);
    }
  };

  const toggleScheduler = async (running: boolean) => {
    clear();
    if (isTogglingScheduler) {
      return;
    }
    const previousScheduler = scheduler;
    setIsTogglingScheduler(true);
    setSchedulerRunningOptimistically(running);
    try {
      const nextScheduler = await apiFetch<SchedulerStatus>(
        running ? "/api/portfolio/scheduler/start" : "/api/portfolio/scheduler/stop",
        { method: "POST" },
      );
      applySchedulerState(nextScheduler);
      showMessage(running ? "定时分析已启动。" : "定时分析已停止。");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      setScheduler(previousScheduler);
      showError(requestError instanceof ApiRequestError ? requestError.message : "更新定时分析状态失败");
    } finally {
      setIsTogglingScheduler(false);
    }
  };

  const renderPositionForm = (isEdit = false) => (
    <form className={styles.moduleSection} onSubmit={isEdit ? submitEditPosition : submitPosition}>
      {isEdit ? <p className={styles.helperText}>当前通过持仓操作菜单编辑该记录，删除入口已收进当前表单。</p> : null}
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
          <select
            id="position-account"
            onChange={(event) => setPositionForm((current) => ({ ...current, account_name: event.target.value }))}
            value={positionForm.account_name}
            disabled={isEdit}
          >
            {SUPPORTED_ACCOUNT_NAMES.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
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
        <button className={styles.primaryButton} disabled={isEdit ? isUpdatingPosition : isSubmittingPosition} type="submit">
          {isEdit ? (isUpdatingPosition ? "保存中..." : "保存修改") : (isSubmittingPosition ? "保存中..." : "保存持仓")}
        </button>
        {isEdit && editingStockId ? (
          <button
            className={styles.dangerButton}
            disabled={deletingStockId === editingStockId}
            onClick={() => void deletePositionAction(editingStockId)}
            type="button"
          >
            {deletingStockId === editingStockId ? "删除中..." : UI.deletePosition}
          </button>
        ) : null}
        <button className={styles.secondaryButton} onClick={resetPositionEditor} type="button">取消</button>
      </div>
    </form>
  );

  const renderInlineEditForm = () => (
    <form className={styles.holdingInlineForm} onSubmit={submitEditPosition}>
      <div className={styles.formGrid}>
        <div className={styles.field}>
          <label htmlFor="inline-position-account">账户</label>
          <select
            id="inline-position-account"
            onChange={(event) => setPositionForm((current) => ({ ...current, account_name: event.target.value }))}
            value={positionForm.account_name}
          >
            {SUPPORTED_ACCOUNT_NAMES.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </div>
        <div className={styles.field}>
          <label htmlFor="inline-position-quantity">数量</label>
          <input
            id="inline-position-quantity"
            onChange={(event) => setPositionForm((current) => ({ ...current, quantity: event.target.value }))}
            value={positionForm.quantity}
          />
        </div>
        <div className={styles.field}>
          <label htmlFor="inline-position-price">价格</label>
          <input
            id="inline-position-price"
            onChange={(event) => setPositionForm((current) => ({ ...current, cost_price: event.target.value }))}
            value={positionForm.cost_price}
          />
        </div>
        <div className={styles.field}>
          <label htmlFor="inline-position-date">日期</label>
          <input
            id="inline-position-date"
            onChange={(event) => setPositionForm((current) => ({ ...current, buy_date: event.target.value }))}
            type="date"
            value={positionForm.buy_date}
          />
        </div>
        <div className={styles.field}>
          <label htmlFor="inline-position-note">备注</label>
          <input
            id="inline-position-note"
            onChange={(event) => setPositionForm((current) => ({ ...current, note: event.target.value }))}
            value={positionForm.note}
          />
        </div>
      </div>
      <div className={styles.actions}>
        <button className={styles.primaryButton} disabled={isUpdatingPosition} type="submit">
          {isUpdatingPosition ? "保存中..." : "保存修改"}
        </button>
        {editingStockId ? (
          <button
            className={styles.dangerButton}
            disabled={deletingStockId === editingStockId}
            onClick={() => void deletePositionAction(editingStockId)}
            type="button"
          >
            {deletingStockId === editingStockId ? "删除中..." : UI.deletePosition}
          </button>
        ) : null}
        <button className={styles.secondaryButton} onClick={closeHoldingPanel} type="button">取消</button>
      </div>
    </form>
  );

  const renderInlineTradeForm = () => (
    <form className={styles.holdingInlineForm} onSubmit={submitTrade}>
      <div className={styles.dualToggleGrid}>
        <button
          className={tradeForm.trade_type === "buy" ? styles.primaryButton : styles.secondaryButton}
          onClick={() => setTradeForm((current) => ({ ...current, trade_type: "buy" }))}
          type="button"
        >
          买入
        </button>
        <button
          className={tradeForm.trade_type === "sell" ? styles.primaryButton : styles.secondaryButton}
          onClick={() => setTradeForm((current) => ({ ...current, trade_type: "sell" }))}
          type="button"
        >
          卖出
        </button>
      </div>
      <div className={styles.formGrid}>
        <div className={styles.field}>
          <label htmlFor="inline-trade-quantity">数量</label>
          <input
            id="inline-trade-quantity"
            onChange={(event) => setTradeForm((current) => ({ ...current, quantity: event.target.value }))}
            value={tradeForm.quantity}
          />
        </div>
        <div className={styles.field}>
          <label htmlFor="inline-trade-price">价格</label>
          <input
            id="inline-trade-price"
            onChange={(event) => setTradeForm((current) => ({ ...current, price: event.target.value }))}
            value={tradeForm.price}
          />
        </div>
        <div className={styles.field}>
          <label htmlFor="inline-trade-date">日期</label>
          <input
            id="inline-trade-date"
            onChange={(event) => setTradeForm((current) => ({ ...current, trade_date: event.target.value }))}
            type="date"
            value={tradeForm.trade_date}
          />
        </div>
        <div className={styles.field}>
          <label htmlFor="inline-trade-note">备注</label>
          <input
            id="inline-trade-note"
            onChange={(event) => setTradeForm((current) => ({ ...current, note: event.target.value }))}
            value={tradeForm.note}
          />
        </div>
      </div>
      <div className={styles.actions}>
        <button className={styles.primaryButton} disabled={isSubmittingTrade} type="submit">
          {isSubmittingTrade ? "保存中..." : "保存交易"}
        </button>
        <button className={styles.secondaryButton} onClick={closeHoldingPanel} type="button">取消</button>
      </div>
    </form>
  );

  const inlineEditForm = activeHoldingPanel === "edit" && editingStockId ? renderInlineEditForm() : null;
  const inlineTradeForm = activeHoldingPanel === "trade" && tradeForm.stock_id ? renderInlineTradeForm() : null;

  return (
    <PageFrame
      title={UI.title}
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      actions={(
        <>
          <button
            className={styles.secondaryButton}
            disabled={isRefreshingPage}
            onClick={handleManualRefresh}
            type="button"
          >
            {isRefreshingPage ? "更新中..." : "刷新"}
          </button>
        </>
      )}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        {section === "overview" ? (
          <ModuleCard
            title="持仓总览"
            summary="账户总资产、风险提醒和持仓分布收敛到一个总览模块。"
            hideTitleOnMobile
          >
            <div className={styles.moduleSection}>
              <div className={styles.field}>
                <label htmlFor="portfolio-account-overview">账户</label>
                {renderAccountSelect("portfolio-account-overview")}
              </div>
              <div className={styles.summaryMetricGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>总资产</span>
                  <strong>{numberText(risk?.total_assets)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>持仓市值</span>
                  <strong>{numberText(risk?.total_market_value)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>可用资金</span>
                  <strong>{numberText(risk?.available_cash)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>仓位利用率</span>
                  <strong>{percentText(risk?.position_usage_pct)}</strong>
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
              <PortfolioDoughnutChart data={industryPieData} emptyText="暂无行业分布数据" />
            </div>

            <div className={styles.moduleSection}>
              <h3>个股分布</h3>
              <PortfolioDoughnutChart data={stockPieData} emptyText="暂无个股分布数据" />
            </div>
          </ModuleCard>
        ) : null}

        {section === "holdings" ? (
          <>
            <ModuleCard
              title="持仓操作"
              summary="新增持仓、买卖登记和批量分析统一收在一个入口。"
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
                {renderHoldingsAnalysisTask()}
              </div>
              {activeEditor === "position" ? renderPositionForm(false) : null}
            </ModuleCard>

            <ModuleCard title="持仓数据" summary="点击任一股票行展开横向操作菜单，处理修改、交易和分析。">
              <div className={styles.moduleSection}>
                <PortfolioHoldingsTable
                  stocks={stocks}
                  selectedAccount={selectedAccount}
                  holdingMetrics={holdingMetrics}
                  activeHoldingMenuId={activeHoldingMenuId}
                  activeHoldingPanel={activeHoldingPanel}
                  editingStockId={editingStockId}
                  tradeStockId={tradeForm.stock_id}
                  historyLoadingStockId={historyLoadingStockId}
                  inlineEditForm={inlineEditForm}
                  inlineTradeForm={inlineTradeForm}
                  onToggleHoldingMenu={toggleHoldingMenu}
                  onOpenEditPosition={openEditPosition}
                  onOpenTradeEditor={openTradeEditor}
                  onOpenDeepAnalysis={openDeepAnalysis}
                  onOpenHistoryAnalysis={openHistoryAnalysis}
                />
              </div>
            </ModuleCard>
          </>
        ) : null}

        {section === "scheduler" ? (
          <ModuleCard hideTitleOnMobile title="定时分析" summary="执行时间、模式、分析师配置和运行状态统一收口到一个模块。">
            <SchedulerControl
              enabled={Boolean(scheduler?.is_running)}
              label="启用定时分析"
              busy={isSavingScheduler || isTogglingScheduler}
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
                  <div className={styles.noticeCard}>
                    <div className={styles.noticeMeta}>
                      <strong>分析范围</strong>
                      <StatusBadge label="自动覆盖" tone="default" />
                    </div>
                    <div>当前不再单独设置启用账户，定时分析会默认覆盖全部持仓账户。</div>
                    <div className={styles.muted}>{schedulerCoverageAccounts.join("、")}</div>
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
                      <strong>{schedulerAnalystLabels.join("、") || "未配置"}</strong>
                    </div>
                    <div className={styles.metric}>
                      <span className={styles.muted}>覆盖账户</span>
                      <strong>{schedulerCoverageAccounts.length} 个账户</strong>
                      <div className={styles.muted}>
                        {schedulerCoverageAccounts.join("、")}
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
