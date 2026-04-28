import { FormEvent, Fragment, memo, useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { ModuleCard } from "../../components/common/ModuleCard";
import { DoughnutChart } from "../../components/common/DoughnutChart";
import { PageFeedback } from "../../components/common/PageFeedback";
import { SchedulerControl } from "../../components/common/SchedulerControl";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import { analystConfigToKeys, analystKeysToConfig, normalizeAnalystKeys } from "../../constants/analysts";
import { DEFAULT_SCHEDULER_TIME } from "../../constants/scheduler";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { usePollingLoader } from "../../hooks/usePollingLoader";
import { useSelectedModels } from "../../hooks/useSelectedModels";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch, apiFetchCached } from "../../lib/api";
import { decodeIntent } from "../../lib/intents";
import { useDeepAnalysisStore } from "../../stores/deepAnalysisStore";
import { usePortfolioStore, type PortfolioPageCache } from "../../stores/portfolioStore";
import { useSmartMonitorStore } from "../../stores/smartMonitorStore";
import styles from "../ConsolePage.module.scss";

interface PortfolioStock {
  id: number;
  code: string;
  name: string;
  account_name?: string;
  cost_price?: number;
  current_price?: number;
  market_value?: number;
  cost_value?: number;
  pnl?: number;
  pnl_pct?: number;
  weight?: number;
  asset_weight?: number;
  industry?: string;
  quantity?: number;
  note?: string;
  analysis_record_id?: number;
  last_trade_at?: string;
}

interface PortfolioStockDistribution {
  stock_id?: number;
  account_name?: string;
  code?: string;
  name?: string;
  current_price?: number;
  market_value?: number;
  cost_value?: number;
  pnl?: number;
  pnl_pct?: number;
  weight?: number;
  asset_weight?: number;
}

interface HoldingMetricSummary {
  currentPrice?: number;
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
  task_type?: string;
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
type HoldingActionPanel = "edit" | "trade" | null;

const UI = {
  title: "持仓列表",
  addPosition: "新增持仓",
  deletePosition: "删除",
  noWarnings: "当前没有风险提醒。",
  noHoldings: "当前没有持仓记录。",
};

const PAGE_CACHE_TTL_MS = 30_000;
const RISK_REFRESH_URL = "/api/portfolio/risk?refresh=1";
const PIE_COLORS = ["#c65d4b", "#db7c57", "#d6a45f", "#7f9b6d", "#4f7c82", "#6f6d9b", "#9a5f7c", "#8b7d64"];
const PIE_CHART_OPTIONS = {
  responsive: true,
  maintainAspectRatio: false,
  cutout: "58%",
  plugins: { legend: { position: "bottom" as const } },
};

const defaultPositionForm = {
  code: "",
  name: "",
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

function todayDateInput() {
  return new Date().toLocaleDateString("en-CA");
}

function buildHoldingMetricKey(stockId?: number | null, code?: string, name?: string) {
  if (typeof stockId === "number" && Number.isFinite(stockId) && stockId > 0) {
    return `id:${stockId}`;
  }
  return `code:${code || ""}::${name || ""}`;
}

function mergeStocksWithRiskMetrics(stocks: PortfolioStock[], riskData: PortfolioRisk | null) {
  if (!stocks.length) {
    return [];
  }

  const distribution = riskData?.stock_distribution ?? [];
  if (!distribution.length) {
    return stocks.map((stock) => ({ ...stock }));
  }

  const metricsMap = new Map<string, PortfolioStockDistribution>();
  distribution.forEach((item) => {
    metricsMap.set(buildHoldingMetricKey(item.stock_id, item.code, item.name), item);
    metricsMap.set(buildHoldingMetricKey(null, item.code, item.name), item);
  });

  return stocks.map((stock) => {
    const metrics = (
      metricsMap.get(buildHoldingMetricKey(stock.id, stock.code, stock.name))
      ?? metricsMap.get(buildHoldingMetricKey(null, stock.code, stock.name))
    );
    if (!metrics) {
      return { ...stock };
    }
    return {
      ...stock,
      current_price: metrics.current_price ?? stock.current_price,
      market_value: metrics.market_value ?? stock.market_value,
      cost_value: metrics.cost_value ?? stock.cost_value,
      pnl: metrics.pnl ?? stock.pnl,
      pnl_pct: metrics.pnl_pct ?? stock.pnl_pct,
      weight: metrics.weight ?? stock.weight,
      asset_weight: metrics.asset_weight ?? stock.asset_weight,
    };
  });
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
      <DoughnutChart data={data} options={PIE_CHART_OPTIONS} />
    </div>
  );
});

interface PortfolioHoldingRowProps {
  stock: PortfolioStock;
  metrics?: HoldingMetricSummary;
  isMenuOpen: boolean;
  activeHoldingPanel: HoldingActionPanel;
  currentTradeType: "buy" | "sell" | "clear";
  singleAnalysisBusy: boolean;
  pendingSingleAnalysisSymbol: string;
  isEditingRow: boolean;
  isTradingRow: boolean;
  inlineEditForm: ReactNode;
  inlineTradeForm: ReactNode;
  onToggleHoldingMenu: (stockId: number) => void;
  onOpenEditPosition: (stock: PortfolioStock) => void;
  onOpenTradeEditor: (stock: PortfolioStock, tradeType: "buy" | "sell") => void;
  onOpenDeepAnalysis: (stock: PortfolioStock) => void;
}

function areHoldingRowPropsEqual(prev: PortfolioHoldingRowProps, next: PortfolioHoldingRowProps) {
  if (
    prev.stock !== next.stock
    || prev.metrics !== next.metrics
    || prev.isMenuOpen !== next.isMenuOpen
    || prev.isEditingRow !== next.isEditingRow
    || prev.isTradingRow !== next.isTradingRow
    || prev.currentTradeType !== next.currentTradeType
    || prev.singleAnalysisBusy !== next.singleAnalysisBusy
    || prev.pendingSingleAnalysisSymbol !== next.pendingSingleAnalysisSymbol
    || prev.inlineEditForm !== next.inlineEditForm
    || prev.inlineTradeForm !== next.inlineTradeForm
    || prev.onToggleHoldingMenu !== next.onToggleHoldingMenu
    || prev.onOpenEditPosition !== next.onOpenEditPosition
    || prev.onOpenTradeEditor !== next.onOpenTradeEditor
    || prev.onOpenDeepAnalysis !== next.onOpenDeepAnalysis
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
  metrics,
  isMenuOpen,
  activeHoldingPanel,
  currentTradeType,
  singleAnalysisBusy,
  pendingSingleAnalysisSymbol,
  isEditingRow,
  isTradingRow,
  inlineEditForm,
  inlineTradeForm,
  onToggleHoldingMenu,
  onOpenEditPosition,
  onOpenTradeEditor,
  onOpenDeepAnalysis,
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
                  className={`${styles.holdingActionMenuButton} ${isTradingRow && currentTradeType === "buy" ? styles.holdingActionMenuButtonActive : ""}`}
                  onClick={() => onOpenTradeEditor(stock, "buy")}
                  type="button"
                >
                  买入
                </button>
                <button
                  className={`${styles.holdingActionMenuButton} ${isTradingRow && currentTradeType !== "buy" ? styles.holdingActionMenuButtonActive : ""}`}
                  onClick={() => onOpenTradeEditor(stock, "sell")}
                  type="button"
                >
                  卖出
                </button>
                <button
                  className={styles.holdingActionMenuButton}
                  onClick={() => onOpenDeepAnalysis(stock)}
                  type="button"
                >
                  深度分析
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
  holdingMetrics: Map<string, HoldingMetricSummary>;
  activeHoldingMenuId: number | null;
  activeHoldingPanel: HoldingActionPanel;
  currentTradeType: "buy" | "sell" | "clear";
  singleAnalysisBusy: boolean;
  pendingSingleAnalysisSymbol: string;
  editingStockId: number | null;
  tradeStockId: string;
  inlineEditForm: ReactNode;
  inlineTradeForm: ReactNode;
  onToggleHoldingMenu: (stockId: number) => void;
  onOpenEditPosition: (stock: PortfolioStock) => void;
  onOpenTradeEditor: (stock: PortfolioStock, tradeType: "buy" | "sell") => void;
  onOpenDeepAnalysis: (stock: PortfolioStock) => void;
}

const PortfolioHoldingsTable = memo(function PortfolioHoldingsTable({
  stocks,
  holdingMetrics,
  activeHoldingMenuId,
  activeHoldingPanel,
  currentTradeType,
  singleAnalysisBusy,
  pendingSingleAnalysisSymbol,
  editingStockId,
  tradeStockId,
  inlineEditForm,
  inlineTradeForm,
  onToggleHoldingMenu,
  onOpenEditPosition,
  onOpenTradeEditor,
  onOpenDeepAnalysis,
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
                metrics={
                  holdingMetrics.get(buildHoldingMetricKey(stock.id, stock.code, stock.name))
                  ?? holdingMetrics.get(buildHoldingMetricKey(null, stock.code, stock.name))
                  ?? {
                    currentPrice: stock.current_price,
                    pnl: stock.pnl,
                    pnlPct: stock.pnl_pct,
                    marketValue: stock.market_value,
                    assetWeight: stock.asset_weight,
                    investedWeight: stock.weight,
                  }
                }
                isMenuOpen={isMenuOpen}
                activeHoldingPanel={activeHoldingPanel}
                currentTradeType={currentTradeType}
                singleAnalysisBusy={singleAnalysisBusy}
                pendingSingleAnalysisSymbol={pendingSingleAnalysisSymbol}
                isEditingRow={isEditingRow}
                isTradingRow={isTradingRow}
                inlineEditForm={isEditingRow ? inlineEditForm : null}
                inlineTradeForm={isTradingRow ? inlineTradeForm : null}
                onToggleHoldingMenu={onToggleHoldingMenu}
                onOpenEditPosition={onOpenEditPosition}
                onOpenTradeEditor={onOpenTradeEditor}
                onOpenDeepAnalysis={onOpenDeepAnalysis}
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

interface PortfolioPageProps {
  embedded?: boolean;
}

export function PortfolioPage({ embedded = false }: PortfolioPageProps = {}) {
  const { lightweightModel, reasoningModel } = useSelectedModels();
  const [searchParams, setSearchParams] = useSearchParams();
  const deepAnalysisAnalysts = useDeepAnalysisStore((state) => state.analysts);
  const schedulerTaskId = usePortfolioStore((state) => state.schedulerTaskId);
  const setSchedulerTaskId = usePortfolioStore((state) => state.setSchedulerTaskId);
  const holdingsAnalysisTaskId = usePortfolioStore((state) => state.holdingsAnalysisTaskId);
  const setHoldingsAnalysisTaskId = usePortfolioStore((state) => state.setHoldingsAnalysisTaskId);
  const cachedPage = usePortfolioStore((state) => state.pageCache ?? null);
  const setPageCache = usePortfolioStore((state) => state.setPageCache);
  const clearSmartMonitorPageCache = useSmartMonitorStore((state) => state.clearPageCache);

  const [stocks, setStocks] = useState<PortfolioStock[]>(() => (cachedPage?.stocks as PortfolioStock[]) ?? []);
  const [risk, setRisk] = useState<PortfolioRisk | null>(() => (cachedPage?.risk as PortfolioRisk | null) ?? null);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(() => (cachedPage?.scheduler as SchedulerStatus | null) ?? null);
  const [positionForm, setPositionForm] = useState(defaultPositionForm);
  const [tradeForm, setTradeForm] = useState(defaultTradeForm);
  const [schedulerTimes, setSchedulerTimes] = useState(() => cachedPage?.schedulerTimes ?? DEFAULT_SCHEDULER_TIME);
  const [activeEditor, setActiveEditor] = useState<EditorPanel>(null);
  const [editingStockId, setEditingStockId] = useState<number | null>(null);
  const [activeHoldingMenuId, setActiveHoldingMenuId] = useState<number | null>(null);
  const [activeHoldingPanel, setActiveHoldingPanel] = useState<HoldingActionPanel>(null);
  const [isSubmittingHoldingsAnalysis, setIsSubmittingHoldingsAnalysis] = useState(false);
  const [isSubmittingSingleAnalysis, setIsSubmittingSingleAnalysis] = useState(false);
  const [pendingSingleAnalysisSymbol, setPendingSingleAnalysisSymbol] = useState("");
  const [holdingsAnalysisTask, setHoldingsAnalysisTask] = useState<AnalysisTaskSummary | null>(null);
  const [isSubmittingPosition, setIsSubmittingPosition] = useState(false);
  const [isSubmittingTrade, setIsSubmittingTrade] = useState(false);
  const [isUpdatingPosition, setIsUpdatingPosition] = useState(false);
  const [deletingStockId, setDeletingStockId] = useState<number | null>(null);
  const [isSavingScheduler, setIsSavingScheduler] = useState(false);
  const [isTogglingScheduler, setIsTogglingScheduler] = useState(false);
  const [isRefreshingPage, setIsRefreshingPage] = useState(false);
  const [singleAnalysisTaskId, setSingleAnalysisTaskId] = useState<string | null>(null);
  const [singleAnalysisTask, setSingleAnalysisTask] = useState<AnalysisTaskSummary | null>(null);
  const [schedulerTask, setSchedulerTask] = useState<AnalysisTaskSummary | null>(null);
  const { message, error, clear, showError, showMessage } = usePageFeedback();
  const schedulerTerminalTaskRef = useRef<string>("");
  const singleAnalysisTerminalTaskRef = useRef<string>("");
  const holdingsAnalysisTerminalTaskRef = useRef<string>("");
  const pageLoadRequestRef = useRef(0);
  const stocksRef = useRef<PortfolioStock[]>(stocks);
  const riskRef = useRef<PortfolioRisk | null>(risk);
  const schedulerRef = useRef<SchedulerStatus | null>(scheduler);
  const schedulerTimesRef = useRef(schedulerTimes);
  const deepAnalysisSelectedAnalysts = useMemo(
    () => normalizeAnalystKeys(analystConfigToKeys(deepAnalysisAnalysts)),
    [deepAnalysisAnalysts],
  );

  useEffect(() => {
    stocksRef.current = stocks;
  }, [stocks]);

  useEffect(() => {
    riskRef.current = risk;
  }, [risk]);

  useEffect(() => {
    schedulerRef.current = scheduler;
  }, [scheduler]);

  useEffect(() => {
    schedulerTimesRef.current = schedulerTimes;
  }, [schedulerTimes]);

  const applySchedulerState = (schedulerData: SchedulerStatus | null) => {
    setScheduler(schedulerData);
    schedulerRef.current = schedulerData;
    const nextSchedulerTimes = (schedulerData?.schedule_times ?? [])[0] || DEFAULT_SCHEDULER_TIME;
    setSchedulerTimes(nextSchedulerTimes);
    schedulerTimesRef.current = nextSchedulerTimes;
  };

  const persistPageCache = (overrides?: {
    stocks?: PortfolioStock[];
    risk?: PortfolioRisk | null;
    scheduler?: SchedulerStatus | null;
    schedulerTimes?: string;
  }) => {
    setPageCache({
      stocks: overrides?.stocks ?? stocksRef.current,
      risk: overrides?.risk ?? riskRef.current,
      scheduler: overrides?.scheduler ?? schedulerRef.current,
      schedulerTimes: overrides?.schedulerTimes ?? schedulerTimesRef.current,
      updatedAt: Date.now(),
    });
  };

  const applyStocksData = (stockData: PortfolioStock[], riskData?: PortfolioRisk | null) => {
    const mergedStocks = mergeStocksWithRiskMetrics(stockData, riskData ?? riskRef.current);
    setStocks(mergedStocks);
    stocksRef.current = mergedStocks;
    return mergedStocks;
  };

  const applyRiskData = (riskData: PortfolioRisk | null, stockData?: PortfolioStock[]) => {
    setRisk(riskData);
    riskRef.current = riskData;
    const mergedStocks = mergeStocksWithRiskMetrics(stockData ?? stocksRef.current, riskData);
    setStocks(mergedStocks);
    stocksRef.current = mergedStocks;
    return mergedStocks;
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
    const cachedRisk = (cache.risk as PortfolioRisk | null) ?? null;
    setRisk(cachedRisk);
    riskRef.current = cachedRisk;
    applyStocksData((cache.stocks as PortfolioStock[]) ?? [], cachedRisk);
    applySchedulerState((cache.scheduler as SchedulerStatus | null) ?? null);
  };

  const loadAll = async (force = false, options?: { background?: boolean }) => {
    if (!force && cachedPage && Date.now() - cachedPage.updatedAt < PAGE_CACHE_TTL_MS) {
      applyPageCache(cachedPage);
      return;
    }
    const requestId = pageLoadRequestRef.current + 1;
    pageLoadRequestRef.current = requestId;
    setIsRefreshingPage(true);
    try {
      const useFreshRequest = force || options?.background;
      const fetchStocks = useFreshRequest ? apiFetch<PortfolioStock[]> : apiFetchCached<PortfolioStock[]>;
      const fetchRisk = useFreshRequest ? apiFetch<PortfolioRisk> : apiFetchCached<PortfolioRisk>;
      const riskUrl = options?.background ? RISK_REFRESH_URL : "/api/portfolio/risk";
      const [stockData, schedulerData] = await Promise.all([
        fetchStocks("/api/portfolio/stocks"),
        apiFetch<SchedulerStatus>("/api/portfolio/scheduler"),
      ]);

      if (pageLoadRequestRef.current !== requestId) {
        return;
      }

      const mergedStockData = applyStocksData(stockData);
      applySchedulerState(schedulerData);
      persistPageCache({
        stocks: mergedStockData,
        scheduler: schedulerData,
        schedulerTimes: (schedulerData?.schedule_times ?? [])[0] || DEFAULT_SCHEDULER_TIME,
      });
      setTradeForm((current) => {
        if (current.stock_id || !stockData[0]) {
          return current;
        }
        return { ...current, stock_id: String(stockData[0].id) };
      });

      if (pageLoadRequestRef.current === requestId) {
        setIsRefreshingPage(false);
      }

      const riskData = await fetchRisk(riskUrl);
      if (pageLoadRequestRef.current !== requestId) {
        return;
      }
      const mergedStocksWithRisk = applyRiskData(riskData, stockData);
      persistPageCache({
        stocks: mergedStocksWithRisk,
        risk: riskData,
        scheduler: schedulerData,
        schedulerTimes: (schedulerData?.schedule_times ?? [])[0] || DEFAULT_SCHEDULER_TIME,
      });
    } finally {
      if (pageLoadRequestRef.current === requestId) {
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
  }, []);

  useEffect(() => {
    const intent = decodeIntent<PositionIntentPayload>(searchParams.get("intent"));
    if (!intent || intent.type !== "portfolio") {
      return;
    }

    const payload = intent.payload || {};
    setPositionForm((current) => ({
      ...current,
      code: payload.symbol || "",
      name: "",
      cost_price: payload.default_cost_price !== undefined ? String(payload.default_cost_price) : "",
      note: payload.default_note || "",
      origin_analysis_id: payload.origin_analysis_id,
    }));
    setActiveEditor("position");

    const nextParams = new URLSearchParams(searchParams);
    nextParams.delete("intent");
    setSearchParams(nextParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const clearSingleAnalysisTask = () => {
    setSingleAnalysisTaskId(null);
    setSingleAnalysisTask(null);
    setPendingSingleAnalysisSymbol("");
    singleAnalysisTerminalTaskRef.current = "";
  };

  const clearHoldingsAnalysisTask = useCallback(() => {
    setHoldingsAnalysisTaskId(null);
    setHoldingsAnalysisTask(null);
    holdingsAnalysisTerminalTaskRef.current = "";
  }, [setHoldingsAnalysisTaskId]);

  const clearSchedulerTask = () => {
    setSchedulerTaskId(null);
    setSchedulerTask(null);
    schedulerTerminalTaskRef.current = "";
  };

  const loadSingleAnalysisTask = async (taskIdOverride?: string | null) => {
    const taskId = taskIdOverride ?? singleAnalysisTaskId;
    if (!taskId) {
      setSingleAnalysisTask(null);
      return;
    }
    try {
      const task = await apiFetch<AnalysisTaskSummary>(`/api/tasks/${taskId}`);
      setSingleAnalysisTask(task);
    } catch (requestError) {
      if (requestError instanceof ApiRequestError && requestError.status === 404) {
        clearSingleAnalysisTask();
      }
    }
  };

  const loadHoldingsAnalysisTask = useCallback(async (taskIdOverride?: string | null) => {
    const taskId = taskIdOverride ?? holdingsAnalysisTaskId;
    if (taskId) {
      try {
        const task = await apiFetch<AnalysisTaskSummary>(`/api/tasks/${taskId}`);
        if (task.task_type && task.task_type !== "portfolio_holdings_analysis") {
          clearHoldingsAnalysisTask();
          return;
        }
        setHoldingsAnalysisTask(task);
        return;
      } catch (requestError) {
        if (requestError instanceof ApiRequestError && requestError.status === 404) {
          clearHoldingsAnalysisTask();
          return;
        }
      }
    }

    try {
      const pendingTasks = await apiFetch<AnalysisTaskSummary[]>("/api/tasks/pending");
      const holdingsTask = pendingTasks.find((task) => task.task_type === "portfolio_holdings_analysis") || null;
      if (holdingsTask) {
        setHoldingsAnalysisTaskId(holdingsTask.id);
        setHoldingsAnalysisTask(holdingsTask);
        return;
      }
      setHoldingsAnalysisTask(null);
    } catch (requestError) {
      if (requestError instanceof ApiRequestError && requestError.status === 404) {
        clearHoldingsAnalysisTask();
      }
    }
  }, [clearHoldingsAnalysisTask, holdingsAnalysisTaskId, setHoldingsAnalysisTaskId]);

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

  const schedulerTaskPending = isPendingTaskStatus(schedulerTask?.status || "");
  const schedulerPollingIntervalMs = schedulerTaskId || schedulerTaskPending
    ? 2500
    : 15000;
  const holdingsAnalysisTaskPending = isPendingTaskStatus(holdingsAnalysisTask?.status || "");
  const holdingsAnalysisBusy = isSubmittingHoldingsAnalysis || holdingsAnalysisTaskPending;

  usePollingLoader({
    load: loadSchedulerTask,
    intervalMs: schedulerPollingIntervalMs,
    enabled:
      Boolean(schedulerTaskId)
      || schedulerTaskPending,
    immediate: true,
    dependencies: [schedulerTaskId, schedulerTask?.status],
  });

  usePollingLoader({
    load: loadHoldingsAnalysisTask,
    intervalMs: 2500,
    enabled: Boolean(holdingsAnalysisTaskId) || holdingsAnalysisTaskPending,
    immediate: true,
    dependencies: [holdingsAnalysisTaskId, holdingsAnalysisTask?.status],
  });

  usePollingLoader({
    load: loadSingleAnalysisTask,
    intervalMs: 2000,
    enabled: Boolean(singleAnalysisTaskId && isPendingTaskStatus(singleAnalysisTask?.status || "running")),
    immediate: true,
    dependencies: [singleAnalysisTaskId, singleAnalysisTask?.status],
  });

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
    clearSchedulerTask();
  }, [clearSmartMonitorPageCache, schedulerTask?.id, schedulerTask?.status]);

  useEffect(() => {
    if (!singleAnalysisTask || isPendingTaskStatus(singleAnalysisTask.status)) {
      return;
    }
    const terminalKey = `${singleAnalysisTask.id}:${singleAnalysisTask.status}`;
    if (singleAnalysisTerminalTaskRef.current === terminalKey) {
      return;
    }
    singleAnalysisTerminalTaskRef.current = terminalKey;
    void loadAll(true).catch(() => undefined);
    clearSmartMonitorPageCache();
    clearSingleAnalysisTask();
  }, [clearSmartMonitorPageCache, singleAnalysisTask?.id, singleAnalysisTask?.status]);

  useEffect(() => {
    if (!holdingsAnalysisTask || isPendingTaskStatus(holdingsAnalysisTask.status)) {
      return;
    }
    const terminalKey = `${holdingsAnalysisTask.id}:${holdingsAnalysisTask.status}`;
    if (holdingsAnalysisTerminalTaskRef.current === terminalKey) {
      return;
    }
    holdingsAnalysisTerminalTaskRef.current = terminalKey;
    clearHoldingsAnalysisTask();
    void loadAll(true).catch(() => undefined);
    clearSmartMonitorPageCache();
  }, [clearHoldingsAnalysisTask, clearSmartMonitorPageCache, holdingsAnalysisTask?.id, holdingsAnalysisTask?.status]);

  useEffect(() => {
    void loadHoldingsAnalysisTask().catch(() => undefined);
  }, [loadHoldingsAnalysisTask]);

  const handleManualRefresh = () => {
    clear();
    void loadAll(true).catch((requestError) => {
      showError(requestError instanceof ApiRequestError ? requestError.message : "刷新持仓数据失败");
    });
  };

  const riskWarnings = risk?.risk_warnings ?? [];
  const holdingMetrics = useMemo(() => {
    const distribution = risk?.stock_distribution ?? [];
    const metricsMap = new Map<string, HoldingMetricSummary>();
    distribution.forEach((item) => {
      const summary = {
        currentPrice: item.current_price,
        pnl: item.pnl,
        pnlPct: item.pnl_pct,
        marketValue: item.market_value,
        assetWeight: item.asset_weight,
        investedWeight: item.weight,
      };
      metricsMap.set(buildHoldingMetricKey(item.stock_id, item.code, item.name), summary);
      metricsMap.set(buildHoldingMetricKey(null, item.code, item.name), summary);
    });
    return metricsMap;
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

  const singleAnalysisBusy =
    isSubmittingSingleAnalysis
    || Boolean(singleAnalysisTaskId && !singleAnalysisTask)
    || isPendingTaskStatus(singleAnalysisTask?.status);
  const singleAnalysisTaskStatus = taskStatusMeta(singleAnalysisTask);
  const schedulerTaskStatus = taskStatusMeta(schedulerTask);
  const singleAnalysisTaskSummary = summarizeHoldingsTask(singleAnalysisTask);
  const schedulerTaskSummary = summarizeSchedulerTask(schedulerTask);

  const renderSingleAnalysisTask = () => {
    if (!singleAnalysisTaskId && !singleAnalysisTask) {
      return null;
    }
    return (
      <div className={styles.moduleSection}>
        <div className={styles.noticeMeta}>
          <div>
            <strong>单股深度分析进度</strong>
            <div className={styles.muted}>{singleAnalysisTask?.label || "等待任务状态..."}</div>
          </div>
          <StatusBadge label={singleAnalysisTaskStatus.label} tone={singleAnalysisTaskStatus.tone} />
        </div>
        <TaskProgressBar
          current={singleAnalysisTask?.current ?? 0}
          message={singleAnalysisTask?.message || "等待单股深度分析任务状态..."}
          tone={taskProgressTone(singleAnalysisTask)}
          total={singleAnalysisTaskSummary?.total || singleAnalysisTask?.total || 0}
        />
        {singleAnalysisTaskSummary ? (
          <div className={styles.summaryMetricGrid}>
            <div className={styles.metric}>
              <span className={styles.muted}>分析总数</span>
              <strong>{singleAnalysisTaskSummary.total}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>成功</span>
              <strong>{singleAnalysisTaskSummary.success}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>失败</span>
              <strong>{singleAnalysisTaskSummary.failed}</strong>
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>已写入历史</span>
              <strong>{singleAnalysisTaskSummary.saved}</strong>
            </div>
          </div>
        ) : null}
        {singleAnalysisTaskSummary?.failedSymbols?.length ? (
          <p className={styles.dangerText}>失败股票：{singleAnalysisTaskSummary.failedSymbols.join("、")}</p>
        ) : null}
        {singleAnalysisTask?.error ? <p className={styles.dangerText}>{singleAnalysisTask.error}</p> : null}
        <div className={styles.actions}>
          <button className={styles.secondaryButton} onClick={() => void loadSingleAnalysisTask()} type="button">
            刷新状态
          </button>
          {!isPendingTaskStatus(singleAnalysisTask?.status) ? (
            <button className={styles.secondaryButton} onClick={clearSingleAnalysisTask} type="button">
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
            <strong>持仓列表任务进度</strong>
            <div className={styles.muted}>{schedulerTask?.label || "等待任务状态..."}</div>
          </div>
          <StatusBadge label={schedulerTaskStatus.label} tone={schedulerTaskStatus.tone} />
        </div>
        <TaskProgressBar
          current={schedulerTask?.current ?? 0}
          message={schedulerTask?.message || "等待持仓列表任务状态..."}
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
          </div>
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

  const openTradeEditor = useCallback((stock: PortfolioStock, tradeType: "buy" | "sell") => {
    setTradeForm({
      ...defaultTradeForm,
      stock_id: String(stock.id),
      trade_type: tradeType,
      trade_date: todayDateInput(),
    });
    setActiveEditor("trade");
    setActiveHoldingMenuId(stock.id);
    setActiveHoldingPanel("trade");
  }, []);

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

  const handleOpenDeepAnalysis = useCallback(async (stock: PortfolioStock) => {
    const symbol = String(stock.code || "").trim().toUpperCase();
    if (!symbol) {
      showError("无法找到该持仓代码。");
      return;
    }

    clear();
    setIsSubmittingSingleAnalysis(true);
    setPendingSingleAnalysisSymbol(symbol);
    closeHoldingPanel();
    try {
      const result = await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: symbol,
          batch_mode: "顺序分析",
          max_workers: 1,
          analysts: deepAnalysisAnalysts,
          lightweight_model: lightweightModel || undefined,
          reasoning_model: reasoningModel || undefined,
        }),
      });
      const taskId = result.task_id || null;
      if (taskId) {
        setSingleAnalysisTaskId(taskId);
        setSingleAnalysisTask({
          id: taskId,
          label: `深度分析 ${symbol}`,
          status: "queued",
          message: `正在提交 ${symbol} 的深度分析任务...`,
          current: 0,
          total: 1,
        });
        void loadSingleAnalysisTask(taskId).catch(() => undefined);
      }
      showMessage(`已开始分析 ${symbol}。`);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "启动深度分析失败");
    } finally {
      setIsSubmittingSingleAnalysis(false);
    }
  }, [clear, closeHoldingPanel, deepAnalysisAnalysts, loadSingleAnalysisTask, showError, showMessage]);

  const handleAnalyzeAllHoldings = async () => {
    if (!stocks.length) {
      showError("当前没有可分析的持仓。");
      return;
    }
    if (holdingsAnalysisBusy) {
      return;
    }
    clear();
    setIsSubmittingHoldingsAnalysis(true);
    try {
      const taskData = await apiFetch<{ task_id: string }>("/api/portfolio/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          batch_mode: "顺序分析",
          max_workers: 1,
          analysts: deepAnalysisAnalysts,
          lightweight_model: lightweightModel || undefined,
          reasoning_model: reasoningModel || undefined,
        }),
      });
      if (taskData.task_id) {
        setHoldingsAnalysisTaskId(taskData.task_id);
        setHoldingsAnalysisTask({
          id: taskData.task_id,
          label: "持仓批量分析",
          status: "queued",
          message: "正在提交持仓批量分析任务...",
          current: 0,
          total: 0,
        });
      }
      showMessage(`已提交 ${stocks.length} 只持仓的深度分析任务${taskData.task_id ? `（任务 ${taskData.task_id}）` : ""}。`);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "提交持仓深度分析失败");
    } finally {
      setIsSubmittingHoldingsAnalysis(false);
    }
  };

  const submitPosition = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    clear();
    setIsSubmittingPosition(true);
    const nextCode = positionForm.code.trim();
    const nextName = "";
    const nextCostPrice = positionForm.cost_price ? Number(positionForm.cost_price) : undefined;
    const nextQuantity = positionForm.quantity ? Number(positionForm.quantity) : undefined;
    const nextNote = positionForm.note;
    const nextBuyDate = positionForm.buy_date || undefined;
    const nextAutoMonitor = positionForm.auto_monitor;
    const nextOriginAnalysisId = positionForm.origin_analysis_id;
    const nextPosition = {
      id: -Date.now(),
      code: nextCode,
      name: nextName,
      cost_price: nextCostPrice,
      quantity: nextQuantity,
      note: nextNote,
      analysis_record_id: nextOriginAnalysisId,
      last_trade_at: nextBuyDate,
    } satisfies PortfolioStock;
    setStocks((current) => [nextPosition, ...current]);
    setPositionForm(defaultPositionForm);
    setEditingStockId(null);
    setActiveEditor(null);
    closeHoldingPanel();
    showMessage(`正在保存持仓：${nextCode}`);
    try {
      await apiFetch("/api/portfolio/stocks", {
        method: "POST",
        body: JSON.stringify({
          code: nextCode,
          name: nextName,
          cost_price: nextCostPrice ?? null,
          quantity: nextQuantity ?? null,
          note: nextNote,
          buy_date: nextBuyDate ?? null,
          auto_monitor: nextAutoMonitor,
          origin_analysis_id: nextOriginAnalysisId ?? null,
        }),
      });
      showMessage(`持仓已新增：${nextCode}`);
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
    const isClearTrade = tradeForm.trade_type === "clear";
    if (!tradeForm.price || Number(tradeForm.price) <= 0) {
      showError("成交价格必须大于 0。");
      return;
    }
    if (!isClearTrade && (!tradeForm.quantity || Number(tradeForm.quantity) <= 0)) {
      showError("交易数量必须大于 0。");
      return;
    }

    clear();
    setIsSubmittingTrade(true);
    const tradePayload = {
      trade_type: tradeForm.trade_type,
      quantity: isClearTrade ? 0 : Number(tradeForm.quantity),
      price: Number(tradeForm.price),
      trade_date: tradeForm.trade_date || null,
      note: tradeForm.note,
    };
    const currentStockId = tradeForm.stock_id;
    setTradeForm((current) => ({ ...defaultTradeForm, stock_id: current.stock_id }));
    closeHoldingPanel();
    try {
      await apiFetch(`/api/portfolio/stocks/${currentStockId}/trades`, {
        method: "POST",
        body: JSON.stringify(tradePayload),
      });
      showMessage(tradePayload.trade_type === "buy" ? "买入记录已保存。" : tradePayload.trade_type === "clear" ? "清仓记录已保存。" : "卖出记录已保存。");
      void loadAll(true).catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "登记交易失败");
      void loadAll(true).catch(() => undefined);
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
    const nextCostPrice = positionForm.cost_price ? Number(positionForm.cost_price) : undefined;
    const nextQuantity = positionForm.quantity ? Number(positionForm.quantity) : undefined;
    const nextNote = positionForm.note;
    const nextBuyDate = positionForm.buy_date || undefined;
    const previousStock = stocks.find((item) => item.id === editingStockId) ?? null;
    setStocks((current) =>
      current.map((stock) =>
        stock.id === editingStockId
          ? {
            ...stock,
            code: nextCode,
            name: nextName,
            cost_price: nextCostPrice,
            quantity: nextQuantity,
            note: nextNote,
            last_trade_at: nextBuyDate ?? stock.last_trade_at,
          }
          : stock,
      ),
    );
    resetPositionEditor();
    closeHoldingPanel();
    showMessage(`正在保存持仓：${nextCode}`);
    try {
      await apiFetch(`/api/portfolio/stocks/${editingStockId}`, {
        method: "PATCH",
        body: JSON.stringify({
          code: nextCode,
          name: nextName,
          cost_price: nextCostPrice ?? null,
          quantity: nextQuantity ?? null,
          note: nextNote,
          buy_date: nextBuyDate ?? null,
        }),
      });
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
    if (!deepAnalysisSelectedAnalysts.length) {
      showError("请至少选择一位分析师。");
      return;
    }
    setIsSavingScheduler(true);
    try {
      const nextScheduler = await apiFetch<SchedulerStatus>("/api/portfolio/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          schedule_times: [schedulerTimes.trim() || DEFAULT_SCHEDULER_TIME],
          selected_agents: deepAnalysisSelectedAnalysts,
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
      {tradeForm.trade_type !== "buy" ? (
        <div className={styles.dualToggleGrid}>
          <button
            className={tradeForm.trade_type === "sell" ? styles.primaryButton : styles.secondaryButton}
            onClick={() => setTradeForm((current) => ({ ...current, trade_type: "sell" }))}
            type="button"
          >
            卖出
          </button>
          <button
            className={tradeForm.trade_type === "clear" ? styles.dangerButton : styles.secondaryButton}
            onClick={() => setTradeForm((current) => ({ ...current, trade_type: "clear", quantity: "" }))}
            type="button"
          >
            清仓
          </button>
        </div>
      ) : null}
      <div className={styles.formGrid}>
        {tradeForm.trade_type !== "clear" ? (
          <div className={styles.field}>
            <label htmlFor="inline-trade-quantity">数量</label>
            <input
              id="inline-trade-quantity"
              onChange={(event) => setTradeForm((current) => ({ ...current, quantity: event.target.value }))}
              value={tradeForm.quantity}
            />
          </div>
        ) : null}
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
      {tradeForm.trade_type === "clear" ? (
        <p className={styles.helperText}>清仓会按当前全部持仓数量卖出，不需要填写数量。</p>
      ) : null}
      <div className={styles.actions}>
        <button className={styles.primaryButton} disabled={isSubmittingTrade} type="submit">
          {isSubmittingTrade ? "保存中..." : tradeForm.trade_type === "buy" ? "保存买入" : tradeForm.trade_type === "clear" ? "确认清仓" : "保存卖出"}
        </button>
        <button className={styles.secondaryButton} onClick={closeHoldingPanel} type="button">取消</button>
      </div>
    </form>
  );

  const inlineEditForm = activeHoldingPanel === "edit" && editingStockId ? renderInlineEditForm() : null;
  const inlineTradeForm = activeHoldingPanel === "trade" && tradeForm.stock_id ? renderInlineTradeForm() : null;

  const pageBody = (
    <div className={styles.stack}>
      <PageFeedback error={error} message={message} />

      <ModuleCard
        title="持仓操作"
        summary="新增持仓、买卖登记统一收在一个入口。"
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
          {renderSingleAnalysisTask()}
        </div>
        {activeEditor === "position" ? renderPositionForm(false) : null}
      </ModuleCard>

      <ModuleCard
        title="持仓数据"
        summary="点击任一股票行展开横向操作菜单，处理修改、交易和分析。"
        toolbar={(
          <div className={styles.holdingsAnalysisToolbar}>
            <button
              className={`${styles.secondaryButton} ${styles.holdingsAnalysisButton}`}
              disabled={isRefreshingPage || holdingsAnalysisBusy || !stocks.length}
              onClick={() => void handleAnalyzeAllHoldings()}
              type="button"
            >
              {holdingsAnalysisBusy ? "分析中..." : "分析所有持仓"}
            </button>
          </div>
        )}
      >
        <div className={styles.moduleSection}>
          <PortfolioHoldingsTable
            stocks={stocks}
            holdingMetrics={holdingMetrics}
            activeHoldingMenuId={activeHoldingMenuId}
            activeHoldingPanel={activeHoldingPanel}
            currentTradeType={tradeForm.trade_type}
            singleAnalysisBusy={singleAnalysisBusy}
            pendingSingleAnalysisSymbol={pendingSingleAnalysisSymbol}
            editingStockId={editingStockId}
            tradeStockId={tradeForm.stock_id}
            inlineEditForm={inlineEditForm}
            inlineTradeForm={inlineTradeForm}
            onToggleHoldingMenu={toggleHoldingMenu}
            onOpenEditPosition={openEditPosition}
            onOpenTradeEditor={openTradeEditor}
            onOpenDeepAnalysis={handleOpenDeepAnalysis}
          />
        </div>
      </ModuleCard>

      <ModuleCard hideTitleOnMobile title="定时分析" summary="这里只维护执行时间；分析师配置直接跟随深度分析页面的选择。">
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
              </div>
              <div className={styles.noticeCard}>
                <div className={styles.noticeMeta}>
                  <strong>分析师配置</strong>
                  <StatusBadge label="跟随深度分析" tone="default" />
                </div>
                <div>定时任务会直接使用深度分析页面里保存的分析师组合，不再单独维护一套持仓列表配置。</div>
              </div>
              <div className={styles.noticeCard}>
                <div className={styles.noticeMeta}>
                  <strong>分析范围</strong>
                  <StatusBadge label="自动覆盖" tone="default" />
                </div>
                <div>定时分析会默认覆盖当前全部持仓。</div>
              </div>
            </>
          )}
          statusFields={renderSchedulerTask()}
        />
      </ModuleCard>
    </div>
  );

  return pageBody;
}
