import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { AnalystSelector } from "../../components/common/AnalystSelector";
import { ModuleCard } from "../../components/common/ModuleCard";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import type { ActionPayload } from "../../components/research/AnalysisActionButtons";
import { type AnalysisRecordDetail } from "../../components/research/AnalysisDetailPanel";
import { ANALYST_OPTIONS, analystConfigToKeys, analystKeysToConfig, type AnalystKey } from "../../constants/analysts";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { usePollingLoader } from "../../hooks/usePollingLoader";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { encodeIntent } from "../../lib/intents";
import { useDeepAnalysisStore } from "../../stores/deepAnalysisStore";
import { useSmartMonitorStore } from "../../stores/smartMonitorStore";
import styles from "../ConsolePage.module.scss";

interface TaskResultRow {
  symbol: string;
  success: boolean;
  error?: string;
  record_id?: number;
}

interface TaskResult {
  mode: "single" | "batch";
  record_id?: number;
  symbol?: string;
  stock_info?: {
    name?: string;
  } | null;
  results?: TaskResultRow[];
}

interface TaskDetail {
  id: string;
  label?: string;
  status: string;
  message: string;
  current?: number;
  total?: number;
  error?: string;
  result?: TaskResult | null;
}

interface FollowupAsset {
  id: number;
  symbol: string;
  name: string;
  account_name?: string;
  followup_status_label?: string;
  latest_analysis_time?: string;
  latest_analysis_rating?: string;
  latest_analysis_summary?: string;
  latest_analysis_id?: number;
  status?: string;
  action_payload?: ActionPayload | null;
}

type SectionKey = "start" | "watchlist" | "viewed";

const sectionTabs = [
  { key: "start", label: "开始分析" },
  { key: "watchlist", label: "关注中" },
  { key: "viewed", label: "看过" },
];

function buildSingleRecordFromTask(task: TaskDetail | null): AnalysisRecordDetail | null {
  const result = task?.result;
  if (!result || result.mode !== "single") {
    return null;
  }
  return {
    id: result.record_id,
    symbol: result.symbol,
    stock_name: result.stock_info?.name || result.symbol,
  };
}

function isWatchlistAsset(asset: FollowupAsset) {
  return asset.followup_status_label === "关注中" || asset.status === "watchlist";
}

function taskProgressTone(task: TaskDetail | null): "running" | "success" | "danger" {
  if (!task) {
    return "running";
  }
  if (task.status === "success") {
    return "success";
  }
  if (task.status === "failed" || task.status === "cancelled") {
    return "danger";
  }
  return "running";
}

function FollowupAssetList({
  assets,
  emptyText,
  followupSearch,
  busy = false,
  pendingSymbol = "",
  onSearchChange,
  onRefresh,
  onOpenMonitor,
  onReAnalyze,
  onOpenHistory,
}: {
  assets: FollowupAsset[];
  emptyText: string;
  followupSearch: string;
  busy?: boolean;
  pendingSymbol?: string;
  onSearchChange: (value: string) => void;
  onRefresh: () => void;
  onOpenMonitor: (asset: FollowupAsset) => void;
  onReAnalyze: (symbol: string) => void;
  onOpenHistory: (recordId: number) => void;
}) {
  return (
    <>
      <div className={styles.moduleSection}>
        <div className={styles.formGrid}>
          <div className={styles.field}>
            <label htmlFor="followupSearch">搜索</label>
            <input
              id="followupSearch"
              onChange={(event) => onSearchChange(event.target.value)}
              placeholder="搜索代码 / 名称"
              value={followupSearch}
            />
          </div>
        </div>
        <div className={styles.responsiveActionGrid}>
          <button className={styles.secondaryButton} onClick={onRefresh} type="button">
            刷新列表
          </button>
        </div>
      </div>
      <div className={styles.moduleSection}>
        <div className={styles.list}>
          {assets.map((asset) => (
            <div className={styles.listItem} key={asset.id}>
              <strong>
                {asset.name} ({asset.symbol})
              </strong>
              <p className={styles.muted}>
                {asset.followup_status_label || asset.status} | {asset.latest_analysis_rating || "未评级"} |{" "}
                {formatDateTime(asset.latest_analysis_time, "暂无时间")}
              </p>
              <p>{asset.latest_analysis_summary || "暂无摘要"}</p>
              <div className={styles.actions}>
                <button className={styles.secondaryButton} onClick={() => onOpenMonitor(asset)} type="button">
                  加入盯盘
                </button>
                <button
                  className={styles.secondaryButton}
                  disabled={busy}
                  onClick={() => onReAnalyze(asset.symbol)}
                  type="button"
                >
                  {pendingSymbol === asset.symbol ? "分析中..." : "再次分析"}
                </button>
                {asset.latest_analysis_id ? (
                  <button
                    className={styles.secondaryButton}
                    onClick={() => onOpenHistory(asset.latest_analysis_id ?? 0)}
                    type="button"
                  >
                    分析历史
                  </button>
                ) : null}
              </div>
            </div>
          ))}
          {!assets.length ? <div className={styles.muted}>{emptyText}</div> : null}
        </div>
      </div>
    </>
  );
}

export function DeepAnalysisPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const analysts = useDeepAnalysisStore((state) => state.analysts);
  const setAnalysts = useDeepAnalysisStore((state) => state.setAnalysts);
  const clearSmartMonitorPageCache = useSmartMonitorStore((state) => state.clearPageCache);

  const [stockInput, setStockInput] = useState("");
  const [section, setSection] = useState<SectionKey>("start");
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [queuedTasks, setQueuedTasks] = useState<TaskDetail[]>([]);
  const [singleRecord, setSingleRecord] = useState<AnalysisRecordDetail | null>(null);
  const [followupAssets, setFollowupAssets] = useState<FollowupAsset[]>([]);
  const [followupSearch, setFollowupSearch] = useState("");
  const [isSubmittingAnalysis, setIsSubmittingAnalysis] = useState(false);
  const [pendingReAnalyzeSymbol, setPendingReAnalyzeSymbol] = useState("");
  const { message, error, clear, showError, showMessage } = usePageFeedback();
  const lastTerminalTaskRef = useRef<string>("");

  const selectedAnalysts = useMemo(
    () => analystConfigToKeys(analysts as Partial<Record<AnalystKey, boolean>>),
    [analysts],
  );

  const loadTask = async () => {
    const [active, latest, pending] = await Promise.all([
      apiFetch<TaskDetail | null>("/api/tasks/active"),
      apiFetch<TaskDetail | null>("/api/tasks/latest"),
      apiFetch<TaskDetail[]>("/api/tasks/pending"),
    ]);
    setTask(active);
    setQueuedTasks((pending ?? []).filter((item) => item.status === "queued" && item.id !== active?.id));
    if (latest?.status === "success" && latest.result?.mode === "single" && latest.result.record_id) {
      const record = await apiFetch<AnalysisRecordDetail>(`/api/analysis-history/${latest.result.record_id}`);
      setSingleRecord(record);
      return;
    }
    if (latest?.status === "success") {
      setSingleRecord(buildSingleRecordFromTask(latest));
      return;
    }
    setSingleRecord(null);
  };

  const loadFollowupAssets = async () => {
    const data = await apiFetch<FollowupAsset[]>(
      `/api/followup-assets${buildQuery({
        search_term: followupSearch,
      })}`,
    );
    setFollowupAssets(data);
  };

  usePollingLoader({ load: loadTask, intervalMs: 2000 });

  useEffect(() => {
    if (!task || task.status === "queued" || task.status === "running") {
      return;
    }
    const terminalKey = `${task.id}:${task.status}:${task.result?.record_id ?? "na"}`;
    if (lastTerminalTaskRef.current === terminalKey) {
      return;
    }
    lastTerminalTaskRef.current = terminalKey;
    clearSmartMonitorPageCache();
    void loadFollowupAssets().catch(() => undefined);
  }, [clearSmartMonitorPageCache, task?.id, task?.status, task?.result?.record_id]);

  useEffect(() => {
    void loadFollowupAssets();
  }, [followupSearch]);

  useEffect(() => {
    const presetSymbol = searchParams.get("symbol");
    if (!presetSymbol) {
      return;
    }
    setStockInput(presetSymbol);
    setSection("start");
  }, [searchParams]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    clear();
    setIsSubmittingAnalysis(true);
    setPendingReAnalyzeSymbol(stockInput.trim().split(/[\s,，\n]+/)[0] || "");
    try {
      await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: stockInput,
          batch_mode: "顺序分析",
          max_workers: 1,
          analysts,
        }),
      });
      showMessage("分析任务已提交，正在准备执行...");
      void loadTask().catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "提交任务失败");
    } finally {
      setIsSubmittingAnalysis(false);
      setPendingReAnalyzeSymbol("");
    }
  };

  const handleReAnalyze = async (symbol: string) => {
    setStockInput(symbol);
    clear();
    setIsSubmittingAnalysis(true);
    setPendingReAnalyzeSymbol(symbol);
    try {
      await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: symbol,
          batch_mode: "顺序分析",
          max_workers: 1,
          analysts,
        }),
      });
      showMessage(`已重新提交 ${symbol} 的分析任务`);
      setSection("start");
      void loadTask().catch(() => undefined);
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "再次分析失败");
    } finally {
      setIsSubmittingAnalysis(false);
      setPendingReAnalyzeSymbol("");
    }
  };

  const handlePromoteWatchlist = async (assetId: number) => {
    await apiFetch(`/api/followup-assets/${assetId}/watchlist`, { method: "POST" });
    await loadFollowupAssets();
  };

  const watchlistAssets = useMemo(
    () => followupAssets.filter((asset) => isWatchlistAsset(asset)),
    [followupAssets],
  );
  const viewedAssets = useMemo(
    () => followupAssets.filter((asset) => !isWatchlistAsset(asset)),
    [followupAssets],
  );

  const buildMonitorPath = (payload?: ActionPayload | null) =>
    payload
      ? `/investment/smart-monitor?intent=${encodeIntent({ type: "watchlist", payload })}`
      : "/investment/smart-monitor";

  const handleOpenMonitor = async (asset: FollowupAsset) => {
    if (!isWatchlistAsset(asset)) {
      void handlePromoteWatchlist(asset.id).catch(() => undefined);
    }
    navigate(buildMonitorPath(asset.action_payload));
  };

  const taskSection = (
    <div className={styles.moduleSection}>
      <TaskProgressBar
        current={task?.current ?? (task?.status === "success" ? task?.total ?? 1 : 0)}
        total={task?.total ?? 1}
        message={task?.message || "等待任务状态..."}
        tone={taskProgressTone(task)}
        showCounter={false}
      />
      {queuedTasks.length ? (
        <div className={styles.list}>
          <div className={styles.listItem}>
            <strong>排队任务</strong>
            <p className={styles.muted}>当前任务继续执行，以下任务会按顺序依次开始。</p>
            <div className={styles.list}>
              {queuedTasks.map((queuedTask, index) => (
                <div className={styles.listItem} key={queuedTask.id}>
                  <strong>
                    {index + 1}. {queuedTask.label || "深度分析任务"}
                  </strong>
                  <p className={styles.muted}>{queuedTask.message || "等待前序任务完成后开始执行"}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      ) : null}
      {task?.error ? <p className={styles.dangerText}>{task.error}</p> : null}
      {task?.status === "success" && task.result?.mode === "batch" ? (
        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>股票</th>
                <th>结果</th>
                <th>历史</th>
              </tr>
            </thead>
            <tbody>
              {(task.result.results ?? []).map((item) => (
                <tr key={`${item.symbol}-${item.record_id ?? "na"}`}>
                  <td>{item.symbol}</td>
                  <td>{item.success ? "成功" : item.error || "失败"}</td>
                  <td>
                    {item.record_id ? (
                      <button
                        className={styles.secondaryButton}
                        onClick={() => navigate(`/research/history?recordId=${item.record_id}`)}
                        type="button"
                      >
                        查看
                      </button>
                    ) : (
                      <span className={styles.muted}>无</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );

  return (
    <PageFrame
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      title="深度分析"
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        {section === "start" ? (
          <ModuleCard hideTitleOnMobile title="分析任务" summary="股票输入和分析师配置集中在同一模块内；深度分析当前固定按顺序执行。">
            <form className={styles.moduleSection} id="deep-analysis-form" onSubmit={handleSubmit}>
              <div className={styles.field}>
                <label htmlFor="stockInput">股票代码（支持逗号或换行分隔）</label>
                <textarea
                  id="stockInput"
                  onChange={(event) => setStockInput(event.target.value)}
                  placeholder="000001,600519,AAPL"
                  rows={4}
                  value={stockInput}
                />
              </div>
              <p className={styles.muted}>当前有任务运行时仍可继续提交，新任务会按顺序排队执行。</p>
              <div className={styles.field}>
                <label>分析师配置</label>
                <AnalystSelector
                  onChange={(next) =>
                    setAnalysts(analystKeysToConfig(next) as typeof analysts)
                  }
                  value={selectedAnalysts}
                />
              </div>
              <div className={styles.responsiveActionGrid}>
                <button className={styles.primaryButton} disabled={isSubmittingAnalysis} type="submit">
                  {isSubmittingAnalysis ? "提交中..." : "开始深度分析"}
                </button>
              </div>
            </form>

            {task ? taskSection : null}

            {singleRecord?.id ? (
              <div className={styles.moduleSection}>
                <div className={styles.listItem}>
                  <strong>分析已完成</strong>
                  <p className={styles.muted}>
                    {singleRecord.stock_name || singleRecord.symbol} 的详细报告不在当前页展开显示。
                  </p>
                  <div className={styles.actions}>
                    <button
                      className={styles.secondaryButton}
                      onClick={() => navigate(`/research/history?recordId=${singleRecord.id}`)}
                      type="button"
                    >
                      查看分析历史
                    </button>
                  </div>
                </div>
              </div>
            ) : null}
          </ModuleCard>
        ) : null}

        {section === "watchlist" ? (
          <ModuleCard hideTitleOnMobile title="关注中" summary="搜索、刷新和后续操作都收拢到同一模块。">
            <FollowupAssetList
              assets={watchlistAssets}
              emptyText="暂无关注中的股票"
              busy={isSubmittingAnalysis}
              followupSearch={followupSearch}
              pendingSymbol={pendingReAnalyzeSymbol}
              onOpenHistory={(recordId) => navigate(`/research/history?recordId=${recordId}`)}
              onOpenMonitor={(asset) => void handleOpenMonitor(asset)}
              onReAnalyze={(symbol) => void handleReAnalyze(symbol)}
              onRefresh={() => void loadFollowupAssets()}
              onSearchChange={setFollowupSearch}
            />
          </ModuleCard>
        ) : null}

        {section === "viewed" ? (
          <ModuleCard hideTitleOnMobile title="看过" summary="保留再次分析、进入盯盘和历史记录的主路径。">
            <FollowupAssetList
              assets={viewedAssets}
              emptyText="暂无看过的股票"
              busy={isSubmittingAnalysis}
              followupSearch={followupSearch}
              pendingSymbol={pendingReAnalyzeSymbol}
              onOpenHistory={(recordId) => navigate(`/research/history?recordId=${recordId}`)}
              onOpenMonitor={(asset) => void handleOpenMonitor(asset)}
              onReAnalyze={(symbol) => void handleReAnalyze(symbol)}
              onRefresh={() => void loadFollowupAssets()}
              onSearchChange={setFollowupSearch}
            />
          </ModuleCard>
        ) : null}
      </div>
    </PageFrame>
  );
}
