import { FormEvent, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { AnalystSelector } from "../../components/common/AnalystSelector";
import { ModuleCard } from "../../components/common/ModuleCard";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import type { ActionPayload } from "../../components/research/AnalysisActionButtons";
import {
  AnalysisDetailPanel,
  type AnalysisRecordDetail,
} from "../../components/research/AnalysisDetailPanel";
import { ANALYST_OPTIONS, analystConfigToKeys, analystKeysToConfig, type AnalystKey } from "../../constants/analysts";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { usePollingLoader } from "../../hooks/usePollingLoader";
import { ApiRequestError, apiFetch, apiFetchCached, buildQuery } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { encodeIntent } from "../../lib/intents";
import { useDeepAnalysisStore } from "../../stores/deepAnalysisStore";
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
  results?: TaskResultRow[];
}

interface TaskDetail {
  id: string;
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
    stock_name: result.symbol,
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
  onSearchChange,
  onRefresh,
  onOpenMonitor,
  onReAnalyze,
  onOpenHistory,
}: {
  assets: FollowupAsset[];
  emptyText: string;
  followupSearch: string;
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
              placeholder="搜索代码 / 名称 / 账户"
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
                <button className={styles.secondaryButton} onClick={() => onReAnalyze(asset.symbol)} type="button">
                  再次分析
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
  const batchMode = useDeepAnalysisStore((state) => state.batchMode);
  const setBatchMode = useDeepAnalysisStore((state) => state.setBatchMode);
  const maxWorkers = useDeepAnalysisStore((state) => state.maxWorkers);
  const setMaxWorkers = useDeepAnalysisStore((state) => state.setMaxWorkers);
  const analysts = useDeepAnalysisStore((state) => state.analysts);
  const setAnalysts = useDeepAnalysisStore((state) => state.setAnalysts);

  const [stockInput, setStockInput] = useState("");
  const [section, setSection] = useState<SectionKey>("start");
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [singleRecord, setSingleRecord] = useState<AnalysisRecordDetail | null>(null);
  const [followupAssets, setFollowupAssets] = useState<FollowupAsset[]>([]);
  const [followupSearch, setFollowupSearch] = useState("");
  const { message, error, clear, showError, showMessage } = usePageFeedback();

  const selectedAnalysts = useMemo(
    () => analystConfigToKeys(analysts as Partial<Record<AnalystKey, boolean>>),
    [analysts],
  );

  const loadTask = async () => {
    const latest = await apiFetch<TaskDetail | null>("/api/tasks/latest");
    setTask(latest);
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
    const data = await apiFetchCached<FollowupAsset[]>(
      `/api/followup-assets${buildQuery({
        search_term: followupSearch,
      })}`,
    );
    setFollowupAssets(data);
  };

  usePollingLoader({ load: loadTask, intervalMs: 2000 });

  useEffect(() => {
    void loadFollowupAssets();
  }, [followupSearch]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    clear();
    try {
      await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: stockInput,
          batch_mode: batchMode,
          max_workers: maxWorkers,
          analysts,
        }),
      });
      showMessage("分析任务已提交，正在准备执行...");
      await loadTask();
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "提交任务失败");
    }
  };

  const handleReAnalyze = async (symbol: string) => {
    setStockInput(symbol);
    clear();
    try {
      await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: symbol,
          batch_mode: "顺序分析",
          max_workers: maxWorkers,
          analysts,
        }),
      });
      showMessage(`已重新提交 ${symbol} 的分析任务`);
      setSection("start");
      await loadTask();
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "再次分析失败");
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
    clear();
    try {
      if (!isWatchlistAsset(asset)) {
        await handlePromoteWatchlist(asset.id);
        showMessage(`${asset.symbol} 已加入智能盯盘`);
      }
      navigate(buildMonitorPath(asset.action_payload));
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "加入盯盘失败");
    }
  };

  const taskSection = (
    <div className={styles.moduleSection}>
      <TaskProgressBar
        current={task?.current ?? (task?.status === "success" ? task?.total ?? 1 : 0)}
        total={task?.total ?? 1}
        message={task?.message || "等待任务状态..."}
        tone={taskProgressTone(task)}
      />
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
          <ModuleCard hideTitleOnMobile title="分析任务" summary="股票输入、分析模式、并发和分析师配置集中在同一模块内。">
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
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="batchMode">批量模式</label>
                  <select
                    id="batchMode"
                    onChange={(event) => setBatchMode(event.target.value as "顺序分析" | "多线程并行")}
                    value={batchMode}
                  >
                    <option value="顺序分析">顺序分析</option>
                    <option value="多线程并行">多线程并行</option>
                  </select>
                </div>
                {batchMode === "多线程并行" ? (
                  <div className={styles.field}>
                    <label htmlFor="maxWorkers">并行线程数</label>
                    <select
                      id="maxWorkers"
                      onChange={(event) => setMaxWorkers(Number(event.target.value) || 3)}
                      value={maxWorkers}
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
                  onChange={(next) =>
                    setAnalysts(analystKeysToConfig(next) as typeof analysts)
                  }
                  value={selectedAnalysts}
                />
              </div>
              <div className={styles.responsiveActionGrid}>
                <button className={styles.primaryButton} type="submit">
                  开始深度分析
                </button>
              </div>
            </form>

            {task ? taskSection : null}

            {singleRecord ? (
              <div className={styles.moduleSection}>
                <AnalysisDetailPanel record={singleRecord} />
              </div>
            ) : null}
          </ModuleCard>
        ) : null}

        {section === "watchlist" ? (
          <ModuleCard hideTitleOnMobile title="关注中" summary="搜索、刷新和后续操作都收拢到同一模块。">
            <FollowupAssetList
              assets={watchlistAssets}
              emptyText="暂无关注中的股票"
              followupSearch={followupSearch}
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
              followupSearch={followupSearch}
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
