import { useEffect, useState } from "react";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame, type PageFrameSectionTab } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import { usePollingLoader } from "../../hooks/usePollingLoader";
import { ApiRequestError, apiFetch } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import styles from "../ConsolePage.module.scss";

interface SchedulerStatus {
  running: boolean;
  enabled?: boolean;
  schedule_time?: string;
  max_workers?: number;
  next_run_time?: string | null;
  last_run_time?: string | null;
}

interface LifecycleItem {
  sector_name?: string;
  heat_score?: number;
  lifecycle_stage?: string;
  delta_1?: number | null;
  delta_2?: number | null;
  action_hint?: string;
  defense_line_type?: string;
  selection_veto?: boolean;
  source_type?: string;
  observation_count?: number;
  window_size_used?: number;
  lifecycle_details?: Record<string, Record<string, unknown>>;
  trajectory?: Array<{ day_offset?: number; score?: number }>;
}

interface LatestLifecycle {
  available?: boolean;
  analysis_id?: number;
  analysis_date?: string;
  daily_heat_panel?: {
    available?: boolean;
    board_date?: string;
    total_count?: number;
    items?: Array<Record<string, unknown>>;
  };
  summary?: {
    counts?: Record<string, number>;
    startup?: LifecycleItem[];
    explosive?: LifecycleItem[];
    decay?: LifecycleItem[];
  };
}

interface SmartSelectionResultItem {
  symbol?: string;
  name?: string;
  primary_sector?: string;
  score?: number;
  reason?: string;
  lifecycle_stage?: string;
  defense_line_type?: string;
  trajectory?: Array<{ day_offset?: number; score?: number }>;
  delta_1?: number | null;
  delta_2?: number | null;
  anticipation_score?: number;
  washout_score?: number;
  shrinkage_score?: number;
  relative_strength_score?: number;
  tail_confirmation_score?: number;
  distribution_penalty?: number;
}

interface SmartSelectionRun {
  run_id: string;
  status: string;
  message: string;
  current?: number;
  total?: number;
  error?: string;
  created_at?: string;
  started_at?: string;
  finished_at?: string;
  warnings?: string[];
  sector_report_reused?: boolean;
  result?: {
    lifecycle_summary?: LatestLifecycle["summary"];
    watch_pool_size?: number;
    observed_startup_candidates?: SmartSelectionResultItem[];
    ranked_action_candidates?: SmartSelectionResultItem[];
    final_selected?: SmartSelectionResultItem[];
    excluded_by_lifecycle_veto?: SmartSelectionResultItem[];
  };
}

interface WatchPoolItem {
  symbol?: string;
  name?: string;
  source_sector?: string;
  lifecycle_stage?: string;
  defense_line_type?: string;
  reason?: string;
  trajectory?: Array<{ day_offset?: number; score?: number }>;
  last_seen_at?: string;
}

interface HubAssetLite {
  id: number;
  symbol: string;
  name: string;
  status: string;
  manual_pin?: boolean;
  pool_reason?: string;
}

interface OverviewPayload {
  latest_run?: SmartSelectionRun | null;
  watch_pool_count?: number;
  lifecycle?: LatestLifecycle | null;
  daily_heat_panel?: {
    available?: boolean;
    board_date?: string;
    total_count?: number;
    items?: Array<Record<string, unknown>>;
  } | null;
  scheduler?: SchedulerStatus | null;
}

type LifecycleDetailKey = "startup" | "explosive" | "decay" | "watch-pool";
type PageSectionKey = "overview" | "pipeline" | "scheduler";

const pageSectionTabs: PageFrameSectionTab[] = [
  { key: "overview", label: "总览" },
  { key: "pipeline", label: "选股流程" },
  { key: "scheduler", label: "调度设置" },
];

function isPendingRunStatus(status?: string) {
  return status === "queued" || status === "running";
}

function taskProgressTone(task: SmartSelectionRun | null): "running" | "success" | "danger" {
  if (!task || isPendingRunStatus(task.status)) {
    return "running";
  }
  if (task.status === "success") {
    return "success";
  }
  return "danger";
}

function taskStatusMeta(task: SmartSelectionRun | null): { label: string; tone: "default" | "success" | "warning" | "danger" } {
  if (!task) {
    return { label: "暂无任务", tone: "default" };
  }
  if (task.status === "success") {
    return { label: "已完成", tone: "success" };
  }
  if (task.status === "failed") {
    return { label: "执行失败", tone: "danger" };
  }
  return { label: "执行中", tone: "warning" };
}

function formatTrajectory(items?: Array<{ score?: number }>) {
  if (!items?.length) {
    return "-";
  }
  return items.map((item) => String(item.score ?? 0)).join(" -> ");
}

function stageLabel(stage?: string) {
  if (stage === "startup") return "启动期";
  if (stage === "explosive") return "爆发期";
  if (stage === "decay") return "衰退期";
  return "中性";
}

function formatScore(value?: number | null, digits = 2) {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return "-";
  }
  return Number(value).toFixed(digits);
}

export function SmartSelectionPage() {
  const [section, setSection] = useState<PageSectionKey>("overview");
  const [overview, setOverview] = useState<OverviewPayload | null>(null);
  const [latestRun, setLatestRun] = useState<SmartSelectionRun | null>(null);
  const [watchPool, setWatchPool] = useState<WatchPoolItem[]>([]);
  const [hubAssets, setHubAssets] = useState<HubAssetLite[]>([]);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [scheduleTime, setScheduleTime] = useState("14:30");
  const [maxWorkers, setMaxWorkers] = useState("6");
  const [schedulerEnabledDraft, setSchedulerEnabledDraft] = useState(false);
  const [schedulerDirty, setSchedulerDirty] = useState(false);
  const [activeLifecycleDetail, setActiveLifecycleDetail] = useState<LifecycleDetailKey | null>("startup");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [isSubmittingRun, setIsSubmittingRun] = useState(false);
  const [isSavingScheduler, setIsSavingScheduler] = useState(false);
  const [focusUpdatingSymbol, setFocusUpdatingSymbol] = useState("");

  const loadOverview = async () => {
    const [overviewData, watchPoolData, hubAssetData] = await Promise.all([
      apiFetch<OverviewPayload>("/api/smart-selection/overview"),
      apiFetch<WatchPoolItem[]>("/api/smart-selection/watch-pool"),
      apiFetch<HubAssetLite[]>("/api/watchlist-hub/assets"),
    ]);
    setOverview(overviewData);
    setLatestRun(overviewData.latest_run ?? null);
    setScheduler(overviewData.scheduler ?? null);
    setWatchPool(watchPoolData);
    setHubAssets(hubAssetData);
    setError("");
  };

  const refreshOverview = async () => {
    try {
      await loadOverview();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "智能选股数据加载失败");
    }
  };

  usePollingLoader({ load: refreshOverview, intervalMs: 3000 });

  useEffect(() => {
    if (schedulerDirty) {
      return;
    }
    setScheduleTime(scheduler?.schedule_time || "14:30");
    setMaxWorkers(String(Math.max(1, scheduler?.max_workers ?? 6)));
    setSchedulerEnabledDraft(Boolean(scheduler?.enabled));
  }, [scheduler?.enabled, scheduler?.max_workers, scheduler?.schedule_time, schedulerDirty]);

  const finalSelected = latestRun?.result?.final_selected ?? [];
  const observedStartupCandidates = latestRun?.result?.observed_startup_candidates ?? [];
  const rankedActionCandidates = latestRun?.result?.ranked_action_candidates ?? [];
  const vetoedCandidates = latestRun?.result?.excluded_by_lifecycle_veto ?? [];
  const lifecycleCounts = overview?.lifecycle?.summary?.counts ?? {};
  const hubAssetLookup = new Map(hubAssets.map((item) => [item.symbol, item]));
  const taskStatus = taskStatusMeta(latestRun);
  const taskPending = isPendingRunStatus(latestRun?.status);
  const taskProgressCurrent = latestRun?.current ?? (latestRun?.status === "success" ? latestRun?.total ?? 100 : 0);
  const lifecycleDetailTitle =
    activeLifecycleDetail === "startup"
      ? "启动期板块"
      : activeLifecycleDetail === "explosive"
        ? "爆发期板块"
        : activeLifecycleDetail === "decay"
          ? "衰退期板块"
          : activeLifecycleDetail === "watch-pool"
            ? "MA10 观察池"
            : "明细面板";

  useEffect(() => {
    if (!activeLifecycleDetail) {
      return;
    }
    if (activeLifecycleDetail === "watch-pool") {
      if (!watchPool.length) {
        setActiveLifecycleDetail("startup");
      }
      return;
    }
    const detailItems = overview?.lifecycle?.summary?.[activeLifecycleDetail] ?? [];
    if (!detailItems.length) {
      if ((overview?.lifecycle?.summary?.startup ?? []).length) {
        setActiveLifecycleDetail("startup");
      } else if ((overview?.lifecycle?.summary?.explosive ?? []).length) {
        setActiveLifecycleDetail("explosive");
      } else if ((overview?.lifecycle?.summary?.decay ?? []).length) {
        setActiveLifecycleDetail("decay");
      } else if (watchPool.length) {
        setActiveLifecycleDetail("watch-pool");
      } else {
        setActiveLifecycleDetail(null);
      }
    }
  }, [activeLifecycleDetail, overview?.lifecycle?.summary, watchPool]);

  const resetSchedulerDrafts = () => {
    setScheduleTime(scheduler?.schedule_time || "14:30");
    setMaxWorkers(String(Math.max(1, scheduler?.max_workers ?? 6)));
    setSchedulerEnabledDraft(Boolean(scheduler?.enabled));
    setSchedulerDirty(false);
  };

  const handleRun = async () => {
    setMessage("");
    setError("");
    setIsSubmittingRun(true);
    try {
      const result = await apiFetch<{ run_id: string }>("/api/smart-selection/runs", {
        method: "POST",
        body: JSON.stringify({ trigger_source: "manual" }),
      });
      setMessage(`智能选股任务已提交：${result.run_id}`);
      await loadOverview();
      setSection("overview");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交智能选股任务失败");
    } finally {
      setIsSubmittingRun(false);
    }
  };

  const handleSaveScheduler = async () => {
    setMessage("");
    setError("");
    setIsSavingScheduler(true);
    try {
      const data = await apiFetch<SchedulerStatus>("/api/smart-selection/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          enabled: schedulerEnabledDraft,
          schedule_time: scheduleTime,
          max_workers: Math.max(1, Number(maxWorkers) || 1),
        }),
      });
      setScheduler(data);
      setScheduleTime(data.schedule_time || "14:30");
      setMaxWorkers(String(Math.max(1, data.max_workers ?? 6)));
      setSchedulerEnabledDraft(Boolean(data.enabled));
      setSchedulerDirty(false);
      setMessage(
        data.enabled
          ? `智能选股调度已更新为每天 ${data.schedule_time || scheduleTime}，并发 ${Math.max(1, data.max_workers ?? 6)}`
          : "智能选股调度已停止",
      );
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新调度失败");
    } finally {
      setIsSavingScheduler(false);
    }
  };

  const renderLifecycleDetail = () => {
    if (!activeLifecycleDetail) {
      return <div className={styles.muted}>当前没有可展示的明细数据</div>;
    }
    if (activeLifecycleDetail === "watch-pool") {
      if (!watchPool.length) {
        return <div className={styles.muted}>暂无 MA10 观察池数据</div>;
      }
      return (
        <div className={styles.selectionCompactList}>
          {watchPool.map((item) => (
            <div className={styles.selectionCompactItem} key={`${item.symbol}-${item.last_seen_at}`}>
              <strong>{item.name || item.symbol}</strong>
              <div className={styles.muted}>
                {item.symbol} | {item.source_sector || "-"} | {stageLabel(item.lifecycle_stage)} | {item.defense_line_type || "NONE"}
              </div>
              <div className={styles.selectionCompactReason}>
                轨迹 {formatTrajectory(item.trajectory)} | 最近命中 {formatDateTime(item.last_seen_at, "-")}
              </div>
              <div className={styles.selectionCompactReason}>{item.reason || "暂无说明"}</div>
            </div>
          ))}
        </div>
      );
    }

    const detailItems = overview?.lifecycle?.summary?.[activeLifecycleDetail] ?? [];
    if (!detailItems.length) {
      return <div className={styles.muted}>暂无数据</div>;
    }
    return (
      <div className={styles.selectionCompactList}>
        {detailItems.map((item) => (
          <div className={styles.selectionCompactItem} key={`${activeLifecycleDetail}-${item.sector_name}`}>
            <strong>{item.sector_name}</strong>
            <div className={styles.muted}>
              热度 {formatScore(item.heat_score)} | Δ1 {formatScore(item.delta_1, 1)} | Δ2 {formatScore(item.delta_2, 1)} | 防守线{" "}
              {item.defense_line_type || "NONE"}
            </div>
            <div className={styles.selectionCompactReason}>
              轨迹 {formatTrajectory(item.trajectory)} | 来源 {item.source_type || "-"} | 主窗口 {item.window_size_used || "-"} 日
            </div>
            <div className={styles.selectionCompactReason}>{item.action_hint || "暂无动作提示"}</div>
          </div>
        ))}
      </div>
    );
  };

  const renderFinalSelectionCard = () => (
    <section className={`${styles.selectionResultCard} ${styles.selectionResultWide}`}>
      <div className={styles.cardHeader}>
        <div>
          <strong>最终执行清单</strong>
          <p className={styles.helperText}>这是尾盘执行池。启动期观察和爆发期候选都在流程页展开，避免把“跟踪”和“执行”混在一起。</p>
        </div>
      </div>
      <div className={styles.selectionCompactList}>
        {finalSelected.map((item) => {
          const symbol = item.symbol || "";
          const boundAsset = hubAssetLookup.get(symbol);
          const inFocus = boundAsset?.status === "focus" || boundAsset?.manual_pin;
          return (
            <div className={styles.selectionCompactItem} key={symbol}>
              <div className={styles.selectionCardHeader}>
                <div>
                  <strong>{item.name || symbol}</strong>
                  <div className={styles.muted}>{symbol}</div>
                </div>
                <button
                  aria-label={inFocus ? `移出备选关注 ${symbol}` : `加入备选关注 ${symbol}`}
                  className={inFocus ? styles.researchHubStarButtonActive : styles.researchHubStarButton}
                  disabled={!boundAsset || focusUpdatingSymbol === symbol}
                  onClick={async () => {
                    if (!boundAsset) {
                      return;
                    }
                    setMessage("");
                    setError("");
                    setFocusUpdatingSymbol(symbol);
                    try {
                      await apiFetch(`/api/watchlist-hub/assets/${boundAsset.id}`, {
                        method: "PATCH",
                        body: JSON.stringify(
                          inFocus
                            ? {
                                target_status: "research",
                                manual_pin: false,
                                pool_reason: "智能选股页手动移出备选关注，回到研究池",
                              }
                            : {
                                target_status: "focus",
                                manual_pin: true,
                                pool_reason: item.reason || "智能选股页手动加入备选关注",
                              },
                        ),
                      });
                      setMessage(inFocus ? `${symbol} 已移出备选关注并回到研究池` : `${symbol} 已加入备选关注并自动加入盯盘`);
                      await loadOverview();
                    } catch (requestError) {
                      setError(requestError instanceof ApiRequestError ? requestError.message : "更新备选关注失败");
                    } finally {
                      setFocusUpdatingSymbol("");
                    }
                  }}
                  title={boundAsset ? (inFocus ? "移出备选关注" : "加入备选关注") : "该标的未绑定到投研档案"}
                  type="button"
                >
                  {inFocus ? "★" : "☆"}
                </button>
              </div>
              <div className={styles.selectionMetaRow}>
                <span>{item.primary_sector || "-"}</span>
                <span>{stageLabel(item.lifecycle_stage)}</span>
                <span>{item.defense_line_type || "NONE"}</span>
                <span>综合分 {formatScore(item.score)}</span>
              </div>
              <div className={styles.selectionCompactReason}>
                预期差 {formatScore(item.anticipation_score, 1)} / 缩量 {formatScore(item.shrinkage_score, 1)} / 相对强度{" "}
                {formatScore(item.relative_strength_score, 1)} / 尾盘确认 {formatScore(item.tail_confirmation_score, 1)}
              </div>
              <div className={styles.selectionCompactReason}>{item.reason || "暂无说明"}</div>
              {!boundAsset ? <div className={styles.selectionCompactReason}>未绑定投研档案，当前不能直接加入备选关注。</div> : null}
            </div>
          );
        })}
        {!finalSelected.length ? <div className={styles.muted}>暂无最终执行清单</div> : null}
      </div>
    </section>
  );

  const renderOverview = () => (
    <div className={styles.stack}>
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <h2>运行总览</h2>
            <p className={styles.helperText}>先看最新任务状态和最终执行池，再回头看上游生命周期与候选流转。</p>
          </div>
        </div>
        <div className={styles.actions}>
          <button className={styles.primaryButton} disabled={taskPending || isSubmittingRun} onClick={() => void handleRun()} type="button">
            {taskPending ? "智能选股执行中..." : isSubmittingRun ? "提交中..." : "开始智能选股"}
          </button>
        </div>

        <div className={styles.noticeMeta}>
          <div>
            <strong>最新任务</strong>
            <div className={styles.muted}>
              最新完成：{formatDateTime(latestRun?.finished_at || latestRun?.created_at, "-")} |{" "}
              {latestRun?.sector_report_reused ? "复用 12 小时内智策报告" : "按最新智策报告执行"}
            </div>
          </div>
          <StatusBadge label={taskStatus.label} tone={taskStatus.tone} />
        </div>

        <TaskProgressBar
          current={taskProgressCurrent}
          total={latestRun?.total ?? 100}
          message={latestRun?.message || "等待智能选股任务状态..."}
          tone={taskProgressTone(latestRun)}
        />

        {latestRun?.error ? <div className={styles.dangerText}>{latestRun.error}</div> : null}
        {latestRun?.warnings?.length ? (
          <div className={styles.list}>
            {latestRun.warnings.map((warning) => (
              <div className={styles.listItem} key={warning}>
                <strong>执行告警</strong>
                <div className={styles.muted}>{warning}</div>
              </div>
            ))}
          </div>
        ) : null}
      </section>

      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <h2>关键指标</h2>
            <p className={styles.helperText}>把“最终执行”“观察候选”“市场依据”拆开，避免在同一个列表里做不同决策。</p>
          </div>
        </div>
        <div className={styles.summaryMetricGrid}>
          <button className={styles.historySummaryCellButtonActive} onClick={() => setSection("overview")} type="button">
            <span>最终执行</span>
            <strong>{finalSelected.length}</strong>
            <div className={styles.muted}>尾盘执行池</div>
          </button>
          <button className={styles.historySummaryCellButton} onClick={() => setSection("pipeline")} type="button">
            <span>启动期观察</span>
            <strong>{observedStartupCandidates.length}</strong>
            <div className={styles.muted}>先观察，再跟踪</div>
          </button>
          <button className={styles.historySummaryCellButton} onClick={() => setSection("pipeline")} type="button">
            <span>爆发期候选</span>
            <strong>{rankedActionCandidates.length}</strong>
            <div className={styles.muted}>进入执行筛选</div>
          </button>
          <button
            className={styles.historySummaryCellButton}
            onClick={() => {
              setSection("pipeline");
              setActiveLifecycleDetail("watch-pool");
            }}
            type="button"
          >
            <span>MA10 观察池</span>
            <strong>{overview?.watch_pool_count ?? 0}</strong>
            <div className={styles.muted}>保留启动期线索</div>
          </button>
        </div>
      </section>

      <section className={styles.card}>
        <div className={styles.selectionResultGrid}>
          {renderFinalSelectionCard()}

          <section className={`${styles.selectionResultCard} ${styles.selectionResultWide}`}>
            <div className={styles.cardHeader}>
              <div>
                <strong>当日热度面板</strong>
                <p className={styles.helperText}>
                  面板日期 {formatDateTime(overview?.daily_heat_panel?.board_date, "-")}，用于确认市场热度是否和生命周期判断一致。
                </p>
              </div>
            </div>
            <div className={styles.selectionCompactList}>
              {(overview?.daily_heat_panel?.items ?? []).slice(0, 8).map((item, index) => (
                <div className={styles.selectionCompactItem} key={`daily-heat-${index}`}>
                  <strong>{String(item.sector_name ?? "-")}</strong>
                  <div className={styles.muted}>
                    热度 {formatScore(Number(item.heat_score ?? 0))} | 涨跌幅 {formatScore(Number(item.change_pct ?? 0))}% | 来源{" "}
                    {String(item.source_type ?? "-")}
                  </div>
                </div>
              ))}
              {!(overview?.daily_heat_panel?.items ?? []).length ? <div className={styles.muted}>暂无当日热度面板</div> : null}
            </div>
          </section>
        </div>
      </section>

      <details className={styles.historyDetailPanel}>
        <summary className={styles.historyDetailSummary}>生命周期观察</summary>
        <div className={styles.historyDetailPanelBody}>
          <div className={styles.cardHeader} style={{ marginBottom: 12 }}>
            <div>
              <strong>生命周期依据</strong>
              <p className={styles.helperText}>
                智策时间 {formatDateTime(overview?.lifecycle?.analysis_date, "-")}，这是智能选股最上游的市场判断。
              </p>
            </div>
          </div>
          <div className={styles.summaryMetricGrid}>
            <button
              className={activeLifecycleDetail === "startup" ? styles.historySummaryCellButtonActive : styles.historySummaryCellButton}
              onClick={() => setActiveLifecycleDetail("startup")}
              type="button"
            >
              <span>启动期</span>
              <strong>{lifecycleCounts.startup ?? 0}</strong>
              <div className={styles.muted}>适合观察和跟踪</div>
            </button>
            <button
              className={activeLifecycleDetail === "explosive" ? styles.historySummaryCellButtonActive : styles.historySummaryCellButton}
              onClick={() => setActiveLifecycleDetail("explosive")}
              type="button"
            >
              <span>爆发期</span>
              <strong>{lifecycleCounts.explosive ?? 0}</strong>
              <div className={styles.muted}>适合进入候选</div>
            </button>
            <button
              className={activeLifecycleDetail === "decay" ? styles.historySummaryCellButtonActive : styles.historySummaryCellButton}
              onClick={() => setActiveLifecycleDetail("decay")}
              type="button"
            >
              <span>衰退期</span>
              <strong>{lifecycleCounts.decay ?? 0}</strong>
              <div className={styles.muted}>一票否决来源</div>
            </button>
            <button
              className={activeLifecycleDetail === "watch-pool" ? styles.historySummaryCellButtonActive : styles.historySummaryCellButton}
              onClick={() => setActiveLifecycleDetail("watch-pool")}
              type="button"
            >
              <span>MA10观察池</span>
              <strong>{overview?.watch_pool_count ?? 0}</strong>
              <div className={styles.muted}>启动期延续观察</div>
            </button>
          </div>
          <section className={`${styles.selectionResultCard} ${styles.selectionResultWide}`}>
            <strong>{lifecycleDetailTitle}</strong>
            {renderLifecycleDetail()}
          </section>
        </div>
      </details>
    </div>
  );

  const renderPipeline = () => (
    <div className={styles.stack}>
      <section className={styles.card}>
        <div className={styles.selectionResultGrid}>
          <section className={styles.selectionResultCard}>
            <div className={styles.cardHeader}>
              <div>
                <strong>启动期观察候选</strong>
                <p className={styles.helperText}>这些标的不直接进执行池，保留到 MA10 观察池持续跟踪。</p>
              </div>
            </div>
            <div className={styles.selectionCompactList}>
              {observedStartupCandidates.map((item) => (
                <div className={styles.selectionCompactItem} key={`startup-${item.symbol}`}>
                  <strong>{item.name || item.symbol}</strong>
                  <div className={styles.muted}>
                    {item.symbol} | {item.primary_sector || "-"} | {item.defense_line_type || "MA10"}
                  </div>
                  <div className={styles.selectionCompactReason}>
                    轨迹 {formatTrajectory(item.trajectory)} | 综合分 {formatScore(item.score)}
                  </div>
                  <div className={styles.selectionCompactReason}>{item.reason || "暂无说明"}</div>
                </div>
              ))}
              {!observedStartupCandidates.length ? <div className={styles.muted}>暂无启动期候选</div> : null}
            </div>
          </section>

          <section className={styles.selectionResultCard}>
            <div className={styles.cardHeader}>
              <div>
                <strong>爆发期候选</strong>
                <p className={styles.helperText}>这是最终执行清单的上游候选池，还未经过尾盘执行阈值和板块集中度约束。</p>
              </div>
            </div>
            <div className={styles.selectionCompactList}>
              {rankedActionCandidates.map((item) => (
                <div className={styles.selectionCompactItem} key={`action-${item.symbol}`}>
                  <strong>{item.name || item.symbol}</strong>
                  <div className={styles.muted}>
                    {item.symbol} | {item.primary_sector || "-"} | 综合分 {formatScore(item.score)}
                  </div>
                  <div className={styles.selectionCompactReason}>
                    缩量 {formatScore(item.shrinkage_score, 1)} / 相对强度 {formatScore(item.relative_strength_score, 1)} / 尾盘确认{" "}
                    {formatScore(item.tail_confirmation_score, 1)}
                  </div>
                  <div className={styles.selectionCompactReason}>{item.reason || "暂无说明"}</div>
                </div>
              ))}
              {!rankedActionCandidates.length ? <div className={styles.muted}>暂无爆发期候选</div> : null}
            </div>
          </section>

          <section className={styles.selectionResultCard}>
            <div className={styles.cardHeader}>
              <div>
                <strong>生命周期否决</strong>
                <p className={styles.helperText}>这些标的是研究池里有机会但所在板块处于衰退期，因此直接否决。</p>
              </div>
            </div>
            <div className={styles.selectionCompactList}>
              {vetoedCandidates.map((item) => (
                <div className={styles.selectionCompactItem} key={`veto-${item.symbol}`}>
                  <strong>{item.name || item.symbol}</strong>
                  <div className={styles.muted}>
                    {item.symbol} | {item.primary_sector || "-"} | {stageLabel(item.lifecycle_stage)}
                  </div>
                  <div className={styles.selectionCompactReason}>{item.reason || "-"}</div>
                </div>
              ))}
              {!vetoedCandidates.length ? <div className={styles.muted}>暂无生命周期否决项</div> : null}
            </div>
          </section>

          <section className={styles.selectionResultCard}>
            <div className={styles.cardHeader}>
              <div>
                <strong>MA10 观察池</strong>
                <p className={styles.helperText}>这里保留启动期线索，避免把“还没到执行时点”的标的误当成当日结果。</p>
              </div>
            </div>
            <div className={styles.selectionCompactList}>
              {watchPool.map((item) => (
                <div className={styles.selectionCompactItem} key={`${item.symbol}-${item.last_seen_at}`}>
                  <strong>{item.name || item.symbol}</strong>
                  <div className={styles.muted}>
                    {item.symbol} | {item.source_sector || "-"} | {stageLabel(item.lifecycle_stage)} | {item.defense_line_type || "NONE"}
                  </div>
                  <div className={styles.selectionCompactReason}>
                    轨迹 {formatTrajectory(item.trajectory)} | 最近命中 {formatDateTime(item.last_seen_at, "-")}
                  </div>
                  <div className={styles.selectionCompactReason}>{item.reason || "暂无说明"}</div>
                </div>
              ))}
              {!watchPool.length ? <div className={styles.muted}>暂无 MA10 观察池数据</div> : null}
            </div>
          </section>
        </div>
      </section>
    </div>
  );

  const renderScheduler = () => (
    <div className={styles.stack}>
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <h2>调度配置</h2>
            <p className={styles.helperText}>调度只保留开关和定时时间，避免把运行状态和执行参数放进配置区。</p>
          </div>
        </div>

        <div className={styles.schedulerControl}>
          <label className={styles.switchField}>
            <div className={styles.switchBody}>
              <span className={styles.switchLabel}>启用定时智能选股</span>
              <span className={styles.switchDescription}>先调整时间，再统一保存，避免轮询刷新覆盖正在输入的配置。</span>
            </div>
            <span className={styles.switchControl}>
              <input
                checked={schedulerEnabledDraft}
                disabled={isSavingScheduler}
                onChange={(event) => {
                  setSchedulerEnabledDraft(event.target.checked);
                  setSchedulerDirty(true);
                }}
                type="checkbox"
              />
              <span aria-hidden="true" className={styles.switchTrack}>
                <span className={styles.switchThumb} />
              </span>
            </span>
          </label>

          <div className={styles.formGrid}>
            <div className={styles.field}>
              <label htmlFor="smartSelectionScheduleTime">定时时间</label>
              <input
                id="smartSelectionScheduleTime"
                onChange={(event) => {
                  setScheduleTime(event.target.value);
                  setSchedulerDirty(true);
                }}
                type="time"
                value={scheduleTime}
              />
            </div>
          </div>

          <div className={styles.schedulerControlActions}>
            <button className={styles.primaryButton} disabled={isSavingScheduler || !schedulerDirty} onClick={() => void handleSaveScheduler()} type="button">
              {isSavingScheduler ? "保存中..." : schedulerEnabledDraft ? "保存并启用" : "保存并停用"}
            </button>
            <button className={styles.tertiaryButton} disabled={isSavingScheduler || !schedulerDirty} onClick={resetSchedulerDrafts} type="button">
              还原线上配置
            </button>
          </div>

          {schedulerDirty ? <div className={styles.muted}>当前表单有未保存修改。</div> : null}
        </div>
      </section>
    </div>
  );

  return (
    <PageFrame
      activeSectionKey={section}
      onSectionChange={(key) => setSection(key as PageSectionKey)}
      sectionTabs={pageSectionTabs}
      title="智能选股"
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />
        {section === "overview" ? renderOverview() : section === "pipeline" ? renderPipeline() : renderScheduler()}
      </div>
    </PageFrame>
  );
}
