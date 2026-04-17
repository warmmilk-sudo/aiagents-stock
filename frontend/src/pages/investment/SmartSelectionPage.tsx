import { useEffect, useState } from "react";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
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

function taskProgressTone(task: SmartSelectionRun | null): "running" | "success" | "danger" {
  if (!task || task.status === "queued" || task.status === "running") {
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

export function SmartSelectionPage() {
  type LifecycleDetailKey = "startup" | "explosive" | "decay" | "watch-pool";
  const [overview, setOverview] = useState<OverviewPayload | null>(null);
  const [latestRun, setLatestRun] = useState<SmartSelectionRun | null>(null);
  const [watchPool, setWatchPool] = useState<WatchPoolItem[]>([]);
  const [hubAssets, setHubAssets] = useState<HubAssetLite[]>([]);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [scheduleTime, setScheduleTime] = useState("14:30");
  const [maxWorkers, setMaxWorkers] = useState("6");
  const [activeLifecycleDetail, setActiveLifecycleDetail] = useState<LifecycleDetailKey | null>(null);
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
    setScheduleTime(overviewData.scheduler?.schedule_time || "14:30");
    setMaxWorkers(String(Math.max(1, overviewData.scheduler?.max_workers ?? 6)));
    setWatchPool(watchPoolData);
    setHubAssets(hubAssetData);
  };

  usePollingLoader({ load: loadOverview, intervalMs: 3000 });

  useEffect(() => {
    void loadOverview().catch((requestError) => {
      setError(requestError instanceof ApiRequestError ? requestError.message : "智能选股数据加载失败");
    });
  }, []);

  const finalSelected = latestRun?.result?.final_selected ?? [];
  const lifecycleCounts = overview?.lifecycle?.summary?.counts ?? {};
  const draftMaxWorkers = Number(maxWorkers);
  const displayMaxWorkers =
    scheduler?.max_workers ??
    (Number.isFinite(draftMaxWorkers) && draftMaxWorkers > 0 ? Math.max(1, draftMaxWorkers) : 6);

  useEffect(() => {
    if (!activeLifecycleDetail) {
      return;
    }
    if (activeLifecycleDetail === "watch-pool") {
      if (!watchPool.length) {
        setActiveLifecycleDetail(null);
      }
      return;
    }
    const detailItems = overview?.lifecycle?.summary?.[activeLifecycleDetail] ?? [];
    if (!detailItems.length) {
      setActiveLifecycleDetail(null);
    }
  }, [activeLifecycleDetail, overview?.lifecycle?.summary, watchPool]);

  const handleRun = async () => {
    setMessage("");
    setError("");
    setIsSubmittingRun(true);
    try {
      const result = await apiFetch<{ run_id: string }>("/api/smart-selection/runs", { method: "POST", body: JSON.stringify({ trigger_source: "manual" }) });
      setMessage(`智能选股任务已提交：${result.run_id}`);
      await loadOverview();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交智能选股任务失败");
    } finally {
      setIsSubmittingRun(false);
    }
  };

  const handleSaveScheduler = async (enabled: boolean) => {
    setMessage("");
    setError("");
    setIsSavingScheduler(true);
    try {
      const data = await apiFetch<SchedulerStatus>("/api/smart-selection/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          enabled,
          schedule_time: scheduleTime,
          max_workers: Math.max(1, Number(maxWorkers) || 1),
        }),
      });
      setScheduler(data);
      setMaxWorkers(String(Math.max(1, data.max_workers ?? 6)));
      setMessage(enabled ? `智能选股调度已更新为每天 ${scheduleTime}，并发 ${Math.max(1, data.max_workers ?? 6)}` : "智能选股调度已停止");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新调度失败");
    } finally {
      setIsSavingScheduler(false);
    }
  };

  const taskStatus = taskStatusMeta(latestRun);
  const hubAssetLookup = new Map(hubAssets.map((item) => [item.symbol, item]));
  const lifecycleDetailTitle =
    activeLifecycleDetail === "startup"
      ? "启动期详情"
      : activeLifecycleDetail === "explosive"
        ? "爆发期详情"
        : activeLifecycleDetail === "decay"
          ? "衰退期详情"
          : activeLifecycleDetail === "watch-pool"
            ? "MA10 观察池详情"
            : "";

  return (
    <PageFrame title="智能选股">
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.primaryButton} disabled={isSubmittingRun} onClick={() => void handleRun()} type="button">
              {isSubmittingRun ? "提交中..." : "开始智能选股"}
            </button>
          </div>
        </section>

        <section className={styles.card}>
          <div className={styles.noticeMeta}>
            <div>
              <strong>任务状态</strong>
              <div className={styles.muted}>最新任务：{latestRun?.run_id || "暂无"}</div>
            </div>
            <StatusBadge label={taskStatus.label} tone={taskStatus.tone} />
          </div>
          <TaskProgressBar
            current={latestRun?.current ?? 0}
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
              <h2>生命周期总览</h2>
              <p className={styles.helperText}>最新智策时间：{formatDateTime(overview?.lifecycle?.analysis_date, "-")}</p>
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
            </button>
            <button
              className={activeLifecycleDetail === "explosive" ? styles.historySummaryCellButtonActive : styles.historySummaryCellButton}
              onClick={() => setActiveLifecycleDetail("explosive")}
              type="button"
            >
              <span>爆发期</span>
              <strong>{lifecycleCounts.explosive ?? 0}</strong>
            </button>
            <button
              className={activeLifecycleDetail === "decay" ? styles.historySummaryCellButtonActive : styles.historySummaryCellButton}
              onClick={() => setActiveLifecycleDetail("decay")}
              type="button"
            >
              <span>衰退期</span>
              <strong>{lifecycleCounts.decay ?? 0}</strong>
            </button>
            <button
              className={activeLifecycleDetail === "watch-pool" ? styles.historySummaryCellButtonActive : styles.historySummaryCellButton}
              onClick={() => setActiveLifecycleDetail("watch-pool")}
              type="button"
            >
              <span>MA10观察池</span>
              <strong>{overview?.watch_pool_count ?? 0}</strong>
            </button>
          </div>
          <section className={`${styles.selectionResultCard} ${styles.selectionResultWide}`}>
            <strong>{lifecycleDetailTitle || "明细面板"}</strong>
            {!activeLifecycleDetail ? <div className={styles.muted}>点击上方生命周期卡片查看具体内容</div> : null}
            {activeLifecycleDetail && activeLifecycleDetail !== "watch-pool" ? (
              (overview?.lifecycle?.summary?.[activeLifecycleDetail] ?? []).length ? (
                <div className={styles.selectionCompactList}>
                  {(overview?.lifecycle?.summary?.[activeLifecycleDetail] ?? []).map((item) => (
                    <div className={styles.selectionCompactItem} key={`${activeLifecycleDetail}-${item.sector_name}`}>
                      <strong>{item.sector_name}</strong>
                      <div className={styles.muted}>
                        热度 {item.heat_score ?? 0} | Δ1 {item.delta_1 ?? "-"} | 防守线 {item.defense_line_type || "NONE"}
                      </div>
                      <div className={styles.selectionCompactReason}>
                        {formatTrajectory(item.trajectory)} | 来源 {item.source_type || "-"} | 主窗口 {item.window_size_used || "-"} 日
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className={styles.muted}>暂无数据</div>
              )
            ) : null}
            {activeLifecycleDetail === "watch-pool" ? (
              watchPool.length ? (
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
                    </div>
                  ))}
                </div>
              ) : (
                <div className={styles.muted}>暂无 MA10 观察池数据</div>
              )
            ) : null}
          </section>
        </section>

        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <div>
              <h2>当日热度面板</h2>
              <p className={styles.helperText}>基于行业/概念原始板块数据归一化后的当日热度排名。</p>
            </div>
          </div>
          <div className={styles.list}>
            {(overview?.daily_heat_panel?.items ?? []).slice(0, 10).map((item, index) => (
              <div className={styles.listItem} key={`daily-heat-${index}`}>
                <strong>{String(item.sector_name ?? "-")}</strong>
                <div className={styles.muted}>
                  热度 {Number(item.heat_score ?? 0).toFixed(2)} | 涨跌幅 {Number(item.change_pct ?? 0).toFixed(2)}% | 来源 {String(item.source_type ?? "-")}
                </div>
              </div>
            ))}
            {!(overview?.daily_heat_panel?.items ?? []).length ? <div className={styles.muted}>暂无当日热度面板</div> : null}
          </div>
        </section>

        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <div>
              <h2>最终执行清单</h2>
              <p className={styles.helperText}>点星标可直接加入或移出备选关注；加入后会自动同步到盯盘列表。</p>
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
                    <span>综合分 {item.score?.toFixed(2) || "0.00"}</span>
                  </div>
                  <div className={styles.selectionCompactReason}>
                    预期差 {item.anticipation_score?.toFixed(1) || "-"} / 缩量 {item.shrinkage_score?.toFixed(1) || "-"} / 尾盘 {item.tail_confirmation_score?.toFixed(1) || "-"}
                  </div>
                  <div className={styles.selectionCompactReason}>{item.reason || "暂无说明"}</div>
                </div>
              );
            })}
            {!finalSelected.length ? <div className={styles.muted}>暂无最终执行清单</div> : null}
          </div>
        </section>

        <section className={styles.card}>
          <div className={styles.selectionResultGrid}>
            <section className={styles.selectionResultCard}>
              <strong>启动期观察候选</strong>
              {(latestRun?.result?.observed_startup_candidates ?? []).length ? (
                <div className={styles.selectionCompactList}>
                  {(latestRun?.result?.observed_startup_candidates ?? []).map((item) => (
                    <div className={styles.selectionCompactItem} key={`startup-${item.symbol}`}>
                      <strong>{item.name || item.symbol}</strong>
                      <div className={styles.muted}>
                        {item.symbol} | {item.primary_sector || "-"} | {item.defense_line_type || "MA10"}
                      </div>
                      <div className={styles.selectionCompactReason}>{formatTrajectory(item.trajectory)}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className={styles.muted}>暂无启动期候选</div>
              )}
            </section>

            <section className={styles.selectionResultCard}>
              <strong>爆发期候选</strong>
              {(latestRun?.result?.ranked_action_candidates ?? []).length ? (
                <div className={styles.selectionCompactList}>
                  {(latestRun?.result?.ranked_action_candidates ?? []).map((item) => (
                    <div className={styles.selectionCompactItem} key={`action-${item.symbol}`}>
                      <strong>{item.name || item.symbol}</strong>
                      <div className={styles.muted}>
                        {item.symbol} | {item.primary_sector || "-"} | 分数 {item.score?.toFixed(2) || "0.00"}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className={styles.muted}>暂无爆发期候选</div>
              )}
            </section>

            <section className={styles.selectionResultCard}>
              <strong>生命周期否决</strong>
              {(latestRun?.result?.excluded_by_lifecycle_veto ?? []).length ? (
                <div className={styles.selectionCompactList}>
                  {(latestRun?.result?.excluded_by_lifecycle_veto ?? []).map((item) => (
                    <div className={styles.selectionCompactItem} key={`veto-${item.symbol}`}>
                      <strong>{item.name || item.symbol}</strong>
                      <div className={styles.muted}>
                        {item.symbol} | {item.primary_sector || "-"} | {stageLabel(item.lifecycle_stage)}
                      </div>
                      <div className={styles.selectionCompactReason}>{item.reason || "-"}</div>
                    </div>
                  ))}
                </div>
              ) : (
                <div className={styles.muted}>暂无生命周期否决项</div>
              )}
            </section>
          </div>
        </section>

        <section className={styles.card}>
          <h2 className={styles.mobileDuplicateHeading}>调度设置</h2>
          <div className={styles.formGrid}>
            <div className={styles.field}>
              <label htmlFor="smartSelectionScheduleTime">定时时间</label>
              <input id="smartSelectionScheduleTime" onChange={(event) => setScheduleTime(event.target.value)} type="time" value={scheduleTime} />
            </div>
            <div className={styles.field}>
              <label htmlFor="smartSelectionMaxWorkers">并发数</label>
              <input
                id="smartSelectionMaxWorkers"
                min={1}
                onChange={(event) => setMaxWorkers(event.target.value)}
                step={1}
                type="number"
                value={maxWorkers}
              />
            </div>
            <div className={styles.metric}>
              <span className={styles.muted}>当前状态</span>
              <strong>{scheduler?.running ? "运行中" : "未运行"}</strong>
              <div className={styles.muted}>评分并发：{displayMaxWorkers}</div>
              <div className={styles.muted}>下次运行：{formatDateTime(scheduler?.next_run_time, "-")}</div>
              <div className={styles.muted}>上次运行：{formatDateTime(scheduler?.last_run_time, "-")}</div>
            </div>
          </div>
          <div className={styles.summaryMetricGrid} style={{ marginTop: 16 }}>
            <label className={styles.switchField}>
              <span className={styles.switchLabel}>启用定时智能选股</span>
              <span className={styles.switchControl}>
                <input
                  checked={Boolean(scheduler?.enabled)}
                  disabled={isSavingScheduler}
                  onChange={(event) => void handleSaveScheduler(event.target.checked)}
                  type="checkbox"
                />
                <span aria-hidden="true" className={styles.switchTrack}>
                  <span className={styles.switchThumb} />
                </span>
              </span>
            </label>
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
