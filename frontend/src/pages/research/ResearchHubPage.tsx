import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { ModuleCard } from "../../components/common/ModuleCard";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { usePollingLoader } from "../../hooks/usePollingLoader";
import { getViewedAnalysisId, isAnalysisUnread, markAnalysisViewed } from "../../lib/analysisReadState";
import { ApiRequestError, apiFetch, apiFetchCached, buildQuery } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { useResearchStore, type ResearchHubPageCache } from "../../stores/researchStore";
import { DeepAnalysisPage } from "./DeepAnalysisPage";
import { PortfolioPage } from "../investment/PortfolioPage";
import { StockArchiveContent } from "./HistoryPage";
import styles from "../ConsolePage.module.scss";

type PoolKey = "holding" | "focus" | "research";
type HubSectionKey = "holdings" | "analysis" | "control";

interface HubAsset {
  id: number;
  symbol: string;
  name: string;
  status: string;
  status_label: string;
  display_tags?: string[];
  primary_industry?: string;
  core_concepts?: string[];
  extra_tags_count?: number;
  display_tag_summary?: string[];
  note?: string;
  manual_pin?: boolean;
  pool_reason?: string;
  latest_analysis_summary?: string;
  latest_analysis_time?: string;
  latest_analysis_rating?: string;
  latest_analysis_id?: number;
  last_funnel_score?: number;
}

interface HubOverview {
  counts: Record<string, number>;
  focus_capacity: number;
}

interface BackgroundTask {
  id: string;
  status: string;
  message: string;
  current?: number;
  total?: number;
  error?: string;
  result?: {
    final_selected?: Array<{ symbol: string; name?: string; score?: number; reason?: string; selection_type?: string; primary_sector?: string }>;
    ranked_top15?: Array<{ symbol: string; name?: string; score?: number; reason?: string; primary_sector?: string }>;
    extracted_sectors?: Array<{ sector?: string; heat_score?: number; reason?: string }>;
    excluded_by_dedup?: Array<{ symbol: string; name?: string; reason?: string }>;
    kept_manual_pins?: Array<{ symbol: string; risk_flagged?: boolean; risk_notes?: string[] }>;
    demoted?: Array<{ symbol: string; reason?: string }>;
    warnings?: string[];
    sector_strategy_reused?: boolean;
  } | null;
}

type ConfirmAction =
  | { kind: "toggle-pin"; item: HubAsset }
  | { kind: "delete"; item: HubAsset };

function isPendingTaskStatus(status?: string | null) {
  return status === "queued" || status === "running";
}

function taskProgressTone(task: BackgroundTask | null): "running" | "success" | "danger" {
  if (!task || isPendingTaskStatus(task.status)) {
    return "running";
  }
  if (task.status === "success") {
    return "success";
  }
  return "danger";
}

function taskStatusMeta(task: BackgroundTask | null): { label: string; tone: "default" | "success" | "warning" | "danger" } {
  if (!task) {
    return { label: "暂无任务", tone: "default" };
  }
  if (task.status === "success") {
    return { label: "已完成", tone: "success" };
  }
  if (task.status === "failed" || task.status === "cancelled") {
    return { label: task.status === "cancelled" ? "已取消" : "执行失败", tone: "danger" };
  }
  return { label: "执行中", tone: "warning" };
}

function shortenText(value: string, limit: number) {
  if (!value) {
    return "";
  }
  return value.length > limit ? `${value.slice(0, limit)}…` : value;
}

function formatHoldingStatusLabel(item: HubAsset) {
  const rating = String(item.latest_analysis_rating || "").trim();
  const analysisTime = formatDateTime(item.latest_analysis_time, "暂无时间");
  if (rating || analysisTime !== "暂无时间") {
    return `${rating || "未评级"} | ${analysisTime}`;
  }
  return item.status_label;
}

const pools: Array<{ key: PoolKey; label: string }> = [
  { key: "holding", label: "持仓中" },
  { key: "focus", label: "备选关注" },
  { key: "research", label: "研究池库" },
];

const hubSectionTabs: Array<{ key: HubSectionKey; label: string }> = [
  { key: "analysis", label: "深度分析" },
  { key: "control", label: "投研档案" },
  { key: "holdings", label: "持仓列表" },
];
const HUB_CACHE_TTL_MS = 60_000;
const HUB_PAGE_CACHE_MAX_AGE_MS = 12 * 60 * 60 * 1000;

function isHubPageCacheUsable(cache: ResearchHubPageCache | null) {
  return Boolean(cache && Number.isFinite(cache.updatedAt) && Date.now() - cache.updatedAt <= HUB_PAGE_CACHE_MAX_AGE_MS);
}

function PoolPanel({
  activePool,
  items,
  counts,
  activeId,
  pinUpdatingAssetId,
  deletingAssetId,
  onPoolChange,
  onSelect,
  onToggleManualPin,
  onDeleteResearchCard,
  renderAssetTags,
}: {
  activePool: PoolKey;
  items: HubAsset[];
  counts: Record<string, number>;
  activeId: number | null;
  pinUpdatingAssetId: number | null;
  deletingAssetId: number | null;
  onPoolChange: (pool: PoolKey) => void;
  onSelect: (item: HubAsset) => void;
  onToggleManualPin: (item: HubAsset) => void;
  onDeleteResearchCard: (item: HubAsset) => void;
  renderAssetTags: (item: HubAsset) => ReactNode;
}) {
  return (
    <div className={styles.researchHubSidebar}>
      <div className={styles.researchHubPoolTabs}>
        {pools.map((pool) => (
          <button
            className={activePool === pool.key ? styles.nestedTabButtonActive : styles.nestedTabButton}
            key={pool.key}
            onClick={() => onPoolChange(pool.key)}
            type="button"
          >
            {pool.label}
            <span>{counts[pool.key] || 0}</span>
          </button>
        ))}
      </div>
      <div className={styles.researchHubAssetList}>
        {items.map((item) => {
          const unreadAnalysis = item.status === "research" && isAnalysisUnread(item.symbol, item.latest_analysis_id);
          const descriptor = item.primary_industry || item.display_tag_summary?.[0] || item.status_label;
          const holdingRating = String(item.latest_analysis_rating || "").trim() || "未评级";
          const holdingTime = formatDateTime(item.latest_analysis_time, "暂无时间").slice(0, 10);
          return (
            <div
              className={item.id === activeId ? styles.researchHubAssetButtonActive : styles.researchHubAssetButton}
              key={item.id}
            >
              <div className={styles.researchHubAssetRow}>
                <span
                  aria-hidden="true"
                  className={unreadAnalysis ? styles.researchHubUnreadDot : styles.researchHubUnreadDotHidden}
                  title={unreadAnalysis ? "有未看的最新分析" : undefined}
                />
                <button
                  className={unreadAnalysis ? `${styles.researchHubAssetMain} ${styles.researchHubAssetMainUnread}` : styles.researchHubAssetMain}
                  onClick={() => onSelect(item)}
                  type="button"
                >
                  <strong>{item.name}</strong>
                  <span>{item.symbol} | {descriptor}</span>
                  <div className={styles.researchHubTagList}>
                    {renderAssetTags(item)}
                  </div>
                </button>
                <div className={styles.researchHubAssetActions}>
                  {activePool === "holding" ? (
                    <div className={styles.researchHubAssetInfoStack}>
                      <span className={styles.researchHubAssetInfoLine}>{holdingRating}</span>
                      <span className={styles.researchHubAssetInfoLineMuted}>{holdingTime}</span>
                    </div>
                  ) : (
                    <div className={styles.researchHubAssetActionStack}>
                      <button
                        aria-label={item.status === "focus" || item.manual_pin ? `移出备选关注 ${item.symbol}` : `加入备选关注 ${item.symbol}`}
                        className={item.status === "focus" || item.manual_pin ? styles.researchHubStarButtonActive : styles.researchHubStarButton}
                        disabled={pinUpdatingAssetId === item.id}
                        onClick={(event) => {
                          event.stopPropagation();
                          onToggleManualPin(item);
                        }}
                        title={item.status === "focus" || item.manual_pin ? "移出备选关注" : "加入备选关注"}
                        type="button"
                      >
                        {item.status === "focus" || item.manual_pin ? "★" : "☆"}
                      </button>
                      {activePool === "research" ? (
                        <button
                          aria-label={`删除研究池卡片 ${item.symbol}`}
                          className={styles.researchHubDeleteButton}
                          disabled={deletingAssetId === item.id}
                          onClick={(event) => {
                            event.stopPropagation();
                            onDeleteResearchCard(item);
                          }}
                          title="删除研究池卡片"
                          type="button"
                        >
                          {deletingAssetId === item.id ? (
                            "…"
                          ) : (
                            <svg aria-hidden="true" viewBox="0 0 16 16" focusable="false">
                              <path d="M5.5 2.5h5l.5 1.5H13a.75.75 0 0 1 0 1.5h-.45l-.55 7.02A1.75 1.75 0 0 1 10.26 14H5.74A1.75 1.75 0 0 1 4 12.52L3.45 5.5H3a.75.75 0 0 1 0-1.5h2.02l.48-1.5Zm.6 1.5-.16.5h4.12l-.16-.5H6.1Zm-1.14 1.5.53 6.9a.25.25 0 0 0 .25.22h4.52a.25.25 0 0 0 .25-.22l.53-6.9H4.96Zm2.04 1.25c.41 0 .75.34.75.75v3a.75.75 0 0 1-1.5 0v-3c0-.41.34-.75.75-.75Zm2 0c.41 0 .75.34.75.75v3a.75.75 0 0 1-1.5 0v-3c0-.41.34-.75.75-.75Z" />
                            </svg>
                          )}
                        </button>
                      ) : null}
                    </div>
                  )}
                </div>
              </div>
              {item.status === "focus" ? <small>{item.manual_pin ? "已加入备选关注，且手动置顶保留" : "已加入备选关注并自动同步到盯盘"}</small> : null}
            </div>
          );
        })}
        {!items.length ? <div className={styles.muted}>{activePool === "focus" ? "备选关注栏已清空" : "暂无标的"}</div> : null}
      </div>
    </div>
  );
}

export function ResearchHubPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const cachedPage = useResearchStore((state) => state.hubPageCache);
  const setHubPageCache = useResearchStore((state) => state.setHubPageCache);
  const clearHubPageCache = useResearchStore((state) => state.clearHubPageCache);
  const initialCachedPage = useResearchStore.getState().hubPageCache;
  const archiveSymbol = searchParams.get("symbol") || "";
  const archiveRecordId = searchParams.get("recordId") || "";
  const requestedSection = searchParams.get("section");
  const section: HubSectionKey =
    requestedSection === "control"
      ? "control"
      : requestedSection === "holdings"
        ? "holdings"
        : requestedSection === "analysis"
          ? "analysis"
          : archiveSymbol || archiveRecordId
            ? "control"
            : "analysis";
  const [overview, setOverview] = useState<HubOverview | null>(
    () => (initialCachedPage?.overview as HubOverview | null) ?? null,
  );
  const [assets, setAssets] = useState<HubAsset[]>(
    () => (initialCachedPage?.assets as HubAsset[]) ?? [],
  );
  const [selectedAssetId, setSelectedAssetId] = useState<number | null>(
    () => initialCachedPage?.selectedAssetId ?? null,
  );
  const [activePool, setActivePool] = useState<PoolKey>(
    () => (initialCachedPage?.activePool as PoolKey) || "research",
  );
  const [searchTerm, setSearchTerm] = useState(() => initialCachedPage?.searchTerm ?? "");
  const [selectionTask, setSelectionTask] = useState<BackgroundTask | null>(null);
  const [pinUpdatingAssetId, setPinUpdatingAssetId] = useState<number | null>(null);
  const [deletingAssetId, setDeletingAssetId] = useState<number | null>(null);
  const [confirmAction, setConfirmAction] = useState<ConfirmAction | null>(null);
  const [expandedTagAssetIds, setExpandedTagAssetIds] = useState<Record<number, boolean>>({});
  const { message, error, showError, showMessage, clear } = usePageFeedback();
  const pageLoadRequestRef = useRef(0);

  const persistHubPageCache = (overrides?: {
    overview?: HubOverview | null;
    assets?: HubAsset[];
    selectedAssetId?: number | null;
    activePool?: PoolKey;
    searchTerm?: string;
  }) => {
    setHubPageCache({
      overview: overrides?.overview ?? overview,
      assets: overrides?.assets ?? assets,
      selectedAssetId: overrides?.selectedAssetId ?? selectedAssetId,
      activePool: overrides?.activePool ?? activePool,
      searchTerm: overrides?.searchTerm ?? searchTerm,
      updatedAt: Date.now(),
    });
  };

  const loadAssets = async (options?: { background?: boolean }) => {
    const requestId = pageLoadRequestRef.current + 1;
    pageLoadRequestRef.current = requestId;
    const [overviewData, assetData] = await Promise.all([
      options?.background
        ? apiFetch<HubOverview>("/api/watchlist-hub/overview")
        : apiFetchCached<HubOverview>("/api/watchlist-hub/overview", {}, { ttlMs: HUB_CACHE_TTL_MS }),
      options?.background
        ? apiFetch<HubAsset[]>(`/api/watchlist-hub/assets${buildQuery({ search_term: searchTerm })}`)
        : apiFetchCached<HubAsset[]>(
          `/api/watchlist-hub/assets${buildQuery({ search_term: searchTerm })}`,
          {},
          { ttlMs: HUB_CACHE_TTL_MS },
        ),
    ]);
    if (pageLoadRequestRef.current !== requestId) {
      return;
    }
    setOverview(overviewData);
    setAssets(assetData);
    assetData.forEach((item) => {
      if (item.status !== "research") {
        return;
      }
      if (item.latest_analysis_id && getViewedAnalysisId(item.symbol) <= 0) {
        markAnalysisViewed(item.symbol, item.latest_analysis_id);
      }
    });
    let nextSelectedAssetId: number | null = null;
    setSelectedAssetId((current) => {
      if (current && assetData.some((item) => item.id === current)) {
        nextSelectedAssetId = current;
        return current;
      }
      nextSelectedAssetId = assetData[0]?.id ?? null;
      return nextSelectedAssetId;
    });
    persistHubPageCache({
      overview: overviewData,
      assets: assetData,
      selectedAssetId: nextSelectedAssetId,
      searchTerm,
    });
  };

  const loadSelectionTask = async () => {
    try {
      const task = await apiFetch<BackgroundTask | null>("/api/watchlist-hub/selection/tasks/latest");
      setSelectionTask(task);
    } catch {
      setSelectionTask(null);
    }
  };

  usePollingLoader({ load: loadSelectionTask, intervalMs: 2000 });

  useEffect(() => {
    if (requestedSection === "selection" || requestedSection === "funnel") {
      navigate("/investment/smart-selection", { replace: true });
    }
  }, [navigate, requestedSection]);

  useEffect(() => {
    if (isHubPageCacheUsable(cachedPage) && cachedPage?.searchTerm === searchTerm) {
      setOverview((cachedPage.overview as HubOverview | null) ?? null);
      setAssets((cachedPage.assets as HubAsset[]) ?? []);
      setSelectedAssetId(cachedPage.selectedAssetId ?? null);
      setActivePool((cachedPage.activePool as PoolKey) || "research");
    } else {
      setAssets([]);
      setSelectedAssetId(null);
    }
    void loadAssets({ background: Boolean(isHubPageCacheUsable(cachedPage) && cachedPage?.searchTerm === searchTerm) }).catch((requestError) => {
      showError(requestError instanceof ApiRequestError ? requestError.message : "投研中心数据加载失败");
    });
  }, [searchTerm]);

  useEffect(() => {
    if (!selectionTask || !["success", "failed", "cancelled"].includes(selectionTask.status)) {
      return;
    }
    clearHubPageCache();
    void loadAssets({ background: true }).catch(() => undefined);
  }, [selectionTask?.id, selectionTask?.status]);

  const groupedAssets = useMemo(
    () => ({
      holding: assets.filter((item) => item.status === "holding"),
      focus: assets.filter((item) => item.status === "focus"),
      research: assets.filter((item) => item.status === "research"),
    }),
    [assets],
  );

  const activePoolItems = groupedAssets[activePool];
  const displayCounts = useMemo(
    () => ({
      holding: groupedAssets.holding.length,
      focus: groupedAssets.focus.length,
      research: groupedAssets.research.length,
    }),
    [groupedAssets],
  );
  const manualPinCount = useMemo(
    () => assets.filter((item) => Boolean(item.manual_pin)).length,
    [assets],
  );
  useEffect(() => {
    setSelectedAssetId((current) => {
      if (current && activePoolItems.some((item) => item.id === current)) {
        return current;
      }
      return activePoolItems[0]?.id ?? null;
    });
  }, [activePool, activePoolItems]);

  useEffect(() => {
    persistHubPageCache();
  }, [activePool, selectedAssetId]);

  const handleRunSelection = async () => {
    clear();
    try {
      const result = await apiFetch<{ task_id: string }>("/api/watchlist-hub/selection/run", { method: "POST" });
      showMessage(`已提交智能选股任务 ${result.task_id}`);
      await loadSelectionTask();
    } catch (requestError) {
      showError(requestError instanceof ApiRequestError ? requestError.message : "智能选股启动失败");
    }
  };

  const updateLocalAssets = (updater: (current: HubAsset[]) => HubAsset[]) => {
    setAssets((current) => {
      const nextAssets = updater(current);
      persistHubPageCache({ assets: nextAssets });
      return nextAssets;
    });
  };

  const handleToggleManualPin = (item: HubAsset) => {
    setConfirmAction({ kind: "toggle-pin", item });
  };

  const executeToggleManualPin = async (item: HubAsset) => {
    clear();
    setPinUpdatingAssetId(item.id);
    const inFocus = item.status === "focus" || item.manual_pin;
    const optimisticItem: Partial<HubAsset> = inFocus
      ? {
          status: "research",
          status_label: "研究池",
          manual_pin: false,
          pool_reason: "手动移出备选关注，回到研究池",
        }
      : {
          status: "focus",
          status_label: "备选关注",
          manual_pin: true,
          pool_reason: "手动加入备选关注",
        };
    updateLocalAssets((current) =>
      current.map((asset) => (asset.id === item.id ? { ...asset, ...optimisticItem } : asset)),
    );
    try {
      const updatedItem = await apiFetch<HubAsset>(`/api/watchlist-hub/assets/${item.id}`, {
        method: "PATCH",
        body: JSON.stringify(
          inFocus
            ? {
                target_status: "research",
                manual_pin: false,
                pool_reason: "手动移出备选关注，回到研究池",
              }
            : {
                target_status: "focus",
                manual_pin: true,
                pool_reason: "手动加入备选关注",
              },
        ),
      });
      updateLocalAssets((current) =>
        current.map((asset) => (asset.id === item.id ? { ...asset, ...updatedItem } : asset)),
      );
      showMessage(inFocus ? `${item.symbol} 已移出备选关注并回到研究池` : `${item.symbol} 已加入备选关注并自动加入盯盘`);
      await loadAssets({ background: true });
    } catch (requestError) {
      await loadAssets({ background: true }).catch(() => undefined);
      showError(requestError instanceof ApiRequestError ? requestError.message : "关注状态更新失败");
    } finally {
      setPinUpdatingAssetId(null);
    }
  };

  const handleDeleteResearchCard = (item: HubAsset) => {
    setConfirmAction({ kind: "delete", item });
  };

  const executeDeleteResearchCard = async (item: HubAsset) => {
    clear();
    setDeletingAssetId(item.id);
    updateLocalAssets((current) => current.filter((asset) => asset.id !== item.id));
    try {
      await apiFetch(`/api/watchlist-hub/assets/${item.id}`, { method: "DELETE" });
      showMessage(`${item.symbol} 研究池卡片已删除`);
      await loadAssets({ background: true });
    } catch (requestError) {
      await loadAssets({ background: true }).catch(() => undefined);
      showError(requestError instanceof ApiRequestError ? requestError.message : "删除研究池卡片失败");
    } finally {
      setDeletingAssetId(null);
    }
  };

  const confirmDialogMeta = useMemo(() => {
    if (!confirmAction) {
      return null;
    }
    if (confirmAction.kind === "toggle-pin") {
      const inFocus = confirmAction.item.status === "focus" || confirmAction.item.manual_pin;
      return {
        title: inFocus ? "确认移出备选关注" : "确认加入备选关注",
        description: inFocus
          ? `确定将 ${confirmAction.item.name}（${confirmAction.item.symbol}）移回研究池吗？`
          : `确定将 ${confirmAction.item.name}（${confirmAction.item.symbol}）加入备选关注并同步到盯盘吗？`,
        confirmLabel: inFocus ? "确认移出" : "确认加入",
        confirmTone: "primary" as const,
      };
    }
    return {
      title: "确认删除研究池卡片",
      description: `确定要删除 ${confirmAction.item.name}（${confirmAction.item.symbol}）这张研究池卡片吗？此操作不可恢复。`,
      confirmLabel: "确认删除",
      confirmTone: "danger" as const,
    };
  }, [confirmAction]);

  const handleConfirmAction = async () => {
    if (!confirmAction) {
      return;
    }
    const currentAction = confirmAction;
    setConfirmAction(null);
    if (currentAction.kind === "toggle-pin") {
      await executeToggleManualPin(currentAction.item);
      return;
    }
    await executeDeleteResearchCard(currentAction.item);
  };

  const handleToggleTagExpansion = (assetId: number) => {
    setExpandedTagAssetIds((current) => ({
      ...current,
      [assetId]: !current[assetId],
    }));
  };

  const handleOpenStockProfile = (item: HubAsset) => {
    markAnalysisViewed(item.symbol, item.latest_analysis_id);
    setSelectedAssetId(item.id);
    setSearchParams({ section: "control", symbol: item.symbol });
  };

  const handleSectionChange = (nextSection: string) => {
    if (nextSection === "control" || nextSection === "holdings") {
      setSearchParams({ section: nextSection });
      return;
    }
    setSearchParams({ section: "analysis" });
  };

  const renderAssetTags = (item: HubAsset) => {
    const expanded = Boolean(expandedTagAssetIds[item.id]);
    const industryTag = (item.primary_industry || item.display_tag_summary?.[0] || "").trim();
    const conceptTags = (item.core_concepts || [])
      .map((tag) => String(tag || "").trim())
      .filter(Boolean);
    const fallbackConceptTags = (item.display_tags || [])
      .map((tag) => String(tag || "").trim())
      .filter(Boolean)
      .filter((tag) => tag !== industryTag);
    const visibleTags = conceptTags.length ? conceptTags : fallbackConceptTags;
    const fullTags = (item.display_tags || []).map((tag) => String(tag || "").trim()).filter(Boolean);
    const hiddenTags = conceptTags.length ? [] : fullTags.filter((tag) => tag !== industryTag && !visibleTags.includes(tag));

    if (!visibleTags.length && !hiddenTags.length) {
      return <span className={styles.researchHubTagMuted}>暂无概念/行业标签</span>;
    }

    return (
      <>
        {visibleTags.map((tag, index) => (
          <span
            className={index === 0 ? styles.researchHubTagPrimary : styles.researchHubTag}
            key={`${item.symbol}-${tag}`}
          >
            {tag}
          </span>
        ))}
        {item.extra_tags_count ? (
          <span className={styles.researchHubTagMuted} title="还有更多概念标签未展开">
            +{item.extra_tags_count}
          </span>
        ) : null}
        {hiddenTags.length ? (
          expanded ? (
            <>
              {hiddenTags.map((tag) => (
                <span className={styles.researchHubTag} key={`${item.symbol}-extra-${tag}`}>
                  {tag}
                </span>
              ))}
              <button
                className={styles.researchHubTagToggle}
                onClick={(event) => {
                  event.stopPropagation();
                  handleToggleTagExpansion(item.id);
                }}
                type="button"
              >
                收起
              </button>
            </>
          ) : (
            <button
              className={styles.researchHubTagToggle}
              onClick={(event) => {
                event.stopPropagation();
                handleToggleTagExpansion(item.id);
              }}
              type="button"
              title={`展开 ${hiddenTags.length} 个隐藏标签`}
            >
              +{hiddenTags.length}
            </button>
          )
        ) : null}
      </>
    );
  };

  const renderControlContent = () => {
    if (section === "control" && (archiveSymbol || archiveRecordId)) {
      return (
        <StockArchiveContent
          embedded
          initialSymbol={archiveSymbol}
          onBackToHub={() => {
            setSearchParams({ section: "control" });
            clear();
          }}
        />
      );
    }

    return (
      <div className={styles.researchHubControlPage}>
        <PageFeedback error={error} message={message} />

        <ModuleCard
          className={styles.researchHubControlFilters}
          hideHeader
          title="投研控制台"
        >
          <div className={styles.moduleSection}>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="hubSearch">列表搜索</label>
                <input
                  id="hubSearch"
                  onChange={(event) => setSearchTerm(event.target.value)}
                  placeholder="搜索代码 / 名称 / 理由"
                  value={searchTerm}
                />
              </div>
            </div>
          </div>
        </ModuleCard>

        <ModuleCard
          className={styles.researchHubControlListCard}
          title="池列表"
          summary={`持仓 ${displayCounts.holding} | 备选 ${displayCounts.focus} | 研究池库 ${displayCounts.research} | 手动关注 ${manualPinCount}`}
          hideTitleOnMobile
        >
          <PoolPanel
            activeId={selectedAssetId}
          activePool={activePool}
          counts={displayCounts}
          items={activePoolItems}
          pinUpdatingAssetId={pinUpdatingAssetId}
          deletingAssetId={deletingAssetId}
          onPoolChange={setActivePool}
          onSelect={handleOpenStockProfile}
          onDeleteResearchCard={handleDeleteResearchCard}
          onToggleManualPin={handleToggleManualPin}
          renderAssetTags={renderAssetTags}
        />
        </ModuleCard>

      </div>
    );
  };

  const renderHoldingsContent = () => (
    <PortfolioPage embedded />
  );

  const renderSelectionItems = (
    items: Array<{ symbol?: string; name?: string; score?: number; reason?: string; primary_sector?: string; selection_type?: string }> | undefined,
    emptyText: string,
    mode: "standard" | "compact" = "standard",
  ) => {
    if (!items?.length) {
      return <div className={styles.muted}>{emptyText}</div>;
    }
    return (
      <div className={mode === "compact" ? styles.selectionCompactList : styles.stack}>
        {items.map((item) => (
          <div className={mode === "compact" ? styles.selectionCompactItem : styles.listItem} key={`${item.symbol}-${item.selection_type || ""}`}>
            <strong>{mode === "compact" ? (item.name || item.symbol || "未知") : `${item.name || item.symbol || "未知"}${item.primary_sector ? ` | ${item.primary_sector}` : ""}`}</strong>
            <div className={styles.muted}>
              {mode === "compact"
                ? shortenText(item.reason || item.symbol || "暂无说明", 72)
                : `${item.symbol || "未知"}${item.score !== undefined ? ` | 得分 ${item.score}` : item.selection_type === "manual" ? " | 手动加星保留" : " | 已入选"}`}
            </div>
            {mode === "standard" && item.reason ? <div className={styles.selectionCompactReason}>{shortenText(item.reason, 96)}</div> : null}
          </div>
        ))}
      </div>
    );
  };

  const renderSelectionBlock = (
    title: string,
    items: Array<{ symbol?: string; name?: string; score?: number; reason?: string; primary_sector?: string; selection_type?: string }> | undefined,
    emptyText: string,
    tone: "normal" | "wide" = "normal",
    mode: "standard" | "compact" = "standard",
  ) => (
    <section className={`${styles.selectionResultCard} ${tone === "wide" ? styles.selectionResultWide : ""}`}>
      <strong>{title}</strong>
      {renderSelectionItems(items, emptyText, mode)}
    </section>
  );

  const renderSelectionContent = () => {
    const selectionTaskStatus = taskStatusMeta(selectionTask);
    const selectionTaskCurrent = selectionTask?.current ?? (selectionTask?.status === "success" ? selectionTask?.total ?? 100 : 0);
    const selectionTaskPending = isPendingTaskStatus(selectionTask?.status);

    return (
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        <div className={styles.moduleSection}>
          <div className={styles.actions}>
            <button
              className={styles.primaryButton}
              disabled={selectionTaskPending}
              onClick={handleRunSelection}
              type="button"
            >
              {selectionTaskPending ? "智能选股执行中..." : "一键 AI 智能选股"}
            </button>
          </div>

          {selectionTask ? (
            <>
              <div className={styles.noticeMeta}>
                <div>
                  <strong>选股任务进度</strong>
                  <div className={styles.muted}>{selectionTask.result?.sector_strategy_reused ? "已复用 12 小时内智策报告" : "按最新智策报告执行选股"}</div>
                </div>
                <StatusBadge label={selectionTaskStatus.label} tone={selectionTaskStatus.tone} />
              </div>
              <TaskProgressBar
                current={selectionTaskCurrent}
                message={selectionTask.message || "等待智能选股任务状态..."}
                tone={taskProgressTone(selectionTask)}
                total={selectionTask?.total ?? 100}
              />
              {selectionTask.error ? <div className={styles.dangerText}>{selectionTask.error}</div> : null}

              <div className={styles.selectionResultGrid}>
                {renderSelectionBlock("Top 15 准入围", selectionTask.result?.ranked_top15, "暂无准入围名单")}
                {renderSelectionBlock("最终 Top 10", selectionTask.result?.final_selected, "暂无最终入选名单")}
                {renderSelectionBlock(
                  "去重剔除",
                  selectionTask.result?.excluded_by_dedup?.map((item) => ({
                    symbol: item.symbol,
                    name: item.name || item.symbol,
                    reason: item.reason || "同质化去重",
                  })),
                  "暂无去重剔除",
                  "normal",
                  "compact",
                )}
                {selectionTask.result?.warnings?.length ? (
                  <section className={`${styles.selectionResultCard} ${styles.selectionResultWide}`}>
                    <strong>执行告警</strong>
                    <div className={styles.selectionCompactList}>
                      {selectionTask.result.warnings.map((warning) => (
                        <div className={styles.selectionCompactItem} key={warning}>
                          <div className={styles.muted}>{shortenText(warning, 120)}</div>
                        </div>
                      ))}
                    </div>
                  </section>
                ) : null}
              </div>
            </>
          ) : (
            <div className={styles.muted}>暂无选股结果</div>
          )}
        </div>
      </div>
    );
  };

  return (
    <>
      <PageFrame
        activeSectionKey={section}
        onSectionChange={handleSectionChange}
        sectionTabs={hubSectionTabs}
        title="投研中心"
      >
        {section === "holdings"
          ? renderHoldingsContent()
          : section === "analysis"
            ? <DeepAnalysisPage startOnly />
            : renderControlContent()}
      </PageFrame>
      {confirmDialogMeta ? (
        <div
          aria-modal="true"
          className={styles.dialogBackdrop}
          onClick={() => setConfirmAction(null)}
          role="dialog"
        >
          <div
            className={styles.dialogCard}
            onClick={(event) => event.stopPropagation()}
          >
            <div className={styles.dialogHeader}>
              <strong>{confirmDialogMeta.title}</strong>
              <button
                aria-label="关闭确认弹窗"
                className={styles.dialogCloseButton}
                onClick={() => setConfirmAction(null)}
                type="button"
              >
                ×
              </button>
            </div>
            <div className={styles.dialogBody}>{confirmDialogMeta.description}</div>
            <div className={styles.dialogActions}>
              <button
                className={styles.secondaryButton}
                onClick={() => setConfirmAction(null)}
                type="button"
              >
                取消
              </button>
              <button
                className={confirmDialogMeta.confirmTone === "danger" ? styles.dangerButton : styles.primaryButton}
                onClick={() => {
                  void handleConfirmAction();
                }}
                type="button"
              >
                {confirmDialogMeta.confirmLabel}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
