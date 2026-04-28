import { create } from "zustand";
import { persist } from "zustand/middleware";

import { createSafeJSONStorage } from "../persistStorage";

type ResearchIntentType =
  | "watchlist"
  | "portfolio"
  | "price_alert"
  | "history_detail"
  | "analysis_baseline";

interface ResearchIntent {
  type: ResearchIntentType | string;
  symbol?: string;
  payload?: unknown;
}

export interface ResearchHubPageCache {
  overview: unknown | null;
  assets: unknown[];
  selectedAssetId: number | null;
  activePool: string;
  searchTerm: string;
  updatedAt: number;
}

interface ResearchState {
  pendingIntent: ResearchIntent | null;
  hubPageCache: ResearchHubPageCache | null;
  setIntent: (intent: ResearchIntent | null) => void;
  consumeIntent: () => ResearchIntent | null;
  setHubPageCache: (cache: ResearchHubPageCache) => void;
  clearHubPageCache: () => void;
}

const RESEARCH_STORE_VERSION = 3;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function sanitizeHubPageCache(value: unknown): ResearchHubPageCache | null {
  if (!isRecord(value) || !Array.isArray(value.assets)) {
    return null;
  }
  const selectedAssetId =
    typeof value.selectedAssetId === "number" && Number.isFinite(value.selectedAssetId) ? value.selectedAssetId : null;
  const updatedAt = Number(value.updatedAt);
  return {
    overview: sanitizeHubOverviewForCache(value.overview),
    assets: value.assets.map(compactHubAssetForCache).filter((item): item is Record<string, unknown> => Boolean(item)),
    selectedAssetId,
    activePool: typeof value.activePool === "string" ? value.activePool : "research",
    searchTerm: typeof value.searchTerm === "string" ? value.searchTerm : "",
    updatedAt: Number.isFinite(updatedAt) && updatedAt > 0 ? updatedAt : Date.now(),
  };
}

function sanitizeHubOverviewForCache(value: unknown): unknown | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    counts: isRecord(value.counts) ? value.counts : {},
    focus_capacity: Number(value.focus_capacity) || 0,
    sector_report_warning: typeof value.sector_report_warning === "string" ? value.sector_report_warning : undefined,
  };
}

function compactHubAssetForCache(value: unknown): Record<string, unknown> | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    id: value.id,
    symbol: value.symbol,
    name: value.name,
    status: value.status,
    status_label: value.status_label,
    display_tags: Array.isArray(value.display_tags) ? value.display_tags : undefined,
    primary_industry: value.primary_industry,
    core_concepts: Array.isArray(value.core_concepts) ? value.core_concepts : undefined,
    extra_tags_count: value.extra_tags_count,
    display_tag_summary: Array.isArray(value.display_tag_summary) ? value.display_tag_summary : undefined,
    manual_pin: value.manual_pin,
    pool_reason: value.pool_reason,
    latest_analysis_time: value.latest_analysis_time,
    latest_analysis_rating: value.latest_analysis_rating,
    latest_analysis_id: value.latest_analysis_id,
  };
}

function sanitizeResearchState(value: unknown) {
  const state = isRecord(value) ? value : {};
  return {
    hubPageCache: sanitizeHubPageCache(state.hubPageCache),
  };
}

export const useResearchStore = create<ResearchState>()(
  persist(
    (set, get) => ({
      pendingIntent: null,
      hubPageCache: null,
      setIntent: (intent) => set({ pendingIntent: intent }),
      consumeIntent: () => {
        const current = get().pendingIntent;
        set({ pendingIntent: null });
        return current;
      },
      setHubPageCache: (hubPageCache) => set({ hubPageCache: sanitizeHubPageCache(hubPageCache) }),
      clearHubPageCache: () => set({ hubPageCache: null }),
    }),
    {
      name: "research-ui-state",
      version: RESEARCH_STORE_VERSION,
      storage: createSafeJSONStorage<Pick<ResearchState, "hubPageCache">>(),
      migrate: (persistedState) => sanitizeResearchState(persistedState),
      partialize: (state) => ({
        hubPageCache: state.hubPageCache,
      }),
    },
  ),
);
