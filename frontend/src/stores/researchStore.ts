import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";


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
      setHubPageCache: (hubPageCache) => set({ hubPageCache }),
      clearHubPageCache: () => set({ hubPageCache: null }),
    }),
    {
      name: "research-ui-state",
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        hubPageCache: state.hubPageCache,
      }),
    },
  ),
);
