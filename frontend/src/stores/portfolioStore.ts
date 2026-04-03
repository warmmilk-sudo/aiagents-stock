import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

interface DraftPosition {
  code: string;
  name?: string;
  cost_price?: number;
  quantity?: number;
}

export interface PortfolioPageCache {
  stocks: unknown[];
  risk: unknown | null;
  scheduler: unknown | null;
  schedulerTimes: string;
  updatedAt: number;
}

interface PortfolioState {
  draftPosition: DraftPosition | null;
  holdingsAnalysisTaskId: string | null;
  schedulerTaskId: string | null;
  pageCache: PortfolioPageCache | null;
  setDraftPosition: (draft: DraftPosition | null) => void;
  setHoldingsAnalysisTaskId: (taskId: string | null) => void;
  setSchedulerTaskId: (taskId: string | null) => void;
  setPageCache: (cache: PortfolioPageCache) => void;
  clearPageCache: () => void;
}

export const usePortfolioStore = create<PortfolioState>()(
  persist(
    (set) => ({
      draftPosition: null,
      holdingsAnalysisTaskId: null,
      schedulerTaskId: null,
      pageCache: null,
      setDraftPosition: (draftPosition) => set({ draftPosition }),
      setHoldingsAnalysisTaskId: (holdingsAnalysisTaskId) => set({ holdingsAnalysisTaskId }),
      setSchedulerTaskId: (schedulerTaskId) => set({ schedulerTaskId }),
      setPageCache: (pageCache) => set({ pageCache }),
      clearPageCache: () => set({ pageCache: null }),
    }),
    {
      name: "portfolio-ui-state",
      storage: createJSONStorage(() => localStorage),
      merge: (persistedState, currentState) => {
        const typedState = (persistedState as Partial<PortfolioState> | undefined) ?? {};
        return {
          ...currentState,
          ...typedState,
          pageCache: currentState.pageCache,
        };
      },
      partialize: (state) => ({
        holdingsAnalysisTaskId: state.holdingsAnalysisTaskId,
        schedulerTaskId: state.schedulerTaskId,
      }),
    },
  ),
);
