import { create } from "zustand";
import { persist } from "zustand/middleware";

import { createSafeJSONStorage } from "../persistStorage";

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
  schedulerTaskId: string | null;
  holdingsAnalysisTaskId: string | null;
  pageCache: PortfolioPageCache | null;
  setDraftPosition: (draft: DraftPosition | null) => void;
  setSchedulerTaskId: (taskId: string | null) => void;
  setHoldingsAnalysisTaskId: (taskId: string | null) => void;
  setPageCache: (cache: PortfolioPageCache) => void;
  clearPageCache: () => void;
}

export const usePortfolioStore = create<PortfolioState>()(
  persist(
    (set) => ({
      draftPosition: null,
      schedulerTaskId: null,
      holdingsAnalysisTaskId: null,
      pageCache: null,
      setDraftPosition: (draftPosition) => set({ draftPosition }),
      setSchedulerTaskId: (schedulerTaskId) => set({ schedulerTaskId }),
      setHoldingsAnalysisTaskId: (holdingsAnalysisTaskId) => set({ holdingsAnalysisTaskId }),
      setPageCache: (pageCache) => set({ pageCache }),
      clearPageCache: () => set({ pageCache: null }),
    }),
    {
      name: "portfolio-ui-state",
      storage: createSafeJSONStorage<Partial<PortfolioState>>(),
      merge: (persistedState, currentState) => {
        const typedState = (persistedState as Partial<PortfolioState> | undefined) ?? {};
        return {
          ...currentState,
          ...typedState,
          pageCache: typedState.pageCache ?? currentState.pageCache,
        };
      },
      partialize: (state) => ({
        schedulerTaskId: state.schedulerTaskId,
        holdingsAnalysisTaskId: state.holdingsAnalysisTaskId,
        pageCache: state.pageCache,
      }),
    },
  ),
);
