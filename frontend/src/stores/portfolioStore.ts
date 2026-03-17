import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

import {
  ALL_ACCOUNT_NAME,
  DEFAULT_ACCOUNT_NAME,
  normalizeAccountName,
  supportedAccountOptions,
} from "../lib/accounts";

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
  selectedAccount: string;
  knownAccounts: string[];
  draftPosition: DraftPosition | null;
  holdingsAnalysisTaskId: string | null;
  schedulerTaskId: string | null;
  pageCacheByAccount: Record<string, PortfolioPageCache>;
  setSelectedAccount: (account: string) => void;
  setKnownAccounts: (accounts: string[]) => void;
  setDraftPosition: (draft: DraftPosition | null) => void;
  setHoldingsAnalysisTaskId: (taskId: string | null) => void;
  setSchedulerTaskId: (taskId: string | null) => void;
  setPageCache: (account: string, cache: PortfolioPageCache) => void;
  clearPageCache: (account?: string) => void;
}

export const usePortfolioStore = create<PortfolioState>()(
  persist(
    (set) => ({
      selectedAccount: DEFAULT_ACCOUNT_NAME,
      knownAccounts: supportedAccountOptions(true),
      draftPosition: null,
      holdingsAnalysisTaskId: null,
      schedulerTaskId: null,
      pageCacheByAccount: {},
      setSelectedAccount: (selectedAccount) =>
        set({
          selectedAccount:
            normalizeAccountName(selectedAccount, { allowAggregate: true }) || DEFAULT_ACCOUNT_NAME,
        }),
      setKnownAccounts: (accounts) =>
        set((state) => ({
          knownAccounts: Array.from(
            new Set(
              [...state.knownAccounts, ...accounts]
                .map((item) => normalizeAccountName(item, { allowAggregate: true }) || DEFAULT_ACCOUNT_NAME),
            ),
          ),
        })),
      setDraftPosition: (draftPosition) => set({ draftPosition }),
      setHoldingsAnalysisTaskId: (holdingsAnalysisTaskId) => set({ holdingsAnalysisTaskId }),
      setSchedulerTaskId: (schedulerTaskId) => set({ schedulerTaskId }),
      setPageCache: (account, cache) =>
        set((state) => ({
          pageCacheByAccount: {
            ...state.pageCacheByAccount,
            [account]: cache,
          },
        })),
      clearPageCache: (account) =>
        set((state) => {
          if (!account) {
            return { pageCacheByAccount: {} };
          }
          const nextCache = { ...state.pageCacheByAccount };
          delete nextCache[account];
          return { pageCacheByAccount: nextCache };
        }),
    }),
    {
      name: "portfolio-ui-state",
      storage: createJSONStorage(() => localStorage),
      merge: (persistedState, currentState) => {
        const typedState = (persistedState as Partial<PortfolioState> | undefined) ?? {};
        const knownAccounts = Array.from(
          new Set(
            [
              ...supportedAccountOptions(true),
              ...((typedState.knownAccounts ?? currentState.knownAccounts) || []),
            ]
              .map((item) => normalizeAccountName(item, { allowAggregate: true }) || DEFAULT_ACCOUNT_NAME),
          ),
        );
        return {
          ...currentState,
          ...typedState,
          selectedAccount:
            normalizeAccountName(typedState.selectedAccount, { allowAggregate: true })
            || currentState.selectedAccount,
          knownAccounts: knownAccounts.length ? knownAccounts : currentState.knownAccounts,
        };
      },
      partialize: (state) => ({
        selectedAccount: state.selectedAccount,
        knownAccounts: state.knownAccounts,
        holdingsAnalysisTaskId: state.holdingsAnalysisTaskId,
        schedulerTaskId: state.schedulerTaskId,
      }),
    },
  ),
);
