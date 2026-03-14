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
  trades: unknown[];
  tradePage: number;
  tradeTotal: number;
  tradePageSize: number;
  risk: unknown | null;
  scheduler: unknown | null;
  schedulerTimes: string;
  updatedAt: number;
}

interface PortfolioState {
  selectedAccount: string;
  knownAccounts: string[];
  draftPosition: DraftPosition | null;
  pageCacheByAccount: Record<string, PortfolioPageCache>;
  setSelectedAccount: (account: string) => void;
  setKnownAccounts: (accounts: string[]) => void;
  setDraftPosition: (draft: DraftPosition | null) => void;
  setPageCache: (account: string, cache: PortfolioPageCache) => void;
  clearPageCache: (account?: string) => void;
}

export const usePortfolioStore = create<PortfolioState>()(
  persist(
    (set) => ({
      selectedAccount: "ly",
      knownAccounts: ["ly", "zfy", "全部账户"],
      draftPosition: null,
      pageCacheByAccount: {},
      setSelectedAccount: (selectedAccount) => set({ selectedAccount }),
      setKnownAccounts: (accounts) =>
        set((state) => ({
          knownAccounts: Array.from(
            new Set(
              [...state.knownAccounts, ...accounts]
                .map((item) => item.trim())
                .filter(Boolean),
            ),
          ),
        })),
      setDraftPosition: (draftPosition) => set({ draftPosition }),
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
            ["ly", "zfy", "全部账户", ...((typedState.knownAccounts ?? currentState.knownAccounts) || [])]
              .map((item) => (item === "默认账户" ? "ly" : item).trim())
              .filter(Boolean),
          ),
        );
        return {
          ...currentState,
          ...typedState,
          selectedAccount:
            typedState.selectedAccount === "默认账户"
              ? "ly"
              : (typedState.selectedAccount ?? currentState.selectedAccount),
          knownAccounts: knownAccounts.length ? knownAccounts : currentState.knownAccounts,
        };
      },
      partialize: (state) => ({
        selectedAccount: state.selectedAccount,
        knownAccounts: state.knownAccounts,
      }),
    },
  ),
);
