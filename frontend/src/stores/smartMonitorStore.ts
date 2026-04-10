import { create } from "zustand";
import { persist } from "zustand/middleware";

export interface SmartMonitorPageCache {
  systemStatus: unknown | null;
  tasks: unknown[];
  decisionSummary?: unknown | null;
  decisions: unknown[];
  notifications: unknown[];
  monitorConfig?: unknown | null;
  runtimeConfig: unknown | null;
  updatedAt: number;
}

interface SmartMonitorState {
  enabledOnly: boolean;
  pageCacheByMode: Record<string, SmartMonitorPageCache>;
  setEnabledOnly: (enabledOnly: boolean) => void;
  setPageCache: (mode: string, cache: SmartMonitorPageCache) => void;
  clearPageCache: (mode?: string) => void;
}

export const useSmartMonitorStore = create<SmartMonitorState>()(
  persist(
    (set) => ({
      enabledOnly: false,
      pageCacheByMode: {},
      setEnabledOnly: (enabledOnly) => set({ enabledOnly }),
      setPageCache: (mode, cache) =>
        set((state) => ({
          pageCacheByMode: {
            ...state.pageCacheByMode,
            [mode]: cache,
          },
        })),
      clearPageCache: (mode) =>
        set((state) => {
          if (!mode) {
            return { pageCacheByMode: {} };
          }
          const nextCache = { ...state.pageCacheByMode };
          delete nextCache[mode];
          return { pageCacheByMode: nextCache };
        }),
    }),
    {
      name: "smart-monitor-ui-state",
      partialize: (state) => ({
        enabledOnly: state.enabledOnly,
      }),
    },
  ),
);
