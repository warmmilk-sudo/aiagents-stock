import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

export interface ArchiveSummaryCacheEntry {
  items: unknown[];
  updatedAt: number;
}

export interface ArchiveProfileCacheEntry {
  records: unknown[];
  memoryArchive: unknown | null;
  updatedAt: number;
}

export interface ArchiveDetailCacheEntry {
  detail: unknown | null;
  updatedAt: number;
}

interface ArchiveState {
  summaryCacheByQuery: Record<string, ArchiveSummaryCacheEntry>;
  profileCacheBySymbol: Record<string, ArchiveProfileCacheEntry>;
  detailCacheById: Record<string, ArchiveDetailCacheEntry>;
  setSummaryCache: (queryKey: string, cache: ArchiveSummaryCacheEntry) => void;
  setProfileCache: (symbol: string, cache: ArchiveProfileCacheEntry) => void;
  setDetailCache: (recordId: number, cache: ArchiveDetailCacheEntry) => void;
  clearSummaryCache: (queryKey?: string) => void;
  clearProfileCache: (symbol?: string) => void;
  clearDetailCache: (recordId?: number) => void;
}

const MAX_SUMMARY_CACHE_ENTRIES = 4;
const MAX_PROFILE_CACHE_ENTRIES = 12;
const MAX_DETAIL_CACHE_ENTRIES = 24;

function pruneCacheEntries<T extends { updatedAt: number }>(entries: Record<string, T>, limit: number) {
  const orderedEntries = Object.entries(entries).sort(([, left], [, right]) => right.updatedAt - left.updatedAt);
  return Object.fromEntries(orderedEntries.slice(0, limit));
}

export const useArchiveStore = create<ArchiveState>()(
  persist(
    (set) => ({
      summaryCacheByQuery: {},
      profileCacheBySymbol: {},
      detailCacheById: {},
      setSummaryCache: (queryKey, cache) =>
        set((state) => ({
          summaryCacheByQuery: pruneCacheEntries(
            {
              ...state.summaryCacheByQuery,
              [queryKey]: cache,
            },
            MAX_SUMMARY_CACHE_ENTRIES,
          ),
        })),
      setProfileCache: (symbol, cache) =>
        set((state) => ({
          profileCacheBySymbol: pruneCacheEntries(
            {
              ...state.profileCacheBySymbol,
              [symbol.toUpperCase()]: cache,
            },
            MAX_PROFILE_CACHE_ENTRIES,
          ),
        })),
      setDetailCache: (recordId, cache) =>
        set((state) => ({
          detailCacheById: pruneCacheEntries(
            {
              ...state.detailCacheById,
              [String(recordId)]: cache,
            },
            MAX_DETAIL_CACHE_ENTRIES,
          ),
        })),
      clearSummaryCache: (queryKey) =>
        set((state) => {
          if (!queryKey) {
            return { summaryCacheByQuery: {} };
          }
          const nextCache = { ...state.summaryCacheByQuery };
          delete nextCache[queryKey];
          return { summaryCacheByQuery: nextCache };
        }),
      clearProfileCache: (symbol) =>
        set((state) => {
          if (!symbol) {
            return { profileCacheBySymbol: {} };
          }
          const nextCache = { ...state.profileCacheBySymbol };
          delete nextCache[symbol.toUpperCase()];
          return { profileCacheBySymbol: nextCache };
        }),
      clearDetailCache: (recordId) =>
        set((state) => {
          if (recordId === undefined) {
            return { detailCacheById: {} };
          }
          const nextCache = { ...state.detailCacheById };
          delete nextCache[String(recordId)];
          return { detailCacheById: nextCache };
        }),
    }),
    {
      name: "archive-ui-state",
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        summaryCacheByQuery: state.summaryCacheByQuery,
        profileCacheBySymbol: state.profileCacheBySymbol,
        detailCacheById: state.detailCacheById,
      }),
    },
  ),
);
