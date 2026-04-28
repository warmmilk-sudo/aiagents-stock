import { create } from "zustand";
import { persist } from "zustand/middleware";

import { createSafeJSONStorage } from "../persistStorage";

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
const MAX_PROFILE_CACHE_ENTRY_CHARS = 180_000;
const MAX_DETAIL_CACHE_ENTRY_CHARS = 140_000;
const MAX_ARCHIVE_CACHE_TOTAL_CHARS = 650_000;
const ARCHIVE_STORE_VERSION = 3;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function toTimestamp(value: unknown): number {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : Date.now();
}

function sanitizeSummaryCacheEntries(value: unknown): Record<string, ArchiveSummaryCacheEntry> {
  if (!isRecord(value)) {
    return {};
  }
  const entries = Object.entries(value).flatMap(([key, entry]) => {
    if (!isRecord(entry) || !Array.isArray(entry.items)) {
      return [];
    }
    return [[
      key,
      {
        items: entry.items,
        updatedAt: toTimestamp(entry.updatedAt),
      },
    ]];
  });
  return pruneCacheEntries(Object.fromEntries(entries), MAX_SUMMARY_CACHE_ENTRIES, {
    maxTotalChars: MAX_ARCHIVE_CACHE_TOTAL_CHARS,
  });
}

function sanitizeProfileCacheEntries(value: unknown): Record<string, ArchiveProfileCacheEntry> {
  if (!isRecord(value)) {
    return {};
  }
  const entries = Object.entries(value).flatMap(([key, entry]) => {
    if (!isRecord(entry) || !Array.isArray(entry.records)) {
      return [];
    }
    return [[
      key,
      {
        records: entry.records,
        memoryArchive: entry.memoryArchive ?? null,
        updatedAt: toTimestamp(entry.updatedAt),
      },
    ]];
  });
  return pruneCacheEntries(Object.fromEntries(entries), MAX_PROFILE_CACHE_ENTRIES, {
    maxEntryChars: MAX_PROFILE_CACHE_ENTRY_CHARS,
    maxTotalChars: MAX_ARCHIVE_CACHE_TOTAL_CHARS,
  });
}

function sanitizeDetailCacheEntries(value: unknown): Record<string, ArchiveDetailCacheEntry> {
  if (!isRecord(value)) {
    return {};
  }
  const entries = Object.entries(value).flatMap(([key, entry]) => {
    if (!isRecord(entry) || !("detail" in entry)) {
      return [];
    }
    const detail = isRecord(entry.detail) ? entry.detail : null;
    return [[
      key,
      {
        detail,
        updatedAt: toTimestamp(entry.updatedAt),
      },
    ]];
  });
  return pruneCacheEntries(Object.fromEntries(entries), MAX_DETAIL_CACHE_ENTRIES, {
    maxEntryChars: MAX_DETAIL_CACHE_ENTRY_CHARS,
    maxTotalChars: MAX_ARCHIVE_CACHE_TOTAL_CHARS,
  });
}

function sanitizeArchiveState(value: unknown) {
  const state = isRecord(value) ? value : {};
  return {
    summaryCacheByQuery: sanitizeSummaryCacheEntries(state.summaryCacheByQuery),
    profileCacheBySymbol: sanitizeProfileCacheEntries(state.profileCacheBySymbol),
    detailCacheById: sanitizeDetailCacheEntries(state.detailCacheById),
  };
}

function estimateSerializedChars(value: unknown): number {
  try {
    return JSON.stringify(value).length;
  } catch {
    return Number.POSITIVE_INFINITY;
  }
}

function pruneCacheEntries<T extends { updatedAt: number }>(
  entries: Record<string, T>,
  limit: number,
  options: { maxEntryChars?: number; maxTotalChars?: number } = {},
) {
  const orderedEntries = Object.entries(entries).sort(([, left], [, right]) => right.updatedAt - left.updatedAt);
  const selectedEntries: Array<[string, T]> = [];
  let totalChars = 0;
  for (const [key, entry] of orderedEntries) {
    if (selectedEntries.length >= limit) {
      break;
    }
    const entryChars = estimateSerializedChars(entry);
    if (options.maxEntryChars !== undefined && entryChars > options.maxEntryChars) {
      continue;
    }
    if (options.maxTotalChars !== undefined && totalChars + entryChars > options.maxTotalChars) {
      continue;
    }
    selectedEntries.push([key, entry]);
    totalChars += entryChars;
  }
  return Object.fromEntries(selectedEntries);
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
            { maxTotalChars: MAX_ARCHIVE_CACHE_TOTAL_CHARS },
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
            {
              maxEntryChars: MAX_PROFILE_CACHE_ENTRY_CHARS,
              maxTotalChars: MAX_ARCHIVE_CACHE_TOTAL_CHARS,
            },
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
            {
              maxEntryChars: MAX_DETAIL_CACHE_ENTRY_CHARS,
              maxTotalChars: MAX_ARCHIVE_CACHE_TOTAL_CHARS,
            },
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
      version: ARCHIVE_STORE_VERSION,
      storage: createSafeJSONStorage<Pick<ArchiveState, "summaryCacheByQuery" | "profileCacheBySymbol" | "detailCacheById">>(),
      migrate: (persistedState) => sanitizeArchiveState(persistedState),
      partialize: (state) => ({
        summaryCacheByQuery: state.summaryCacheByQuery,
        profileCacheBySymbol: state.profileCacheBySymbol,
        detailCacheById: state.detailCacheById,
      }),
    },
  ),
);
