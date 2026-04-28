import { createJSONStorage, type PersistStorage, type StateStorage } from "zustand/middleware";

function isQuotaExceededError(error: unknown) {
  if (!error || typeof error !== "object") {
    return false;
  }
  const record = error as { code?: number; name?: string; message?: string };
  return (
    record.name === "QuotaExceededError" ||
    record.name === "NS_ERROR_DOM_QUOTA_REACHED" ||
    record.code === 22 ||
    record.code === 1014 ||
    /quota/i.test(record.message || "")
  );
}

function warnStorageFailure(action: string, key: string, error: unknown) {
  if (typeof console === "undefined") {
    return;
  }
  console.warn(`[persistStorage] ${action} failed for ${key}`, error);
}

function createSafeLocalStorage(): StateStorage<void> {
  return {
    getItem: (name) => {
      try {
        return window.localStorage.getItem(name);
      } catch (error) {
        warnStorageFailure("read", name, error);
        return null;
      }
    },
    setItem: (name, value) => {
      try {
        window.localStorage.setItem(name, value);
        return;
      } catch (error) {
        if (!isQuotaExceededError(error)) {
          warnStorageFailure("write", name, error);
          return;
        }
      }

      try {
        window.localStorage.removeItem(name);
        window.localStorage.setItem(name, value);
      } catch (retryError) {
        try {
          window.localStorage.removeItem(name);
        } catch {
          // Ignore cleanup failure; the in-memory Zustand state is still valid.
        }
        warnStorageFailure("quota write", name, retryError);
      }
    },
    removeItem: (name) => {
      try {
        window.localStorage.removeItem(name);
      } catch (error) {
        warnStorageFailure("remove", name, error);
      }
    },
  };
}

export function createSafeJSONStorage<T>(): PersistStorage<T, void> | undefined {
  return createJSONStorage<T, void>(() => createSafeLocalStorage());
}
