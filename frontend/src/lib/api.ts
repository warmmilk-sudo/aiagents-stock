export interface ApiEnvelope<T> {
  success: boolean;
  message: string;
  data: T;
  error_code?: string;
  details?: unknown;
}

export class ApiRequestError extends Error {
  status: number;
  errorCode?: string;
  details?: unknown;

  constructor(status: number, message: string, errorCode?: string, details?: unknown) {
    super(message);
    this.name = "ApiRequestError";
    this.status = status;
    this.errorCode = errorCode;
    this.details = details;
  }
}

interface ApiFetchCacheEntry {
  expiresAt: number;
  data: unknown;
}

interface ApiFetchCachedOptions {
  ttlMs?: number;
}

const DEFAULT_CACHE_TTL_MS = 15_000;
const apiResponseCache = new Map<string, ApiFetchCacheEntry>();
const inFlightRequests = new Map<string, Promise<unknown>>();

function buildRequestKey(path: string, method: string) {
  return `${method.toUpperCase()} ${path}`;
}

function getCachedResponse<T>(key: string): T | null {
  const cached = apiResponseCache.get(key);
  if (!cached) {
    return null;
  }
  if (cached.expiresAt <= Date.now()) {
    apiResponseCache.delete(key);
    return null;
  }
  return cached.data as T;
}

function setCachedResponse<T>(key: string, data: T, ttlMs: number) {
  apiResponseCache.set(key, {
    expiresAt: Date.now() + Math.max(0, ttlMs),
    data,
  });
}

function clearApiCache() {
  apiResponseCache.clear();
}

async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers ?? {});
  const isJsonBody = init.body !== undefined && !(init.body instanceof FormData);
  if (isJsonBody && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(path, {
    credentials: "include",
    ...init,
    headers,
  });

  let payload: ApiEnvelope<T> | null = null;
  try {
    payload = (await response.json()) as ApiEnvelope<T>;
  } catch {
    payload = null;
  }

  if (!response.ok || !payload?.success) {
    throw new ApiRequestError(
      response.status,
      payload?.message || response.statusText || "请求失败",
      payload?.error_code,
      payload?.details,
    );
  }

  return payload.data;
}

export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const method = (init.method ?? "GET").toUpperCase();
  const requestKey = buildRequestKey(path, method);

  if (method === "GET") {
    const inFlight = inFlightRequests.get(requestKey);
    if (inFlight) {
      return inFlight as Promise<T>;
    }
    const request = requestJson<T>(path, init).finally(() => {
      inFlightRequests.delete(requestKey);
    });
    inFlightRequests.set(requestKey, request);
    return request;
  }

  const result = await requestJson<T>(path, init);
  clearApiCache();
  return result;
}

export async function apiFetchCached<T>(
  path: string,
  init: RequestInit = {},
  options: ApiFetchCachedOptions = {},
): Promise<T> {
  const method = (init.method ?? "GET").toUpperCase();
  if (method !== "GET") {
    return apiFetch<T>(path, init);
  }

  const ttlMs = options.ttlMs ?? DEFAULT_CACHE_TTL_MS;
  const requestKey = buildRequestKey(path, method);
  const cached = getCachedResponse<T>(requestKey);
  if (cached !== null) {
    return cached;
  }

  const inFlight = inFlightRequests.get(requestKey);
  if (inFlight) {
    return inFlight as Promise<T>;
  }

  const request = requestJson<T>(path, init)
    .then((data) => {
      setCachedResponse(requestKey, data, ttlMs);
      return data;
    })
    .finally(() => {
      inFlightRequests.delete(requestKey);
    });

  inFlightRequests.set(requestKey, request);
  return request;
}

export async function downloadApiFile(path: string, init: RequestInit = {}): Promise<void> {
  const headers = new Headers(init.headers ?? {});
  const isJsonBody = init.body !== undefined && !(init.body instanceof FormData);
  if (isJsonBody && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(path, {
    credentials: "include",
    ...init,
    headers,
  });

  if (!response.ok) {
    let message = response.statusText || "下载失败";
    try {
      const payload = (await response.json()) as ApiEnvelope<unknown>;
      if (!payload.success) {
        message = payload.message || message;
      }
    } catch {
      // ignore malformed json
    }
    throw new ApiRequestError(response.status, message);
  }

  const blob = await response.blob();
  const contentDisposition = response.headers.get("content-disposition") || "";
  const filenameMatch = contentDisposition.match(/filename="?(.*?)"?$/i);
  const filename = filenameMatch?.[1] || "download.bin";
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

export function buildQuery(params: Record<string, string | number | boolean | null | undefined>): string {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    search.set(key, String(value));
  });
  const text = search.toString();
  return text ? `?${text}` : "";
}
