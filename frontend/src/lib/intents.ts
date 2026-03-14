export interface TypedIntent<TPayload = unknown> {
  type: string;
  payload?: TPayload;
  record_id?: number;
}

export function encodeIntent(intent: TypedIntent): string {
  const json = JSON.stringify(intent);
  return btoa(unescape(encodeURIComponent(json)))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

export function decodeIntent<TPayload = unknown>(value: string | null): TypedIntent<TPayload> | null {
  if (!value) {
    return null;
  }
  try {
    const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
    const padding = normalized.length % 4 === 0 ? "" : "=".repeat(4 - (normalized.length % 4));
    const json = decodeURIComponent(escape(atob(`${normalized}${padding}`)));
    const payload = JSON.parse(json);
    return payload && typeof payload === "object" ? (payload as TypedIntent<TPayload>) : null;
  } catch {
    return null;
  }
}
