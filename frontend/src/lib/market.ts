export function asText(value: unknown, fallback = "N/A"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

export function asNumber(value: unknown): number | null {
  const normalized = String(value ?? "").replace(/,/g, "").trim();
  const result = Number(normalized);
  return Number.isFinite(result) ? result : null;
}

export function numberText(value: unknown, digits = 2): string {
  const result = asNumber(value);
  return result === null ? "N/A" : result.toFixed(digits);
}

export function integerText(value: unknown): string {
  const result = asNumber(value);
  return result === null ? "N/A" : String(Math.round(result));
}

export function normalizeSymbol(value: unknown): string {
  const text = asText(value, "").trim().toUpperCase();
  if (!text) {
    return "";
  }
  return text.includes(".") ? text.split(".")[0] : text;
}

export function findKey(record: Record<string, unknown> | undefined, patterns: string[]): string | null {
  if (!record) {
    return null;
  }
  const keys = Object.keys(record);
  for (const pattern of patterns) {
    const matched = keys.find((key) => key.includes(pattern));
    if (matched) {
      return matched;
    }
  }
  return null;
}

export function downloadCsvRows(rows: Array<Record<string, unknown>>, filename: string) {
  if (!rows.length) {
    return;
  }
  const headers = Array.from(new Set(rows.flatMap((item) => Object.keys(item))));
  const escapeCell = (value: unknown) => `"${String(value ?? "").replace(/"/g, '""')}"`;
  const lines = [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => escapeCell(row[header])).join(",")),
  ];
  const blob = new Blob(["\ufeff", lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}
