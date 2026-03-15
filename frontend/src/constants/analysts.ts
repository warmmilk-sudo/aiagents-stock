export const ANALYST_OPTIONS = [
  { key: "technical", label: "技术分析师" },
  { key: "fundamental", label: "基本面分析师" },
  { key: "fund_flow", label: "资金流分析师" },
  { key: "risk", label: "风险控制分析师" },
  { key: "sentiment", label: "市场情绪分析师" },
  { key: "news", label: "新闻事件分析师" },
] as const;

export type AnalystKey = (typeof ANALYST_OPTIONS)[number]["key"];

export const ANALYST_LABELS: Record<AnalystKey, string> = Object.fromEntries(
  ANALYST_OPTIONS.map((item) => [item.key, item.label]),
) as Record<AnalystKey, string>;

export const DEFAULT_ANALYST_KEYS: AnalystKey[] = [
  "technical",
  "fundamental",
  "fund_flow",
  "risk",
];

export function normalizeAnalystKeys(
  value?: string[] | null,
  fallback: AnalystKey[] = DEFAULT_ANALYST_KEYS,
): AnalystKey[] {
  const allowed = new Set<AnalystKey>(ANALYST_OPTIONS.map((item) => item.key));
  const nextValue = Array.isArray(value)
    ? value.filter((item): item is AnalystKey => allowed.has(item as AnalystKey))
    : [];
  return nextValue.length ? Array.from(new Set(nextValue)) : [...fallback];
}

export function analystKeysToConfig(selectedKeys: string[]) {
  const normalized = new Set(normalizeAnalystKeys(selectedKeys));
  return Object.fromEntries(
    ANALYST_OPTIONS.map((item) => [item.key, normalized.has(item.key)]),
  ) as Record<AnalystKey, boolean>;
}

export function analystConfigToKeys(
  config: Partial<Record<AnalystKey, boolean>> | undefined,
  fallback: AnalystKey[] = DEFAULT_ANALYST_KEYS,
) {
  const selectedKeys = ANALYST_OPTIONS.filter((item) => Boolean(config?.[item.key])).map((item) => item.key);
  return selectedKeys.length ? selectedKeys : [...fallback];
}
