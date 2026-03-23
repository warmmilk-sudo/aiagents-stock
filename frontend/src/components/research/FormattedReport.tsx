import { Fragment } from "react";

import styles from "./ResearchPanels.module.scss";

export interface SplitReportSections {
  body: string;
  reasoning: string;
}

export interface ReportMetricItem {
  label: string;
  value: string;
}

export type FormattedReportVariant = "rich" | "plainList";

interface ReportListItem {
  text: string;
  marker: string;
  ordered: boolean;
  level: number;
  children: ReportListItem[];
}

type ReportBlock =
  | { type: "heading"; level: number; text: string }
  | { type: "paragraph"; lines: string[] }
  | { type: "list"; items: ReportListItem[] }
  | { type: "metrics"; items: ReportMetricItem[] }
  | { type: "table"; headers: string[]; rows: string[][] };

const REPORT_START_PATTERNS = [
  /^#\s*.+(?:分析报告|报告|深度分析|分析).*$/m,
  /^##\s*基本概况.*$/m,
  /^##\s*.+$/m,
  /^(?:一、|1[\.、])\s*(?:趋势分析|基本概况|核心结论|技术分析|投资建议|市场分析|新闻分析|风险分析|资金分析).*$/m,
  /^(?:##\s*)?(?:一、|1[\.、])\s*(?:周期仪表盘|康波周期仪表盘|综合资产配置建议|不同人群的具体建议|核心观点总结|周金涛名言对照).*$/m,
  /^以下(?:为|是).*(?:分析报告|报告|研判|复盘).*$/m,
  /^整体结论先行[:：]?\s*$/m,
  /^\*\*(?:核心判断|核心结论|总体判断).*\*\*$/m,
];

const REPORT_BODY_MARKERS = [
  /^\s*(?:以下|下面)是(?:最终)?(?:正式)?(?:分析)?报告[:：]?\s*$/m,
  /[\[【]?(?:报告正文|正文内容|分析正文|最终报告|正式报告)[\]】]?[:：]?\s*/m,
];

const PREAMBLE_LINE_PATTERNS = [
  /^(?:好的|下面|以下|基于|根据|综合|结合|我将|我会|先对|接下来|这里是)/u,
  /(?:分析报告如下|正式报告如下|报告如下|为你提供|为您提供)/u,
];

const STRUCTURED_HEADING_PATTERNS: Array<{ level: number; pattern: RegExp }> = [
  { level: 1, pattern: /^[一二三四五六七八九十]+[、.]\s*(.+)$/u },
  { level: 2, pattern: /^[（(][一二三四五六七八九十]+[)）]\s*(.+)$/u },
  { level: 2, pattern: /^\d+[.)、]\s*(.+)$/u },
  { level: 3, pattern: /^\d+(?:\.\d+){1,2}\s+(.+)$/u },
  { level: 3, pattern: /^[①②③④⑤⑥⑦⑧⑨⑩]\s*(.+)$/u },
  { level: 2, pattern: /^\*\*(.+)\*\*$/u },
];

const METRIC_LABEL_ALIASES: Record<string, string> = {
  rating: "评级",
  investment_rating: "评级",
  confidence_level: "信心度",
  confidence_score: "置信度",
  target_price: "目标价",
  entry_range: "进场区间",
  entry_min: "进场下沿",
  entry_max: "进场上沿",
  take_profit: "止盈位",
  stop_loss: "止损位",
  holding_period: "持有周期",
  position_size: "仓位建议",
  risk_level: "风险等级",
  market_outlook: "市场展望",
  market_view: "市场观点",
  key_opportunity: "主线机会",
  major_risk: "主要风险",
  strategy: "策略",
  direction: "方向",
  stage: "阶段",
  time_window: "关注周期",
  sector: "板块",
  score: "热度",
  trend: "趋势",
  sustainability: "持续性",
  data_source: "数据来源",
  total_records: "榜单记录",
  total_stocks: "涉及股票",
  total_youzi: "涉及游资",
};

const STRUCTURED_METRIC_FIELDS = [
  "rating",
  "investment_rating",
  "confidence_level",
  "confidence_score",
  "risk_level",
  "market_outlook",
  "target_price",
  "entry_range",
  "take_profit",
  "stop_loss",
  "holding_period",
  "position_size",
  "market_view",
  "key_opportunity",
  "major_risk",
  "strategy",
  "direction",
  "stage",
  "time_window",
  "sector",
  "score",
  "trend",
  "sustainability",
  "data_source",
  "total_records",
  "total_stocks",
  "total_youzi",
];

const METRIC_TEXT_FALLBACK_LABELS = new Set([
  "评级",
  "信心度",
  "置信度",
  "目标价",
  "进场区间",
  "进场下沿",
  "进场上沿",
  "止盈位",
  "止损位",
  "持有周期",
  "仓位建议",
  "风险等级",
  "市场展望",
  "市场观点",
  "主线机会",
  "主要风险",
  "策略",
  "方向",
  "阶段",
  "关注周期",
  "板块",
  "热度",
  "趋势",
  "持续性",
  "数据来源",
  "榜单记录",
  "涉及股票",
  "涉及游资",
]);

const LONG_VALUE_LABELS = new Set(["市场观点", "主线机会", "主要风险", "策略", "风险提示", "操作建议"]);
const METRIC_IGNORED_LABELS = new Set(["推理过程", "思考过程", "分析过程", "报告正文", "正文内容", "分析正文"]);

function toReportText(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  if (typeof value === "string") {
    return value.trim();
  }
  return JSON.stringify(value, null, 2);
}

function removeInlineFormatting(text: string): string {
  return text
    .replace(/\*\*(.*?)\*\*/g, "$1")
    .replace(/__(.*?)__/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[(.*?)\]\([^)]+\)/g, "$1")
    .trim();
}

function findReportBodyStart(text: string): number | null {
  const positions = REPORT_START_PATTERNS
    .map((pattern) => text.search(pattern))
    .filter((position) => position >= 0);
  return positions.length ? Math.min(...positions) : null;
}

function cleanReasoningLabel(text: string): string {
  return text
    .replace(/^\s*[\[【]?(?:推理过程|思考过程|分析过程|推演过程)[\]】]?\s*/u, "")
    .replace(/^\s*(?:推理过程|思考过程|分析过程|推演过程)[:：]\s*/u, "")
    .trim();
}

function cleanBodyLabel(text: string): string {
  return text
    .replace(/^\s*[\[【]?(?:报告正文|正文内容|分析正文|最终报告|正式报告)[\]】]?\s*/u, "")
    .replace(/^\s*(?:以下|下面)是(?:最终)?(?:正式)?(?:分析)?报告[:：]\s*/u, "")
    .trim();
}

function findBodyMarker(text: string): RegExpMatchArray | null {
  for (const pattern of REPORT_BODY_MARKERS) {
    const match = text.match(pattern);
    if (match && match.index !== undefined) {
      return match;
    }
  }
  return null;
}

function isPreambleLine(line: string): boolean {
  const normalized = line.trim();
  if (!normalized) {
    return true;
  }
  return PREAMBLE_LINE_PATTERNS.some((pattern) => pattern.test(normalized));
}

function splitLeadingPreamble(text: string): { body: string; preamble: string } {
  const reportStart = findReportBodyStart(text);
  if (reportStart === null || reportStart <= 0) {
    return { body: text.trim(), preamble: "" };
  }

  const preamble = text.slice(0, reportStart).trim();
  const body = text.slice(reportStart).trim();
  if (!preamble) {
    return { body, preamble: "" };
  }

  const preambleLines = preamble
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  if (preamble.length <= 220 || preambleLines.every((line) => isPreambleLine(line))) {
    return { body, preamble };
  }

  return { body: text.trim(), preamble: "" };
}

function cleanExtractedBody(text: string): string {
  return text.replace(/^\s*分析报告(?:正文)?\s*[:：]\s*/u, "").trim();
}

function combineReasoningParts(...parts: string[]): string {
  return parts.filter(Boolean).join("\n\n").trim();
}

export function splitReportSections(value: unknown): SplitReportSections {
  let text = toReportText(value);
  if (!text) {
    return { body: "", reasoning: "" };
  }

  const reasoningParts: string[] = [];
  text = text.replace(/<think>([\s\S]*?)<\/think>/gi, (_match, captured: string) => {
    const reasoning = captured.trim();
    if (reasoning) {
      reasoningParts.push(reasoning);
    }
    return "";
  }).trim();

  const bodyMarker = findBodyMarker(text);
  if (bodyMarker && bodyMarker.index !== undefined) {
    const beforeMarker = text.slice(0, bodyMarker.index).trim();
    const afterMarker = cleanBodyLabel(text.slice(bodyMarker.index + bodyMarker[0].length));
    const { body, preamble } = splitLeadingPreamble(afterMarker);
    return {
      body: cleanExtractedBody(body),
      reasoning: combineReasoningParts(reasoningParts.join("\n\n"), beforeMarker, preamble),
    };
  }

  const marker = text.match(/[\[【](?:推理过程|思考过程|分析过程|推演过程)[\]】]|^\s*(?:推理过程|思考过程|分析过程|推演过程)[:：]/m);
  if (!marker || marker.index === undefined) {
    const { body, preamble } = splitLeadingPreamble(text);
    return {
      body: cleanExtractedBody(body),
      reasoning: combineReasoningParts(reasoningParts.join("\n\n"), preamble),
    };
  }

  const beforeMarker = text.slice(0, marker.index).trim();
  const afterMarker = cleanReasoningLabel(text.slice(marker.index + marker[0].length).replace(/^[：:\n\s]+/u, ""));

  let body = "";
  let reasoning = "";

  if (beforeMarker) {
    body = beforeMarker;
    reasoning = afterMarker;
  } else {
    const reportStart = findReportBodyStart(afterMarker);
    if (reportStart !== null && reportStart > 0) {
      body = afterMarker.slice(reportStart).trim();
      reasoning = afterMarker.slice(0, reportStart).trim();
    } else {
      body = reportStart === 0 ? afterMarker : "";
      reasoning = reportStart === 0 ? "" : afterMarker;
    }
  }

  return {
    body: cleanExtractedBody(body),
    reasoning: combineReasoningParts(reasoningParts.join("\n\n"), cleanReasoningLabel(reasoning)),
  };
}

function normalizePlainListItem(text: string): string {
  return text
    .replace(/^#{1,6}\s+/u, "")
    .replace(/^\s*(?:[-*+]|(?:\d+[.)、])|(?:[一二三四五六七八九十]+[、.])|(?:[（(][一二三四五六七八九十]+[)）])|[①②③④⑤⑥⑦⑧⑨⑩])\s+/u, "")
    .replace(/[：:]\s*$/u, "")
    .trim();
}

function composePlainListItem(heading: string, content = ""): string {
  const normalizedHeading = normalizePlainListItem(heading);
  const normalizedContent = normalizePlainListItem(content);
  if (normalizedHeading && normalizedContent) {
    return `${normalizedHeading}：${normalizedContent}`;
  }
  return normalizedContent || normalizedHeading;
}

function isTableLine(line: string): boolean {
  const trimmed = line.trim();
  return trimmed.startsWith("|") && trimmed.endsWith("|");
}

function isTableSeparator(line: string): boolean {
  return line
    .trim()
    .split("|")
    .filter(Boolean)
    .every((cell) => /^:?-{3,}:?$/.test(cell.trim()));
}

function parseTableRow(line: string): string[] {
  return line
    .trim()
    .split("|")
    .slice(1, -1)
    .map((cell) => cell.trim());
}

function looksLikeStructuredTitle(text: string): boolean {
  const normalized = removeInlineFormatting(text).replace(/\s+/g, " ").trim();
  if (!normalized) {
    return false;
  }
  if (/[:：]\s*$/.test(normalized)) {
    return true;
  }
  if (/[，,；;]/u.test(normalized) && normalized.length > 18) {
    return false;
  }
  if (/[。！？]/u.test(normalized) && normalized.length > 16) {
    return false;
  }
  return normalized.length <= 32;
}

function parseStructuredHeading(line: string): { level: number; text: string } | null {
  if (/^\s+/.test(line)) {
    return null;
  }

  const candidate = line.replace(/^>\s*/, "").trim();
  if (!candidate) {
    return null;
  }
  for (const matcher of STRUCTURED_HEADING_PATTERNS) {
    const match = candidate.match(matcher.pattern);
    if (!match) {
      continue;
    }
    const text = match[1]?.trim() || "";
    if (!looksLikeStructuredTitle(text)) {
      return null;
    }
    return { level: matcher.level, text: candidate };
  }
  return null;
}

function isListLine(line: string): boolean {
  return Boolean(parseListLine(line));
}

function getListIndentLevel(prefix: string): number {
  const normalizedPrefix = prefix.replace(/\t/g, "  ");
  return Math.floor(normalizedPrefix.length / 2);
}

function getListMarkerLevel(marker: string): number {
  if (/^\d+(?:\.\d+){1,2}$/.test(marker)) {
    return Math.max(1, marker.split(".").length - 1);
  }
  if (/^[（(][一二三四五六七八九十\d]+[)）]$/u.test(marker)) {
    return 1;
  }
  if (/^[①②③④⑤⑥⑦⑧⑨⑩]$/u.test(marker)) {
    return 2;
  }
  return 0;
}

function parseListLine(line: string): { marker: string; text: string; ordered: boolean; level: number } | null {
  const match = line.match(
    /^(\s*)((?:[-*+•·▪◦‣])|(?:\d+(?:\.\d+){1,2})|(?:\d+[.)、])|(?:[一二三四五六七八九十]+[、.])|(?:[（(][一二三四五六七八九十\d]+[)）])|(?:[①②③④⑤⑥⑦⑧⑨⑩]))\s+(.+)$/u,
  );
  if (!match) {
    return null;
  }

  const marker = match[2];
  const text = match[3].trim();
  if (!text) {
    return null;
  }

  const ordered = !/^[-*+•·▪◦‣]$/u.test(marker);
  const indentLevel = getListIndentLevel(match[1]);
  return {
    marker,
    text,
    ordered,
    level: Math.max(indentLevel, getListMarkerLevel(marker)),
  };
}

function buildNestedList(items: Array<{ marker: string; text: string; ordered: boolean; level: number }>): ReportListItem[] {
  const roots: ReportListItem[] = [];
  const stack: ReportListItem[] = [];

  items.forEach((entry) => {
    const item: ReportListItem = {
      text: entry.text,
      marker: entry.marker,
      ordered: entry.ordered,
      level: entry.level,
      children: [],
    };

    while (stack.length && item.level <= stack[stack.length - 1].level) {
      stack.pop();
    }

    if (stack.length) {
      stack[stack.length - 1].children.push(item);
    } else {
      roots.push(item);
    }

    stack.push(item);
  });

  return roots;
}

function flattenNestedListItems(items: ReportListItem[]): string[] {
  return items.flatMap((item) => [normalizePlainListItem(item.text), ...flattenNestedListItems(item.children)]).filter(Boolean);
}

function humanizeMetricLabel(label: string): string {
  const normalized = label
    .trim()
    .replace(/^#+\s*/u, "")
    .replace(/[：:]+$/u, "")
    .replace(/\s+/g, "_")
    .toLowerCase();
  return METRIC_LABEL_ALIASES[normalized] || label.trim();
}

function isMetricValueValid(label: string, value: string, segmented: boolean): boolean {
  const normalized = removeInlineFormatting(value).replace(/\s+/g, " ").trim();
  if (!normalized) {
    return false;
  }
  if (/^[{\[]/.test(normalized) || /^(?:true|false|null)$/i.test(normalized)) {
    return false;
  }
  if (/^(?:以下|下面)是/u.test(normalized)) {
    return false;
  }
  const maxLength = LONG_VALUE_LABELS.has(label) ? 72 : segmented ? 52 : 42;
  if (normalized.length > maxLength) {
    return false;
  }
  if (/[。！？]/u.test(normalized) && normalized.length > 24 && !LONG_VALUE_LABELS.has(label)) {
    return false;
  }
  return true;
}

function dedupeMetrics(items: ReportMetricItem[]): ReportMetricItem[] {
  const seen = new Set<string>();
  const result: ReportMetricItem[] = [];
  items.forEach((item) => {
    const label = humanizeMetricLabel(item.label);
    const value = removeInlineFormatting(item.value).replace(/\s+/g, " ").trim();
    if (!label || !value) {
      return;
    }
    const key = `${label}::${value}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    result.push({ label, value });
  });
  return result;
}

function extractMetricPairsFromLine(line: string): ReportMetricItem[] {
  const candidate = line
    .replace(/^>\s*/u, "")
    .replace(/^\s*(?:[-*+]|(?:\d+[.)、])|(?:[一二三四五六七八九十]+[、.])|(?:[（(][一二三四五六七八九十]+[)）])|[①②③④⑤⑥⑦⑧⑨⑩])\s+/u, "")
    .trim();
  if (!candidate) {
    return [];
  }

  const segments = candidate.split(/[|｜]/u).map((item) => item.trim()).filter(Boolean);
  const sources = segments.length > 1 ? segments : [candidate];

  return dedupeMetrics(
    sources.flatMap((segment) => {
      const match = removeInlineFormatting(segment).match(/^([^：:]{1,14}?)\s*[：:]\s*(.+)$/u);
      if (!match) {
        return [];
      }

      const label = humanizeMetricLabel(match[1]);
      const value = match[2].trim();
      if (!label || !value || METRIC_IGNORED_LABELS.has(label)) {
        return [];
      }
      if (!METRIC_TEXT_FALLBACK_LABELS.has(label) && !LONG_VALUE_LABELS.has(label) && label.length > 10) {
        return [];
      }
      if (!isMetricValueValid(label, value, segments.length > 1)) {
        return [];
      }
      return [{ label, value }];
    }),
  );
}

function flattenBlocksToPlainItems(text: string): string[] {
  const blocks = parseBlocks(text);
  if (!blocks.length) {
    return [];
  }

  const items: string[] = [];
  let pendingHeading = "";

  const flushHeading = () => {
    if (pendingHeading) {
      items.push(normalizePlainListItem(pendingHeading));
      pendingHeading = "";
    }
  };

  blocks.forEach((block) => {
    if (block.type === "heading") {
      flushHeading();
      pendingHeading = block.text;
      return;
    }

    if (block.type === "paragraph") {
      const paragraph = block.lines.map((line) => line.trim()).filter(Boolean).join(" ");
      const item = composePlainListItem(pendingHeading, paragraph);
      if (item) {
        items.push(item);
      }
      pendingHeading = "";
      return;
    }

    if (block.type === "list") {
      const listItems = flattenNestedListItems(block.items);
      if (!listItems.length) {
        flushHeading();
        return;
      }
      if (pendingHeading) {
        listItems.forEach((item, index) => {
          items.push(index === 0 ? composePlainListItem(pendingHeading, item) : item);
        });
        pendingHeading = "";
        return;
      }
      items.push(...listItems);
      return;
    }

    if (block.type === "metrics") {
      const metricItems = block.items.map((item) => `${item.label}：${item.value}`);
      if (!metricItems.length) {
        flushHeading();
        return;
      }
      if (pendingHeading) {
        items.push(composePlainListItem(pendingHeading, metricItems[0]));
        items.push(...metricItems.slice(1));
        pendingHeading = "";
        return;
      }
      items.push(...metricItems);
      return;
    }

    const tableItems = block.rows
      .map((row) => block.headers
        .map((header, index) => {
          const value = row[index]?.trim() || "";
          return value ? `${header}：${value}` : "";
        })
        .filter(Boolean)
        .join("；"))
      .filter(Boolean);
    if (!tableItems.length) {
      flushHeading();
      return;
    }
    if (pendingHeading) {
      items.push(composePlainListItem(pendingHeading, tableItems[0]));
      items.push(...tableItems.slice(1));
      pendingHeading = "";
      return;
    }
    items.push(...tableItems);
  });

  flushHeading();
  return items.filter(Boolean);
}

function parseBlocks(text: string): ReportBlock[] {
  const normalized = text.replace(/\r\n/g, "\n").trim();
  if (!normalized) {
    return [];
  }

  const lines = normalized.split("\n");
  const blocks: ReportBlock[] = [];
  let index = 0;

  while (index < lines.length) {
    const currentLine = lines[index].trimEnd();
    const trimmed = currentLine.trim();

    if (!trimmed) {
      index += 1;
      continue;
    }

    const headingMatch = trimmed.match(/^(#{1,4})\s+(.+)$/);
    if (headingMatch) {
      blocks.push({
        type: "heading",
        level: headingMatch[1].length,
        text: headingMatch[2].trim(),
      });
      index += 1;
      continue;
    }

    const structuredHeading = parseStructuredHeading(currentLine);
    if (structuredHeading) {
      blocks.push({
        type: "heading",
        level: structuredHeading.level,
        text: structuredHeading.text,
      });
      index += 1;
      continue;
    }

    if (isTableLine(trimmed) && index + 1 < lines.length && isTableSeparator(lines[index + 1])) {
      const headers = parseTableRow(trimmed);
      const rows: string[][] = [];
      index += 2;
      while (index < lines.length && isTableLine(lines[index].trim())) {
        rows.push(parseTableRow(lines[index]));
        index += 1;
      }
      blocks.push({ type: "table", headers, rows });
      continue;
    }

    const metricItems = extractMetricPairsFromLine(trimmed);
    if (metricItems.length) {
      const collected = [...metricItems];
      index += 1;
      while (index < lines.length) {
        const nextTrimmed = lines[index].trim();
        if (!nextTrimmed) {
          index += 1;
          break;
        }
        const nextMetrics = extractMetricPairsFromLine(nextTrimmed);
        if (!nextMetrics.length) {
          break;
        }
        collected.push(...nextMetrics);
        index += 1;
      }
      blocks.push({ type: "metrics", items: dedupeMetrics(collected) });
      continue;
    }

    if (isListLine(trimmed)) {
      const items: Array<{ marker: string; text: string; ordered: boolean; level: number }> = [];
      while (index < lines.length) {
        const parsed = parseListLine(lines[index]);
        if (!parsed) {
          break;
        }
        items.push(parsed);
        index += 1;
      }
      blocks.push({ type: "list", items: buildNestedList(items) });
      continue;
    }

    const paragraphLines: string[] = [];
    while (index < lines.length) {
      const candidate = lines[index].trimEnd();
      const nextTrimmed = candidate.trim();
      if (!nextTrimmed) {
        index += 1;
        break;
      }
      if (
        nextTrimmed.match(/^(#{1,4})\s+(.+)$/) ||
        parseStructuredHeading(candidate) ||
        (isTableLine(nextTrimmed) && index + 1 < lines.length && isTableSeparator(lines[index + 1])) ||
        extractMetricPairsFromLine(nextTrimmed).length ||
        isListLine(nextTrimmed)
      ) {
        break;
      }
      paragraphLines.push(nextTrimmed);
      index += 1;
    }
    blocks.push({ type: "paragraph", lines: paragraphLines });
  }

  return blocks;
}

function renderInline(text: string) {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, index) => {
    if (part.startsWith("**") && part.endsWith("**") && part.length > 4) {
      return <strong key={`${part}-${index}`}>{part.slice(2, -2)}</strong>;
    }
    return <Fragment key={`${part}-${index}`}>{part}</Fragment>;
  });
}

function renderListItems(items: ReportListItem[], level = 0) {
  return (
    <ul className={styles.reportListTree} data-level={level}>
      {items.map((item, index) => (
        <li
          className={styles.reportListItem}
          data-level={level}
          data-ordered={item.ordered ? "true" : "false"}
          key={`${item.marker}-${item.text}-${index}`}
        >
          <div className={styles.reportListItemLine}>
            <span className={styles.reportListMarker}>{item.marker}</span>
            <span className={styles.reportListContent}>{renderInline(item.text)}</span>
          </div>
          {item.children.length ? renderListItems(item.children, level + 1) : null}
        </li>
      ))}
    </ul>
  );
}

function formatMetricValue(label: string, value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  if (label === "进场区间" && typeof value === "object" && !Array.isArray(value)) {
    const payload = value as { min?: unknown; max?: unknown };
    const min = payload.min ?? "";
    const max = payload.max ?? "";
    return [min, max].filter(Boolean).join(" - ");
  }
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean).join(" / ");
  }
  if (typeof value === "number") {
    if (label === "置信度" || label === "热度") {
      return `${Math.round(value)}分`;
    }
    return Number.isInteger(value) ? String(value) : value.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
  }
  return String(value).trim();
}

function collectStructuredMetrics(value: unknown, items: ReportMetricItem[]): void {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return;
  }

  const payload = value as Record<string, unknown>;
  const entryRangeValue =
    payload.entry_range && typeof payload.entry_range === "object" && !Array.isArray(payload.entry_range)
      ? formatMetricValue("进场区间", payload.entry_range)
      : payload.entry_range
        ? formatMetricValue("进场区间", payload.entry_range)
        : payload.entry_min !== undefined || payload.entry_max !== undefined
          ? [payload.entry_min, payload.entry_max].filter((item) => item !== null && item !== undefined && item !== "").join(" - ")
          : "";
  if (entryRangeValue) {
    items.push({ label: "进场区间", value: entryRangeValue });
  }

  STRUCTURED_METRIC_FIELDS.forEach((field) => {
    if (!(field in payload) || field === "entry_range") {
      return;
    }
    const label = METRIC_LABEL_ALIASES[field] || field;
    const formatted = formatMetricValue(label, payload[field]);
    if (formatted) {
      items.push({ label, value: formatted });
    }
  });

  if (payload.summary && typeof payload.summary === "object" && !Array.isArray(payload.summary)) {
    collectStructuredMetrics(payload.summary, items);
  }
}

function collectTextMetrics(value: unknown, items: ReportMetricItem[]): void {
  const text = toReportText(value);
  if (!text) {
    return;
  }
  text
    .replace(/\r\n/g, "\n")
    .split("\n")
    .forEach((line) => {
      items.push(...extractMetricPairsFromLine(line));
    });
}

export function extractReportKeyMetrics(content: unknown, limit = 6): ReportMetricItem[] {
  const collected: ReportMetricItem[] = [];
  collectStructuredMetrics(content, collected);
  collectTextMetrics(content, collected);
  return dedupeMetrics(collected).slice(0, limit);
}

interface FormattedReportProps {
  content: unknown;
  emptyText?: string;
  variant?: FormattedReportVariant;
}

export function FormattedReport({ content, emptyText = "暂无正文", variant = "rich" }: FormattedReportProps) {
  const text = toReportText(content);

  if (variant === "plainList") {
    const items = flattenBlocksToPlainItems(text);
    if (!items.length) {
      return <div className={styles.muted}>{emptyText}</div>;
    }

    return (
      <ul className={styles.reportList}>
        {items.map((item, index) => (
          <li key={`${item}-${index}`}>{renderInline(item)}</li>
        ))}
      </ul>
    );
  }

  const blocks = parseBlocks(text);
  if (!blocks.length) {
    return <div className={styles.muted}>{emptyText}</div>;
  }

  return (
    <div className={styles.reportRichText}>
      {blocks.map((block, index) => {
        if (block.type === "heading") {
          if (block.level <= 1) {
            return <h3 className={styles.reportHeadingPrimary} key={`heading-${index}`}>{renderInline(block.text)}</h3>;
          }
          if (block.level === 2) {
            return <h4 className={styles.reportHeadingSecondary} key={`heading-${index}`}>{renderInline(block.text)}</h4>;
          }
          return <h5 className={styles.reportHeadingTertiary} key={`heading-${index}`}>{renderInline(block.text)}</h5>;
        }

        if (block.type === "metrics") {
          return (
            <div className={styles.reportMetricGrid} key={`metrics-${index}`}>
              {block.items.map((item, itemIndex) => (
                <div className={styles.reportMetricCard} key={`${item.label}-${item.value}-${itemIndex}`}>
                  <span className={styles.reportMetricLabel}>{item.label}</span>
                  <strong className={styles.reportMetricValue}>{renderInline(item.value)}</strong>
                </div>
              ))}
            </div>
          );
        }

        if (block.type === "list") {
          return <Fragment key={`list-${index}`}>{renderListItems(block.items)}</Fragment>;
        }

        if (block.type === "table") {
          return (
            <div className={styles.reportTableWrap} key={`table-${index}`}>
              <table className={styles.reportTable}>
                <thead>
                  <tr>
                    {block.headers.map((header, headerIndex) => (
                      <th key={`header-${headerIndex}`}>{renderInline(header)}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {block.rows.map((row, rowIndex) => (
                    <tr key={`row-${rowIndex}`}>
                      {row.map((cell, cellIndex) => (
                        <td key={`cell-${rowIndex}-${cellIndex}`}>{renderInline(cell)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          );
        }

        return (
          <p className={styles.reportParagraph} key={`paragraph-${index}`}>
            {block.lines.map((line, lineIndex) => (
              <Fragment key={`line-${lineIndex}`}>
                {lineIndex ? <br /> : null}
                {renderInline(line)}
              </Fragment>
            ))}
          </p>
        );
      })}
    </div>
  );
}
