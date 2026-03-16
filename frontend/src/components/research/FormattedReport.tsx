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

type ReportBlock =
  | { type: "heading"; level: number; text: string }
  | { type: "paragraph"; lines: string[] }
  | { type: "list"; ordered: boolean; items: string[] }
  | { type: "metrics"; items: ReportMetricItem[] }
  | { type: "table"; headers: string[]; rows: string[][] };

const REPORT_START_PATTERNS = [
  /^#\s*.+(?:分析报告|报告|深度分析|分析).*$/m,
  /^##\s*.+$/m,
  /^(?:一、|1[\.、])\s*(?:趋势分析|基本概况|核心结论|技术分析|投资建议|市场分析|新闻分析|风险分析|资金分析).*$/m,
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
      body,
      reasoning: [reasoningParts.join("\n\n"), beforeMarker, preamble].filter(Boolean).join("\n\n").trim(),
    };
  }

  const marker = text.match(/[\[【](?:推理过程|思考过程|分析过程|推演过程)[\]】]|^\s*(?:推理过程|思考过程|分析过程|推演过程)[:：]/m);
  if (!marker || marker.index === undefined) {
    const { body, preamble } = splitLeadingPreamble(text);
    return {
      body,
      reasoning: [reasoningParts.join("\n\n"), preamble].filter(Boolean).join("\n\n").trim(),
    };
  }

  const beforeMarker = text.slice(0, marker.index).trim();
  const afterMarker = cleanReasoningLabel(text.slice(marker.index + marker[0].length));

  if (beforeMarker) {
    return {
      body: beforeMarker,
      reasoning: [reasoningParts.join("\n\n"), afterMarker].filter(Boolean).join("\n\n").trim(),
    };
  }

  const reportStart = findReportBodyStart(afterMarker);
  if (reportStart !== null && reportStart > 0) {
    return {
      body: afterMarker.slice(reportStart).trim(),
      reasoning: [reasoningParts.join("\n\n"), afterMarker.slice(0, reportStart).trim()].filter(Boolean).join("\n\n").trim(),
    };
  }

  return {
    body: reportStart === 0 ? afterMarker : "",
    reasoning: [reasoningParts.join("\n\n"), reportStart === 0 ? "" : afterMarker].filter(Boolean).join("\n\n").trim(),
  };
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
    return { level: matcher.level, text };
  }
  return null;
}

function isListLine(line: string): boolean {
  return /^\s*(?:[-*+]|(?:\d+[.)、])|(?:[一二三四五六七八九十]+[、.])|(?:[（(][一二三四五六七八九十]+[)）])|[①②③④⑤⑥⑦⑧⑨⑩])\s+/.test(line);
}

function stripListMarker(line: string): string {
  return line.replace(/^\s*(?:[-*+]|(?:\d+[.)、])|(?:[一二三四五六七八九十]+[、.])|(?:[（(][一二三四五六七八九十]+[)）])|[①②③④⑤⑥⑦⑧⑨⑩])\s+/, "").trim();
}

function isOrderedListLine(line: string): boolean {
  return /^\s*(?:(?:\d+[.)、])|(?:[一二三四五六七八九十]+[、.])|(?:[（(][一二三四五六七八九十]+[)）])|[①②③④⑤⑥⑦⑧⑨⑩])\s+/.test(line);
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

    const structuredHeading = parseStructuredHeading(trimmed);
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
      const ordered = isOrderedListLine(trimmed);
      const items: string[] = [];
      while (index < lines.length && isListLine(lines[index].trim())) {
        items.push(stripListMarker(lines[index]));
        index += 1;
      }
      blocks.push({ type: "list", ordered, items });
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
        parseStructuredHeading(nextTrimmed) ||
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
}

export function FormattedReport({ content, emptyText = "暂无正文" }: FormattedReportProps) {
  const blocks = parseBlocks(toReportText(content));
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
          const ListTag = block.ordered ? "ol" : "ul";
          return (
            <ListTag className={styles.reportList} key={`list-${index}`}>
              {block.items.map((item, itemIndex) => (
                <li key={`item-${itemIndex}`}>{renderInline(item)}</li>
              ))}
            </ListTag>
          );
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
