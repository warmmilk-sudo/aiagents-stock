import { Fragment } from "react";

import styles from "./ResearchPanels.module.scss";

export interface SplitReportSections {
  body: string;
  reasoning: string;
}

type ReportBlock =
  | { type: "heading"; level: number; text: string }
  | { type: "paragraph"; lines: string[] }
  | { type: "list"; ordered: boolean; items: string[] }
  | { type: "table"; headers: string[]; rows: string[][] };

const REPORT_START_PATTERNS = [
  /^#\s*.+(?:分析报告|报告|深度分析|分析).*$/m,
  /^##\s*.+$/m,
  /^(?:一、|1[\.、])\s*(?:趋势分析|基本概况|核心结论|技术分析|投资建议|市场分析|新闻分析|风险分析|资金分析).*$/m,
];

const REPORT_BODY_MARKERS = [
  /[\[【]?(?:报告正文|正文内容|分析正文|最终报告|正式报告)[\]】]?[:：]?\s*/m,
  /^\s*(?:以下|下面)是(?:最终)?(?:正式)?(?:分析)?报告[:：]?\s*$/m,
];

const PREAMBLE_LINE_PATTERNS = [
  /^(?:好的|下面|以下|基于|根据|综合|结合|我将|我会|先对|接下来|这里是)/u,
  /(?:分析报告如下|正式报告如下|报告如下|为你提供|为您提供)/u,
];

function toReportText(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  if (typeof value === "string") {
    return value.trim();
  }
  return JSON.stringify(value, null, 2);
}

function findReportBodyStart(text: string): number | null {
  const positions = REPORT_START_PATTERNS
    .map((pattern) => text.search(pattern))
    .filter((position) => position >= 0);
  return positions.length ? Math.min(...positions) : null;
}

function cleanReasoningLabel(text: string): string {
  return text
    .replace(/^\s*[\[【]?推理过程[\]】]?\s*/u, "")
    .replace(/^\s*推理过程[:：]\s*/u, "")
    .trim();
}

function cleanBodyLabel(text: string): string {
  return text
    .replace(/^\s*[\[【]?(?:报告正文|正文内容|分析正文|最终报告|正式报告)[\]】]?\s*/u, "")
    .replace(/^\s*(?:以下|下面)是(?:最终)?(?:正式)?(?:分析)?报告[:：]?\s*/u, "")
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

  const marker = text.match(/[\[【]推理过程[\]】]|^\s*推理过程[:：]/m);
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

function isListLine(line: string): boolean {
  return /^\s*(?:[-*+]|(?:\d+[.)])|[①②③④⑤⑥⑦⑧⑨⑩])\s+/.test(line);
}

function stripListMarker(line: string): string {
  return line.replace(/^\s*(?:[-*+]|(?:\d+[.)])|[①②③④⑤⑥⑦⑧⑨⑩])\s+/, "").trim();
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

    if (isListLine(trimmed)) {
      const ordered = /^\s*(?:\d+[.)]|[①②③④⑤⑥⑦⑧⑨⑩])\s+/.test(trimmed);
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
        (isTableLine(nextTrimmed) && index + 1 < lines.length && isTableSeparator(lines[index + 1])) ||
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
          return <h4 className={styles.reportHeadingSecondary} key={`heading-${index}`}>{renderInline(block.text)}</h4>;
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
