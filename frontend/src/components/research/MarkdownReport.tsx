import { Fragment, type ReactNode } from "react";

interface MarkdownReportProps {
  content?: unknown;
  className?: string;
  emptyText?: string;
}

function parseTableRow(line: string): string[] | null {
  const trimmed = line.trim();
  if (!trimmed.includes("|")) {
    return null;
  }

  const normalized = trimmed.replace(/^\|/, "").replace(/\|$/, "");
  const cells = normalized.split("|").map((cell) => cell.trim());
  return cells.length >= 2 ? cells : null;
}

function isTableDivider(line: string): boolean {
  const cells = parseTableRow(line);
  if (!cells) {
    return false;
  }
  return cells.every((cell) => /^:?-{3,}:?$/.test(cell));
}

function renderInline(text: string, keyPrefix: string): ReactNode[] {
  return text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g).map((part, index) => {
    if (/^\*\*[^*]+\*\*$/.test(part)) {
      return <strong key={`${keyPrefix}-strong-${index}`}>{part.slice(2, -2)}</strong>;
    }
    if (/^`[^`]+`$/.test(part)) {
      return <code key={`${keyPrefix}-code-${index}`}>{part.slice(1, -1)}</code>;
    }
    return <Fragment key={`${keyPrefix}-text-${index}`}>{part}</Fragment>;
  });
}

export function MarkdownReport({ content, className, emptyText = "暂无正文" }: MarkdownReportProps) {
  const text = String(content || "").replace(/\r\n/g, "\n").trim();

  if (!text) {
    return <div className={className}>{emptyText}</div>;
  }

  const lines = text.split("\n");
  const blocks: ReactNode[] = [];

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      blocks.push(<div aria-hidden="true" key={`spacer-${index}`} />);
      continue;
    }

    if (/^(-{3,}|\*{3,}|_{3,})$/.test(trimmed)) {
      blocks.push(<hr key={`hr-${index}`} />);
      continue;
    }

    const codeFenceMatch = trimmed.match(/^```(?:\s*(\w+))?\s*$/);
    if (codeFenceMatch) {
      const codeLines: string[] = [];
      let cursor = index + 1;
      while (cursor < lines.length && !lines[cursor].trim().match(/^```\s*$/)) {
        codeLines.push(lines[cursor]);
        cursor += 1;
      }
      blocks.push(
        <pre key={`code-${index}`}>
          <code>{codeLines.join("\n")}</code>
        </pre>,
      );
      index = cursor < lines.length ? cursor : lines.length - 1;
      continue;
    }

    const headingMatch = trimmed.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      const headingContent = renderInline(headingMatch[2].trim(), `heading-${index}`);
      if (headingMatch[1].length === 1) {
        blocks.push(<h1 key={`heading-${index}`}>{headingContent}</h1>);
      } else if (headingMatch[1].length === 2) {
        blocks.push(<h2 key={`heading-${index}`}>{headingContent}</h2>);
      } else {
        blocks.push(<h3 key={`heading-${index}`}>{headingContent}</h3>);
      }
      continue;
    }

    const unorderedMatch = trimmed.match(/^[-*]\s+(.*)$/);
    if (unorderedMatch) {
      const items: ReactNode[] = [];
      let cursor = index;
      while (cursor < lines.length) {
        const currentLine = lines[cursor].trim();
        const currentMatch = currentLine.match(/^[-*]\s+(.*)$/);
        if (!currentMatch) {
          break;
        }
        items.push(
          <li key={`ul-item-${cursor}`}>{renderInline(currentMatch[1].trim(), `ul-item-${cursor}`)}</li>,
        );
        cursor += 1;
      }
      blocks.push(<ul key={`ul-${index}`}>{items}</ul>);
      index = cursor - 1;
      continue;
    }

    const blockquoteMatch = trimmed.match(/^>\s?(.*)$/);
    if (blockquoteMatch) {
      const quoteLines: string[] = [];
      let cursor = index;
      while (cursor < lines.length) {
        const currentLine = lines[cursor].trim();
        const currentMatch = currentLine.match(/^>\s?(.*)$/);
        if (!currentMatch) {
          break;
        }
        quoteLines.push(currentMatch[1]);
        cursor += 1;
      }
      blocks.push(
        <blockquote key={`blockquote-${index}`}>
          {quoteLines.map((quoteLine, quoteIndex) => (
            <p key={`blockquote-${index}-line-${quoteIndex}`}>
              {renderInline(quoteLine, `blockquote-${index}-line-${quoteIndex}`)}
            </p>
          ))}
        </blockquote>,
      );
      index = cursor - 1;
      continue;
    }

    const headerCells = parseTableRow(line);
    if (headerCells && index + 1 < lines.length && isTableDivider(lines[index + 1])) {
      const bodyRows: string[][] = [];
      let cursor = index + 2;
      while (cursor < lines.length) {
        const rowCells = parseTableRow(lines[cursor]);
        if (!rowCells || rowCells.length !== headerCells.length) {
          break;
        }
        bodyRows.push(rowCells);
        cursor += 1;
      }

      blocks.push(
        <div className="markdown-table" key={`table-${index}`}>
          <table>
            <thead>
              <tr>
                {headerCells.map((cell, cellIndex) => (
                  <th key={`table-${index}-head-${cellIndex}`}>{renderInline(cell, `table-${index}-head-${cellIndex}`)}</th>
                ))}
              </tr>
            </thead>
            {bodyRows.length ? (
              <tbody>
                {bodyRows.map((row, rowIndex) => (
                  <tr key={`table-${index}-row-${rowIndex}`}>
                    {row.map((cell, cellIndex) => (
                      <td key={`table-${index}-row-${rowIndex}-cell-${cellIndex}`}>
                        {renderInline(cell, `table-${index}-row-${rowIndex}-cell-${cellIndex}`)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            ) : null}
          </table>
        </div>,
      );
      index = cursor - 1;
      continue;
    }

    const orderedMatch = trimmed.match(/^\d+\.\s+(.*)$/);
    if (orderedMatch) {
      const items: ReactNode[] = [];
      let cursor = index;
      while (cursor < lines.length) {
        const currentLine = lines[cursor].trim();
        const currentMatch = currentLine.match(/^\d+\.\s+(.*)$/);
        if (!currentMatch) {
          break;
        }
        items.push(
          <li key={`ol-item-${cursor}`}>{renderInline(currentMatch[1].trim(), `ol-item-${cursor}`)}</li>,
        );
        cursor += 1;
      }
      blocks.push(<ol key={`ol-${index}`}>{items}</ol>);
      index = cursor - 1;
      continue;
    }

    blocks.push(<p key={`paragraph-${index}`}>{renderInline(line, `paragraph-${index}`)}</p>);
  }

  return <div className={className}>{blocks}</div>;
}
