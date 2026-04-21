import { useEffect, useMemo, useState, type CSSProperties } from "react";

import { MarkdownReport } from "./MarkdownReport";
import styles from "../../pages/ConsolePage.module.scss";

export interface ReportWorkspaceEntry {
  key: string;
  label: string;
  title?: string;
  role?: string;
  focusAreas?: string[];
  timestamp?: string;
  rawContent?: string;
  body?: unknown;
  reasoning?: string;
  summary?: string;
}

function sanitizeDiscussionSpeakers(value: unknown): string {
  return String(value || "").replace(
    /【(投资总监（主持）|技术分析师|基本面分析师|资金面分析师|风险管理师|市场情绪分析师|新闻分析师)(?:\s+[^\]】:：]{1,12})?】(?=[:：])/g,
    "【$1】",
  );
}

interface ReportWorkspaceProps {
  entries?: ReportWorkspaceEntry[];
  emptyText?: string;
  ariaLabel?: string;
}

export function ReportWorkspace({
  entries = [],
  emptyText = "暂无报告",
  ariaLabel = "报告分类",
}: ReportWorkspaceProps) {
  const visibleEntries = useMemo(
    () => entries.filter((item) => item.rawContent || item.body || item.reasoning),
    [entries],
  );
  const [activeKey, setActiveKey] = useState(visibleEntries[0]?.key ?? "");

  useEffect(() => {
    if (!visibleEntries.length) {
      setActiveKey("");
      return;
    }
    if (!visibleEntries.some((item) => item.key === activeKey)) {
      setActiveKey(visibleEntries[0].key);
    }
  }, [activeKey, visibleEntries]);

  const activeEntry = visibleEntries.find((item) => item.key === activeKey) ?? visibleEntries[0] ?? null;
  const tabsStyle = { "--nested-tab-count": visibleEntries.length } as CSSProperties;

  if (!visibleEntries.length) {
    return <div className={styles.muted}>{emptyText}</div>;
  }

  return (
    <div className={styles.historyDetailContentStack}>
      <div className={styles.historyDetailTabs} aria-label={ariaLabel} role="tablist" style={tabsStyle}>
        {visibleEntries.map((item) => (
          <button
            aria-selected={item.key === activeEntry?.key}
            className={item.key === activeEntry?.key ? styles.nestedTabButtonActive : styles.nestedTabButton}
            key={item.key}
            onClick={() => setActiveKey(item.key)}
            role="tab"
            type="button"
          >
            {item.label}
          </button>
        ))}
      </div>

      {activeEntry ? (
        <div className={styles.reportWorkbenchPanel}>
          <div className={styles.reportWorkbenchContent}>
            <MarkdownReport
              className={styles.rawReportText}
              content={
                activeEntry.key === "__discussion__"
                  ? sanitizeDiscussionSpeakers(activeEntry.rawContent || String(activeEntry.body || ""))
                  : (activeEntry.rawContent || activeEntry.body || emptyText)
              }
              emptyText={emptyText}
            />
          </div>
        </div>
      ) : null}
    </div>
  );
}
