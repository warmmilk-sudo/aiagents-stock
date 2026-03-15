import { useEffect, useMemo, useState } from "react";

import styles from "../../pages/ConsolePage.module.scss";
import { FormattedReport } from "./FormattedReport";

export interface ReportWorkspaceEntry {
  key: string;
  label: string;
  title?: string;
  role?: string;
  focusAreas?: string[];
  timestamp?: string;
  body?: unknown;
  reasoning?: string;
  summary?: string;
}

interface ReportWorkspaceProps {
  entries?: ReportWorkspaceEntry[];
  emptyText?: string;
  ariaLabel?: string;
}

function summaryText(entry: ReportWorkspaceEntry): string {
  if (entry.summary) {
    return entry.summary;
  }
  const text = String(entry.body || "")
    .replace(/[#>*`]/g, " ")
    .replace(/\*\*/g, "")
    .split("\n")
    .map((line) => line.trim())
    .find(Boolean);
  return text || "暂无摘要";
}

export function ReportWorkspace({
  entries = [],
  emptyText = "暂无报告",
  ariaLabel = "报告分类",
}: ReportWorkspaceProps) {
  const visibleEntries = useMemo(() => entries.filter((item) => item.body || item.reasoning), [entries]);
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

  if (!visibleEntries.length) {
    return <div className={styles.muted}>{emptyText}</div>;
  }

  return (
    <div className={styles.historyDetailContentStack}>
      <div className={styles.historyDetailTabs} aria-label={ariaLabel} role="tablist">
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
          <div className={styles.reportWorkbenchHeader}>
            <div className={styles.reportWorkbenchHeading}>
              <h3>{activeEntry.title || activeEntry.label}</h3>
              {activeEntry.role || activeEntry.focusAreas?.length ? (
                <p className={styles.helperText}>
                  {[activeEntry.role, activeEntry.focusAreas?.join(" / ")].filter(Boolean).join(" | ")}
                </p>
              ) : null}
              <p className={styles.helperText}>{summaryText(activeEntry)}</p>
            </div>
            {activeEntry.timestamp ? (
              <span className={`${styles.helperText} ${styles.reportWorkbenchTimestamp}`}>{activeEntry.timestamp}</span>
            ) : null}
          </div>

          <div className={styles.reportWorkbenchContent}>
            <FormattedReport content={activeEntry.body} emptyText={emptyText} />
          </div>

          {activeEntry.reasoning ? (
            <details className={styles.historyDetailPanel}>
              <summary className={styles.historyDetailSummary}>推理过程</summary>
              <div className={styles.historyDetailPanelBody}>
                <FormattedReport content={activeEntry.reasoning} emptyText="暂无推理过程" />
              </div>
            </details>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
