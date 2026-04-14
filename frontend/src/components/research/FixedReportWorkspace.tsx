import { useEffect, useMemo, useState, type CSSProperties } from "react";

import { MarkdownReport } from "./MarkdownReport";
import styles from "../../pages/ConsolePage.module.scss";

export type FixedReportCategory = "macro" | "sector" | "fund" | "sentiment" | "team";

export interface FixedReportEntry {
  title?: string;
  role?: string;
  focus_areas?: string[];
  timestamp?: string;
  rawContent?: string;
  body?: unknown;
  reasoning?: string;
  summary?: string;
}

export type FixedReportMap = Record<FixedReportCategory, FixedReportEntry | null | undefined>;

const reportTabs: Array<{ key: FixedReportCategory; label: string; emptyText: string }> = [
  { key: "macro", label: "宏观", emptyText: "暂无宏观报告" },
  { key: "sector", label: "板块", emptyText: "暂无板块报告" },
  { key: "fund", label: "资金", emptyText: "暂无资金报告" },
  { key: "sentiment", label: "情绪", emptyText: "暂无情绪报告" },
  { key: "team", label: "综合", emptyText: "暂无综合报告" },
];

interface FixedReportWorkspaceProps {
  reports?: Partial<FixedReportMap> | null;
}

export function FixedReportWorkspace({ reports }: FixedReportWorkspaceProps) {
  const entries = useMemo<FixedReportMap>(() => ({
    macro: reports?.macro ?? null,
    sector: reports?.sector ?? null,
    fund: reports?.fund ?? null,
    sentiment: reports?.sentiment ?? null,
    team: reports?.team ?? null,
  }), [reports]);

  const [activeKey, setActiveKey] = useState<FixedReportCategory>("macro");

  useEffect(() => {
    if (entries[activeKey]) {
      return;
    }
    const fallback = reportTabs.find((item) => entries[item.key])?.key ?? "macro";
    setActiveKey(fallback);
  }, [activeKey, entries]);

  const activeEntry = entries[activeKey] ?? null;
  const activeTab = reportTabs.find((item) => item.key === activeKey) ?? reportTabs[0];
  const tabsStyle = { "--nested-tab-count": reportTabs.length } as CSSProperties;

  if (!reportTabs.some((item) => entries[item.key])) {
    return <div className={styles.muted}>暂无原始报告</div>;
  }

  return (
    <div className={styles.historyDetailContentStack}>
      <div className={styles.historyDetailTabs} aria-label="原始报告分类" role="tablist" style={tabsStyle}>
        {reportTabs.map((item) => (
          <button
            aria-selected={item.key === activeKey}
            className={item.key === activeKey ? styles.nestedTabButtonActive : styles.nestedTabButton}
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
              content={activeEntry.rawContent || String(activeEntry.body || activeTab.emptyText)}
              emptyText={activeTab.emptyText}
            />
          </div>
        </div>
      ) : (
        <div className={styles.muted}>{activeTab.emptyText}</div>
      )}
    </div>
  );
}
