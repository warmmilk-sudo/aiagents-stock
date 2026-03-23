import { useEffect, useMemo, useState } from "react";

import { formatDateTime } from "../../lib/datetime";
import { FormattedReport, splitReportSections } from "./FormattedReport";
import styles from "./ResearchPanels.module.scss";

interface ReportEntry {
  key: string;
  title: string;
  role: string;
  focusAreas: string[];
  timestamp: string;
  body: unknown;
  reasoning: string;
  summary: string;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function buildSummary(value: unknown): string {
  const text = String(value || "")
    .replace(/[#>*`]/g, " ")
    .replace(/\*\*/g, "")
    .split("\n")
    .map((line) => line.trim())
    .find(Boolean);
  return text || "暂无摘要";
}

function toEntries(agentsResults: Record<string, unknown>, discussionResult?: unknown): ReportEntry[] {
  const entries = Object.entries(agentsResults).map(([name, payload]) => {
    const normalizedPayload = asRecord(payload);
    const reportSections = splitReportSections(
      normalizedPayload.analysis || normalizedPayload.report || payload,
    );
    const body = reportSections.body || (reportSections.reasoning ? "" : normalizedPayload.analysis || normalizedPayload.report || payload);
    const focusAreas = Array.isArray(normalizedPayload.focus_areas)
      ? normalizedPayload.focus_areas.filter(Boolean).map(String)
      : [];
    return {
      key: name,
      title: String(normalizedPayload.agent_name || name),
      role: String(normalizedPayload.agent_role || ""),
      focusAreas,
      timestamp: formatDateTime(normalizedPayload.timestamp, ""),
      body,
      reasoning: reportSections.reasoning,
      summary: buildSummary(reportSections.body || normalizedPayload.analysis || normalizedPayload.report || payload),
    };
  });

  if (discussionResult) {
    const sections = splitReportSections(discussionResult);
    const body = sections.body || (sections.reasoning ? "" : discussionResult);
    entries.push({
      key: "__discussion__",
      title: "团队讨论",
      role: "",
      focusAreas: [],
      timestamp: "",
      body,
      reasoning: sections.reasoning,
      summary: buildSummary(body),
    });
  }

  return entries;
}

interface AgentReportBrowserProps {
  agentsResults?: Record<string, unknown>;
  discussionResult?: unknown;
}

export function AgentReportBrowser({
  agentsResults = {},
  discussionResult,
}: AgentReportBrowserProps) {
  const entries = useMemo(
    () => toEntries(agentsResults, discussionResult),
    [agentsResults, discussionResult],
  );
  const [activeKey, setActiveKey] = useState(entries[0]?.key || "");

  useEffect(() => {
    if (!entries.length) {
      setActiveKey("");
      return;
    }
    if (!entries.some((item) => item.key === activeKey)) {
      setActiveKey(entries[0].key);
    }
  }, [activeKey, entries]);

  const activeEntry = entries.find((item) => item.key === activeKey) || null;

  if (!entries.length) {
    return null;
  }

  return (
    <section className={styles.block}>
      <div className={styles.reportBrowserGrid}>
        {entries.map((item) => (
          <button
            className={`${styles.reportBrowserCard} ${item.key === activeKey ? styles.reportBrowserCardActive : ""}`}
            key={item.key}
            onClick={() => setActiveKey(item.key)}
            type="button"
          >
            <strong>{item.title}</strong>
            {item.role ? <span className={styles.reportBrowserMeta}>{item.role}</span> : null}
            {item.focusAreas.length ? (
              <span className={styles.reportBrowserMeta}>{item.focusAreas.join(" / ")}</span>
            ) : null}
            {item.timestamp ? <span className={styles.reportBrowserMeta}>{item.timestamp}</span> : null}
            <p className={styles.reportBrowserSummary}>{item.summary}</p>
          </button>
        ))}
      </div>

      {activeEntry ? (
        <div className={styles.reportSection}>
          <div className={styles.reportSectionHeader}>
            <h3>{activeEntry.title}</h3>
            {activeEntry.role || activeEntry.focusAreas.length || activeEntry.timestamp ? (
              <div className={styles.reportMeta}>
                {activeEntry.role ? <span>职责: {activeEntry.role}</span> : null}
                {activeEntry.focusAreas.length ? <span>关注点: {activeEntry.focusAreas.join(" / ")}</span> : null}
                {activeEntry.timestamp ? <span>生成时间: {activeEntry.timestamp}</span> : null}
              </div>
            ) : null}
          </div>
          <FormattedReport content={activeEntry.body} />
          {activeEntry.reasoning ? (
            <details className={styles.details}>
              <summary className={styles.reportReasoningSummary}>推理过程</summary>
              <div className={styles.detailsContent}>
                <FormattedReport content={activeEntry.reasoning} emptyText="暂无推理过程" />
              </div>
            </details>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
