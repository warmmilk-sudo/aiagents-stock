import { useEffect, useMemo, useState } from "react";

import { formatDateTime } from "../../lib/datetime";
import { MarkdownReport } from "./MarkdownReport";
import { splitReportSections } from "./FormattedReport";
import styles from "./ResearchPanels.module.scss";

interface ReportEntry {
  key: string;
  title: string;
  role: string;
  focusAreas: string[];
  timestamp: string;
  rawContent: string;
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

function sanitizeDiscussionSpeakers(value: unknown): string {
  return String(value || "").replace(
    /【(投资总监（主持）|技术分析师|基本面分析师|资金面分析师|风险管理师|市场情绪分析师|新闻分析师)(?:\s+[^\]】:：]{1,12})?】(?=[:：])/g,
    "【$1】",
  );
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
      rawContent: String(normalizedPayload.analysis || normalizedPayload.report || payload || "").trim(),
      body,
      reasoning: reportSections.reasoning,
      summary: buildSummary(reportSections.body || normalizedPayload.analysis || normalizedPayload.report || payload),
    };
  });

  if (discussionResult) {
    const normalizedDiscussion = sanitizeDiscussionSpeakers(discussionResult);
    const sections = splitReportSections(normalizedDiscussion);
    const body = normalizedDiscussion || (sections.body || (sections.reasoning ? "" : normalizedDiscussion));
    entries.push({
      key: "__discussion__",
      title: "团队讨论",
      role: "",
      focusAreas: [],
      timestamp: "",
      rawContent: String(normalizedDiscussion || "").trim(),
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
          <MarkdownReport
            className={styles.rawReportText}
            content={activeEntry.rawContent || String(activeEntry.body || "暂无正文")}
          />
        </div>
      ) : null}
    </section>
  );
}
