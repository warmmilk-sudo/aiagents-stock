import { useMemo } from "react";

import styles from "../../pages/ConsolePage.module.scss";
import { formatDateTime } from "../../lib/datetime";
import { splitReportSections } from "./FormattedReport";
import { ReportWorkspace, type ReportWorkspaceEntry } from "./ReportWorkspace";

type ExportKind = "pdf" | "markdown";
type MacroSectionKey = "chief" | "kondratieff" | "merrill" | "policy";

interface MacroAgentDetail {
  analysis?: string;
}

interface MacroCycleResultLike {
  timestamp?: string;
  data_errors?: string[];
  agents_analysis?: {
    chief?: MacroAgentDetail;
    kondratieff?: MacroAgentDetail;
    merrill?: MacroAgentDetail;
    policy?: MacroAgentDetail;
  };
}

interface MacroCycleReportDetailViewProps {
  result?: MacroCycleResultLike | null;
  headline?: string;
  onExport: (kind: ExportKind) => void;
}

function metricText(value: unknown, fallback = "暂无"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

export function MacroCycleReportDetailView({
  result,
  headline,
  onExport,
}: MacroCycleReportDetailViewProps) {
  const reportEntries = useMemo<ReportWorkspaceEntry[]>(() => {
    const agents = result?.agents_analysis;
    const entries: Array<[MacroSectionKey, string, string, string | undefined]> = [
      ["chief", "综合结论", "首席宏观策略师", agents?.chief?.analysis],
      ["kondratieff", "康波周期", "康波周期分析师", agents?.kondratieff?.analysis],
      ["merrill", "美林时钟", "美林时钟分析师", agents?.merrill?.analysis],
      ["policy", "政策维度", "中国政策分析师", agents?.policy?.analysis],
    ];

    return entries.map(([key, label, title, analysis]) => {
      const sections = splitReportSections(analysis);
      return {
        key,
        label,
        title,
        timestamp: formatDateTime(result?.timestamp, ""),
        body: sections.body || analysis || "",
        reasoning: sections.reasoning,
        summary: key === "chief" ? headline : undefined,
      };
    });
  }, [headline, result?.agents_analysis, result?.timestamp]);

  const agentCount = reportEntries.filter((item) => item.body || item.reasoning).length;
  const dataErrorCount = result?.data_errors?.length ?? 0;
  const hasHeadline = Boolean(String(headline || "").trim());

  return (
    <>
      <section className={styles.card}>
        <div className={styles.reportHeaderStack}>
          <div className={styles.reportExportGrid}>
            <button className={styles.secondaryButton} onClick={() => onExport("markdown")} type="button">
              导出 Markdown
            </button>
            <button className={styles.secondaryButton} onClick={() => onExport("pdf")} type="button">
              导出 PDF
            </button>
          </div>
        </div>
      </section>

      <section className={styles.card}>
        <p className={styles.helperText}>{formatDateTime(result?.timestamp, "暂无时间")}</p>
        <div className={styles.strategySummaryGrid}>
          {hasHeadline ? (
            <div className={styles.historySummaryCell}>
              <span>核心标题</span>
              <strong>{metricText(headline)}</strong>
            </div>
          ) : null}
          <div className={styles.historySummaryCell}>
            <span>分析师数量</span>
            <strong>{agentCount}</strong>
          </div>
          <div className={styles.historySummaryCell}>
            <span>数据异常</span>
            <strong>{dataErrorCount}</strong>
          </div>
        </div>

        {result?.data_errors?.length ? (
          <div className={`${styles.noticeCard} ${styles.noticeWarning}`}>
            <strong>数据异常提示</strong>
            <div>{result.data_errors.join("；")}</div>
          </div>
        ) : null}
      </section>

      <section className={styles.card}>
        <div className={styles.sectionControlStack}>
          <ReportWorkspace
            ariaLabel="宏观周期分析师报告"
            emptyText="暂无宏观周期报告"
            entries={reportEntries}
          />
        </div>
      </section>
    </>
  );
}
