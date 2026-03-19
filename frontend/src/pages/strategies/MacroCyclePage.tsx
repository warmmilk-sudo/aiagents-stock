import { useEffect, useMemo, useState } from "react";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { MacroCycleReportDetailView } from "../../components/research/MacroCycleReportDetailView";
import { splitReportSections } from "../../components/research/FormattedReport";
import { ApiRequestError, apiFetch, apiFetchCached, downloadApiFile } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { asText } from "../../lib/market";
import styles from "../ConsolePage.module.scss";


type Panel = "analysis" | "history";

const sectionTabs = [
  { key: "analysis", label: "周期分析" },
  { key: "history", label: "历史报告" },
];

interface TaskDetail<T> {
  id: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
  error?: string;
  result?: T | null;
}

interface MacroTaskPayload {
  result?: MacroResult;
  message?: string;
}

interface MacroAgent {
  analysis?: string;
}

interface MacroResult {
  success?: boolean;
  timestamp?: string;
  report_id?: number;
  error?: string;
  data_errors?: string[];
  agents_analysis?: {
    chief?: MacroAgent;
    kondratieff?: MacroAgent;
    merrill?: MacroAgent;
    policy?: MacroAgent;
  };
}

interface MacroHistoryRecord {
  id: number;
  analysis_date?: string;
  summary?: string;
  chief_summary?: string;
  created_at?: string;
  result_parsed?: MacroResult;
}

function normalizeHeadlineCandidate(value: unknown): string {
  const text = String(value || "").replace(/\r\n/g, "\n").trim();
  if (!text) {
    return "";
  }
  const firstLine = text
    .split("\n")
    .map((line) => line.replace(/^[#>*\-\s]+/, "").trim())
    .find(Boolean);
  return firstLine || "";
}

function looksLikeReasoning(value: unknown): boolean {
  const text = String(value || "").trim();
  if (!text) {
    return false;
  }
  return (
    text.startsWith("【推理过程】") ||
    text.includes("用户现在需要") ||
    text.includes("首先得") ||
    text.includes("我需要") ||
    text.includes("理解目标")
  );
}

function historyPreview(record: MacroHistoryRecord): string {
  const parsedBody = splitReportSections(record.result_parsed?.agents_analysis?.chief?.analysis).body;
  const parsedTitle = normalizeHeadlineCandidate(parsedBody);
  if (parsedTitle) {
    return parsedTitle;
  }

  const storedCandidates = [record.summary, record.chief_summary];
  for (const item of storedCandidates) {
    if (!item || looksLikeReasoning(item)) {
      continue;
    }
    const normalized = normalizeHeadlineCandidate(item);
    if (normalized) {
      return normalized;
    }
  }

  return "宏观周期分析报告";
}

export function MacroCyclePage() {
  const [panel, setPanel] = useState<Panel>("analysis");
  const [task, setTask] = useState<TaskDetail<MacroTaskPayload> | null>(null);
  const [history, setHistory] = useState<MacroHistoryRecord[]>([]);
  const [selectedReport, setSelectedReport] = useState<MacroHistoryRecord | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [isSubmittingAnalysis, setIsSubmittingAnalysis] = useState(false);
  const [deletingHistoryId, setDeletingHistoryId] = useState<number | null>(null);

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail<MacroTaskPayload> | null>("/api/strategies/macro-cycle/tasks/latest");
    setTask(data);
  };

  const loadHistory = async () => {
    const data = await apiFetchCached<MacroHistoryRecord[]>("/api/strategies/macro-cycle/history");
    setHistory(data);
  };

  useEffect(() => {
    void Promise.all([loadTask(), loadHistory()]);
    const timer = window.setInterval(() => void loadTask(), 2000);
    return () => window.clearInterval(timer);
  }, []);

  const analysisResult = task?.status === "success" ? task.result?.result ?? null : null;
  const historyResult = selectedReport?.result_parsed ?? null;
  const macroAgentReports = useMemo(() => ({
    chief: { agent_name: "首席宏观策略师", analysis: analysisResult?.agents_analysis?.chief?.analysis },
    kondratieff: { agent_name: "康波周期分析师", analysis: analysisResult?.agents_analysis?.kondratieff?.analysis },
    merrill: { agent_name: "美林时钟分析师", analysis: analysisResult?.agents_analysis?.merrill?.analysis },
    policy: { agent_name: "中国政策分析师", analysis: analysisResult?.agents_analysis?.policy?.analysis },
  }), [analysisResult]);
  const historyAgentReports = useMemo(() => ({
    chief: { agent_name: "首席宏观策略师", analysis: historyResult?.agents_analysis?.chief?.analysis },
    kondratieff: { agent_name: "康波周期分析师", analysis: historyResult?.agents_analysis?.kondratieff?.analysis },
    merrill: { agent_name: "美林时钟分析师", analysis: historyResult?.agents_analysis?.merrill?.analysis },
    policy: { agent_name: "中国政策分析师", analysis: historyResult?.agents_analysis?.policy?.analysis },
  }), [historyResult]);
  const chiefSections = useMemo(
    () => splitReportSections(analysisResult?.agents_analysis?.chief?.analysis),
    [analysisResult?.agents_analysis?.chief?.analysis],
  );
  const historyChiefSections = useMemo(
    () => splitReportSections(historyResult?.agents_analysis?.chief?.analysis),
    [historyResult?.agents_analysis?.chief?.analysis],
  );
  const headline = useMemo(() => {
    const parsedBodyTitle = normalizeHeadlineCandidate(chiefSections.body);
    if (parsedBodyTitle) {
      return parsedBodyTitle;
    }

    const storedCandidates = [selectedReport?.summary, selectedReport?.chief_summary];
    for (const item of storedCandidates) {
      if (!item || looksLikeReasoning(item)) {
        continue;
      }
      const normalized = normalizeHeadlineCandidate(item);
      if (normalized) {
        return normalized;
      }
    }

    return "";
  }, [chiefSections.body, selectedReport?.chief_summary, selectedReport?.summary]);
  const historyHeadline = useMemo(() => {
    const parsedBodyTitle = normalizeHeadlineCandidate(historyChiefSections.body);
    if (parsedBodyTitle) {
      return parsedBodyTitle;
    }

    const storedCandidates = [selectedReport?.summary, selectedReport?.chief_summary];
    for (const item of storedCandidates) {
      if (!item || looksLikeReasoning(item)) {
        continue;
      }
      const normalized = normalizeHeadlineCandidate(item);
      if (normalized) {
        return normalized;
      }
    }

    return "";
  }, [historyChiefSections.body, selectedReport?.chief_summary, selectedReport?.summary]);

  const submitAnalysis = async () => {
    setMessage("");
    setError("");
    setIsSubmittingAnalysis(true);
    try {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/macro-cycle/tasks", {
        method: "POST",
        body: JSON.stringify({}),
      });
      setSelectedReport(null);
      setPanel("analysis");
      setMessage(`宏观周期分析任务已提交: ${data.task_id}`);
      await loadTask().catch(() => undefined);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交宏观周期分析失败");
    } finally {
      setIsSubmittingAnalysis(false);
    }
  };

  const openHistory = async (reportId: number) => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetchCached<MacroHistoryRecord>(`/api/strategies/macro-cycle/history/${reportId}`);
      setSelectedReport(data);
      setPanel("history");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加载历史报告失败");
    }
  };

  const deleteHistory = async (reportId: number) => {
    setMessage("");
    setError("");
    if (deletingHistoryId === reportId) {
      return;
    }
    const removedIndex = history.findIndex((item) => item.id === reportId);
    const removedRecord = history[removedIndex] ?? null;
    setDeletingHistoryId(reportId);
    setHistory((current) => current.filter((item) => item.id !== reportId));
    try {
      await apiFetch(`/api/strategies/macro-cycle/history/${reportId}`, { method: "DELETE" });
      if (selectedReport?.id === reportId) {
        setSelectedReport(null);
      }
      setMessage(`历史报告 #${reportId} 已删除`);
      await loadHistory().catch(() => undefined);
    } catch (requestError) {
      if (removedRecord) {
        setHistory((current) => {
          if (current.some((item) => item.id === removedRecord.id)) {
            return current;
          }
          const next = [...current];
          next.splice(Math.max(0, Math.min(removedIndex, next.length)), 0, removedRecord);
          return next;
        });
      }
      setError(requestError instanceof ApiRequestError ? requestError.message : "删除历史报告失败");
    } finally {
      setDeletingHistoryId((current) => current === reportId ? null : current);
    }
  };

  const exportResult = async (result: MacroResult | null, kind: "pdf" | "markdown") => {
    if (!result) {
      return;
    }
    setMessage("");
    setError("");
    try {
      await downloadApiFile(`/api/exports/macro-cycle/${kind}`, {
        method: "POST",
        body: JSON.stringify({ result }),
      });
      setMessage(kind === "pdf" ? "宏观周期 PDF 已开始下载" : "宏观周期 Markdown 已开始下载");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "导出宏观周期报告失败");
    }
  };

  return (
    <PageFrame
      title="宏观周期"
      summary="当前覆盖周期分析与历史报告，理论说明并入周期分析页。"
      sectionTabs={sectionTabs}
      activeSectionKey={panel}
      onSectionChange={(nextSection) => setPanel(nextSection as Panel)}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        {panel === "analysis" ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <button className={styles.secondaryButton} disabled={isSubmittingAnalysis} onClick={() => void submitAnalysis()} type="button">
                  {isSubmittingAnalysis ? "提交中..." : "开始宏观周期分析"}
                </button>
              </div>
            </section>

            {task ? (
              <section className={styles.card}>
                <h2>任务状态</h2>
                <p>{task.message || "等待宏观周期任务状态..."}</p>
                <p className={styles.muted}>
                  进度: {task.current ?? 0} / {task.total ?? 0}
                </p>
                {task.error ? <p className={styles.dangerText}>{task.error}</p> : null}
              </section>
            ) : null}

            <section className={styles.card}>
              <h2>理论介绍</h2>
              <div className={styles.list}>
                <div className={styles.listItem}>
                  <strong>康波周期</strong>
                  <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
                    康德拉季耶夫长波是 50-60 年级别的超长经济周期，由技术革命驱动，通常分为回升、繁荣、衰退、萧条四个阶段。
                    当前模块用它做长期战略定位，回答“我们正站在大周期的什么位置”。
                  </div>
                </div>
                <div className={styles.listItem}>
                  <strong>美林投资时钟</strong>
                  <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
                    美林时钟关注经济增长和通胀的中短周期变化，常见象限是复苏、过热、滞胀、衰退。
                    当前模块把它作为战术层分析，用来辅助判断资产轮动和行业偏好。
                  </div>
                </div>
                <div className={styles.listItem}>
                  <strong>政策第三维度</strong>
                  <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
                    在中国市场里，货币政策、财政政策、产业政策和房地产政策会直接改变周期节奏。
                    所以这里额外引入政策分析师，把政策环境和两大周期框架合并做综合研判。
                  </div>
                </div>
                <div className={styles.listItem}>
                  <strong>使用方式</strong>
                  <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
                    康波用于判断长期方向，美林用于识别当前阶段，政策维度用于修正和加速判断。
                    三者一致时可以提高信心，三者冲突时应优先控制风险。
                  </div>
                </div>
              </div>
            </section>

            {analysisResult ? (
              <MacroCycleReportDetailView
                headline={headline}
                onExport={(kind) => void exportResult(analysisResult, kind)}
                result={{
                  ...analysisResult,
                  timestamp: analysisResult.timestamp,
                  agents_analysis: {
                    chief: { analysis: macroAgentReports.chief.analysis },
                    kondratieff: { analysis: macroAgentReports.kondratieff.analysis },
                    merrill: { analysis: macroAgentReports.merrill.analysis },
                    policy: { analysis: macroAgentReports.policy.analysis },
                  },
                }}
              />
            ) : null}
          </>
        ) : null}

        {panel === "history" ? (
          <>
            {selectedReport ? (
              <>
                <section className={styles.card}>
                  <div className={styles.actions}>
                    <button className={styles.secondaryButton} onClick={() => setSelectedReport(null)} type="button">
                      返回历史列表
                    </button>
                  </div>
                </section>

                {historyResult ? (
                  <MacroCycleReportDetailView
                    headline={historyHeadline}
                    onExport={(kind) => void exportResult(historyResult, kind)}
                    result={{
                      ...historyResult,
                      timestamp: historyResult.timestamp ?? selectedReport.analysis_date,
                      agents_analysis: {
                        chief: { analysis: historyAgentReports.chief.analysis },
                        kondratieff: { analysis: historyAgentReports.kondratieff.analysis },
                        merrill: { analysis: historyAgentReports.merrill.analysis },
                        policy: { analysis: historyAgentReports.policy.analysis },
                      },
                    }}
                  />
                ) : (
                  <section className={styles.card}>
                    <div className={styles.muted}>该历史报告缺少完整结果数据。</div>
                  </section>
                )}
              </>
            ) : null}

            <section className={styles.card}>
              <h2 className={styles.mobileDuplicateHeading}>历史报告</h2>
              <div className={styles.list}>
                {history.map((item) => (
                  <div className={styles.listItem} key={item.id}>
                    <strong>{formatDateTime(item.analysis_date ?? item.created_at, "未知时间")}</strong>
                    <div style={{ marginTop: 8 }}>{historyPreview(item)}</div>
                    <div className={styles.actions} style={{ marginTop: 12 }}>
                      <button className={styles.secondaryButton} onClick={() => void openHistory(item.id)} type="button">
                        查看
                      </button>
                      <button
                        className={styles.dangerButton}
                        disabled={deletingHistoryId === item.id}
                        onClick={() => void deleteHistory(item.id)}
                        type="button"
                      >
                        {deletingHistoryId === item.id ? "删除中..." : "删除"}
                      </button>
                    </div>
                  </div>
                ))}
                {!history.length ? <div className={styles.muted}>暂无宏观周期历史报告。</div> : null}
              </div>
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
