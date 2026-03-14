import { useEffect, useMemo, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { AgentReportBrowser } from "../../components/research/AgentReportBrowser";
import { ApiRequestError, apiFetch, apiFetchCached, downloadApiFile } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { asText } from "../../lib/market";
import styles from "../ConsolePage.module.scss";


type Panel = "analysis" | "history" | "theory";

const sectionTabs = [
  { key: "analysis", label: "周期分析" },
  { key: "history", label: "历史报告" },
  { key: "theory", label: "理论介绍" },
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

export function MacroCyclePage() {
  const [panel, setPanel] = useState<Panel>("analysis");
  const [task, setTask] = useState<TaskDetail<MacroTaskPayload> | null>(null);
  const [history, setHistory] = useState<MacroHistoryRecord[]>([]);
  const [selectedReport, setSelectedReport] = useState<MacroHistoryRecord | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

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

  const currentResult = selectedReport?.result_parsed ?? (task?.status === "success" ? task.result?.result ?? null : null);
  const macroAgentReports = useMemo(() => ({
    chief: { agent_name: "首席宏观策略师", analysis: currentResult?.agents_analysis?.chief?.analysis },
    kondratieff: { agent_name: "康波周期分析师", analysis: currentResult?.agents_analysis?.kondratieff?.analysis },
    merrill: { agent_name: "美林时钟分析师", analysis: currentResult?.agents_analysis?.merrill?.analysis },
    policy: { agent_name: "中国政策分析师", analysis: currentResult?.agents_analysis?.policy?.analysis },
  }), [currentResult]);
  const headline = useMemo(() => {
    const source = selectedReport?.summary || selectedReport?.chief_summary;
    if (source) {
      return source;
    }
    return currentResult?.agents_analysis?.chief?.analysis?.split("\n")[0] || "宏观周期综合研判";
  }, [currentResult, selectedReport]);

  const submitAnalysis = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/macro-cycle/tasks", {
        method: "POST",
        body: JSON.stringify({}),
      });
      setSelectedReport(null);
      setPanel("analysis");
      setMessage(`宏观周期分析任务已提交: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交宏观周期分析失败");
    }
  };

  const openHistory = async (reportId: number) => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetchCached<MacroHistoryRecord>(`/api/strategies/macro-cycle/history/${reportId}`);
      setSelectedReport(data);
      setPanel("analysis");
      setMessage(`已加载历史报告 #${reportId}`);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加载历史报告失败");
    }
  };

  const deleteHistory = async (reportId: number) => {
    setMessage("");
    setError("");
    try {
      await apiFetch(`/api/strategies/macro-cycle/history/${reportId}`, { method: "DELETE" });
      if (selectedReport?.id === reportId) {
        setSelectedReport(null);
      }
      setMessage(`历史报告 #${reportId} 已删除`);
      await loadHistory();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "删除历史报告失败");
    }
  };

  const exportCurrentResult = async (kind: "pdf" | "markdown") => {
    if (!currentResult) {
      return;
    }
    setMessage("");
    setError("");
    try {
      await downloadApiFile(`/api/exports/macro-cycle/${kind}`, {
        method: "POST",
        body: JSON.stringify({ result: currentResult }),
      });
      setMessage(kind === "pdf" ? "宏观周期 PDF 已开始下载" : "宏观周期 Markdown 已开始下载");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "导出宏观周期报告失败");
    }
  };

  return (
    <PageFrame
      title="宏观周期"
      summary="保留宏观周期分析、历史报告和理论介绍三大模块。"
      sectionTabs={sectionTabs}
      activeSectionKey={panel}
      onSectionChange={(nextSection) => setPanel(nextSection as Panel)}
      actions={
        <>
          <StatusBadge label={selectedReport ? `历史 #${selectedReport.id}` : "最新分析"} tone="default" />
          <StatusBadge
            label={task ? `分析 ${task.status} ${Math.round((task.progress ?? 0) * 100)}%` : "分析空闲"}
            tone={task?.status === "success" ? "success" : task?.status === "failed" ? "danger" : task ? "warning" : "default"}
          />
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.secondaryButton} onClick={() => void submitAnalysis()} type="button">
              开始宏观周期分析
            </button>
            {currentResult ? (
              <>
                <button className={styles.secondaryButton} onClick={() => void exportCurrentResult("pdf")} type="button">
                  导出 PDF
                </button>
                <button className={styles.secondaryButton} onClick={() => void exportCurrentResult("markdown")} type="button">
                  导出 Markdown
                </button>
              </>
            ) : null}
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
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

        {panel === "analysis" ? (
          <>
            {currentResult ? (
              <>
                <section className={styles.card}>
                  <h2>综合摘要</h2>
                  <div className={styles.listItem}>
                    <strong>{headline}</strong>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      分析时间: {formatDateTime(currentResult.timestamp ?? selectedReport?.analysis_date, "-")}
                    </div>
                  </div>
                  {(currentResult.data_errors?.length ?? 0) ? (
                    <div className={styles.list} style={{ marginTop: 16 }}>
                      {currentResult.data_errors?.map((item) => (
                        <div className={styles.listItem} key={item}>
                          {item}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </section>

                <section className={styles.card}>
                  <h2>AI 分析师报告</h2>
                  <AgentReportBrowser agentsResults={macroAgentReports} />
                </section>
              </>
            ) : (
              <section className={styles.card}>
                <div className={styles.muted}>暂无宏观周期分析结果，请先提交分析任务或从历史报告中加载。</div>
              </section>
            )}
          </>
        ) : null}

        {panel === "history" ? (
          <section className={styles.card}>
            <h2>历史报告</h2>
            <div className={styles.list}>
              {history.map((item) => (
                <div className={styles.listItem} key={item.id}>
                  <strong>{formatDateTime(item.analysis_date ?? item.created_at, "未知时间")}</strong>
                  <div style={{ marginTop: 8 }}>{asText(item.summary ?? item.chief_summary, "宏观周期分析报告")}</div>
                  <div className={styles.actions} style={{ marginTop: 12 }}>
                    <button className={styles.secondaryButton} onClick={() => void openHistory(item.id)} type="button">
                      查看
                    </button>
                    <button className={styles.dangerButton} onClick={() => void deleteHistory(item.id)} type="button">
                      删除
                    </button>
                  </div>
                </div>
              ))}
              {!history.length ? <div className={styles.muted}>暂无宏观周期历史报告。</div> : null}
            </div>
          </section>
        ) : null}

        {panel === "theory" ? (
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
        ) : null}
      </div>
    </PageFrame>
  );
}
