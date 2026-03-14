import { useEffect, useMemo, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch, downloadApiFile } from "../../lib/api";
import styles from "../ConsolePage.module.scss";


interface SectorPredictionItem {
  sector?: string;
  confidence?: number;
  reason?: string;
  risk?: string;
  logic?: string;
  time_window?: string;
  advice?: string;
  score?: number;
  trend?: string;
  sustainability?: string;
}

interface SectorPredictions {
  long_short?: {
    bullish?: SectorPredictionItem[];
    bearish?: SectorPredictionItem[];
  };
  rotation?: {
    current_strong?: SectorPredictionItem[];
    potential?: SectorPredictionItem[];
    declining?: SectorPredictionItem[];
  };
  heat?: {
    hottest?: SectorPredictionItem[];
    heating?: SectorPredictionItem[];
    cooling?: SectorPredictionItem[];
  };
  summary?: {
    market_view?: string;
    key_opportunity?: string;
    major_risk?: string;
    strategy?: string;
  };
  confidence_score?: number;
  risk_level?: string;
  market_outlook?: string;
}

interface SectorResult {
  success?: boolean;
  timestamp?: string;
  final_predictions?: SectorPredictions;
  agents_analysis?: Record<string, { agent_name?: string; agent_role?: string; focus_areas?: string[]; analysis?: string }>;
  comprehensive_report?: string;
  cache_meta?: {
    from_cache?: boolean;
    cache_warning?: string;
    data_timestamp?: string;
  };
}

interface SectorTaskPayload {
  result?: SectorResult;
  data_summary?: {
    from_cache?: boolean;
    cache_warning?: string;
    market_overview?: {
      sh_index?: { close?: number; change_pct?: number };
      up_count?: number;
      up_ratio?: number;
    };
    sectors?: Record<string, unknown>;
    concepts?: Record<string, unknown>;
  };
  message?: string;
}

interface TaskDetail<TPayload> {
  id: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
  error?: string;
  result?: TPayload | null;
}

interface SectorSummaryData {
  headline?: string;
  market_view?: string;
  key_opportunity?: string;
  major_risk?: string;
  strategy?: string;
  bullish?: string[];
  bearish?: string[];
  risk_level?: string;
  market_outlook?: string;
  confidence_score?: number;
}

interface SectorHistoryRecord {
  id: number;
  analysis_date?: string;
  created_at?: string;
  data_date_range?: string;
  summary?: string;
  analysis_content_parsed?: SectorResult;
  summary_data?: SectorSummaryData;
}

interface SchedulerStatus {
  running: boolean;
  enabled?: boolean;
  schedule_time?: string;
  last_run_time?: string | null;
  next_run_time?: string | null;
  email_config?: {
    enabled?: boolean;
    smtp_server?: string;
    email_from?: string;
    email_to?: string;
    password_configured?: boolean;
    configured?: boolean;
  };
}

type SectionKey = "overview" | "history" | "scheduler";

const sectionTabs = [
  { key: "overview", label: "分析总览" },
  { key: "history", label: "历史报告" },
  { key: "scheduler", label: "定时设置" },
];

function asText(value: unknown, fallback = "N/A"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function percentText(value: unknown): string {
  const number = Number(value);
  return Number.isFinite(number) ? `${(number * 100).toFixed(1)}%` : "N/A";
}

function renderPredictionGroup(title: string, items: SectorPredictionItem[] | undefined) {
  if (!(items?.length ?? 0)) {
    return null;
  }
  return (
    <div className={styles.listItem}>
      <strong>{title}</strong>
      <div className={styles.list} style={{ marginTop: 12 }}>
        {items?.map((item, index) => (
          <div className={styles.listItem} key={`${title}-${item.sector ?? "item"}-${index}`}>
            <strong>{asText(item.sector)}</strong>
            <div className={styles.compactGrid} style={{ marginTop: 10 }}>
              <div>
                <div className={styles.muted}>理由 / 逻辑</div>
                <div>{asText(item.reason ?? item.logic)}</div>
              </div>
              <div>
                <div className={styles.muted}>风险 / 趋势</div>
                <div>{asText(item.risk ?? item.trend)}</div>
              </div>
              <div>
                <div className={styles.muted}>时间窗口 / 持续性</div>
                <div>{asText(item.time_window ?? item.sustainability)}</div>
              </div>
              <div>
                <div className={styles.muted}>建议 / 评分</div>
                <div>{asText(item.advice ?? item.score)}</div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function SectorStrategyPage() {
  const [task, setTask] = useState<TaskDetail<SectorTaskPayload> | null>(null);
  const [history, setHistory] = useState<SectorHistoryRecord[]>([]);
  const [selectedReport, setSelectedReport] = useState<SectorHistoryRecord | null>(null);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [scheduleTime, setScheduleTime] = useState("09:00");
  const [section, setSection] = useState<SectionKey>("overview");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail<SectorTaskPayload> | null>("/api/strategies/sector-strategy/tasks/latest");
    setTask(data);
    if (data?.status === "success" && data.result?.result) {
      setSelectedReport(null);
    }
  };

  const loadHistory = async () => {
    const data = await apiFetch<SectorHistoryRecord[]>("/api/strategies/sector-strategy/history");
    setHistory(data);
  };

  const loadScheduler = async () => {
    const data = await apiFetch<SchedulerStatus>("/api/strategies/sector-strategy/scheduler");
    setScheduler(data);
    setScheduleTime(data.schedule_time || "09:00");
  };

  useEffect(() => {
    void Promise.all([loadTask(), loadHistory(), loadScheduler()]);
    const taskTimer = window.setInterval(() => void loadTask(), 2000);
    const schedulerTimer = window.setInterval(() => void loadScheduler(), 10000);
    return () => {
      window.clearInterval(taskTimer);
      window.clearInterval(schedulerTimer);
    };
  }, []);

  const currentResult = selectedReport?.analysis_content_parsed ?? task?.result?.result ?? null;
  const currentSummary = selectedReport?.summary_data ?? null;
  const dataSummary = selectedReport ? null : task?.result?.data_summary ?? null;
  const predictions = currentResult?.final_predictions;

  const headline = useMemo(() => {
    if (currentSummary?.headline) {
      return currentSummary.headline;
    }
    const summary = predictions?.summary;
    const parts = [summary?.market_view, summary?.key_opportunity].filter(Boolean);
    return parts.join("；") || "智策板块分析报告";
  }, [currentSummary, predictions]);

  const submitAnalysis = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/sector-strategy/tasks", {
        method: "POST",
        body: JSON.stringify({}),
      });
      setSelectedReport(null);
      setMessage(`智策分析任务已提交: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交智策分析失败");
    }
  };

  const saveScheduler = async (enabled: boolean) => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<SchedulerStatus>("/api/strategies/sector-strategy/scheduler", {
        method: "PUT",
        body: JSON.stringify({ enabled, schedule_time: scheduleTime }),
      });
      setScheduler(data);
      setMessage(enabled ? `智策定时任务已更新为每天 ${scheduleTime}` : "智策定时任务已停止");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新智策定时任务失败");
    }
  };

  const runOnce = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/strategies/sector-strategy/scheduler/run-once", { method: "POST" });
      setSelectedReport(null);
      setMessage("已提交一次智策后台分析");
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交后台分析失败");
    }
  };

  const testEmail = async () => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ ok: boolean }>("/api/strategies/sector-strategy/scheduler/test-email", { method: "POST" });
      if (data.ok) {
        setMessage("测试邮件发送成功");
      }
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "测试邮件发送失败");
    }
  };

  const openHistory = async (reportId: number) => {
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<SectorHistoryRecord>(`/api/strategies/sector-strategy/history/${reportId}`);
      setSelectedReport(data);
      setSection("overview");
      setMessage(`已加载历史报告 #${reportId}`);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加载智策历史报告失败");
    }
  };

  const deleteHistory = async (reportId: number) => {
    setMessage("");
    setError("");
    try {
      await apiFetch(`/api/strategies/sector-strategy/history/${reportId}`, { method: "DELETE" });
      if (selectedReport?.id === reportId) {
        setSelectedReport(null);
      }
      setMessage(`历史报告 #${reportId} 已删除`);
      await loadHistory();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "删除智策历史报告失败");
    }
  };

  const exportCurrentResult = async (kind: "pdf" | "markdown") => {
    if (!currentResult) {
      return;
    }
    setMessage("");
    setError("");
    try {
      await downloadApiFile(`/api/exports/sector-strategy/${kind}`, {
        method: "POST",
        body: JSON.stringify({ result: currentResult }),
      });
      setMessage(kind === "pdf" ? "智策 PDF 已开始下载" : "智策 Markdown 已开始下载");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "导出智策报告失败");
    }
  };

  return (
    <PageFrame
      title="智策板块"
      summary="当前支持分析任务、历史报告和定时任务控制。"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      actions={
        <>
          <StatusBadge label={scheduler?.running ? `定时 ${scheduler.schedule_time}` : "定时空闲"} tone={scheduler?.running ? "success" : "default"} />
          <StatusBadge
            label={task ? `分析 ${task.status} ${Math.round((task.progress ?? 0) * 100)}%` : "分析空闲"}
            tone={
              task?.status === "success"
                ? "success"
                : task?.status === "failed"
                  ? "danger"
                  : task
                    ? "warning"
                    : "default"
            }
          />
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.primaryButton} onClick={() => void submitAnalysis()} type="button">
              开始智策分析
            </button>
            {selectedReport ? (
              <button className={styles.secondaryButton} onClick={() => setSelectedReport(null)} type="button">
                返回最新结果
              </button>
            ) : null}
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
          </div>
        </section>

        {section === "overview" && task ? (
          <section className={styles.card}>
            <h2>任务状态</h2>
            <p>{task.message || "等待智策任务状态..."}</p>
            <p className={styles.muted}>
              进度: {task.current ?? 0} / {task.total ?? 0}
            </p>
            {task.error ? <p className={styles.dangerText}>{task.error}</p> : null}
          </section>
        ) : null}

        {section === "overview" && currentResult ? (
          <>
            <section className={styles.card}>
              <div className={styles.actions}>
                <h2>报告摘要</h2>
                <button className={styles.secondaryButton} onClick={() => void exportCurrentResult("pdf")} type="button">
                  导出 PDF
                </button>
                <button className={styles.secondaryButton} onClick={() => void exportCurrentResult("markdown")} type="button">
                  导出 Markdown
                </button>
              </div>
              <div className={styles.listItem}>
                <strong>{headline}</strong>
                {(currentSummary?.bullish?.length ?? 0) ? (
                  <div style={{ marginTop: 10 }}>看多板块: {currentSummary?.bullish?.join("、")}</div>
                ) : null}
                {(currentSummary?.bearish?.length ?? 0) ? (
                  <div style={{ marginTop: 6 }}>关注风险板块: {currentSummary?.bearish?.join("、")}</div>
                ) : null}
              </div>
              <div className={styles.compactGrid} style={{ marginTop: 16 }}>
                <div className={styles.metric}>
                  <span className={styles.muted}>置信度</span>
                  <strong>{percentText(currentSummary?.confidence_score ?? predictions?.confidence_score)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>风险等级</span>
                  <strong>{asText(currentSummary?.risk_level ?? predictions?.risk_level)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>市场展望</span>
                  <strong>{asText(currentSummary?.market_outlook ?? predictions?.market_outlook)}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>分析时间</span>
                  <strong>{asText(currentResult.timestamp ?? selectedReport?.analysis_date, "-")}</strong>
                </div>
              </div>
              {dataSummary ? (
                <div className={styles.compactGrid} style={{ marginTop: 16 }}>
                  <div className={styles.metric}>
                    <span className={styles.muted}>上证指数</span>
                    <strong>{asText(dataSummary.market_overview?.sh_index?.close, "N/A")}</strong>
                    <div className={styles.muted}>{asText(dataSummary.market_overview?.sh_index?.change_pct, "N/A")}%</div>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>上涨股票</span>
                    <strong>{asText(dataSummary.market_overview?.up_count, "N/A")}</strong>
                    <div className={styles.muted}>占比 {asText(dataSummary.market_overview?.up_ratio, "N/A")}%</div>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>行业板块</span>
                    <strong>{Object.keys(dataSummary.sectors ?? {}).length}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>概念板块</span>
                    <strong>{Object.keys(dataSummary.concepts ?? {}).length}</strong>
                  </div>
                </div>
              ) : null}
              {currentResult.cache_meta?.cache_warning ? (
                <div className={styles.dangerText} style={{ marginTop: 12 }}>
                  {currentResult.cache_meta.cache_warning}
                </div>
              ) : null}
            </section>

            <section className={styles.card}>
              <h2>核心预测</h2>
              <div className={styles.stack}>
                {renderPredictionGroup("看多板块", predictions?.long_short?.bullish)}
                {renderPredictionGroup("看空板块", predictions?.long_short?.bearish)}
                {renderPredictionGroup("当前强势板块", predictions?.rotation?.current_strong)}
                {renderPredictionGroup("潜力接力板块", predictions?.rotation?.potential)}
                {renderPredictionGroup("衰退板块", predictions?.rotation?.declining)}
                {renderPredictionGroup("最热板块", predictions?.heat?.hottest)}
                {renderPredictionGroup("升温板块", predictions?.heat?.heating)}
                {renderPredictionGroup("降温板块", predictions?.heat?.cooling)}
                {predictions?.summary ? (
                  <div className={styles.listItem}>
                    <strong>策略总结</strong>
                    <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
                      市场观点：{asText(predictions.summary.market_view)}
                      {"\n"}核心机会：{asText(predictions.summary.key_opportunity)}
                      {"\n"}主要风险：{asText(predictions.summary.major_risk)}
                      {"\n"}整体策略：{asText(predictions.summary.strategy)}
                    </div>
                  </div>
                ) : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>AI 智能体分析</h2>
              <div className={styles.list}>
                {Object.entries(currentResult.agents_analysis ?? {}).map(([key, agent]) => (
                  <div className={styles.listItem} key={key}>
                    <strong>{asText(agent.agent_name, key)}</strong>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      职责: {asText(agent.agent_role)} | 关注领域: {(agent.focus_areas ?? []).join("、") || "N/A"}
                    </div>
                    <div style={{ marginTop: 12, whiteSpace: "pre-wrap" }}>{asText(agent.analysis)}</div>
                  </div>
                ))}
                {!Object.keys(currentResult.agents_analysis ?? {}).length ? (
                  <div className={styles.muted}>暂无智能体分析内容。</div>
                ) : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>综合研判</h2>
              <div className={styles.listItem} style={{ whiteSpace: "pre-wrap" }}>
                {asText(currentResult.comprehensive_report, "暂无综合研判")}
              </div>
            </section>
          </>
        ) : null}

        {section === "history" ? (
          <section className={styles.card}>
            <h2>历史报告</h2>
            <div className={styles.list}>
              {history.map((item) => (
                <div className={styles.listItem} key={item.id}>
                  <strong>{asText(item.analysis_date ?? item.created_at, "未知时间")}</strong>
                  <div style={{ marginTop: 8 }}>{asText(item.summary_data?.headline ?? item.summary, "无摘要")}</div>
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
              {!history.length ? <div className={styles.muted}>暂无智策历史报告。</div> : null}
            </div>
          </section>
        ) : null}

        {section === "scheduler" ? (
          <section className={styles.card}>
            <h2>定时分析设置</h2>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="scheduleTime">定时时间</label>
                <input id="scheduleTime" type="time" value={scheduleTime} onChange={(event) => setScheduleTime(event.target.value)} />
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>当前状态</span>
                <strong>{scheduler?.running ? "运行中" : "未运行"}</strong>
                <div className={styles.muted}>下次运行: {asText(scheduler?.next_run_time, "-")}</div>
                <div className={styles.muted}>上次运行: {asText(scheduler?.last_run_time, "-")}</div>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>邮件配置</span>
                <strong>{scheduler?.email_config?.configured ? "完整" : "未完成"}</strong>
                <div className={styles.muted}>启用: {scheduler?.email_config?.enabled ? "是" : "否"}</div>
                <div className={styles.muted}>SMTP: {asText(scheduler?.email_config?.smtp_server, "未配置")}</div>
                <div className={styles.muted}>发件箱: {asText(scheduler?.email_config?.email_from, "未配置")}</div>
              </div>
            </div>
            <div className={styles.actions} style={{ marginTop: 16 }}>
              <button className={styles.primaryButton} onClick={() => void saveScheduler(true)} type="button">
                启动 / 更新
              </button>
              <button className={styles.secondaryButton} onClick={() => void runOnce()} type="button">
                立即运行
              </button>
              <button className={styles.secondaryButton} onClick={() => void testEmail()} type="button">
                测试邮件
              </button>
              <button className={styles.dangerButton} onClick={() => void saveScheduler(false)} type="button">
                停止
              </button>
            </div>
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
