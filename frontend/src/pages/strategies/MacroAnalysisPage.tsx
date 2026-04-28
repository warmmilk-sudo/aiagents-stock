import { useMemo, useState } from "react";

import { MarkdownReport } from "../../components/research/MarkdownReport";
import { splitReportSections } from "../../components/research/FormattedReport";
import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import { useSelectedModels } from "../../hooks/useSelectedModels";
import { usePollingLoader } from "../../hooks/usePollingLoader";
import { ApiRequestError, apiFetch } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { asText, numberText } from "../../lib/market";
import styles from "../ConsolePage.module.scss";

type Panel = "overview" | "data" | "sector" | "stocks";

interface TaskDetail<T> {
  id: string;
  status: string;
  message: string;
  current?: number;
  total?: number;
  error?: string;
  result?: T | null;
}

interface MacroSnapshotItem {
  label?: string;
  value?: number | string | null;
  unit?: string;
  period_label?: string;
  previous_value?: number | string | null;
  previous_period_label?: string;
  change?: number | null;
}

interface MacroIndexItem {
  close?: number | null;
  date?: string;
  daily_change_pct?: number | null;
  pct_20d?: number | null;
  pct_60d?: number | null;
}

interface MacroNewsItem {
  title?: string;
  summary?: string;
  publish_time?: string;
  url?: string;
}

interface MacroSectorItem {
  sector?: string;
  logic?: string;
  confidence?: number | null;
  score?: number | null;
}

interface CandidateStock {
  code?: string;
  name?: string;
  sector?: string;
  industry?: string;
  price?: number | null;
  daily_change_pct?: number | null;
  pe_ratio?: number | null;
  pb_ratio?: number | null;
  market_cap?: number | null;
  recent_20d_return?: number | null;
  recent_60d_return?: number | null;
  reason?: string;
  risk?: string;
  style?: string;
  confidence?: number | null;
}

interface MacroAgent {
  agent_name?: string;
  agent_role?: string;
  analysis?: string;
  focus_areas?: string[];
}

interface MacroResult {
  success?: boolean;
  timestamp?: string;
  error?: string;
  data_errors?: string[];
  raw_data?: {
    macro_snapshot?: Record<string, MacroSnapshotItem>;
    macro_tables?: Record<string, Array<Record<string, unknown>>>;
    market_indices?: Record<string, MacroIndexItem>;
    news?: MacroNewsItem[];
  };
  sector_view?: {
    market_view?: string;
    bullish_sectors?: MacroSectorItem[];
    bearish_sectors?: MacroSectorItem[];
    watch_signals?: string[];
  };
  stock_view?: {
    recommended_stocks?: CandidateStock[];
    watchlist?: CandidateStock[];
  };
  candidate_stocks?: CandidateStock[];
  agents_analysis?: {
    chief?: MacroAgent;
    macro?: MacroAgent;
    policy?: MacroAgent;
    sector?: MacroAgent;
    stock?: MacroAgent;
  };
}

interface MacroTaskPayload {
  result?: MacroResult;
  message?: string;
}

const sectionTabs = [
  { key: "overview", label: "分析总览" },
  { key: "data", label: "宏观数据" },
  { key: "sector", label: "行业映射" },
  { key: "stocks", label: "优质标的" },
];

const keyMetrics = [
  "gdp_yoy",
  "manufacturing_pmi",
  "cpi_yoy",
  "m2_yoy",
  "retail_sales_yoy",
  "urban_unemployment",
];

function normalizeHeadlineCandidate(value: unknown): string {
  const text = String(value || "").replace(/\r\n/g, "\n").trim();
  if (!text) {
    return "";
  }
  return (
    text
      .split("\n")
      .map((line) => line.replace(/^[#>*\-\s]+/, "").trim())
      .find(Boolean) || ""
  );
}

function formatMetricValue(item: MacroSnapshotItem | undefined) {
  if (!item) {
    return "N/A";
  }
  return `${asText(item.value, "-")}${item.unit || ""}`;
}

function formatMetricDelta(item: MacroSnapshotItem | undefined) {
  if (!item || item.change === null || item.change === undefined) {
    return "较上期无数据";
  }
  return `${item.change >= 0 ? "+" : ""}${numberText(item.change)}${item.unit || ""}`;
}

function formatPercent(value: unknown) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}%` : "-";
}

function metricText(value: unknown, fallback = "暂无") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function taskProgressTone(task: TaskDetail<MacroTaskPayload> | null): "running" | "success" | "danger" {
  if (!task) {
    return "running";
  }
  if (task.status === "success") {
    return "success";
  }
  if (task.status === "failed" || task.status === "cancelled") {
    return "danger";
  }
  return "running";
}

export function MacroAnalysisPage() {
  const { lightweightModel, reasoningModel } = useSelectedModels();
  const [panel, setPanel] = useState<Panel>("overview");
  const [task, setTask] = useState<TaskDetail<MacroTaskPayload> | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [isSubmittingAnalysis, setIsSubmittingAnalysis] = useState(false);
  const taskStatusVisible = task?.status === "queued" || task?.status === "running" || task?.status === "failed" || task?.status === "cancelled";

  const loadTask = async () => {
    const data = await apiFetch<TaskDetail<MacroTaskPayload> | null>("/api/strategies/macro-analysis/tasks/latest");
    setTask(data);
  };

  usePollingLoader({ load: loadTask, intervalMs: 2000 });

  const currentResult = task?.status === "success" ? task.result?.result ?? null : null;
  const currentChiefSections = useMemo(
    () => splitReportSections(currentResult?.agents_analysis?.chief?.analysis),
    [currentResult?.agents_analysis?.chief?.analysis],
  );
  const currentHeadline = useMemo(() => normalizeHeadlineCandidate(currentChiefSections.body), [currentChiefSections.body]);
  const macroSnapshot = currentResult?.raw_data?.macro_snapshot ?? {};
  const marketIndices = currentResult?.raw_data?.market_indices ?? {};
  const news = currentResult?.raw_data?.news ?? [];
  const bullish = currentResult?.sector_view?.bullish_sectors ?? [];
  const bearish = currentResult?.sector_view?.bearish_sectors ?? [];
  const recommendedStocks = currentResult?.stock_view?.recommended_stocks ?? [];
  const watchlist = currentResult?.stock_view?.watchlist ?? [];
  const submitAnalysis = async () => {
    setMessage("");
    setError("");
    setIsSubmittingAnalysis(true);
    try {
      const data = await apiFetch<{ task_id: string }>("/api/strategies/macro-analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          lightweight_model: lightweightModel || undefined,
          reasoning_model: reasoningModel || undefined,
        }),
      });
      setPanel("overview");
      setMessage(`宏观分析任务已提交: ${data.task_id}`);
      void loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交宏观分析失败");
    } finally {
      setIsSubmittingAnalysis(false);
    }
  };

  return (
    <PageFrame
      title="宏观分析"
      sectionTabs={sectionTabs}
      activeSectionKey={panel}
      onSectionChange={(nextSection) => setPanel(nextSection as Panel)}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />

        <section className={styles.card}>
          <div className={styles.actions}>
            <button
              className={styles.secondaryButton}
              disabled={isSubmittingAnalysis}
              onClick={() => void submitAnalysis()}
              type="button"
            >
              {isSubmittingAnalysis ? "提交中..." : "开始宏观分析"}
            </button>
          </div>
        </section>

        {taskStatusVisible ? (
          <section className={styles.card}>
            <h2>任务状态</h2>
            <p className={styles.helperText}>{task?.message || "等待宏观分析任务状态..."}</p>
            <TaskProgressBar
              current={task?.current}
              total={task?.total}
              message={task?.message || "等待宏观分析任务状态..."}
              tone={taskProgressTone(task)}
            />
            {task?.error ? <p className={styles.dangerText}>{task.error}</p> : null}
          </section>
        ) : null}

        {!currentResult ? (
          <section className={styles.card}>
            <div className={styles.muted}>暂无宏观分析结果，重新分析会覆盖当前展示结果。</div>
          </section>
        ) : null}

        {currentResult && panel === "overview" ? (
          <>
            <section className={styles.card}>
              <p className={styles.helperText}>{formatDateTime(currentResult.timestamp, "暂无时间")}</p>
              <div className={styles.strategySummaryGrid}>
                {currentHeadline ? (
                  <div className={styles.historySummaryCell}>
                    <span>核心标题</span>
                    <strong>{currentHeadline}</strong>
                  </div>
                ) : null}
                <div className={styles.historySummaryCell}>
                  <span>利好行业</span>
                  <strong>{bullish.length}</strong>
                </div>
                <div className={styles.historySummaryCell}>
                  <span>优先关注</span>
                  <strong>{recommendedStocks.length}</strong>
                </div>
                <div className={styles.historySummaryCell}>
                  <span>数据异常</span>
                  <strong>{currentResult.data_errors?.length || 0}</strong>
                </div>
              </div>
              {currentResult.data_errors?.length ? (
                <div className={`${styles.noticeCard} ${styles.noticeWarning}`}>
                  <strong>数据异常提示</strong>
                  <div>{currentResult.data_errors.join("；")}</div>
                </div>
              ) : null}
            </section>

            <section className={styles.card}>
              <h2>核心宏观指标</h2>
              <div className={styles.strategySummaryGrid}>
                {keyMetrics.map((key) => (
                  <div className={styles.historySummaryCell} key={key}>
                    <span>{metricText(macroSnapshot[key]?.label, key)}</span>
                    <strong>{formatMetricValue(macroSnapshot[key])}</strong>
                    <span>{formatMetricDelta(macroSnapshot[key])}</span>
                  </div>
                ))}
              </div>
            </section>

            <section className={styles.card}>
              <h2>首席策略官结论</h2>
              <MarkdownReport
                className={styles.rawReportText}
                content={currentChiefSections.body || currentResult.agents_analysis?.chief?.analysis || "暂无综合结论"}
                emptyText="暂无综合结论"
              />
            </section>
          </>
        ) : null}

        {currentResult && panel === "data" ? (
          <>
            <section className={styles.card}>
              <h2>宏观指标快照</h2>
              <div className={styles.list}>
                {Object.entries(macroSnapshot).map(([key, item]) => (
                  <div className={styles.listItem} key={key}>
                    <strong>{metricText(item.label, key)}</strong>
                    <div style={{ marginTop: 8 }}>
                      最新值：{formatMetricValue(item)} | 期间：{metricText(item.period_label, "-")}
                    </div>
                    <div style={{ marginTop: 6 }}>
                      前值：{item.previous_value ?? "-"}{item.unit || ""} | 变动：{formatMetricDelta(item)}
                    </div>
                  </div>
                ))}
                {!Object.keys(macroSnapshot).length ? <div className={styles.muted}>暂无宏观指标。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>A股指数快照</h2>
              <div className={styles.list}>
                {Object.entries(marketIndices).map(([name, item]) => (
                  <div className={styles.listItem} key={name}>
                    <strong>{name}</strong>
                    <div style={{ marginTop: 8 }}>
                      收盘：{metricText(item.close, "-")} | 日期：{metricText(item.date, "-")}
                    </div>
                    <div style={{ marginTop: 6 }}>
                      日涨跌：{formatPercent(item.daily_change_pct)} | 20日：{formatPercent(item.pct_20d)} | 60日：{formatPercent(item.pct_60d)}
                    </div>
                  </div>
                ))}
                {!Object.keys(marketIndices).length ? <div className={styles.muted}>暂无指数快照。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>宏观新闻样本</h2>
              <div className={styles.list}>
                {news.map((item, index) => (
                  <div className={styles.listItem} key={`${item.title || "news"}-${index}`}>
                    {item.url ? (
                      <strong>
                        <a href={item.url} rel="noreferrer" target="_blank">
                          {metricText(item.title, "未命名新闻")}
                        </a>
                      </strong>
                    ) : (
                      <strong>{metricText(item.title, "未命名新闻")}</strong>
                    )}
                    <div style={{ marginTop: 8 }}>{metricText(item.summary, "暂无摘要")}</div>
                    <div className={styles.helperText}>{formatDateTime(item.publish_time, "暂无时间")}</div>
                  </div>
                ))}
                {!news.length ? <div className={styles.muted}>暂无宏观新闻样本。</div> : null}
              </div>
            </section>
          </>
        ) : null}

        {currentResult && panel === "sector" ? (
          <>
            <section className={styles.card}>
              <h2>市场判断</h2>
              <div className={styles.strategySummaryGrid}>
                <div className={styles.historySummaryCell}>
                  <span>当前判断</span>
                  <strong>{metricText(currentResult.sector_view?.market_view)}</strong>
                </div>
                <div className={styles.historySummaryCell}>
                  <span>利好行业数</span>
                  <strong>{bullish.length}</strong>
                </div>
                <div className={styles.historySummaryCell}>
                  <span>利空行业数</span>
                  <strong>{bearish.length}</strong>
                </div>
              </div>
            </section>

            <section className={styles.card}>
              <h2>利好行业</h2>
              <div className={styles.list}>
                {bullish.map((item, index) => (
                  <div className={styles.listItem} key={`${item.sector || "bull"}-${index}`}>
                    <strong>{metricText(item.sector)}</strong>
                    <div style={{ marginTop: 8 }}>置信度：{item.confidence ?? item.score ?? "-"}</div>
                    <div style={{ marginTop: 6 }}>{metricText(item.logic, "暂无逻辑说明")}</div>
                  </div>
                ))}
                {!bullish.length ? <div className={styles.muted}>暂无利好行业。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>利空行业</h2>
              <div className={styles.list}>
                {bearish.map((item, index) => (
                  <div className={styles.listItem} key={`${item.sector || "bear"}-${index}`}>
                    <strong>{metricText(item.sector)}</strong>
                    <div style={{ marginTop: 8 }}>置信度：{item.confidence ?? item.score ?? "-"}</div>
                    <div style={{ marginTop: 6 }}>{metricText(item.logic, "暂无逻辑说明")}</div>
                  </div>
                ))}
                {!bearish.length ? <div className={styles.muted}>暂无利空行业。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>重点跟踪信号</h2>
              <div className={styles.list}>
                {(currentResult.sector_view?.watch_signals ?? []).map((signal, index) => (
                  <div className={styles.listItem} key={`watch-${index}`}>
                    {signal}
                  </div>
                ))}
                {!currentResult.sector_view?.watch_signals?.length ? <div className={styles.muted}>暂无重点跟踪信号。</div> : null}
              </div>
            </section>
          </>
        ) : null}

        {currentResult && panel === "stocks" ? (
          <>
            <section className={styles.card}>
              <h2>优先关注</h2>
              <div className={styles.list}>
                {recommendedStocks.map((item, index) => (
                  <div className={styles.listItem} key={`${item.code || "recommend"}-${index}`}>
                    <strong>{metricText(item.name)} ({metricText(item.code, "-")})</strong>
                    <div style={{ marginTop: 8 }}>
                      方向：{metricText(item.sector, "-")} | 现价：{metricText(item.price, "-")} | PE：{metricText(item.pe_ratio, "-")} | PB：{metricText(item.pb_ratio, "-")}
                    </div>
                    <div style={{ marginTop: 6 }}>
                      20日：{formatPercent(item.recent_20d_return)} | 60日：{formatPercent(item.recent_60d_return)} | 风格：{metricText(item.style, "-")}
                    </div>
                    <div style={{ marginTop: 6 }}>推荐逻辑：{metricText(item.reason, "暂无推荐逻辑")}</div>
                    <div style={{ marginTop: 6 }}>主要风险：{metricText(item.risk, "暂无风险提示")}</div>
                  </div>
                ))}
                {!recommendedStocks.length ? <div className={styles.muted}>暂无推荐标的。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>观察名单</h2>
              <div className={styles.list}>
                {watchlist.map((item, index) => (
                  <div className={styles.listItem} key={`${item.code || "watch"}-${index}`}>
                    <strong>{metricText(item.name)} ({metricText(item.code, "-")})</strong>
                    <div style={{ marginTop: 8 }}>
                      行业：{metricText(item.sector, "-")} | 现价：{metricText(item.price, "-")}
                    </div>
                    <div style={{ marginTop: 6 }}>{metricText(item.reason, "暂无观察逻辑")}</div>
                  </div>
                ))}
                {!watchlist.length ? <div className={styles.muted}>暂无观察名单。</div> : null}
              </div>
            </section>

            <section className={styles.card}>
              <h2>候选股票池快照</h2>
              <div className={styles.list}>
                {(currentResult.candidate_stocks ?? []).map((item, index) => (
                  <div className={styles.listItem} key={`${item.code || "candidate"}-${index}`}>
                    <strong>{metricText(item.name)} ({metricText(item.code, "-")})</strong>
                    <div style={{ marginTop: 8 }}>
                      行业：{metricText(item.sector, "-")} | 现价：{metricText(item.price, "-")} | 市值：{metricText(item.market_cap, "-")}
                    </div>
                    <div style={{ marginTop: 6 }}>
                      日涨跌：{formatPercent(item.daily_change_pct)} | 20日：{formatPercent(item.recent_20d_return)} | 60日：{formatPercent(item.recent_60d_return)}
                    </div>
                  </div>
                ))}
                {!currentResult.candidate_stocks?.length ? <div className={styles.muted}>暂无候选股票池。</div> : null}
              </div>
            </section>
          </>
        ) : null}

      </div>
    </PageFrame>
  );
}
