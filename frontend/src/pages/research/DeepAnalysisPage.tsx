import { FormEvent, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { TaskProgressBar } from "../../components/common/TaskProgressBar";
import type { ActionPayload } from "../../components/research/AnalysisActionButtons";
import {
  AnalysisDetailPanel,
  type AnalysisRecordDetail,
} from "../../components/research/AnalysisDetailPanel";
import { ApiRequestError, apiFetch, apiFetchCached, buildQuery } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { encodeIntent } from "../../lib/intents";
import {
  useDeepAnalysisStore,
  type DeepAnalysisAnalystConfig,
} from "../../stores/deepAnalysisStore";
import styles from "../ConsolePage.module.scss";

interface TaskResultRow {
  symbol: string;
  success: boolean;
  error?: string;
  record_id?: number;
}

interface TaskResult {
  mode: "single" | "batch";
  record_id?: number;
  symbol?: string;
  period?: string;
  max_workers?: number;
  results?: TaskResultRow[];
}

interface TaskDetail {
  id: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
  error?: string;
  result?: TaskResult | null;
}

interface FollowupAsset {
  id: number;
  symbol: string;
  name: string;
  account_name?: string;
  followup_status_label?: string;
  latest_analysis_time?: string;
  latest_analysis_rating?: string;
  latest_analysis_scope?: string;
  latest_analysis_summary?: string;
  latest_analysis_id?: number;
  status?: string;
  action_payload?: ActionPayload | null;
}

type SectionKey = "start" | "watchlist" | "viewed" | "settings";

const sectionTabs = [
  { key: "start", label: "开始分析" },
  { key: "watchlist", label: "关注中" },
  { key: "viewed", label: "看过" },
  { key: "settings", label: "设置" },
];

const analystLabels: Record<keyof DeepAnalysisAnalystConfig, string> = {
  technical: "技术分析师",
  fundamental: "基本面分析师",
  fund_flow: "资金流分析师",
  risk: "风险控制分析师",
  sentiment: "市场情绪分析师",
  news: "新闻事件分析师",
};

function buildSingleRecordFromTask(task: TaskDetail | null): AnalysisRecordDetail | null {
  const result = task?.result;
  if (!result || result.mode !== "single") {
    return null;
  }
  return {
    id: result.record_id,
    symbol: result.symbol,
    stock_name: result.symbol,
  };
}

function isWatchlistAsset(asset: FollowupAsset) {
  return asset.followup_status_label === "关注中" || asset.status === "watchlist";
}

function taskBadge(task: TaskDetail | null): { label: string; tone: "warning" | "success" | "danger" } | null {
  if (!task) {
    return null;
  }
  if (task.status === "success") {
    return { label: "分析完成", tone: "success" };
  }
  if (task.status === "failed" || task.status === "cancelled") {
    return { label: "分析失败", tone: "danger" };
  }
  return { label: "分析中", tone: "warning" };
}

function taskProgressTone(task: TaskDetail | null): "running" | "success" | "danger" {
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

export function DeepAnalysisPage() {
  const navigate = useNavigate();
  const batchMode = useDeepAnalysisStore((state) => state.batchMode);
  const setBatchMode = useDeepAnalysisStore((state) => state.setBatchMode);
  const maxWorkers = useDeepAnalysisStore((state) => state.maxWorkers);
  const setMaxWorkers = useDeepAnalysisStore((state) => state.setMaxWorkers);
  const analysts = useDeepAnalysisStore((state) => state.analysts);
  const setAnalysts = useDeepAnalysisStore((state) => state.setAnalysts);

  const [stockInput, setStockInput] = useState("");
  const [section, setSection] = useState<SectionKey>("start");
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [singleRecord, setSingleRecord] = useState<AnalysisRecordDetail | null>(null);
  const [followupAssets, setFollowupAssets] = useState<FollowupAsset[]>([]);
  const [followupSearch, setFollowupSearch] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadTask = async () => {
    const latest = await apiFetch<TaskDetail | null>("/api/tasks/latest");
    setTask(latest);
    if (latest?.status === "success" && latest.result?.mode === "single" && latest.result.record_id) {
      const record = await apiFetch<AnalysisRecordDetail>(`/api/analysis-history/${latest.result.record_id}`);
      setSingleRecord(record);
      return;
    }
    if (latest?.status === "success") {
      setSingleRecord(buildSingleRecordFromTask(latest));
      return;
    }
    setSingleRecord(null);
  };

  const loadFollowupAssets = async () => {
    const data = await apiFetchCached<FollowupAsset[]>(
      `/api/followup-assets${buildQuery({
        search_term: followupSearch,
      })}`,
    );
    setFollowupAssets(data);
  };

  useEffect(() => {
    void loadTask();
    const timer = window.setInterval(() => void loadTask(), 2000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    void loadFollowupAssets();
  }, [followupSearch]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: stockInput,
          batch_mode: batchMode,
          max_workers: maxWorkers,
          analysts,
        }),
      });
      setMessage(`分析任务已提交 ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交任务失败");
    }
  };

  const handleReAnalyze = async (symbol: string) => {
    setStockInput(symbol);
    setError("");
    setMessage("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: symbol,
          batch_mode: "顺序分析",
          max_workers: maxWorkers,
          analysts,
        }),
      });
      setMessage(`已重新提交 ${symbol} 的分析任务 ${data.task_id}`);
      setSection("start");
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "再次分析失败");
    }
  };

  const handlePromoteWatchlist = async (assetId: number) => {
    await apiFetch(`/api/followup-assets/${assetId}/watchlist`, { method: "POST" });
    await loadFollowupAssets();
  };

  const watchlistAssets = useMemo(
    () => followupAssets.filter((asset) => isWatchlistAsset(asset)),
    [followupAssets],
  );
  const viewedAssets = useMemo(
    () => followupAssets.filter((asset) => !isWatchlistAsset(asset)),
    [followupAssets],
  );

  const buildMonitorPath = (payload?: ActionPayload | null) =>
    payload
      ? `/investment/smart-monitor?intent=${encodeIntent({ type: "watchlist", payload })}`
      : "/investment/smart-monitor";

  const handleOpenMonitor = async (asset: FollowupAsset) => {
    setMessage("");
    setError("");
    try {
      if (!isWatchlistAsset(asset)) {
        await handlePromoteWatchlist(asset.id);
        setMessage(`${asset.symbol} 已加入智能盯盘`);
      }
      navigate(buildMonitorPath(asset.action_payload));
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加入盯盘失败");
    }
  };

  const renderAssetSection = (assets: FollowupAsset[], emptyText: string) => (
    <section className={styles.card}>
      <div className={styles.stack}>
        <div className={styles.field}>
          <label htmlFor="followupSearch">搜索</label>
          <input
            id="followupSearch"
            onChange={(event) => setFollowupSearch(event.target.value)}
            placeholder="搜索代码 / 名称 / 账户"
            value={followupSearch}
          />
        </div>
        <div className={styles.responsiveActionGrid}>
          <button className={styles.secondaryButton} onClick={() => void loadFollowupAssets()} type="button">
            刷新
          </button>
        </div>
      </div>
      {message ? <p className={styles.successText}>{message}</p> : null}
      {error ? <p className={styles.dangerText}>{error}</p> : null}
      <div className={styles.list}>
        {assets.map((asset) => (
          <div className={styles.listItem} key={asset.id}>
            <strong>
              {asset.name} ({asset.symbol})
            </strong>
            <p className={styles.muted}>
              {asset.followup_status_label || asset.status} | {asset.latest_analysis_rating || "未评级"} |{" "}
              {formatDateTime(asset.latest_analysis_time, "暂无时间")}
            </p>
            <p>{asset.latest_analysis_summary || "暂无摘要"}</p>
            <div className={styles.actions}>
              <button className={styles.secondaryButton} onClick={() => void handleOpenMonitor(asset)} type="button">
                加入盯盘
              </button>
              <button className={styles.secondaryButton} onClick={() => void handleReAnalyze(asset.symbol)} type="button">
                再次分析
              </button>
              {asset.latest_analysis_id ? (
                <button
                  className={styles.secondaryButton}
                  onClick={() => navigate(`/research/history?recordId=${asset.latest_analysis_id}`)}
                  type="button"
                >
                  分析历史
                </button>
              ) : null}
            </div>
          </div>
        ))}
        {assets.length === 0 ? <div className={styles.muted}>{emptyText}</div> : null}
      </div>
    </section>
  );

  return (
    <PageFrame
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      title="深度分析"
    >
      <div className={styles.stack}>
        {section === "start" ? (
          <>
            <section className={styles.card}>
              {message ? <p className={styles.successText}>{message}</p> : null}
              {error ? <p className={styles.dangerText}>{error}</p> : null}
              <form className={styles.stack} id="deep-analysis-form" onSubmit={handleSubmit}>
                <div className={styles.field}>
                  <label htmlFor="stockInput">股票代码（支持逗号或换行分隔）</label>
                  <textarea
                    id="stockInput"
                    onChange={(event) => setStockInput(event.target.value)}
                    placeholder={"000001,600519,AAPL"}
                    rows={4}
                    value={stockInput}
                  />
                </div>
                <div className={styles.responsiveActionGrid}>
                  <button className={styles.primaryButton} type="submit">
                    开始深度分析
                  </button>
                </div>
              </form>
            </section>

            {task ? (
              <section className={styles.card}>
                <div className={styles.cardHeader}>
                  <div>
                    <h2>任务状态</h2>
                    <p className={styles.helperText}>进度实时刷新，批量任务完成后可直接跳转对应历史记录。</p>
                  </div>
                </div>
                <TaskProgressBar
                  current={task.current ?? (task.status === "success" ? task.total ?? 1 : 0)}
                  total={task.total ?? 1}
                  message={task.message || "等待任务状态..."}
                  tone={taskProgressTone(task)}
                />
                {task.error ? <p className={styles.dangerText}>{task.error}</p> : null}
                {task.status === "success" && task.result?.mode === "batch" ? (
                  <div className={styles.tableWrap}>
                    <table className={styles.table}>
                      <thead>
                        <tr>
                          <th>股票</th>
                          <th>结果</th>
                          <th>历史</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(task.result.results ?? []).map((item) => (
                          <tr key={`${item.symbol}-${item.record_id ?? "na"}`}>
                            <td>{item.symbol}</td>
                            <td>{item.success ? "成功" : item.error || "失败"}</td>
                            <td>
                              {item.record_id ? (
                                <button
                                  className={styles.secondaryButton}
                                  onClick={() => navigate(`/research/history?recordId=${item.record_id}`)}
                                  type="button"
                                >
                                  查看
                                </button>
                              ) : (
                                <span className={styles.muted}>无</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : null}
              </section>
            ) : null}

            {singleRecord ? (
              <section className={styles.card}>
                <div className={styles.cardHeader}>
                  <div>
                    <h2>最新分析结果</h2>
                  </div>
                </div>
                <AnalysisDetailPanel record={singleRecord} />
              </section>
            ) : null}
          </>
        ) : null}

        {section === "watchlist" ? renderAssetSection(watchlistAssets, "暂无关注中的股票") : null}

        {section === "viewed" ? renderAssetSection(viewedAssets, "暂无看过的股票") : null}

        {section === "settings" ? (
          <section className={styles.card}>
            <p className={styles.helperText}>股票数据周期由系统配置中的“默认股票数据周期”统一控制，这里不再单独设置。</p>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="batchMode">批量模式</label>
                <select
                  id="batchMode"
                  onChange={(event) => setBatchMode(event.target.value as "顺序分析" | "多线程并行")}
                  value={batchMode}
                >
                  <option value="顺序分析">顺序分析</option>
                  <option value="多线程并行">多线程并行</option>
                </select>
              </div>
              {batchMode === "多线程并行" ? (
                <div className={styles.field}>
                  <label htmlFor="maxWorkers">并行线程数</label>
                  <select
                    id="maxWorkers"
                    onChange={(event) => setMaxWorkers(Number(event.target.value) || 3)}
                    value={maxWorkers}
                  >
                    {[1, 2, 3, 4, 5].map((value) => (
                      <option key={value} value={value}>
                        {value}
                      </option>
                    ))}
                  </select>
                </div>
              ) : null}
            </div>
            <div className={styles.field} style={{ marginTop: 14 }}>
              <label>分析师配置</label>
              <div className={styles.analystSelectionGroup}>
                {Object.entries(analysts).map(([key, enabled]) => (
                  <label className={styles.analystOption} key={key}>
                    <input
                      checked={enabled}
                      onChange={(event) =>
                        setAnalysts({
                          ...analysts,
                          [key]: event.target.checked,
                        })
                      }
                      type="checkbox"
                    />
                    <span>{analystLabels[key as keyof DeepAnalysisAnalystConfig] || key}</span>
                  </label>
                ))}
              </div>
            </div>
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
