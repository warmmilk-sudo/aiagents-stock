import { FormEvent, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { AnalysisActionButtons, type ActionPayload } from "../../components/research/AnalysisActionButtons";
import {
  AnalysisDetailPanel,
  type AnalysisRecordDetail,
} from "../../components/research/AnalysisDetailPanel";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import styles from "../ConsolePage.module.scss";


interface AnalystConfig {
  technical: boolean;
  fundamental: boolean;
  fund_flow: boolean;
  risk: boolean;
  sentiment: boolean;
  news: boolean;
}

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

const defaultAnalysts: AnalystConfig = {
  technical: true,
  fundamental: true,
  fund_flow: true,
  risk: true,
  sentiment: false,
  news: false,
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

export function DeepAnalysisPage() {
  const navigate = useNavigate();
  const [stockInput, setStockInput] = useState("");
  const [period, setPeriod] = useState("1y");
  const [batchMode, setBatchMode] = useState<"顺序分析" | "多线程并行">("顺序分析");
  const [analysts, setAnalysts] = useState<AnalystConfig>(defaultAnalysts);
  const [task, setTask] = useState<TaskDetail | null>(null);
  const [singleRecord, setSingleRecord] = useState<AnalysisRecordDetail | null>(null);
  const [followupAssets, setFollowupAssets] = useState<FollowupAsset[]>([]);
  const [followupFilter, setFollowupFilter] = useState("全部");
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
    }
  };

  const loadFollowupAssets = async () => {
    const data = await apiFetch<FollowupAsset[]>(
      `/api/followup-assets${buildQuery({
        status_filter: followupFilter,
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
  }, [followupFilter, followupSearch]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setMessage("");
    setError("");
    try {
      const data = await apiFetch<{ task_id: string }>("/api/analysis/tasks", {
        method: "POST",
        body: JSON.stringify({
          stock_input: stockInput,
          period,
          batch_mode: batchMode,
          analysts,
        }),
      });
      setMessage(`分析任务已提交: ${data.task_id}`);
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
          period,
          batch_mode: "顺序分析",
          analysts,
        }),
      });
      setMessage(`已重新提交 ${symbol} 的分析任务: ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "再次分析失败");
    }
  };

  const handlePromoteWatchlist = async (assetId: number) => {
    await apiFetch(`/api/followup-assets/${assetId}/watchlist`, { method: "POST" });
    await loadFollowupAssets();
  };

  const handleBackToResearch = async (assetId: number, note: string) => {
    await apiFetch(`/api/followup-assets/${assetId}/research`, {
      method: "POST",
      body: JSON.stringify({ note }),
    });
    await loadFollowupAssets();
  };

  const activeAnalystCount = useMemo(
    () => Object.values(analysts).filter(Boolean).length,
    [analysts],
  );

  return (
    <PageFrame
      title="深度分析"
      summary="这里直接提交后台分析任务、轮询状态并展示结果，同时保留看过/关注链路。"
      actions={
        <>
          <StatusBadge label={`分析师 ${activeAnalystCount}/6`} tone="info" />
          {task ? (
            <StatusBadge
              label={`${task.status} ${Math.round((task.progress ?? 0) * 100)}%`}
              tone={task.status === "success" ? "success" : task.status === "failed" ? "danger" : "warning"}
            />
          ) : null}
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <form className={styles.stack} onSubmit={handleSubmit}>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="stockInput">股票代码</label>
                <textarea
                  id="stockInput"
                  rows={5}
                  value={stockInput}
                  onChange={(event) => setStockInput(event.target.value)}
                  placeholder={"000001\n600519\nAAPL"}
                />
              </div>
              <div className={styles.field}>
                <label htmlFor="period">周期</label>
                <select id="period" value={period} onChange={(event) => setPeriod(event.target.value)}>
                  <option value="6mo">6 个月</option>
                  <option value="1y">1 年</option>
                  <option value="2y">2 年</option>
                  <option value="5y">5 年</option>
                </select>
              </div>
              <div className={styles.field}>
                <label htmlFor="batchMode">批量模式</label>
                <select
                  id="batchMode"
                  value={batchMode}
                  onChange={(event) => setBatchMode(event.target.value as "顺序分析" | "多线程并行")}
                >
                  <option value="顺序分析">顺序分析</option>
                  <option value="多线程并行">多线程并行</option>
                </select>
              </div>
            </div>

            <div className={styles.compactGrid}>
              {Object.entries(analysts).map(([key, enabled]) => (
                <label className={styles.listItem} key={key}>
                  <input
                    checked={enabled}
                    onChange={(event) =>
                      setAnalysts((current) => ({
                        ...current,
                        [key]: event.target.checked,
                      }))
                    }
                    type="checkbox"
                  />{" "}
                  {key}
                </label>
              ))}
            </div>

            <div className={styles.actions}>
              <button className={styles.primaryButton} type="submit">
                开始深度分析
              </button>
              {message ? <span className={styles.successText}>{message}</span> : null}
              {error ? <span className={styles.dangerText}>{error}</span> : null}
            </div>
          </form>
        </section>

        {task ? (
          <section className={styles.card}>
            <h2>任务状态</h2>
            <p>{task.message || "等待任务状态..."}</p>
            <p className={styles.muted}>
              进度: {task.current ?? 0} / {task.total ?? 0}
            </p>
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
            <h2>最新分析结果</h2>
            <AnalysisDetailPanel record={singleRecord} />
          </section>
        ) : null}

        <section className={styles.card}>
          <h2>看过 / 关注</h2>
          <div className={styles.formGrid}>
            <div className={styles.field}>
              <label htmlFor="followupFilter">范围</label>
              <select id="followupFilter" value={followupFilter} onChange={(event) => setFollowupFilter(event.target.value)}>
                <option value="全部">全部</option>
                <option value="仅关注">仅关注</option>
                <option value="仅看过">仅看过</option>
              </select>
            </div>
            <div className={styles.field}>
              <label htmlFor="followupSearch">搜索</label>
              <input
                id="followupSearch"
                value={followupSearch}
                onChange={(event) => setFollowupSearch(event.target.value)}
                placeholder="代码 / 名称 / 账户"
              />
            </div>
          </div>
          <div className={styles.list}>
            {followupAssets.map((asset) => (
              <div className={styles.listItem} key={asset.id}>
                <strong>
                  {asset.name} ({asset.symbol})
                </strong>
                <p className={styles.muted}>
                  {asset.followup_status_label || asset.status} | {asset.latest_analysis_rating || "未评级"} |{" "}
                  {asset.latest_analysis_time || "暂无时间"}
                </p>
                <p>{asset.latest_analysis_summary || "暂无摘要"}</p>
                <div className={styles.actions}>
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
                  {asset.followup_status_label === "关注中" ? (
                    <>
                      <button
                        className={styles.secondaryButton}
                        onClick={() => navigate("/investment/smart-monitor")}
                        type="button"
                      >
                        打开盯盘
                      </button>
                      <button
                        className={styles.secondaryButton}
                        onClick={() => void handleBackToResearch(asset.id, asset.latest_analysis_summary || "")}
                        type="button"
                      >
                        移回看过
                      </button>
                    </>
                  ) : (
                    <button
                      className={styles.secondaryButton}
                      onClick={() => void handlePromoteWatchlist(asset.id)}
                      type="button"
                    >
                      加入盯盘
                    </button>
                  )}
                  <AnalysisActionButtons actionPayload={asset.action_payload} />
                </div>
              </div>
            ))}
            {followupAssets.length === 0 ? <div className={styles.muted}>暂无看过/关注股票</div> : null}
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
