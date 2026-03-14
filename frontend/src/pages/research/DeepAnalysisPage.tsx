import { FormEvent, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import type { ActionPayload } from "../../components/research/AnalysisActionButtons";
import {
  AnalysisDetailPanel,
  type AnalysisRecordDetail,
} from "../../components/research/AnalysisDetailPanel";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import { encodeIntent } from "../../lib/intents";
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

const analystLabels: Record<keyof AnalystConfig, string> = {
  technical: "技术分析师",
  fundamental: "基本面分析师",
  fund_flow: "资金流分析师",
  risk: "风险控制分析师",
  sentiment: "市场情绪分析师",
  news: "新闻事件分析师",
};

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

function isWatchlistAsset(asset: FollowupAsset) {
  return asset.followup_status_label === "关注中" || asset.status === "watchlist";
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
          period,
          batch_mode: batchMode,
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
          period,
          batch_mode: "顺序分析",
          analysts,
        }),
      });
      setMessage(`已重新提交 ${symbol} 的分析任务 ${data.task_id}`);
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "再次分析失败");
    }
  };

  const handlePromoteWatchlist = async (assetId: number) => {
    await apiFetch(`/api/followup-assets/${assetId}/watchlist`, { method: "POST" });
    await loadFollowupAssets();
  };

  const activeAnalystCount = useMemo(
    () => Object.values(analysts).filter(Boolean).length,
    [analysts],
  );

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

  const renderAssetCard = (title: string, assets: FollowupAsset[], emptyText: string) => (
    <section className={`${styles.card} ${styles.span6}`}>
      <div className={styles.cardHeader}>
        <div>
          <h2>{title}</h2>
          <p className={styles.helperText}>只保留盯盘、再次分析和历史回看三类高频操作。</p>
        </div>
      </div>
      <div className={styles.list}>
        {assets.map((asset) => (
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
      actions={
        <>
          <StatusBadge label={`分析师 ${activeAnalystCount}/6`} tone="default" />
          {task ? (
            <StatusBadge
              label={`${task.status} ${Math.round((task.progress ?? 0) * 100)}%`}
              tone={task.status === "success" ? "success" : task.status === "failed" ? "danger" : "warning"}
            />
          ) : null}
        </>
      }
      title="深度分析"
    >
      <div className={styles.grid}>
        <section className={`${styles.card} ${styles.span6}`}>
          <div className={styles.cardHeader}>
            <div>
              <h2>开始分析</h2>
              <p className={styles.helperText}>录入股票后直接提交任务，结果和任务状态会保留在当前页。</p>
            </div>
          </div>
          <form className={styles.stack} onSubmit={handleSubmit}>
            <div className={styles.field}>
              <label htmlFor="stockInput">股票代码</label>
              <textarea
                id="stockInput"
                onChange={(event) => setStockInput(event.target.value)}
                placeholder={"000001\n600519\nAAPL"}
                rows={8}
                value={stockInput}
              />
            </div>
            <div className={styles.actions}>
              <button className={styles.primaryButton} type="submit">
                开始深度分析
              </button>
            </div>
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
          </form>
        </section>

        <section className={`${styles.card} ${styles.span6}`}>
          <div className={styles.cardHeader}>
            <div>
              <h2>设置</h2>
              <p className={styles.helperText}>周期、批量模式和分析师选择独立放在这里，避免和输入区混在一起。</p>
            </div>
          </div>
          <div className={styles.stack}>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="period">周期</label>
                <select id="period" onChange={(event) => setPeriod(event.target.value)} value={period}>
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
                  onChange={(event) => setBatchMode(event.target.value as "顺序分析" | "多线程并行")}
                  value={batchMode}
                >
                  <option value="顺序分析">顺序分析</option>
                  <option value="多线程并行">多线程并行</option>
                </select>
              </div>
              <div className={styles.field}>
                <label htmlFor="followupSearch">跟进检索</label>
                <input
                  id="followupSearch"
                  onChange={(event) => setFollowupSearch(event.target.value)}
                  placeholder="搜索代码 / 名称 / 账户"
                  value={followupSearch}
                />
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
                  {analystLabels[key as keyof AnalystConfig] || key}
                </label>
              ))}
            </div>
          </div>
        </section>

        {renderAssetCard("关注中", watchlistAssets, "暂无关注中的股票")}
        {renderAssetCard("看过", viewedAssets, "暂无看过的股票")}

        {task ? (
          <section className={`${styles.card} ${styles.span12}`}>
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
          <section className={`${styles.card} ${styles.span12}`}>
            <h2>最新分析结果</h2>
            <AnalysisDetailPanel record={singleRecord} />
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
