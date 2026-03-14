import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { AnalysisActionButtons } from "../../components/research/AnalysisActionButtons";
import { FormattedReport, splitReportSections } from "../../components/research/FormattedReport";
import { apiFetch, apiFetchCached, buildQuery } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import styles from "../ConsolePage.module.scss";

interface ActionPayload {
  symbol: string;
  stock_name: string;
  account_name: string;
  origin_analysis_id?: number;
  default_cost_price?: number;
  default_note?: string;
  strategy_context?: Record<string, unknown>;
}

interface AnalysisRecordDetail {
  id?: number;
  symbol?: string;
  stock_name?: string;
  analysis_time_text?: string;
  period?: string;
  account_name?: string;
  portfolio_state_label?: string;
  linked_asset_status_label?: string;
  summary?: string;
  portfolio_action_label?: string;
  is_in_portfolio?: boolean;
  action_payload?: ActionPayload | null;
  final_decision?: Record<string, unknown>;
  stock_info?: Record<string, unknown>;
  agents_results?: Record<string, unknown>;
  discussion_result?: unknown;
}

interface AnalysisHistoryItem extends AnalysisRecordDetail {
  id: number;
  symbol: string;
  stock_name: string;
  analysis_source_label?: string;
  decision_label?: string;
}

interface RawReportEntry {
  key: string;
  title: string;
  role: string;
  focusAreas: string[];
  timestamp: string;
  body: unknown;
  reasoning: string;
  summary: string;
}

type ReportCategory = "technical" | "fundamental" | "fund_flow" | "market" | "news" | "risk" | "team";

const reportTabs: Array<{ key: ReportCategory; label: string; emptyText: string }> = [
  { key: "technical", label: "技术", emptyText: "暂无技术报告" },
  { key: "fundamental", label: "基本面", emptyText: "暂无基本面报告" },
  { key: "fund_flow", label: "资金", emptyText: "暂无资金报告" },
  { key: "market", label: "市场", emptyText: "暂无市场报告" },
  { key: "news", label: "新闻", emptyText: "暂无新闻报告" },
  { key: "risk", label: "风险", emptyText: "暂无风险报告" },
  { key: "team", label: "团队", emptyText: "暂无团队报告" },
];

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

function detectReportCategory(name: string, payload: Record<string, unknown>): ReportCategory | null {
  const text = [
    name,
    payload.agent_name,
    payload.agent_role,
    Array.isArray(payload.focus_areas) ? payload.focus_areas.join(" ") : "",
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (/news|新闻|事件|公告|舆情|资讯/.test(text)) {
    return "news";
  }
  if (/sentiment|市场情绪|情绪|热点|赚钱效应|热度/.test(text)) {
    return "market";
  }
  if (/fund[_\s-]?flow|资金|主力|资金流/.test(text)) {
    return "fund_flow";
  }
  if (/fundamental|基本面|财务|估值/.test(text)) {
    return "fundamental";
  }
  if (/technical|技术|趋势|形态|均线/.test(text)) {
    return "technical";
  }
  if (/(^|[\s_-])risk($|[\s_-])|风险|风控/.test(text)) {
    return "risk";
  }
  if (/discussion|chief|团队|首席|综合/.test(text)) {
    return "team";
  }
  return null;
}

function buildRawReportEntry(name: string, payload: unknown): RawReportEntry {
  const normalizedPayload = asRecord(payload);
  const reportSections = splitReportSections(
    normalizedPayload.analysis || normalizedPayload.report || payload,
  );
  const body = reportSections.body || normalizedPayload.analysis || normalizedPayload.report || payload;
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
    summary: buildSummary(body),
  };
}

function buildRawReportEntries(
  agentsResults: Record<string, unknown>,
  discussionResult?: unknown,
): Record<ReportCategory, RawReportEntry | null> {
  const entries: Record<ReportCategory, RawReportEntry | null> = {
    technical: null,
    fundamental: null,
    fund_flow: null,
    market: null,
    news: null,
    risk: null,
    team: null,
  };

  Object.entries(agentsResults).forEach(([name, payload]) => {
    const normalizedPayload = asRecord(payload);
    const category = detectReportCategory(name, normalizedPayload);
    if (!category) {
      return;
    }

    const entry = buildRawReportEntry(name, payload);
    if (category === "team" && discussionResult) {
      return;
    }
    if (!entries[category]) {
      entries[category] = entry;
    }
  });

  if (discussionResult) {
    const sections = splitReportSections(discussionResult);
    entries.team = {
      key: "__discussion__",
      title: "团队讨论",
      role: "",
      focusAreas: [],
      timestamp: "",
      body: sections.body || discussionResult,
      reasoning: sections.reasoning,
      summary: buildSummary(sections.body || discussionResult),
    };
  }

  return entries;
}

function formatMetric(value: unknown, fallback = "暂无"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  const numeric = Number(value);
  if (typeof value === "number" || (typeof value === "string" && value.trim() !== "" && Number.isFinite(numeric))) {
    return numeric.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
  }
  return String(value);
}

function cleanOrderedItem(text: string): string {
  return text
    .replace(/^\s*(?:\d+[.)、]?|[①②③④⑤⑥⑦⑧⑨⑩]|[一二三四五六七八九十]+[、.])\s*/u, "")
    .trim();
}

function toOrderedItems(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => cleanOrderedItem(String(item || ""))).filter(Boolean);
  }

  const text = String(value || "").replace(/\r\n/g, "\n").trim();
  if (!text) {
    return [];
  }

  const numberedMatches = text.match(/(?:^|\n)\s*(?:\d+[.)、]?|[①②③④⑤⑥⑦⑧⑨⑩]|[一二三四五六七八九十]+[、.])\s*.+/gu);
  if (numberedMatches?.length) {
    return numberedMatches.map((item) => cleanOrderedItem(item)).filter(Boolean);
  }

  const lines = text
    .split("\n")
    .map((line) => cleanOrderedItem(line))
    .filter(Boolean);
  if (lines.length > 1) {
    return lines;
  }

  const segments = text
    .split(/[；;]+/u)
    .map((item) => cleanOrderedItem(item))
    .filter(Boolean);
  return segments.length ? segments : [text];
}

function renderOrderedContent(value: unknown, emptyText: string) {
  const items = toOrderedItems(value);
  if (!items.length) {
    return <div className={styles.muted}>{emptyText}</div>;
  }
  return (
    <ol className={styles.detailOrderedList}>
      {items.map((item, index) => (
        <li key={`${item}-${index}`}>{item}</li>
      ))}
    </ol>
  );
}

function HistoryDetailSection({
  title,
  children,
  defaultOpen = true,
}: {
  title: string;
  children: ReactNode;
  defaultOpen?: boolean;
}) {
  return (
    <details className={styles.historyDetailPanel} open={defaultOpen}>
      <summary className={styles.historyDetailSummary}>{title}</summary>
      <div className={styles.historyDetailPanelBody}>{children}</div>
    </details>
  );
}

function RawReportWorkspace({
  agentsResults,
  discussionResult,
}: {
  agentsResults?: Record<string, unknown>;
  discussionResult?: unknown;
}) {
  const entries = useMemo(
    () => buildRawReportEntries(agentsResults ?? {}, discussionResult),
    [agentsResults, discussionResult],
  );
  const [activeKey, setActiveKey] = useState<ReportCategory>("technical");
  const availableTabs = useMemo(
    () => reportTabs.filter((item) => entries[item.key]),
    [entries],
  );

  useEffect(() => {
    if (entries[activeKey]) {
      return;
    }
    const fallbackKey = availableTabs[0]?.key || "technical";
    setActiveKey(fallbackKey);
  }, [activeKey, availableTabs, entries]);

  const activeEntry = entries[activeKey];
  const activeTab = availableTabs.find((item) => item.key === activeKey) || availableTabs[0] || null;

  if (!availableTabs.length) {
    return <div className={styles.muted}>暂无原始报告</div>;
  }

  return (
    <div className={styles.historyDetailContentStack}>
      <div className={styles.historyDetailTabs} role="tablist" aria-label="原始报告分类">
        {availableTabs.map((item) => (
          <button
            aria-selected={item.key === activeKey}
            className={item.key === activeKey ? styles.primaryButton : styles.secondaryButton}
            key={item.key}
            onClick={() => setActiveKey(item.key)}
            role="tab"
            type="button"
          >
            {item.label}
          </button>
        ))}
      </div>

      {activeEntry ? (
        <div className={styles.reportWorkbenchPanel}>
          <div className={styles.reportWorkbenchHeader}>
            <div>
              <h3>{activeEntry.title}</h3>
              {activeEntry.role || activeEntry.focusAreas.length ? (
                <p className={styles.helperText}>
                  {[activeEntry.role, activeEntry.focusAreas.join(" / ")].filter(Boolean).join(" | ")}
                </p>
              ) : null}
              {activeEntry.summary ? <p className={styles.helperText}>{activeEntry.summary}</p> : null}
            </div>
            {activeEntry.timestamp ? <span className={styles.historyMeta}>{activeEntry.timestamp}</span> : null}
          </div>
          <div className={styles.reportWorkbenchContent}>
            <FormattedReport content={activeEntry.body} emptyText={activeTab?.emptyText || "暂无正文"} />
          </div>
          {activeEntry.reasoning ? (
            <details className={styles.historyDetailPanel}>
              <summary className={styles.historyDetailSummary}>推理过程</summary>
              <div className={styles.historyDetailPanelBody}>
                <FormattedReport content={activeEntry.reasoning} emptyText="暂无推理过程" />
              </div>
            </details>
          ) : null}
        </div>
      ) : (
        <div className={styles.muted}>{activeTab?.emptyText || "暂无正文"}</div>
      )}
    </div>
  );
}

export function HistoryPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [records, setRecords] = useState<AnalysisHistoryItem[]>([]);
  const [recordDetails, setRecordDetails] = useState<Record<number, AnalysisRecordDetail>>({});
  const [portfolioState, setPortfolioState] = useState("全部");
  const [accountName, setAccountName] = useState("全部账户");
  const [searchTerm, setSearchTerm] = useState("");

  const selectedRecordId = Number(searchParams.get("recordId") || 0);
  const selectedRecord = selectedRecordId ? (recordDetails[selectedRecordId] ?? null) : null;

  const loadList = async () => {
    const data = await apiFetchCached<AnalysisHistoryItem[]>(
      `/api/analysis-history${buildQuery({
        portfolio_state: portfolioState,
        account_name: accountName === "全部账户" ? "" : accountName,
        search_term: searchTerm,
      })}`,
    );
    setRecords(data);
  };

  const loadDetail = async (recordId: number) => {
    if (!recordId) {
      return;
    }
    const data = await apiFetchCached<AnalysisRecordDetail>(`/api/analysis-history/${recordId}`);
    setRecordDetails((current) => ({ ...current, [recordId]: data }));
  };

  useEffect(() => {
    void loadList();
  }, [portfolioState, accountName, searchTerm]);

  useEffect(() => {
    if (selectedRecordId && !recordDetails[selectedRecordId]) {
      void loadDetail(selectedRecordId);
    }
  }, [selectedRecordId, recordDetails]);

  const accounts = useMemo(() => {
    const set = new Set<string>(["全部账户"]);
    records.forEach((item) => set.add(item.account_name || "默认账户"));
    return Array.from(set);
  }, [records]);

  const openRecord = (recordId: number) => {
    setSearchParams({ recordId: String(recordId) });
  };

  const closeDetail = () => {
    setSearchParams({});
  };

  const handleDelete = async (recordId: number) => {
    await apiFetch(`/api/analysis-history/${recordId}`, { method: "DELETE" });
    if (selectedRecordId === recordId) {
      setSearchParams({});
    }
    setRecordDetails((current) => {
      const next = { ...current };
      delete next[recordId];
      return next;
    });
    await loadList();
  };

  return (
    <PageFrame
      actions={selectedRecordId ? undefined : <StatusBadge label={`记录 ${records.length}`} tone="default" />}
      title="分析历史"
    >
      <div className={selectedRecordId ? `${styles.stack} ${styles.historyDetailPageStack}` : styles.stack}>
        {selectedRecordId ? (
          <>
            <section className={styles.card}>
              <div className={styles.stack}>
                <div className={styles.actions}>
                  <button className={styles.secondaryButton} onClick={closeDetail} type="button">
                    返回历史列表
                  </button>
                </div>

                {selectedRecord ? (
                  (() => {
                    return (
                      <>
                        <div className={styles.historyRecordTop}>
                          <div>
                            <strong className={styles.historyRecordTitle}>
                              {selectedRecord.stock_name || selectedRecord.symbol || "未知股票"}
                              {selectedRecord.symbol ? `（${selectedRecord.symbol}）` : ""}
                            </strong>
                            <p className={styles.historyMeta}>
                              {formatDateTime(selectedRecord.analysis_time_text, "暂无时间")} | {selectedRecord.portfolio_state_label || "未持仓"} | {selectedRecord.account_name || "默认账户"}
                            </p>
                          </div>
                          <div className={styles.historyActionRow}>
                            <AnalysisActionButtons
                              actionPayload={selectedRecord.action_payload}
                              className={styles.historyInlineActions}
                              isInPortfolio={Boolean(selectedRecord.is_in_portfolio)}
                              portfolioLabel={selectedRecord.portfolio_action_label}
                              showPortfolioAction={false}
                              watchlistButtonClassName={styles.secondaryButton}
                            />
                            <button className={styles.dangerButton} onClick={() => void handleDelete(selectedRecordId)} type="button">
                              删除记录
                            </button>
                          </div>
                        </div>
                      </>
                    );
                  })()
                ) : (
                  <div className={styles.muted}>加载详情中...</div>
                )}
              </div>
            </section>

            {selectedRecord ? (
              (() => {
                const detailFinalDecision = asRecord(selectedRecord.final_decision);

                return (
                  <section className={`${styles.stack} ${styles.historyDetailStack}`}>
                    <HistoryDetailSection title="核心建议">
                      {renderOrderedContent(
                        detailFinalDecision.operation_advice || selectedRecord.summary,
                        "暂无核心建议",
                      )}
                    </HistoryDetailSection>

                    <HistoryDetailSection title="关键位置">
                      <div className={styles.historyLevelGrid}>
                        <div className={styles.historySummaryCell}>
                          <span>入场区间</span>
                          <strong>{`${formatMetric(detailFinalDecision.entry_min)} - ${formatMetric(detailFinalDecision.entry_max)}`}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>止盈位</span>
                          <strong>{formatMetric(detailFinalDecision.take_profit)}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>止损位</span>
                          <strong>{formatMetric(detailFinalDecision.stop_loss)}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>持有周期</span>
                          <strong>{formatMetric(detailFinalDecision.holding_period)}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>当前状态</span>
                          <strong>{selectedRecord.linked_asset_status_label || selectedRecord.portfolio_state_label || "暂无"}</strong>
                        </div>
                        <div className={styles.historySummaryCell}>
                          <span>账户</span>
                          <strong>{selectedRecord.account_name || "默认账户"}</strong>
                        </div>
                      </div>
                    </HistoryDetailSection>

                    <HistoryDetailSection title="风险提示" defaultOpen={false}>
                      {renderOrderedContent(detailFinalDecision.risk_warning, "暂无风险提示")}
                    </HistoryDetailSection>

                    <HistoryDetailSection title="原始报告">
                      <RawReportWorkspace
                        agentsResults={selectedRecord.agents_results}
                        discussionResult={selectedRecord.discussion_result}
                      />
                    </HistoryDetailSection>
                  </section>
                );
              })()
            ) : null}
          </>
        ) : (
          <>
            <section className={styles.card}>
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="portfolioState">持仓状态</label>
                  <select id="portfolioState" onChange={(event) => setPortfolioState(event.target.value)} value={portfolioState}>
                    <option value="全部">全部</option>
                    <option value="在持仓">在持仓</option>
                    <option value="未持仓">未持仓</option>
                  </select>
                </div>
                <div className={styles.field}>
                  <label htmlFor="accountName">账户</label>
                  <select id="accountName" onChange={(event) => setAccountName(event.target.value)} value={accountName}>
                    {accounts.map((item) => (
                      <option key={item} value={item}>
                        {item}
                      </option>
                    ))}
                  </select>
                </div>
                <div className={styles.field}>
                  <label htmlFor="searchTerm">搜索</label>
                  <input
                    id="searchTerm"
                    onChange={(event) => setSearchTerm(event.target.value)}
                    placeholder="股票代码 / 名称"
                    value={searchTerm}
                  />
                </div>
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.list}>
                {records.map((record) => {
                  const finalDecision = asRecord(record.final_decision);

                  return (
                    <div className={styles.historyRecordCard} key={record.id}>
                      <div className={styles.historyRecordTop}>
                        <div>
                          <strong className={styles.historyRecordTitle}>
                            {record.stock_name}（{record.symbol}）
                          </strong>
                          <p className={styles.historyMeta}>
                            {formatDateTime(record.analysis_time_text, "暂无时间")} | {record.portfolio_state_label || "未持仓"} | {record.analysis_source_label || "历史分析"}
                          </p>
                        </div>
                        <div className={`${styles.historyActionRow} ${styles.historyListActionRow}`}>
                          {record.action_payload ? (
                            <AnalysisActionButtons
                              actionPayload={record.action_payload}
                              className={`${styles.historyInlineActions} ${styles.historyListInlineActions}`}
                              isInPortfolio={Boolean(record.is_in_portfolio)}
                              portfolioLabel={record.portfolio_action_label}
                              showPortfolioAction={false}
                              watchlistButtonClassName={styles.secondaryButton}
                            />
                          ) : null}
                          <button
                            className={styles.secondaryButton}
                            onClick={() => openRecord(record.id)}
                            type="button"
                          >
                            查看历史
                          </button>
                        </div>
                      </div>

                      <div className={styles.historyListBody}>
                        <div className={styles.historyListMetrics} aria-label="分析摘要指标">
                          <span className={styles.historyListMetric}>
                            评级：<strong>{formatMetric(finalDecision.rating || record.decision_label)}</strong>
                          </span>
                          <span className={styles.historyListMetric}>
                            目标价：<strong>{formatMetric(finalDecision.target_price)}</strong>
                          </span>
                          <span className={styles.historyListMetric}>
                            信心度：<strong>{formatMetric(finalDecision.confidence_level)}</strong>
                          </span>
                        </div>
                        <p className={styles.historyListSummary}>{record.summary || "暂无摘要"}</p>
                      </div>
                    </div>
                  );
                })}
                {records.length === 0 ? <div className={styles.muted}>暂无匹配记录</div> : null}
              </div>
            </section>
          </>
        )}
      </div>
    </PageFrame>
  );
}
