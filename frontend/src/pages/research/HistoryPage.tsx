import { useEffect, useMemo, useState, type CSSProperties, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { MarkdownReport } from "../../components/research/MarkdownReport";
import { usePageFeedback } from "../../hooks/usePageFeedback";
import { apiFetch, buildQuery } from "../../lib/api";
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
  analysis_scope?: string;
  analysis_scope_label?: string;
  account_name?: string;
  linked_asset_account_name?: string;
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

interface AnalysisHistoryStockSummary {
  symbol: string;
  stock_name: string;
  latest_analysis_date: string;
  latest_decision_label: string;
  latest_analysis_scope?: string;
  latest_analysis_scope_label?: string;
  latest_source_label?: string;
  latest_summary?: string;
  portfolio_state_label: string;
  is_in_portfolio: boolean;
  linked_asset_status_label?: string;
  report_count: number;
  account_name?: string;
  has_memory: boolean;
  first_analysis_date?: string;
  research_report_count: number;
  portfolio_report_count: number;
}

interface FactualMemory {
  id: number;
  fact_content: string;
  category: string;
  timestamp: string;
  importance_score: number;
  is_ignored: boolean;
}

interface MemoryArchive {
  stock_code: string;
  long_term_profile: { macro_profile: string; last_updated: string } | null;
  working_memories: { analysis_date: string; decision_summary: string }[];
  factual_memories: FactualMemory[];
  summary: { working_count: number; factual_count: number; has_long_term_profile: boolean };
}

interface RawReportEntry {
  key: string;
  rawContent: string;
}

type ReportCategory = "technical" | "fundamental" | "fund_flow" | "market" | "news" | "risk" | "team";
type ArchiveCategory = "持仓" | "关注" | "研究池";

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

function sanitizeDiscussionSpeakers(value: unknown): string {
  return String(value || "").replace(
    /【(投资总监（主持）|技术分析师|基本面分析师|资金面分析师|风险管理师|市场情绪分析师|新闻分析师)(?:\s+[^\]】:：]{1,12})?】(?=[:：])/g,
    "【$1】",
  );
}

function detectReportCategory(name: string, payload: Record<string, unknown>): ReportCategory | null {
  const normalizedName = String(name || "").trim().toLowerCase();
  const exactKeyMap: Record<string, ReportCategory> = {
    technical: "technical",
    fundamental: "fundamental",
    fund_flow: "fund_flow",
    risk: "risk",
    risk_management: "risk",
    market: "market",
    sentiment: "market",
    market_sentiment: "market",
    news: "news",
    team: "team",
    discussion: "team",
    chief: "team",
  };
  if (exactKeyMap[normalizedName]) {
    return exactKeyMap[normalizedName];
  }

  const text = [
    name,
    payload.agent_name,
    payload.agent_role,
    Array.isArray(payload.focus_areas) ? payload.focus_areas.join(" ") : "",
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (/risk_management|风险管理|风控|风险识别|风险量化/.test(text)) {
    return "risk";
  }
  if (/discussion|team|chief|团队|首席|综合研判|团队讨论/.test(text)) {
    return "team";
  }
  if (/fund[_\s-]?flow|资金面|资金流向|主力动向|流动性/.test(text)) {
    return "fund_flow";
  }
  if (/technical|技术|趋势|形态|均线|交易信号/.test(text)) {
    return "technical";
  }
  if (/fundamental|基本面|财务|估值|公司价值|成长性/.test(text)) {
    return "fundamental";
  }
  if (/news|新闻|事件|公告|舆情|资讯/.test(text)) {
    return "news";
  }
  if (/sentiment|市场情绪|情绪|热点|赚钱效应|热度/.test(text)) {
    return "market";
  }
  if (/(^|[\s_-])risk($|[\s_-])|风险/.test(text)) {
    return "risk";
  }
  return null;
}

function buildRawReportEntry(name: string, payload: unknown): RawReportEntry {
  const normalizedPayload = asRecord(payload);
  const rawContent = String(normalizedPayload.analysis || normalizedPayload.report || payload || "").trim();

  return {
    key: name,
    rawContent,
  };
}

function buildRawReportEntries(
  agentsResults: Record<string, unknown>,
  discussionResult?: unknown,
): Record<ReportCategory, RawReportEntry[]> {
  const entries: Record<ReportCategory, RawReportEntry[]> = {
    technical: [],
    fundamental: [],
    fund_flow: [],
    market: [],
    news: [],
    risk: [],
    team: [],
  };

  Object.entries(agentsResults).forEach(([name, payload]) => {
    const normalizedPayload = asRecord(payload);
    const category = detectReportCategory(name, normalizedPayload);
    if (!category) {
      return;
    }

    const entry = buildRawReportEntry(name, payload);
    entries[category].push(entry);
  });

  if (discussionResult) {
    const normalizedDiscussion = sanitizeDiscussionSpeakers(discussionResult);
    entries.team.push({
      key: "__discussion__",
      rawContent: String(normalizedDiscussion || "").trim(),
    });
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

function formatRangeMetric(min: unknown, max: unknown, fallback: unknown, emptyText = "暂无"): string {
  if (fallback !== null && fallback !== undefined && fallback !== "") {
    return formatMetric(fallback, emptyText);
  }
  const parts = [min, max].filter((item) => item !== null && item !== undefined && item !== "").map((item) => formatMetric(item, ""));
  return parts.length ? parts.join(" - ") : emptyText;
}

function getRatingLabel(record: { final_decision?: Record<string, unknown>; decision_label?: string; latest_decision_label?: string }): string {
  return formatMetric(
    asRecord(record.final_decision).rating || record.decision_label || record.latest_decision_label,
    "暂无",
  );
}

function getArchiveCategoryLabel(item: { is_in_portfolio?: boolean; linked_asset_status_label?: string }): ArchiveCategory {
  if (item.is_in_portfolio) {
    return "持仓";
  }
  if (item.linked_asset_status_label === "盯盘中") {
    return "关注";
  }
  return "研究池";
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

  const inlineNumberedMatches = Array.from(
    text.matchAll(
      /(?:^|[；;\n]\s*)((?:\d+[.)、]?|[①②③④⑤⑥⑦⑧⑨⑩]|[一二三四五六七八九十]+[、.]))\s*(.*?)(?=(?:[；;\n]\s*(?:\d+[.)、]?|[①②③④⑤⑥⑦⑧⑨⑩]|[一二三四五六七八九十]+[、.])\s*)|$)/gu,
    ),
  )
    .map((match) => cleanOrderedItem(`${match[1]} ${match[2]}`))
    .filter(Boolean);
  if (inlineNumberedMatches.length > 1) {
    return inlineNumberedMatches;
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
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className={styles.historyDetailPanel}>
      <div className={styles.historyDetailSummary}>{title}</div>
      <div className={styles.historyDetailPanelBody}>{children}</div>
    </section>
  );
}

function StockMemoryPanel({ symbol, archive, onUpdate }: { symbol: string, archive: MemoryArchive | null, onUpdate: () => void }) {
  const [editingProfile, setEditingProfile] = useState(false);
  const [profileText, setProfileText] = useState("");
  const [isRebuilding, setIsRebuilding] = useState(false);
  const { showError, showMessage } = usePageFeedback();

  if (!archive) {
    return null;
  }

  const handleEditProfile = () => {
    setProfileText(archive.long_term_profile?.macro_profile || "");
    setEditingProfile(true);
  };

  const handleSaveProfile = async () => {
    try {
      await apiFetch(`/api/memory/${symbol}/profile`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ macro_profile: profileText }),
      });
      showMessage("底色已更新");
      setEditingProfile(false);
      onUpdate();
    } catch (e) {
      showError(e instanceof Error ? e.message : "保存失败");
    }
  };

  const activeFacts = archive.factual_memories.filter((f) => !f.is_ignored);

  const handleRebuild = async () => {
    if (!window.confirm(`确定要为 ${symbol} 重建全部核心事实和底色记忆吗？此操作将发送请求给大模型，可能需要耗居几十秒或一分钟等较长时间。`)) {
      return;
    }
    setIsRebuilding(true);
    try {
      await apiFetch(`/api/memory/${symbol}/rebuild`, { method: "POST" });
      showMessage("记忆重建已完成");
      onUpdate();
    } catch (e) {
      showError(e instanceof Error ? e.message : "重建记忆失败");
    } finally {
      setIsRebuilding(false);
    }
  };

  return (
    <section className={styles.card}>
      <div className={styles.historyDetailSummary} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>多智能体记忆库</span>
        <button
          onClick={handleRebuild}
          disabled={isRebuilding}
          className={styles.secondaryButton}
          style={{ padding: '4px 10px', fontSize: '0.9rem' }}
          type="button"
        >
          {isRebuilding ? '正在重建...' : '重建记忆'}
        </button>
      </div>
      <div className={styles.historyDetailPanelBody} style={{ display: 'flex', flexDirection: 'column', gap: '1rem', marginTop: '1rem' }}>
        <div style={{ backgroundColor: '#2a2a2a', padding: '1rem', borderRadius: '8px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
            <strong>长期底色 </strong>
            {!editingProfile && <button onClick={handleEditProfile} className={styles.secondaryButton} style={{ padding: '2px 8px', fontSize: '0.8rem' }}>编辑</button>}
          </div>
          {editingProfile ? (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              <textarea
                value={profileText}
                onChange={e => setProfileText(e.target.value)}
                style={{ width: '100%', minHeight: '100px', backgroundColor: 'transparent', color: 'inherit', border: '1px solid #444', padding: '8px' }}
              />
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button onClick={handleSaveProfile} className={styles.primaryButton}>保存</button>
                <button onClick={() => setEditingProfile(false)} className={styles.secondaryButton}>取消</button>
              </div>
            </div>
          ) : (
            <div className={styles.historyListSummary} style={{ textAlign: "justify", fontStyle: archive.long_term_profile ? 'normal' : 'italic' }}>
              {archive.long_term_profile?.macro_profile || "暂无宏观底色画像。"}
            </div>
          )}
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <details style={{ backgroundColor: '#2a2a2a', padding: '1rem', borderRadius: '8px' }}>
            <summary style={{ fontWeight: 'bold', cursor: 'pointer', outline: 'none' }}>核心事实 (Top 5)</summary>
            <div style={{ marginTop: '0.5rem' }}>
              {activeFacts.length > 0 ? (
                <ul className={styles.historyListSummary} style={{ paddingLeft: '1.2rem', margin: 0, textAlign: "justify" }}>
                  {activeFacts.slice(0, 5).map((f) => (
                    <li key={f.id}>{f.fact_content} (权重: {f.importance_score})</li>
                  ))}
                </ul>
              ) : <div className={styles.muted}>暂无核心事实记忆。</div>}
            </div>
          </details>

          <details style={{ backgroundColor: '#2a2a2a', padding: '1rem', borderRadius: '8px' }}>
            <summary style={{ fontWeight: 'bold', cursor: 'pointer', outline: 'none' }}>近期决策</summary>
            <div style={{ marginTop: '0.5rem' }}>
              {archive.working_memories.length > 0 ? (
                <ul className={styles.historyListSummary} style={{ paddingLeft: '1.2rem', margin: 0, textAlign: "justify" }}>
                  {archive.working_memories.map((m, i) => (
                    <li key={i}><span className={styles.muted}>{formatDateTime(m.analysis_date)}</span>: {m.decision_summary}</li>
                  ))}
                </ul>
              ) : <div className={styles.muted}>暂无近期工作记忆。</div>}
            </div>
          </details>
        </div>
      </div>
    </section>
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
    () => reportTabs.filter((item) => entries[item.key].length),
    [entries],
  );

  useEffect(() => {
    if (entries[activeKey].length) {
      return;
    }
    const fallbackKey = availableTabs[0]?.key || "technical";
    setActiveKey(fallbackKey);
  }, [activeKey, availableTabs, entries]);

  const activeEntries = entries[activeKey];
  const activeTab = availableTabs.find((item) => item.key === activeKey) || availableTabs[0] || null;
  const tabsStyle = { "--nested-tab-count": availableTabs.length } as CSSProperties;

  if (!availableTabs.length) {
    return <div className={styles.muted}>暂无原始报告</div>;
  }

  return (
    <div className={styles.historyDetailContentStack}>
      <div className={styles.historyDetailTabsGrid} role="tablist" aria-label="原始报告分类" style={tabsStyle}>
        {availableTabs.map((item) => (
          <button
            aria-selected={item.key === activeKey}
            className={item.key === activeKey ? styles.nestedTabButtonActive : styles.nestedTabButton}
            key={item.key}
            onClick={() => setActiveKey(item.key)}
            role="tab"
            type="button"
          >
            {entries[item.key].length > 1 ? `${item.label} (${entries[item.key].length})` : item.label}
          </button>
        ))}
      </div>

      {activeEntries.length ? (
        <div className={styles.historyDetailContentStack}>
          {activeEntries.map((activeEntry) => (
            <div className={styles.reportWorkbenchPanel} key={`${activeKey}-${activeEntry.key}`}>
              <div className={styles.reportWorkbenchContent}>
                <MarkdownReport
                  className={styles.rawReportText}
                  content={activeEntry.rawContent || activeTab?.emptyText || "暂无正文"}
                  emptyText={activeTab?.emptyText || "暂无正文"}
                />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className={styles.muted}>{activeTab?.emptyText || "暂无正文"}</div>
      )}
    </div>
  );
}

export function HistoryPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  // States for Level 1 (Stock Summaries)
  const [stockSummaries, setStockSummaries] = useState<AnalysisHistoryStockSummary[]>([]);

  // States for Level 2 (Stock Profile)
  const [stockRecords, setStockRecords] = useState<AnalysisHistoryItem[]>([]);
  const [memoryArchive, setMemoryArchive] = useState<MemoryArchive | null>(null);

  // States for Level 3 (Record Detail)
  const [recordDetails, setRecordDetails] = useState<Record<number, AnalysisRecordDetail>>({});

  // Filters
  const [archiveCategory, setArchiveCategory] = useState<ArchiveCategory>("持仓");
  const [searchTerm, setSearchTerm] = useState("");
  const [deletingRecordId, setDeletingRecordId] = useState<number | null>(null);
  const { message, error, clear, showError, showMessage } = usePageFeedback();

  // Navigation state derived from URL
  const selectedSymbol = searchParams.get("symbol") || "";
  const selectedRecordId = Number(searchParams.get("recordId") || 0);
  const selectedRecord = selectedRecordId ? (recordDetails[selectedRecordId] ?? null) : null;
  const archiveCategoryCounts = useMemo(() => {
    const counts: Record<ArchiveCategory, number> = {
      持仓: 0,
      关注: 0,
      研究池: 0,
    };
    stockSummaries.forEach((item) => {
      counts[getArchiveCategoryLabel(item)] += 1;
    });
    return counts;
  }, [stockSummaries]);
  const visibleStockSummaries = useMemo(
    () => stockSummaries.filter((item) => getArchiveCategoryLabel(item) === archiveCategory),
    [archiveCategory, stockSummaries],
  );

  // Level 1: Load stock summaries
  const loadStockSummaries = async () => {
    const data = await apiFetch<AnalysisHistoryStockSummary[]>(
      `/api/analysis-history/stocks${buildQuery({
        search_term: searchTerm,
      })}`,
    );
    setStockSummaries(data);
  };

  // Level 2: Load stock records and memory
  const loadStockProfile = async (symbol: string) => {
    if (!symbol) return;
    try {
      const records = await apiFetch<AnalysisHistoryItem[]>(
        `/api/analysis-history/stocks/${encodeURIComponent(symbol)}`,
      );
      setStockRecords(records);

      try {
        const memArchive = await apiFetch<MemoryArchive>(`/api/memory/${encodeURIComponent(symbol)}`);
        setMemoryArchive(memArchive);
      } catch (memError) {
        console.warn("Memory not available for", symbol, memError);
        setMemoryArchive(null);
      }
    } catch (err) {
      showError(err instanceof Error ? err.message : "获取档案失败");
    }
  };

  // Level 3: Load record detail
  const loadDetail = async (recordId: number) => {
    if (!recordId) return;
    const data = await apiFetch<AnalysisRecordDetail>(`/api/analysis-history/${recordId}`);
    setRecordDetails((current) => ({ ...current, [recordId]: data }));
  };

  useEffect(() => {
    if (!selectedSymbol && !selectedRecordId) {
      void loadStockSummaries();
    }
  }, [searchTerm, selectedSymbol, selectedRecordId]);

  useEffect(() => {
    if (selectedSymbol && !selectedRecordId) {
      void loadStockProfile(selectedSymbol);
    }
  }, [selectedSymbol, selectedRecordId]);

  useEffect(() => {
    if (selectedRecordId && !recordDetails[selectedRecordId]) {
      void loadDetail(selectedRecordId);
    }
  }, [selectedRecordId, recordDetails]);

  // Navigation handlers
  const openStock = (symbol: string) => {
    setSearchParams({ symbol });
    clear();
  };

  const openRecord = (recordId: number, symbol?: string) => {
    if (symbol) {
      setSearchParams({ symbol, recordId: String(recordId) });
    } else if (selectedSymbol) {
      setSearchParams({ symbol: selectedSymbol, recordId: String(recordId) });
    } else {
      setSearchParams({ recordId: String(recordId) });
    }
    clear();
  };

  const goBackToStocks = () => {
    setSearchParams({});
    clear();
  };

  const goBackToProfile = () => {
    if (selectedSymbol) {
      setSearchParams({ symbol: selectedSymbol });
    } else {
      setSearchParams({});
    }
    clear();
  };

  const handleDelete = async (recordId: number) => {
    if (deletingRecordId === recordId) {
      return;
    }
    clear();
    const removedIndex = stockRecords.findIndex((item) => item.id === recordId);
    const removedRecord = stockRecords[removedIndex] ?? null;
    const removedDetail = recordDetails[recordId] ?? null;

    setDeletingRecordId(recordId);
    setStockRecords((current) => current.filter((item) => item.id !== recordId));
    setRecordDetails((current) => {
      const next = { ...current };
      delete next[recordId];
      return next;
    });
    try {
      await apiFetch(`/api/analysis-history/${recordId}`, { method: "DELETE" });
      if (selectedRecordId === recordId) {
        goBackToProfile();
      }
      if (!selectedSymbol) {
        void loadStockSummaries().catch(() => undefined);
      }
      showMessage("历史记录已删除");
    } catch (requestError) {
      if (removedRecord) {
        setStockRecords((current) => {
          if (current.some((item) => item.id === removedRecord.id)) return current;
          const next = [...current];
          next.splice(Math.max(0, Math.min(removedIndex, next.length)), 0, removedRecord);
          return next;
        });
      }
      if (removedDetail) {
        setRecordDetails((current) => ({ ...current, [recordId]: removedDetail }));
      }
      showError(requestError instanceof Error ? requestError.message : "删除失败");
    } finally {
      setDeletingRecordId((current) => current === recordId ? null : current);
    }
  };

  return (
    <PageFrame title="股票档案">
      <div className={selectedRecordId ? `${styles.stack} ${styles.historyDetailPageStack}` : styles.stack}>
        <PageFeedback error={error} message={message} />

        {/* LEVEL 3: Report Detail */}
        {selectedRecordId ? (
          <>
            <section className={styles.card}>
              <div className={styles.stack}>
                <div className={styles.actions}>
                  <button className={styles.secondaryButton} onClick={goBackToProfile} type="button">
                    {selectedSymbol ? '返回个股档案' : '返回股票档案'}
                  </button>
                </div>

                {selectedRecord ? (
                  <div className={styles.historyRecordTop}>
                    <div>
                      <strong className={styles.historyRecordTitle}>
                        {selectedRecord.stock_name || selectedRecord.symbol || "未知股票"}
                        {selectedRecord.symbol ? `（${selectedRecord.symbol}）` : ""}
                      </strong>
                      <p className={styles.historyMeta}>
                        {formatDateTime(selectedRecord.analysis_time_text, "暂无时间")} | {selectedRecord.portfolio_state_label || "未持仓"}
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className={styles.muted}>加载详情中...</div>
                )}
              </div>
            </section>

            {selectedRecord && (
              <section className={`${styles.stack} ${styles.historyDetailStack}`}>
                <HistoryDetailSection title="核心建议">
                  {renderOrderedContent(
                    asRecord(selectedRecord.final_decision).operation_advice || selectedRecord.summary,
                    "暂无核心建议",
                  )}
                </HistoryDetailSection>
                <HistoryDetailSection title="关键位置">
                  {renderOrderedContent([
                    `入场区间：${formatRangeMetric(asRecord(selectedRecord.final_decision).entry_min, asRecord(selectedRecord.final_decision).entry_max, asRecord(selectedRecord.final_decision).entry_range)}`,
                    `止盈位：${formatMetric(asRecord(selectedRecord.final_decision).take_profit)}`,
                    `止损位：${formatMetric(asRecord(selectedRecord.final_decision).stop_loss)}`,
                    `持有周期：${formatMetric(asRecord(selectedRecord.final_decision).holding_period)}`,
                    `当前状态：${selectedRecord.linked_asset_status_label || selectedRecord.portfolio_state_label || "暂无"}`,
                  ], "暂无关键位置")}
                </HistoryDetailSection>
                <HistoryDetailSection title="风险提示">
                  {renderOrderedContent(asRecord(selectedRecord.final_decision).risk_warning, "暂无风险提示")}
                </HistoryDetailSection>
                <HistoryDetailSection title="原始报告">
                  <RawReportWorkspace
                    agentsResults={selectedRecord.agents_results}
                    discussionResult={selectedRecord.discussion_result}
                  />
                </HistoryDetailSection>
              </section>
            )}
          </>
        ) : selectedSymbol ? (
          /* LEVEL 2: Stock Profile View */
          <>
            <section className={styles.card}>
              <div className={styles.stack}>
                <div className={styles.actions} style={{ justifyContent: "flex-start" }}>
                  <button className={styles.secondaryButton} onClick={goBackToStocks} type="button">
                    返回所有股票
                  </button>
                </div>
                <h2 style={{ margin: 0, textAlign: "center" }}>{stockRecords[0]?.stock_name || "加载中..."} ({selectedSymbol}) 个股档案</h2>
              </div>
            </section>

            <StockMemoryPanel symbol={selectedSymbol} archive={memoryArchive} onUpdate={() => loadStockProfile(selectedSymbol)} />

            <section className={styles.card}>
              <div className={styles.historyDetailSummary} style={{ marginBottom: '1rem' }}>档案记录 ({stockRecords.length})</div>
              <div className={styles.list}>
                {stockRecords.map((record) => (
                  <div className={styles.historyRecordCard} key={record.id}>
                    <div className={styles.historyRecordTop}>
                      <div>
                        <strong className={styles.historyRecordTitle}>
                          {formatDateTime(record.analysis_time_text, "暂无时间")}
                        </strong>
                        <p className={styles.historyMeta}>
                          {record.portfolio_state_label || "未持仓"} | 评级: {getRatingLabel(record)}
                        </p>
                      </div>
                      <div className={`${styles.historyActionRow} ${styles.historyListActionRow}`} style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                        <button className={styles.secondaryButton} onClick={() => openRecord(record.id, selectedSymbol)} type="button">
                          查看详情
                        </button>
                        <button
                          className={styles.dangerButton}
                          disabled={deletingRecordId === record.id}
                          onClick={() => void handleDelete(record.id)}
                          type="button"
                        >
                          {deletingRecordId === record.id ? "删除中..." : "删除记录"}
                        </button>
                      </div>
                    </div>
                    <div className={styles.historyListBody}>
                      <p className={styles.historyMeta} style={{ margin: 0 }}>
                        建议入场区间：{formatRangeMetric(asRecord(record.final_decision).entry_min, asRecord(record.final_decision).entry_max, asRecord(record.final_decision).entry_range)}
                      </p>
                      <p className={styles.historyMeta} style={{ margin: 0 }}>
                        目标止盈价格：{formatMetric(asRecord(record.final_decision).take_profit)}
                      </p>
                      <p className={styles.historyMeta} style={{ margin: 0 }}>
                        预期止损价格：{formatMetric(asRecord(record.final_decision).stop_loss)}
                      </p>
                    </div>
                  </div>
                ))}
                {stockRecords.length === 0 ? <div className={styles.muted}>暂无报告记录</div> : null}
              </div>
            </section>
          </>
        ) : (
          /* LEVEL 1: Stock List View */
          <>
            <section className={styles.card}>
              <div className={styles.formGrid}>
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
              <div className={styles.historyListMetrics} style={{ marginTop: "1rem", gap: "0.5rem", flexWrap: "wrap", overflowX: "visible", whiteSpace: "normal" }}>
                {(["持仓", "关注", "研究池"] as ArchiveCategory[]).map((item) => {
                  const active = archiveCategory === item;
                  return (
                    <button
                      key={item}
                      className={active ? styles.primaryButton : styles.secondaryButton}
                      onClick={() => setArchiveCategory(item)}
                      type="button"
                      style={{ flex: "1 1 140px", padding: "4px 10px", fontSize: "0.8rem", minWidth: 0 }}
                    >
                      {item}({archiveCategoryCounts[item]})
                    </button>
                  );
                })}
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.list}>
                {visibleStockSummaries.map((stock) => (
                  <div className={styles.historyRecordCard} key={stock.symbol}>
                    <div className={styles.historyRecordTop}>
                      <div>
                        <strong className={styles.historyRecordTitle}>
                          {stock.stock_name}（{stock.symbol}）
                        </strong>
                        <p className={styles.historyMeta}>
                          最近: {formatDateTime(stock.latest_analysis_date, "未知")} | 首次: {formatDateTime(stock.first_analysis_date, "未知")}
                        </p>
                      </div>
                    </div>

                    <div className={styles.historyListBody}>
                      <p className={styles.historyMeta} style={{ margin: 0 }}>
                        最近一次评级：{getRatingLabel(stock)}
                      </p>
                      <div className={styles.historyActionRow} style={{ justifyContent: "flex-start" }}>
                        <button className={styles.primaryButton} onClick={() => openStock(stock.symbol)} type="button">
                          进入股票档案
                        </button>
                      </div>
                    </div>
                  </div>
                ))}
                {visibleStockSummaries.length === 0 ? <div className={styles.muted}>暂无匹配股票</div> : null}
              </div>
            </section>
          </>
        )}
      </div>
    </PageFrame>
  );
}
