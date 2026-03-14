import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import {
  AnalysisDetailPanel,
  type AnalysisRecordDetail,
} from "../../components/research/AnalysisDetailPanel";
import { AnalysisActionButtons } from "../../components/research/AnalysisActionButtons";
import { apiFetch, buildQuery } from "../../lib/api";
import styles from "../ConsolePage.module.scss";

interface AnalysisHistoryItem extends AnalysisRecordDetail {
  id: number;
  symbol: string;
  stock_name: string;
  analysis_source_label?: string;
}

type SectionKey = "list" | "detail";

const sectionTabs = [
  { key: "list", label: "历史记录" },
  { key: "detail", label: "记录详情" },
];

export function HistoryPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [records, setRecords] = useState<AnalysisHistoryItem[]>([]);
  const [selectedRecord, setSelectedRecord] = useState<AnalysisRecordDetail | null>(null);
  const [portfolioState, setPortfolioState] = useState("全部");
  const [accountName, setAccountName] = useState("全部账户");
  const [searchTerm, setSearchTerm] = useState("");
  const [section, setSection] = useState<SectionKey>("list");

  const selectedRecordId = Number(searchParams.get("recordId") || 0);

  const loadList = async () => {
    const data = await apiFetch<AnalysisHistoryItem[]>(
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
      setSelectedRecord(null);
      return;
    }
    const data = await apiFetch<AnalysisRecordDetail>(`/api/analysis-history/${recordId}`);
    setSelectedRecord(data);
  };

  useEffect(() => {
    void loadList();
  }, [portfolioState, accountName, searchTerm]);

  useEffect(() => {
    if (selectedRecordId) {
      setSection("detail");
      void loadDetail(selectedRecordId);
    } else {
      setSelectedRecord(null);
      setSection("list");
    }
  }, [selectedRecordId]);

  const accounts = useMemo(() => {
    const set = new Set<string>(["全部账户"]);
    records.forEach((item) => set.add(item.account_name || "默认账户"));
    return Array.from(set);
  }, [records]);

  const handleDelete = async (recordId: number) => {
    await apiFetch(`/api/analysis-history/${recordId}`, { method: "DELETE" });
    if (selectedRecordId === recordId) {
      setSearchParams({});
    }
    await loadList();
  };

  return (
    <PageFrame
      actions={<StatusBadge label={`记录 ${records.length}`} tone="default" />}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      sectionTabs={sectionTabs}
      title="分析历史"
    >
      <div className={styles.stack}>
        {section === "list" ? (
          <>
            <section className={styles.card}>
              <div className={styles.formGrid}>
                <div className={styles.field}>
                  <label htmlFor="portfolioState">持仓状态</label>
                  <select id="portfolioState" value={portfolioState} onChange={(event) => setPortfolioState(event.target.value)}>
                    <option value="全部">全部</option>
                    <option value="在持仓">在持仓</option>
                    <option value="未持仓">未持仓</option>
                  </select>
                </div>
                <div className={styles.field}>
                  <label htmlFor="accountName">账户</label>
                  <select id="accountName" value={accountName} onChange={(event) => setAccountName(event.target.value)}>
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
              <h2>历史记录</h2>
              <div className={styles.list}>
                {records.map((record) => (
                  <div className={styles.listItem} key={record.id}>
                    <strong>
                      {record.stock_name} ({record.symbol})
                    </strong>
                    <p className={styles.muted}>
                      {record.analysis_time_text || "暂无时间"} | {record.analysis_source_label || "历史分析"} |{" "}
                      {record.portfolio_state_label || "未持仓"}
                    </p>
                    <p>{record.summary || "暂无摘要"}</p>
                    <div className={styles.actions}>
                      <button
                        className={styles.secondaryButton}
                        onClick={() => setSearchParams({ recordId: String(record.id) })}
                        type="button"
                      >
                        查看详情
                      </button>
                      <AnalysisActionButtons
                        actionPayload={record.action_payload}
                        isInPortfolio={Boolean(record.is_in_portfolio)}
                        portfolioLabel={record.portfolio_action_label}
                        showPortfolioAction={false}
                      />
                      <button className={styles.dangerButton} onClick={() => void handleDelete(record.id)} type="button">
                        删除
                      </button>
                    </div>
                  </div>
                ))}
                {records.length === 0 ? <div className={styles.muted}>暂无匹配记录</div> : null}
              </div>
            </section>
          </>
        ) : null}

        {section === "detail" ? (
          <section className={styles.card}>
            <h2>记录详情</h2>
            {selectedRecord ? (
              <AnalysisDetailPanel record={selectedRecord} showPortfolioAction={false} />
            ) : (
              <div className={styles.muted}>请选择一条历史记录查看详情。</div>
            )}
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
