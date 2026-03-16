import { useMemo, useState, type CSSProperties } from "react";

import {
  ArcElement,
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  RadialLinearScale,
  Tooltip,
} from "chart.js";
import { Bar, Pie, Radar } from "react-chartjs-2";

import styles from "../../pages/ConsolePage.module.scss";
import { FormattedReport, splitReportSections } from "./FormattedReport";
import { ReportWorkspace, type ReportWorkspaceEntry } from "./ReportWorkspace";

ChartJS.register(ArcElement, BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, RadialLinearScale, Tooltip);

type ExportKind = "pdf" | "markdown";
type LonghubangSectionKey = "summary" | "recommended" | "ranking" | "agents" | "overview";

interface LonghubangAgentDetail {
  analysis?: string;
  agent_role?: string;
  timestamp?: string;
}

interface LonghubangResultLike {
  timestamp?: string;
  data_info?: {
    total_records?: number;
    total_stocks?: number;
    total_youzi?: number;
    data_source?: string;
    update_hint?: string;
    summary?: {
      total_buy_amount?: number;
      total_sell_amount?: number;
      total_net_inflow?: number;
      top_youzi?: Record<string, number>;
      top_stocks?: Array<{ code?: string; name?: string; net_inflow?: number }>;
      hot_concepts?: Record<string, number>;
    };
  };
  final_report?: { title?: string; summary?: string };
  recommended_stocks?: Array<{
    rank?: number;
    code?: string;
    name?: string;
    net_inflow?: number;
    reason?: string;
    confidence?: string;
    hold_period?: string;
  }>;
  agents_analysis?: Record<string, LonghubangAgentDetail>;
  scoring_ranking?: Array<{
    排名?: number;
    股票名称?: string;
    股票代码?: string;
    综合评分?: number;
    资金含金量?: number;
    净买入额?: number;
    卖出压力?: number;
    机构共振?: number;
    加分项?: number;
    净流入?: number;
  }>;
}

interface LonghubangReportDetailViewProps {
  result?: LonghubangResultLike | null;
  onExport: (kind: ExportKind) => void;
}

const AGENT_LABELS: Array<[string, string]> = [
  ["chief", "综合"],
  ["youzi", "游资"],
  ["stock", "个股"],
  ["theme", "题材"],
  ["risk", "风控"],
];

function asText(value: unknown, fallback = "暂无"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function asNumber(value: unknown): number | null {
  const result = Number(String(value ?? "").replace(/,/g, "").trim());
  return Number.isFinite(result) ? result : null;
}

function numberText(value: unknown, digits = 2): string {
  const result = asNumber(value);
  return result === null ? "暂无" : result.toFixed(digits);
}

function currencyText(value: unknown): string {
  const result = asNumber(value);
  return result === null ? "暂无" : result.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
}

function integerText(value: unknown): string {
  const result = asNumber(value);
  return result === null ? "暂无" : String(Math.round(result));
}

function normalizeSymbol(value: unknown): string {
  const text = asText(value, "").trim().toUpperCase();
  if (!text) {
    return "";
  }
  return text.includes(".") ? text.split(".")[0] : text;
}

export function LonghubangReportDetailView({
  result,
  onExport,
}: LonghubangReportDetailViewProps) {
  const [activeSection, setActiveSection] = useState<LonghubangSectionKey>("summary");
  const sectionTabsStyle = { "--nested-tab-count": 5 } as CSSProperties;

  const scoringRows = useMemo(
    () =>
      (result?.scoring_ranking ?? []).map((item) => ({
        ...item,
        排名: asNumber(item.排名) ?? 0,
        综合评分: asNumber(item.综合评分) ?? 0,
        资金含金量: asNumber(item.资金含金量) ?? 0,
        净买入额: asNumber(item.净买入额) ?? 0,
        卖出压力: asNumber(item.卖出压力) ?? 0,
        机构共振: asNumber(item.机构共振) ?? 0,
        加分项: asNumber(item.加分项) ?? 0,
        净流入: asNumber(item.净流入) ?? 0,
      })),
    [result?.scoring_ranking],
  );

  const summary = result?.data_info?.summary;
  const topStocks = (summary?.top_stocks ?? []).slice(0, 10);
  const topYouzi = Object.entries(summary?.top_youzi ?? {}).slice(0, 10);
  const hotConcepts = Object.entries(summary?.hot_concepts ?? {}).slice(0, 15);
  const chiefSections = splitReportSections(result?.agents_analysis?.chief?.analysis);

  const scoreBarData = useMemo(
    () => ({
      labels: scoringRows.slice(0, 10).map((item) => asText(item.股票名称)),
      datasets: [{ label: "综合评分", data: scoringRows.slice(0, 10).map((item) => item.综合评分 ?? 0), backgroundColor: "#c46b3d", borderRadius: 10 }],
    }),
    [scoringRows],
  );

  const radarData = useMemo(
    () => ({
      labels: ["资金含金量", "净买入额", "卖出压力", "机构共振", "加分项"],
      datasets: scoringRows.slice(0, 5).map((item, index) => ({
        label: asText(item.股票名称, `TOP${index + 1}`),
        data: [((item.资金含金量 ?? 0) / 30) * 100, ((item.净买入额 ?? 0) / 25) * 100, ((item.卖出压力 ?? 0) / 20) * 100, ((item.机构共振 ?? 0) / 15) * 100, ((item.加分项 ?? 0) / 10) * 100],
        backgroundColor: `rgba(${80 + index * 25}, ${110 + index * 15}, ${140 + index * 10}, 0.15)`,
        borderColor: `rgba(${80 + index * 25}, ${110 + index * 15}, ${140 + index * 10}, 0.95)`,
      })),
    }),
    [scoringRows],
  );

  const topStocksChart = useMemo(
    () => ({
      labels: topStocks.map((item) => asText(item.name)),
      datasets: [{ label: "净流入金额", data: topStocks.map((item) => asNumber(item.net_inflow) ?? 0), backgroundColor: "#3b7ea1", borderRadius: 10 }],
    }),
    [topStocks],
  );

  const hotConceptChart = useMemo(
    () => ({
      labels: hotConcepts.map(([name]) => name),
      datasets: [{ label: "出现次数", data: hotConcepts.map(([, count]) => asNumber(count) ?? 0), backgroundColor: ["#c46b3d", "#3b7ea1", "#5f8b4c", "#d2a03d", "#8b5d7d", "#7d6b5d", "#557f8d", "#9b6e3f", "#4f6f6c", "#b35a4c", "#6b8e23", "#708090", "#c08497", "#8d99ae", "#52796f"] }],
    }),
    [hotConcepts],
  );

  const reportEntries = useMemo<ReportWorkspaceEntry[]>(
    () =>
      AGENT_LABELS.map(([key, label]) => {
        const report = result?.agents_analysis?.[key];
        const sections = splitReportSections(report?.analysis);
        return {
          key,
          label,
          title: key === "chief" ? "首席策略师综合研判" : label,
          role: report?.agent_role || "",
          timestamp: report?.timestamp || result?.timestamp || "",
          body: sections.body || report?.analysis || "",
          reasoning: sections.reasoning,
        };
      }).filter((item) => item.body || item.reasoning),
    [result?.agents_analysis, result?.timestamp],
  );

  return (
    <>
      <section className={styles.card}>
        <div className={styles.reportHeaderStack}>
          <div className={styles.reportExportGrid}>
            <button className={styles.secondaryButton} onClick={() => onExport("markdown")} type="button">
              导出 Markdown
            </button>
            <button className={styles.secondaryButton} onClick={() => onExport("pdf")} type="button">
              导出 PDF
            </button>
          </div>
        </div>
      </section>

      <section className={styles.card}>
        <p className={styles.helperText}>
          {asText(result?.timestamp, "暂无时间")}
          {result?.data_info?.data_source ? ` | 数据来源 ${result.data_info.data_source}` : ""}
          {result?.data_info?.update_hint ? ` | ${result.data_info.update_hint}` : ""}
        </p>
        <div className={styles.detailMetricGrid}>
          <div className={styles.historySummaryCell}>
            <span>龙虎榜记录</span>
            <strong>{result?.data_info?.total_records ?? 0}</strong>
          </div>
          <div className={styles.historySummaryCell}>
            <span>涉及股票</span>
            <strong>{result?.data_info?.total_stocks ?? 0}</strong>
          </div>
          <div className={styles.historySummaryCell}>
            <span>涉及游资</span>
            <strong>{result?.data_info?.total_youzi ?? 0}</strong>
          </div>
          <div className={styles.historySummaryCell}>
            <span>推荐股票</span>
            <strong>{result?.recommended_stocks?.length ?? 0}</strong>
          </div>
        </div>
        {result?.final_report?.summary ? (
          <div className={`${styles.noticeCard} ${styles.noticeInfo}`}>
            <strong>{asText(result.final_report.title, "智瞰龙虎榜综合分析报告")}</strong>
            <div>{result.final_report.summary}</div>
          </div>
        ) : null}
      </section>

      <section className={styles.card}>
        <div className={styles.sectionControlStack}>
          <div className={styles.historyDetailTabs} style={sectionTabsStyle}>
            <button
              className={activeSection === "summary" ? styles.nestedTabButtonActive : styles.nestedTabButton}
              onClick={() => setActiveSection("summary")}
              type="button"
            >
              综合结论
            </button>
            <button
              className={activeSection === "recommended" ? styles.nestedTabButtonActive : styles.nestedTabButton}
              onClick={() => setActiveSection("recommended")}
              type="button"
            >
              推荐股票
            </button>
            <button
              className={activeSection === "ranking" ? styles.nestedTabButtonActive : styles.nestedTabButton}
              onClick={() => setActiveSection("ranking")}
              type="button"
            >
              评分排名
            </button>
            <button
              className={activeSection === "agents" ? styles.nestedTabButtonActive : styles.nestedTabButton}
              onClick={() => setActiveSection("agents")}
              type="button"
            >
              分析师报告
            </button>
            <button
              className={activeSection === "overview" ? styles.nestedTabButtonActive : styles.nestedTabButton}
              onClick={() => setActiveSection("overview")}
              type="button"
            >
              数据概览
            </button>
          </div>
          <div className={styles.sectionDivider} />

          {activeSection === "summary" ? (
            <div className={styles.sectionControlStack}>
              {result?.final_report?.summary ? <div>{result.final_report.summary}</div> : null}
              <FormattedReport content={chiefSections.body || result?.agents_analysis?.chief?.analysis} emptyText="暂无综合结论" />
              {chiefSections.reasoning ? (
                <details className={styles.historyDetailPanel}>
                  <summary className={styles.historyDetailSummary}>推理过程</summary>
                  <div className={styles.historyDetailPanelBody}>
                    <FormattedReport content={chiefSections.reasoning} emptyText="暂无推理过程" />
                  </div>
                </details>
              ) : null}
            </div>
          ) : null}

          {activeSection === "recommended" ? (
            result?.recommended_stocks?.length ? (
              <div className={styles.strategyEntryList}>
                {result.recommended_stocks.map((item, index) => (
                  <div className={styles.strategyEntryItem} key={`${normalizeSymbol(item.code) || "recommended"}-${index}`}>
                    <strong>
                      第 {item.rank ?? index + 1} 名 · {asText(item.name)} ({asText(item.code)})
                    </strong>
                    <div className={styles.strategyEntryMeta}>
                      净流入 {currencyText(item.net_inflow)} | 信心度 {asText(item.confidence)} | 持有周期 {asText(item.hold_period)}
                    </div>
                    <div>{asText(item.reason, "暂无推荐理由")}</div>
                  </div>
                ))}
              </div>
            ) : (
              <div className={styles.muted}>暂无推荐股票</div>
            )
          ) : null}

          {activeSection === "ranking" ? (
            scoringRows.length ? (
              <div className={styles.sectionControlStack}>
                <div className={styles.grid}>
                  <div className={`${styles.card} ${styles.span6}`}>
                    <h3>TOP10 综合评分</h3>
                    <div className={styles.chartWrap}>
                      <Bar data={scoreBarData} options={{ responsive: true, plugins: { legend: { display: false } } }} />
                    </div>
                  </div>
                  <div className={`${styles.card} ${styles.span6}`}>
                    <h3>TOP5 五维评分</h3>
                    <div className={styles.chartWrap}>
                      <Radar data={radarData} options={{ responsive: true, scales: { r: { beginAtZero: true, max: 100 } } }} />
                    </div>
                  </div>
                </div>
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>排名</th>
                        <th>股票</th>
                        <th>综合评分</th>
                        <th>资金含金量</th>
                        <th>净买入额</th>
                        <th>卖出压力</th>
                        <th>机构共振</th>
                        <th>加分项</th>
                        <th>净流入</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scoringRows.map((item, index) => (
                        <tr key={`${normalizeSymbol(item.股票代码) || "score"}-${index}`}>
                          <td>{integerText(item.排名)}</td>
                          <td>
                            {asText(item.股票名称)} ({asText(item.股票代码)})
                          </td>
                          <td>{numberText(item.综合评分, 1)}</td>
                          <td>{numberText(item.资金含金量, 1)}</td>
                          <td>{numberText(item.净买入额, 1)}</td>
                          <td>{numberText(item.卖出压力, 1)}</td>
                          <td>{numberText(item.机构共振, 1)}</td>
                          <td>{numberText(item.加分项, 1)}</td>
                          <td>{currencyText(item.净流入)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : (
              <div className={styles.muted}>暂无评分排名</div>
            )
          ) : null}

          {activeSection === "agents" ? (
            <ReportWorkspace
              ariaLabel="智瞰龙虎分析师报告"
              emptyText="暂无分析师报告"
              entries={reportEntries}
            />
          ) : null}

          {activeSection === "overview" ? (
            <div className={styles.sectionControlStack}>
              <div className={styles.strategySummaryGrid}>
                <div className={styles.historySummaryCell}>
                  <span>总买入金额</span>
                  <strong>{currencyText(summary?.total_buy_amount)}</strong>
                </div>
                <div className={styles.historySummaryCell}>
                  <span>总卖出金额</span>
                  <strong>{currencyText(summary?.total_sell_amount)}</strong>
                </div>
                <div className={styles.historySummaryCell}>
                  <span>净流入金额</span>
                  <strong>{currencyText(summary?.total_net_inflow)}</strong>
                </div>
              </div>
              <div className={styles.grid}>
                <div className={`${styles.card} ${styles.span6}`}>
                  <h3>TOP10 股票资金净流入</h3>
                  {topStocks.length ? (
                    <div className={styles.chartWrap}>
                      <Bar data={topStocksChart} options={{ responsive: true, plugins: { legend: { display: false } } }} />
                    </div>
                  ) : (
                    <div className={styles.muted}>暂无净流入图表数据</div>
                  )}
                </div>
                <div className={`${styles.card} ${styles.span6}`}>
                  <h3>热门概念分布</h3>
                  {hotConcepts.length ? (
                    <div className={styles.chartWrap}>
                      <Pie data={hotConceptChart} options={{ responsive: true }} />
                    </div>
                  ) : (
                    <div className={styles.muted}>暂无热门概念图表数据</div>
                  )}
                </div>
              </div>
              <div className={styles.grid}>
                <div className={`${styles.card} ${styles.span6}`}>
                  <h3>活跃游资 TOP10</h3>
                  <div className={styles.tableWrap}>
                    <table className={styles.table}>
                      <thead>
                        <tr>
                          <th>排名</th>
                          <th>游资名称</th>
                          <th>净流入金额</th>
                        </tr>
                      </thead>
                      <tbody>
                        {topYouzi.map(([name, amount], index) => (
                          <tr key={`${name}-${index}`}>
                            <td>{index + 1}</td>
                            <td>{name}</td>
                            <td>{currencyText(amount)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
                <div className={`${styles.card} ${styles.span6}`}>
                  <h3>资金净流入 TOP20 股票</h3>
                  <div className={styles.tableWrap}>
                    <table className={styles.table}>
                      <thead>
                        <tr>
                          <th>股票代码</th>
                          <th>股票名称</th>
                          <th>净流入金额</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(summary?.top_stocks ?? []).slice(0, 20).map((item, index) => (
                          <tr key={`${normalizeSymbol(item.code) || "stock"}-${index}`}>
                            <td>{asText(item.code)}</td>
                            <td>{asText(item.name)}</td>
                            <td>{currencyText(item.net_inflow)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          ) : null}
        </div>
      </section>
    </>
  );
}
