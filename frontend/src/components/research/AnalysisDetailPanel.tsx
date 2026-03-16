import { AnalysisActionButtons, type ActionPayload } from "./AnalysisActionButtons";
import { formatDateTime } from "../../lib/datetime";
import { AgentReportBrowser } from "./AgentReportBrowser";
import { FormattedReport } from "./FormattedReport";
import styles from "./ResearchPanels.module.scss";

export interface AnalysisRecordDetail {
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

function prettyText(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "暂无";
  }
  if (typeof value === "string") {
    return value;
  }
  return JSON.stringify(value, null, 2);
}

function metricValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "N/A";
  }
  return String(value);
}

function rangeValue(min: unknown, max: unknown, fallback: unknown): string {
  if (fallback !== null && fallback !== undefined && fallback !== "") {
    return metricValue(fallback);
  }
  const minText = min === null || min === undefined || min === "" ? "" : String(min);
  const maxText = max === null || max === undefined || max === "" ? "" : String(max);
  if (minText && maxText) {
    return `${minText} - ${maxText}`;
  }
  return minText || maxText || "N/A";
}

interface AnalysisDetailPanelProps {
  record: AnalysisRecordDetail;
  showPortfolioAction?: boolean;
}

export function AnalysisDetailPanel({
  record,
  showPortfolioAction = true,
}: AnalysisDetailPanelProps) {
  const finalDecision = record.final_decision ?? {};
  const stockInfo = record.stock_info ?? {};
  const agentsResults = record.agents_results ?? {};
  const locationMetrics = [
    {
      label: "进场区间",
      value: rangeValue(finalDecision.entry_min, finalDecision.entry_max, finalDecision.entry_range),
    },
    { label: "止盈位", value: metricValue(finalDecision.take_profit) },
    { label: "止损位", value: metricValue(finalDecision.stop_loss) },
    { label: "持有周期", value: metricValue(finalDecision.holding_period) },
    { label: "当前状态", value: metricValue(record.linked_asset_status_label || record.portfolio_state_label) },
    { label: "账户", value: metricValue(record.account_name) },
    { label: "分析时间", value: metricValue(formatDateTime(record.analysis_time_text, "N/A")) },
    { label: "周期", value: metricValue(record.period) },
  ];

  return (
    <div className={styles.contentGrid}>
      <div className={styles.headerGrid}>
        <div className={styles.metricCard}>
          <span>股票</span>
          <strong>{metricValue(record.stock_name || record.symbol)}</strong>
        </div>
        <div className={styles.metricCard}>
          <span>评级</span>
          <strong>{metricValue(finalDecision.rating)}</strong>
        </div>
        <div className={styles.metricCard}>
          <span>目标价</span>
          <strong>{metricValue(finalDecision.target_price)}</strong>
        </div>
        <div className={styles.metricCard}>
          <span>信心度</span>
          <strong>{metricValue(finalDecision.confidence_level)}</strong>
        </div>
      </div>

      <AnalysisActionButtons
        actionPayload={record.action_payload}
        isInPortfolio={Boolean(record.is_in_portfolio)}
        portfolioLabel={record.portfolio_action_label}
        recordId={record.id}
        showPortfolioAction={showPortfolioAction}
      />

      <section className={styles.block}>
        <h3>核心建议</h3>
        <FormattedReport content={finalDecision.operation_advice || record.summary} emptyText="暂无建议" />
      </section>

      <section className={styles.block}>
        <h3>关键位置</h3>
        <div className={styles.detailMetricGrid}>
          {locationMetrics.map((item) => (
            <div className={styles.detailMetricCell} key={item.label}>
              <span>{item.label}</span>
              <strong>{item.value}</strong>
            </div>
          ))}
        </div>
      </section>

      <section className={styles.block}>
        <h3>风险提示</h3>
        <FormattedReport content={finalDecision.risk_warning} emptyText="暂无风险提示" />
      </section>

      <details className={styles.details}>
        <summary>股票基础信息</summary>
        <div className={styles.detailsContent}>
          <pre className={styles.code}>{prettyText(stockInfo)}</pre>
        </div>
      </details>

      <AgentReportBrowser
        agentsResults={
          agentsResults && typeof agentsResults === "object" && !Array.isArray(agentsResults)
            ? (agentsResults as Record<string, unknown>)
            : {}
        }
        discussionResult={record.discussion_result}
      />
    </div>
  );
}
