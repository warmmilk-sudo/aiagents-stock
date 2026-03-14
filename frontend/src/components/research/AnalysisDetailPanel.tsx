import { AnalysisActionButtons, type ActionPayload } from "./AnalysisActionButtons";
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
        <p className={styles.text}>{prettyText(finalDecision.operation_advice || record.summary)}</p>
      </section>

      <section className={styles.block}>
        <h3>关键位置</h3>
        <p className={styles.text}>
          进场区间:{" "}
          {metricValue(
            finalDecision.entry_range ||
              `${metricValue(finalDecision.entry_min)} - ${metricValue(finalDecision.entry_max)}`,
          )}
          {"\n"}止盈位: {metricValue(finalDecision.take_profit)}
          {"\n"}止损位: {metricValue(finalDecision.stop_loss)}
          {"\n"}持有周期: {metricValue(finalDecision.holding_period)}
          {"\n"}当前状态: {metricValue(record.linked_asset_status_label || record.portfolio_state_label)}
          {"\n"}账户: {metricValue(record.account_name)}
          {"\n"}分析时间: {metricValue(record.analysis_time_text)}
          {"\n"}周期: {metricValue(record.period)}
        </p>
      </section>

      <section className={styles.block}>
        <h3>风险提示</h3>
        <p className={styles.text}>{prettyText(finalDecision.risk_warning)}</p>
      </section>

      <details className={styles.details}>
        <summary>股票基础信息</summary>
        <div className={styles.detailsContent}>
          <pre className={styles.code}>{prettyText(stockInfo)}</pre>
        </div>
      </details>

      {Object.entries(agentsResults).map(([name, payload]) => (
        <details className={styles.details} key={name}>
          <summary>{name}</summary>
          <div className={styles.detailsContent}>
            <pre className={styles.code}>{prettyText(payload)}</pre>
          </div>
        </details>
      ))}

      {record.discussion_result ? (
        <details className={styles.details}>
          <summary>团队讨论</summary>
          <div className={styles.detailsContent}>
            <pre className={styles.code}>{prettyText(record.discussion_result)}</pre>
          </div>
        </details>
      ) : null}
    </div>
  );
}
