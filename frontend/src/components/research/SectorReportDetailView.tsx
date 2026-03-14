import type { ReactNode } from "react";

import styles from "../../pages/ConsolePage.module.scss";
import { formatDateTime } from "../../lib/datetime";
import { FixedReportWorkspace, type FixedReportMap } from "./FixedReportWorkspace";

export interface SectorPredictionItem {
  sector?: string;
  direction?: string;
  reason?: string;
  confidence?: number;
  risk?: string;
  stage?: string;
  logic?: string;
  time_window?: string;
  advice?: string;
  score?: number;
  trend?: string;
  sustainability?: string;
}

export interface SectorStrategySummaryView {
  headline?: string;
  market_view?: string;
  key_opportunity?: string;
  major_risk?: string;
  strategy?: string;
  confidence_score?: number;
  risk_level?: string;
  market_outlook?: string;
}

export interface SectorStrategyPredictionGroups {
  long_short?: {
    bullish?: SectorPredictionItem[];
    neutral?: SectorPredictionItem[];
    bearish?: SectorPredictionItem[];
  };
  rotation?: {
    current_strong?: SectorPredictionItem[];
    potential?: SectorPredictionItem[];
    declining?: SectorPredictionItem[];
  };
  heat?: {
    hottest?: SectorPredictionItem[];
    heating?: SectorPredictionItem[];
    cooling?: SectorPredictionItem[];
  };
  raw_fallback_text?: string;
}

export type SectorStrategyRawReportMap = FixedReportMap;

export interface SectorStrategyWarningState {
  parse_warning?: string;
  language_warning?: string;
  missing_fields?: string[];
}

export interface SectorStrategyReportView {
  meta?: {
    timestamp?: string;
    from_cache?: boolean;
    cache_warning?: string;
    data_timestamp?: string;
  };
  summary?: SectorStrategySummaryView;
  predictions?: SectorStrategyPredictionGroups;
  market_snapshot?: {
    from_cache?: boolean;
    cache_warning?: string;
    data_timestamp?: string;
    market_overview?: {
      sh_index?: { close?: number; change_pct?: number };
      sz_index?: { close?: number; change_pct?: number };
      cyb_index?: { close?: number; change_pct?: number };
      up_count?: number;
      up_ratio?: number;
      limit_up?: number;
      limit_down?: number;
    };
    sectors_count?: number;
    concepts_count?: number;
  } | null;
  raw_reports?: Partial<SectorStrategyRawReportMap> | null;
  warnings?: SectorStrategyWarningState;
}

type ExportKind = "pdf" | "markdown";

function asText(value: unknown, fallback = "暂无"): string {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function formatConfidence(value: unknown): string {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `${Math.round(numeric)}分` : "0分";
}

function formatNumber(value: unknown, fractionDigits = 2, fallback = "暂无"): string {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? numeric.toLocaleString("zh-CN", { maximumFractionDigits: fractionDigits })
    : fallback;
}

function formatPercent(value: unknown): string {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `${numeric.toFixed(2)}%` : "暂无";
}

function splitSegments(values: Array<unknown>): string[] {
  return values
    .flatMap((value) => String(value || "").replace(/\r\n/g, "\n").split(/\n|[；;]+/u))
    .map((item) => item.replace(/^\s*(?:\d+[.)、]?|[①②③④⑤⑥⑦⑧⑨⑩]|[一二三四五六七八九十]+[、.])\s*/u, "").trim())
    .filter(Boolean);
}

function PredictionGroupCard({
  title,
  items,
  metaBuilder,
  detailBuilder,
}: {
  title: string;
  items?: SectorPredictionItem[];
  metaBuilder: (item: SectorPredictionItem) => string[];
  detailBuilder: (item: SectorPredictionItem) => string[];
}) {
  return (
    <div className={styles.strategyGroupCard}>
      <div className={styles.strategyGroupHeader}>
        <strong>{title}</strong>
        <span className={styles.helperText}>{items?.length ?? 0} 项</span>
      </div>

      {items?.length ? (
        <div className={styles.strategyEntryList}>
          {items.map((item, index) => (
            <div className={styles.strategyEntryItem} key={`${title}-${item.sector || "item"}-${index}`}>
              <strong>{asText(item.sector)}</strong>
              {metaBuilder(item).filter(Boolean).length ? (
                <div className={styles.strategyEntryMeta}>{metaBuilder(item).filter(Boolean).join(" | ")}</div>
              ) : null}
              {detailBuilder(item).filter(Boolean).map((detail, detailIndex) => (
                <div key={`${detail}-${detailIndex}`}>{detail}</div>
              ))}
            </div>
          ))}
        </div>
      ) : (
        <div className={styles.muted}>暂无数据</div>
      )}
    </div>
  );
}

function DetailSection({
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

interface SectorReportDetailViewProps {
  title: string;
  backLabel: string;
  onBack: () => void;
  onExport: (kind: ExportKind) => void;
  reportView?: SectorStrategyReportView | null;
}

export function SectorReportDetailView({
  title,
  backLabel,
  onBack,
  onExport,
  reportView,
}: SectorReportDetailViewProps) {
  if (!reportView) {
    return (
      <div className={`${styles.stack} ${styles.historyDetailPageStack}`}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.secondaryButton} onClick={onBack} type="button">
              {backLabel}
            </button>
          </div>
        </section>

        <section className={styles.card}>
          <h2>{title}</h2>
          <div className={styles.muted}>加载报告中...</div>
        </section>
      </div>
    );
  }

  const summary = reportView?.summary;
  const predictions = reportView?.predictions;
  const marketSnapshot = reportView?.market_snapshot;
  const warnings = reportView?.warnings;
  const overview = marketSnapshot?.market_overview;
  const headlineSegments = splitSegments([summary?.headline, summary?.market_view, summary?.key_opportunity]);
  const dataTimestamp = reportView?.meta?.data_timestamp;

  return (
    <div className={`${styles.stack} ${styles.historyDetailPageStack}`}>
      <section className={styles.card}>
        <div className={styles.actions}>
          <button className={styles.secondaryButton} onClick={onBack} type="button">
            {backLabel}
          </button>
        </div>
      </section>

      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <h2>{title}</h2>
            <p className={styles.helperText}>
              {formatDateTime(reportView?.meta?.timestamp, "暂无时间")}
              {dataTimestamp ? ` | 数据时间 ${formatDateTime(dataTimestamp, dataTimestamp)}` : ""}
            </p>
          </div>

          <div className={styles.actions}>
            <button className={styles.secondaryButton} onClick={() => onExport("markdown")} type="button">
              导出 Markdown
            </button>
            <button className={styles.secondaryButton} onClick={() => onExport("pdf")} type="button">
              导出 PDF
            </button>
          </div>
        </div>

        <div className={styles.strategySummaryGrid}>
          <div className={styles.historySummaryCell}>
            <span>标题</span>
            <strong>{asText(summary?.headline || title)}</strong>
          </div>
          <div className={styles.historySummaryCell}>
            <span>风险等级</span>
            <strong>{asText(summary?.risk_level)}</strong>
          </div>
          <div className={styles.historySummaryCell}>
            <span>市场展望</span>
            <strong>{asText(summary?.market_outlook)}</strong>
          </div>
          <div className={styles.historySummaryCell}>
            <span>置信度</span>
            <strong>{formatConfidence(summary?.confidence_score)}</strong>
          </div>
        </div>

        {reportView?.meta?.cache_warning ? (
          <div className={`${styles.noticeCard} ${styles.noticeWarning}`}>
            <strong>缓存提示</strong>
            <div>{reportView.meta.cache_warning}</div>
          </div>
        ) : null}

        {warnings?.parse_warning ? (
          <div className={`${styles.noticeCard} ${styles.noticeDanger}`}>
            <strong>解析提示</strong>
            <div>{warnings.parse_warning}</div>
          </div>
        ) : null}

        {warnings?.language_warning ? (
          <div className={`${styles.noticeCard} ${styles.noticeInfo}`}>
            <strong>语言提示</strong>
            <div>以下字段已按中文展示要求回退处理：{warnings.language_warning}</div>
          </div>
        ) : null}

        {warnings?.missing_fields?.length ? (
          <div className={`${styles.noticeCard} ${styles.noticeWarning}`}>
            <strong>字段补齐</strong>
            <div>模型未完整返回以下字段，已使用默认值补齐：{warnings.missing_fields.join("、")}</div>
          </div>
        ) : null}
      </section>

      <section className={`${styles.stack} ${styles.historyDetailStack}`}>
        <DetailSection title="核心结论">
          {headlineSegments.length ? (
            <ol className={styles.detailOrderedList}>
              {headlineSegments.map((item, index) => (
                <li key={`${item}-${index}`}>{item}</li>
              ))}
            </ol>
          ) : (
            <div className={styles.muted}>暂无核心结论</div>
          )}
        </DetailSection>

        <DetailSection title="板块多空">
          <div className={styles.strategyGroupGrid}>
            <PredictionGroupCard
              detailBuilder={(item) => [item.reason || "", item.risk ? `风险提示：${item.risk}` : ""]}
              items={predictions?.long_short?.bullish}
              metaBuilder={(item) => [item.direction || "看多", formatConfidence(item.confidence)]}
              title="看多板块"
            />
            <PredictionGroupCard
              detailBuilder={(item) => [item.reason || "", item.risk ? `观察点：${item.risk}` : ""]}
              items={predictions?.long_short?.neutral}
              metaBuilder={(item) => [item.direction || "中性", formatConfidence(item.confidence)]}
              title="中性观察"
            />
            <PredictionGroupCard
              detailBuilder={(item) => [item.reason || "", item.risk ? `风险提示：${item.risk}` : ""]}
              items={predictions?.long_short?.bearish}
              metaBuilder={(item) => [item.direction || "看空", formatConfidence(item.confidence)]}
              title="风险板块"
            />
          </div>
        </DetailSection>

        <DetailSection title="轮动机会">
          <div className={styles.strategyGroupGrid}>
            <PredictionGroupCard
              detailBuilder={(item) => [item.logic || "", item.advice ? `操作建议：${item.advice}` : ""]}
              items={predictions?.rotation?.current_strong}
              metaBuilder={(item) => [item.stage || "强势", item.time_window || "暂无周期"]}
              title="当前强势"
            />
            <PredictionGroupCard
              detailBuilder={(item) => [item.logic || "", item.advice ? `操作建议：${item.advice}` : ""]}
              items={predictions?.rotation?.potential}
              metaBuilder={(item) => [item.stage || "潜力", item.time_window || "暂无周期"]}
              title="潜力接力"
            />
            <PredictionGroupCard
              detailBuilder={(item) => [item.logic || "", item.advice ? `操作建议：${item.advice}` : ""]}
              items={predictions?.rotation?.declining}
              metaBuilder={(item) => [item.stage || "衰退", item.time_window || "暂无周期"]}
              title="衰退方向"
            />
          </div>
        </DetailSection>

        <DetailSection title="热度观察">
          <div className={styles.strategyGroupGrid}>
            <PredictionGroupCard
              detailBuilder={(item) => [item.sustainability ? `持续性：${item.sustainability}` : ""]}
              items={predictions?.heat?.hottest}
              metaBuilder={(item) => [`热度 ${formatNumber(item.score, 0, "0")}`, item.trend || "暂无趋势"]}
              title="最热板块"
            />
            <PredictionGroupCard
              detailBuilder={(item) => [item.sustainability ? `持续性：${item.sustainability}` : ""]}
              items={predictions?.heat?.heating}
              metaBuilder={(item) => [`热度 ${formatNumber(item.score, 0, "0")}`, item.trend || "暂无趋势"]}
              title="升温板块"
            />
            <PredictionGroupCard
              detailBuilder={(item) => [item.sustainability ? `持续性：${item.sustainability}` : ""]}
              items={predictions?.heat?.cooling}
              metaBuilder={(item) => [`热度 ${formatNumber(item.score, 0, "0")}`, item.trend || "暂无趋势"]}
              title="降温板块"
            />
          </div>
        </DetailSection>

        <DetailSection title="风险与策略">
          <div className={styles.strategySummaryGrid}>
            <div className={styles.historySummaryCell}>
              <span>主要风险</span>
              <strong>{asText(summary?.major_risk)}</strong>
            </div>
            <div className={styles.historySummaryCell}>
              <span>整体策略</span>
              <strong>{asText(summary?.strategy)}</strong>
            </div>
          </div>

          {predictions?.raw_fallback_text ? (
            <div className={`${styles.noticeCard} ${styles.noticeInfo}`}>
              <strong>原始预测文本</strong>
              <div className={styles.code}>{predictions.raw_fallback_text}</div>
            </div>
          ) : null}
        </DetailSection>

        {marketSnapshot ? (
          <DetailSection title="市场快照">
            <div className={styles.strategySummaryGrid}>
              <div className={styles.historySummaryCell}>
                <span>上证指数</span>
                <strong>
                  {formatNumber(overview?.sh_index?.close)} | {formatPercent(overview?.sh_index?.change_pct)}
                </strong>
              </div>
              <div className={styles.historySummaryCell}>
                <span>市场上涨家数</span>
                <strong>
                  {formatNumber(overview?.up_count, 0)} | 占比 {formatPercent(overview?.up_ratio)}
                </strong>
              </div>
              <div className={styles.historySummaryCell}>
                <span>行业板块数</span>
                <strong>{formatNumber(marketSnapshot.sectors_count, 0)}</strong>
              </div>
              <div className={styles.historySummaryCell}>
                <span>概念板块数</span>
                <strong>{formatNumber(marketSnapshot.concepts_count, 0)}</strong>
              </div>
              <div className={styles.historySummaryCell}>
                <span>涨停 / 跌停</span>
                <strong>
                  {formatNumber(overview?.limit_up, 0)} / {formatNumber(overview?.limit_down, 0)}
                </strong>
              </div>
              <div className={styles.historySummaryCell}>
                <span>数据状态</span>
                <strong>{marketSnapshot.from_cache ? "缓存数据" : "实时数据"}</strong>
              </div>
            </div>

            {marketSnapshot.cache_warning ? (
              <div className={`${styles.noticeCard} ${styles.noticeWarning}`}>
                <strong>市场数据说明</strong>
                <div>{marketSnapshot.cache_warning}</div>
              </div>
            ) : null}
          </DetailSection>
        ) : null}

        <DetailSection title="原始报告">
          <FixedReportWorkspace reports={reportView?.raw_reports || null} />
        </DetailSection>
      </section>
    </div>
  );
}
