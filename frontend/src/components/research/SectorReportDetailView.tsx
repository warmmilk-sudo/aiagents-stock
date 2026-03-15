import { useState, type ReactNode } from "react";

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
type ViewSectionKey = "long_short" | "rotation" | "heat" | "raw";

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
  const normalizeSegment = (value: string): string =>
    value
      .replace(/\s+/g, " ")
      .replace(/[；;。.!！？、,，:：]/g, "")
      .trim()
      .toLowerCase();

  const seen = new Set<string>();
  const results: string[] = [];

  values
    .flatMap((value) => String(value || "").replace(/\r\n/g, "\n").split(/\n|[；;]+/u))
    .map((item) => item.replace(/^\s*(?:\d+[.)、]?|[①②③④⑤⑥⑦⑧⑨⑩]|[一二三四五六七八九十]+[、.])\s*/u, "").trim())
    .filter(Boolean)
    .forEach((item) => {
      const normalized = normalizeSegment(item);
      if (!normalized || seen.has(normalized)) {
        return;
      }

      const duplicatedByExisting = results.some((existing) => {
        const current = normalizeSegment(existing);
        return current.includes(normalized) || normalized.includes(current);
      });

      if (duplicatedByExisting) {
        return;
      }

      seen.add(normalized);
      results.push(item);
    });

  return results;
}

function PredictionEntryList({
  items,
  metaBuilder,
  detailBuilder,
}: {
  items?: SectorPredictionItem[];
  metaBuilder: (item: SectorPredictionItem) => string[];
  detailBuilder: (item: SectorPredictionItem) => string[];
}) {
  return (
    items?.length ? (
      <div className={styles.strategyEntryList}>
        {items.map((item, index) => (
          <div className={styles.strategyEntryItem} key={`${item.sector || "item"}-${index}`}>
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
    )
  );
}

interface PredictionToggleOption {
  key: string;
  label: string;
  items?: SectorPredictionItem[];
}

function PredictionToggleSection({
  options,
  metaBuilder,
  detailBuilder,
}: {
  options: PredictionToggleOption[];
  metaBuilder: (item: SectorPredictionItem) => string[];
  detailBuilder: (item: SectorPredictionItem) => string[];
}) {
  const [activeKey, setActiveKey] = useState(options[0]?.key ?? "");
  const activeOption = options.find((item) => item.key === activeKey) ?? options[0];

  if (!activeOption) {
    return <div className={styles.muted}>暂无数据</div>;
  }

  return (
    <div className={styles.sectionControlStack}>
      <div className={styles.reportTabGrid}>
        {options.map((item) => (
          <button
            className={item.key === activeOption.key ? styles.nestedTabButtonActive : styles.nestedTabButton}
            key={item.key}
            onClick={() => setActiveKey(item.key)}
            type="button"
          >
            {item.label}
          </button>
        ))}
      </div>
      <PredictionEntryList
        detailBuilder={detailBuilder}
        items={activeOption.items}
        metaBuilder={metaBuilder}
      />
    </div>
  );
}

interface HeatToggleOption extends PredictionToggleOption {
  tone: "danger" | "warning" | "info";
}

function scorePercent(value: unknown): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.max(0, Math.min(100, numeric));
}

function HeatObservationSection({ options }: { options: HeatToggleOption[] }) {
  const [activeKey, setActiveKey] = useState(options[0]?.key ?? "");
  const activeOption = options.find((item) => item.key === activeKey) ?? options[0];

  if (!activeOption) {
    return <div className={styles.muted}>暂无数据</div>;
  }

  const toneClass =
    activeOption.tone === "danger"
      ? styles.heatChartFillDanger
      : activeOption.tone === "warning"
        ? styles.heatChartFillWarning
        : styles.heatChartFillInfo;

  return (
    <div className={styles.sectionControlStack}>
      <div className={styles.reportTabGrid}>
        {options.map((item) => (
          <button
            className={item.key === activeOption.key ? styles.nestedTabButtonActive : styles.nestedTabButton}
            key={item.key}
            onClick={() => setActiveKey(item.key)}
            type="button"
          >
            {item.label}
          </button>
        ))}
      </div>

      {activeOption.items?.length ? (
        <div className={styles.heatChartList}>
          {activeOption.items.map((item, index) => (
            <div className={styles.heatChartCard} key={`${activeOption.key}-${item.sector || "item"}-${index}`}>
              <div className={styles.heatChartTop}>
                <strong>{asText(item.sector)}</strong>
                <span>{formatNumber(item.score, 0, "0")}</span>
              </div>
              <div className={styles.heatChartTrack}>
                <div
                  className={`${styles.heatChartFill} ${toneClass}`}
                  style={{ width: `${scorePercent(item.score)}%` }}
                />
              </div>
              <div className={styles.heatChartMeta}>
                <span>趋势：{asText(item.trend)}</span>
                <span>持续性：{asText(item.sustainability)}</span>
              </div>
              {item.reason ? <div>{item.reason}</div> : null}
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
  const [activeSection, setActiveSection] = useState<ViewSectionKey>("long_short");

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
        <div className={styles.reportHeaderStack}>
          <div className={styles.actions}>
            <button className={styles.secondaryButton} onClick={onBack} type="button">
              {backLabel}
            </button>
          </div>
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
          {formatDateTime(reportView?.meta?.timestamp, "暂无时间")}
          {dataTimestamp ? ` | 数据时间 ${formatDateTime(dataTimestamp, dataTimestamp)}` : ""}
        </p>

        <div className={styles.reportMetricTriplet}>
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
        <div className={styles.sectionControlStack}>
          {headlineSegments.length ? (
            <ol className={styles.detailOrderedList}>
              {headlineSegments.map((item, index) => (
                <li key={`${item}-${index}`}>{item}</li>
              ))}
            </ol>
          ) : (
            <div className={styles.muted}>暂无核心结论</div>
          )}

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

          <div className={styles.reportTabGridFour}>
            <button
              className={activeSection === "long_short" ? styles.primaryButton : styles.secondaryButton}
              onClick={() => setActiveSection("long_short")}
              type="button"
            >
              板块多空
            </button>
            <button
              className={activeSection === "rotation" ? styles.primaryButton : styles.secondaryButton}
              onClick={() => setActiveSection("rotation")}
              type="button"
            >
              轮动机会
            </button>
            <button
              className={activeSection === "heat" ? styles.primaryButton : styles.secondaryButton}
              onClick={() => setActiveSection("heat")}
              type="button"
            >
              热度观察
            </button>
            <button
              className={activeSection === "raw" ? styles.primaryButton : styles.secondaryButton}
              onClick={() => setActiveSection("raw")}
              type="button"
            >
              原始报告
            </button>
          </div>
          <div className={styles.sectionDivider} />

          {activeSection === "long_short" ? (
            <PredictionToggleSection
              detailBuilder={(item) => [item.reason || "", item.risk ? `风险提示：${item.risk}` : ""]}
              metaBuilder={(item) => [item.direction || "看多", formatConfidence(item.confidence)]}
              options={[
                { key: "bullish", label: "看多板块", items: predictions?.long_short?.bullish },
                { key: "neutral", label: "中性观察", items: predictions?.long_short?.neutral },
                { key: "bearish", label: "风险板块", items: predictions?.long_short?.bearish },
              ]}
            />
          ) : null}

          {activeSection === "rotation" ? (
            <PredictionToggleSection
              detailBuilder={(item) => [item.logic || "", item.advice ? `操作建议：${item.advice}` : ""]}
              metaBuilder={(item) => [item.stage || "强势", item.time_window || "暂无周期"]}
              options={[
                { key: "strong", label: "当前强势", items: predictions?.rotation?.current_strong },
                { key: "potential", label: "潜力接力", items: predictions?.rotation?.potential },
                { key: "declining", label: "衰退方向", items: predictions?.rotation?.declining },
              ]}
            />
          ) : null}

          {activeSection === "heat" ? (
            <HeatObservationSection
              options={[
                { key: "hottest", label: "最热板块", items: predictions?.heat?.hottest, tone: "danger" },
                { key: "heating", label: "升温板块", items: predictions?.heat?.heating, tone: "warning" },
                { key: "cooling", label: "降温板块", items: predictions?.heat?.cooling, tone: "info" },
              ]}
            />
          ) : null}

          {activeSection === "raw" ? <FixedReportWorkspace reports={reportView?.raw_reports || null} /> : null}
        </div>

        {predictions?.raw_fallback_text ? (
          <div className={`${styles.noticeCard} ${styles.noticeInfo}`}>
            <strong>原始预测文本</strong>
            <div className={styles.code}>{predictions.raw_fallback_text}</div>
          </div>
        ) : null}

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
      </section>
    </div>
  );
}
