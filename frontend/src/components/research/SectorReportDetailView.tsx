import { useState, type CSSProperties, type ReactNode } from "react";

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
    top_sectors?: Array<{
      name?: string;
      change_pct?: number;
      turnover?: number;
      top_stock?: string;
      top_stock_change?: number;
    }>;
    top_concepts?: Array<{
      name?: string;
      change_pct?: number;
      turnover?: number;
      top_stock?: string;
      top_stock_change?: number;
    }>;
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
  const tabsStyle = { "--nested-tab-count": options.length } as CSSProperties;

  if (!activeOption) {
    return <div className={styles.muted}>暂无数据</div>;
  }

  return (
    <div className={`${styles.sectionControlStack} ${styles.tabControlStack}`}>
      <div className={styles.historyDetailTabs} style={tabsStyle}>
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

function BoardSourceSection({
  marketSnapshot,
}: {
  marketSnapshot?: SectorStrategyReportView["market_snapshot"] | null;
}) {
  const [activeKey, setActiveKey] = useState<"industry" | "concept">("industry");
  const industryItems = marketSnapshot?.top_sectors ?? [];
  const conceptItems = marketSnapshot?.top_concepts ?? [];
  const activeItems = activeKey === "industry" ? industryItems : conceptItems;
  const tabsStyle = { "--nested-tab-count": 2 } as CSSProperties;

  return (
    <div className={`${styles.sectionControlStack} ${styles.tabControlStack}`}>
      <div className={styles.historyDetailTabs} style={tabsStyle}>
        <button
          className={activeKey === "industry" ? styles.nestedTabButtonActive : styles.nestedTabButton}
          onClick={() => setActiveKey("industry")}
          type="button"
        >
          行业板块
        </button>
        <button
          className={activeKey === "concept" ? styles.nestedTabButtonActive : styles.nestedTabButton}
          onClick={() => setActiveKey("concept")}
          type="button"
        >
          概念板块
        </button>
      </div>
      {activeKey === "industry" ? (
        <div className={styles.muted}>共 {marketSnapshot?.sectors_count ?? 0} 个行业板块</div>
      ) : (
        <div className={styles.muted}>共 {marketSnapshot?.concepts_count ?? 0} 个概念板块</div>
      )}
      {activeItems.length ? (
        <div className={styles.strategyBoardList}>
          {activeItems.map((item, index) => (
            <div className={styles.strategyBoardItem} key={`${activeKey}-${item.name || "board"}-${index}`}>
              <div className={styles.heatChartTop}>
                <strong>{asText(item.name)}</strong>
                <span>{formatPercent(item.change_pct)}</span>
              </div>
              <div className={styles.strategyBoardMeta}>
                <span>换手率：{formatNumber(item.turnover, 2)}</span>
                <span>领涨：{asText(item.top_stock)}</span>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className={styles.muted}>暂无数据</div>
      )}
    </div>
  );
}

function BoardInsightSection({
  predictions,
}: {
  predictions?: SectorStrategyReportView["predictions"] | null;
}) {
  return (
    <div className={styles.strategyInsightGrid}>
      <section className={styles.strategyInsightCard}>
        <div className={styles.strategyGroupHeader}>
          <strong>板块多空</strong>
        </div>
        <PredictionToggleSection
          detailBuilder={(item) => [item.reason || "", item.risk ? `风险提示：${item.risk}` : ""]}
          metaBuilder={(item) => [item.direction || "看多", formatConfidence(item.confidence)]}
          options={[
            { key: "bullish", label: "看多板块", items: predictions?.long_short?.bullish },
            { key: "neutral", label: "中性观察", items: predictions?.long_short?.neutral },
            { key: "bearish", label: "风险板块", items: predictions?.long_short?.bearish },
          ]}
        />
      </section>

      <section className={styles.strategyInsightCard}>
        <div className={styles.strategyGroupHeader}>
          <strong>轮动机会</strong>
        </div>
        <PredictionToggleSection
          detailBuilder={(item) => [item.logic || "", item.advice ? `操作建议：${item.advice}` : ""]}
          metaBuilder={(item) => [item.stage || "强势", item.time_window || "暂无周期"]}
          options={[
            { key: "strong", label: "当前强势", items: predictions?.rotation?.current_strong },
            { key: "potential", label: "潜力接力", items: predictions?.rotation?.potential },
            { key: "declining", label: "衰退方向", items: predictions?.rotation?.declining },
          ]}
        />
      </section>

      <section className={styles.strategyInsightCard}>
        <div className={styles.strategyGroupHeader}>
          <strong>热度观察</strong>
        </div>
        <HeatObservationSection
          options={[
            { key: "hottest", label: "最热板块", items: predictions?.heat?.hottest, tone: "danger" },
            { key: "heating", label: "升温板块", items: predictions?.heat?.heating, tone: "warning" },
            { key: "cooling", label: "降温板块", items: predictions?.heat?.cooling, tone: "info" },
          ]}
        />
      </section>
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
  const tabsStyle = { "--nested-tab-count": options.length } as CSSProperties;

  return (
    <div className={`${styles.sectionControlStack} ${styles.tabControlStack}`}>
      <div className={styles.historyDetailTabs} style={tabsStyle}>
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
  lifecycleItems?: Array<Record<string, unknown>> | null;
  dailyHeatPanel?: { available?: boolean; board_date?: string; total_count?: number; items?: Array<Record<string, unknown>> | null } | null;
}

export function SectorReportDetailView({
  title,
  backLabel,
  onBack,
  onExport,
  reportView,
  lifecycleItems,
  dailyHeatPanel,
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
  const warnings = reportView?.warnings;
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

        {headlineSegments.length ? (
          <div className={styles.listItem}>
            <strong>核心结论</strong>
            <ol className={styles.detailOrderedList} style={{ marginTop: 10 }}>
              {headlineSegments.map((item, index) => (
                <li key={`${item}-${index}`}>{item}</li>
              ))}
            </ol>
          </div>
        ) : (
          <div className={styles.listItem}>
            <strong>核心结论</strong>
            <div className={styles.muted} style={{ marginTop: 10 }}>暂无核心结论</div>
          </div>
        )}

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

      <DetailSection title="板块研判">
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

        <BoardInsightSection predictions={predictions} />
        <BoardSourceSection marketSnapshot={reportView?.market_snapshot ?? null} />
      </DetailSection>

      <DetailSection title="生命周期观察">
        {lifecycleItems?.length ? (
          <div className={styles.heatChartList}>
            {lifecycleItems.map((item, index) => {
              const trajectory = Array.isArray(item.trajectory)
                ? item.trajectory.map((entry) => String((entry as { score?: unknown }).score ?? 0)).join(" -> ")
                : "-";
              const details =
                item.lifecycle_details && typeof item.lifecycle_details === "object"
                  ? (item.lifecycle_details as Record<string, Record<string, unknown>>)
                  : {};
              const stage = String(item.lifecycle_stage || "neutral");
              const stageText = stage === "startup" ? "启动期" : stage === "explosive" ? "爆发期" : stage === "decay" ? "衰退期" : "中性";
              return (
                <div className={styles.heatChartCard} key={`${String(item.sector_name || "sector")}-${index}`}>
                  <div className={styles.heatChartTop}>
                    <strong>{asText(item.sector_name)}</strong>
                    <span>{formatNumber(item.heat_score, 0, "0")}</span>
                  </div>
                  <div className={styles.heatChartMeta}>
                    <span>阶段：{stageText}</span>
                    <span>防守线：{asText(item.defense_line_type, "NONE")}</span>
                  </div>
                  <div className={styles.muted}>
                    来源：{asText(item.source_type, "-")} | 观察点数：{asText(item.observation_count, "-")} | 主窗口：{asText(item.window_size_used, "-")} 日
                  </div>
                  <div>生命周期轨迹：{trajectory}</div>
                  <div className={styles.muted}>Δ1：{asText(item.delta_1, "-")} | Δ2：{asText(item.delta_2, "-")}</div>
                  {Object.keys(details).length ? (
                    <div className={styles.muted}>
                      {Object.entries(details)
                        .map(([windowKey, metrics]) => {
                          const typedMetrics = metrics as Record<string, unknown>;
                          return `${windowKey}日: 变化${asText(typedMetrics.change, "-")} / 斜率${asText(typedMetrics.slope, "-")} / 回撤${asText(typedMetrics.drawdown, "-")}`;
                        })
                        .join(" | ")}
                    </div>
                  ) : null}
                  <div>{asText(item.action_hint, "暂无动作提示")}</div>
                  {Boolean(item.selection_veto) ? <div className={styles.dangerText}>生命周期衰退，一票否决</div> : null}
                </div>
              );
            })}
          </div>
        ) : (
          <div className={styles.muted}>暂无生命周期数据</div>
        )}
      </DetailSection>

      <DetailSection title="当日热度面板">
        {(dailyHeatPanel?.items ?? []).length ? (
          <div className={styles.tableWrap}>
            <table className={`${styles.table} ${styles.tableCompact}`}>
              <thead>
                <tr>
                  <th>排名</th>
                  <th>板块</th>
                  <th>来源</th>
                  <th>热度</th>
                  <th>涨跌幅</th>
                  <th>换手</th>
                </tr>
              </thead>
              <tbody>
                {(dailyHeatPanel?.items ?? []).slice(0, 20).map((item, index) => (
                  <tr key={`daily-heat-row-${index}`}>
                    <td>{asText(item.rank_order, String(index + 1))}</td>
                    <td>{asText(item.sector_name)}</td>
                    <td>{asText(item.source_type, "-")}</td>
                    <td>{formatNumber(item.heat_score, 2, "0.00")}</td>
                    <td>{formatNumber(item.change_pct, 2, "0.00")}%</td>
                    <td>{formatNumber(item.turnover, 2, "0.00")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className={styles.muted}>暂无当日热度面板数据</div>
        )}
      </DetailSection>

      <DetailSection title="原始报告">
        <FixedReportWorkspace reports={reportView?.raw_reports || null} />
        {predictions?.raw_fallback_text ? (
          <div className={`${styles.noticeCard} ${styles.noticeInfo}`}>
            <strong>原始预测文本</strong>
            <div className={styles.code}>{predictions.raw_fallback_text}</div>
          </div>
        ) : null}
      </DetailSection>
    </div>
  );
}
