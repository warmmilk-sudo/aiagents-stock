import { useEffect, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { FormattedReport } from "../../components/research/FormattedReport";
import { apiFetchCached } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import styles from "../ConsolePage.module.scss";

interface ActivitySnapshot {
  price_alert_notifications: Array<Record<string, unknown>>;
  recent_events: Array<Record<string, unknown>>;
  ai_decisions: Array<Record<string, unknown>>;
  trade_records: Array<Record<string, unknown>>;
  pending_actions: Array<Record<string, unknown>>;
}

type SectionKey = "overview" | "events" | "decisions" | "pending" | "trades";

const sectionTabs = [
  { key: "overview", label: "活动总览" },
  { key: "events", label: "最近事件" },
  { key: "decisions", label: "AI决策" },
  { key: "pending", label: "待办动作" },
  { key: "trades", label: "成交记录" },
];

function asText(value: unknown, fallback = "-") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

export function ActivityPage() {
  const [snapshot, setSnapshot] = useState<ActivitySnapshot | null>(null);
  const [section, setSection] = useState<SectionKey>("overview");

  const load = async () => {
    const data = await apiFetchCached<ActivitySnapshot>("/api/investment-activity/snapshot");
    setSnapshot(data);
  };

  useEffect(() => {
    void load();
  }, []);

  return (
    <PageFrame
      title="投资活动"
      summary="聚合展示最近通知、AI 决策、成交记录和待处理动作，便于做日常巡检。"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      actions={
        <button className={styles.secondaryButton} onClick={() => void load()} type="button">
          刷新
        </button>
      }
    >
      <div className={styles.stack}>
        {section === "overview" ? (
          <section className={styles.card}>
            <div className={styles.compactGrid}>
              <div className={styles.metric}>
                <span className={styles.muted}>价格预警通知</span>
                <strong>{snapshot?.price_alert_notifications.length ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>近期事件</span>
                <strong>{snapshot?.recent_events.length ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>AI 决策</span>
                <strong>{snapshot?.ai_decisions.length ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>待办动作</span>
                <strong>{snapshot?.pending_actions.length ?? 0}</strong>
              </div>
              <div className={styles.metric}>
                <span className={styles.muted}>成交记录</span>
                <strong>{snapshot?.trade_records.length ?? 0}</strong>
              </div>
            </div>
          </section>
        ) : null}

        {section === "events" ? (
          <section className={styles.card}>
            <div className={styles.list}>
              {(snapshot?.recent_events ?? []).map((item, index) => (
                <div className={styles.listItem} key={`${String(item.id ?? index)}-${index}`}>
                  <strong>{asText(item.symbol ?? item.stock_code)}</strong>
                  <p>{asText(item.message ?? item.event_type, "暂无描述")}</p>
                  <small className={styles.muted}>{formatDateTime(item.created_at ?? item.triggered_at, "暂无时间")}</small>
                </div>
              ))}
              {!snapshot?.recent_events.length ? <div className={styles.muted}>暂无事件</div> : null}
            </div>
          </section>
        ) : null}

        {section === "decisions" ? (
          <section className={styles.card}>
            <div className={styles.list}>
              {(snapshot?.ai_decisions ?? []).map((item, index) => (
                <div className={styles.listItem} key={`${String(item.id ?? index)}-${index}`}>
                  <strong>
                    {asText(item.stock_code)} · {asText(item.action)}
                  </strong>
                  <FormattedReport content={asText(item.reasoning, "无推理摘要")} />
                  <small className={styles.muted}>{formatDateTime(item.decision_time ?? item.created_at, "暂无时间")}</small>
                </div>
              ))}
              {!snapshot?.ai_decisions.length ? <div className={styles.muted}>暂无 AI 决策</div> : null}
            </div>
          </section>
        ) : null}

        {section === "pending" ? (
          <section className={styles.card}>
            <div className={styles.list}>
              {(snapshot?.pending_actions ?? []).map((item, index) => (
                <div className={styles.listItem} key={`${String(item.id ?? index)}-${index}`}>
                  <strong>{asText(item.action_type ?? item.type, "待办动作")}</strong>
                  <p className={styles.muted}>
                    股票: {asText(item.stock_code ?? item.symbol, "未指定")}
                  </p>
                  <small className={styles.muted}>
                    {formatDateTime(item.created_at ?? item.triggered_at, "暂无时间")} | 状态: {asText(item.status, "待处理")}
                  </small>
                </div>
              ))}
              {!snapshot?.pending_actions.length ? <div className={styles.muted}>暂无待办动作</div> : null}
            </div>
          </section>
        ) : null}

        {section === "trades" ? (
          <section className={styles.card}>
            <div className={styles.list}>
              {(snapshot?.trade_records ?? []).map((item, index) => (
                <div className={styles.listItem} key={`${String(item.id ?? index)}-${index}`}>
                  <strong>
                    {asText(item.stock_name ?? item.symbol, "未知股票")} ({asText(item.stock_code ?? item.symbol, "-")})
                  </strong>
                  <p>
                    {asText(item.trade_type, "未知类型")} | 数量 {asText(item.quantity, "0")} | 价格 {asText(item.price, "0")}
                  </p>
                  <small className={styles.muted}>{formatDateTime(item.trade_time ?? item.created_at, "暂无时间")}</small>
                </div>
              ))}
              {!snapshot?.trade_records.length ? <div className={styles.muted}>暂无成交记录</div> : null}
            </div>
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
