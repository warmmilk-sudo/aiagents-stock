import { useEffect, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { apiFetch } from "../../lib/api";
import styles from "../ConsolePage.module.scss";


interface ActivitySnapshot {
  price_alert_notifications: Array<Record<string, unknown>>;
  recent_events: Array<Record<string, unknown>>;
  ai_decisions: Array<Record<string, unknown>>;
  trade_records: Array<Record<string, unknown>>;
  pending_actions: Array<Record<string, unknown>>;
}

export function ActivityPage() {
  const [snapshot, setSnapshot] = useState<ActivitySnapshot | null>(null);

  const load = async () => {
    const data = await apiFetch<ActivitySnapshot>("/api/investment-activity/snapshot");
    setSnapshot(data);
  };

  useEffect(() => {
    void load();
  }, []);

  return (
    <PageFrame
      title="投资活动"
      summary="聚合展示最近通知、AI 决策、成交记录和待处理动作，便于做日常巡检。"
      actions={
        <button className={styles.secondaryButton} onClick={() => void load()} type="button">
          刷新
        </button>
      }
    >
      <div className={styles.stack}>
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
          </div>
        </section>

        <section className={styles.card}>
          <h2>最近事件</h2>
          <div className={styles.list}>
            {(snapshot?.recent_events ?? []).map((item, index) => (
              <div className={styles.listItem} key={`${String(item.id ?? index)}-${index}`}>
                <strong>{String(item.symbol ?? item.stock_code ?? "-")}</strong>
                <p>{String(item.message ?? item.event_type ?? "")}</p>
                <small className={styles.muted}>{String(item.created_at ?? item.triggered_at ?? "")}</small>
              </div>
            ))}
            {!snapshot?.recent_events.length ? <div className={styles.muted}>暂无事件</div> : null}
          </div>
        </section>

        <section className={styles.card}>
          <h2>AI 决策</h2>
          <div className={styles.list}>
            {(snapshot?.ai_decisions ?? []).map((item, index) => (
              <div className={styles.listItem} key={`${String(item.id ?? index)}-${index}`}>
                <strong>
                  {String(item.stock_code ?? "-")} · {String(item.action ?? "-")}
                </strong>
                <p>{String(item.reasoning ?? "无推理摘要")}</p>
              </div>
            ))}
            {!snapshot?.ai_decisions.length ? <div className={styles.muted}>暂无 AI 决策</div> : null}
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
