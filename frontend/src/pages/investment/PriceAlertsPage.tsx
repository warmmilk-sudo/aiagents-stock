import { FormEvent, useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch } from "../../lib/api";
import { decodeIntent } from "../../lib/intents";
import styles from "../ConsolePage.module.scss";


interface PriceAlert {
  id: number;
  symbol: string;
  name: string;
  rating: string;
  entry_range?: {
    min?: number;
    max?: number;
  };
  take_profit?: number;
  stop_loss?: number;
  check_interval: number;
  notification_enabled: boolean;
  account_name?: string;
}

interface PriceAlertNotification {
  id: number;
  symbol: string;
  message: string;
  triggered_at: string;
}

interface FormState {
  symbol: string;
  name: string;
  entry_min: string;
  entry_max: string;
  take_profit: string;
  stop_loss: string;
}

const defaultForm: FormState = {
  symbol: "",
  name: "",
  entry_min: "",
  entry_max: "",
  take_profit: "",
  stop_loss: "",
};

export function PriceAlertsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [alerts, setAlerts] = useState<PriceAlert[]>([]);
  const [notifications, setNotifications] = useState<PriceAlertNotification[]>([]);
  const [form, setForm] = useState<FormState>(defaultForm);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const load = async () => {
    const [alertData, notificationData] = await Promise.all([
      apiFetch<PriceAlert[]>("/api/price-alerts"),
      apiFetch<PriceAlertNotification[]>("/api/price-alerts/notifications?limit=12"),
    ]);
    setAlerts(alertData);
    setNotifications(notificationData);
  };

  useEffect(() => {
    void load();
  }, []);

  useEffect(() => {
    const intent = decodeIntent<{ strategy_context?: Record<string, unknown>; symbol?: string; stock_name?: string }>(
      searchParams.get("intent"),
    );
    if (!intent || intent.type !== "price_alert") {
      return;
    }
    const payload = intent.payload || {};
    const strategyContext = payload.strategy_context || {};
    setForm({
      symbol: String(payload.symbol || ""),
      name: String(payload.stock_name || ""),
      entry_min: String(strategyContext.entry_min ?? ""),
      entry_max: String(strategyContext.entry_max ?? ""),
      take_profit: String(strategyContext.take_profit ?? ""),
      stop_loss: String(strategyContext.stop_loss ?? ""),
    });
    searchParams.delete("intent");
    setSearchParams(searchParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const handleCreate = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");
    setMessage("");
    try {
      await apiFetch<{ alert_id: number }>("/api/price-alerts", {
        method: "POST",
        body: JSON.stringify({
          symbol: form.symbol,
          name: form.name || form.symbol,
          entry_min: Number(form.entry_min),
          entry_max: Number(form.entry_max),
          take_profit: form.take_profit ? Number(form.take_profit) : null,
          stop_loss: form.stop_loss ? Number(form.stop_loss) : null,
        }),
      });
      setForm(defaultForm);
      setMessage("价格预警已创建");
      await load();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "创建失败");
    }
  };

  const handleDelete = async (alertId: number) => {
    await apiFetch(`/api/price-alerts/${alertId}`, { method: "DELETE" });
    await load();
  };

  const handleToggle = async (alert: PriceAlert) => {
    await apiFetch(
      `/api/price-alerts/${alert.id}/notification?enabled=${String(!alert.notification_enabled)}`,
      { method: "POST" },
    );
    await load();
  };

  return (
    <PageFrame
      title="价格预警"
      summary="这一页已经切成独立前端页面，负责管理价格预警、通知开关和近期触发记录。"
      actions={<StatusBadge label={`${alerts.length} 条预警`} tone="info" />}
    >
      <div className={styles.grid}>
        <section className={`${styles.card} ${styles.span6}`}>
          <h2>新增预警</h2>
          <form className={styles.stack} onSubmit={handleCreate}>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="symbol">股票代码</label>
                <input
                  id="symbol"
                  value={form.symbol}
                  onChange={(event) => setForm((state) => ({ ...state, symbol: event.target.value }))}
                />
              </div>
              <div className={styles.field}>
                <label htmlFor="name">股票名称</label>
                <input
                  id="name"
                  value={form.name}
                  onChange={(event) => setForm((state) => ({ ...state, name: event.target.value }))}
                />
              </div>
              <div className={styles.field}>
                <label htmlFor="entryMin">买入下沿</label>
                <input
                  id="entryMin"
                  value={form.entry_min}
                  onChange={(event) => setForm((state) => ({ ...state, entry_min: event.target.value }))}
                />
              </div>
              <div className={styles.field}>
                <label htmlFor="entryMax">买入上沿</label>
                <input
                  id="entryMax"
                  value={form.entry_max}
                  onChange={(event) => setForm((state) => ({ ...state, entry_max: event.target.value }))}
                />
              </div>
              <div className={styles.field}>
                <label htmlFor="takeProfit">止盈</label>
                <input
                  id="takeProfit"
                  value={form.take_profit}
                  onChange={(event) => setForm((state) => ({ ...state, take_profit: event.target.value }))}
                />
              </div>
              <div className={styles.field}>
                <label htmlFor="stopLoss">止损</label>
                <input
                  id="stopLoss"
                  value={form.stop_loss}
                  onChange={(event) => setForm((state) => ({ ...state, stop_loss: event.target.value }))}
                />
              </div>
            </div>
            <div className={styles.actions}>
              <button className={styles.primaryButton} type="submit">
                保存预警
              </button>
              {message ? <span className={styles.successText}>{message}</span> : null}
              {error ? <span className={styles.dangerText}>{error}</span> : null}
            </div>
          </form>
        </section>

        <section className={`${styles.card} ${styles.span6}`}>
          <h2>近期通知</h2>
          <div className={styles.list}>
            {notifications.length === 0 ? <div className={styles.muted}>暂无触发通知</div> : null}
            {notifications.map((item) => (
              <div className={styles.listItem} key={item.id}>
                <strong>{item.symbol}</strong>
                <p>{item.message}</p>
                <small className={styles.muted}>{item.triggered_at}</small>
              </div>
            ))}
          </div>
        </section>

        <section className={`${styles.card} ${styles.span12}`}>
          <h2>当前预警</h2>
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>股票</th>
                  <th>区间</th>
                  <th>止盈/止损</th>
                  <th>通知</th>
                  <th>账户</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {alerts.map((alert) => (
                  <tr key={alert.id}>
                    <td>
                      <strong>{alert.symbol}</strong>
                      <div className={styles.muted}>{alert.name}</div>
                    </td>
                    <td>
                      {(alert.entry_range?.min ?? "-")} - {(alert.entry_range?.max ?? "-")}
                    </td>
                    <td>
                      {alert.take_profit ?? "-"} / {alert.stop_loss ?? "-"}
                    </td>
                    <td>{alert.notification_enabled ? "已开启" : "已关闭"}</td>
                    <td>{alert.account_name ?? "默认账户"}</td>
                    <td>
                      <div className={styles.actions}>
                        <button className={styles.secondaryButton} onClick={() => void handleToggle(alert)} type="button">
                          {alert.notification_enabled ? "关闭通知" : "开启通知"}
                        </button>
                        <button className={styles.dangerButton} onClick={() => void handleDelete(alert.id)} type="button">
                          删除
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
                {alerts.length === 0 ? (
                  <tr>
                    <td className={styles.muted} colSpan={6}>
                      暂无价格预警
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
