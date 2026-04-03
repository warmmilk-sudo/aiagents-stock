import { FormEvent, useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { PageFeedback } from "../../components/common/PageFeedback";
import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError, apiFetch } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
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

type NotificationTone = "danger" | "success" | "warning" | "info";
type SectionKey = "overview" | "notifications" | "alerts";

const sectionTabs = [
  { key: "overview", label: "预警总览" },
  { key: "notifications", label: "近期通知" },
  { key: "alerts", label: "当前预警" },
];

const defaultForm: FormState = {
  symbol: "",
  name: "",
  entry_min: "",
  entry_max: "",
  take_profit: "",
  stop_loss: "",
};

const notificationToneClass: Record<NotificationTone, string> = {
  danger: styles.noticeDanger,
  success: styles.noticeSuccess,
  warning: styles.noticeWarning,
  info: styles.noticeInfo,
};

function formatNumber(value?: number) {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? numeric.toLocaleString("zh-CN", { maximumFractionDigits: 2 })
    : "-";
}

function resolveNotificationMeta(message: string): { tone: NotificationTone; label: string } {
  if (/(止损|跌破|下破|失守|回撤)/.test(message)) {
    return { tone: "danger", label: "风险预警" };
  }
  if (/(止盈|突破|上破|目标价|盈利)/.test(message)) {
    return { tone: "success", label: "收益信号" };
  }
  if (/(买入|入场|区间|接近)/.test(message)) {
    return { tone: "info", label: "关注提醒" };
  }
  return { tone: "warning", label: "价格提醒" };
}

export function PriceAlertsPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [alerts, setAlerts] = useState<PriceAlert[]>([]);
  const [notifications, setNotifications] = useState<PriceAlertNotification[]>([]);
  const [form, setForm] = useState<FormState>(defaultForm);
  const [showComposer, setShowComposer] = useState(false);
  const [section, setSection] = useState<SectionKey>("overview");
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
    setShowComposer(true);
    setSection("overview");

    const nextParams = new URLSearchParams(searchParams);
    nextParams.delete("intent");
    setSearchParams(nextParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const handleCreate = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");
    setMessage("");

    const entryMin = Number(form.entry_min);
    const entryMax = Number(form.entry_max);

    if (!form.symbol.trim()) {
      setError("请先填写股票代码");
      return;
    }
    if (!Number.isFinite(entryMin) || !Number.isFinite(entryMax)) {
      setError("请输入有效的入场区间");
      return;
    }

    try {
      await apiFetch<{ alert_id: number }>("/api/price-alerts", {
        method: "POST",
        body: JSON.stringify({
          symbol: form.symbol.trim(),
          name: form.name.trim() || form.symbol.trim(),
          entry_min: entryMin,
          entry_max: entryMax,
          take_profit: form.take_profit ? Number(form.take_profit) : null,
          stop_loss: form.stop_loss ? Number(form.stop_loss) : null,
        }),
      });
      setForm(defaultForm);
      setShowComposer(false);
      setMessage("价格预警已创建");
      await load();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "创建失败");
    }
  };

  const handleDelete = async (alertId: number) => {
    setError("");
    setMessage("");
    try {
      await apiFetch(`/api/price-alerts/${alertId}`, { method: "DELETE" });
      setMessage("价格预警已删除");
      await load();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "删除失败");
    }
  };

  const handleToggle = async (alert: PriceAlert) => {
    setError("");
    setMessage("");
    try {
      await apiFetch(
        `/api/price-alerts/${alert.id}/notification?enabled=${String(!alert.notification_enabled)}`,
        { method: "POST" },
      );
      setMessage(alert.notification_enabled ? "已关闭通知" : "已开启通知");
      await load();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新通知状态失败");
    }
  };

  const enabledAlerts = alerts.filter((item) => item.notification_enabled).length;
  const protectedAlerts = alerts.filter((item) => item.take_profit || item.stop_loss).length;

  return (
    <PageFrame
      title="价格预警"
      summary="优先展示近期触发通知与现有规则，新增预警在需要时再展开。"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
    >
      <div className={styles.stack}>
        <PageFeedback error={error} message={message} />
        <section className={styles.card}>
          <div className={styles.cardHeader}>
            <div>
              <h2>页面操作</h2>
            </div>
            <div className={styles.actions}>
              <button
                className={styles.secondaryButton}
                onClick={() => navigate("/investment/smart-monitor")}
                type="button"
              >
                返回智能盯盘
              </button>
              <button className={styles.secondaryButton} onClick={() => void load()} type="button">
                刷新
              </button>
              <button
                className={showComposer ? styles.primaryButton : styles.secondaryButton}
                onClick={() => {
                  setSection("overview");
                  setShowComposer((current) => !current);
                }}
                type="button"
              >
                {showComposer ? "收起新增预警" : "新增预警"}
              </button>
            </div>
          </div>
        </section>

        {section === "overview" ? (
          <>
            {showComposer ? (
              <section className={styles.card}>
                <div className={styles.cardHeader}>
                  <div>
                    <h2>新增预警</h2>
                    <p className={styles.helperText}>录入完成后自动回到总览信息视图，减少页面干扰。</p>
                  </div>
                  <button className={styles.tertiaryButton} onClick={() => setShowComposer(false)} type="button">
                    收起
                  </button>
                </div>
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
                      <label htmlFor="entryMin">入场下沿</label>
                      <input
                        id="entryMin"
                        value={form.entry_min}
                        onChange={(event) => setForm((state) => ({ ...state, entry_min: event.target.value }))}
                      />
                    </div>
                    <div className={styles.field}>
                      <label htmlFor="entryMax">入场上沿</label>
                      <input
                        id="entryMax"
                        value={form.entry_max}
                        onChange={(event) => setForm((state) => ({ ...state, entry_max: event.target.value }))}
                      />
                    </div>
                    <div className={styles.field}>
                      <label htmlFor="takeProfit">止盈价</label>
                      <input
                        id="takeProfit"
                        value={form.take_profit}
                        onChange={(event) => setForm((state) => ({ ...state, take_profit: event.target.value }))}
                      />
                    </div>
                    <div className={styles.field}>
                      <label htmlFor="stopLoss">止损价</label>
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
                    <button className={styles.secondaryButton} onClick={() => setShowComposer(false)} type="button">
                      取消
                    </button>
                  </div>
                </form>
              </section>
            ) : null}

            <section className={styles.card}>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>当前预警</span>
                  <strong>{alerts.length}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>通知已开启</span>
                  <strong>{enabledAlerts}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>已设止盈止损</span>
                  <strong>{protectedAlerts}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>近 12 条触发</span>
                  <strong>{notifications.length}</strong>
                </div>
              </div>
            </section>
          </>
        ) : null}

        {section === "notifications" ? (
          <section className={styles.card}>
            <div className={styles.cardHeader}>
              <div>
                <h2 className={styles.mobileDuplicateHeading}>近期通知</h2>
                <p className={styles.helperText}>用颜色区分不同类型，优先识别风险、收益和关注信号。</p>
              </div>
            </div>
            <div className={styles.list}>
              {notifications.map((item) => {
                const meta = resolveNotificationMeta(item.message);
                return (
                  <div
                    className={`${styles.noticeCard} ${notificationToneClass[meta.tone]}`}
                    key={item.id}
                  >
                    <div className={styles.noticeMeta}>
                      <StatusBadge label={meta.label} tone={meta.tone} />
                      <strong>{item.symbol}</strong>
                    </div>
                    <div>{item.message}</div>
                    <small className={styles.muted}>{formatDateTime(item.triggered_at, "暂无时间")}</small>
                  </div>
                );
              })}
              {notifications.length === 0 ? (
                <div className={styles.noticeCard}>
                  <div className={styles.noticeMeta}>
                    <StatusBadge label="暂无触发" tone="default" />
                  </div>
                  <div>最近还没有新的价格通知。</div>
                </div>
              ) : null}
            </div>
          </section>
        ) : null}

        {section === "alerts" ? (
          <section className={styles.card}>
            <div className={styles.cardHeader}>
              <div>
                <h2 className={styles.mobileDuplicateHeading}>当前预警</h2>
              </div>
            </div>
            <div className={styles.tableWrap}>
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th>股票</th>
                    <th>入场区间</th>
                    <th>止盈 / 止损</th>
                    <th>通知状态</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {alerts.map((alert) => (
                    <tr key={alert.id}>
                      <td>
                        <strong>{alert.symbol}</strong>
                        <div className={styles.muted}>{alert.name}</div>
                        <div style={{ marginTop: 8 }}>
                          <StatusBadge label={alert.rating || "买入"} tone="default" />
                        </div>
                      </td>
                      <td>
                        {formatNumber(alert.entry_range?.min)} - {formatNumber(alert.entry_range?.max)}
                      </td>
                      <td>
                        {formatNumber(alert.take_profit)} / {formatNumber(alert.stop_loss)}
                      </td>
                      <td>
                        <StatusBadge
                          label={alert.notification_enabled ? "通知开启" : "通知关闭"}
                          tone={alert.notification_enabled ? "default" : "warning"}
                        />
                      </td>
                      <td>
                        <div className={styles.actions}>
                          <button
                            className={styles.secondaryButton}
                            onClick={() => void handleToggle(alert)}
                            type="button"
                          >
                            {alert.notification_enabled ? "关闭通知" : "开启通知"}
                          </button>
                          <button
                            className={styles.dangerButton}
                            onClick={() => void handleDelete(alert.id)}
                            type="button"
                          >
                            删除
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {alerts.length === 0 ? (
                    <tr>
                      <td className={styles.muted} colSpan={5}>
                        暂无价格预警
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}
      </div>
    </PageFrame>
  );
}
