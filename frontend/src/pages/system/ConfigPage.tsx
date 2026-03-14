import { useEffect, useMemo, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError } from "../../lib/api";
import { type ConfigField, useConfigStore } from "../../stores/configStore";
import styles from "../ConsolePage.module.scss";

type SectionKey = "basic" | "data" | "notification";

const sectionTabs = [
  { key: "basic", label: "基本配置" },
  { key: "data", label: "数据源配置" },
  { key: "notification", label: "通知配置" },
];

const BASIC_PANELS = [
  { title: "基础连接", keys: ["DEEPSEEK_API_KEY", "DEEPSEEK_BASE_URL"] },
  {
    title: "模型配置",
    keys: ["LIGHTWEIGHT_MODEL_NAME", "LIGHTWEIGHT_MODEL_OPTIONS", "REASONING_MODEL_NAME", "REASONING_MODEL_OPTIONS"],
  },
  {
    title: "盯盘默认",
    keys: [
      "SMART_MONITOR_HTTP_TIMEOUT_SECONDS",
      "SMART_MONITOR_HTTP_RETRY_COUNT",
      "SMART_MONITOR_AI_TIMEOUT_SECONDS",
      "SMART_MONITOR_REASONING_MAX_TOKENS",
      "SMART_MONITOR_INTRADAY_TDX_RETRY_COUNT",
      "SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT",
      "SMART_MONITOR_DEFAULT_STOP_LOSS_PCT",
      "SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT",
      "SMART_MONITOR_AI_INTERVAL_MINUTES",
      "SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES",
    ],
  },
  {
    title: "登录与系统",
    keys: [
      "ADMIN_PASSWORD",
      "ADMIN_PASSWORD_HASH",
      "LOGIN_MAX_ATTEMPTS",
      "LOGIN_LOCKOUT_SECONDS",
      "ADMIN_SESSION_TTL_SECONDS",
      "ICP_NUMBER",
      "ICP_LINK",
    ],
  },
];

const DATA_PANELS = [
  { title: "Tushare 数据源", keys: ["TUSHARE_TOKEN", "TUSHARE_URL"] },
  { title: "TDX 数据源", keys: ["TDX_ENABLED", "TDX_BASE_URL", "TDX_TIMEOUT_SECONDS"] },
  { title: "默认数据周期", keys: ["DATA_PERIOD"] },
];

const NOTIFICATION_PANELS = [
  { title: "邮件通知", keys: ["EMAIL_ENABLED", "SMTP_SERVER", "SMTP_PORT", "EMAIL_FROM", "EMAIL_PASSWORD", "EMAIL_TO"] },
  { title: "Webhook 通知", keys: ["WEBHOOK_ENABLED", "WEBHOOK_TYPE", "WEBHOOK_URL", "WEBHOOK_KEYWORD"] },
];

function renderField(
  key: string,
  field: ConfigField,
  setValue: (key: string, value: string) => void,
) {
  return (
    <div className={styles.field} key={key}>
      <label htmlFor={key}>
        {field.description}
        {field.required ? " *" : ""}
      </label>
      {field.options?.length ? (
        <select id={key} value={field.value} onChange={(event) => setValue(key, event.target.value)}>
          {field.options.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      ) : field.type === "boolean" ? (
        <select id={key} value={field.value} onChange={(event) => setValue(key, event.target.value)}>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      ) : (
        <input
          id={key}
          onChange={(event) => setValue(key, event.target.value)}
          type={field.type === "password" ? "password" : "text"}
          value={field.value}
        />
      )}
      <small className={styles.muted}>{key}</small>
    </div>
  );
}

export function ConfigPage() {
  const fields = useConfigStore((state) => state.fields);
  const webhookStatus = useConfigStore((state) => state.webhookStatus);
  const loading = useConfigStore((state) => state.loading);
  const fetchConfig = useConfigStore((state) => state.fetchConfig);
  const setValue = useConfigStore((state) => state.setValue);
  const save = useConfigStore((state) => state.save);
  const testWebhook = useConfigStore((state) => state.testWebhook);
  const [section, setSection] = useState<SectionKey>("basic");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    void fetchConfig();
  }, [fetchConfig]);

  const handleSave = async () => {
    setMessage("");
    setError("");
    try {
      await save();
      setMessage("配置已保存并重新加载");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "保存失败");
    }
  };

  const handleWebhookTest = async () => {
    setMessage("");
    setError("");
    try {
      const result = await testWebhook();
      setMessage(result);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "Webhook 测试失败");
    }
  };

  const fieldEntries = useMemo(() => Object.entries(fields), [fields]);
  const renderPanel = (title: string, keys: string[]) => {
    const entries = keys
      .map((key) => [key, fields[key]] as const)
      .filter((item): item is [string, ConfigField] => Boolean(item[1]));

    if (!entries.length) {
      return null;
    }

    return (
      <section className={styles.card} key={title}>
        <h2>{title}</h2>
        <div className={styles.formGrid}>
          {entries.map(([key, field]) => renderField(key, field, setValue))}
        </div>
      </section>
    );
  };

  const unmatchedKeys = useMemo(() => {
    const usedKeys = new Set(
      [...BASIC_PANELS, ...DATA_PANELS, ...NOTIFICATION_PANELS].flatMap((panel) => panel.keys),
    );
    return fieldEntries.filter(([key]) => !usedKeys.has(key));
  }, [fieldEntries]);

  return (
    <PageFrame
      title="系统配置"
      sectionTabs={sectionTabs}
      activeSectionKey={section}
      onSectionChange={(nextSection) => setSection(nextSection as SectionKey)}
      actions={<StatusBadge label={loading ? "读取中" : `字段 ${Object.keys(fields).length}`} tone={loading ? "warning" : "default"} />}
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.primaryButton} onClick={() => void handleSave()} type="button">
              保存配置
            </button>
            {section === "notification" ? (
              <button className={styles.secondaryButton} onClick={() => void handleWebhookTest()} type="button">
                测试 Webhook
              </button>
            ) : null}
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
          </div>
        </section>

        {section === "basic" ? (
          <>
            {BASIC_PANELS.map((panel) => renderPanel(panel.title, panel.keys))}
            {unmatchedKeys.length ? (
              <section className={styles.card}>
                <h2>其他配置</h2>
                <div className={styles.formGrid}>
                  {unmatchedKeys.map(([key, field]) => renderField(key, field, setValue))}
                </div>
              </section>
            ) : null}
          </>
        ) : null}

        {section === "data" ? (
          <>{DATA_PANELS.map((panel) => renderPanel(panel.title, panel.keys))}</>
        ) : null}

        {section === "notification" ? (
          <>
            {NOTIFICATION_PANELS.map((panel) => renderPanel(panel.title, panel.keys))}
            <section className={styles.card}>
              <h2>Webhook 状态</h2>
              <pre className={styles.listItem}>{JSON.stringify(webhookStatus, null, 2)}</pre>
            </section>
          </>
        ) : null}
      </div>
    </PageFrame>
  );
}
