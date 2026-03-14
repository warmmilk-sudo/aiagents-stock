import { useEffect, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { StatusBadge } from "../../components/common/StatusBadge";
import { ApiRequestError } from "../../lib/api";
import { useConfigStore } from "../../stores/configStore";
import styles from "../ConsolePage.module.scss";


export function ConfigPage() {
  const fields = useConfigStore((state) => state.fields);
  const webhookStatus = useConfigStore((state) => state.webhookStatus);
  const loading = useConfigStore((state) => state.loading);
  const fetchConfig = useConfigStore((state) => state.fetchConfig);
  const setValue = useConfigStore((state) => state.setValue);
  const save = useConfigStore((state) => state.save);
  const testWebhook = useConfigStore((state) => state.testWebhook);
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

  return (
    <PageFrame
      title="系统配置"
      summary="配置页已改成纯 API 表单，直接管理当前 `.env` 语义、保存校验和 Webhook 测试。"
      actions={
        <>
          <StatusBadge label={loading ? "读取中" : "已连接"} tone={loading ? "warning" : "success"} />
          <StatusBadge label={`字段 ${Object.keys(fields).length}`} tone="info" />
        </>
      }
    >
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.primaryButton} onClick={() => void handleSave()} type="button">
              保存配置
            </button>
            <button className={styles.secondaryButton} onClick={() => void handleWebhookTest()} type="button">
              测试 Webhook
            </button>
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
          </div>
        </section>

        <section className={styles.card}>
          <h2>Webhook 状态</h2>
          <pre className={styles.listItem}>{JSON.stringify(webhookStatus, null, 2)}</pre>
        </section>

        <section className={styles.card}>
          <h2>环境变量</h2>
          <div className={styles.formGrid}>
            {Object.entries(fields).map(([key, field]) => (
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
                    type={field.type === "password" ? "password" : "text"}
                    value={field.value}
                    onChange={(event) => setValue(key, event.target.value)}
                  />
                )}
                <small className={styles.muted}>{key}</small>
              </div>
            ))}
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
