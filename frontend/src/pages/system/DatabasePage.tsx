import { useEffect, useState } from "react";

import { PageFrame } from "../../components/common/PageFrame";
import { ApiRequestError, apiFetch } from "../../lib/api";
import styles from "../ConsolePage.module.scss";

interface DatabaseFileInfo {
  name: string;
  label: string;
  size_bytes: number;
  updated_at: string;
}

interface BackupInfo {
  name: string;
  created_at: string;
  file_count: number;
  size_bytes: number;
}

interface DatabaseStatusPayload {
  databases: DatabaseFileInfo[];
  backups: BackupInfo[];
}

function fileSizeText(value: number) {
  const size = Number(value) || 0;
  if (size >= 1024 * 1024 * 1024) {
    return `${(size / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  }
  if (size >= 1024 * 1024) {
    return `${(size / (1024 * 1024)).toFixed(2)} MB`;
  }
  if (size >= 1024) {
    return `${(size / 1024).toFixed(1)} KB`;
  }
  return `${size} B`;
}

export function DatabasePage() {
  const [status, setStatus] = useState<DatabaseStatusPayload>({ databases: [], backups: [] });
  const [cleanupDays, setCleanupDays] = useState("7");
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const loadStatus = async () => {
    const data = await apiFetch<DatabaseStatusPayload>("/api/system/database");
    setStatus(data);
  };

  useEffect(() => {
    void loadStatus();
  }, []);

  const withAction = async (action: () => Promise<void>) => {
    setLoading(true);
    setMessage("");
    setError("");
    try {
      await action();
      await loadStatus();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "操作失败");
    } finally {
      setLoading(false);
    }
  };

  return (
    <PageFrame title="数据库管理">
      <div className={styles.stack}>
        <section className={styles.card}>
          <div className={styles.actions}>
            <button className={styles.secondaryButton} disabled={loading} onClick={() => void withAction(loadStatus)} type="button">
              刷新状态
            </button>
            {message ? <span className={styles.successText}>{message}</span> : null}
            {error ? <span className={styles.dangerText}>{error}</span> : null}
          </div>
        </section>

        <section className={styles.card}>
          <h2>数据库文件</h2>
          <div className={styles.compactGrid}>
            {status.databases.map((item) => (
              <div className={styles.metric} key={item.name}>
                <span className={styles.muted}>{item.label}</span>
                <strong>{item.name}</strong>
                <div className={styles.muted}>大小：{fileSizeText(item.size_bytes)}</div>
                <div className={styles.muted}>更新时间：{item.updated_at}</div>
              </div>
            ))}
            {!status.databases.length ? <div className={styles.muted}>暂无数据库文件</div> : null}
          </div>
        </section>

        <section className={styles.card}>
          <h2>历史清理</h2>
          <p className={styles.helperText}>按天数清理分析与监测历史，不会删除当前持仓、任务配置和系统参数。</p>
          <div className={styles.responsiveActionGrid}>
            <div className={styles.field}>
              <label htmlFor="cleanupDays">清理多少天前的数据</label>
              <select id="cleanupDays" onChange={(event) => setCleanupDays(event.target.value)} value={cleanupDays}>
                {["7", "15", "30", "90", "180", "365"].map((value) => (
                  <option key={value} value={value}>
                    {value} 天前
                  </option>
                ))}
              </select>
            </div>
            <button
              className={styles.primaryButton}
              disabled={loading}
              onClick={() =>
                void withAction(async () => {
                  const result = await apiFetch<{ total_deleted_rows: number; cutoff: string }>("/api/system/database/cleanup", {
                    method: "POST",
                    body: JSON.stringify({ days: Number(cleanupDays) || 7 }),
                  });
                  setMessage(`历史数据已清理，共删除 ${result.total_deleted_rows} 条，截止时间 ${result.cutoff}`);
                })
              }
              type="button"
            >
              清理历史数据
            </button>
          </div>
        </section>

        <section className={styles.card}>
          <h2>备份与还原</h2>
          <div className={styles.actions}>
            <button
              className={styles.primaryButton}
              disabled={loading}
              onClick={() =>
                void withAction(async () => {
                  const result = await apiFetch<BackupInfo>("/api/system/database/backup", { method: "POST" });
                  setMessage(`备份已创建：${result.name}`);
                })
              }
              type="button"
            >
              创建备份
            </button>
          </div>
          <div className={styles.list} style={{ marginTop: 14 }}>
            {status.backups.map((item) => (
              <div className={styles.listItem} key={item.name}>
                <div className={styles.noticeMeta}>
                  <strong>{item.name}</strong>
                  <span className={styles.muted}>{item.created_at}</span>
                </div>
                <div className={styles.muted}>文件数：{item.file_count} | 大小：{fileSizeText(item.size_bytes)}</div>
                <div className={styles.actions} style={{ marginTop: 10 }}>
                  <button
                    className={styles.secondaryButton}
                    disabled={loading}
                    onClick={() =>
                      void withAction(async () => {
                        await apiFetch("/api/system/database/restore", {
                          method: "POST",
                          body: JSON.stringify({ backup_name: item.name }),
                        });
                        setMessage(`已从备份 ${item.name} 还原数据`);
                      })
                    }
                    type="button"
                  >
                    还原此备份
                  </button>
                </div>
              </div>
            ))}
            {!status.backups.length ? <div className={styles.muted}>暂无备份记录</div> : null}
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
