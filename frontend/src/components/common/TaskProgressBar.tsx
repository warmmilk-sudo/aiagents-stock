import styles from "./TaskProgressBar.module.scss";

type Tone = "running" | "success" | "danger";

interface TaskProgressBarProps {
  current?: number;
  total?: number;
  message?: string;
  tone?: Tone;
  showCounter?: boolean;
}

export function TaskProgressBar({
  current = 0,
  total = 0,
  message = "",
  tone = "running",
  showCounter = true,
}: TaskProgressBarProps) {
  const resolvedTotal = total > 0 ? total : 0;
  const resolvedCurrent = Math.max(0, current);
  const ratio = resolvedTotal > 0 ? Math.min(100, Math.max(0, (resolvedCurrent / resolvedTotal) * 100)) : 0;

  return (
    <div className={styles.wrap}>
      <div aria-label={`当前进度 ${resolvedCurrent}/${resolvedTotal || 0}`} className={styles.track} role="progressbar">
        <div className={`${styles.fill} ${styles[tone]}`} style={{ width: `${ratio}%` }} />
      </div>
      <div className={styles.meta}>
        {showCounter ? <span>{resolvedCurrent} / {resolvedTotal || 0}</span> : null}
        {message ? <span>{message}</span> : null}
      </div>
    </div>
  );
}
