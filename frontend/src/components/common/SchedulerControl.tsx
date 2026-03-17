import type { ReactNode } from "react";

import { DEFAULT_RUN_ONCE_LABEL, DEFAULT_SAVE_LABEL } from "../../constants/scheduler";
import styles from "../../pages/ConsolePage.module.scss";

interface SchedulerControlProps {
  enabled: boolean;
  label: string;
  scheduleFields: ReactNode;
  statusFields?: ReactNode;
  showToggle?: boolean;
  onToggle: (next: boolean) => void | Promise<void>;
  onSave?: () => void | Promise<void>;
  onRunOnce?: () => void | Promise<void>;
  saveLabel?: string;
  runOnceLabel?: string;
  runOnceDisabled?: boolean;
}

export function SchedulerControl({
  enabled,
  label,
  scheduleFields,
  statusFields,
  showToggle = true,
  onToggle,
  onSave,
  onRunOnce,
  saveLabel = DEFAULT_SAVE_LABEL,
  runOnceLabel = DEFAULT_RUN_ONCE_LABEL,
  runOnceDisabled = false,
}: SchedulerControlProps) {
  return (
    <div className={styles.schedulerControl}>
      {statusFields ? <div className={styles.moduleSection}>{statusFields}</div> : null}
      <div className={styles.moduleSection}>{scheduleFields}</div>
      <div className={styles.schedulerControlActions}>
        {onSave ? (
          <button className={styles.secondaryButton} onClick={() => void onSave()} type="button">
            {saveLabel}
          </button>
        ) : null}
        {showToggle ? (
          <label className={styles.switchField}>
            <span className={styles.switchLabel}>{label}</span>
            <span className={styles.switchControl}>
              <input checked={enabled} onChange={(event) => void onToggle(event.target.checked)} type="checkbox" />
              <span className={styles.switchTrack} aria-hidden="true">
                <span className={styles.switchThumb} />
              </span>
            </span>
          </label>
        ) : null}
        {onRunOnce ? (
          <button className={styles.primaryButton} disabled={runOnceDisabled} onClick={() => void onRunOnce()} type="button">
            {runOnceLabel}
          </button>
        ) : null}
      </div>
    </div>
  );
}
