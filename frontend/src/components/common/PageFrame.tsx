import type { ReactNode } from "react";

import styles from "./PageFrame.module.scss";

export interface PageFrameSectionTab {
  key: string;
  label: string;
}

interface PageFrameProps {
  title: string;
  summary?: string;
  actions?: ReactNode;
  sectionTabs?: PageFrameSectionTab[];
  activeSectionKey?: string;
  onSectionChange?: (key: string) => void;
  children: ReactNode;
}

export function PageFrame({
  title,
  actions,
  sectionTabs,
  activeSectionKey,
  onSectionChange,
  children,
}: PageFrameProps) {
  return (
    <section className={styles.frame}>
      {actions ? <header className={styles.header}>{actions ? <div className={styles.actions}>{actions}</div> : null}</header> : null}
      {sectionTabs?.length ? (
        <div className={styles.sectionTabsWrap}>
          <div className={styles.sectionTabs} aria-label={`${title}页面分区`} role="tablist">
            {sectionTabs.map((item) => (
              <button
                aria-selected={item.key === activeSectionKey}
                className={`${styles.sectionTabButton} ${
                  item.key === activeSectionKey ? styles.sectionTabButtonActive : ""
                }`}
                key={item.key}
                onClick={() => onSectionChange?.(item.key)}
                role="tab"
                type="button"
              >
                {item.label}
              </button>
            ))}
          </div>
        </div>
      ) : null}
      <div className={styles.body}>{children}</div>
    </section>
  );
}
