import type { ReactNode } from "react";

import styles from "../../pages/ConsolePage.module.scss";

interface ModuleCardProps {
  title: string;
  summary?: string;
  toolbar?: ReactNode;
  footer?: ReactNode;
  className?: string;
  children: ReactNode;
}

export function ModuleCard({
  title,
  toolbar,
  footer,
  className = "",
  children,
}: ModuleCardProps) {
  return (
    <section className={`${styles.card} ${styles.moduleCard} ${className}`.trim()}>
      <div className={styles.moduleHeader}>
        <div className={styles.moduleHeading}>
          <h2>{title}</h2>
        </div>
        {toolbar ? <div className={styles.moduleToolbar}>{toolbar}</div> : null}
      </div>
      <div className={styles.moduleBody}>{children}</div>
      {footer ? <div className={styles.moduleFooter}>{footer}</div> : null}
    </section>
  );
}
