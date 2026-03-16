import type { ReactNode } from "react";

import styles from "../../pages/ConsolePage.module.scss";

interface ModuleCardProps {
  title: string;
  summary?: string;
  toolbar?: ReactNode;
  footer?: ReactNode;
  className?: string;
  hideTitleOnMobile?: boolean;
  children: ReactNode;
}

export function ModuleCard({
  title,
  toolbar,
  footer,
  className = "",
  hideTitleOnMobile = false,
  children,
}: ModuleCardProps) {
  const headerClassName = [
    styles.moduleHeader,
    hideTitleOnMobile ? styles.moduleHeaderMobileTitleHidden : "",
    !toolbar ? styles.moduleHeaderTitleOnly : "",
  ].filter(Boolean).join(" ");
  const headingClassName = [
    styles.moduleHeading,
    hideTitleOnMobile ? styles.moduleHeadingMobileHidden : "",
  ].filter(Boolean).join(" ");

  return (
    <section className={`${styles.card} ${styles.moduleCard} ${className}`.trim()}>
      <div className={headerClassName}>
        <div className={headingClassName}>
          <h2>{title}</h2>
        </div>
        {toolbar ? <div className={styles.moduleToolbar}>{toolbar}</div> : null}
      </div>
      <div className={styles.moduleBody}>{children}</div>
      {footer ? <div className={styles.moduleFooter}>{footer}</div> : null}
    </section>
  );
}
