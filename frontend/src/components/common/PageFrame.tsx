import type { ReactNode } from "react";

import styles from "./PageFrame.module.scss";


interface PageFrameProps {
  title: string;
  summary?: string;
  actions?: ReactNode;
  children: ReactNode;
}

export function PageFrame({ title, summary, actions, children }: PageFrameProps) {
  return (
    <section className={styles.frame}>
      <header className={styles.header}>
        <div>
          <p className={styles.eyebrow}>AIAGENTS STOCK</p>
          <h1>{title}</h1>
          {summary ? <p className={styles.summary}>{summary}</p> : null}
        </div>
        {actions ? <div className={styles.actions}>{actions}</div> : null}
      </header>
      <div className={styles.body}>{children}</div>
    </section>
  );
}
