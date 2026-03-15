import { useEffect, useState } from "react";

import styles from "../../pages/ConsolePage.module.scss";

interface PageFeedbackProps {
  message?: string;
  error?: string;
}

const FEEDBACK_VISIBLE_MS = 5000;

export function PageFeedback({ message, error }: PageFeedbackProps) {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (!message && !error) {
      setVisible(false);
      return undefined;
    }

    setVisible(true);
    const timer = window.setTimeout(() => {
      setVisible(false);
    }, FEEDBACK_VISIBLE_MS);

    return () => {
      window.clearTimeout(timer);
    };
  }, [message, error]);

  if ((!message && !error) || !visible) {
    return null;
  }

  return (
    <section className={`${styles.card} ${styles.feedbackCard}`}>
      {message ? <div className={`${styles.feedbackText} ${styles.successText}`}>{message}</div> : null}
      {error ? <div className={`${styles.feedbackText} ${styles.dangerText}`}>{error}</div> : null}
    </section>
  );
}
