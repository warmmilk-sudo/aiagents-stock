import styles from "../../pages/ConsolePage.module.scss";

interface PageFeedbackProps {
  message?: string;
  error?: string;
}

export function PageFeedback({ error }: PageFeedbackProps) {
  if (!error) {
    return null;
  }

  return (
    <section className={`${styles.card} ${styles.feedbackCard}`}>
      <div className={`${styles.feedbackText} ${styles.dangerText}`}>{error}</div>
    </section>
  );
}
