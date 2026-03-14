import styles from "./StatusBadge.module.scss";


type Tone = "default" | "success" | "warning" | "danger" | "info";

interface StatusBadgeProps {
  label: string;
  tone?: Tone;
}

export function StatusBadge({ label, tone = "default" }: StatusBadgeProps) {
  return <span className={`${styles.badge} ${styles[tone]}`}>{label}</span>;
}
