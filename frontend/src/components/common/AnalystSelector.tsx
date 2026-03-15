import { ANALYST_OPTIONS, type AnalystKey } from "../../constants/analysts";
import styles from "../../pages/ConsolePage.module.scss";

interface AnalystSelectorProps {
  value: string[];
  options?: Array<{ key: string; label: string }>;
  onChange: (next: string[]) => void;
  columns?: 1 | 2;
}

export function AnalystSelector({
  value,
  options = [...ANALYST_OPTIONS],
  onChange,
  columns = 2,
}: AnalystSelectorProps) {
  const selected = new Set(value);

  const toggleValue = (key: string, checked: boolean) => {
    const next = checked
      ? Array.from(new Set([...value, key]))
      : value.filter((item) => item !== key);
    onChange(next);
  };

  return (
    <div
      className={`${styles.analystSelectionGroup} ${
        columns === 1 ? styles.analystSelectionColumns1 : styles.analystSelectionColumns2
      }`}
    >
      {options.map((item) => (
        <label className={styles.analystOption} htmlFor={`analyst-${item.key}`} key={item.key}>
          <input
            checked={selected.has(item.key as AnalystKey)}
            id={`analyst-${item.key}`}
            onChange={(event) => toggleValue(item.key, event.target.checked)}
            type="checkbox"
          />
          <span>{item.label}</span>
        </label>
      ))}
    </div>
  );
}
