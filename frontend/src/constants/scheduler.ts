export const DEFAULT_SCHEDULER_TIME = "09:30";
export const DEFAULT_SCHEDULER_WORKERS = 3;
export const DEFAULT_SAVE_LABEL = "保存配置";
export const DEFAULT_RUN_ONCE_LABEL = "立即执行";

export function schedulerModeLabel(value?: string) {
  return value === "parallel" ? "并行分析" : "顺序分析";
}
