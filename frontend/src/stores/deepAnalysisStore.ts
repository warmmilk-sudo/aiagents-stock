import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

export interface DeepAnalysisAnalystConfig {
  technical: boolean;
  fundamental: boolean;
  fund_flow: boolean;
  risk: boolean;
  sentiment: boolean;
  news: boolean;
}

interface DeepAnalysisState {
  batchMode: "顺序分析" | "多线程并行";
  maxWorkers: number;
  analysts: DeepAnalysisAnalystConfig;
  setBatchMode: (batchMode: "顺序分析" | "多线程并行") => void;
  setMaxWorkers: (maxWorkers: number) => void;
  setAnalysts: (analysts: DeepAnalysisAnalystConfig) => void;
}

const defaultAnalysts: DeepAnalysisAnalystConfig = {
  technical: true,
  fundamental: true,
  fund_flow: true,
  risk: true,
  sentiment: false,
  news: false,
};

export const useDeepAnalysisStore = create<DeepAnalysisState>()(
  persist(
    (set) => ({
      batchMode: "顺序分析",
      maxWorkers: 3,
      analysts: defaultAnalysts,
      setBatchMode: (batchMode) => set({ batchMode }),
      setMaxWorkers: (maxWorkers) => set({ maxWorkers }),
      setAnalysts: (analysts) => set({ analysts }),
    }),
    {
      name: "deep-analysis-ui-state",
      storage: createJSONStorage(() => localStorage),
    },
  ),
);
