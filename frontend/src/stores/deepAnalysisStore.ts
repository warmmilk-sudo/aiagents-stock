import { create } from "zustand";
import { persist } from "zustand/middleware";

import { createSafeJSONStorage } from "../persistStorage";

export interface DeepAnalysisAnalystConfig {
  technical: boolean;
  fundamental: boolean;
  fund_flow: boolean;
  risk: boolean;
  sentiment: boolean;
  news: boolean;
}

interface DeepAnalysisState {
  analysts: DeepAnalysisAnalystConfig;
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
      analysts: defaultAnalysts,
      setAnalysts: (analysts) => set({ analysts }),
    }),
    {
      name: "deep-analysis-ui-state",
      storage: createSafeJSONStorage<DeepAnalysisState>(),
    },
  ),
);
