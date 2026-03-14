import { create } from "zustand";


type ResearchIntentType =
  | "watchlist"
  | "portfolio"
  | "price_alert"
  | "history_detail"
  | "analysis_baseline";

interface ResearchIntent {
  type: ResearchIntentType | string;
  symbol?: string;
  payload?: unknown;
}

interface ResearchState {
  pendingIntent: ResearchIntent | null;
  setIntent: (intent: ResearchIntent | null) => void;
  consumeIntent: () => ResearchIntent | null;
}

export const useResearchStore = create<ResearchState>((set, get) => ({
  pendingIntent: null,
  setIntent: (intent) => set({ pendingIntent: intent }),
  consumeIntent: () => {
    const current = get().pendingIntent;
    set({ pendingIntent: null });
    return current;
  },
}));
