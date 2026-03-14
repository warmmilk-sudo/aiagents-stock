import { create } from "zustand";


interface DraftPosition {
  code: string;
  name?: string;
  cost_price?: number;
  quantity?: number;
}

interface PortfolioState {
  selectedAccount: string;
  draftPosition: DraftPosition | null;
  setSelectedAccount: (account: string) => void;
  setDraftPosition: (draft: DraftPosition | null) => void;
}

export const usePortfolioStore = create<PortfolioState>((set) => ({
  selectedAccount: "全部账户",
  draftPosition: null,
  setSelectedAccount: (selectedAccount) => set({ selectedAccount }),
  setDraftPosition: (draftPosition) => set({ draftPosition }),
}));
