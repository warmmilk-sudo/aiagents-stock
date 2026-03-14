import { create } from "zustand";


interface SmartMonitorState {
  enabledOnly: boolean;
  setEnabledOnly: (enabledOnly: boolean) => void;
}

export const useSmartMonitorStore = create<SmartMonitorState>((set) => ({
  enabledOnly: false,
  setEnabledOnly: (enabledOnly) => set({ enabledOnly }),
}));
