import { create } from "zustand";
import { persist } from "zustand/middleware";


interface ShellState {
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;
}

export const useShellStore = create<ShellState>()(
  persist(
    (set, get) => ({
      sidebarCollapsed: false,
      toggleSidebar: () => {
        set({ sidebarCollapsed: !get().sidebarCollapsed });
      },
    }),
    {
      name: "aiagents-shell",
      partialize: (state) => ({
        sidebarCollapsed: state.sidebarCollapsed,
      }),
    },
  ),
);
