import { create } from "zustand";
import { persist } from "zustand/middleware";


type ThemeMode = "dawn" | "ink";

interface ShellState {
  theme: ThemeMode;
  sidebarCollapsed: boolean;
  toggleTheme: () => void;
  toggleSidebar: () => void;
}

export const useShellStore = create<ShellState>()(
  persist(
    (set, get) => ({
      theme: "dawn",
      sidebarCollapsed: false,
      toggleTheme: () => {
        set({ theme: get().theme === "dawn" ? "ink" : "dawn" });
      },
      toggleSidebar: () => {
        set({ sidebarCollapsed: !get().sidebarCollapsed });
      },
    }),
    {
      name: "aiagents-shell",
      partialize: (state) => ({
        theme: state.theme,
        sidebarCollapsed: state.sidebarCollapsed,
      }),
    },
  ),
);
