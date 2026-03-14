import { create } from "zustand";

import { apiFetch } from "../lib/api";


export interface TaskSummary {
  id: string;
  label: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
}

interface TaskState {
  activeTask: TaskSummary | null;
  latestTask: TaskSummary | null;
  refresh: () => Promise<void>;
}

export const useTaskStore = create<TaskState>((set) => ({
  activeTask: null,
  latestTask: null,
  refresh: async () => {
    try {
      const [activeTask, latestTask] = await Promise.all([
        apiFetch<TaskSummary | null>("/api/tasks/active"),
        apiFetch<TaskSummary | null>("/api/tasks/latest"),
      ]);
      set({ activeTask, latestTask });
    } catch {
      set({ activeTask: null, latestTask: null });
    }
  },
}));
