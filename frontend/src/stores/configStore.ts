import { create } from "zustand";

import { apiFetch } from "../lib/api";


export interface ConfigField {
  value: string;
  description: string;
  required: boolean;
  type: string;
  options?: string[];
}

interface ConfigPayload {
  config: Record<string, ConfigField>;
  webhook_status: Record<string, unknown>;
}

interface ConfigState {
  fields: Record<string, ConfigField>;
  webhookStatus: Record<string, unknown>;
  loading: boolean;
  fetchConfig: () => Promise<void>;
  setValue: (key: string, value: string) => void;
  save: () => Promise<void>;
  testWebhook: () => Promise<string>;
}

export const useConfigStore = create<ConfigState>((set, get) => ({
  fields: {},
  webhookStatus: {},
  loading: false,

  fetchConfig: async () => {
    set({ loading: true });
    const data = await apiFetch<ConfigPayload>("/api/config");
    set({
      fields: data.config,
      webhookStatus: data.webhook_status,
      loading: false,
    });
  },

  setValue: (key, value) => {
    const current = get().fields[key];
    if (!current) {
      return;
    }
    set({
      fields: {
        ...get().fields,
        [key]: {
          ...current,
          value,
        },
      },
    });
  },

  save: async () => {
    const values = Object.fromEntries(
      Object.entries(get().fields).map(([key, field]) => [key, field.value]),
    );
    const data = await apiFetch<ConfigPayload>("/api/config", {
      method: "PUT",
      body: JSON.stringify({ values }),
    });
    set({
      fields: data.config,
      webhookStatus: data.webhook_status,
    });
  },

  testWebhook: async () => {
    await apiFetch<{ ok: boolean }>("/api/config/test-webhook", { method: "POST" });
    return "Webhook 测试已发送";
  },
}));
