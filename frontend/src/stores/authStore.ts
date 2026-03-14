import { create } from "zustand";

import { apiFetch } from "../lib/api";


interface SessionInfo {
  issued_at: number;
  expires_at: number;
  fingerprint: string;
  session_key: string;
}

interface LockInfo {
  failed_attempts: number;
  lock_until: number;
}

interface SessionResponse {
  authenticated: boolean;
  session: SessionInfo | null;
  lock: LockInfo;
}

interface LoginResponse {
  authenticated: boolean;
  issued_at: number;
  expires_at: number;
  fingerprint: string;
  session_key: string;
}

interface AuthState {
  checking: boolean;
  authenticated: boolean;
  session: SessionInfo | null;
  lock: LockInfo | null;
  hydrate: () => Promise<void>;
  login: (password: string) => Promise<void>;
  logout: () => Promise<void>;
}

export const useAuthStore = create<AuthState>((set) => ({
  checking: true,
  authenticated: false,
  session: null,
  lock: null,

  hydrate: async () => {
    try {
      const data = await apiFetch<SessionResponse>("/api/auth/session");
      set({
        checking: false,
        authenticated: data.authenticated,
        session: data.session,
        lock: data.lock,
      });
    } catch {
      set({
        checking: false,
        authenticated: false,
        session: null,
      });
    }
  },

  login: async (password: string) => {
    const data = await apiFetch<LoginResponse>("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ password }),
    });
    set({
      checking: false,
      authenticated: data.authenticated,
      session: {
        issued_at: data.issued_at,
        expires_at: data.expires_at,
        fingerprint: data.fingerprint,
        session_key: data.session_key,
      },
      lock: null,
    });
  },

  logout: async () => {
    await apiFetch("/api/auth/logout", { method: "POST" });
    set({
      authenticated: false,
      session: null,
    });
  },
}));
