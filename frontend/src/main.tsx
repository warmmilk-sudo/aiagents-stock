import { Component, type ErrorInfo, type ReactNode } from "react";
import ReactDOM from "react-dom/client";
import { RouterProvider } from "react-router-dom";

import { router } from "./app/router";
import "./styles/global.scss";

const CHUNK_ERROR_PATTERNS = [
  "Failed to fetch dynamically imported module",
  "Importing a module script failed",
  "ChunkLoadError",
];
const CHUNK_RELOAD_KEY = "stock_center.chunk_reload_attempt";

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === "string") {
    return error;
  }

  if (error && typeof error === "object") {
    const maybeMessage = (error as { message?: unknown }).message;
    if (typeof maybeMessage === "string") {
      return maybeMessage;
    }
  }

  return "";
}

function isChunkLoadError(error: unknown) {
  const message = getErrorMessage(error);
  return CHUNK_ERROR_PATTERNS.some((pattern) => message.includes(pattern));
}

function shouldReloadChunkError() {
  try {
    const rawValue = window.sessionStorage.getItem(CHUNK_RELOAD_KEY);
    if (!rawValue) {
      return true;
    }

    const state = JSON.parse(rawValue) as { pathname?: string; timestamp?: number };
    return state.pathname !== window.location.pathname;
  } catch {
    return true;
  }
}

function markChunkReloadAttempt() {
  window.sessionStorage.setItem(
    CHUNK_RELOAD_KEY,
    JSON.stringify({
      pathname: window.location.pathname,
      timestamp: Date.now(),
    }),
  );
}

function showChunkLoadFallback() {
  const root = document.getElementById("root");
  if (!root) {
    return;
  }

  root.innerHTML = `
    <div style="min-height:100vh;display:flex;align-items:center;justify-content:center;padding:32px;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f6f7fb;color:#1f2937;">
      <div style="max-width:480px;width:100%;background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:24px;box-shadow:0 10px 30px rgba(15,23,42,0.08);">
        <div style="font-size:18px;font-weight:600;margin-bottom:12px;">页面资源加载失败</div>
        <div style="font-size:14px;line-height:1.8;color:#4b5563;margin-bottom:20px;">
          当前页面需要的前端模块未能加载，通常是部署后静态资源版本不一致导致的。请刷新页面重试。
        </div>
        <button id="chunk-reload-button" style="appearance:none;border:none;border-radius:10px;background:#111827;color:#fff;padding:10px 16px;font-size:14px;cursor:pointer;">
          刷新页面
        </button>
      </div>
    </div>
  `;

  const button = document.getElementById("chunk-reload-button");
  button?.addEventListener("click", () => {
    window.location.reload();
  });
}

function handleChunkLoadFailure(error: unknown) {
  if (!isChunkLoadError(error)) {
    return false;
  }

  if (shouldReloadChunkError()) {
    markChunkReloadAttempt();
    window.location.reload();
  } else {
    showChunkLoadFallback();
  }

  return true;
}

interface AppErrorBoundaryProps {
  children: ReactNode;
}

interface AppErrorBoundaryState {
  error: Error | null;
  componentStack: string;
}

class AppErrorBoundary extends Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
  state: AppErrorBoundaryState = {
    error: null,
    componentStack: "",
  };

  static getDerivedStateFromError(error: Error) {
    return {
      error,
      componentStack: "",
    };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("[AppErrorBoundary] uncaught render error", error, info);
    this.setState({
      error,
      componentStack: info.componentStack || "",
    });
  }

  render() {
    if (!this.state.error) {
      return this.props.children;
    }

    return (
      <div
        style={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "32px",
          fontFamily: "system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif",
          background: "#0f1115",
          color: "#e5e7eb",
        }}
      >
        <div
          style={{
            maxWidth: "760px",
            width: "100%",
            background: "#151922",
            border: "1px solid rgba(255,255,255,0.08)",
            borderRadius: "16px",
            padding: "24px",
            boxShadow: "0 18px 42px rgba(0,0,0,0.35)",
          }}
        >
          <div style={{ fontSize: "20px", fontWeight: 700, marginBottom: "12px" }}>页面运行时异常</div>
          <div style={{ fontSize: "14px", lineHeight: 1.8, color: "#cbd5e1", marginBottom: "20px" }}>
            页面已经捕获到前端异常。先刷新一次；如果仍然复现，请把下面的错误信息发出来，避免只看到压缩后的
            React 调用栈。
          </div>
          <div
            style={{
              borderRadius: "12px",
              border: "1px solid rgba(255,255,255,0.08)",
              background: "#0f141d",
              padding: "14px 16px",
              marginBottom: "16px",
              wordBreak: "break-word",
            }}
          >
            {this.state.error.message || "未知异常"}
          </div>
          <div style={{ display: "flex", gap: "12px", marginBottom: "16px" }}>
            <button
              onClick={() => window.location.reload()}
              style={{
                appearance: "none",
                border: "none",
                borderRadius: "10px",
                background: "#e5c9a8",
                color: "#111827",
                padding: "10px 16px",
                fontSize: "14px",
                cursor: "pointer",
                fontWeight: 600,
              }}
              type="button"
            >
              刷新页面
            </button>
          </div>
          <details>
            <summary style={{ cursor: "pointer", color: "#cbd5e1" }}>展开技术详情</summary>
            <pre
              style={{
                marginTop: "12px",
                padding: "14px 16px",
                borderRadius: "12px",
                background: "#0b0f16",
                color: "#d1d5db",
                overflowX: "auto",
                whiteSpace: "pre-wrap",
                fontSize: "12px",
                lineHeight: 1.6,
              }}
            >
              {[this.state.error.stack, this.state.componentStack].filter(Boolean).join("\n\n")}
            </pre>
          </details>
        </div>
      </div>
    );
  }
}

window.addEventListener("error", (event) => {
  handleChunkLoadFailure(event.error ?? event.message);
});

window.addEventListener("unhandledrejection", (event) => {
  if (handleChunkLoadFailure(event.reason)) {
    event.preventDefault();
  }
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <AppErrorBoundary>
    <RouterProvider router={router} />
  </AppErrorBoundary>,
);
