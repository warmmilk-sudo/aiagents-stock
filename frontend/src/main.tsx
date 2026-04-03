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

window.addEventListener("error", (event) => {
  handleChunkLoadFailure(event.error ?? event.message);
});

window.addEventListener("unhandledrejection", (event) => {
  if (handleChunkLoadFailure(event.reason)) {
    event.preventDefault();
  }
});

ReactDOM.createRoot(document.getElementById("root")!).render(<RouterProvider router={router} />);
