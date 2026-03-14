import { useEffect, useMemo, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";

import { routeItems } from "../../app/routes";
import { ApiRequestError, apiFetch } from "../../lib/api";
import { useAuthStore } from "../../stores/authStore";
import { type ConfigField } from "../../stores/configStore";
import { useShellStore } from "../../stores/shellStore";
import { useTaskStore } from "../../stores/taskStore";
import { StatusBadge } from "../common/StatusBadge";
import styles from "./AppLayout.module.scss";

const MOBILE_BREAKPOINT = "(max-width: 960px)";

const LABELS = {
  close: "关闭",
  openMenu: "打开导航",
  expandNav: "展开导航",
  collapseNav: "收起导航",
  lightweightModel: "轻量模型",
  reasoningModel: "推理模型",
  savingModel: "正在保存模型配置...",
  modelHint: "切换后自动保存到当前 .env",
  noModelOptions: "暂无可用模型",
  lightweightSaved: "轻量模型已更新",
  reasoningSaved: "推理模型已更新",
  quickSettingsError: "快捷模型配置读取失败",
  saveModelError: "模型配置保存失败",
  logout: "退出登录",
};

type ModelFieldKey = "LIGHTWEIGHT_MODEL_NAME" | "REASONING_MODEL_NAME";

interface ConfigPayload {
  config: Record<string, ConfigField>;
}

function parseModelOptions(rawValue: string | undefined, currentValue: string | undefined) {
  const values = [currentValue ?? "", ...(rawValue ?? "").split(/[\n,]/)]
    .map((item) => item.trim())
    .filter(Boolean);

  return Array.from(new Set(values));
}

function resolveCurrentRoute(pathname: string) {
  const exactRoute = routeItems.find((item) => item.path === pathname);
  if (exactRoute) {
    return exactRoute;
  }

  return [...routeItems]
    .sort((left, right) => right.path.length - left.path.length)
    .find((item) => pathname.startsWith(item.path));
}

export function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const sidebarCollapsed = useShellStore((state) => state.sidebarCollapsed);
  const toggleSidebar = useShellStore((state) => state.toggleSidebar);
  const logout = useAuthStore((state) => state.logout);
  const activeTask = useTaskStore((state) => state.activeTask);
  const refreshTasks = useTaskStore((state) => state.refresh);
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(() =>
    typeof window !== "undefined" && window.matchMedia(MOBILE_BREAKPOINT).matches,
  );
  const [configFields, setConfigFields] = useState<Record<string, ConfigField>>({});
  const [configLoading, setConfigLoading] = useState(false);
  const [modelMessage, setModelMessage] = useState("");
  const [modelError, setModelError] = useState("");
  const [savingModelKey, setSavingModelKey] = useState<ModelFieldKey | null>(null);

  useEffect(() => {
    let cancelled = false;

    const loadQuickConfig = async () => {
      setConfigLoading(true);
      try {
        const data = await apiFetch<ConfigPayload>("/api/config");
        if (!cancelled) {
          setConfigFields(data.config);
          setModelError("");
        }
      } catch {
        if (!cancelled) {
          setModelError(LABELS.quickSettingsError);
        }
      } finally {
        if (!cancelled) {
          setConfigLoading(false);
        }
      }
    };

    void loadQuickConfig();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const taskTimer = window.setInterval(() => void refreshTasks(), 2000);
    return () => {
      window.clearInterval(taskTimer);
    };
  }, [refreshTasks]);

  useEffect(() => {
    const mediaQuery = window.matchMedia(MOBILE_BREAKPOINT);
    const handleMediaChange = (event: MediaQueryListEvent) => {
      setIsMobile(event.matches);
      if (!event.matches) {
        setMobileSidebarOpen(false);
      }
    };

    setIsMobile(mediaQuery.matches);
    mediaQuery.addEventListener("change", handleMediaChange);

    return () => {
      mediaQuery.removeEventListener("change", handleMediaChange);
    };
  }, []);

  useEffect(() => {
    if (isMobile) {
      setMobileSidebarOpen(false);
    }
  }, [isMobile, location.pathname]);

  useEffect(() => {
    if (!isMobile || !mobileSidebarOpen) {
      return undefined;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setMobileSidebarOpen(false);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    document.body.style.overflow = "hidden";

    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = "";
    };
  }, [isMobile, mobileSidebarOpen]);

  const groupedRoutes = useMemo(() => {
    const groups = new Map<string, typeof routeItems>();
    routeItems.filter((item) => !item.hidden).forEach((item) => {
      const existing = groups.get(item.group) ?? [];
      existing.push(item);
      groups.set(item.group, existing);
    });
    return Array.from(groups.entries());
  }, []);

  const currentRoute = useMemo(() => resolveCurrentRoute(location.pathname), [location.pathname]);
  const currentPageLabel = currentRoute ? `${currentRoute.group}-${currentRoute.title}` : "Stock Center";

  const lightweightModel = configFields.LIGHTWEIGHT_MODEL_NAME?.value ?? "";
  const reasoningModel = configFields.REASONING_MODEL_NAME?.value ?? "";
  const lightweightOptions = useMemo(
    () => parseModelOptions(configFields.LIGHTWEIGHT_MODEL_OPTIONS?.value, lightweightModel),
    [configFields.LIGHTWEIGHT_MODEL_OPTIONS?.value, lightweightModel],
  );
  const reasoningOptions = useMemo(
    () => parseModelOptions(configFields.REASONING_MODEL_OPTIONS?.value, reasoningModel),
    [configFields.REASONING_MODEL_OPTIONS?.value, reasoningModel],
  );
  const desktopSidebarHidden = !isMobile && sidebarCollapsed;
  const modelControlsDisabled = configLoading || Boolean(savingModelKey);

  const handleSidebarToggle = () => {
    if (isMobile) {
      setMobileSidebarOpen((value) => !value);
      return;
    }

    toggleSidebar();
  };

  const handleLogout = async () => {
    await logout();
    navigate("/login", { replace: true });
  };

  const handleModelChange = async (fieldKey: ModelFieldKey, value: string) => {
    if (!value || configFields[fieldKey]?.value === value) {
      return;
    }

    setModelMessage("");
    setModelError("");
    setSavingModelKey(fieldKey);

    try {
      const values = Object.fromEntries(
        Object.entries(configFields).map(([key, field]) => [key, field.value]),
      );
      const data = await apiFetch<ConfigPayload>("/api/config", {
        method: "PUT",
        body: JSON.stringify({
          values: {
            ...values,
            [fieldKey]: value,
          },
        }),
      });
      setConfigFields(data.config);
      setModelMessage(fieldKey === "LIGHTWEIGHT_MODEL_NAME" ? LABELS.lightweightSaved : LABELS.reasoningSaved);
    } catch (requestError) {
      setModelError(
        requestError instanceof ApiRequestError ? requestError.message : LABELS.saveModelError,
      );
    } finally {
      setSavingModelKey(null);
    }
  };

  return (
    <div
      className={`${styles.shell} ${desktopSidebarHidden ? styles.sidebarHidden : ""} ${
        isMobile && mobileSidebarOpen ? styles.mobileSidebarOpen : ""
      }`}
    >
      <button
        aria-hidden={!isMobile || !mobileSidebarOpen}
        className={styles.backdrop}
        onClick={() => setMobileSidebarOpen(false)}
        tabIndex={isMobile && mobileSidebarOpen ? 0 : -1}
        type="button"
      />
      <aside className={styles.sidebar}>
        <div className={styles.sidebarHeader}>
          <div className={styles.brand}>
            <h1>Stock Center</h1>
          </div>
          <button
            aria-label={LABELS.close}
            className={`${styles.iconButton} ${styles.sidebarCloseButton}`}
            onClick={() => setMobileSidebarOpen(false)}
            type="button"
          >
            ×
          </button>
        </div>
        <nav className={styles.nav}>
          {groupedRoutes.map(([group, items]) => (
            <section key={group} className={styles.navGroup}>
              <p className={styles.groupTitle}>{group}</p>
              {items.map((item) => (
                <NavLink
                  key={item.path}
                  className={({ isActive }) => `${styles.navItem} ${isActive ? styles.navItemActive : ""}`}
                  onClick={() => {
                    if (isMobile) {
                      setMobileSidebarOpen(false);
                    }
                  }}
                  to={item.path}
                >
                  <span>{item.title}</span>
                </NavLink>
              ))}
              {group === "系统管理" ? (
                <section className={styles.sidebarTools}>
                  <div className={styles.sidebarField}>
                    <label htmlFor="sidebar-lightweight-model">{LABELS.lightweightModel}</label>
                    <select
                      className={styles.sidebarSelect}
                      disabled={modelControlsDisabled || !lightweightOptions.length}
                      id="sidebar-lightweight-model"
                      onChange={(event) => void handleModelChange("LIGHTWEIGHT_MODEL_NAME", event.target.value)}
                      value={lightweightModel}
                    >
                      {lightweightOptions.length ? (
                        lightweightOptions.map((item) => (
                          <option key={item} value={item}>
                            {item}
                          </option>
                        ))
                      ) : (
                        <option value="">{LABELS.noModelOptions}</option>
                      )}
                    </select>
                  </div>
                  <div className={styles.sidebarField}>
                    <label htmlFor="sidebar-reasoning-model">{LABELS.reasoningModel}</label>
                    <select
                      className={styles.sidebarSelect}
                      disabled={modelControlsDisabled || !reasoningOptions.length}
                      id="sidebar-reasoning-model"
                      onChange={(event) => void handleModelChange("REASONING_MODEL_NAME", event.target.value)}
                      value={reasoningModel}
                    >
                      {reasoningOptions.length ? (
                        reasoningOptions.map((item) => (
                          <option key={item} value={item}>
                            {item}
                          </option>
                        ))
                      ) : (
                        <option value="">{LABELS.noModelOptions}</option>
                      )}
                    </select>
                  </div>
                  <p className={styles.sidebarHint}>{savingModelKey ? LABELS.savingModel : LABELS.modelHint}</p>
                  {modelMessage ? <p className={styles.sidebarSuccess}>{modelMessage}</p> : null}
                  {modelError ? <p className={styles.sidebarError}>{modelError}</p> : null}
                </section>
              ) : null}
            </section>
          ))}
        </nav>
        <div className={styles.sidebarFooter}>
          <button className={styles.sidebarLogoutButton} onClick={() => void handleLogout()} type="button">
            {LABELS.logout}
          </button>
        </div>
      </aside>
      <div className={styles.main}>
        <header className={styles.topbar}>
          <div className={styles.topbarLeft}>
            <button
              aria-label={isMobile ? LABELS.openMenu : sidebarCollapsed ? LABELS.expandNav : LABELS.collapseNav}
              className={styles.iconButton}
              onClick={handleSidebarToggle}
              type="button"
            >
              ≡
            </button>
            <div className={styles.topbarTitleWrap}>
              <strong className={styles.topbarTitle}>{currentPageLabel}</strong>
            </div>
          </div>
          <div className={styles.topbarRight}>
            {activeTask ? (
              <StatusBadge
                label={`${activeTask.label} ${Math.round((activeTask.progress ?? 0) * 100)}%`}
                tone="warning"
              />
            ) : null}
          </div>
        </header>
        <main className={styles.content}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}
