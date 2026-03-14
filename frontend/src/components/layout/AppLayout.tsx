import { useEffect, useMemo, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";

import { routeItems } from "../../app/routes";
import { apiFetch } from "../../lib/api";
import { useAuthStore } from "../../stores/authStore";
import { useShellStore } from "../../stores/shellStore";
import { useTaskStore } from "../../stores/taskStore";
import { StatusBadge } from "../common/StatusBadge";
import styles from "./AppLayout.module.scss";


interface SystemStatus {
  api_key_configured: boolean;
  monitor_service?: {
    running?: boolean;
  };
  portfolio_scheduler?: {
    is_running?: boolean;
  };
}

export function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const theme = useShellStore((state) => state.theme);
  const sidebarCollapsed = useShellStore((state) => state.sidebarCollapsed);
  const toggleSidebar = useShellStore((state) => state.toggleSidebar);
  const toggleTheme = useShellStore((state) => state.toggleTheme);
  const logout = useAuthStore((state) => state.logout);
  const activeTask = useTaskStore((state) => state.activeTask);
  const refreshTasks = useTaskStore((state) => state.refresh);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  useEffect(() => {
    let cancelled = false;

    const loadStatus = async () => {
      try {
        const data = await apiFetch<SystemStatus>("/api/system/status");
        if (!cancelled) {
          setSystemStatus(data);
        }
      } catch {
        if (!cancelled) {
          setSystemStatus(null);
        }
      }
    };

    void loadStatus();
    const taskTimer = window.setInterval(() => void refreshTasks(), 2000);
    const statusTimer = window.setInterval(() => void loadStatus(), 10000);

    return () => {
      cancelled = true;
      window.clearInterval(taskTimer);
      window.clearInterval(statusTimer);
    };
  }, [refreshTasks]);

  const groupedRoutes = useMemo(() => {
    const groups = new Map<string, typeof routeItems>();
    routeItems.forEach((item) => {
      const existing = groups.get(item.group) ?? [];
      existing.push(item);
      groups.set(item.group, existing);
    });
    return Array.from(groups.entries());
  }, []);

  const handleLogout = async () => {
    await logout();
    navigate("/login", { replace: true });
  };

  return (
    <div className={`${styles.shell} ${sidebarCollapsed ? styles.collapsed : ""}`}>
      <aside className={styles.sidebar}>
        <div className={styles.brand}>
          <p>AI AGENTS</p>
          <h1>Stock Center</h1>
        </div>
        <nav className={styles.nav}>
          {groupedRoutes.map(([group, items]) => (
            <section key={group} className={styles.navGroup}>
              <p className={styles.groupTitle}>{group}</p>
              {items.map((item) => (
                <NavLink
                  key={item.path}
                  className={({ isActive }) =>
                    `${styles.navItem} ${isActive ? styles.navItemActive : ""}`
                  }
                  to={item.path}
                >
                  <span>{item.title}</span>
                </NavLink>
              ))}
            </section>
          ))}
        </nav>
      </aside>
      <div className={styles.main}>
        <header className={styles.topbar}>
          <div className={styles.topbarLeft}>
            <button className={styles.iconButton} onClick={toggleSidebar} type="button">
              {sidebarCollapsed ? "展开" : "收起"}
            </button>
            <div>
              <p className={styles.topbarLabel}>当前页面</p>
              <strong>{routeItems.find((item) => item.path === location.pathname)?.title ?? "控制台"}</strong>
            </div>
          </div>
          <div className={styles.topbarRight}>
            <StatusBadge
              label={systemStatus?.api_key_configured ? "API Key 已配置" : "API Key 未配置"}
              tone={systemStatus?.api_key_configured ? "success" : "warning"}
            />
            <StatusBadge
              label={systemStatus?.monitor_service?.running ? "盯盘运行中" : "盯盘空闲"}
              tone={systemStatus?.monitor_service?.running ? "info" : "default"}
            />
            <StatusBadge
              label={systemStatus?.portfolio_scheduler?.is_running ? "定时分析已启用" : "定时分析未启用"}
              tone={systemStatus?.portfolio_scheduler?.is_running ? "success" : "default"}
            />
            {activeTask ? (
              <StatusBadge
                label={`${activeTask.label} ${Math.round((activeTask.progress ?? 0) * 100)}%`}
                tone="warning"
              />
            ) : null}
            <button className={styles.iconButton} onClick={toggleTheme} type="button">
              {theme === "dawn" ? "夜色" : "晨光"}
            </button>
            <button className={styles.primaryButton} onClick={handleLogout} type="button">
              退出
            </button>
          </div>
        </header>
        <main className={styles.content}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}
