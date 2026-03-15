import { lazy, Suspense, useEffect, type ReactNode } from "react";
import { Navigate, createBrowserRouter, useLocation } from "react-router-dom";

import { AppLayout } from "../components/layout/AppLayout";
import { useAuthStore } from "../stores/authStore";
import { LoginPage } from "../pages/LoginPage";
import { NotFoundPage } from "../pages/NotFoundPage";

const ActivityPage = lazy(() =>
  import("../pages/investment/ActivityPage").then((module) => ({ default: module.ActivityPage })),
);
const PortfolioPage = lazy(() =>
  import("../pages/investment/PortfolioPage").then((module) => ({ default: module.PortfolioPage })),
);
const SmartMonitorPage = lazy(() =>
  import("../pages/investment/SmartMonitorPage").then((module) => ({
    default: module.SmartMonitorPage,
  })),
);
const DeepAnalysisPage = lazy(() =>
  import("../pages/research/DeepAnalysisPage").then((module) => ({ default: module.DeepAnalysisPage })),
);
const HistoryPage = lazy(() =>
  import("../pages/research/HistoryPage").then((module) => ({ default: module.HistoryPage })),
);
const LowPriceBullPage = lazy(() =>
  import("../pages/selectors/LowPriceBullPage").then((module) => ({ default: module.LowPriceBullPage })),
);
const MainForcePage = lazy(() =>
  import("../pages/selectors/MainForcePage").then((module) => ({ default: module.MainForcePage })),
);
const ProfitGrowthPage = lazy(() =>
  import("../pages/selectors/ProfitGrowthPage").then((module) => ({ default: module.ProfitGrowthPage })),
);
const SmallCapPage = lazy(() =>
  import("../pages/selectors/SmallCapPage").then((module) => ({ default: module.SmallCapPage })),
);
const ValueStockPage = lazy(() =>
  import("../pages/selectors/ValueStockPage").then((module) => ({ default: module.ValueStockPage })),
);
const LonghubangPage = lazy(() =>
  import("../pages/strategies/LonghubangPage").then((module) => ({ default: module.LonghubangPage })),
);
const MacroCyclePage = lazy(() =>
  import("../pages/strategies/MacroCyclePage").then((module) => ({ default: module.MacroCyclePage })),
);
const NewsFlowPage = lazy(() =>
  import("../pages/strategies/NewsFlowPage").then((module) => ({ default: module.NewsFlowPage })),
);
const SectorStrategyPage = lazy(() =>
  import("../pages/strategies/SectorStrategyPage").then((module) => ({
    default: module.SectorStrategyPage,
  })),
);
const ConfigPage = lazy(() =>
  import("../pages/system/ConfigPage").then((module) => ({ default: module.ConfigPage })),
);
const DatabasePage = lazy(() =>
  import("../pages/system/DatabasePage").then((module) => ({ default: module.DatabasePage })),
);

function withPageSuspense(element: ReactNode) {
  return <Suspense fallback={<div style={{ padding: 32 }}>正在加载页面...</div>}>{element}</Suspense>;
}


function AuthBootstrap() {
  const checking = useAuthStore((state) => state.checking);
  const authenticated = useAuthStore((state) => state.authenticated);
  const hydrate = useAuthStore((state) => state.hydrate);

  useEffect(() => {
    void hydrate();
  }, [hydrate]);

  if (checking) {
    return <div style={{ padding: 32 }}>正在连接后端...</div>;
  }

  return <Navigate replace to={authenticated ? "/research/deep-analysis" : "/login"} />;
}

function RequireAuth() {
  const checking = useAuthStore((state) => state.checking);
  const authenticated = useAuthStore((state) => state.authenticated);
  const hydrate = useAuthStore((state) => state.hydrate);

  useEffect(() => {
    if (checking) {
      void hydrate();
    }
  }, [checking, hydrate]);

  if (checking) {
    return <div style={{ padding: 32 }}>正在校验会话...</div>;
  }
  if (!authenticated) {
    return <Navigate replace to="/login" />;
  }
  return <AppLayout />;
}

function RedirectToSmartMonitor() {
  const location = useLocation();
  return <Navigate replace to={`/investment/smart-monitor${location.search}`} />;
}

export const router = createBrowserRouter([
  {
    path: "/",
    element: <AuthBootstrap />,
  },
  {
    path: "/login",
    element: <LoginPage />,
  },
  {
    path: "/",
    element: <RequireAuth />,
    children: [
      {
        path: "/research/deep-analysis",
        element: withPageSuspense(<DeepAnalysisPage />),
      },
      {
        path: "/research/history",
        element: withPageSuspense(<HistoryPage />),
      },
      {
        path: "/investment/portfolio",
        element: withPageSuspense(<PortfolioPage />),
      },
      {
        path: "/investment/price-alerts",
        element: <RedirectToSmartMonitor />,
      },
      {
        path: "/investment/smart-monitor",
        element: withPageSuspense(<SmartMonitorPage />),
      },
      {
        path: "/investment/activity",
        element: withPageSuspense(<ActivityPage />),
      },
      {
        path: "/selectors/main-force",
        element: withPageSuspense(<MainForcePage />),
      },
      {
        path: "/selectors/low-price-bull",
        element: withPageSuspense(<LowPriceBullPage />),
      },
      {
        path: "/selectors/small-cap",
        element: withPageSuspense(<SmallCapPage />),
      },
      {
        path: "/selectors/profit-growth",
        element: withPageSuspense(<ProfitGrowthPage />),
      },
      {
        path: "/selectors/value-stock",
        element: withPageSuspense(<ValueStockPage />),
      },
      {
        path: "/strategies/sector-strategy",
        element: withPageSuspense(<SectorStrategyPage />),
      },
      {
        path: "/strategies/longhubang",
        element: withPageSuspense(<LonghubangPage />),
      },
      {
        path: "/strategies/news-flow",
        element: withPageSuspense(<NewsFlowPage />),
      },
      {
        path: "/strategies/macro-cycle",
        element: withPageSuspense(<MacroCyclePage />),
      },
      {
        path: "/system/config",
        element: withPageSuspense(<ConfigPage />),
      },
      {
        path: "/system/database",
        element: withPageSuspense(<DatabasePage />),
      },
    ],
  },
  {
    path: "*",
    element: <NotFoundPage />,
  },
]);
