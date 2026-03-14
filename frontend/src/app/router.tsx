import { useEffect } from "react";
import { Navigate, createBrowserRouter, useLocation } from "react-router-dom";

import { AppLayout } from "../components/layout/AppLayout";
import { useAuthStore } from "../stores/authStore";
import { LoginPage } from "../pages/LoginPage";
import { NotFoundPage } from "../pages/NotFoundPage";
import { ActivityPage } from "../pages/investment/ActivityPage";
import { PortfolioPage } from "../pages/investment/PortfolioPage";
import { SmartMonitorPage } from "../pages/investment/SmartMonitorPage";
import { DeepAnalysisPage } from "../pages/research/DeepAnalysisPage";
import { HistoryPage } from "../pages/research/HistoryPage";
import { LowPriceBullPage } from "../pages/selectors/LowPriceBullPage";
import { MainForcePage } from "../pages/selectors/MainForcePage";
import { ProfitGrowthPage } from "../pages/selectors/ProfitGrowthPage";
import { SmallCapPage } from "../pages/selectors/SmallCapPage";
import { ValueStockPage } from "../pages/selectors/ValueStockPage";
import { LonghubangPage } from "../pages/strategies/LonghubangPage";
import { MacroCyclePage } from "../pages/strategies/MacroCyclePage";
import { NewsFlowPage } from "../pages/strategies/NewsFlowPage";
import { SectorStrategyPage } from "../pages/strategies/SectorStrategyPage";
import { ConfigPage } from "../pages/system/ConfigPage";


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
        element: <DeepAnalysisPage />,
      },
      {
        path: "/research/history",
        element: <HistoryPage />,
      },
      {
        path: "/investment/portfolio",
        element: <PortfolioPage />,
      },
      {
        path: "/investment/price-alerts",
        element: <RedirectToSmartMonitor />,
      },
      {
        path: "/investment/smart-monitor",
        element: <SmartMonitorPage />,
      },
      {
        path: "/investment/activity",
        element: <ActivityPage />,
      },
      {
        path: "/selectors/main-force",
        element: <MainForcePage />,
      },
      {
        path: "/selectors/low-price-bull",
        element: <LowPriceBullPage />,
      },
      {
        path: "/selectors/small-cap",
        element: <SmallCapPage />,
      },
      {
        path: "/selectors/profit-growth",
        element: <ProfitGrowthPage />,
      },
      {
        path: "/selectors/value-stock",
        element: <ValueStockPage />,
      },
      {
        path: "/strategies/sector-strategy",
        element: <SectorStrategyPage />,
      },
      {
        path: "/strategies/longhubang",
        element: <LonghubangPage />,
      },
      {
        path: "/strategies/news-flow",
        element: <NewsFlowPage />,
      },
      {
        path: "/strategies/macro-cycle",
        element: <MacroCyclePage />,
      },
      {
        path: "/system/config",
        element: <ConfigPage />,
      },
    ],
  },
  {
    path: "*",
    element: <NotFoundPage />,
  },
]);
