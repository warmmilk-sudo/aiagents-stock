export interface AppRouteItem {
  path: string;
  title: string;
  group: string;
  hidden?: boolean;
}

export const routeItems: AppRouteItem[] = [
  { path: "/research/watchlist-hub", title: "投研中心", group: "投资管理" },
  { path: "/research/history", title: "股票档案", group: "投资管理", hidden: true },
  { path: "/investment/portfolio", title: "持仓列表", group: "投资管理", hidden: true },
  { path: "/investment/smart-monitor", title: "智能盯盘", group: "投资管理" },
  { path: "/investment/smart-selection", title: "智能选股", group: "投资管理" },
  {
    path: "/investment/price-alerts",
    title: "价格预警",
    group: "投资管理",
    hidden: true,
  },
  {
    path: "/investment/activity",
    title: "投资活动",
    group: "投资管理",
    hidden: true,
  },
  { path: "/strategies/sector-strategy", title: "智策板块", group: "策略分析" },
  { path: "/strategies/longhubang", title: "智瞰龙虎", group: "策略分析" },
  { path: "/strategies/news-flow", title: "新闻流量", group: "策略分析" },
  { path: "/strategies/macro-analysis", title: "宏观分析", group: "策略分析" },
  { path: "/strategies/macro-cycle", title: "宏观周期", group: "策略分析" },
  { path: "/selectors/main-force", title: "主力选股", group: "选股" },
  { path: "/selectors/low-price-bull", title: "低价擒牛", group: "选股" },
  { path: "/selectors/small-cap", title: "小市值", group: "选股" },
  { path: "/selectors/profit-growth", title: "净利增长", group: "选股" },
  { path: "/selectors/value-stock", title: "低估值", group: "选股" },
  { path: "/system/config", title: "系统配置", group: "系统管理" },
  { path: "/system/database", title: "数据库管理", group: "系统管理" },
];

export const defaultRoute = "/research/watchlist-hub";
