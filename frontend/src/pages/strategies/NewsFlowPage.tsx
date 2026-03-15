import { useEffect, useMemo, useRef, useState } from "react";

import {
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from "chart.js";
import { Line } from "react-chartjs-2";

import { PageFrame } from "../../components/common/PageFrame";
import { ApiRequestError, apiFetch, buildQuery } from "../../lib/api";
import { formatDateTime } from "../../lib/datetime";
import { asNumber, asText, integerText, numberText } from "../../lib/market";
import styles from "../ConsolePage.module.scss";


ChartJS.register(CategoryScale, Legend, LineElement, LinearScale, PointElement, Tooltip);

type Panel = "dashboard" | "analysis" | "trend" | "history";

interface TaskDetail<T> {
  id: string;
  status: string;
  message: string;
  progress?: number;
  current?: number;
  total?: number;
  error?: string;
  result?: T | null;
}

interface NewsFlowResult {
  fetch_time?: string;
  duration?: number;
  flow_data?: Record<string, unknown>;
  model_data?: Record<string, unknown>;
  sentiment_data?: Record<string, unknown>;
  ai_analysis?: Record<string, unknown>;
  trading_signals?: Record<string, unknown>;
  stock_news?: Array<Record<string, unknown>>;
  hot_topics?: Array<Record<string, unknown>>;
}

interface NewsFlowTaskPayload {
  result?: NewsFlowResult;
  message?: string;
}

interface DashboardData {
  latest_snapshot?: Record<string, unknown> | null;
  latest_sentiment?: Record<string, unknown> | null;
  latest_ai_analysis?: Record<string, unknown> | null;
  flow_trend?: Record<string, unknown> | null;
  scheduler_status?: SchedulerStatus | null;
}

interface SchedulerStatus {
  running?: boolean;
  task_enabled?: Record<string, boolean>;
  task_intervals?: Record<string, number>;
  next_run_times?: Record<string, string | null>;
}

interface TopicBubbleRenderItem {
  key: string;
  topic: string;
  label: string;
  heat: number;
  count: string;
  crossPlatform: string;
  showValue: boolean;
  labelStyle: { fontSize: number };
  style: {
    width: number;
    height: number;
    left: string;
    top: string;
    zIndex: number;
    background: string;
    borderColor: string;
    boxShadow: string;
  };
}

const panelOptions: Array<{ key: Panel; label: string }> = [
  { key: "dashboard", label: "仪表盘" },
  { key: "analysis", label: "实时监测" },
  { key: "trend", label: "趋势分析" },
  { key: "history", label: "历史记录" },
];

const categoryOptions = [
  { label: "全部平台", value: "" },
  { label: "财经平台", value: "finance" },
  { label: "社交媒体", value: "social" },
  { label: "新闻媒体", value: "news" },
  { label: "科技媒体", value: "tech" },
];

const noiseTopicKeywords = new Set([
  "今天",
  "今日",
  "昨日",
  "当天",
  "目前",
  "最新",
  "刚刚",
  "刚才",
  "现场",
  "视频",
  "画面",
  "消息",
  "通报",
  "回应",
  "公告",
  "报道",
  "曝光",
  "热议",
  "关注",
  "提醒",
  "发布",
  "披露",
  "情况",
  "原因",
  "结果",
  "事件",
  "详情",
  "内容",
  "问题",
  "影响",
  "进展",
  "后续",
  "背后",
  "真相",
  "实情",
  "网友称",
  "网友说",
  "网友热议",
  "男子",
  "女子",
  "老人",
  "大爷",
  "大妈",
  "小伙",
  "小伙子",
  "小伙儿",
  "小女孩",
  "小男孩",
  "男孩",
  "女孩",
  "儿童",
  "家长",
  "学生",
  "老师",
  "司机",
  "乘客",
  "顾客",
  "老板",
  "员工",
  "市民",
  "人员",
  "群众",
  "民众",
  "居民",
  "行人",
  "路人",
  "网友",
  "博主",
  "专家",
  "记者",
  "警方",
  "官方",
  "平台",
  "企业",
  "公司",
  "品牌",
  "多人",
  "有人",
  "一些人",
  "不少人",
  "很多人",
  "部分人",
  "相关人士",
  "知情人",
  "一名",
  "一个",
  "一家",
  "一位",
  "一人",
  "一地",
  "一事",
  "一起",
  "一则",
  "一图",
  "一文",
  "这名",
  "这位",
  "这个",
  "这起",
  "这类",
  "该名",
  "该位",
  "该公司",
  "此事",
  "其后",
  "其间",
  "其中",
  "他们",
  "我们",
  "你们",
  "大家",
  "自己",
  "什么",
  "到底",
  "究竟",
  "为何会",
  "为何要",
  "何时",
  "何处",
  "谁在",
  "谁是",
  "为何",
  "如何",
  "怎么",
  "为什么",
  "是否",
  "竟然",
  "居然",
  "原来",
  "果然",
  "其实",
  "真的",
  "真有",
  "确实",
  "实为",
  "堪称",
  "堪比",
  "直击",
  "速看",
  "快看",
  "必看",
  "值得",
  "注意",
  "警惕",
  "提示",
  "揭秘",
  "解读",
  "梳理",
  "汇总",
  "盘点",
  "观察",
  "点评",
  "分析",
  "预测",
  "研判",
  "判断",
  "结论",
  "信号",
  "趋势",
  "机会",
  "风险",
  "不能",
  "不会",
  "没有",
  "不是",
  "可以",
  "已经",
  "正在",
  "仍在",
  "再次",
  "又一",
  "再度",
  "再现",
  "引发",
  "带来",
  "涉及",
  "关于",
  "更多",
  "不少",
  "多个",
  "有关",
  "相关",
  "重要",
  "重大",
  "重磅",
  "紧急",
  "突发",
  "特别",
  "明显",
  "显著",
  "核心",
  "关键",
  "热门",
  "火爆",
  "高位",
  "强势",
  "全面",
  "持续",
  "进一步",
  "首次",
  "首次回应",
  "最新回应",
  "最新进展",
  "情况通报",
  "警方通报",
  "官方通报",
]);
const noiseTopicPatterns = [
  /^(第?\d+[次年月日号楼名位条个家人])/,
  /^[一二三四五六七八九十百千万两\d]+(名|位|人|家|个|则|起|条|件|次|天|年|月|日)$/,
  /^(某|这|那|该|其)[名位个人家事地公司平台机构]/,
  /^(男子|女子|男孩|女孩|老人|孩子|家长|学生|老师|司机|乘客|顾客|员工|老板|网友|博主|专家|记者|警方|官方)/,
  /(回应|通报|曝光|披露|发布|提醒|关注|热议|报道|进展|后续|详情|真相|原因|结果)$/,
  /^(今天|今日|昨日|目前|刚刚|刚才|最新|现场|视频|画面|消息|公告|情况)/,
  /^(到底|究竟|为何|如何|怎么|为什么|是否|何时|何处|谁在|谁是)/,
  /^(速看|快看|必看|值得|注意|警惕|揭秘|解读|梳理|汇总|盘点|观察|点评|分析|预测|研判|判断)/,
  /^(重要|重大|重磅|紧急|突发|特别|明显|显著|核心|关键|热门|火爆|高位|强势)/,
];
const topicBubblePalette = [
  {
    background: "rgba(244, 199, 63, 0.7)",
    borderColor: "rgba(214, 164, 95, 0.42)",
  },
  {
    background: "rgba(105, 193, 221, 0.68)",
    borderColor: "rgba(92, 137, 167, 0.4)",
  },
  {
    background: "rgba(239, 124, 74, 0.66)",
    borderColor: "rgba(198, 93, 75, 0.38)",
  },
  {
    background: "rgba(150, 210, 116, 0.66)",
    borderColor: "rgba(113, 155, 77, 0.38)",
  },
  {
    background: "rgba(170, 140, 244, 0.64)",
    borderColor: "rgba(130, 108, 201, 0.38)",
  },
  {
    background: "rgba(79, 201, 171, 0.66)",
    borderColor: "rgba(58, 154, 130, 0.38)",
  },
  {
    background: "rgba(244, 152, 188, 0.64)",
    borderColor: "rgba(196, 113, 146, 0.36)",
  },
  {
    background: "rgba(124, 176, 255, 0.66)",
    borderColor: "rgba(86, 128, 196, 0.38)",
  },
  {
    background: "rgba(255, 173, 92, 0.65)",
    borderColor: "rgba(210, 129, 52, 0.38)",
  },
  {
    background: "rgba(112, 204, 116, 0.64)",
    borderColor: "rgba(75, 153, 84, 0.36)",
  },
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return isRecord(value) ? value : undefined;
}

function asRecordArray(value: unknown): Array<Record<string, unknown>> {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => asText(item, "")).filter(Boolean) : [];
}

function toDisplayText(value: unknown, preferredKeys: string[] = []): string {
  if (typeof value === "string" || typeof value === "number") {
    return String(value);
  }
  if (!isRecord(value)) {
    return "";
  }

  for (const key of preferredKeys) {
    const candidate = value[key];
    if (typeof candidate === "string" || typeof candidate === "number") {
      return String(candidate);
    }
  }

  for (const key of ["name", "label", "title", "topic", "sector", "theme", "reason"]) {
    const candidate = value[key];
    if (typeof candidate === "string" || typeof candidate === "number") {
      return String(candidate);
    }
  }

  return "";
}

function asDisplayStringArray(value: unknown, preferredKeys: string[] = []): string[] {
  return Array.isArray(value) ? value.map((item) => toDisplayText(item, preferredKeys)).filter(Boolean) : [];
}

function extractRecommendedStocks(aiAnalysis: Record<string, unknown> | undefined): Array<Record<string, unknown>> {
  const directRecommendations = asRecordArray(aiAnalysis?.recommended_stocks);
  if (directRecommendations.length) {
    return directRecommendations;
  }
  return asRecordArray(asRecord(aiAnalysis?.stock_recommend)?.recommended_stocks);
}

function extractAiAnalysisSummary(aiAnalysis: Record<string, unknown> | undefined) {
  const investmentAdvice = asRecord(aiAnalysis?.investment_advice);
  const riskAssess = asRecord(aiAnalysis?.risk_assess);
  const sectorAnalysis = asRecord(aiAnalysis?.sector_analysis);
  return {
    summary: asText(aiAnalysis?.summary ?? investmentAdvice?.summary, "暂无摘要"),
    advice: asText(aiAnalysis?.advice ?? investmentAdvice?.advice, "观望"),
    confidence: integerText(aiAnalysis?.confidence ?? investmentAdvice?.confidence),
    riskLevel: asText(aiAnalysis?.risk_level ?? riskAssess?.risk_level, "N/A"),
    riskFactors: asDisplayStringArray(aiAnalysis?.risk_factors ?? riskAssess?.risk_factors),
    affectedSectors: asDisplayStringArray(aiAnalysis?.affected_sectors ?? sectorAnalysis?.benefited_sectors, ["name", "sector", "theme"]),
    analysisTime: numberText(aiAnalysis?.analysis_time, 2),
    timestamp: asText(aiAnalysis?.fetch_time ?? aiAnalysis?.created_at, ""),
  };
}

function normalizeTopicText(topic: unknown): string {
  return asText(topic, "")
    .replace(/[【】\[\]（）()《》<>「」『』"'`]/g, "")
    .replace(/\s+/g, "")
    .trim();
}

function filterMeaningfulTopics(topics: Array<Record<string, unknown>>): Array<Record<string, unknown>> {
  const seenTopics = new Set<string>();
  return topics.filter((item) => {
    const normalizedTopic = normalizeTopicText(item.topic);
    if (
      !normalizedTopic ||
      normalizedTopic.length <= 1 ||
      noiseTopicKeywords.has(normalizedTopic) ||
      noiseTopicPatterns.some((pattern) => pattern.test(normalizedTopic))
    ) {
      return false;
    }
    if (seenTopics.has(normalizedTopic)) {
      return false;
    }
    seenTopics.add(normalizedTopic);
    return true;
  });
}

function buildTopicBubbleItems(topics: Array<Record<string, unknown>>, keyPrefix: string, containerWidth: number, containerHeight: number): TopicBubbleRenderItem[] {
  const heatValues = topics.map((item) => asNumber(item.heat) ?? 0);
  const minHeat = heatValues.length ? Math.min(...heatValues) : 0;
  const maxHeat = heatValues.length ? Math.max(...heatValues) : 0;
  const placedBubbles: Array<{ left: number; top: number; size: number }> = [];
  const width = Math.max(containerWidth, 320);
  const height = Math.max(containerHeight, 260);
  const edgePadding = Math.max(26, Math.min(width, height) * 0.05);

  return topics.map((item, index) => {
    const heat = asNumber(item.heat) ?? 0;
    const ratio = maxHeat > minHeat ? (heat - minHeat) / (maxHeat - minHeat) : 0.5;
    const size = Math.round(Math.max(54, Math.min(164, 54 + ratio * Math.min(width * 0.16, 108))));
    const paletteIndex = (index * 3 + Math.round(ratio * 7)) % topicBubblePalette.length;
    const palette = topicBubblePalette[paletteIndex];
    const topic = asText(item.topic, "N/A");
    const seed = `${keyPrefix}-${topic}-${index}`;
    let hash = 0;
    for (let cursor = 0; cursor < seed.length; cursor += 1) {
      hash = ((hash << 5) - hash + seed.charCodeAt(cursor)) | 0;
    }
    const safeXMin = edgePadding + size / 2;
    const safeXMax = width - edgePadding - size / 2;
    const safeYMin = edgePadding + size / 2;
    const safeYMax = height - edgePadding - size / 2;
    let left = safeXMin + (((Math.abs(hash) % 1000) / 999) * Math.max(12, safeXMax - safeXMin));
    let top = safeYMin + (((Math.abs(hash * 31) % 1000) / 999) * Math.max(12, safeYMax - safeYMin));

    for (let attempt = 0; attempt < 24; attempt += 1) {
      let moved = false;
      for (const placedBubble of placedBubbles) {
        const dx = left - placedBubble.left;
        const dy = top - placedBubble.top;
        const distance = Math.hypot(dx, dy);
        const minDistance = (size + placedBubble.size) / 2 + 10;
        if (distance < minDistance) {
          const pushAngle = distance > 0.001 ? Math.atan2(dy, dx) : (((Math.abs(hash) + attempt * 97) % 360) * Math.PI) / 180;
          const pushDistance = minDistance - distance + 4;
          left += Math.cos(pushAngle) * pushDistance;
          top += Math.sin(pushAngle) * pushDistance;
          left = Math.min(safeXMax, Math.max(safeXMin, left));
          top = Math.min(safeYMax, Math.max(safeYMin, top));
          moved = true;
        }
      }
      if (!moved) {
        break;
      }
    }

    placedBubbles.push({ left, top, size });
    const label = size < 92 && topic.length > 4 ? `${topic.slice(0, 4)}…` : topic;

    return {
      key: `${keyPrefix}-topic-${index}-${asText(item.topic, "topic")}`,
      topic,
      label,
      heat,
      count: integerText(item.count),
      crossPlatform: asText(item.cross_platform, "N/A"),
      showValue: size >= 118,
      labelStyle: {
        fontSize: Math.max(11, Math.min(26, Math.round(size * 0.16))),
      },
      style: {
        width: size,
        height: size,
        left: `${left}px`,
        top: `${top}px`,
        zIndex: 1 + ((index * 5 + paletteIndex) % 20),
        background: palette.background,
        borderColor: palette.borderColor,
        boxShadow: "none",
      },
    };
  });
}

function TopicBubbleCloud({
  topics,
  cloudKeyPrefix,
  emptyText,
}: {
  topics: Array<Record<string, unknown>>;
  cloudKeyPrefix: string;
  emptyText: string;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [containerSize, setContainerSize] = useState({ width: 760, height: 420 });

  useEffect(() => {
    const node = containerRef.current;
    if (!node) {
      return;
    }

    const updateSize = () => {
      const nextWidth = Math.round(node.clientWidth);
      const nextHeight = Math.round(node.clientHeight);
      if (nextWidth > 0 && nextHeight > 0) {
        setContainerSize((current) =>
          current.width === nextWidth && current.height === nextHeight
            ? current
            : { width: nextWidth, height: nextHeight },
        );
      }
    };

    updateSize();
    const observer = new ResizeObserver(() => updateSize());
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const bubbleItems = useMemo(
    () => buildTopicBubbleItems(topics, cloudKeyPrefix, containerSize.width, containerSize.height),
    [cloudKeyPrefix, containerSize.height, containerSize.width, topics],
  );

  return (
    <div className={styles.topicBubbleCloud} ref={containerRef}>
      {bubbleItems.map((item) => (
        <div
          className={styles.topicBubble}
          key={item.key}
          style={item.style}
          title={`${item.topic} | 热度 ${numberText(item.heat)} | 提及次数 ${item.count} | 跨平台 ${item.crossPlatform}`}
        >
          <div className={styles.topicBubbleLabel} style={item.labelStyle}>{item.label}</div>
          {item.showValue ? <div className={styles.topicBubbleValue}>热度 {numberText(item.heat)}</div> : null}
        </div>
      ))}
      {!bubbleItems.length ? <div className={styles.muted}>{emptyText}</div> : null}
    </div>
  );
}

export function NewsFlowPage() {
  const syncedSuccessTaskIdRef = useRef("");
  const [panel, setPanel] = useState<Panel>("dashboard");
  const [task, setTask] = useState<TaskDetail<NewsFlowTaskPayload> | null>(null);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [history, setHistory] = useState<Array<Record<string, unknown>>>([]);
  const [historyDetail, setHistoryDetail] = useState<Record<string, unknown> | null>(null);
  const [latestSnapshotDetail, setLatestSnapshotDetail] = useState<Record<string, unknown> | null>(null);
  const [trendData, setTrendData] = useState<Record<string, unknown> | null>(null);
  const [sentimentHistory, setSentimentHistory] = useState<Array<Record<string, unknown>>>([]);
  const [dailyStatistics, setDailyStatistics] = useState<Array<Record<string, unknown>>>([]);
  const [aiHistory, setAiHistory] = useState<Array<Record<string, unknown>>>([]);
  const [platforms, setPlatforms] = useState<Array<Record<string, unknown>>>([]);
  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [schedulerLogs, setSchedulerLogs] = useState<Array<Record<string, unknown>>>([]);
  const [searchKeyword, setSearchKeyword] = useState("");
  const [searchResults, setSearchResults] = useState<Array<Record<string, unknown>>>([]);
  const [category, setCategory] = useState("");
  const [trendDays, setTrendDays] = useState("7");
  const [taskEnabled, setTaskEnabled] = useState<Record<string, boolean>>({});
  const [taskIntervals, setTaskIntervals] = useState<Record<string, string>>({});
  const [taskConfigExpanded, setTaskConfigExpanded] = useState(false);
  const [platformsExpanded, setPlatformsExpanded] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const currentResult = task?.status === "success" ? task.result?.result ?? null : null;
  const historySnapshot = (historyDetail?.snapshot as Record<string, unknown> | undefined) ?? undefined;
  const historySentiment = (historyDetail?.sentiment as Record<string, unknown> | undefined) ?? undefined;
  const historyAiAnalysis = (historyDetail?.ai_analysis as Record<string, unknown> | undefined) ?? undefined;
  const historyHotTopics = filterMeaningfulTopics((historyDetail?.hot_topics as Array<Record<string, unknown>> | undefined) ?? []).slice(0, 10);
  const historyRelatedNews = ((historyDetail?.stock_news as Array<Record<string, unknown>> | undefined) ?? []).slice(0, 8);
  const latestAiAnalysis =
    asRecord(latestSnapshotDetail?.ai_analysis) ??
    asRecord(dashboard?.latest_ai_analysis) ??
    asRecord(aiHistory[0]) ??
    asRecord(currentResult?.ai_analysis);
  const latestAiSummary = extractAiAnalysisSummary(latestAiAnalysis);
  const latestRecommendedStocks = extractRecommendedStocks(latestAiAnalysis).slice(0, 8);
  const latestHotTopics = filterMeaningfulTopics(((latestSnapshotDetail?.hot_topics as Array<Record<string, unknown>> | undefined) ?? currentResult?.hot_topics ?? [])).slice(0, 10);
  const latestHotNews = (((latestSnapshotDetail?.stock_news as Array<Record<string, unknown>> | undefined) ?? currentResult?.stock_news ?? [])).slice(0, 8);

  const loadTask = async () => {
    setTask(await apiFetch<TaskDetail<NewsFlowTaskPayload> | null>("/api/strategies/news-flow/tasks/latest"));
  };

  const loadDashboard = async () => {
    const data = await apiFetch<DashboardData>("/api/strategies/news-flow/dashboard");
    setDashboard(data);
    setScheduler((data.scheduler_status as SchedulerStatus | null) ?? null);
    setTrendData((data.flow_trend as Record<string, unknown> | null) ?? null);
  };

  const loadHistory = async () => {
    const [historyData, aiData] = await Promise.all([
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/history?limit=50"),
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/ai-history?limit=20"),
    ]);
    setHistory(historyData);
    setAiHistory(aiData);

    const latestSnapshotId = Number(historyData[0]?.id ?? 0);
    if (Number.isFinite(latestSnapshotId) && latestSnapshotId > 0) {
      try {
        setLatestSnapshotDetail(await apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/history/${latestSnapshotId}`));
      } catch {
        setLatestSnapshotDetail(null);
      }
      return;
    }

    setLatestSnapshotDetail(null);
  };

  const loadTrend = async () => {
    const [trend, sentiment, statistics] = await Promise.all([
      apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/trend${buildQuery({ days: trendDays })}`),
      apiFetch<Array<Record<string, unknown>>>(`/api/strategies/news-flow/sentiment-history${buildQuery({ limit: Number(trendDays) * 3 })}`),
      apiFetch<Array<Record<string, unknown>>>(`/api/strategies/news-flow/daily-statistics${buildQuery({ days: trendDays })}`),
    ]);
    setTrendData(trend);
    setSentimentHistory(sentiment);
    setDailyStatistics(statistics);
  };

  const loadSettings = async () => {
    const [platformData, schedulerData, logData] = await Promise.all([
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/platforms"),
      apiFetch<SchedulerStatus>("/api/strategies/news-flow/scheduler"),
      apiFetch<Array<Record<string, unknown>>>("/api/strategies/news-flow/scheduler/logs?days=7"),
    ]);
    setPlatforms(platformData);
    setScheduler(schedulerData);
    setSchedulerLogs(logData);
    setTaskEnabled(schedulerData.task_enabled ?? {});
    setTaskIntervals(Object.fromEntries(Object.entries(schedulerData.task_intervals ?? {}).map(([key, value]) => [key, String(value)])));
  };

  useEffect(() => {
    void Promise.all([loadTask(), loadDashboard(), loadHistory(), loadTrend(), loadSettings()]);
    const taskTimer = window.setInterval(() => void loadTask(), 2000);
    const statusTimer = window.setInterval(() => void Promise.all([loadDashboard(), loadSettings()]), 10000);
    return () => {
      window.clearInterval(taskTimer);
      window.clearInterval(statusTimer);
    };
  }, []);

  useEffect(() => {
    if (task?.status !== "success" || !task.id || syncedSuccessTaskIdRef.current === task.id) {
      return;
    }
    syncedSuccessTaskIdRef.current = task.id;
    void Promise.all([loadDashboard(), loadHistory(), loadTrend()]);
  }, [task?.id, task?.status]);

  const trendChartData = useMemo(() => ({
    labels: (trendData?.dates as string[] | undefined) ?? [],
    datasets: [
      { label: "平均得分", data: (trendData?.avg_scores as number[] | undefined) ?? [], borderColor: "#b54d2b", backgroundColor: "rgba(181,77,43,0.12)" },
      { label: "最高得分", data: (trendData?.max_scores as number[] | undefined) ?? [], borderColor: "#134074", backgroundColor: "rgba(19,64,116,0.08)" },
    ],
  }), [trendData]);

  const sentimentChartData = useMemo(() => ({
    labels: sentimentHistory.slice().reverse().map((item) => asText(item.fetch_time ?? item.created_at, "").slice(5, 16)),
    datasets: [
      { label: "情绪指数", data: sentimentHistory.slice().reverse().map((item) => asNumber(item.sentiment_index) ?? 50), borderColor: "#6a4c93", backgroundColor: "rgba(106,76,147,0.12)" },
      { label: "K值x20", data: sentimentHistory.slice().reverse().map((item) => (asNumber(item.viral_k) ?? 1) * 20), borderColor: "#2a9d8f", backgroundColor: "rgba(42,157,143,0.08)" },
    ],
  }), [sentimentHistory]);

  const submitAnalysis = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch<{ task_id: string }>("/api/strategies/news-flow/tasks", { method: "POST", body: JSON.stringify({ category: category || null }) });
      setPanel("analysis");
      setMessage("新闻流量分析任务已提交，正在准备分析...");
      await loadTask();
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "提交新闻流量分析失败");
    }
  };

  const runQuickAnalysis = async () => {
    setMessage("");
    setError("");
    try {
      await apiFetch("/api/strategies/news-flow/quick-analysis", { method: "POST", body: JSON.stringify({ category: category || null }) });
      setMessage("热点同步已完成");
      await Promise.all([loadDashboard(), loadHistory(), loadTrend()]);
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "热点同步失败");
    }
  };

  const openHistoryDetail = async (snapshotId: number) => {
    setMessage("");
    setError("");
    try {
      setHistoryDetail(await apiFetch<Record<string, unknown>>(`/api/strategies/news-flow/history/${snapshotId}`));
      setPanel("history");
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "加载历史记录失败");
    }
  };

  const saveSchedulerConfig = async (taskKeys?: string[]) => {
    setMessage("");
    setError("");
    try {
      const enabledPayload = taskKeys
        ? Object.fromEntries(taskKeys.map((key) => [key, Boolean(taskEnabled[key])]))
        : taskEnabled;
      const intervalPayload = taskKeys
        ? Object.fromEntries(taskKeys.map((key) => [key, Number(taskIntervals[key]) || 5]))
        : Object.fromEntries(Object.entries(taskIntervals).map(([key, value]) => [key, Number(value) || 5]));
      const data = await apiFetch<SchedulerStatus>("/api/strategies/news-flow/scheduler", {
        method: "PUT",
        body: JSON.stringify({
          task_enabled: enabledPayload,
          task_intervals: intervalPayload,
        }),
      });
      setScheduler(data);
      setTaskEnabled(data.task_enabled ?? {});
      setTaskIntervals(Object.fromEntries(Object.entries(data.task_intervals ?? {}).map(([key, value]) => [key, String(value)])));
      setMessage("定时任务配置已更新");
      if (!taskKeys) {
        setTaskConfigExpanded(false);
      }
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "保存定时任务配置失败");
    }
  };

  const toggleScheduler = async (running: boolean) => {
    setMessage("");
    setError("");
    try {
      setScheduler(await apiFetch<SchedulerStatus>(running ? "/api/strategies/news-flow/scheduler/start" : "/api/strategies/news-flow/scheduler/stop", { method: "POST" }));
      setMessage(running ? "调度器已启动" : "调度器已停止");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "更新调度器状态失败");
    }
  };

  const searchStockNews = async () => {
    if (!searchKeyword.trim()) {
      setSearchResults([]);
      return;
    }
    setMessage("");
    setError("");
    try {
      setSearchResults(await apiFetch<Array<Record<string, unknown>>>(`/api/strategies/news-flow/search-stock-news${buildQuery({ keyword: searchKeyword.trim(), limit: 50 })}`));
      setMessage("历史新闻检索已完成");
    } catch (requestError) {
      setError(requestError instanceof ApiRequestError ? requestError.message : "搜索相关新闻失败");
    }
  };

  return (
    <PageFrame
      title="新闻流量"
      summary="覆盖仪表盘、实时分析、趋势、历史和调度设置。"
      sectionTabs={panelOptions}
      activeSectionKey={panel}
      onSectionChange={(nextSection) => setPanel(nextSection as Panel)}
    >
      <div className={`${styles.stack} ${styles.newsFlowPage}`}>
        {(message || error) ? (
          <section className={styles.card}>
            {message ? <div className={styles.successText}>{message}</div> : null}
            {error ? <div className={styles.dangerText}>{error}</div> : null}
          </section>
        ) : null}

        {task ? (
          <section className={styles.card}>
            <h2>AI 分析任务状态</h2>
            <p>{task.message || "等待新闻流量任务状态..."}</p>
            <p className={styles.muted}>进度: {task.current ?? 0} / {task.total ?? 0}</p>
            {task.error ? <p className={styles.dangerText}>{task.error}</p> : null}
          </section>
        ) : null}

        {panel === "dashboard" ? (
          <section className={styles.card}>
            <div className={styles.stack}>
              <div className={`${styles.field} ${styles.newsFlowToolbarField}`}>
                <label htmlFor="newsFlowCategory">平台类别</label>
                <select id="newsFlowCategory" value={category} onChange={(event) => setCategory(event.target.value)}>
                  {categoryOptions.map((item) => (
                    <option key={item.label} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </div>
              <div className={styles.responsiveActionGrid}>
                <button className={styles.primaryButton} onClick={() => void runQuickAnalysis()} type="button">
                  热点同步
                </button>
              </div>
            </div>
          </section>
        ) : null}

        {panel === "dashboard" ? (
          <>
            <section className={styles.card}>
              <div className={styles.summaryMetricGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>流量得分</span>
                  <strong>{integerText(dashboard?.latest_snapshot?.total_score)}</strong>
                  <div className={styles.muted}>{asText(dashboard?.latest_snapshot?.flow_level, "无数据")}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>情绪指数</span>
                  <strong>{integerText(dashboard?.latest_sentiment?.sentiment_index)}</strong>
                  <div className={styles.muted}>{asText(dashboard?.latest_sentiment?.sentiment_class, "中性")}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>流量阶段</span>
                  <strong>{asText(dashboard?.latest_sentiment?.flow_stage, "未知")}</strong>
                  <div className={styles.muted}>K值 {numberText(dashboard?.latest_sentiment?.viral_k)}</div>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>AI 建议</span>
                  <strong>{asText(dashboard?.latest_ai_analysis?.advice, "观望")}</strong>
                  <div className={styles.muted}>置信度 {integerText(dashboard?.latest_ai_analysis?.confidence)}%</div>
                </div>
              </div>
            </section>

            {latestAiAnalysis || dashboard?.latest_snapshot?.analysis ? (
              <section className={styles.card}>
                <h2>AI 分析摘要</h2>
                {dashboard?.latest_snapshot?.analysis ? (
                  <div className={styles.listItem}>
                    <strong>快照分析</strong>
                    <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>{asText(dashboard.latest_snapshot.analysis, "")}</div>
                  </div>
                ) : null}
                {latestAiAnalysis ? (
                  <div className={styles.listItem} style={{ marginTop: dashboard?.latest_snapshot?.analysis ? 16 : 0 }}>
                    <strong>AI 分析</strong>
                    <div style={{ marginTop: 10 }}>{latestAiSummary.summary}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      建议: {latestAiSummary.advice} | 风险等级: {latestAiSummary.riskLevel} | 置信度 {latestAiSummary.confidence}%
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      影响板块: {latestAiSummary.affectedSectors.join("、") || "N/A"}
                    </div>
                    {latestAiSummary.riskFactors.length ? (
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        风险因素: {latestAiSummary.riskFactors.join("、")}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </section>
            ) : null}

            <section className={styles.card}>
              <h2>热点话题列表</h2>
              <TopicBubbleCloud cloudKeyPrefix="dashboard" emptyText="暂无最新热点话题。" topics={latestHotTopics} />
            </section>

            <section className={styles.card}>
              <h2>热点新闻列表</h2>
              <div className={styles.list}>
                {latestHotNews.map((item, index) => (
                  <div className={styles.listItem} key={`dashboard-news-${index}`}>
                    <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      {((item.matched_keywords as string[] | undefined) ?? []).join("、") || "无关键词"} | {formatDateTime(item.publish_time ?? item.fetch_time, "")}
                    </div>
                  </div>
                ))}
                {!latestHotNews.length ? <div className={styles.muted}>暂无最新热点新闻。</div> : null}
              </div>
            </section>
          </>
        ) : null}

        {panel === "analysis" ? (
          <>
            <section className={styles.card}>
              <h2>监测任务与调度器状态</h2>
              {!taskConfigExpanded ? (
                <div className={styles.responsiveActionGrid}>
                  <button className={styles.primaryButton} onClick={() => setTaskConfigExpanded(true)} type="button">
                    配置监测任务
                  </button>
                </div>
              ) : (
                <>
                  <div className={styles.list}>
                    {[
                      ["sync_hotspots", "热点同步"],
                      ["deep_analysis", "深度分析"],
                    ].map(([key, label]) => (
                      <div className={styles.listItem} key={key}>
                        <div className={styles.responsiveActionGrid}>
                          <label className={`${styles.listItem} ${styles.actionToggle}`}>
                            <input checked={Boolean(taskEnabled[key])} onChange={(event) => setTaskEnabled((current) => ({ ...current, [key]: event.target.checked }))} type="checkbox" /> {label}
                          </label>
                          <input
                            className={styles.shortInput}
                            value={taskIntervals[key] ?? ""}
                            onChange={(event) => setTaskIntervals((current) => ({ ...current, [key]: event.target.value }))}
                          />
                          <span className={styles.muted}>分钟</span>
                          {key === "deep_analysis" ? (
                            <button className={styles.secondaryButton} onClick={() => void saveSchedulerConfig([key])} type="button">
                              保存深度分析
                            </button>
                          ) : null}
                        </div>
                      </div>
                    ))}
                  </div>
                  <div className={styles.responsiveActionGrid} style={{ marginTop: 16 }}>
                    <button className={styles.primaryButton} onClick={() => void saveSchedulerConfig()} type="button">
                      保存任务配置
                    </button>
                  </div>
                </>
              )}
              <h2 style={{ marginTop: 20 }}>调度器状态</h2>
              <div className={styles.compactGrid}>
                <div className={styles.metric}>
                  <span className={styles.muted}>运行状态</span>
                  <strong>{scheduler?.running ? "运行中" : "已停止"}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>热点同步</span>
                  <strong>{formatDateTime(scheduler?.next_run_times?.sync_hotspots, "N/A")}</strong>
                </div>
                <div className={styles.metric}>
                  <span className={styles.muted}>深度分析</span>
                  <strong>{formatDateTime(scheduler?.next_run_times?.deep_analysis, "N/A")}</strong>
                </div>
              </div>
              <div className={styles.responsiveActionGrid} style={{ marginTop: 16 }}>
                <label className={styles.switchField}>
                  <span className={styles.switchLabel}>启用调度器</span>
                  <span className={styles.switchControl}>
                    <input checked={Boolean(scheduler?.running)} onChange={(event) => void toggleScheduler(event.target.checked)} type="checkbox" />
                    <span className={styles.switchTrack} aria-hidden="true">
                      <span className={styles.switchThumb} />
                    </span>
                  </span>
                </label>
              </div>
            </section>

            <section className={styles.card}>
              <div className={styles.responsiveActionGrid}>
                <button className={styles.primaryButton} onClick={() => void submitAnalysis()} type="button">
                  开始 AI 智能分析
                </button>
              </div>
            </section>

            {latestAiAnalysis ? (
              <>
                <section className={styles.card}>
                  <h2>AI 分析摘要</h2>
                  <div className={styles.listItem}>
                    <div>{latestAiSummary.summary}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      建议: {latestAiSummary.advice} | 风险等级: {latestAiSummary.riskLevel} | 置信度 {latestAiSummary.confidence}%
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      影响板块: {latestAiSummary.affectedSectors.join("、") || "N/A"}
                    </div>
                    {latestAiSummary.riskFactors.length ? (
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        风险因素: {latestAiSummary.riskFactors.join("、")}
                      </div>
                    ) : null}
                    {latestAiSummary.timestamp ? (
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        分析时间: {formatDateTime(latestAiSummary.timestamp, latestAiSummary.timestamp)}
                      </div>
                    ) : null}
                    {latestAiSummary.analysisTime !== "N/A" ? (
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        AI 分析耗时: {latestAiSummary.analysisTime} 秒
                      </div>
                    ) : null}
                  </div>
                </section>

                <section className={styles.card}>
                  <h2>AI 选股推荐</h2>
                  <div className={styles.list}>
                    {latestRecommendedStocks.map((item, index) => (
                      <div className={styles.listItem} key={`stock-${index}`}>
                        <strong>{asText(item.code, "")} {asText(item.name, "")}</strong>
                        <div style={{ marginTop: 8 }}>
                          板块: {asText(item.sector, "N/A")} | 风险: {asText(item.risk_level, "N/A")} | 理由: {asText(item.reason, "暂无推荐理由")}
                        </div>
                      </div>
                    ))}
                    {!latestRecommendedStocks.length ? <div className={styles.muted}>当前最新 AI 分析未生成推荐股票。</div> : null}
                  </div>
                </section>
              </>
            ) : (
              <section className={styles.card}>
                <div className={styles.muted}>暂无最新 AI 分析结果，请先提交新闻流量分析任务。</div>
              </section>
            )}

            <section className={styles.card}>
              <div className={styles.cardHeader}>
                <h2>支持的平台</h2>
                <button className={styles.secondaryButton} onClick={() => setPlatformsExpanded((current) => !current)} type="button">
                  {platformsExpanded ? "收起" : "展开"}
                </button>
              </div>
              {platformsExpanded ? (
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>平台</th>
                        <th>类别</th>
                        <th>权重</th>
                      </tr>
                    </thead>
                    <tbody>
                      {platforms.map((item) => (
                        <tr key={asText(item.platform ?? item.name)}>
                          <td>{asText(item.name, "N/A")}</td>
                          <td>{asText(item.category, "N/A")}</td>
                          <td>{integerText(item.weight)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className={styles.muted}>平台列表默认折叠，点击右上角“展开”查看。</div>
              )}
            </section>

          </>
        ) : null}

        {panel === "trend" ? (
          <>
            <section className={styles.card}>
              <div className={styles.stack}>
                <div className={styles.field}>
                  <label htmlFor="trendDays">分析天数</label>
                  <select id="trendDays" value={trendDays} onChange={(event) => setTrendDays(event.target.value)}>
                    <option value="3">3 天</option>
                    <option value="7">7 天</option>
                    <option value="14">14 天</option>
                    <option value="30">30 天</option>
                  </select>
                </div>
                <div className={styles.responsiveActionGrid}>
                  <button className={styles.secondaryButton} onClick={() => void loadTrend()} type="button">
                    刷新趋势
                  </button>
                </div>
              </div>
            </section>

            <section className={styles.card}>
              <h2>流量趋势图</h2>
              <div className={styles.chartWrap}>
                <Line data={trendChartData} />
              </div>
            </section>

            <section className={styles.card}>
              <h2>情绪趋势</h2>
              <div className={styles.chartWrap}>
                <Line data={sentimentChartData} />
              </div>
            </section>

            <section className={styles.card}>
              <h2>每日统计</h2>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>日期</th>
                      <th>平均得分</th>
                      <th>最高得分</th>
                      <th>最低得分</th>
                      <th>采集次数</th>
                      <th>热门话题</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dailyStatistics.map((item) => (
                      <tr key={asText(item.date, Math.random().toString())}>
                        <td>{asText(item.date)}</td>
                        <td>{integerText(item.avg_score)}</td>
                        <td>{integerText(item.max_score)}</td>
                        <td>{integerText(item.min_score)}</td>
                        <td>{integerText(item.snapshot_count)}</td>
                        <td>{((item.top_topics as string[] | undefined) ?? []).slice(0, 3).join("、") || "N/A"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!dailyStatistics.length ? <div className={styles.muted}>暂无每日统计数据。</div> : null}
            </section>
          </>
        ) : null}

        {panel === "history" ? (
          historyDetail ? (
            <>
              <section className={styles.card}>
                <div className={styles.responsiveActionGrid}>
                  <button className={styles.secondaryButton} onClick={() => setHistoryDetail(null)} type="button">
                    返回历史列表
                  </button>
                </div>
              </section>

              <section className={styles.card}>
                <h2>历史详情</h2>
                <div className={styles.compactGrid}>
                  <div className={styles.metric}>
                    <span className={styles.muted}>流量得分</span>
                    <strong>{integerText(historySnapshot?.total_score)}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>流量等级</span>
                    <strong>{asText(historySnapshot?.flow_level, "N/A")}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>情绪指数</span>
                    <strong>{integerText(historySentiment?.sentiment_index)}</strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.muted}>AI 建议</span>
                    <strong>{asText(historyAiAnalysis?.advice, "N/A")}</strong>
                  </div>
                </div>
                {historySnapshot?.analysis ? (
                  <div className={styles.listItem} style={{ marginTop: 16 }}>
                    <strong>快照分析</strong>
                    <div style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>{asText(historySnapshot.analysis, "")}</div>
                  </div>
                ) : null}
                {historyAiAnalysis ? (
                  <div className={styles.listItem} style={{ marginTop: 16 }}>
                    <strong>AI 分析摘要</strong>
                    <div style={{ marginTop: 10 }}>{asText(historyAiAnalysis.summary, "暂无摘要")}</div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险等级: {asText(historyAiAnalysis.risk_level, "N/A")} | 置信度 {integerText(historyAiAnalysis.confidence)}%
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      影响板块: {asDisplayStringArray(historyAiAnalysis.affected_sectors, ["name", "sector", "theme"]).join("、") || "N/A"}
                    </div>
                    <div className={styles.muted} style={{ marginTop: 8 }}>
                      风险因素: {asDisplayStringArray(historyAiAnalysis.risk_factors).join("、") || "N/A"}
                    </div>
                  </div>
                ) : null}
              </section>

              <section className={styles.card}>
                <h2>热点话题列表</h2>
                <TopicBubbleCloud cloudKeyPrefix="history" emptyText="暂无热点话题详情。" topics={historyHotTopics} />
              </section>

              <section className={styles.card}>
                <h2>热点新闻列表</h2>
                <div className={styles.list}>
                  {historyRelatedNews.map((item, index) => (
                    <div className={styles.listItem} key={`history-news-${index}`}>
                      <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        {((item.matched_keywords as string[] | undefined) ?? []).join("、") || "无关键词"} | {formatDateTime(item.publish_time ?? item.fetch_time, "")}
                      </div>
                    </div>
                  ))}
                  {!historyRelatedNews.length ? <div className={styles.muted}>暂无热点新闻详情。</div> : null}
                </div>
              </section>
            </>
          ) : (
            <>
              <section className={styles.card}>
                <h2>历史快照</h2>
                <div className={styles.list}>
                  {history.map((item) => (
                    <div className={styles.listItem} key={String(item.id)}>
                      <strong>{formatDateTime(item.fetch_time, "")} - 流量得分 {integerText(item.total_score)} ({asText(item.flow_level, "中")})</strong>
                      <div style={{ marginTop: 8 }}>{asText(item.analysis, "")}</div>
                      <div className={styles.responsiveActionGrid} style={{ marginTop: 12 }}>
                        <button className={styles.secondaryButton} onClick={() => void openHistoryDetail(Number(item.id))} type="button">
                          查看详情
                        </button>
                      </div>
                    </div>
                  ))}
                  {!history.length ? <div className={styles.muted}>暂无历史快照。</div> : null}
                </div>
              </section>

              <section className={styles.card}>
                <h2>AI 分析历史</h2>
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>时间</th>
                        <th>建议</th>
                        <th>置信度</th>
                        <th>风险</th>
                        <th>摘要</th>
                      </tr>
                    </thead>
                    <tbody>
                      {aiHistory.map((item) => (
                        <tr key={String(item.id ?? item.created_at)}>
                          <td>{formatDateTime(item.created_at ?? item.fetch_time, "")}</td>
                          <td>{asText(item.advice, "N/A")}</td>
                          <td>{integerText(item.confidence)}%</td>
                          <td>{asText(item.risk_level, "N/A")}</td>
                          <td>{asText(item.summary, "")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className={styles.card}>
                <h2>调度日志</h2>
                <div className={styles.tableWrap}>
                  <table className={styles.table}>
                    <thead>
                      <tr>
                        <th>时间</th>
                        <th>任务</th>
                        <th>状态</th>
                        <th>消息</th>
                      </tr>
                    </thead>
                    <tbody>
                      {schedulerLogs.map((item) => (
                        <tr key={String(item.id ?? item.executed_at)}>
                          <td>{asText(item.executed_at, "")}</td>
                          <td>{asText(item.task_name, "N/A")}</td>
                          <td>{asText(item.status, "N/A")}</td>
                          <td>{asText(item.message, "")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className={styles.card}>
                <h2>股票相关新闻检索</h2>
                <div className={styles.responsiveActionGrid}>
                  <input
                    className={styles.actionInput}
                    placeholder="输入股票名、代码或关键词"
                    value={searchKeyword}
                    onChange={(event) => setSearchKeyword(event.target.value)}
                  />
                  <button className={styles.primaryButton} onClick={() => void searchStockNews()} type="button">
                    搜索
                  </button>
                </div>
                <div className={styles.list} style={{ marginTop: 16 }}>
                  {searchResults.map((item, index) => (
                    <div className={styles.listItem} key={`search-${index}`}>
                      <strong>[{asText(item.platform_name, "平台")}] {asText(item.title)}</strong>
                      <div className={styles.muted} style={{ marginTop: 8 }}>
                        {((item.matched_keywords as string[] | undefined) ?? []).join("、") || "无关键词"} | {formatDateTime(item.fetch_time ?? item.publish_time, "")}
                      </div>
                    </div>
                  ))}
                  {!searchResults.length ? <div className={styles.muted}>输入关键词后可检索历史股票相关新闻。</div> : null}
                </div>
              </section>
            </>
          )
        ) : null}

      </div>
    </PageFrame>
  );
}
