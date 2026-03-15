import { useEffect, useMemo, useRef, useState } from "react";

import { asNumber, asText, integerText, numberText } from "../../lib/market";
import styles from "../../pages/ConsolePage.module.scss";

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

const noiseTopicKeywords = new Set([
  "今天", "今日", "昨日", "当天", "目前", "最新", "刚刚", "刚才", "现场", "视频", "画面", "消息", "通报", "回应", "公告",
  "报道", "曝光", "热议", "关注", "提醒", "发布", "披露", "情况", "原因", "结果", "事件", "详情", "内容", "问题", "影响",
  "进展", "后续", "背后", "真相", "实情", "网友称", "网友说", "网友热议", "男子", "女子", "老人", "大爷", "大妈", "小伙",
  "小伙子", "小伙儿", "小女孩", "小男孩", "男孩", "女孩", "儿童", "家长", "学生", "老师", "司机", "乘客", "顾客", "老板",
  "员工", "市民", "人员", "群众", "民众", "居民", "行人", "路人", "网友", "博主", "专家", "记者", "警方", "官方", "平台",
  "企业", "公司", "品牌", "多人", "有人", "一些人", "不少人", "很多人", "部分人", "相关人士", "知情人", "一名", "一个", "一家",
  "一位", "一人", "一地", "一事", "一起", "一则", "一图", "一文", "这名", "这位", "这个", "这起", "这类", "该名", "该位",
  "该公司", "此事", "其后", "其间", "其中", "他们", "我们", "你们", "大家", "自己", "什么", "到底", "究竟", "为何会", "为何要",
  "何时", "何处", "谁在", "谁是", "为何", "如何", "怎么", "为什么", "是否", "竟然", "居然", "原来", "果然", "其实", "真的",
  "真有", "确实", "实为", "堪称", "堪比", "直击", "速看", "快看", "必看", "值得", "注意", "警惕", "提示", "揭秘", "解读",
  "梳理", "汇总", "盘点", "观察", "点评", "分析", "预测", "研判", "判断", "结论", "信号", "趋势", "机会", "风险", "不能",
  "不会", "没有", "不是", "可以", "已经", "正在", "仍在", "再次", "又一", "再度", "再现", "引发", "带来", "涉及", "关于",
  "更多", "不少", "多个", "有关", "相关", "重要", "重大", "重磅", "紧急", "突发", "特别", "明显", "显著", "核心", "关键",
  "热门", "火爆", "高位", "强势", "全面", "持续", "进一步", "首次", "首次回应", "最新回应", "最新进展", "情况通报", "警方通报",
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
  { background: "rgba(244, 199, 63, 0.7)", borderColor: "rgba(214, 164, 95, 0.42)" },
  { background: "rgba(105, 193, 221, 0.68)", borderColor: "rgba(92, 137, 167, 0.4)" },
  { background: "rgba(239, 124, 74, 0.66)", borderColor: "rgba(198, 93, 75, 0.38)" },
  { background: "rgba(150, 210, 116, 0.66)", borderColor: "rgba(113, 155, 77, 0.38)" },
  { background: "rgba(170, 140, 244, 0.64)", borderColor: "rgba(130, 108, 201, 0.38)" },
  { background: "rgba(79, 201, 171, 0.66)", borderColor: "rgba(58, 154, 130, 0.38)" },
  { background: "rgba(244, 152, 188, 0.64)", borderColor: "rgba(196, 113, 146, 0.36)" },
  { background: "rgba(124, 176, 255, 0.66)", borderColor: "rgba(86, 128, 196, 0.38)" },
  { background: "rgba(255, 173, 92, 0.65)", borderColor: "rgba(210, 129, 52, 0.38)" },
  { background: "rgba(112, 204, 116, 0.64)", borderColor: "rgba(75, 153, 84, 0.36)" },
];

function normalizeTopicText(topic: unknown): string {
  return asText(topic, "")
    .replace(/[【】\[\]（）()《》<>「」『』"'`]/g, "")
    .replace(/\s+/g, "")
    .trim();
}

export function filterMeaningfulTopics(topics: Array<Record<string, unknown>>): Array<Record<string, unknown>> {
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

function buildTopicBubbleItems(
  topics: Array<Record<string, unknown>>,
  keyPrefix: string,
  containerWidth: number,
  containerHeight: number,
): TopicBubbleRenderItem[] {
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
          const pushAngle =
            distance > 0.001
              ? Math.atan2(dy, dx)
              : (((Math.abs(hash) + attempt * 97) % 360) * Math.PI) / 180;
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

export function TopicBubbleCloud({
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
