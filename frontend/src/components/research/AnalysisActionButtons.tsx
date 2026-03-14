import { useNavigate } from "react-router-dom";

import { encodeIntent, type TypedIntent } from "../../lib/intents";
import { useResearchStore } from "../../stores/researchStore";
import styles from "./ResearchPanels.module.scss";

export interface ActionPayload {
  symbol: string;
  stock_name: string;
  account_name: string;
  origin_analysis_id?: number;
  default_cost_price?: number;
  default_note?: string;
  strategy_context?: Record<string, unknown>;
}

interface AnalysisActionButtonsProps {
  actionPayload?: ActionPayload | null;
  recordId?: number;
  portfolioLabel?: string;
  isInPortfolio?: boolean;
  showPortfolioAction?: boolean;
}

export function AnalysisActionButtons({
  actionPayload,
  recordId,
  portfolioLabel,
  isInPortfolio = false,
  showPortfolioAction = true,
}: AnalysisActionButtonsProps) {
  const navigate = useNavigate();
  const setIntent = useResearchStore((state) => state.setIntent);

  const openIntent = (path: string, intent: TypedIntent<ActionPayload>) => {
    setIntent(intent);
    navigate(`${path}?intent=${encodeIntent(intent)}`);
  };

  return (
    <div className={styles.actionRow}>
      {recordId ? (
        <button className={styles.actionButton} onClick={() => navigate(`/research/history?recordId=${recordId}`)} type="button">
          查看历史
        </button>
      ) : null}
      {actionPayload ? (
        <>
          <button
            className={styles.actionButton}
            onClick={() => openIntent("/investment/smart-monitor", { type: "watchlist", payload: actionPayload })}
            type="button"
          >
            加入盯盘
          </button>
          {showPortfolioAction ? (
            <button
              className={styles.primaryAction}
              onClick={() =>
                isInPortfolio
                  ? navigate("/investment/portfolio")
                  : openIntent("/investment/portfolio", { type: "portfolio", payload: actionPayload })
              }
              type="button"
            >
              {portfolioLabel || (isInPortfolio ? "跳转持仓" : "设为持仓")}
            </button>
          ) : null}
        </>
      ) : null}
    </div>
  );
}
