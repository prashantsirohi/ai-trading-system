import type { StockRow } from '@/types/dashboard';

export interface SymbolDetailFallback {
  thesis: string;
  riskNote: string;
  patternConfidence: number;
  stopLoss: number | null;
  target: number | null;
  catalysts: string[];
}

const DEFAULT_FALLBACK: SymbolDetailFallback = {
  thesis: 'Momentum setup is active; waiting for confirmation from the next session close.',
  riskNote: 'Use disciplined sizing while market breadth is mixed.',
  patternConfidence: 62,
  stopLoss: null,
  target: null,
  catalysts: ['Relative strength leadership', 'Sector rotation support'],
};

const DETAIL_FALLBACKS: Record<string, SymbolDetailFallback> = {
  RELIANCE: {
    thesis: 'Energy major remains in leadership with broad participation from large-cap flows.',
    riskNote: 'Watch crude and macro headline risk around open.',
    patternConfidence: 84,
    stopLoss: 2868,
    target: 3040,
    catalysts: ['Strong institutional participation', 'Breakout above consolidation zone'],
  },
  INFY: {
    thesis: 'IT momentum improving with multi-session follow-through.',
    riskNote: 'Sensitive to overnight global tech sentiment.',
    patternConfidence: 77,
    stopLoss: 1598,
    target: 1715,
    catalysts: ['Earnings season positioning', 'Sector-relative trend acceleration'],
  },
  HDFCBANK: {
    thesis: 'Banking heavyweight showing stable trend continuation.',
    riskNote: 'Needs sustained volume expansion for stronger conviction.',
    patternConfidence: 69,
    stopLoss: 1718,
    target: 1830,
    catalysts: ['Broad banking participation', 'Defensive quality bid'],
  },
};

export function getSymbolDetailFallback(row: StockRow): SymbolDetailFallback {
  return DETAIL_FALLBACKS[row.symbol] ?? DEFAULT_FALLBACK;
}
