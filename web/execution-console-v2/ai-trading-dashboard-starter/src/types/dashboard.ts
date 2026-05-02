export type NavKey =
  | 'pipeline'
  | 'ranking'
  | 'patterns'
  | 'sectors'
  | 'execution'
  | 'runs'
  | 'shadow'
  | 'research';

export interface MetricCard {
  label: string;
  value: string;
  tone?: 'blue' | 'green' | 'yellow' | 'purple';
}

export interface StockRow {
  symbol: string;
  score: number;
  rs: number;
  volume: 'High' | 'Medium' | 'Low';
  sector: string;
  breakout: boolean;
  pattern: string;
  patternState?: string | null;
  setupQuality?: number | null;
  pivotPrice?: number | null;
  invalidationPrice?: number | null;
  reclaimSignal?: boolean;
  tier: 'A' | 'B' | 'C';
  price: number;
  sectorStrength: number;
  trend: number;
  aboveSma20?: boolean | null;
  aboveSma50?: boolean | null;
  aboveSma200?: boolean | null;
  stageLabel?: string | null;
  stageTransition?: string | null;
  barsInStage?: number | null;
  stageEntryDate?: string | null;
  stageFreshnessBucket?: string | null;
  momentumAccelerationScore?: number | null;
  exhaustionPenalty?: number | null;
  exhaustionFlag?: string | null;
  distanceFromPivotAtr?: number | null;
}

export interface RunStage {
  stage: string;
  status: string;
  duration: string;
}

export interface SectorScore {
  sector: string;
  rs: number;
  rs20: number;
  rs50: number;
  rs100: number;
  momentum: number;
  rank: number;
  rankPct: number;
  momentumRank: number;
  quadrant: string;
}

export interface ShadowModelRow {
  model: string;
  date: string;
  agreement: string;
  drift: string;
  status: string;
}
