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
  rankPosition?: number | null;
  rs: number;
  volume: 'High' | 'Medium' | 'Low';
  sector: string;
  breakout: boolean;
  pattern: string;
  patternState?: string | null;
  setupQuality?: number | null;
  pivotPrice?: number | null;
  invalidationPrice?: number | null;
  patternSignalDate?: string | null;
  patternStartDate?: string | null;
  patternEndDate?: string | null;
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
  fundamentalScore?: number | null;
  fundamentalTier?: 'A' | 'B' | 'C' | 'Reject' | null;
  qualityScore?: number | null;
  growthScore?: number | null;
  balanceSheetScore?: number | null;
  valuationScore?: number | null;
  ownershipScore?: number | null;
  redFlags?: string | null;
  watchlistBucket?: string | null;
  nextAction?: string | null;
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
  stageS1Pct: number;
  stageS2Pct: number;
  stageS3Pct: number;
  stageS4Pct: number;
  stageS1Count: number;
  stageS2Count: number;
  stageS3Count: number;
  stageS4Count: number;
  stageTotal: number;
  valuationUniverseId?: string | null;
  valuationDate?: string | null;
  valuationConstituentCount?: number | null;
  sectorPeTtm?: number | null;
  sectorEarningsYield?: number | null;
  sectorLossMcapPct?: number | null;
  sectorPePctile3y?: number | null;
  sectorPePctile5y?: number | null;
  sectorPePctile10y?: number | null;
  valuationZone?: string | null;
  cycleSignal?: string | null;
  valuationInterpretation?: string | null;
  sectorEarningsGrowthScore?: number | null;
  sectorSalesYoyGrowth?: number | null;
  sectorProfitYoyGrowth?: number | null;
  sectorSalesQoqGrowth?: number | null;
  sectorProfitQoqGrowth?: number | null;
  salesYoyPositivePct?: number | null;
  profitYoyPositivePct?: number | null;
  marginExpansionPct?: number | null;
  earningsTrendLabel?: string | null;
  earningsReportDate?: string | null;
}

export interface ShadowModelRow {
  model: string;
  date: string;
  agreement: string;
  drift: string;
  status: string;
}
