/**
 * Fetchers for the engine-driven backtest endpoints:
 *
 *   * ``GET  /api/execution/backtest/profiles`` — list risk profiles
 *   * ``POST /api/execution/backtest/run``      — run an engine backtest
 *
 * The backend payload is snake_case; we map to camelCase shapes the React
 * components consume.
 */

import {
  fetchDashboardJsonStrict,
  postDashboardJson,
} from '@/lib/api/client';

// ---------------------------------------------------------------------------
// Profile listing
// ---------------------------------------------------------------------------

export interface RiskProfileEntry {
  requireStage2: boolean;
  requirePriceAboveSma200: boolean;
  requireSectorPositive: boolean;
  minVolumeRatio: number;
  requireDeliveryAboveSectorMedian: boolean;
}

export interface RiskProfileStop {
  method: string;
  atrMultiple: number;
  stopPct: number;
  hybridAtrMultiple: number;
}

export interface RiskProfileExit {
  emergencyExitBelowSma200: boolean;
  dmaExitWindow: number | null;
  dmaWhipsawBufferPct: number;
  exitOnRankDeterioration: boolean;
  maxHoldRank: number;
  rankDeteriorationBars: number;
  exitOnScoreDeterioration: boolean;
  minHoldScore: number;
  scoreDeteriorationBars: number;
  timeStopDays: number | null;
}

export interface RiskProfileSizing {
  method: string;
  riskPerTradePct: number;
  maxPositionPct: number;
}

export interface RiskProfileConstraints {
  maxConcurrentPositions: number;
  maxStockWeightPct: number;
  maxSectorExposurePct: number;
}

export interface RiskProfile {
  name: string;
  path: string;
  entry: RiskProfileEntry;
  stop: RiskProfileStop;
  exit: RiskProfileExit;
  sizing: RiskProfileSizing;
  constraints: RiskProfileConstraints;
}

export interface RiskProfilesResponse {
  profiles: RiskProfile[];
}

interface BackendProfile {
  name?: string;
  path?: string;
  entry?: Record<string, unknown>;
  stop?: Record<string, unknown>;
  exit?: Record<string, unknown>;
  sizing?: Record<string, unknown>;
  constraints?: Record<string, unknown>;
}

const num = (v: unknown, fallback = 0): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};
const bool = (v: unknown, fallback = false): boolean =>
  typeof v === 'boolean' ? v : fallback;

function mapProfile(raw: BackendProfile): RiskProfile {
  const e = raw.entry ?? {};
  const s = raw.stop ?? {};
  const x = raw.exit ?? {};
  const z = raw.sizing ?? {};
  const c = raw.constraints ?? {};
  return {
    name: raw.name ?? 'unknown',
    path: raw.path ?? '',
    entry: {
      requireStage2: bool(e['require_stage_2'], true),
      requirePriceAboveSma200: bool(e['require_price_above_sma200'], true),
      requireSectorPositive: bool(e['require_sector_positive'], true),
      minVolumeRatio: num(e['min_volume_ratio'], 1.5),
      requireDeliveryAboveSectorMedian: bool(e['require_delivery_above_sector_median']),
    },
    stop: {
      method: String(s['method'] ?? 'atr'),
      atrMultiple: num(s['atr_multiple'], 2.0),
      stopPct: num(s['stop_pct'], 0.05),
      hybridAtrMultiple: num(s['hybrid_atr_multiple'], 2.5),
    },
    exit: {
      emergencyExitBelowSma200: bool(x['emergency_exit_below_sma200'], true),
      dmaExitWindow: x['dma_exit_window'] == null ? null : Number(x['dma_exit_window']),
      dmaWhipsawBufferPct: num(x['dma_whipsaw_buffer_pct'], 0.5),
      exitOnRankDeterioration: bool(x['exit_on_rank_deterioration'], true),
      maxHoldRank: num(x['max_hold_rank'], 50),
      rankDeteriorationBars: num(x['rank_deterioration_bars'], 3),
      exitOnScoreDeterioration: bool(x['exit_on_score_deterioration'], true),
      minHoldScore: num(x['min_hold_score'], 60),
      scoreDeteriorationBars: num(x['score_deterioration_bars'], 3),
      timeStopDays: x['time_stop_days'] == null ? null : Number(x['time_stop_days']),
    },
    sizing: {
      method: String(z['method'] ?? 'equal_weight'),
      riskPerTradePct: num(z['risk_per_trade_pct'], 1.0),
      maxPositionPct: num(z['max_position_pct'], 12.0),
    },
    constraints: {
      maxConcurrentPositions: num(c['max_concurrent_positions'], 8),
      maxStockWeightPct: num(c['max_stock_weight_pct'], 12.0),
      maxSectorExposurePct: num(c['max_sector_exposure_pct'], 30.0),
    },
  };
}

export async function getRiskProfiles(): Promise<RiskProfilesResponse> {
  const raw = await fetchDashboardJsonStrict<{ profiles: BackendProfile[] }>(
    '/api/execution/backtest/profiles',
    { profiles: [] },
  );
  return { profiles: (raw.profiles ?? []).map(mapProfile) };
}

// ---------------------------------------------------------------------------
// Backtest run
// ---------------------------------------------------------------------------

export interface BacktestTrade {
  symbolId: string;
  exchange: string;
  entryDate: string;
  entryPrice: number;
  entryReason: string;
  stopPrice: number | null;
  stopMethod: string | null;
  rankAtEntry: number | null;
  scoreAtEntry: number | null;
  sector: string;
  shares: number;
  exitDate: string | null;
  exitPrice: number | null;
  exitReason: string | null;
  rankAtExit: number | null;
  scoreAtExit: number | null;
  dmaExitLine: number | null;
  barsHeld: number;
  pnl: number | null;
  pnlPct: number | null;
}

export interface BacktestEquityPoint {
  date: string;
  equity: number;
  openPositions: number;
}

export interface BacktestRunResult {
  status: string;
  profile: string;
  dataSource: string;
  fromDate: string | null;
  toDate: string | null;
  startingEquity: number;
  endingEquity: number;
  tradingDays: number;
  tradeCount: number;
  exitReasonCounts: Record<string, number>;
  trades: BacktestTrade[];
  equityCurve: BacktestEquityPoint[];
  artifactDir: string | null;
  message?: string;
}

interface BackendTrade {
  symbol_id?: string;
  exchange?: string;
  entry_date?: string | null;
  entry_price?: number | null;
  entry_reason?: string | null;
  stop_price?: number | null;
  stop_method?: string | null;
  rank_at_entry?: number | null;
  score_at_entry?: number | null;
  sector?: string | null;
  shares?: number | null;
  exit_date?: string | null;
  exit_price?: number | null;
  exit_reason?: string | null;
  rank_at_exit?: number | null;
  score_at_exit?: number | null;
  dma_exit_line?: number | null;
  bars_held?: number | null;
  pnl?: number | null;
  pnl_pct?: number | null;
}

interface BackendEquity {
  date?: string | null;
  equity?: number | null;
  open_positions?: number | null;
}

interface BackendBacktestResult {
  status?: string;
  profile?: string;
  data_source?: string;
  from_date?: string | null;
  to_date?: string | null;
  starting_equity?: number;
  ending_equity?: number;
  trading_days?: number;
  trade_count?: number;
  exit_reason_counts?: Record<string, number>;
  trades?: BackendTrade[];
  equity_curve?: BackendEquity[];
  artifact_dir?: string | null;
  message?: string;
}

function mapTrade(raw: BackendTrade): BacktestTrade {
  return {
    symbolId: raw.symbol_id ?? '',
    exchange: raw.exchange ?? 'NSE',
    entryDate: raw.entry_date ?? '',
    entryPrice: num(raw.entry_price),
    entryReason: raw.entry_reason ?? '',
    stopPrice: raw.stop_price == null ? null : Number(raw.stop_price),
    stopMethod: raw.stop_method ?? null,
    rankAtEntry: raw.rank_at_entry == null ? null : Number(raw.rank_at_entry),
    scoreAtEntry: raw.score_at_entry == null ? null : Number(raw.score_at_entry),
    sector: raw.sector ?? '',
    shares: num(raw.shares),
    exitDate: raw.exit_date ?? null,
    exitPrice: raw.exit_price == null ? null : Number(raw.exit_price),
    exitReason: raw.exit_reason ?? null,
    rankAtExit: raw.rank_at_exit == null ? null : Number(raw.rank_at_exit),
    scoreAtExit: raw.score_at_exit == null ? null : Number(raw.score_at_exit),
    dmaExitLine: raw.dma_exit_line == null ? null : Number(raw.dma_exit_line),
    barsHeld: num(raw.bars_held),
    pnl: raw.pnl == null ? null : Number(raw.pnl),
    pnlPct: raw.pnl_pct == null ? null : Number(raw.pnl_pct),
  };
}

export interface RunBacktestParams {
  profile: string;
  dataSource?: string;
  fromDate?: string;
  toDate?: string;
  equity?: number;
  persist?: boolean;
  customConfig?: RiskProfileCustomConfig;
}

export interface RiskProfileCustomConfig {
  entry?: Record<string, unknown>;
  stop?: Record<string, unknown>;
  exit?: Record<string, unknown>;
  sizing?: Record<string, unknown>;
  constraints?: Record<string, unknown>;
}

export async function runBacktest(
  params: RunBacktestParams,
): Promise<BacktestRunResult> {
  const body: Record<string, unknown> = {
    profile: params.profile,
    data_source: params.dataSource ?? 'pipeline_replay',
    from_date: params.fromDate ?? null,
    to_date: params.toDate ?? null,
    equity: params.equity ?? 1_000_000,
    persist: params.persist ?? true,
  };
  if (params.customConfig) {
    body.custom_config = params.customConfig;
  }
  const raw = await postDashboardJson<BackendBacktestResult>(
    '/api/execution/backtest/run',
    body,
  );
  return {
    status: raw.status ?? 'unknown',
    profile: raw.profile ?? params.profile,
    dataSource: raw.data_source ?? params.dataSource ?? 'pipeline_replay',
    fromDate: raw.from_date ?? null,
    toDate: raw.to_date ?? null,
    startingEquity: num(raw.starting_equity, params.equity ?? 1_000_000),
    endingEquity: num(raw.ending_equity, num(raw.starting_equity, 1_000_000)),
    tradingDays: num(raw.trading_days),
    tradeCount: num(raw.trade_count),
    exitReasonCounts: raw.exit_reason_counts ?? {},
    trades: (raw.trades ?? []).map(mapTrade),
    equityCurve: (raw.equity_curve ?? []).map((e) => ({
      date: e.date ?? '',
      equity: num(e.equity),
      openPositions: num(e.open_positions),
    })),
    artifactDir: raw.artifact_dir ?? null,
    message: raw.message,
  };
}
