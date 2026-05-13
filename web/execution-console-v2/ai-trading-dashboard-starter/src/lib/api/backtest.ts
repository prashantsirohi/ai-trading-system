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
  sync: BacktestSyncSummary | null;
  dataQuality: BacktestDataQuality | null;
  runMetadata: BacktestRunMetadata | null;
  message?: string;
}

export interface BacktestSyncSummary {
  status?: string;
  exchange?: string;
  sourceFromDate?: string | null;
  sourceToDate?: string | null;
  sourceRows?: number | null;
  rowsToCopy?: number | null;
  insertedRows?: number | null;
  targetRowsInSourceRange?: number | null;
  totalTargetRows?: number | null;
  syncedAt?: string | null;
  refreshMode?: string | null;
  masterdata?: {
    status?: string;
    tableCount?: number;
    syncedAt?: string | null;
  } | null;
}

export interface BacktestDataQuality {
  status?: string;
  rowCount?: number;
  symbolCount?: number;
  minDate?: string | null;
  maxDate?: string | null;
  missingOhlcvRows?: number;
  duplicateTimestampCount?: number;
  duplicateDailyCount?: number;
  insufficientSma200SymbolCount?: number;
  masterdataExists?: boolean;
  warnings?: string[];
}

export interface BacktestRunMetadata {
  gitCommit?: string | null;
  rankingMethodVersion?: string | null;
  generatedAt?: string | null;
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
  sync?: Record<string, unknown> | null;
  data_quality?: Record<string, unknown> | null;
  run_metadata?: Record<string, unknown> | null;
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
    sync: mapSync(raw.sync),
    dataQuality: mapDataQuality(raw.data_quality),
    runMetadata: mapRunMetadata(raw.run_metadata),
    message: raw.message,
  };
}

// ---------------------------------------------------------------------------
// Winner capture
// ---------------------------------------------------------------------------

export interface WinnerCaptureSummary {
  winnerCount: number;
  rankCutoff: number;
  capturedCount: number;
  missedCount: number;
  captureRate: number;
  medianDaysToCapture: number | null;
  medianFirstCaptureRank: number | null;
  averageYearlyReturnCaptured: number | null;
  averageYearlyReturnMissed: number | null;
}

export interface WinnerCaptureRow {
  rankInYear: number;
  symbolId: string;
  exchange: string;
  yearlyReturn: number;
  startDate: string | null;
  endDate: string | null;
  startClose: number;
  endClose: number;
  captured: boolean;
  firstCaptureDate: string | null;
  firstCaptureRank: number | null;
  firstCaptureScore: number | null;
  firstCaptureClose: number | null;
  bestRank: number | null;
  bestRankDate: string | null;
  daysToCapture: number | null;
  returnAtCapture: number | null;
  remainingReturnAfterCapture: number | null;
}

export interface WinnerCaptureResult {
  status: string;
  year: number;
  exchange: string;
  topGainers: number;
  rankCutoff: number;
  startDate: string;
  endDate: string;
  summary: WinnerCaptureSummary;
  winners: WinnerCaptureRow[];
  artifactDir: string | null;
  sync: BacktestSyncSummary | null;
  dataQuality: BacktestDataQuality | null;
  runMetadata: BacktestRunMetadata | null;
  message?: string;
}

export interface RunWinnerCaptureParams {
  year: number;
  exchange?: string;
  topGainers?: number;
  rankCutoff?: number;
  persist?: boolean;
}

interface BackendWinnerCaptureSummary {
  winner_count?: number;
  rank_cutoff?: number;
  captured_count?: number;
  missed_count?: number;
  capture_rate?: number;
  median_days_to_capture?: number | null;
  median_first_capture_rank?: number | null;
  average_yearly_return_captured?: number | null;
  average_yearly_return_missed?: number | null;
}

interface BackendWinnerCaptureRow {
  rank_in_year?: number;
  symbol_id?: string;
  exchange?: string;
  yearly_return?: number;
  start_date?: string | null;
  end_date?: string | null;
  start_close?: number;
  end_close?: number;
  captured?: boolean;
  first_capture_date?: string | null;
  first_capture_rank?: number | null;
  first_capture_score?: number | null;
  first_capture_close?: number | null;
  best_rank?: number | null;
  best_rank_date?: string | null;
  days_to_capture?: number | null;
  return_at_capture?: number | null;
  remaining_return_after_capture?: number | null;
}

interface BackendWinnerCaptureResult {
  status?: string;
  year?: number;
  exchange?: string;
  top_gainers?: number;
  rank_cutoff?: number;
  start_date?: string;
  end_date?: string;
  summary?: BackendWinnerCaptureSummary;
  winners?: BackendWinnerCaptureRow[];
  artifact_dir?: string | null;
  sync?: Record<string, unknown> | null;
  data_quality?: Record<string, unknown> | null;
  run_metadata?: Record<string, unknown> | null;
  message?: string;
}

export async function runWinnerCapture(
  params: RunWinnerCaptureParams,
): Promise<WinnerCaptureResult> {
  const raw = await postDashboardJson<BackendWinnerCaptureResult>(
    '/api/execution/backtest/winner-capture',
    {
      year: params.year,
      exchange: params.exchange ?? 'NSE',
      top_gainers: params.topGainers ?? 50,
      rank_cutoff: params.rankCutoff ?? 50,
      persist: params.persist ?? true,
    },
  );
  const summary = raw.summary ?? {};
  return {
    status: raw.status ?? 'unknown',
    year: num(raw.year, params.year),
    exchange: raw.exchange ?? params.exchange ?? 'NSE',
    topGainers: num(raw.top_gainers, params.topGainers ?? 50),
    rankCutoff: num(raw.rank_cutoff, params.rankCutoff ?? 50),
    startDate: raw.start_date ?? '',
    endDate: raw.end_date ?? '',
    summary: {
      winnerCount: num(summary.winner_count),
      rankCutoff: num(summary.rank_cutoff, params.rankCutoff ?? 50),
      capturedCount: num(summary.captured_count),
      missedCount: num(summary.missed_count),
      captureRate: num(summary.capture_rate),
      medianDaysToCapture: summary.median_days_to_capture == null ? null : Number(summary.median_days_to_capture),
      medianFirstCaptureRank: summary.median_first_capture_rank == null ? null : Number(summary.median_first_capture_rank),
      averageYearlyReturnCaptured: summary.average_yearly_return_captured == null ? null : Number(summary.average_yearly_return_captured),
      averageYearlyReturnMissed: summary.average_yearly_return_missed == null ? null : Number(summary.average_yearly_return_missed),
    },
    winners: (raw.winners ?? []).map((row) => ({
      rankInYear: num(row.rank_in_year),
      symbolId: row.symbol_id ?? '',
      exchange: row.exchange ?? 'NSE',
      yearlyReturn: num(row.yearly_return),
      startDate: row.start_date ?? null,
      endDate: row.end_date ?? null,
      startClose: num(row.start_close),
      endClose: num(row.end_close),
      captured: Boolean(row.captured),
      firstCaptureDate: row.first_capture_date ?? null,
      firstCaptureRank: row.first_capture_rank == null ? null : Number(row.first_capture_rank),
      firstCaptureScore: row.first_capture_score == null ? null : Number(row.first_capture_score),
      firstCaptureClose: row.first_capture_close == null ? null : Number(row.first_capture_close),
      bestRank: row.best_rank == null ? null : Number(row.best_rank),
      bestRankDate: row.best_rank_date ?? null,
      daysToCapture: row.days_to_capture == null ? null : Number(row.days_to_capture),
      returnAtCapture: row.return_at_capture == null ? null : Number(row.return_at_capture),
      remainingReturnAfterCapture: row.remaining_return_after_capture == null ? null : Number(row.remaining_return_after_capture),
    })),
    artifactDir: raw.artifact_dir ?? null,
    sync: mapSync(raw.sync),
    dataQuality: mapDataQuality(raw.data_quality),
    runMetadata: mapRunMetadata(raw.run_metadata),
    message: raw.message,
  };
}

function mapSync(raw?: Record<string, unknown> | null): BacktestSyncSummary | null {
  if (!raw) return null;
  const master = raw['masterdata'] && typeof raw['masterdata'] === 'object'
    ? (raw['masterdata'] as Record<string, unknown>)
    : null;
  return {
    status: String(raw['status'] ?? ''),
    exchange: String(raw['exchange'] ?? ''),
    sourceFromDate: raw['source_from_date'] == null ? null : String(raw['source_from_date']),
    sourceToDate: raw['source_to_date'] == null ? null : String(raw['source_to_date']),
    sourceRows: raw['source_rows'] == null ? null : num(raw['source_rows']),
    rowsToCopy: raw['rows_to_copy'] == null ? null : num(raw['rows_to_copy']),
    insertedRows: raw['inserted_rows'] == null ? null : num(raw['inserted_rows']),
    targetRowsInSourceRange: raw['target_rows_in_source_range'] == null ? null : num(raw['target_rows_in_source_range']),
    totalTargetRows: raw['total_target_rows'] == null ? null : num(raw['total_target_rows']),
    syncedAt: raw['synced_at'] == null ? null : String(raw['synced_at']),
    refreshMode: raw['refresh_mode'] == null ? null : String(raw['refresh_mode']),
    masterdata: master
      ? {
          status: String(master['status'] ?? ''),
          tableCount: num(master['table_count']),
          syncedAt: master['synced_at'] == null ? null : String(master['synced_at']),
        }
      : null,
  };
}

function mapDataQuality(raw?: Record<string, unknown> | null): BacktestDataQuality | null {
  if (!raw) return null;
  return {
    status: String(raw['status'] ?? ''),
    rowCount: num(raw['row_count']),
    symbolCount: num(raw['symbol_count']),
    minDate: raw['min_date'] == null ? null : String(raw['min_date']),
    maxDate: raw['max_date'] == null ? null : String(raw['max_date']),
    missingOhlcvRows: num(raw['missing_ohlcv_rows']),
    duplicateTimestampCount: num(raw['duplicate_timestamp_count']),
    duplicateDailyCount: num(raw['duplicate_daily_count']),
    insufficientSma200SymbolCount: num(raw['insufficient_sma200_symbol_count']),
    masterdataExists: Boolean(raw['masterdata_exists']),
    warnings: Array.isArray(raw['warnings']) ? raw['warnings'].map(String) : [],
  };
}

function mapRunMetadata(raw?: Record<string, unknown> | null): BacktestRunMetadata | null {
  if (!raw) return null;
  return {
    gitCommit: raw['git_commit'] == null ? null : String(raw['git_commit']),
    rankingMethodVersion: raw['ranking_method_version'] == null ? null : String(raw['ranking_method_version']),
    generatedAt: raw['generated_at'] == null ? null : String(raw['generated_at']),
  };
}
