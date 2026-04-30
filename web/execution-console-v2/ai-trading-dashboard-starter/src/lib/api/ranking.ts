/**
 * Fetchers for the ranking endpoints:
 *
 *   * ``GET /api/execution/ranking?limit=`` — list (existing).
 *   * ``GET /api/execution/ranking/{symbol}?run_id=`` — per-symbol detail
 *     used by the expandable row in the new ranking view (PR #8).
 *   * ``GET /api/execution/ranking/{symbol}/history?limit=`` — historical
 *     rank position sparkline data.
 *
 * Every response goes through a thin mapper into a stable camelCase shape
 * so React components don't depend on snake_case backend column names.
 */
import type { RankingResponse } from '@/types/api';
import { rankingMock } from '@/lib/mock/ranking';
import { getRankingDetailFallback, getRankingHistoryFallback } from '@/lib/mock/rankingDetails';
import { fetchDashboardJsonStrict } from '@/lib/api/client';
import { mapBackendStockRow } from '@/lib/api/mappers';

interface BackendRankingResponse {
  top_ranked?: Array<Record<string, string | number | boolean | null>>;
}

export async function getRanking(): Promise<RankingResponse> {
  const response = await fetchDashboardJsonStrict<BackendRankingResponse>(
    '/api/execution/ranking?limit=25',
    {
      top_ranked: rankingMock.rows as unknown as Array<Record<string, string | number | boolean | null>>,
    },
  );

  return {
    rows: (response.top_ranked ?? []).map(mapBackendStockRow),
  };
}

// ---------------------------------------------------------------------------
// /ranking/{symbol}
// ---------------------------------------------------------------------------

export type FactorBucket = 'rs' | 'volume' | 'trend' | 'sector' | 'other';

export interface FactorContributor {
  column: string;
  value: number;
}

export interface FactorBlock {
  bucket: FactorBucket;
  value: number;
  contributors: FactorContributor[];
}

export interface LifecycleStage {
  key: 'rank' | 'breakout' | 'pattern' | 'execution';
  label: string;
  state: 'pending' | 'active' | 'complete' | 'blocked';
  detail: string | null;
}

export interface RankingDetailRanking {
  rankPosition: number | null;
  universeSize: number;
  compositeScore: number | null;
  sectorName: string | null;
  category: string | null;
  inBreakoutScan: boolean;
  inPatternScan: boolean;
  stageLabel?: string | null;
  stageTransition?: string | null;
  barsInStage?: number | null;
  stageEntryDate?: string | null;
}

export interface RankingDetailDecision {
  verdict: string | null;
  confidence: string | null;
  reason: string | null;
}

export interface RankingDetail {
  available: boolean;
  symbol: string;
  runId: string | null;
  ranking: RankingDetailRanking | null;
  lifecycle: LifecycleStage[];
  decision: RankingDetailDecision;
  factors: FactorBlock[];
  sectorContext: Record<string, string | number | boolean | null> | null;
  operatorContext: RankingOperatorContext;
  rawRow: Record<string, string | number | boolean | null> | null;
}

export interface RankingOperatorContext {
  stageLabel: string | null;
  stageTransition: string | null;
  barsInStage: number | null;
  stageEntryDate: string | null;
  stageFreshnessBucket: string | null;
  momentumAccelerationScore: number | null;
  exhaustionPenalty: number | null;
  exhaustionFlag: string | null;
  distanceFromPivotAtr: number | null;
  topPatternFamily: string | null;
  topPatternState: string | null;
  topPatternSetupQuality: number | null;
  topPatternPivotPrice: number | null;
  topPatternInvalidationPrice: number | null;
  reclaimSignalFlag: boolean;
  explanation: string[];
}

interface BackendRankingDetail {
  available?: boolean;
  symbol?: string;
  run_id?: string | null;
  ranking?: {
    rank_position?: number | null;
    universe_size?: number | null;
    composite_score?: number | null;
    sector_name?: string | null;
    category?: string | null;
    in_breakout_scan?: boolean | null;
    in_pattern_scan?: boolean | null;
    stage_label?: string | null;
    stage_transition?: string | null;
    bars_in_stage?: number | null;
    stage_entry_date?: string | null;
  } | null;
  lifecycle?: Record<string, BackendLifecycleStage | null> | null;
  decision?: { verdict?: string | null; confidence?: string | null; reason?: string | null } | null;
  factors?: Record<string, { value?: number | null; contributors?: Array<{ column?: string; value?: number | null }> }> | null;
  sector_context?: Record<string, string | number | boolean | null> | null;
  operator_context?: {
    stage_label?: string | null;
    stage_transition?: string | null;
    bars_in_stage?: number | null;
    stage_entry_date?: string | null;
    stage_freshness_bucket?: string | null;
    momentum_acceleration_score?: number | null;
    exhaustion_penalty?: number | null;
    exhaustion_flag?: string | null;
    distance_from_pivot_atr?: number | null;
    top_pattern_family?: string | null;
    top_pattern_state?: string | null;
    top_pattern_setup_quality?: number | null;
    top_pattern_pivot_price?: number | null;
    top_pattern_invalidation_price?: number | null;
    reclaim_signal_flag?: boolean | null;
    explanation?: string[];
  } | null;
  raw_row?: Record<string, string | number | boolean | null> | null;
}

interface BackendLifecycleStage {
  state?: string | null;
  detail?: string | null;
}

const STAGE_KEYS: LifecycleStage['key'][] = ['rank', 'breakout', 'pattern', 'execution'];
const STAGE_LABELS: Record<LifecycleStage['key'], string> = {
  rank: 'Ranked',
  breakout: 'Breakout',
  pattern: 'Pattern',
  execution: 'Execution',
};

function asNum(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function asString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const str = String(value).trim();
  return str === '' ? null : str;
}

function mapStageState(raw: string | null | undefined): LifecycleStage['state'] {
  const norm = (raw ?? '').toLowerCase();
  if (norm === 'complete' || norm === 'completed' || norm === 'done') return 'complete';
  if (norm === 'active' || norm === 'in_progress' || norm === 'running') return 'active';
  if (norm === 'blocked' || norm === 'failed' || norm === 'rejected') return 'blocked';
  return 'pending';
}

function mapLifecycle(raw: BackendRankingDetail['lifecycle']): LifecycleStage[] {
  return STAGE_KEYS.map((key) => {
    const stage = raw?.[key] ?? null;
    return {
      key,
      label: STAGE_LABELS[key],
      state: mapStageState(stage?.state),
      detail: asString(stage?.detail),
    };
  });
}

function mapFactors(raw: BackendRankingDetail['factors']): FactorBlock[] {
  if (!raw) return [];
  const blocks: FactorBlock[] = [];
  for (const [bucket, slot] of Object.entries(raw)) {
    const value = asNum(slot?.value) ?? 0;
    const contributors = (slot?.contributors ?? [])
      .map((c) => ({ column: asString(c?.column) ?? '', value: asNum(c?.value) ?? 0 }))
      .filter((c) => c.column !== '');
    blocks.push({
      bucket: (bucket as FactorBucket) ?? 'other',
      value,
      contributors,
    });
  }
  // Stable display order: rs → volume → trend → sector → other.
  const order: Record<FactorBucket, number> = {
    rs: 0,
    volume: 1,
    trend: 2,
    sector: 3,
    other: 4,
  };
  blocks.sort((a, b) => (order[a.bucket] ?? 99) - (order[b.bucket] ?? 99));
  return blocks;
}

function mapOperatorContext(raw: BackendRankingDetail['operator_context']): RankingOperatorContext {
  return {
    stageLabel: asString(raw?.stage_label),
    stageTransition: asString(raw?.stage_transition),
    barsInStage: asNum(raw?.bars_in_stage),
    stageEntryDate: asString(raw?.stage_entry_date),
    stageFreshnessBucket: asString(raw?.stage_freshness_bucket),
    momentumAccelerationScore: asNum(raw?.momentum_acceleration_score),
    exhaustionPenalty: asNum(raw?.exhaustion_penalty),
    exhaustionFlag: asString(raw?.exhaustion_flag),
    distanceFromPivotAtr: asNum(raw?.distance_from_pivot_atr),
    topPatternFamily: asString(raw?.top_pattern_family),
    topPatternState: asString(raw?.top_pattern_state),
    topPatternSetupQuality: asNum(raw?.top_pattern_setup_quality),
    topPatternPivotPrice: asNum(raw?.top_pattern_pivot_price),
    topPatternInvalidationPrice: asNum(raw?.top_pattern_invalidation_price),
    reclaimSignalFlag: Boolean(raw?.reclaim_signal_flag),
    explanation: Array.isArray(raw?.explanation) ? raw.explanation.map(String) : [],
  };
}

export async function getRankingDetail(
  symbol: string,
  runId?: string | null,
): Promise<RankingDetail> {
  const path =
    runId != null
      ? `/api/execution/ranking/${encodeURIComponent(symbol)}?run_id=${encodeURIComponent(runId)}`
      : `/api/execution/ranking/${encodeURIComponent(symbol)}`;

  const raw = await fetchDashboardJsonStrict<BackendRankingDetail>(
    path,
    getRankingDetailFallback(symbol),
  );

  return {
    available: Boolean(raw.available),
    symbol: asString(raw.symbol) ?? symbol,
    runId: asString(raw.run_id ?? undefined),
    ranking: raw.ranking
      ? {
          rankPosition: asNum(raw.ranking.rank_position),
          universeSize: asNum(raw.ranking.universe_size) ?? 0,
          compositeScore: asNum(raw.ranking.composite_score),
          sectorName: asString(raw.ranking.sector_name),
          category: asString(raw.ranking.category),
          inBreakoutScan: Boolean(raw.ranking.in_breakout_scan),
          inPatternScan: Boolean(raw.ranking.in_pattern_scan),
          stageLabel: asString(raw.ranking.stage_label),
          stageTransition: asString(raw.ranking.stage_transition),
          barsInStage: asNum(raw.ranking.bars_in_stage),
          stageEntryDate: asString(raw.ranking.stage_entry_date),
        }
      : null,
    lifecycle: mapLifecycle(raw.lifecycle),
    decision: {
      verdict: asString(raw.decision?.verdict),
      confidence: asString(raw.decision?.confidence),
      reason: asString(raw.decision?.reason),
    },
    factors: mapFactors(raw.factors),
    sectorContext: raw.sector_context ?? null,
    operatorContext: mapOperatorContext(raw.operator_context),
    rawRow: raw.raw_row ?? null,
  };
}

// ---------------------------------------------------------------------------
// /ranking/{symbol}/history
// ---------------------------------------------------------------------------

export interface RankingHistoryPoint {
  runId: string;
  runDate: string | null;
  rankPosition: number | null;
  compositeScore: number | null;
}

export interface RankingHistory {
  available: boolean;
  symbol: string;
  history: RankingHistoryPoint[];
}

interface BackendRankingHistory {
  available?: boolean;
  symbol?: string;
  history?: Array<{
    run_id?: string | null;
    run_date?: string | null;
    rank_position?: number | null;
    composite_score?: number | null;
  }>;
}

export async function getRankingHistory(
  symbol: string,
  limit = 20,
): Promise<RankingHistory> {
  const raw = await fetchDashboardJsonStrict<BackendRankingHistory>(
    `/api/execution/ranking/${encodeURIComponent(symbol)}/history?limit=${limit}`,
    getRankingHistoryFallback(symbol, limit),
  );

  return {
    available: Boolean(raw.available),
    symbol: asString(raw.symbol) ?? symbol,
    history: (raw.history ?? []).map((point) => ({
      runId: asString(point.run_id) ?? '',
      runDate: asString(point.run_date),
      rankPosition: asNum(point.rank_position),
      compositeScore: asNum(point.composite_score),
    })),
  };
}
