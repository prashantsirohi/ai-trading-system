/**
 * Typed react-query hooks for the execution console.
 *
 * Each hook wraps a fetcher in `src/lib/api/*` with a stable cache key
 * and a sensible refetch policy. Components should import from this
 * module instead of calling `useQuery` directly so cache keys stay
 * consistent across the codebase.
 */

import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { UseQueryOptions, UseQueryResult } from '@tanstack/react-query';

import { DEFAULT_REFETCH_INTERVAL_MS } from '@/lib/api/client';
import { getPatterns } from '@/lib/api/patterns';
import { getMarketBreadth, type MarketBreadthPoint } from '@/lib/api/breadth';
import { getPipelineWorkspace } from '@/lib/api/pipeline';
import {
  getRanking,
  getRankingDetail,
  getRankingHistory,
  type RankingDetail,
  type RankingHistory,
} from '@/lib/api/ranking';
import {
  getRuns,
  getRunsList,
  getRunDetail,
  getRunDqResults,
  getRunArtifacts,
  type RunsListResponse,
  type RunDetail,
  type DqResults,
  type RunArtifacts,
} from '@/lib/api/runs';
import { getSectors } from '@/lib/api/sectors';
import { getShadow } from '@/lib/api/shadow';
import {
  getStockDetail,
  getStockOhlcv,
  type StockDetail,
  type StockOhlcv,
} from '@/lib/api/stocks';
import { getWorkspaceSnapshot } from '@/lib/api/workspace';
import type { WorkspaceSnapshot } from '@/lib/api/workspace';
import type {
  PatternResponse,
  PipelineWorkspaceResponse,
  RankingResponse,
  RunsResponse,
  SectorResponse,
  ShadowResponse,
} from '@/types/api';

import { queryKeys } from './keys';

/** Options that callers may override; the cache key + queryFn are fixed. */
type QueryOverrides<TData> = Omit<
  UseQueryOptions<TData, Error, TData>,
  'queryKey' | 'queryFn'
>;

const LIVE_QUERY_DEFAULTS = {
  refetchInterval: DEFAULT_REFETCH_INTERVAL_MS || false,
} as const;

export function usePipelineWorkspace(
  options: QueryOverrides<PipelineWorkspaceResponse> = {},
): UseQueryResult<PipelineWorkspaceResponse, Error> {
  return useQuery<PipelineWorkspaceResponse, Error>({
    queryKey: queryKeys.pipelineWorkspace(),
    queryFn: getPipelineWorkspace,
    ...LIVE_QUERY_DEFAULTS,
    ...options,
  });
}

export function useWorkspaceSnapshot(
  topN: number = 3,
  options: QueryOverrides<WorkspaceSnapshot> = {},
): UseQueryResult<WorkspaceSnapshot, Error> {
  return useQuery<WorkspaceSnapshot, Error>({
    queryKey: queryKeys.workspaceSnapshot(topN),
    queryFn: () => getWorkspaceSnapshot(topN),
    ...LIVE_QUERY_DEFAULTS,
    ...options,
  });
}

export function useMarketBreadth(
  limit = 0,
  options: QueryOverrides<MarketBreadthPoint[]> = {},
): UseQueryResult<MarketBreadthPoint[], Error> {
  return useQuery<MarketBreadthPoint[], Error>({
    queryKey: queryKeys.marketBreadth(limit),
    queryFn: () => getMarketBreadth(limit),
    ...LIVE_QUERY_DEFAULTS,
    ...options,
  });
}

export function useRanking(
  options: QueryOverrides<RankingResponse> = {},
): UseQueryResult<RankingResponse, Error> {
  return useQuery<RankingResponse, Error>({
    queryKey: queryKeys.ranking(),
    queryFn: getRanking,
    ...LIVE_QUERY_DEFAULTS,
    ...options,
  });
}

export function useRankingDetail(
  symbol: string | null | undefined,
  runId: string | null = null,
  options: QueryOverrides<RankingDetail> = {},
): UseQueryResult<RankingDetail, Error> {
  const enabled = Boolean(symbol);
  return useQuery<RankingDetail, Error>({
    queryKey: queryKeys.rankingDetail(symbol ?? '__none__', runId),
    queryFn: () => getRankingDetail(symbol as string, runId),
    enabled,
    ...options,
  });
}

export function useRankingHistory(
  symbol: string | null | undefined,
  limit = 20,
  options: QueryOverrides<RankingHistory> = {},
): UseQueryResult<RankingHistory, Error> {
  const enabled = Boolean(symbol);
  return useQuery<RankingHistory, Error>({
    queryKey: queryKeys.rankingHistory(symbol ?? '__none__', limit),
    queryFn: () => getRankingHistory(symbol as string, limit),
    enabled,
    ...options,
  });
}

export function useRecentRuns(
  options: QueryOverrides<RunsResponse> = {},
): UseQueryResult<RunsResponse, Error> {
  return useQuery<RunsResponse, Error>({
    queryKey: queryKeys.recentRuns(),
    queryFn: getRuns,
    ...options,
  });
}

export function useRunsList(
  limit = 25,
  options: QueryOverrides<RunsListResponse> = {},
): UseQueryResult<RunsListResponse, Error> {
  return useQuery<RunsListResponse, Error>({
    queryKey: queryKeys.runsList(limit),
    queryFn: () => getRunsList(limit),
    ...LIVE_QUERY_DEFAULTS,
    ...options,
  });
}

export function useRunDetail(
  runId: string | null | undefined,
  options: QueryOverrides<RunDetail> = {},
): UseQueryResult<RunDetail, Error> {
  const enabled = Boolean(runId);
  return useQuery<RunDetail, Error>({
    queryKey: queryKeys.runDetail(runId ?? '__none__'),
    queryFn: () => getRunDetail(runId as string),
    enabled,
    ...options,
  });
}

export function useRunDqResults(
  runId: string | null | undefined,
  options: QueryOverrides<DqResults> = {},
): UseQueryResult<DqResults, Error> {
  const enabled = Boolean(runId);
  return useQuery<DqResults, Error>({
    queryKey: queryKeys.runDq(runId ?? '__none__'),
    queryFn: () => getRunDqResults(runId as string),
    enabled,
    ...options,
  });
}

export function useRunArtifacts(
  runId: string | null | undefined,
  options: QueryOverrides<RunArtifacts> = {},
): UseQueryResult<RunArtifacts, Error> {
  const enabled = Boolean(runId);
  return useQuery<RunArtifacts, Error>({
    queryKey: queryKeys.runArtifacts(runId ?? '__none__'),
    queryFn: () => getRunArtifacts(runId as string),
    enabled,
    ...options,
  });
}

export function usePatterns(
  options: QueryOverrides<PatternResponse> = {},
): UseQueryResult<PatternResponse, Error> {
  return useQuery<PatternResponse, Error>({
    queryKey: queryKeys.patterns(),
    queryFn: getPatterns,
    ...options,
  });
}

export function useSectors(
  options: QueryOverrides<SectorResponse> = {},
): UseQueryResult<SectorResponse, Error> {
  return useQuery<SectorResponse, Error>({
    queryKey: queryKeys.sectors(),
    queryFn: getSectors,
    ...options,
  });
}

export function useStockDetail(
  symbol: string | null | undefined,
  options: QueryOverrides<StockDetail> = {},
): UseQueryResult<StockDetail, Error> {
  const enabled = Boolean(symbol);
  return useQuery<StockDetail, Error>({
    queryKey: queryKeys.stockDetail(symbol ?? '__none__'),
    queryFn: () => getStockDetail(symbol as string),
    enabled,
    ...options,
  });
}

export function useStockOhlcv(
  symbol: string | null | undefined,
  limit = 180,
  options: QueryOverrides<StockOhlcv> = {},
): UseQueryResult<StockOhlcv, Error> {
  const enabled = Boolean(symbol);
  return useQuery<StockOhlcv, Error>({
    queryKey: queryKeys.stockOhlcv(symbol ?? '__none__', limit),
    queryFn: () => getStockOhlcv(symbol as string, limit),
    enabled,
    ...options,
  });
}

export function useShadow(
  options: QueryOverrides<ShadowResponse> = {},
): UseQueryResult<ShadowResponse, Error> {
  return useQuery<ShadowResponse, Error>({
    queryKey: queryKeys.shadow(),
    queryFn: getShadow,
    ...options,
  });
}

/**
 * Returns a `refresh()` function that invalidates every execution-domain
 * query in the cache. Use for the global "Refresh" button in the top bar.
 */
export function useRefreshAll(): () => Promise<void> {
  const queryClient = useQueryClient();
  return async () => {
    await queryClient.invalidateQueries({ queryKey: queryKeys.all });
  };
}

export { queryKeys };
