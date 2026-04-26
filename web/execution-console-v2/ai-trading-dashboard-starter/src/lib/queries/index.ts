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
import { getPipelineWorkspace } from '@/lib/api/pipeline';
import {
  getRanking,
  getRankingDetail,
  getRankingHistory,
  type RankingDetail,
  type RankingHistory,
} from '@/lib/api/ranking';
import { getRuns } from '@/lib/api/runs';
import { getSectors } from '@/lib/api/sectors';
import { getShadow } from '@/lib/api/shadow';
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
