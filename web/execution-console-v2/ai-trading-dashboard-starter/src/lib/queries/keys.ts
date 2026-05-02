/**
 * Centralised react-query cache-key registry.
 *
 * Treat this file as the single source of truth for query identifiers.
 * Hooks under `src/lib/queries/*` use these factories so callers can
 * invalidate related queries without re-typing magic strings.
 *
 * Convention: scoped under a `['execution', <domain>, ...]` tuple so a
 * future blanket invalidation (e.g. on logout) can target everything with
 * `queryKey: ['execution']`.
 */

export const queryKeys = {
  all: ['execution'] as const,
  pipelineWorkspace: () => ['execution', 'pipeline-workspace'] as const,
  workspaceSnapshot: (topN: number) =>
    ['execution', 'workspace-snapshot', topN] as const,
  marketBreadth: (limit: number) => ['execution', 'market-breadth', limit] as const,
  ranking: () => ['execution', 'ranking'] as const,
  rankingDetail: (symbol: string, runId: string | null = null) =>
    ['execution', 'ranking-detail', symbol, runId] as const,
  rankingHistory: (symbol: string, limit: number) =>
    ['execution', 'ranking-history', symbol, limit] as const,
  recentRuns: () => ['execution', 'runs'] as const,
  runsList: (limit: number) => ['execution', 'runs-list', limit] as const,
  runDetail: (runId: string) => ['execution', 'run-detail', runId] as const,
  runDq: (runId: string) => ['execution', 'run-dq', runId] as const,
  runArtifacts: (runId: string) => ['execution', 'run-artifacts', runId] as const,
  stockDetail: (symbol: string) => ['execution', 'stock-detail', symbol] as const,
  stockOhlcv: (symbol: string, limit: number) =>
    ['execution', 'stock-ohlcv', symbol, limit] as const,
  patterns: () => ['execution', 'patterns'] as const,
  sectors: () => ['execution', 'sectors'] as const,
  shadow: () => ['execution', 'shadow'] as const,
} as const;

export type QueryKeyFactories = typeof queryKeys;
