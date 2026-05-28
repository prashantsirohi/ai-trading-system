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
  fundamentalsDashboard: () => ['execution', 'fundamentals-dashboard'] as const,
  shadow: () => ['execution', 'shadow'] as const,
  riskProfiles: () => ['execution', 'risk-profiles'] as const,
  backtestRun: (profile: string, fromDate: string | null, toDate: string | null, equity: number) =>
    ['execution', 'backtest-run', profile, fromDate, toDate, equity] as const,
  perfCoverage: () => ['execution', 'perf-coverage'] as const,
  perfCohorts: (lookbackDays: number) =>
    ['execution', 'perf-cohorts', lookbackDays] as const,
  perfBuckets: (lookbackDays: number) =>
    ['execution', 'perf-buckets', lookbackDays] as const,
  perfBucketCoverage: () => ['execution', 'perf-bucket-coverage'] as const,
  perfSameDateBuckets: (lookbackDays: number) =>
    ['execution', 'perf-same-date-buckets', lookbackDays] as const,
  perfFactorIc: (windows: number[]) =>
    ['execution', 'perf-factor-ic', windows.join(',')] as const,
  perfConditionalFactorIc: (windows: number[]) =>
    ['execution', 'perf-conditional-factor-ic', windows.join(',')] as const,
  perfFactorCoverage: () => ['execution', 'perf-factor-coverage'] as const,
  perfDrift: () => ['execution', 'perf-drift'] as const,
  perfBucketComposition: () => ['execution', 'perf-bucket-composition'] as const,
  perfBucketDaily: (lookbackDays: number) =>
    ['execution', 'perf-bucket-daily', lookbackDays] as const,
  perfConcentration: (lookbackDays: number) =>
    ['execution', 'perf-concentration', lookbackDays] as const,
  perfDigestList: () => ['execution', 'perf-digest-list'] as const,
  perfDigestDoc: (filename: string | null) =>
    ['execution', 'perf-digest-doc', filename ?? ''] as const,
  // --- optimization (Wave 5b) -----------------------------------------
  optimizationRuns: (recipe: string | undefined, status: string | undefined, limit: number) =>
    ['execution', 'optimization-runs', recipe ?? '', status ?? '', limit] as const,
  optimizationRunDetail: (runId: string) =>
    ['execution', 'optimization-run-detail', runId] as const,
  optimizationRunTrials: (runId: string, sort: string, limit: number) =>
    ['execution', 'optimization-run-trials', runId, sort, limit] as const,
  optimizationLeaderboard: (metric: string, top: number) =>
    ['execution', 'optimization-leaderboard', metric, top] as const,
  optimizationReport: (runId: string) =>
    ['execution', 'optimization-report', runId] as const,
} as const;

export type QueryKeyFactories = typeof queryKeys;
