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
  ranking: () => ['execution', 'ranking'] as const,
  recentRuns: () => ['execution', 'runs'] as const,
  patterns: () => ['execution', 'patterns'] as const,
  sectors: () => ['execution', 'sectors'] as const,
  shadow: () => ['execution', 'shadow'] as const,
} as const;

export type QueryKeyFactories = typeof queryKeys;
