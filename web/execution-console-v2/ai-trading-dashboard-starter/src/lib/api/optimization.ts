/**
 * Client for the optimization endpoints under /api/execution/optimization/*.
 *
 * Backed by the Wave 2 readmodel + Wave 5b POST /promote endpoint. Shapes
 * mirror the Pydantic response models in
 * `src/ai_trading_system/ui/execution_api/schemas/optimization.py`.
 *
 * Every fetch falls back to an empty payload in mock mode so the page can
 * render in offline dev.
 */

import {
  fetchDashboardJson,
  fetchDashboardJsonStrict,
  postDashboardJson,
} from '@/lib/api/client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface OptimizationRunListItem {
  optimization_run_id: string;
  recipe_name: string;
  strategy_id: string;
  status: string; // pending | running | completed | failed | cancelled
  from_date: string;
  to_date: string;
  seed: number;
  max_trials: number;
  started_at: string;
  completed_at: string | null;
  champion_rule_pack_id: string | null;
  error: string | null;
  trial_count: number;
}

export interface OptimizationRunsResponse {
  available: boolean;
  runs: OptimizationRunListItem[];
}

export interface FoldMetrics {
  fold_index: number;
  fitness: number | null;
  cagr: number | null;
  sharpe: number | null;
  max_drawdown_pct: number | null;
  win_rate: number | null;
  trade_count: number | null;
  total_return_pct: number | null;
  benchmark_return_pct: number | null;
}

export interface OptimizationRunDetail {
  available: boolean;
  optimization_run_id: string;
  recipe_name: string;
  strategy_id: string;
  status: string;
  from_date: string;
  to_date: string;
  seed: number;
  max_trials: number;
  started_at: string;
  completed_at: string | null;
  error: string | null;
  baseline_rule_pack_id: string;
  baseline_folds: FoldMetrics[];
  champion_rule_pack_id: string | null;
  champion_folds: FoldMetrics[];
  champion_lifecycle_status: string | null;
  trial_count: number;
  report_path: string | null;
  report_exists: boolean;
}

export interface OptimizationTrial {
  iteration: number;
  rule_pack_id: string;
  fitness: number | null;
  cagr: number | null;
  sharpe: number | null;
  max_drawdown_pct: number | null;
  win_rate: number | null;
  trade_count: number | null;
  total_return_pct: number | null;
  accepted: boolean | null;
  rejection_reason: string | null;
  created_at: string | null;
}

export interface OptimizationTrialsResponse {
  available: boolean;
  optimization_run_id: string;
  trials: OptimizationTrial[];
}

export interface LeaderboardRow {
  recipe_name: string;
  strategy_id: string;
  optimization_run_id: string;
  champion_rule_pack_id: string;
  champion_lifecycle_status: string;
  fitness: number | null;
  cagr: number | null;
  sharpe: number | null;
  max_drawdown_pct: number | null;
  win_rate: number | null;
  trade_count: number | null;
  total_return_pct: number | null;
  completed_at: string | null;
}

export interface LeaderboardResponse {
  available: boolean;
  metric: string;
  rows: LeaderboardRow[];
}

export interface ReportContent {
  optimization_run_id: string;
  recipe_name: string;
  report_path: string;
  content: string;
}

export interface PromoteResult {
  optimization_run_id: string;
  rule_pack_id: string;
  previous_status: string;
  new_status: string;
}

// Lifecycle ladder (mirrors promote.py::LIFECYCLE_ORDER).
export const LIFECYCLE_STATUSES = [
  'draft',
  'backtested',
  'walkforward_passed',
  'shadow',
  'paper_approved',
  'production_candidate',
  'active',
] as const;
export type LifecycleStatus = (typeof LIFECYCLE_STATUSES)[number];

// ---------------------------------------------------------------------------
// Fetchers
// ---------------------------------------------------------------------------

export async function getOptimizationRuns(params: {
  recipe?: string;
  status?: string;
  limit?: number;
} = {}): Promise<OptimizationRunsResponse> {
  const qs = new URLSearchParams();
  if (params.recipe) qs.set('recipe', params.recipe);
  if (params.status) qs.set('status', params.status);
  qs.set('limit', String(params.limit ?? 50));
  return fetchDashboardJson<OptimizationRunsResponse>(
    `/api/execution/optimization/runs?${qs.toString()}`,
    { available: false, runs: [] },
  );
}

export async function getOptimizationRunDetail(
  runId: string,
): Promise<OptimizationRunDetail | null> {
  // 404s should bubble as null so the page can show "not found" instead of
  // crashing on a stale URL bookmark.
  try {
    return await fetchDashboardJsonStrict<OptimizationRunDetail>(
      `/api/execution/optimization/runs/${encodeURIComponent(runId)}`,
      {
        available: false,
        optimization_run_id: runId,
        recipe_name: '',
        strategy_id: '',
        status: '',
        from_date: '',
        to_date: '',
        seed: 0,
        max_trials: 0,
        started_at: '',
        completed_at: null,
        error: null,
        baseline_rule_pack_id: '',
        baseline_folds: [],
        champion_rule_pack_id: null,
        champion_folds: [],
        champion_lifecycle_status: null,
        trial_count: 0,
        report_path: null,
        report_exists: false,
      },
    );
  } catch (err) {
    if (err instanceof Error && err.message.includes('(404)')) return null;
    throw err;
  }
}

export async function getOptimizationTrials(
  runId: string,
  params: { limit?: number; sort?: string } = {},
): Promise<OptimizationTrialsResponse> {
  const qs = new URLSearchParams();
  qs.set('limit', String(params.limit ?? 200));
  qs.set('sort', params.sort ?? 'iteration');
  return fetchDashboardJson<OptimizationTrialsResponse>(
    `/api/execution/optimization/runs/${encodeURIComponent(runId)}/trials?${qs.toString()}`,
    { available: false, optimization_run_id: runId, trials: [] },
  );
}

export async function getOptimizationLeaderboard(
  params: { metric?: string; top?: number } = {},
): Promise<LeaderboardResponse> {
  const qs = new URLSearchParams();
  qs.set('metric', params.metric ?? 'sharpe');
  qs.set('top', String(params.top ?? 20));
  return fetchDashboardJson<LeaderboardResponse>(
    `/api/execution/optimization/leaderboard?${qs.toString()}`,
    { available: false, metric: params.metric ?? 'sharpe', rows: [] },
  );
}

export async function getOptimizationReport(
  runId: string,
): Promise<ReportContent | null> {
  try {
    return await fetchDashboardJsonStrict<ReportContent>(
      `/api/execution/optimization/runs/${encodeURIComponent(runId)}/report`,
      {
        optimization_run_id: runId,
        recipe_name: '',
        report_path: '',
        content: '',
      },
    );
  } catch (err) {
    if (err instanceof Error && err.message.includes('(404)')) return null;
    throw err;
  }
}

export async function promoteOptimizationRun(
  runId: string,
  to: LifecycleStatus = 'shadow',
): Promise<PromoteResult> {
  return postDashboardJson<PromoteResult>(
    `/api/execution/optimization/runs/${encodeURIComponent(runId)}/promote`,
    { to },
  );
}
