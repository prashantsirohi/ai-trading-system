/**
 * Client for the performance tracker endpoints (Phase 0 feedback loop).
 *
 * Reads ``rank_cohort_performance`` via the FastAPI router at
 * /api/execution/perf-tracker/*. All endpoints are read-only.
 */

import { fetchDashboardJson } from '@/lib/api/client';

export interface PerfCoverage {
  first_date: string | null;
  last_date: string | null;
  dates: number;
  rows: number;
}

export interface CohortRow {
  cohort: string;
  n_total: number;
  n_5d: number;
  n_20d: number;
  avg_5d: number | null;
  avg_10d: number | null;
  avg_20d: number | null;
  avg_60d: number | null;
  hitrate_5d: number | null;
  hitrate_20d: number | null;
}

export interface BucketRow {
  bucket: string;
  n: number;
  n_5d: number;
  n_20d: number;
  avg_5d: number | null;
  avg_10d: number | null;
  avg_20d: number | null;
  hitrate_5d: number | null;
  hitrate_20d: number | null;
}

export interface FactorIcRow {
  factor: string;
  [key: `ic_${number}d`]: number | null;
  [key: `n_${number}d`]: number;
}

export interface DriftRow {
  factor: string;
  ic_recent: number | null;
  ic_baseline: number | null;
  delta_pct: number | null;
  alert: boolean;
}

export interface PerfCohortsResponse {
  lookback_days: number;
  cohorts: CohortRow[];
}

export interface PerfBucketsResponse {
  lookback_days: number;
  buckets: BucketRow[];
}

export interface PerfFactorIcResponse {
  windows: number[];
  factors: FactorIcRow[];
}

export interface PerfDriftResponse {
  recent_window: number;
  baseline_window: number;
  threshold_pct: number;
  factors: DriftRow[];
  flagged: DriftRow[];
}

const EMPTY_COVERAGE: PerfCoverage = {
  first_date: null, last_date: null, dates: 0, rows: 0,
};

export function getPerfCoverage(): Promise<PerfCoverage> {
  return fetchDashboardJson('/api/execution/perf-tracker/coverage', EMPTY_COVERAGE);
}

export function getPerfCohorts(lookbackDays: number): Promise<PerfCohortsResponse> {
  return fetchDashboardJson(
    `/api/execution/perf-tracker/cohorts?lookback_days=${lookbackDays}`,
    { lookback_days: lookbackDays, cohorts: [] },
  );
}

export function getPerfBuckets(lookbackDays: number): Promise<PerfBucketsResponse> {
  return fetchDashboardJson(
    `/api/execution/perf-tracker/buckets?lookback_days=${lookbackDays}`,
    { lookback_days: lookbackDays, buckets: [] },
  );
}

export function getPerfFactorIc(windows: number[] = [30, 90, 180]): Promise<PerfFactorIcResponse> {
  const qs = windows.join(',');
  return fetchDashboardJson(
    `/api/execution/perf-tracker/factor-ic?windows=${qs}`,
    { windows, factors: [] },
  );
}

export function getPerfDrift(): Promise<PerfDriftResponse> {
  return fetchDashboardJson(
    '/api/execution/perf-tracker/drift',
    { recent_window: 30, baseline_window: 180, threshold_pct: 30, factors: [], flagged: [] },
  );
}
