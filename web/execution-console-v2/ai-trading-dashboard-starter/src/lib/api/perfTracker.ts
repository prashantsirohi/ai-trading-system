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

export interface BucketCoverageRow {
  bucket: string;
  first_date: string | null;
  last_date: string | null;
  rows: number;
  dates: number;
  symbols_count?: number;
  pct_of_all_rows?: number | null;
  pct_with_fwd_5d?: number | null;
  pct_with_fwd_20d?: number | null;
}

export interface SameDateBucketRow extends BucketRow {
  control_avg_5d: number | null;
  control_avg_10d: number | null;
  control_avg_20d: number | null;
  excess_5d: number | null;
  excess_10d: number | null;
  excess_20d: number | null;
  trading_days?: number;
  small_sample?: boolean;
}

export interface BucketCompositionRow {
  bucket: string;
  n: number;
  avg_rank_position: number | null;
  [key: `avg_${string}`]: number | null | string;
}

export interface PerfBucketCompositionResponse {
  available_columns: string[];
  missing_columns: string[];
  composition: BucketCompositionRow[];
}

export interface BucketDailyRow {
  run_date: string | null;
  bucket: string;
  n: number;
  avg_5d: number | null;
  hitrate_5d: number | null;
}

export interface PerfBucketDailyResponse {
  lookback_days: number;
  rows: BucketDailyRow[];
}

export interface ConcentrationCohortRow {
  cohort: string;
  n: number;
  avg_5d: number | null;
  avg_10d: number | null;
  avg_20d: number | null;
  hitrate_20d: number | null;
  delta_vs_top_200: number | null;
  delta_vs_201_plus: number | null;
}

export type ConcentrationSignal = 'weak' | 'mixed' | 'strong' | 'unknown';

export interface PerfConcentrationResponse {
  lookback_days: number;
  cohorts: ConcentrationCohortRow[];
  signal: ConcentrationSignal;
  message: string;
  top10_avg_20d: number | null;
  top200_avg_20d: number | null;
  top200_minus_201_plus_avg_20d: number | null;
}

export interface DigestListEntry {
  filename: string;
  mtime: number;
  size_bytes: number;
}

export interface PerfDigestListResponse {
  digests: DigestListEntry[];
}

export interface PerfDigestDocResponse {
  filename: string;
  markdown: string;
}

export interface FactorIcRow {
  factor: string;
  [key: `ic_${number}d`]: number | null;
  [key: `n_${number}d`]: number;
}

export interface ConditionalFactorIcRow {
  factor: string;
  [key: string]: string | number | null;
}

export type FactorCoverageStatus = 'not_wired' | 'poor_coverage' | 'partial_coverage' | 'ok';

export interface FactorCoverageRow {
  factor: string;
  non_null_count: number;
  null_pct: number | null;
  first_available_date: string | null;
  last_available_date: string | null;
  total_rows?: number;
  non_null_rows?: number;
  null_rows?: number;
  coverage_pct?: number | null;
  status?: FactorCoverageStatus;
}

export type DriftStatus =
  | 'insufficient_sample'
  | 'no_baseline'
  | 'ok'
  | 'watch'
  | 'warning'
  | 'critical'
  | 'unreliable_coverage';

export interface DriftRow {
  factor: string;
  ic_recent: number | null;
  ic_baseline: number | null;
  recent_n: number;
  baseline_n: number;
  delta_ic: number | null;
  delta_pct: number | null;
  status: DriftStatus;
  alert: boolean;
  coverage_pct?: number | null;
  coverage_status?: FactorCoverageStatus;
}

export interface PerfCohortsResponse {
  lookback_days: number;
  cohorts: CohortRow[];
}

export interface PerfBucketsResponse {
  lookback_days: number;
  buckets: BucketRow[];
}

export interface PerfBucketCoverageResponse {
  buckets: BucketCoverageRow[];
}

export interface PerfSameDateBucketsResponse {
  lookback_days: number;
  control: {
    n: number;
    n_5d: number;
    n_20d: number;
    avg_5d: number | null;
    avg_10d: number | null;
    avg_20d: number | null;
  };
  buckets: SameDateBucketRow[];
}

export interface PerfFactorIcResponse {
  windows: number[];
  factors: FactorIcRow[];
}

export interface PerfConditionalFactorIcResponse {
  windows: number[];
  cohorts: string[];
  factors: ConditionalFactorIcRow[];
}

export interface PerfFactorCoverageResponse {
  rows: number;
  factors: FactorCoverageRow[];
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

export function getPerfBucketCoverage(): Promise<PerfBucketCoverageResponse> {
  return fetchDashboardJson(
    '/api/execution/perf-tracker/bucket-coverage',
    { buckets: [] },
  );
}

export function getPerfSameDateBuckets(
  lookbackDays: number,
): Promise<PerfSameDateBucketsResponse> {
  return fetchDashboardJson(
    `/api/execution/perf-tracker/buckets/same-date?lookback_days=${lookbackDays}`,
    {
      lookback_days: lookbackDays,
      control: { n: 0, n_5d: 0, n_20d: 0, avg_5d: null, avg_10d: null, avg_20d: null },
      buckets: [],
    },
  );
}

export function getPerfFactorIc(windows: number[] = [30, 90, 180]): Promise<PerfFactorIcResponse> {
  const qs = windows.join(',');
  return fetchDashboardJson(
    `/api/execution/perf-tracker/factor-ic?windows=${qs}`,
    { windows, factors: [] },
  );
}

export function getPerfConditionalFactorIc(
  windows: number[] = [30, 90, 180],
): Promise<PerfConditionalFactorIcResponse> {
  const qs = windows.join(',');
  return fetchDashboardJson(
    `/api/execution/perf-tracker/factor-ic/conditional?windows=${qs}`,
    { windows, cohorts: [], factors: [] },
  );
}

export function getPerfFactorCoverage(): Promise<PerfFactorCoverageResponse> {
  return fetchDashboardJson(
    '/api/execution/perf-tracker/factor-coverage',
    { rows: 0, factors: [] },
  );
}

export function getPerfDrift(): Promise<PerfDriftResponse> {
  return fetchDashboardJson(
    '/api/execution/perf-tracker/drift',
    { recent_window: 30, baseline_window: 180, threshold_pct: 30, factors: [], flagged: [] },
  );
}

export function getPerfBucketComposition(): Promise<PerfBucketCompositionResponse> {
  return fetchDashboardJson(
    '/api/execution/perf-tracker/buckets/composition',
    { available_columns: [], missing_columns: [], composition: [] },
  );
}

export function getPerfBucketDaily(lookbackDays: number): Promise<PerfBucketDailyResponse> {
  return fetchDashboardJson(
    `/api/execution/perf-tracker/buckets/daily?lookback_days=${lookbackDays}`,
    { lookback_days: lookbackDays, rows: [] },
  );
}

export function getPerfConcentration(lookbackDays: number): Promise<PerfConcentrationResponse> {
  return fetchDashboardJson(
    `/api/execution/perf-tracker/concentration?lookback_days=${lookbackDays}`,
    {
      lookback_days: lookbackDays,
      cohorts: [],
      signal: 'unknown',
      message: '',
      top10_avg_20d: null,
      top200_avg_20d: null,
      top200_minus_201_plus_avg_20d: null,
    },
  );
}

export function getPerfDigestList(): Promise<PerfDigestListResponse> {
  return fetchDashboardJson(
    '/api/execution/perf-tracker/digests',
    { digests: [] },
  );
}

export function getPerfDigestDoc(filename: string): Promise<PerfDigestDocResponse> {
  return fetchDashboardJson(
    `/api/execution/perf-tracker/digests/${encodeURIComponent(filename)}`,
    { filename, markdown: '' },
  );
}
