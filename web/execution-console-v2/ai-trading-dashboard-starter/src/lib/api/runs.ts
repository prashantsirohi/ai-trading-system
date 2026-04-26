/**
 * Fetchers for the run-audit endpoints (PR #11):
 *
 *   * ``GET /api/execution/runs?limit=`` — recent run history.
 *   * ``GET /api/execution/runs/{run_id}`` — full detail (run, stages, alerts, delivery_logs).
 *   * ``GET /api/execution/runs/{run_id}/dq`` — DQ rule results with severity aggregates.
 *   * ``GET /api/execution/runs/{run_id}/artifacts`` — per-stage artifact registry.
 *
 * Backend payloads are snake_case ``dict[str, Any]``; we map them into stable
 * camelCase shapes the React components consume. Each response also goes
 * through a fallback when ``VITE_USE_MOCK_API=true`` so FE-only development
 * still works.
 */
import type { RunsResponse } from '@/types/api';
import { runsMock } from '@/lib/mock/runs';
import { fetchDashboardJson, fetchDashboardJsonStrict } from '@/lib/api/client';

// ---------------------------------------------------------------------------
// /runs (list)
// ---------------------------------------------------------------------------

export interface RunSummary {
  runId: string;
  runDate: string | null;
  status: string;
  currentStage: string | null;
  startedAt: string | null;
  endedAt: string | null;
  errorClass: string | null;
  errorMessage: string | null;
  durationLabel: string;
}

export interface RunsListResponse {
  runs: RunSummary[];
}

interface BackendRunSummary {
  run_id?: string;
  run_date?: string | null;
  status?: string | null;
  current_stage?: string | null;
  started_at?: string | null;
  ended_at?: string | null;
  error_class?: string | null;
  error_message?: string | null;
}

interface BackendRunsListResponse {
  runs?: BackendRunSummary[];
}

function durationLabel(startedAt: string | null, endedAt: string | null): string {
  if (!startedAt) return '—';
  const start = new Date(startedAt).getTime();
  const end = endedAt ? new Date(endedAt).getTime() : Date.now();
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return '—';
  const ms = end - start;
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  const remSec = sec % 60;
  if (min < 60) return remSec ? `${min}m ${remSec}s` : `${min}m`;
  const hr = Math.floor(min / 60);
  const remMin = min % 60;
  return remMin ? `${hr}h ${remMin}m` : `${hr}h`;
}

function mapRunSummary(raw: BackendRunSummary): RunSummary {
  return {
    runId: raw.run_id ?? 'unknown',
    runDate: raw.run_date ?? null,
    status: (raw.status ?? 'unknown').toString(),
    currentStage: raw.current_stage ?? null,
    startedAt: raw.started_at ?? null,
    endedAt: raw.ended_at ?? null,
    errorClass: raw.error_class ?? null,
    errorMessage: raw.error_message ?? null,
    durationLabel: durationLabel(raw.started_at ?? null, raw.ended_at ?? null),
  };
}

export async function getRunsList(limit = 25): Promise<RunsListResponse> {
  const raw = await fetchDashboardJsonStrict<BackendRunsListResponse>(
    `/api/execution/runs?limit=${limit}`,
    { runs: [] },
  );
  return { runs: (raw.runs ?? []).map(mapRunSummary) };
}

// ---------------------------------------------------------------------------
// /runs/{id} (detail)
// ---------------------------------------------------------------------------

export interface RunStageDetail {
  stageName: string;
  status: string;
  attemptNumber: number | null;
  startedAt: string | null;
  endedAt: string | null;
  durationLabel: string;
  warnings: string[];
  errorMessage: string | null;
  rowCount: number | null;
}

export interface RunAlert {
  alertId: string;
  severity: string;
  channel: string | null;
  message: string;
  createdAt: string | null;
}

export interface RunDeliveryLog {
  logId: string;
  channel: string;
  status: string;
  message: string | null;
  createdAt: string | null;
}

export interface RunDetail {
  run: RunSummary | null;
  stages: RunStageDetail[];
  alerts: RunAlert[];
  deliveryLogs: RunDeliveryLog[];
}

interface BackendRunStage {
  stage_name?: string | null;
  status?: string | null;
  attempt_number?: number | null;
  started_at?: string | null;
  ended_at?: string | null;
  warnings?: unknown;
  error_message?: string | null;
  row_count?: number | null;
}

interface BackendRunAlert {
  alert_id?: string;
  severity?: string | null;
  channel?: string | null;
  message?: string | null;
  created_at?: string | null;
}

interface BackendRunDeliveryLog {
  log_id?: string;
  channel?: string | null;
  status?: string | null;
  message?: string | null;
  created_at?: string | null;
}

interface BackendRunDetail {
  run?: BackendRunSummary | null;
  stages?: BackendRunStage[] | null;
  alerts?: BackendRunAlert[] | null;
  delivery_logs?: BackendRunDeliveryLog[] | null;
}

function asStringArray(value: unknown): string[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.map((v) => String(v)).filter((v) => v.trim() !== '');
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) return parsed.map((v) => String(v));
    } catch {
      // fall through
    }
    return [trimmed];
  }
  return [];
}

function mapRunStage(raw: BackendRunStage): RunStageDetail {
  return {
    stageName: raw.stage_name ?? 'unknown',
    status: (raw.status ?? 'unknown').toString(),
    attemptNumber:
      raw.attempt_number === null || raw.attempt_number === undefined
        ? null
        : Number(raw.attempt_number),
    startedAt: raw.started_at ?? null,
    endedAt: raw.ended_at ?? null,
    durationLabel: durationLabel(raw.started_at ?? null, raw.ended_at ?? null),
    warnings: asStringArray(raw.warnings),
    errorMessage: raw.error_message ?? null,
    rowCount:
      raw.row_count === null || raw.row_count === undefined ? null : Number(raw.row_count),
  };
}

function mapRunAlert(raw: BackendRunAlert): RunAlert {
  return {
    alertId: raw.alert_id ?? '',
    severity: (raw.severity ?? 'info').toString(),
    channel: raw.channel ?? null,
    message: raw.message ?? '',
    createdAt: raw.created_at ?? null,
  };
}

function mapRunDeliveryLog(raw: BackendRunDeliveryLog): RunDeliveryLog {
  return {
    logId: raw.log_id ?? '',
    channel: raw.channel ?? 'unknown',
    status: (raw.status ?? 'unknown').toString(),
    message: raw.message ?? null,
    createdAt: raw.created_at ?? null,
  };
}

export async function getRunDetail(runId: string): Promise<RunDetail> {
  const raw = await fetchDashboardJsonStrict<BackendRunDetail>(
    `/api/execution/runs/${encodeURIComponent(runId)}`,
    { run: null, stages: [], alerts: [], delivery_logs: [] },
  );
  return {
    run: raw.run ? mapRunSummary(raw.run) : null,
    stages: (raw.stages ?? []).map(mapRunStage),
    alerts: (raw.alerts ?? []).map(mapRunAlert),
    deliveryLogs: (raw.delivery_logs ?? []).map(mapRunDeliveryLog),
  };
}

// ---------------------------------------------------------------------------
// /runs/{id}/dq
// ---------------------------------------------------------------------------

export interface DqResult {
  resultId: string;
  runId: string;
  stageName: string;
  ruleId: string;
  severity: string;
  status: string;
  failedCount: number;
  message: string | null;
  sampleUri: string | null;
  createdAt: string | null;
}

export interface DqSeverityCounts {
  failed: number;
  passed: number;
}

export interface DqResults {
  available: boolean;
  runId: string;
  results: DqResult[];
  totalFailed: number;
  totalPassed: number;
  countsBySeverity: Record<string, DqSeverityCounts>;
}

interface BackendDqResults {
  available?: boolean;
  run_id?: string;
  results?: Array<{
    result_id?: string;
    run_id?: string;
    stage_name?: string | null;
    rule_id?: string | null;
    severity?: string | null;
    status?: string | null;
    failed_count?: number | null;
    message?: string | null;
    sample_uri?: string | null;
    created_at?: string | null;
  }>;
  summary?: {
    total?: number;
    total_failed?: number;
    total_passed?: number;
    counts_by_severity?: Record<string, { failed?: number; passed?: number }>;
  };
}

export async function getRunDqResults(runId: string): Promise<DqResults> {
  const raw = await fetchDashboardJsonStrict<BackendDqResults>(
    `/api/execution/runs/${encodeURIComponent(runId)}/dq`,
    { available: false, run_id: runId, results: [] },
  );
  const counts: Record<string, DqSeverityCounts> = {};
  for (const [sev, c] of Object.entries(raw.summary?.counts_by_severity ?? {})) {
    counts[sev] = { failed: Number(c?.failed ?? 0), passed: Number(c?.passed ?? 0) };
  }
  return {
    available: Boolean(raw.available),
    runId: raw.run_id ?? runId,
    results: (raw.results ?? []).map((r) => ({
      resultId: r.result_id ?? '',
      runId: r.run_id ?? runId,
      stageName: r.stage_name ?? 'unknown',
      ruleId: r.rule_id ?? 'unknown',
      severity: (r.severity ?? 'info').toString(),
      status: (r.status ?? 'unknown').toString(),
      failedCount: Number(r.failed_count ?? 0),
      message: r.message ?? null,
      sampleUri: r.sample_uri ?? null,
      createdAt: r.created_at ?? null,
    })),
    totalFailed: Number(raw.summary?.total_failed ?? 0),
    totalPassed: Number(raw.summary?.total_passed ?? 0),
    countsBySeverity: counts,
  };
}

// ---------------------------------------------------------------------------
// /runs/{id}/artifacts
// ---------------------------------------------------------------------------

export interface ArtifactRecord {
  artifactId: string;
  runId: string;
  stageName: string;
  attemptNumber: number;
  artifactType: string;
  uri: string;
  name: string;
  contentHash: string | null;
  rowCount: number | null;
  createdAt: string | null;
  downloadUrl: string;
}

export interface RunArtifacts {
  available: boolean;
  runId: string;
  artifacts: ArtifactRecord[];
  countsByStage: Record<string, number>;
  total: number;
}

interface BackendRunArtifacts {
  available?: boolean;
  run_id?: string;
  artifacts?: Array<{
    artifact_id?: string;
    run_id?: string;
    stage_name?: string | null;
    attempt_number?: number | null;
    artifact_type?: string | null;
    uri?: string | null;
    name?: string | null;
    content_hash?: string | null;
    row_count?: number | null;
    created_at?: string | null;
    download_url?: string | null;
  }>;
  counts_by_stage?: Record<string, number>;
  total?: number;
}

export async function getRunArtifacts(runId: string): Promise<RunArtifacts> {
  const raw = await fetchDashboardJsonStrict<BackendRunArtifacts>(
    `/api/execution/runs/${encodeURIComponent(runId)}/artifacts`,
    { available: false, run_id: runId, artifacts: [] },
  );
  return {
    available: Boolean(raw.available),
    runId: raw.run_id ?? runId,
    artifacts: (raw.artifacts ?? []).map((a) => ({
      artifactId: a.artifact_id ?? '',
      runId: a.run_id ?? runId,
      stageName: a.stage_name ?? 'unknown',
      attemptNumber: Number(a.attempt_number ?? 0),
      artifactType: a.artifact_type ?? 'unknown',
      uri: a.uri ?? '',
      name: a.name ?? '',
      contentHash: a.content_hash ?? null,
      rowCount:
        a.row_count === null || a.row_count === undefined ? null : Number(a.row_count),
      createdAt: a.created_at ?? null,
      downloadUrl: a.download_url ?? '',
    })),
    countsByStage: raw.counts_by_stage ?? {},
    total: Number(raw.total ?? raw.artifacts?.length ?? 0),
  };
}

// ---------------------------------------------------------------------------
// Legacy Pipeline-stage-flow (kept for back-compat with existing RunsPage usage)
// ---------------------------------------------------------------------------

export async function getRuns(): Promise<RunsResponse> {
  try {
    const res = await fetchDashboardJson<{ runs: BackendRunSummary[] }>(
      '/api/execution/runs?limit=20',
      { runs: [] },
    );
    if (res.runs?.length) {
      return {
        stages: res.runs.map((r) => ({
          id: r.run_id ?? 'unknown',
          status: r.status ?? 'unknown',
          stage: r.current_stage ?? 'N/A',
        })),
      } as unknown as RunsResponse;
    }
  } catch (e) {
    console.warn('Runs API failed, using mock', e);
  }
  return runsMock;
}
