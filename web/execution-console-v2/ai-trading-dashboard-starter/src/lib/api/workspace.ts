/**
 * Fetcher for the slim Control Tower workspace endpoint
 * (``GET /api/execution/workspace/snapshot``).
 *
 * The shape mirrors :func:`get_workspace_snapshot_compact` on the backend:
 * a top-N action list, sector leaders, output counts, and the underlying
 * dashboard-payload summary. Heavier ``/workspace/pipeline`` data stays on
 * its own hook — this endpoint is for the landing page.
 */
import type { components } from '@/types/api.gen';
import { fetchDashboardJsonStrict } from '@/lib/api/client';

type WorkspaceSnapshotRaw = components['schemas'] extends Record<string, unknown>
  ? Record<string, unknown>
  : Record<string, unknown>;

export interface WorkspaceTopAction {
  symbol: string;
  compositeScore: number | null;
  sectorName: string | null;
  verdict: string | null;
  confidence: string | null;
}

export interface WorkspaceSectorLeader {
  sector: string;
  rsRankPct: number | null;
  quadrant: string | null;
  raw: Record<string, string | number | boolean | null>;
}

export interface WorkspaceCounts {
  ranked: number;
  breakouts: number;
  patterns: number;
  sectors: number;
}

export interface WorkspaceSummary {
  topSector: string | null;
  breakoutCount: number | null;
  patternCount: number | null;
  dataTrustStatus: string | null;
  raw: Record<string, string | number | boolean | null>;
}

export interface WorkspaceSnapshot {
  available: boolean;
  artifactPath: string | null;
  summary: WorkspaceSummary;
  topActions: WorkspaceTopAction[];
  sectorLeaders: WorkspaceSectorLeader[];
  counts: WorkspaceCounts;
}

/** Raw shape of the JSON returned by ``/workspace/snapshot``. */
interface BackendWorkspaceSnapshot extends WorkspaceSnapshotRaw {
  available?: boolean;
  artifact_path?: string | null;
  summary?: Record<string, string | number | boolean | null> | null;
  top_actions?: Array<{
    symbol?: string;
    composite_score?: number | null;
    sector_name?: string | null;
    verdict?: string | null;
    confidence?: string | null;
  }>;
  sector_leaders?: Array<Record<string, string | number | boolean | null>>;
  counts?: { ranked?: number; breakouts?: number; patterns?: number; sectors?: number };
}

/** Raw-shape fallback used when the API is unreachable in mock mode. */
const RAW_FALLBACK: BackendWorkspaceSnapshot = {
  available: false,
  artifact_path: null,
  summary: {},
  top_actions: [],
  sector_leaders: [],
  counts: { ranked: 0, breakouts: 0, patterns: 0, sectors: 0 },
};

function asNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function asString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const str = String(value).trim();
  return str === '' ? null : str;
}

function mapTopActions(rows: BackendWorkspaceSnapshot['top_actions']): WorkspaceTopAction[] {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => ({
    symbol: asString(row.symbol) ?? '—',
    compositeScore: asNumber(row.composite_score),
    sectorName: asString(row.sector_name),
    verdict: asString(row.verdict),
    confidence: asString(row.confidence),
  }));
}

function mapSectorLeaders(
  rows: BackendWorkspaceSnapshot['sector_leaders'],
): WorkspaceSectorLeader[] {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => {
    // Backend sends ``Sector`` (capitalised, sector_dashboard convention) or
    // ``sector_name`` (ranked_signals convention) — accept both.
    const sector = asString(row.Sector) ?? asString(row.sector_name) ?? '—';
    const rsRankPct = asNumber(row.RS_rank_pct ?? row.rs_rank_pct ?? row.rs);
    const quadrant = asString(row.Quadrant ?? row.quadrant);
    return { sector, rsRankPct, quadrant, raw: row };
  });
}

function mapSummary(
  raw: BackendWorkspaceSnapshot['summary'],
): WorkspaceSummary {
  const safe = raw ?? {};
  return {
    topSector: asString(safe.top_sector),
    breakoutCount: asNumber(safe.breakout_count),
    patternCount: asNumber(safe.pattern_count),
    dataTrustStatus: asString(safe.data_trust_status),
    raw: safe,
  };
}

export async function getWorkspaceSnapshot(topN = 3): Promise<WorkspaceSnapshot> {
  const raw = await fetchDashboardJsonStrict<BackendWorkspaceSnapshot>(
    `/api/execution/workspace/snapshot?top_n=${topN}`,
    RAW_FALLBACK,
  );

  return {
    available: Boolean(raw.available),
    artifactPath: raw.artifact_path ?? null,
    summary: mapSummary(raw.summary),
    topActions: mapTopActions(raw.top_actions),
    sectorLeaders: mapSectorLeaders(raw.sector_leaders),
    counts: {
      ranked: Number(raw.counts?.ranked ?? 0),
      breakouts: Number(raw.counts?.breakouts ?? 0),
      patterns: Number(raw.counts?.patterns ?? 0),
      sectors: Number(raw.counts?.sectors ?? 0),
    },
  };
}
