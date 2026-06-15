import { fetchDashboardJsonStrict } from '@/lib/api/client';

export interface InvestigatorSnapshot {
  summary: Record<string, unknown>;
  today_gainers: Array<Record<string, unknown>>;
  high_conviction: Array<Record<string, unknown>>;
  repeat_tracker: Array<Record<string, unknown>>;
  trap_log: Array<Record<string, unknown>>;
  active_watchlist: Array<Record<string, unknown>>;
  archive_summary: {
    count: number;
    by_reason: Record<string, number>;
    rows: Array<Record<string, unknown>>;
  };
  source_artifacts?: Record<string, string>;
}

const FALLBACK: InvestigatorSnapshot = {
  summary: {},
  today_gainers: [],
  high_conviction: [],
  repeat_tracker: [],
  trap_log: [],
  active_watchlist: [],
  archive_summary: { count: 0, by_reason: {}, rows: [] },
  source_artifacts: {},
};

export async function getInvestigatorSnapshot(): Promise<InvestigatorSnapshot> {
  return fetchDashboardJsonStrict<InvestigatorSnapshot>('/api/execution/investigator', FALLBACK);
}
