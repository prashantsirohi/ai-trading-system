import { fetchDashboardJsonStrict } from '@/lib/api/client';

export interface FundamentalsDashboard {
  summary: Record<string, unknown>;
  valuation_chart: Array<Record<string, unknown>>;
  great_results_top: Array<Record<string, unknown>>;
  turnarounds_top: Array<Record<string, unknown>>;
  compounders_top: Array<Record<string, unknown>>;
  sector_earnings_top: Array<Record<string, unknown>>;
  source_path?: string | null;
}

const FALLBACK: FundamentalsDashboard = {
  summary: {},
  valuation_chart: [],
  great_results_top: [],
  turnarounds_top: [],
  compounders_top: [],
  sector_earnings_top: [],
  source_path: null,
};

export async function getFundamentalsDashboard(): Promise<FundamentalsDashboard> {
  return fetchDashboardJsonStrict<FundamentalsDashboard>('/api/execution/fundamentals/dashboard', FALLBACK);
}
