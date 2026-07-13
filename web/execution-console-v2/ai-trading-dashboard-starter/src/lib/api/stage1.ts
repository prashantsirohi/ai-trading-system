import { fetchDashboardJsonStrict } from '@/lib/api/client';

export type Stage1Row = Record<string, unknown> & {
  symbol_id?: string; stage1_lifecycle_state?: string; stage1_substate?: string;
  operator_status?: string; operator_priority?: string; operator_action?: string;
  operator_reason?: string;
};
export interface Stage1Summary {
  as_of?: string; active_count: number; base_building_count: number;
  accumulating_count: number; late_stage1_count: number; breakout_ready_count: number;
  promotion_pending_count: number; regressed_count: number; stale_count: number;
  invalidated_today: number; new_discoveries_today: number; progressions_today: number;
  regressions_today: number; top_emerging_candidates: Stage1Row[];
  top_score_improvers: Stage1Row[]; top_rank_improvers: Stage1Row[];
}
export interface Stage1RowsResponse { as_of?: string; total?: number; limit?: number; offset?: number; rows: Stage1Row[] }
export interface Stage1AnalyticsResponse { metadata: Record<string, unknown>; rows: Stage1Row[] }
export interface Stage1Detail { symbol_id: string; current?: Stage1Row | null; state: Stage1Row[]; transitions: Stage1Row[]; histories: Record<string, Stage1Row[]> }
export interface Stage1Params {
  lifecycle_state?: string; operator_status?: string; operator_priority?: string; sector?: string;
  golden_cross_status?: string; pattern_promotion_state?: string; promotion_eligibility?: boolean;
  search?: string; include_blocked?: boolean; limit?: number; offset?: number; sort_by?: string;
  sort_direction?: 'asc' | 'desc';
}

function query(params: object = {}): string {
  const values = new URLSearchParams();
  Object.entries(params as Record<string, unknown>).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') values.set(key, String(value));
  });
  const encoded = values.toString();
  return encoded ? `?${encoded}` : '';
}

const EMPTY_ROWS: Stage1RowsResponse = { rows: [] };
const EMPTY_SUMMARY: Stage1Summary = { active_count: 0, base_building_count: 0, accumulating_count: 0, late_stage1_count: 0, breakout_ready_count: 0, promotion_pending_count: 0, regressed_count: 0, stale_count: 0, invalidated_today: 0, new_discoveries_today: 0, progressions_today: 0, regressions_today: 0, top_emerging_candidates: [], top_score_improvers: [], top_rank_improvers: [] };
export const getStage1Summary = () => fetchDashboardJsonStrict<Stage1Summary>('/api/execution/investigator/stage1/summary', EMPTY_SUMMARY);
export const getStage1Current = (params: Stage1Params = {}) => fetchDashboardJsonStrict<Stage1RowsResponse>(`/api/execution/investigator/stage1/current${query(params)}`, EMPTY_ROWS);
export const getStage1Transitions = () => fetchDashboardJsonStrict<Stage1RowsResponse>('/api/execution/investigator/stage1/transitions', EMPTY_ROWS);
export const getStage1Exits = () => fetchDashboardJsonStrict<Stage1RowsResponse>('/api/execution/investigator/stage1/exits', EMPTY_ROWS);
export const getStage1Detail = (symbol: string) => fetchDashboardJsonStrict<Stage1Detail>(`/api/execution/investigator/stage1/${encodeURIComponent(symbol)}`, { symbol_id: symbol, state: [], transitions: [], histories: {} });
export const getStage1Analytics = (symbol: string) => fetchDashboardJsonStrict<Stage1AnalyticsResponse>(`/api/execution/investigator/stage1/${encodeURIComponent(symbol)}/analytics-history`, { metadata: {}, rows: [] });
