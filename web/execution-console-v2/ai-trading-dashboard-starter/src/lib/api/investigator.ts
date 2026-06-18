import { fetchDashboardJsonStrict } from '@/lib/api/client';

export interface InvestigatorSnapshot {
  summary: Record<string, unknown>;
  raw_summary?: Record<string, unknown>;
  summary_deltas?: Record<string, number>;
  run_id?: string;
  run_date?: string;
  data_trust_status?: string;
  stage_status?: Record<string, string>;
  pattern_confirmation?: Record<string, unknown>;
  decision_queue?: Array<Record<string, unknown>>;
  closest_to_high_conviction?: Array<Record<string, unknown>>;
  repeat_quality?: Array<Record<string, unknown>>;
  trap_radar?: Array<Record<string, unknown>>;
  archive_today?: Array<Record<string, unknown>>;
  charts?: {
    funnel?: Array<Record<string, unknown>>;
    repeat_price_scatter?: Array<Record<string, unknown>>;
    four_week_trend?: Array<Record<string, unknown>>;
    funnel_today?: Array<Record<string, unknown>>;
    funnel_window?: Array<Record<string, unknown>>;
    trend?: Array<Record<string, unknown>>;
  };
  row_details?: Record<string, Record<string, unknown>>;
  decision_payload?: Record<string, unknown>;
  today_gainers: Array<Record<string, unknown>>;
  high_conviction: Array<Record<string, unknown>>;
  repeat_tracker: Array<Record<string, unknown>>;
  trap_log: Array<Record<string, unknown>>;
  active_watchlist: Array<Record<string, unknown>>;
  investigator_pattern_scan: Array<Record<string, unknown>>;
  archive_summary: {
    count: number;
    by_reason: Record<string, number>;
    rows: Array<Record<string, unknown>>;
  };
  source_artifacts?: Record<string, string>;
}

const FALLBACK: InvestigatorSnapshot = {
  run_id: 'mock-investigator-run',
  run_date: '2026-06-17',
  data_trust_status: 'trusted',
  stage_status: { rank: 'completed', investigator: 'completed', publish: 'pending' },
  summary: {
    daily_gainers: 51,
    new_candidates: 12,
    new_in_window: 12,
    active_queue: 42,
    repeat_ge3: 8,
    improving_repeats: 4,
    high_conviction: 0,
    trap_rate: 0.31,
    traps: 16,
    trap_count: 16,
    fresh_trap_today: 3,
    repeat_trap: 5,
    archived: 9,
  },
  raw_summary: { daily_gainer_count: 51, active_count: 42, trap_count: 16 },
  summary_deltas: { daily_gainers: 5, new_candidates: 2, new_in_window: 2, active_queue: 7, repeat_ge3: 3, improving_repeats: 1, high_conviction: -1, traps: 4, trap_count: 4, fresh_trap_today: 1, repeat_trap: 2, archived: 2 },
  pattern_confirmation: {
    scanned_count: 18,
    s1_base_forming: 7,
    s1_near_breakout: 5,
    s1_to_s2_transition: 2,
    s2_confirmed: 1,
    top_setups: [
      { symbol_id: 'KICL', pattern_family: 'round_bottom', pattern_state: 'watchlist', pattern_score: 72, setup_quality: 63, s1_promotion_state: 'S1_TO_S2_TRANSITION', promotion_reason: 'High pattern score with volume confirmation' },
      { symbol_id: 'KILBURN', pattern_family: 'cup_handle', pattern_state: 'watchlist', pattern_score: 66, setup_quality: 61, s1_promotion_state: 'S1_NEAR_BREAKOUT', promotion_reason: 'Pattern quality near breakout threshold' },
    ],
  },
  decision_queue: [
    {
      symbol_id: 'KICL',
      decision_verdict: 'Investigate',
      decision_reason: 'Repeat + price holding',
      investigator_score: 78,
      appearance_count_20d: 5,
      price_progression_pct: 8.8,
      rank_change_20d: -10,
      volume_signal: 'Rising',
      sector: 'Industrials',
      setup: 'Weekly Gainer',
      days_since_last_seen: 0,
      pattern_family: 'round_bottom',
      pattern_state: 'watchlist',
      pattern_lifecycle_state: 'watchlist',
      pattern_score: 72,
      setup_quality: 63,
      s1_promotion_state: 'S1_TO_S2_TRANSITION',
      promotion_reason: 'High pattern score with volume confirmation',
      breakout_level: 126,
      watchlist_trigger_level: 121,
      invalidation_price: 104,
    },
    {
      symbol_id: 'KILBURN',
      decision_verdict: 'Watch',
      decision_reason: 'Repeat but rank slipping',
      investigator_score: 71,
      appearance_count_20d: 4,
      price_progression_pct: 8.8,
      rank_change_20d: 44,
      volume_signal: 'Flat',
      sector: 'Capital Goods',
      setup: 'Repeat Gainer',
      days_since_last_seen: 1,
      pattern_family: 'cup_handle',
      pattern_state: 'watchlist',
      pattern_lifecycle_state: 'watchlist',
      pattern_score: 66,
      setup_quality: 61,
      s1_promotion_state: 'S1_NEAR_BREAKOUT',
      promotion_reason: 'Pattern quality near breakout threshold',
    },
    {
      symbol_id: 'EKC',
      decision_verdict: 'Trap Risk',
      decision_reason: 'Price fade',
      investigator_score: 64,
      appearance_count_20d: 4,
      price_progression_pct: -2.1,
      rank_change_20d: -96,
      volume_signal: 'Rising',
      sector: 'Energy',
      setup: 'Volume Shock',
      trap_category: 'Price fade',
      days_since_last_seen: 0,
    },
  ],
  closest_to_high_conviction: [
    { symbol_id: 'KICL', investigator_score: 78, decision_verdict: 'Investigate', decision_reason: 'Needs volume confirmation' },
    { symbol_id: 'KILBURN', investigator_score: 72, decision_verdict: 'Watch', decision_reason: 'Rank change negative' },
  ],
  repeat_quality: [
    { symbol_id: 'KICL', appearance_count_20d: 5, price_progression_pct: 8.8, rank_signal: 'Improving', volume_signal: 'Rising', high_priority_repeat: true, repeat_strength: 86, price_sustain: 72, rank_momentum: 80, volume_confirmation: 100 },
    { symbol_id: 'KILBURN', appearance_count_20d: 4, price_progression_pct: 8.8, rank_signal: 'Falling', volume_signal: 'Flat', repeat_strength: 75, price_sustain: 70, rank_momentum: 32, volume_confirmation: 0 },
  ],
  trap_radar: [
    { trap_category: 'One-day spike', count: 8, examples: ['EKC'] },
    { trap_category: 'Price fade', count: 5, examples: ['XYZ'] },
    { trap_category: 'Rank collapse', count: 3, examples: ['ABC'] },
  ],
  archive_today: [],
  charts: {
    funnel: [
      { key: 'daily', label: 'Daily Gainers', count: 51 },
      { key: 'active', label: 'Active Queue', count: 42 },
      { key: 'repeat', label: 'Repeat >=3x', count: 8 },
      { key: 'improving', label: 'Improving', count: 4 },
      { key: 'high', label: 'High Conviction', count: 0 },
      { key: 'traps', label: 'Trap Count', count: 16 },
      { key: 'archived', label: 'Archived', count: 9 },
    ],
    funnel_today: [
      { key: 'daily', label: 'Daily Gainers (today)', count: 51 },
      { key: 'fresh_traps', label: 'Fresh Traps (today)', count: 3 },
      { key: 'high', label: 'High Conviction (today)', count: 0 },
    ],
    funnel_window: [
      { key: 'new_window', label: 'New In Window', count: 12 },
      { key: 'active', label: 'Active Queue', count: 42 },
      { key: 'repeat', label: 'Repeat >=3x', count: 8 },
      { key: 'improving', label: 'Improving', count: 4 },
      { key: 'repeat_trap', label: 'Repeat Trap', count: 5 },
      { key: 'archived', label: 'Archived', count: 9 },
    ],
    repeat_price_scatter: [
      { symbol_id: 'KICL', appearance_count_20d: 5, price_progression_pct: 8.8, investigator_score: 78, decision_verdict: 'Investigate' },
      { symbol_id: 'KILBURN', appearance_count_20d: 4, price_progression_pct: 8.8, investigator_score: 71, decision_verdict: 'Watch' },
      { symbol_id: 'EKC', appearance_count_20d: 4, price_progression_pct: -2.1, investigator_score: 64, decision_verdict: 'Trap Risk' },
    ],
    four_week_trend: [
      { week: '2026-W22', active: 20, traps: 7, archived: 5 },
      { week: '2026-W23', active: 28, traps: 11, archived: 7 },
      { week: '2026-W24', active: 42, traps: 16, archived: 9 },
    ],
    trend: [
      { date: '2026-06-15', new: 4, repeat: 11, improving: 2, traps: 4, archived: 2, high_conviction: 1 },
      { date: '2026-06-16', new: 5, repeat: 14, improving: 3, traps: 5, archived: 3, high_conviction: 0 },
      { date: '2026-06-17', new: 3, repeat: 17, improving: 4, traps: 7, archived: 4, high_conviction: 0 },
    ],
  },
  row_details: {
    KICL: {
      summary: { symbol_id: 'KICL', sector: 'Industrials', decision_reason: 'Repeat + price holding', decision_verdict: 'Investigate', investigator_score: 78, price_progression_pct: 8.8, appearance_count_20d: 5, rank_change_20d: -10, setup: 'Weekly Gainer', pattern_family: 'round_bottom', pattern_state: 'watchlist', pattern_score: 72, setup_quality: 63, s1_promotion_state: 'S1_TO_S2_TRANSITION', promotion_reason: 'High pattern score with volume confirmation', breakout_level: 126, watchlist_trigger_level: 121, invalidation_price: 104 },
      repeat: { first_seen_date: '2026-06-03', last_seen_date: '2026-06-17', appearance_count_20d: 5, repeat_score: 86 },
    },
  },
  today_gainers: [{ symbol_id: 'KICL', daily_return_pct: 6.1 }],
  high_conviction: [],
  repeat_tracker: [],
  trap_log: [],
  investigator_pattern_scan: [
    { symbol_id: 'KICL', pattern_family: 'round_bottom', pattern_state: 'watchlist', pattern_lifecycle_state: 'watchlist', pattern_score: 72, setup_quality: 63, s1_promotion_state: 'S1_TO_S2_TRANSITION', promotion_reason: 'High pattern score with volume confirmation' },
    { symbol_id: 'KILBURN', pattern_family: 'cup_handle', pattern_state: 'watchlist', pattern_lifecycle_state: 'watchlist', pattern_score: 66, setup_quality: 61, s1_promotion_state: 'S1_NEAR_BREAKOUT', promotion_reason: 'Pattern quality near breakout threshold' },
  ],
  active_watchlist: [
    { symbol_id: 'KICL', decision_verdict: 'Investigate', decision_reason: 'Repeat + price holding', investigator_score: 78, appearance_count_20d: 5, price_progression_pct: 8.8, rank_change_20d: -10, volume_signal: 'Rising', sector: 'Industrials', setup: 'Weekly Gainer', days_since_last_seen: 0, pattern_family: 'round_bottom', pattern_state: 'watchlist', pattern_score: 72, setup_quality: 63, s1_promotion_state: 'S1_TO_S2_TRANSITION', promotion_reason: 'High pattern score with volume confirmation' },
    { symbol_id: 'KILBURN', decision_verdict: 'Watch', decision_reason: 'Repeat but rank slipping', investigator_score: 71, appearance_count_20d: 4, price_progression_pct: 8.8, rank_change_20d: 44, volume_signal: 'Flat', sector: 'Capital Goods', setup: 'Repeat Gainer', days_since_last_seen: 1, pattern_family: 'cup_handle', pattern_state: 'watchlist', pattern_score: 66, setup_quality: 61, s1_promotion_state: 'S1_NEAR_BREAKOUT', promotion_reason: 'Pattern quality near breakout threshold' },
  ],
  archive_summary: { count: 1, by_reason: { ONE_CANDLE_DRAMA: 1 }, rows: [{ symbol_id: 'XYZ', drop_reason: 'ONE_CANDLE_DRAMA', verdict: 'WATCH_ONLY' }] },
  source_artifacts: {},
};

export async function getInvestigatorSnapshot(): Promise<InvestigatorSnapshot> {
  return fetchDashboardJsonStrict<InvestigatorSnapshot>('/api/execution/investigator', FALLBACK);
}
