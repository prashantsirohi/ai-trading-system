import { expect, test } from '@playwright/test';

const row = { symbol_id: 'READY', stage1_lifecycle_state: 'BREAKOUT_READY', stage1_previous_lifecycle_state: 'LATE_STAGE1', stage1_substate: 'STAGE_1_BREAKOUT_READY', stage1_maturity_score: 78, stage1_score_peak: 80, stage1_score_delta_5d: 4, stage1_score_delta_20d: 9, stage1_emerging_rank: 2, stage1_emerging_rank_best: 2, emerging_rank_improvement_20d: 18, stage1_first_seen_date: '2026-06-20', stage1_days_in_lifecycle_state: 3, pattern_promotion_state: 'CONFIRMED', golden_cross_status: 'IMMINENT', sma50_sma200_gap_pct: -0.4, distance_to_pivot_pct: 1.5, stage1_eligible: true, promotion_eligibility: true, operator_status: 'WATCH_CLOSELY', operator_priority: 'HIGH', operator_action: 'WATCH_CLOSELY', operator_reason: 'Breakout Ready: score +9.0 over 20D, rank improved 18 over 20D, 1.5% from pivot', operator_queue_eligible: true };

test.beforeEach(async ({ page }) => {
  await page.route('**/api/execution/investigator/stage1/summary', route => route.fulfill({ json: { as_of: '2026-07-11', active_count: 1, base_building_count: 0, accumulating_count: 0, late_stage1_count: 0, breakout_ready_count: 1, promotion_pending_count: 0, regressed_count: 0, stale_count: 0, invalidated_today: 0, new_discoveries_today: 0, progressions_today: 1, regressions_today: 0, top_emerging_candidates: [row], top_score_improvers: [row], top_rank_improvers: [row] } }));
  await page.route('**/api/execution/investigator/stage1/current**', route => route.fulfill({ json: { as_of: '2026-07-11', total: 1, rows: [row] } }));
  await page.route('**/api/execution/investigator/stage1/transitions', route => route.fulfill({ json: { as_of: '2026-07-11', rows: [{ symbol_id: 'READY', trade_date: '2026-07-11', from_lifecycle_state: 'LATE_STAGE1', to_lifecycle_state: 'BREAKOUT_READY', stage1_score_before: 69, stage1_score_after: 78, emerging_rank_before: 20, emerging_rank_after: 2, transition_summary: 'LATE_STAGE1 → BREAKOUT_READY' }] } }));
  await page.route('**/api/execution/investigator/stage1/exits', route => route.fulfill({ json: { as_of: '2026-07-11', rows: [] } }));
  await page.route('**/api/execution/investigator/stage1/READY', route => route.fulfill({ json: { symbol_id: 'READY', current: row, state: [row], transitions: [{ trade_date: '2026-07-11', transition_summary: 'LATE_STAGE1 → BREAKOUT_READY', stage1_score_before: 69, stage1_score_after: 78, emerging_rank_before: 20, emerging_rank_after: 2 }], histories: {} } }));
});

test('filters breakout-ready candidates and opens lifecycle detail', async ({ page }) => {
  await page.goto('/investigator/stage1');
  await expect(page.getByRole('heading', { name: 'Stage-1 Emerging Leaders' })).toBeVisible();
  await page.getByText('Breakout Ready').first().click();
  await page.getByRole('button', { name: 'READY' }).last().click();
  await expect(page.getByRole('button', { name: 'Close', exact: true })).toBeVisible();
  await expect(page.getByText('Golden Cross').last()).toBeVisible();
  await expect(page.getByText('IMMINENT').last()).toBeVisible();
  await expect(page.getByText('LATE STAGE1 → BREAKOUT READY', { exact: true })).toBeVisible();
  await page.screenshot({ path: 'test-results/stage1-detail.png', fullPage: true });
  await page.getByRole('button', { name: 'Close', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Emerging Leaders', exact: true })).toBeVisible();
});
