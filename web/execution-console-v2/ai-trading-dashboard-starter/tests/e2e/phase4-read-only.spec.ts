import { expect, test, type Page } from '@playwright/test';

const limitations = [
  'SINGLE_YEAR_CONCENTRATION', 'COPIED_REALISTIC_BASELINE_MISSING',
  'OPERATOR_MIGRATIONS_NOT_APPLIED', 'EMPTY_REAL_PHASE3B_HISTORY',
];

function meta(partial = false) {
  return { request_id: 'e2e-request', generated_at: '2026-07-15T10:00:00Z', partial, limitations: partial ? limitations : [], lineage: [], lineage_meta: { source_consistent: true }, freshness: { freshness_status: partial ? 'UNKNOWN' : 'FRESH', latest_market_session: '2026-07-14', expected_market_session: '2026-07-14', freshness_reasons: [] } };
}

async function fixtureApi(page: Page, methods: string[]) {
  await page.route('**/api/v1/**', async (route) => {
    const request = route.request();
    methods.push(request.method());
    const path = new URL(request.url()).pathname;
    let data: unknown = [];
    if (path === '/api/v1/system/readiness') data = { readiness_status: 'READY_WITH_LIMITATIONS', phase4_development_ready: true, phase4_production_ready: false, limitations: limitations.map((limitation_id) => ({ limitation_id })) };
    else if (path === '/api/v1/system/limitations') data = limitations.map((limitation_id) => ({ limitation_id, description: limitation_id, production_blocking: true }));
    else if (path === '/api/v1/readiness/checks') data = [{ check_id: 'HISTORY', category: 'calibration quality', severity: 'warning', status: 'WARN', production_blocking: true, limitation: limitations[0] }];
    else if (path === '/api/v1/market/stage') data = { observations: [], conflicts: [] };
    else if (path === '/api/v1/routing') data = [{ decision_id: 'route-1', symbol_id: 'ABC', exchange: 'NSE', effective_scan_tier: 'position_monitor', winning_reason: 'active_position', all_reasons: ['active_position', 'rank_selected'], new_long_structural_block: false, active_position_structural_risk: true, risk_severity: 'high', policy_version: 'v2' }];
    else if (path === '/api/v1/candidates') data = [];
    else if (path === '/api/v1/positions/coverage') data = [{ position_cycle_id: 'cycle-1', symbol_id: 'ABC', coverage_status: 'ROUTED_WITH_INCOMPLETE_DATA', effective_scan_tier: 'position_monitor', market_data_complete: false, evidence_complete: false, missing_fields: ['weekly_close'], episode_compatibility: 'compatible', positive_action_suppressed: true, suppression_reasons: ['market_data_incomplete'] }];
    else if (path === '/api/v1/positions/coverage/cycle-1') data = { position_cycle_id: 'cycle-1', symbol_id: 'ABC', coverage_status: 'ROUTED_WITH_INCOMPLETE_DATA', positive_action_suppressed: true, suppression_reasons: ['market_data_incomplete'] };
    else if (path === '/api/v1/alerts') data = [{ alert_id: 'alert-1', alert_code: 'POSITION_DATA_MISSING', severity: 'critical', status: 'open', symbol_id: 'ABC' }];
    else if (path === '/api/v1/governance/conflicts') data = [{ conflict_id: 'conflict-1', conflict_type: 'COMPETING_TERMINAL_STAGE_OBSERVATIONS', entity_type: 'stock', entity_id: 'ABC', severity: 'critical', status: 'open', message: 'No authoritative stage' }];
    else if (path === '/api/v1/calibration/summary') data = { manifest_id: 'manifest-1', total_samples: 12, eligible_count: 8, excluded_count: 2, quarantined_count: 1, pending_count: 1 };
    else if (path === '/api/v1/calibration/exclusions') data = [{ exclusion_reason: 'LOOK_AHEAD', count: 2 }];
    else if (path === '/api/v1/performance/latest') data = { run_id: 'run-1', functional_status: 'PASS', performance_status: 'WARN', replay_equivalence: 'EXACT_REPLAY' };
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ data, meta: meta(path.includes('readiness') || path.includes('performance')) }) });
  });
}

test('operator completes a read-only Phase 4B flow', async ({ page }) => {
  const methods: string[] = [];
  await fixtureApi(page, methods);
  await page.goto('/');
  await page.getByLabel('API credential').fill('fixture-key');
  await page.getByRole('button', { name: 'Open read-only dashboard' }).click();
  await expect(page.getByText('Development view only — production readiness is blocked.')).toBeVisible();
  await page.getByRole('link', { name: 'System Readiness' }).click();
  await expect(page.getByText('SINGLE_YEAR_CONCENTRATION').first()).toBeVisible();
  await page.getByRole('link', { name: 'Positions' }).click();
  await page.getByRole('row', { name: /ABC/ }).press('Enter');
  await expect(page.locator('.notice-conflict').getByText('Positive action suppressed', { exact: true })).toBeVisible();
  await page.getByRole('link', { name: 'Governance' }).click();
  await expect(page.getByText('COMPETING_TERMINAL_STAGE_OBSERVATIONS')).toBeVisible();
  await page.getByRole('link', { name: 'Calibration' }).click();
  await page.getByRole('tab', { name: 'exclusions' }).click();
  await expect(page.getByText('LOOK_AHEAD')).toBeVisible();
  await page.getByRole('link', { name: 'Performance' }).click();
  await page.getByRole('tab', { name: 'baselines' }).click();
  await expect(page.getByText('Copied-realistic performance baseline not established.')).toBeVisible();
  await page.getByRole('link', { name: 'Market & Sectors' }).click();
  await page.getByLabel('Evidence as of').fill('2026-07-01');
  await expect(page).toHaveURL(/as_of=2026-07-01/);
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByLabel('API credential')).toBeVisible();
  expect(methods.length).toBeGreaterThan(0);
  expect(new Set(methods)).toEqual(new Set(['GET']));
});
