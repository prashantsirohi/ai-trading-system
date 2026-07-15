import { expect, test, type Page } from '@playwright/test';

const limitations = [
  'SINGLE_YEAR_CONCENTRATION', 'COPIED_REALISTIC_BASELINE_MISSING',
  'OPERATOR_MIGRATIONS_NOT_APPLIED', 'EMPTY_REAL_PHASE3B_HISTORY',
];

type ApiRequest = { method: string; url: string; authorization?: string; apiKey?: string };

function meta(partial = false) {
  return { request_id: 'e2e-request', generated_at: '2026-07-15T10:00:00Z', partial, limitations: partial ? limitations : [], lineage: [], lineage_meta: { source_consistent: true }, freshness: { freshness_status: partial ? 'UNKNOWN' : 'FRESH', latest_market_session: '2026-07-14', expected_market_session: '2026-07-14', freshness_reasons: [] } };
}

function errorBody(status: number) {
  const codes: Record<number, string> = { 401: 'UNAUTHORIZED', 403: 'FORBIDDEN', 404: 'NOT_FOUND', 409: 'CONFLICT', 429: 'RATE_LIMITED', 503: 'SOURCE_UNAVAILABLE' };
  return { code: codes[status] ?? `HTTP_${status}`, message: 'safe fixture error', request_id: `e2e-${status}` };
}

async function fixtureApi(page: Page, requests: ApiRequest[], statusByPath: Record<string, number> = {}) {
  await page.route('**/api/v1/**', async (route) => {
    const request = route.request();
    const headers = request.headers();
    requests.push({ method: request.method(), url: request.url(), authorization: headers.authorization, apiKey: headers['x-api-key'] });
    const path = new URL(request.url()).pathname;
    const status = statusByPath[path];
    if (status) {
      await route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(errorBody(status)) });
      return;
    }
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
  const requests: ApiRequest[] = [];
  await fixtureApi(page, requests);
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
  await expect(page.locator('section.notice-warning strong').filter({ hasText: 'Copied-realistic performance baseline not established.' })).toBeVisible();
  await page.getByRole('link', { name: 'Market & Sectors' }).click();
  await page.getByLabel('Evidence as of').fill('2026-07-01');
  await expect(page).toHaveURL(/as_of=2026-07-01/);
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByLabel('API credential')).toBeVisible();
  expect(requests.length).toBeGreaterThan(0);
  expect(new Set(requests.map((request) => request.method))).toEqual(new Set(['GET']));
  expect(requests.every((request) => request.authorization === 'Bearer fixture-key')).toBe(true);
  expect(requests.every((request) => !request.url.includes('fixture-key'))).toBe(true);
  expect(await page.evaluate(() => ({ local: localStorage.length, session: sessionStorage.length }))).toEqual({ local: 0, session: 0 });
});

test('operator can use the API-key header without leaking it into URLs', async ({ page }) => {
  const requests: ApiRequest[] = [];
  await fixtureApi(page, requests);
  await page.goto('/readiness');
  await page.getByLabel('API credential').fill('api-key-secret');
  await page.getByLabel('Authentication mode').selectOption('api-key');
  await page.getByRole('button', { name: 'Open read-only dashboard' }).click();
  await expect(page.getByRole('heading', { name: 'Development and production gates' })).toBeVisible();
  expect(requests.length).toBeGreaterThan(0);
  expect(requests.every((request) => request.apiKey === 'api-key-secret')).toBe(true);
  expect(requests.every((request) => !request.url.includes('api-key-secret'))).toBe(true);
});

test.describe('error states', () => {
  const expected: Record<number, string> = {
    401: 'Authentication required',
    403: 'Authorization denied',
    404: 'Resource not found',
    409: 'Governance conflict',
    429: 'Rate limited',
    503: 'Source unavailable',
  };

  for (const [status, label] of Object.entries(expected)) {
    test(`renders distinct ${status} feedback`, async ({ page }) => {
      const requests: ApiRequest[] = [];
      await fixtureApi(page, requests, { '/api/v1/system/readiness': Number(status) });
      await page.goto('/');
      await page.getByLabel('API credential').fill('fixture-key');
      await page.getByRole('button', { name: 'Open read-only dashboard' }).click();
      await expect(page.getByRole('alert').filter({ hasText: label })).toBeVisible();
      await expect(page.getByText(`Request: e2e-${status}`).first()).toBeVisible();
      expect(new Set(requests.map((request) => request.method))).toEqual(new Set(['GET']));
    });
  }
});

test.describe('real Phase 4A fixture API', () => {
  test.skip(process.env.PLAYWRIGHT_REAL_API !== 'true', 'Set PLAYWRIGHT_REAL_API=true to start the Phase 4A fixture API.');

  test('loads through the same-origin Vite proxy and stays read-only', async ({ page }) => {
    const expectedOrigin = new URL(process.env.PLAYWRIGHT_BASE_URL ?? `http://127.0.0.1:${process.env.PLAYWRIGHT_PORT ?? '4173'}`).origin;
    const requests: ApiRequest[] = [];
    page.on('request', (request) => {
      const url = request.url();
      if (url.includes('/api/v1/')) {
        const headers = request.headers();
        requests.push({ method: request.method(), url, authorization: headers.authorization, apiKey: headers['x-api-key'] });
      }
    });
    await page.goto('/readiness');
    await page.getByLabel('API credential').fill(process.env.PLAYWRIGHT_API_KEY ?? 'local-dev-key');
    await page.getByRole('button', { name: 'Open read-only dashboard' }).click();
    await expect(page.getByRole('heading', { name: 'Development and production gates' })).toBeVisible();
    await expect(page.getByText('Development view only — production readiness is blocked.')).toBeVisible();
    await page.getByRole('link', { name: 'Performance' }).click();
    await expect(page.getByRole('heading', { name: 'Benchmarks, replay, and baselines' })).toBeVisible();
    expect(requests.length).toBeGreaterThan(0);
    expect(new Set(requests.map((request) => request.method))).toEqual(new Set(['GET']));
    expect(requests.every((request) => new URL(request.url).origin === expectedOrigin)).toBe(true);
  });
});
