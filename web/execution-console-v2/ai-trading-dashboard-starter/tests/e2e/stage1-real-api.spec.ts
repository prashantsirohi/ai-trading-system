import { expect, test } from '@playwright/test';

test.skip(process.env.PLAYWRIGHT_REAL_API !== 'true', 'requires the temporary real API harness');

test('renders persisted Stage-1 state and analytics from the temporary API', async ({ page, request }) => {
  const browserErrors: string[] = [];
  const failedRequests: string[] = [];
  page.on('console', message => { if (message.type() === 'error') browserErrors.push(message.text()); });
  page.on('pageerror', error => browserErrors.push(error.message));
  page.on('requestfailed', failed => failedRequests.push(`${failed.method()} ${failed.url()}`));

  const api = process.env.PLAYWRIGHT_API_BASE_URL ?? 'http://127.0.0.1:8090';
  const headers = { 'x-api-key': process.env.PLAYWRIGHT_API_KEY ?? 'local-dev-key' };
  const currentResponse = await request.get(`${api}/api/execution/investigator/stage1/current?include_blocked=true`, { headers });
  expect(currentResponse.ok()).toBeTruthy();
  const current = await currentResponse.json();
  expect(current.rows.map((row: { symbol_id: string }) => row.symbol_id).sort()).toEqual(['BLOCK', 'READY']);

  await page.goto('/investigator/stage1');
  await expect(page.getByText('READY').first()).toBeVisible();
  const readyButtons = page.locator('td button').filter({ hasText: /^READY$/ });
  await expect(readyButtons.first()).toBeVisible();
  const analyticsResponsePromise = page.waitForResponse(response => response.url().includes('/stage1/READY/analytics-history'));
  await readyButtons.first().click();
  const analyticsResponse = await analyticsResponsePromise;
  expect(analyticsResponse.ok()).toBeTruthy();
  expect((await analyticsResponse.json()).rows).toHaveLength(1);
  await expect(page.getByRole('button', { name: 'Close', exact: true })).toBeVisible();
  await expect(page.getByText('Persisted analytical history')).toBeVisible();
  await expect(page.getByRole('cell', { name: '2026-07-10', exact: true })).toBeVisible();
  await expect(page.getByText('LATE STAGE1 → BREAKOUT READY', { exact: true })).toBeVisible();

  const diagnostics = await request.get(`${api}/api/health/decision-read-sources`, { headers });
  expect(diagnostics.ok()).toBeTruthy();
  const sources = (await diagnostics.json()).decision_read_source_summary;
  expect(sources.every((source: { data_source: string; fallback_used: boolean }) => source.data_source === 'DUCKDB' && !source.fallback_used)).toBeTruthy();
  expect(browserErrors).toEqual([]);
  expect(failedRequests).toEqual([]);
});
