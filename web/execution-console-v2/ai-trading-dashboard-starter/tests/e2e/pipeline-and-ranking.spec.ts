import { expect, test } from '@playwright/test';

test.describe('execution-console-v2 smoke', () => {
  test('loads pipeline page with workspace cards', async ({ page }) => {
    await page.goto('/pipeline');

    await expect(page.getByText('Ranked Signals')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Pipeline Workspace' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Workspace Status' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Signal Summaries' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Top Ranked Candidates' })).toBeVisible();
  });

  test('expands ranking row on click and surfaces verdict + lifecycle', async ({ page }) => {
    await page.goto('/ranking');

    const table = page.locator('table');
    await expect(table).toBeVisible();
    const firstRow = page.locator('tbody tr[data-symbol]').first();
    await expect(firstRow).toBeVisible();

    await firstRow.click();

    // Expanded panel surfaces these section headings.
    await expect(page.getByRole('heading', { name: 'Model Explanation' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Score Decomposition' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Lifecycle' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Factor Bars' })).toBeVisible();
  });

  test('comparison tray accepts a symbol', async ({ page }) => {
    await page.goto('/ranking');

    const firstRow = page.locator('tbody tr[data-symbol]').first();
    await firstRow.click();

    await page.getByRole('button', { name: /Add to compare/i }).click();
    await expect(page.getByText(/Compare \(1\/3\)/)).toBeVisible();
  });
});
