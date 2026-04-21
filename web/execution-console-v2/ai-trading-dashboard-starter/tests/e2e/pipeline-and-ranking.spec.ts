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

  test('opens symbol detail drawer from ranking row click', async ({ page }) => {
    await page.goto('/ranking');

    const table = page.locator('table');
    await expect(table).toBeVisible();
    await expect(page.locator('tbody tr').first()).toBeVisible();

    const firstBodyRow = page.locator('tbody tr').first();
    await firstBodyRow.click();

    await expect(page.getByText('Symbol Detail')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Close', exact: true })).toBeVisible();
  });
});
