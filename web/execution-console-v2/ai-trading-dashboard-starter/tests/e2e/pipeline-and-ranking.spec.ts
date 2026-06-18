import { expect, test } from '@playwright/test';

test.describe('execution-console-v2 smoke', () => {
  test('loads pipeline page with workspace cards', async ({ page }) => {
    await page.goto('/pipeline');

    await expect(page.getByText('Ranked Signals')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Pipeline' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Health' })).toBeVisible();
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

  test('renders RRG-style sector rotation chart controls and drawer', async ({ page }) => {
    await page.goto('/sector-rotation');

    await expect(page.getByRole('heading', { name: 'RRG-style Sector Rotation' })).toBeVisible();
    await expect(page.getByTestId('rrg-chart')).toBeVisible();
    await expect(page.getByText('Leading').first()).toBeVisible();
    await expect(page.getByText('Improving').first()).toBeVisible();
    await expect(page.getByText('Weakening').first()).toBeVisible();
    await expect(page.getByText('Lagging').first()).toBeVisible();
    await expect(page.getByTestId('rrg-crosshair-x')).toHaveAttribute('stroke-dasharray', '6 5');
    await expect(page.getByTestId('rrg-crosshair-y')).toHaveAttribute('stroke-dasharray', '6 5');
    await expect(page.getByText('Right = stronger than benchmark')).toBeVisible();
    await expect(page.getByText('Up = improving momentum')).toBeVisible();
    const chartBox = await page.getByTestId('rrg-chart').boundingBox();
    expect(chartBox?.height ?? 0).toBeGreaterThan(520);

    await expect(page.getByTestId('rrg-point-PSU Bank')).toBeVisible();
    await page.getByLabel('Scale mode').selectOption('wide');
    await page.getByLabel('Label mode').selectOption('off');
    await expect(page.getByTestId('rrg-chart').locator('text').filter({ hasText: 'PSU Bank' })).toHaveCount(0);
    await page.getByLabel('Label mode').selectOption('top');
    await page.getByRole('button', { name: 'Sector' }).click();
    await expect(page.getByTestId('rrg-point-Banks')).toBeVisible();

    await page.getByRole('button', { name: 'Industry' }).click();
    await page.getByTestId('rrg-point-PSU Bank').click();
    await expect(page.getByRole('heading', { name: 'PSU Bank', exact: true })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Stock Confirmations', exact: true })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Watchlist Candidates' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Delivery Signals', exact: true }).first()).toBeVisible();
    await page.getByRole('button', { name: 'Close', exact: true }).click();

    await page.getByRole('button', { name: 'Full view' }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await page.getByRole('button', { name: 'Close', exact: true }).click();

    await page.getByRole('button', { name: 'Play' }).click();
    await expect(page.getByText('2026-04-06')).toBeVisible({ timeout: 2500 });
    await page.getByRole('button', { name: 'Pause' }).click();
  });

  test('renders investigator decision board and symbol drawer', async ({ page }) => {
    await page.goto('/investigator');

    await expect(page.getByRole('heading', { name: 'Investigator', exact: true })).toBeVisible();
    await expect(page.getByText('Trust: trusted')).toBeVisible();
    await expect(page.getByText('Daily Gainers').first()).toBeVisible();
    await expect(page.getByText('New In Window').first()).toBeVisible();
    await expect(page.getByText('Trap Count').first()).toBeVisible();
    await expect(page.getByText('Fresh Traps').first()).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Action Queue' })).toBeVisible();
    await expect(page.getByText('No High Conviction today. Showing nearest watchlist candidates ranked by investigator score.')).toBeVisible();
    await expect(page.getByText('Reason: no candidate passed score >=80 + volume confirmation + rank improvement.')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Repeat Quality' })).toBeVisible();
    await expect(page.getByText('Price Sustain').first()).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Trap Radar' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Today Funnel' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Rolling Window Funnel' })).toBeVisible();
    await page.getByRole('button', { name: /Price fade/i }).click();
    await expect(page.getByRole('button', { name: /Trap: Price fade x/i })).toBeVisible();
    await page.getByRole('button', { name: /Trap: Price fade x/i }).click();

    await page.locator('section').filter({ has: page.getByRole('heading', { name: 'Action Queue' }) }).getByRole('button', { name: 'Open' }).first().click();
    await expect(page.getByRole('heading', { name: 'KICL', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Timeline' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Thesis' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Repeat Evidence' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Factor Breakdown' })).toBeVisible();
    await page.getByRole('button', { name: 'Close', exact: true }).click();
  });
});
