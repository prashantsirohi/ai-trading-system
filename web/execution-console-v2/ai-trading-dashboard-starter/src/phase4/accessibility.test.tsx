import { render } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import axe from 'axe-core';
import { describe, expect, it } from 'vitest';
import { AuthProvider, LoginView } from './auth';
import { ConflictBanner, DataTable, PartialDataBanner } from './components';

async function expectAccessible(container: HTMLElement) {
  const results = await axe.run(container, { runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa'] } });
  expect(results.violations.filter((item) => ['serious', 'critical'].includes(item.impact ?? ''))).toEqual([]);
}

describe('automated WCAG checks', () => {
  it('checks the authentication view', async () => {
    const { container } = render(<AuthProvider><LoginView /></AuthProvider>);
    await expectAccessible(container);
  });

  it('checks partial and governance conflict states', async () => {
    const { container } = render(<MemoryRouter><main><h1>Governance</h1><ConflictBanner /><PartialDataBanner meta={{ request_id: 'r', generated_at: '2026-07-15', partial: true, limitations: ['SOURCE_EMPTY'], lineage: [], freshness: { freshness_status: 'UNKNOWN' } }} /></main></MemoryRouter>);
    await expectAccessible(container);
  });

  it('checks a position evidence table', async () => {
    const { container } = render(<DataTable rows={[{ id: 'cycle-1', symbol: 'ABC', coverage: 'FULLY_MONITORED' }]} columns={[{ key: 'symbol', label: 'Symbol' }, { key: 'coverage', label: 'Coverage status' }]} rowKey={(row) => String(row.id)} caption="Position coverage" />);
    await expectAccessible(container);
  });
});
