import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import {
  AsOfSelector, ConflictBanner, DataTable, EmptyState, ErrorState, FreshnessBadge,
  LimitationList, LineageSummary, PaginationControls, PartialDataBanner, StatusBadge,
  UnavailableState,
} from './components';
import { Phase4ApiError, type ResponseMeta } from './types';

const meta: ResponseMeta = {
  request_id: 'request-1', generated_at: '2026-07-15T00:00:00Z', partial: true,
  limitations: ['SOURCE_NOT_MIGRATED'], lineage: [],
  freshness: { freshness_status: 'UNKNOWN', freshness_reasons: ['LINEAGE_UNAVAILABLE'] },
  pagination: { has_more: true, next_cursor: 'opaque', limit: 50 },
};

const wrap = (node: React.ReactNode) => render(<MemoryRouter>{node}</MemoryRouter>);

describe('shared operator metadata components', () => {
  it('maps a known and unknown status to accessible text', () => { wrap(<><StatusBadge value="healthy" /><StatusBadge value="new-state" /></>); expect(screen.getByLabelText('status: healthy')).toBeInTheDocument(); expect(screen.getByText('new-state')).toBeInTheDocument(); });
  it('never defaults missing freshness to fresh', () => { wrap(<FreshnessBadge />); expect(screen.getByLabelText('freshness: UNKNOWN')).toBeInTheDocument(); expect(screen.queryByText('FRESH')).not.toBeInTheDocument(); });
  it('renders partial limitations with plain language', () => { wrap(<PartialDataBanner meta={meta} />); expect(screen.getByText('Partial data')).toBeInTheDocument(); expect(screen.getByText('SOURCE_NOT_MIGRATED')).toBeInTheDocument(); });
  it('renders limitation codes without claiming completeness', () => { wrap(<LimitationList limitations={['COPIED_REALISTIC_BASELINE_MISSING']} />); expect(screen.getByText(/baseline not established/i)).toBeInTheDocument(); });
  it('renders unavailable and empty as distinct states', () => { wrap(<><EmptyState /><UnavailableState /></>); expect(screen.getByText('No available records')).toBeInTheDocument(); expect(screen.getByText('Unavailable')).toBeInTheDocument(); });
  it('renders lineage unavailable without fabricated values', () => { wrap(<LineageSummary meta={meta} />); expect(screen.getByText('Lineage and freshness')).toBeInTheDocument(); expect(screen.getAllByText('Unavailable').length).toBeGreaterThan(0); });
  it('renders governance conflict navigation', () => { wrap(<ConflictBanner />); expect(screen.getByRole('alert')).toHaveTextContent('no authoritative value'); expect(screen.getByRole('link')).toHaveAttribute('href', '/governance'); });
  it.each([[401, 'Authentication required'], [403, 'Authorization denied'], [404, 'Resource not found'], [409, 'Governance conflict'], [429, 'Rate limited'], [503, 'Source unavailable']])('distinguishes HTTP %s', (status, title) => { wrap(<ErrorState error={new Phase4ApiError(Number(status), { code: 'SAFE', message: 'safe', request_id: 'req' })} />); expect(screen.getByText(title)).toBeInTheDocument(); });
  it('renders semantic table headers and stable data', () => { wrap(<DataTable rows={[{ id: 'one', status: 'open' }]} columns={[{ key: 'status', label: 'Status' }]} rowKey={(row) => String(row.id)} caption="Evidence" />); expect(screen.getByRole('columnheader', { name: 'Status' })).toBeInTheDocument(); expect(screen.getByRole('cell', { name: 'open' })).toBeInTheDocument(); });
  it('renders server pagination controls', () => { wrap(<PaginationControls meta={meta} />); expect(screen.getByRole('button', { name: 'Next page' })).toBeEnabled(); });
  it('renders latest as-of state without credentials in the URL', () => { wrap(<AsOfSelector />); expect(screen.getByText('Latest')).toBeInTheDocument(); expect(screen.getByLabelText('Evidence as of')).toHaveAttribute('max'); });
});
