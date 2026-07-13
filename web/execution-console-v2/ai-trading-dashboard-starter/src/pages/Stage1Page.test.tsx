import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Stage1Page from './Stage1Page';

const current = vi.fn();
const detail = vi.fn();
const analytics = vi.fn();
vi.mock('@/lib/queries', () => ({
  useStage1Summary: () => ({ data: { active_count: 2, breakout_ready_count: 1, late_stage1_count: 1, accumulating_count: 0, promotion_pending_count: 0, progressions_today: 1, regressions_today: 0 }, isLoading: false }),
  useStage1Current: (filters: unknown) => current(filters),
  useStage1Transitions: () => ({ data: { rows: [] } }),
  useStage1Exits: () => ({ data: { rows: [] } }),
  useStage1Detail: () => detail(),
  useStage1Analytics: () => analytics(),
}));

const rows = [{ symbol_id: 'READY', stage1_lifecycle_state: 'BREAKOUT_READY', stage1_substate: 'STAGE_1_BREAKOUT_READY', stage1_maturity_score: 78, stage1_emerging_rank: 2, golden_cross_status: 'IMMINENT', operator_status: 'WATCH_CLOSELY', operator_priority: 'HIGH', operator_queue_eligible: true }];

describe('Stage1Page', () => {
  beforeEach(() => {
    current.mockImplementation(() => ({ data: { rows, total: 1 }, isLoading: false }));
    detail.mockReturnValue({ data: undefined, isLoading: false });
    analytics.mockReturnValue({ data: { metadata: {}, rows: [] }, isLoading: false });
  });
  it('renders summary, queue and empty transition states', () => {
    render(<MemoryRouter><Stage1Page /></MemoryRouter>);
    expect(screen.getByText('Stage-1 Emerging Leaders')).toBeInTheDocument();
    expect(screen.getAllByText('READY').length).toBeGreaterThan(0);
    expect(screen.getByText('No Stage-1 transitions today.')).toBeInTheDocument();
  });
  it('applies the breakout-ready filter from its summary card', () => {
    render(<MemoryRouter><Stage1Page /></MemoryRouter>);
    fireEvent.click(screen.getByText('Breakout Ready').closest('button') as HTMLButtonElement);
    expect(current.mock.calls.some(([filters]) => filters && filters.lifecycle_state === 'BREAKOUT_READY')).toBe(true);
  });
  it('renders persisted analytical dates in the candidate drawer', () => {
    detail.mockReturnValue({ data: { symbol_id: 'READY', current: rows[0], state: rows, transitions: [], histories: {} }, isLoading: false });
    analytics.mockReturnValue({ data: { metadata: { data_source: 'DUCKDB' }, rows: [
      { trade_date: '2026-07-09', stage1_maturity_score: 72, stage1_emerging_rank: 4 },
      { trade_date: '2026-07-10', stage1_maturity_score: 78, stage1_emerging_rank: 2 },
    ] }, isLoading: false });
    render(<MemoryRouter><Stage1Page /></MemoryRouter>);
    fireEvent.click(screen.getAllByText('READY')[0]);
    expect(screen.getByText('Persisted analytical history')).toBeInTheDocument();
    expect(screen.getByText('2026-07-09')).toBeInTheDocument();
    expect(screen.getByText('2026-07-10')).toBeInTheDocument();
  });
});
