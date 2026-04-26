/**
 * Patterns view (PR #9).
 *
 * Composes the conversion funnel + per-symbol pattern cards. Both the
 * funnel inputs and the cards source from existing endpoints
 * (``/api/execution/ranking`` + ``/api/execution/market``) — no new
 * backend in this PR. Funnel stage counts are derived purely client-side:
 *
 *   * Universe = ranked-signal count.
 *   * Pattern Found = patterns rows with a non-N/A pattern.
 *   * Qualified (RS>70) = patterns rows with rs > 70.
 *   * Execution Ready = patterns rows with breakout=true && rs > 70.
 */
import { useMemo, useState } from 'react';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import PipelineFunnel, { type FunnelStage } from '@/components/patterns/PipelineFunnel';
import PatternCard from '@/components/patterns/PatternCard';
import { useRanking, usePatterns } from '@/lib/queries';
import type { StockRow } from '@/types/dashboard';

type Filter = 'all' | 'imminent' | 'qualified';

function filterRows(rows: StockRow[], filter: Filter): StockRow[] {
  if (filter === 'imminent') return rows.filter((r) => r.breakout);
  if (filter === 'qualified') return rows.filter((r) => r.rs > 70);
  return rows;
}

function buildStages(universeCount: number, patternRows: StockRow[]): FunnelStage[] {
  const patternFound = patternRows.filter((r) => r.pattern && r.pattern !== 'N/A').length;
  const qualified = patternRows.filter((r) => r.pattern && r.pattern !== 'N/A' && r.rs > 70).length;
  const ready = patternRows.filter(
    (r) => r.pattern && r.pattern !== 'N/A' && r.rs > 70 && r.breakout,
  ).length;
  return [
    { key: 'universe', label: 'Universe', count: universeCount, hint: 'Ranked symbols' },
    { key: 'pattern', label: 'Pattern Found', count: patternFound, hint: 'Active setups' },
    { key: 'qualified', label: 'Qualified (RS>70)', count: qualified, hint: 'Quality filter' },
    { key: 'ready', label: 'Execution Ready', count: ready, hint: 'Breakout confirmed' },
  ];
}

export default function PatternsPage() {
  const patternsQuery = usePatterns();
  const rankingQuery = useRanking();

  const [filter, setFilter] = useState<Filter>('all');
  const [selected, setSelected] = useState<StockRow | null>(null);

  const patternRows = patternsQuery.data?.rows ?? [];
  const universeCount = rankingQuery.data?.rows.length ?? patternRows.length;
  const stages = useMemo(() => buildStages(universeCount, patternRows), [universeCount, patternRows]);
  const visibleRows = useMemo(() => filterRows(patternRows, filter), [patternRows, filter]);

  const isLoading = patternsQuery.isLoading;
  const error = patternsQuery.error;

  return (
    <PageFrame
      title="Patterns"
      description="Pipeline funnel and pattern candidates with urgency, quality, and failure-risk signals."
    >
      <SectionCard
        title="Pipeline Funnel"
        description="From ranked universe to execution-ready candidates."
      >
        {isLoading ? (
          <CardSkeleton />
        ) : error ? (
          <ErrorStateView
            error={`Failed to load patterns: ${error.message}`}
            onRetry={() => patternsQuery.refetch()}
          />
        ) : (
          <PipelineFunnel stages={stages} />
        )}
      </SectionCard>

      <SectionCard title="Pattern Candidates">
        {isLoading ? (
          <CardSkeleton />
        ) : !patternRows.length ? (
          <EmptyState message="No pattern candidates queued." />
        ) : (
          <div className="space-y-4">
            <FilterBar
              filter={filter}
              onChange={setFilter}
              total={patternRows.length}
              matched={visibleRows.length}
            />
            {visibleRows.length === 0 ? (
              <EmptyState message="No pattern candidates match the current filter." />
            ) : (
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                {visibleRows.map((row) => (
                  <PatternCard key={row.symbol} row={row} onSelect={setSelected} />
                ))}
              </div>
            )}
            {selected ? (
              <p className="text-xs text-slate-500">
                Selected: <span className="font-semibold text-slate-300">{selected.symbol}</span> —
                full pattern detail lands in PR #12 (Stock Detail Workspace).
              </p>
            ) : null}
          </div>
        )}
      </SectionCard>
    </PageFrame>
  );
}

interface FilterBarProps {
  filter: Filter;
  onChange: (next: Filter) => void;
  total: number;
  matched: number;
}

const FILTER_OPTIONS: { key: Filter; label: string }[] = [
  { key: 'all', label: 'All Patterns' },
  { key: 'imminent', label: 'Imminent (Breakout)' },
  { key: 'qualified', label: 'Qualified (RS>70)' },
];

function FilterBar({ filter, onChange, total, matched }: FilterBarProps) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div className="flex flex-wrap gap-2">
        {FILTER_OPTIONS.map((opt) => {
          const selected = opt.key === filter;
          return (
            <button
              key={opt.key}
              type="button"
              onClick={() => onChange(opt.key)}
              className={
                'rounded-full border px-3.5 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors ' +
                (selected
                  ? 'border-violet-500/60 bg-violet-500/15 text-violet-200'
                  : 'border-slate-700 bg-slate-900/60 text-slate-300 hover:border-slate-500')
              }
            >
              {opt.label}
            </button>
          );
        })}
      </div>
      <span className="text-xs uppercase tracking-widest text-slate-500">
        {matched} / {total}
      </span>
    </div>
  );
}
