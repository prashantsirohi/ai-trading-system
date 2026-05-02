/**
 * Patterns view (PR #9).
 *
 * Composes the conversion funnel + per-symbol pattern cards. Both the
 * funnel inputs and the cards source from existing endpoints
 * (``/api/execution/ranking`` + ``/api/execution/workspace/pipeline``) — no new
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
import PatternCatalog, { CATALOG } from '@/components/patterns/PatternCatalog';
import { useRanking, usePatterns } from '@/lib/queries';
import { useWorkspace } from '@/components/workspace/WorkspaceContext';
import type { StockRow } from '@/types/dashboard';

type Filter = 'all' | 'imminent' | 'qualified';

function filterRows(rows: StockRow[], filter: Filter, catalogKey: string | null): StockRow[] {
  let out = rows;
  if (catalogKey) {
    const entry = CATALOG.find((e) => e.key === catalogKey);
    if (entry) {
      out = out.filter(
        (r) =>
          r.pattern &&
          r.pattern !== 'N/A' &&
          entry.matches.some((m) => r.pattern.toLowerCase().includes(m)),
      );
    }
  }
  if (filter === 'imminent') return out.filter((r) => r.breakout);
  if (filter === 'qualified') return out.filter((r) => r.rs > 70);
  return out;
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
  const { openWorkspace } = useWorkspace();

  const [filter, setFilter] = useState<Filter>('all');
  const [catalogKey, setCatalogKey] = useState<string | null>(null);
  const [selected, setSelected] = useState<StockRow | null>(null);

  const handleSelect = (row: StockRow) => {
    setSelected(row);
    openWorkspace(row.symbol);
  };

  const patternRows = patternsQuery.data?.rows ?? [];
  const universeCount = rankingQuery.data?.rows.length ?? patternRows.length;
  const stages = useMemo(() => buildStages(universeCount, patternRows), [universeCount, patternRows]);
  const visibleRows = useMemo(
    () => filterRows(patternRows, filter, catalogKey),
    [patternRows, filter, catalogKey],
  );

  const isLoading = patternsQuery.isLoading;
  const error = patternsQuery.error;

  return (
    <PageFrame
      title="Patterns"
      description="Setups by funnel stage, quality, and urgency."
    >
      <SectionCard
        title="Funnel"
        description="Ranked universe to execution-ready candidates."
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

      <SectionCard
        title="Catalog"
        description="Select a setup family to filter active candidates."
      >
        <PatternCatalog
          rows={patternRows}
          activeKey={catalogKey}
          onSelect={setCatalogKey}
        />
      </SectionCard>

      <SectionCard title="Active Setups">
        {isLoading ? (
          <CardSkeleton />
        ) : !patternRows.length ? (
          <EmptyState message="No pattern candidates available — run the pipeline first." />
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
              <>
                <PatternRowsTable rows={visibleRows} onSelect={handleSelect} />
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                  {visibleRows.map((row) => (
                    <PatternCard key={row.symbol} row={row} onSelect={handleSelect} />
                  ))}
                </div>
              </>
            )}
            {selected ? (
              <p className="text-xs text-slate-500">
                Selected: <span className="font-semibold text-slate-300">{selected.symbol}</span> —
                pattern history opens in the Stock Detail Workspace.
              </p>
            ) : null}
          </div>
        )}
      </SectionCard>
    </PageFrame>
  );
}

function PatternRowsTable({
  rows,
  onSelect,
}: {
  rows: StockRow[];
  onSelect: (row: StockRow) => void;
}) {
  return (
    <div className="overflow-x-auto rounded-lg border border-slate-800">
      <table className="w-full min-w-[900px] text-left text-sm">
        <thead className="border-b border-slate-800 bg-slate-950/70 text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th className="px-3 py-2 font-semibold">Symbol</th>
            <th className="px-3 py-2 font-semibold">Pattern family</th>
            <th className="px-3 py-2 font-semibold">State</th>
            <th className="px-3 py-2 font-semibold">Setup quality</th>
            <th className="px-3 py-2 font-semibold">Pivot</th>
            <th className="px-3 py-2 font-semibold">Invalidation</th>
            <th className="px-3 py-2 font-semibold">Warnings</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={row.symbol}
              className="cursor-pointer border-b border-slate-800 last:border-b-0 hover:bg-slate-800/40"
              onClick={() => onSelect(row)}
            >
              <td className="px-3 py-2 font-semibold text-slate-100">{row.symbol}</td>
              <td className="px-3 py-2 text-slate-200">{row.pattern || '—'}</td>
              <td className="px-3 py-2 text-slate-300">{row.patternState ?? '—'}</td>
              <td className="px-3 py-2 tabular-nums text-slate-300">
                {row.setupQuality == null ? '—' : row.setupQuality.toFixed(1)}
              </td>
              <td className="px-3 py-2 tabular-nums text-slate-300">
                {row.pivotPrice == null ? '—' : row.pivotPrice.toFixed(2)}
              </td>
              <td className="px-3 py-2 tabular-nums text-slate-300">
                {row.invalidationPrice == null ? '—' : row.invalidationPrice.toFixed(2)}
              </td>
              <td className="px-3 py-2">
                <div className="flex flex-wrap gap-1">
                  {row.reclaimSignal ? (
                    <span className="rounded-full border border-emerald-500/40 bg-emerald-500/15 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-emerald-200">
                      Reclaim
                    </span>
                  ) : null}
                  {(row.distanceFromPivotAtr ?? 0) >= 2 ? (
                    <span className="rounded-full border border-amber-500/40 bg-amber-500/15 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-amber-200">
                      Pivot extended
                    </span>
                  ) : null}
                  {!row.reclaimSignal && (row.distanceFromPivotAtr ?? 0) < 2 ? (
                    <span className="text-slate-500">—</span>
                  ) : null}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
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
