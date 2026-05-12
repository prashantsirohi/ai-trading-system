/**
 * Research page — Performance Tracker view (Phase 0 of the feedback loop).
 *
 * Mirrors the four sections of the weekly markdown digest:
 *
 *   1. Coverage strip       — date range, # ranking dates, # rows
 *   2. Cohort returns       — top-10 / top-50 / top-200 / 51-200 / 201+
 *   3. Bucket attribution   — TRIGGERED_TODAY / CORE_MOMENTUM / EARLY_STAGE2 / …
 *   4. Factor IC            — Spearman vs fwd-20d return across 30 / 90 / 180 day windows
 *   5. Drift watch          — banner when 30d IC has fallen > 30% vs 180d baseline
 *
 * Data source: ``rank_cohort_performance`` in data/research.duckdb.
 * Backend route: src/ai_trading_system/ui/execution_api/routes/perf_tracker.py.
 */

import { useState } from 'react';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import {
  usePerfCoverage,
  usePerfCohorts,
  usePerfBuckets,
  usePerfFactorIc,
  usePerfDrift,
} from '@/lib/queries';

const LOOKBACK_OPTIONS = [
  { label: '30d', value: 30 },
  { label: '90d', value: 90 },
  { label: '180d', value: 180 },
  { label: 'All', value: 0 },
] as const;

const IC_WINDOWS = [30, 90, 180];

function fmtNum(value: number | null | undefined, suffix = ''): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${value.toFixed(2)}${suffix}`;
}

function fmtPct(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return `${value.toFixed(1)}%`;
}

function fmtInt(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—';
  return value.toLocaleString();
}

interface LookbackPickerProps {
  value: number;
  onChange: (v: number) => void;
}

function LookbackPicker({ value, onChange }: LookbackPickerProps) {
  return (
    <div className="inline-flex rounded-md border border-slate-700 bg-slate-900 text-xs">
      {LOOKBACK_OPTIONS.map((opt) => (
        <button
          key={opt.value}
          type="button"
          onClick={() => onChange(opt.value)}
          className={
            'px-3 py-1 transition-colors ' +
            (value === opt.value
              ? 'bg-slate-700 text-white'
              : 'text-slate-300 hover:bg-slate-800')
          }
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}

export default function ResearchPage() {
  const [lookback, setLookback] = useState<number>(90);

  const coverageQ = usePerfCoverage();
  const cohortsQ = usePerfCohorts(lookback);
  const bucketsQ = usePerfBuckets(lookback);
  const factorIcQ = usePerfFactorIc(IC_WINDOWS);
  const driftQ = usePerfDrift();

  const flagged = driftQ.data?.flagged ?? [];

  return (
    <PageFrame
      title="Research — Performance Tracker"
      description="Forward-return cohorts, bucket attribution, and factor information coefficient drawn from rank_cohort_performance."
      compactHeader
    >
      <SectionCard
        title="Coverage"
        description="Date range and row count of the underlying rank cohort table."
      >
        {coverageQ.isLoading ? (
          <CardSkeleton />
        ) : coverageQ.error ? (
          <ErrorStateView
            error={`Failed to load coverage: ${coverageQ.error.message}`}
            onRetry={() => coverageQ.refetch()}
          />
        ) : !coverageQ.data || coverageQ.data.rows === 0 ? (
          <EmptyState message="No rows in rank_cohort_performance yet. Run the pipeline to populate." />
        ) : (
          <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
            <CoverageStat label="First date" value={coverageQ.data.first_date ?? '—'} />
            <CoverageStat label="Last date" value={coverageQ.data.last_date ?? '—'} />
            <CoverageStat label="Ranking dates" value={fmtInt(coverageQ.data.dates)} />
            <CoverageStat label="(date, symbol) rows" value={fmtInt(coverageQ.data.rows)} />
          </div>
        )}
      </SectionCard>

      {flagged.length > 0 ? (
        <div className="rounded-lg border border-amber-700 bg-amber-950/60 px-4 py-3 text-sm text-amber-200">
          <strong>Drift watch:</strong> {flagged.length} factor
          {flagged.length === 1 ? '' : 's'} with 30-day IC down &gt;30% vs 180-day baseline
          {' ('}
          {flagged.map((f) => f.factor).join(', ')}
          {').'}
        </div>
      ) : null}

      <SectionCard
        title="Cohort forward returns"
        description="Top-N picks should outperform the rest. If top-10 avg_20d is indistinguishable from 201+, the ranking isn't discriminating."
      >
        <div className="mb-3">
          <LookbackPicker value={lookback} onChange={setLookback} />
        </div>
        {cohortsQ.isLoading ? (
          <CardSkeleton />
        ) : cohortsQ.error ? (
          <ErrorStateView
            error={`Failed to load cohorts: ${cohortsQ.error.message}`}
            onRetry={() => cohortsQ.refetch()}
          />
        ) : (cohortsQ.data?.cohorts ?? []).length === 0 ? (
          <EmptyState message="No cohort rows in window." />
        ) : (
          <DataTable
            headers={['Cohort', 'n', 'n_20d', 'avg_5d', 'avg_10d', 'avg_20d', 'avg_60d', 'hit_5d', 'hit_20d']}
            rows={(cohortsQ.data?.cohorts ?? []).map((r) => [
              r.cohort,
              fmtInt(r.n_total),
              fmtInt(r.n_20d),
              fmtNum(r.avg_5d, '%'),
              fmtNum(r.avg_10d, '%'),
              fmtNum(r.avg_20d, '%'),
              fmtNum(r.avg_60d, '%'),
              fmtPct(r.hitrate_5d),
              fmtPct(r.hitrate_20d),
            ])}
          />
        )}
      </SectionCard>

      <SectionCard
        title="Bucket attribution"
        description="Phase 5 watchlist taxonomy: TRIGGERED_TODAY 5d return should lead CORE_MOMENTUM. Older rows are 'unassigned' — sparse until Phase 5-aware runs accumulate."
      >
        {bucketsQ.isLoading ? (
          <CardSkeleton />
        ) : bucketsQ.error ? (
          <ErrorStateView
            error={`Failed to load buckets: ${bucketsQ.error.message}`}
            onRetry={() => bucketsQ.refetch()}
          />
        ) : (bucketsQ.data?.buckets ?? []).length === 0 ? (
          <EmptyState message="No bucket rows in window." />
        ) : (
          <DataTable
            headers={['Bucket', 'n', 'n_20d', 'avg_5d', 'avg_10d', 'avg_20d', 'hit_5d', 'hit_20d']}
            rows={(bucketsQ.data?.buckets ?? []).map((r) => [
              r.bucket,
              fmtInt(r.n),
              fmtInt(r.n_20d),
              fmtNum(r.avg_5d, '%'),
              fmtNum(r.avg_10d, '%'),
              fmtNum(r.avg_20d, '%'),
              fmtPct(r.hitrate_5d),
              fmtPct(r.hitrate_20d),
            ])}
          />
        )}
      </SectionCard>

      <SectionCard
        title="Factor information coefficient (Spearman vs fwd-20d)"
        description="Higher IC = factor doing real predictive work. Drops between windows indicate decay."
      >
        {factorIcQ.isLoading ? (
          <CardSkeleton />
        ) : factorIcQ.error ? (
          <ErrorStateView
            error={`Failed to load factor IC: ${factorIcQ.error.message}`}
            onRetry={() => factorIcQ.refetch()}
          />
        ) : (factorIcQ.data?.factors ?? []).length === 0 ? (
          <EmptyState message="No factor data yet." />
        ) : (
          <DataTable
            headers={['Factor', ...IC_WINDOWS.map((w) => `ic_${w}d`), ...IC_WINDOWS.map((w) => `n_${w}d`)]}
            rows={(factorIcQ.data?.factors ?? []).map((r) => {
              const driftRow = (driftQ.data?.factors ?? []).find((d) => d.factor === r.factor);
              const isFlagged = driftRow?.alert ?? false;
              const cells: (string | { text: string; className: string })[] = [
                isFlagged
                  ? { text: `${r.factor} ⚠`, className: 'text-amber-400 font-semibold' }
                  : r.factor,
              ];
              const bag = r as unknown as Record<string, number | null>;
              for (const w of IC_WINDOWS) {
                cells.push(fmtNum(bag[`ic_${w}d`]));
              }
              for (const w of IC_WINDOWS) {
                cells.push(fmtInt(bag[`n_${w}d`]));
              }
              return cells;
            })}
          />
        )}
      </SectionCard>
    </PageFrame>
  );
}

function CoverageStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-slate-800 bg-slate-950/60 px-3 py-2">
      <div className="text-[11px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-0.5 text-sm font-semibold text-slate-100">{value}</div>
    </div>
  );
}

interface DataTableProps {
  headers: string[];
  rows: Array<Array<string | { text: string; className: string }>>;
}

function DataTable({ headers, rows }: DataTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead className="text-left text-slate-400">
          <tr>
            {headers.map((h) => (
              <th key={h} className="border-b border-slate-800 px-3 py-2 font-medium">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri} className="text-slate-200 hover:bg-slate-900/60">
              {row.map((cell, ci) => {
                if (typeof cell === 'string') {
                  return (
                    <td
                      key={ci}
                      className="border-b border-slate-900 px-3 py-1.5 font-mono tabular-nums"
                    >
                      {cell}
                    </td>
                  );
                }
                return (
                  <td
                    key={ci}
                    className={
                      'border-b border-slate-900 px-3 py-1.5 font-mono tabular-nums ' +
                      cell.className
                    }
                  >
                    {cell.text}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
