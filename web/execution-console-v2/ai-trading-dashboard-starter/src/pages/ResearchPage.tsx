/**
 * Research page — Performance Tracker (simplified UI).
 *
 * Top-of-page: three verdict cards (top-10 edge, drift alerts, bucket health)
 * + three bar charts (cohort returns, bucket excess vs control, top-200 IC).
 * Detailed tables live behind `<details>` disclosures so the page reads at
 * a glance but power users can still drill in.
 *
 * Backend: src/ai_trading_system/ui/execution_api/routes/perf_tracker.py
 */

import { useMemo, useState, type ReactNode } from 'react';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import StatusBadge, { type StatusTone } from '@/components/common/StatusBadge';
import {
  CohortBarChart,
  BucketExcessBarChart,
  Top200IcBarChart,
} from '@/components/perf-tracker/PerfTrackerCharts';
import {
  usePerfCoverage,
  usePerfCohorts,
  usePerfBuckets,
  usePerfBucketCoverage,
  usePerfSameDateBuckets,
  usePerfFactorIc,
  usePerfConditionalFactorIc,
  usePerfFactorCoverage,
  usePerfDrift,
  usePerfBucketComposition,
  usePerfConcentration,
  usePerfDigestList,
  usePerfDigestDoc,
} from '@/lib/queries';
import type {
  ConcentrationSignal,
  FactorCoverageStatus,
  DriftStatus,
} from '@/lib/api/perfTracker';

const LOOKBACK_OPTIONS = [
  { label: '30d', value: 30 },
  { label: '90d', value: 90 },
  { label: '180d', value: 180 },
  { label: 'All', value: 0 },
] as const;

const IC_WINDOWS = [30, 90, 180];
const COND_IC_WINDOWS = [90];

// --------------------------------------------------------------------------
// Formatters
// --------------------------------------------------------------------------

function fmtNum(value: number | null | undefined, suffix = ''): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return `${value.toFixed(2)}${suffix}`;
}

function fmtPct(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return `${value.toFixed(1)}%`;
}

function fmtPctFromFraction(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return `${(value * 100).toFixed(1)}%`;
}

function fmtInt(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—';
  return value.toLocaleString();
}

function fmtStatus(value: string | null | undefined): string {
  if (value === 'insufficient_sample') return 'Insufficient sample';
  if (value === 'unreliable_coverage') return 'Unreliable coverage';
  if (value === 'no_baseline') return 'No baseline';
  if (value === 'watch') return 'Watch';
  if (value === 'warning') return 'Warning';
  if (value === 'critical') return 'Critical';
  if (value === 'ok') return 'OK';
  return '—';
}

function driftClass(status: DriftStatus | string | undefined): string {
  if (status === 'critical') return 'text-red-300 font-semibold';
  if (status === 'warning') return 'text-amber-300 font-semibold';
  if (status === 'watch') return 'text-yellow-300';
  if (status === 'insufficient_sample' || status === 'unreliable_coverage') return 'text-slate-400';
  return 'text-slate-200';
}

function coverageToneFromStatus(status: FactorCoverageStatus | string | undefined): StatusTone {
  if (status === 'ok') return 'good';
  if (status === 'partial_coverage') return 'warn';
  if (status === 'poor_coverage' || status === 'not_wired') return 'bad';
  return 'neutral';
}

function signalTone(signal: ConcentrationSignal | string | undefined): StatusTone {
  if (signal === 'strong') return 'good';
  if (signal === 'mixed') return 'warn';
  if (signal === 'weak') return 'bad';
  return 'neutral';
}

function bucketRowClass(avg5d: number | null | undefined, hit5d: number | null | undefined): string {
  if (avg5d !== null && avg5d !== undefined && hit5d !== null && hit5d !== undefined) {
    if (avg5d < 0 && hit5d < 40) return 'text-red-300 font-semibold';
  }
  return '';
}

// --------------------------------------------------------------------------
// Small primitives
// --------------------------------------------------------------------------

const TONE_CARD_BG: Record<StatusTone, string> = {
  good: 'border-emerald-700/60 bg-emerald-950/40',
  warn: 'border-amber-700/60 bg-amber-950/40',
  bad: 'border-rose-700/60 bg-rose-950/40',
  neutral: 'border-slate-700 bg-slate-900',
};

const TONE_VALUE: Record<StatusTone, string> = {
  good: 'text-emerald-300',
  warn: 'text-amber-300',
  bad: 'text-rose-300',
  neutral: 'text-slate-200',
};

function VerdictCard({
  label,
  value,
  subtitle,
  tone,
}: {
  label: string;
  value: string;
  subtitle?: string;
  tone: StatusTone;
}) {
  return (
    <div className={`rounded-2xl border p-5 shadow-soft ${TONE_CARD_BG[tone]}`}>
      <div className="text-xs uppercase tracking-wide text-slate-400">{label}</div>
      <div className={`mt-2 text-3xl font-semibold ${TONE_VALUE[tone]}`}>{value}</div>
      {subtitle ? <div className="mt-1 text-xs text-slate-400">{subtitle}</div> : null}
    </div>
  );
}

function RawDataDetails({
  label = 'Show raw data',
  children,
}: {
  label?: string;
  children: ReactNode;
}) {
  return (
    <details className="group mt-3 rounded-md border border-slate-800 bg-slate-950/40">
      <summary className="cursor-pointer list-none px-3 py-2 text-xs text-slate-400 hover:text-slate-200">
        <span className="mr-1 inline-block transition-transform group-open:rotate-90">▶</span>
        {label}
      </summary>
      <div className="border-t border-slate-800 p-3">{children}</div>
    </details>
  );
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

// --------------------------------------------------------------------------
// Page
// --------------------------------------------------------------------------

export default function ResearchPage() {
  const [lookback, setLookback] = useState<number>(90);
  const [selectedDigest, setSelectedDigest] = useState<string | null>(null);

  const coverageQ = usePerfCoverage();
  const cohortsQ = usePerfCohorts(lookback);
  const bucketsQ = usePerfBuckets(lookback);
  const bucketCoverageQ = usePerfBucketCoverage();
  const sameDateBucketsQ = usePerfSameDateBuckets(lookback);
  const compositionQ = usePerfBucketComposition();
  const factorIcQ = usePerfFactorIc(IC_WINDOWS);
  const conditionalIcQ = usePerfConditionalFactorIc(COND_IC_WINDOWS);
  const factorCoverageQ = usePerfFactorCoverage();
  const driftQ = usePerfDrift();
  const concentrationQ = usePerfConcentration(lookback);
  const digestListQ = usePerfDigestList();
  const effectiveDigest = selectedDigest ?? digestListQ.data?.digests[0]?.filename ?? null;
  const digestDocQ = usePerfDigestDoc(effectiveDigest);

  const drift = driftQ.data;
  const conc = concentrationQ.data;
  const sameDate = sameDateBucketsQ.data;
  const conditionalIc = conditionalIcQ.data;
  const factorCoverage = factorCoverageQ.data;

  // ---- Headline verdicts ----------------------------------------------------

  const concSignal = (conc?.signal ?? 'unknown') as ConcentrationSignal;
  const top10EdgeTone = signalTone(concSignal);
  const top10EdgeValue =
    concSignal === 'unknown' ? '—' : concSignal.toUpperCase();
  const top10Subtitle = conc?.message ?? '';

  const flagged = drift?.flagged ?? [];
  const insufficient = (drift?.factors ?? []).filter((f) => f.status === 'insufficient_sample');
  const unreliable = (drift?.factors ?? []).filter((f) => f.status === 'unreliable_coverage');
  const driftTone: StatusTone =
    flagged.length > 0 ? 'bad' : insufficient.length + unreliable.length > 0 ? 'warn' : 'good';
  const driftValue = `${flagged.length}`;
  const driftSubtitle = (() => {
    const parts: string[] = [];
    if (insufficient.length) parts.push(`${insufficient.length} insufficient`);
    if (unreliable.length) parts.push(`${unreliable.length} unreliable`);
    return parts.length ? parts.join(', ') : 'No drift alerts';
  })();

  const eligibleBuckets = (sameDate?.buckets ?? []).filter(
    (b) => b.bucket !== 'unassigned' && !b.small_sample,
  );
  const negativeBuckets = eligibleBuckets.filter(
    (b) => (b.avg_5d ?? 0) < 0 && (b.hitrate_5d ?? 0) < 40,
  );
  const bucketTone: StatusTone =
    negativeBuckets.length >= 2 ? 'bad' : negativeBuckets.length === 1 ? 'warn' : 'good';
  const bucketValue = `${negativeBuckets.length} / ${eligibleBuckets.length}`;
  const bucketSubtitle = negativeBuckets.length
    ? `Weak: ${negativeBuckets.map((b) => b.bucket).join(', ')}`
    : 'All eligible buckets non-negative';

  // ---- Chart data -----------------------------------------------------------

  const cohortChartRows = (conc?.cohorts ?? []).map((c) => ({
    cohort: c.cohort,
    avg_20d: c.avg_20d,
  }));
  const referenceTop200 = conc?.top200_avg_20d ?? null;

  const bucketExcessRows = (sameDate?.buckets ?? [])
    .filter((b) => b.bucket !== 'unassigned')
    .map((b) => ({
      bucket: b.bucket,
      excess_20d: b.excess_20d,
      small_sample: b.small_sample,
    }));

  const top200IcRows = (conditionalIc?.factors ?? []).map((r) => {
    const bag = r as Record<string, string | number | null>;
    return {
      factor: String(r.factor),
      ic_5d: (bag[`ic_5d_${COND_IC_WINDOWS[0]}w_top_200_only`] as number | null) ?? null,
      ic_10d: (bag[`ic_10d_${COND_IC_WINDOWS[0]}w_top_200_only`] as number | null) ?? null,
      ic_20d: (bag[`ic_20d_${COND_IC_WINDOWS[0]}w_top_200_only`] as number | null) ?? null,
    };
  });

  const compositionColumns = useMemo(() => {
    const all = compositionQ.data?.composition[0];
    if (!all) return [] as string[];
    return Object.keys(all).filter((k) => k.startsWith('avg_') && k !== 'avg_rank_position');
  }, [compositionQ.data]);

  return (
    <PageFrame
      title="Research — Performance Tracker"
      description="Verdict-first view of forward-return cohorts, bucket attribution, and factor IC."
      compactHeader
    >
      {/* Observational banner */}
      <div className="rounded-lg border border-slate-700 bg-slate-900/80 px-4 py-3 text-sm text-slate-200">
        <strong>Performance Tracker is observational only.</strong>{' '}
        It does not change ranking weights, paper trading rules, or production configs.
      </div>

      {/* 1. Verdict row */}
      <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
        <VerdictCard
          label="Top-10 edge"
          value={top10EdgeValue}
          subtitle={top10Subtitle}
          tone={top10EdgeTone}
        />
        <VerdictCard
          label="Drift alerts"
          value={driftValue}
          subtitle={driftSubtitle}
          tone={driftTone}
        />
        <VerdictCard
          label="Bucket health"
          value={bucketValue}
          subtitle={bucketSubtitle}
          tone={bucketTone}
        />
      </div>

      {/* Lookback controls (drive cohort + concentration + same-date) */}
      <div className="flex items-center justify-end gap-2 text-xs text-slate-400">
        <span>Window:</span>
        <LookbackPicker value={lookback} onChange={setLookback} />
      </div>

      {/* 2. Concentration: avg_20d by cohort */}
      <SectionCard
        title="Top-N edge"
        description="Average 20-day forward return by rank band. If top-10 is barely above 201+, top-10 isn't earning its concentration risk."
      >
        {concentrationQ.isLoading ? (
          <CardSkeleton />
        ) : concentrationQ.error ? (
          <ErrorStateView
            error={`Failed to load concentration: ${concentrationQ.error.message}`}
            onRetry={() => concentrationQ.refetch()}
          />
        ) : !conc || conc.cohorts.length === 0 ? (
          <EmptyState message="No concentration data in window." />
        ) : (
          <>
            <CohortBarChart rows={cohortChartRows} referenceValue={referenceTop200} />
            <div className="mt-3 flex items-center gap-3 text-xs text-slate-400">
              <StatusBadge status={null} tone={top10EdgeTone} label={`Signal: ${top10EdgeValue}`} />
              <span>{conc.message}</span>
            </div>
            <RawDataDetails>
              <DataTable
                headers={['Cohort', 'n', 'avg_5d', 'avg_10d', 'avg_20d', 'hit_20d', 'Δ vs top-200', 'Δ vs 201+']}
                rows={conc.cohorts.map((r) => [
                  r.cohort,
                  fmtInt(r.n),
                  fmtNum(r.avg_5d, '%'),
                  fmtNum(r.avg_10d, '%'),
                  fmtNum(r.avg_20d, '%'),
                  fmtPct(r.hitrate_20d),
                  fmtNum(r.delta_vs_top_200, '%'),
                  fmtNum(r.delta_vs_201_plus, '%'),
                ])}
              />
            </RawDataDetails>
          </>
        )}
      </SectionCard>

      {/* 3. Bucket attribution: excess_20d vs same-date control */}
      <SectionCard
        title="Bucket excess vs control"
        description="Same-date attribution — each bucket's avg_20d minus the average across dates where any bucket was assigned. Amber bars indicate small samples (directional only)."
      >
        {sameDateBucketsQ.isLoading ? (
          <CardSkeleton />
        ) : sameDateBucketsQ.error ? (
          <ErrorStateView
            error={`Failed to load same-date buckets: ${sameDateBucketsQ.error.message}`}
            onRetry={() => sameDateBucketsQ.refetch()}
          />
        ) : bucketExcessRows.length === 0 ? (
          <EmptyState message="No labelled buckets in window." />
        ) : (
          <>
            <BucketExcessBarChart rows={bucketExcessRows} />
            <RawDataDetails>
              <DataTable
                headers={['Bucket', 'n', 'trading days', 'avg_5d', 'avg_20d', 'ctrl_20d', 'excess_5d', 'excess_20d', 'hit_20d', 'sample']}
                rows={(sameDate?.buckets ?? []).map((r) => {
                  const cls = bucketRowClass(r.avg_5d, r.hitrate_5d);
                  const cell = (text: string) => (cls ? { text, className: cls } : text);
                  const sample: string | { text: string; className: string } = r.small_sample
                    ? { text: 'Small — directional only', className: 'text-amber-300' }
                    : 'ok';
                  return [
                    cell(r.bucket),
                    cell(fmtInt(r.n)),
                    cell(fmtInt(r.trading_days)),
                    cell(fmtNum(r.avg_5d, '%')),
                    cell(fmtNum(r.avg_20d, '%')),
                    cell(fmtNum(r.control_avg_20d, '%')),
                    cell(fmtNum(r.excess_5d, '%')),
                    cell(fmtNum(r.excess_20d, '%')),
                    cell(fmtPct(r.hitrate_20d)),
                    sample,
                  ];
                })}
              />
            </RawDataDetails>
          </>
        )}
      </SectionCard>

      {/* 4. Top-200 IC chart */}
      <SectionCard
        title="In-universe factor IC (top-200)"
        description="Spearman IC inside top-200 across 5/10/20-day horizons. If these bars are near zero, no weight tweak rescues top-10 selection."
      >
        {conditionalIcQ.isLoading ? (
          <CardSkeleton />
        ) : conditionalIcQ.error ? (
          <ErrorStateView
            error={`Failed to load conditional IC: ${conditionalIcQ.error.message}`}
            onRetry={() => conditionalIcQ.refetch()}
          />
        ) : top200IcRows.length === 0 ? (
          <EmptyState message="No conditional IC rows yet." />
        ) : (
          <>
            <Top200IcBarChart rows={top200IcRows} />
            <RawDataDetails>
              <DataTable
                headers={['Factor', 'top200 ic_5d', 'top200 ic_10d', 'top200 ic_20d', 'top200 n', 'full ic_20d', '201+ ic_20d']}
                rows={(conditionalIc?.factors ?? []).map((r) => {
                  const bag = r as Record<string, string | number | null>;
                  return [
                    String(r.factor),
                    fmtNum(bag[`ic_5d_${COND_IC_WINDOWS[0]}w_top_200_only`] as number | null),
                    fmtNum(bag[`ic_10d_${COND_IC_WINDOWS[0]}w_top_200_only`] as number | null),
                    fmtNum(bag[`ic_20d_${COND_IC_WINDOWS[0]}w_top_200_only`] as number | null),
                    fmtInt(bag[`n_20d_${COND_IC_WINDOWS[0]}w_top_200_only`] as number | null),
                    fmtNum(bag[`ic_20d_${COND_IC_WINDOWS[0]}w_full_universe`] as number | null),
                    fmtNum(bag[`ic_20d_${COND_IC_WINDOWS[0]}w_rank_201_plus_only`] as number | null),
                  ];
                })}
              />
            </RawDataDetails>
          </>
        )}
      </SectionCard>

      {/* 5. Factor coverage pill strip */}
      <SectionCard
        title="Factor coverage"
        description="One pill per factor. Red = not wired or poor coverage; amber = partial; green = ok."
      >
        {factorCoverageQ.isLoading ? (
          <CardSkeleton />
        ) : factorCoverageQ.error ? (
          <ErrorStateView
            error={`Failed to load factor coverage: ${factorCoverageQ.error.message}`}
            onRetry={() => factorCoverageQ.refetch()}
          />
        ) : (factorCoverage?.factors ?? []).length === 0 ? (
          <EmptyState message="No factor coverage rows yet." />
        ) : (
          <>
            <div className="flex flex-wrap gap-2">
              {(factorCoverage?.factors ?? []).map((f) => (
                <StatusBadge
                  key={f.factor}
                  status={null}
                  tone={coverageToneFromStatus(f.status)}
                  label={`${f.factor} · ${fmtPct(f.coverage_pct ?? null)}`}
                />
              ))}
            </div>
            <RawDataDetails>
              <DataTable
                headers={['Factor', 'Status', 'Coverage', 'Non-null', 'First', 'Last']}
                rows={(factorCoverage?.factors ?? []).map((r) => [
                  { text: r.factor, className: '' },
                  { text: r.status ?? '—', className: '' },
                  fmtPct(r.coverage_pct ?? null),
                  fmtInt(r.non_null_count),
                  r.first_available_date ?? '—',
                  r.last_available_date ?? '—',
                ])}
              />
            </RawDataDetails>
          </>
        )}
      </SectionCard>

      {/* Advanced diagnostics — everything else */}
      <details className="group rounded-lg border border-slate-800 bg-slate-950/40">
        <summary className="cursor-pointer list-none px-4 py-3 text-sm text-slate-300 hover:text-white">
          <span className="mr-2 inline-block transition-transform group-open:rotate-90">▶</span>
          Advanced diagnostics
        </summary>
        <div className="space-y-4 border-t border-slate-800 p-4">
          {/* Tracker health */}
          <SectionCard
            title="Tracker health"
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
              <EmptyState message="No rows in rank_cohort_performance yet." />
            ) : (
              <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
                <CoverageStat label="First date" value={coverageQ.data.first_date ?? '—'} />
                <CoverageStat label="Last date" value={coverageQ.data.last_date ?? '—'} />
                <CoverageStat label="Ranking dates" value={fmtInt(coverageQ.data.dates)} />
                <CoverageStat label="(date, symbol) rows" value={fmtInt(coverageQ.data.rows)} />
              </div>
            )}
          </SectionCard>

          {/* Cohort table (full) */}
          <SectionCard title="Cohort forward returns (full)" description="">
            {cohortsQ.isLoading ? (
              <CardSkeleton />
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

          {/* Plain bucket attribution */}
          <SectionCard
            title="Bucket attribution (raw)"
            description="Plain bucket-level returns without same-date control. Use Bucket excess (above) for the apples-to-apples view."
          >
            {bucketsQ.isLoading ? (
              <CardSkeleton />
            ) : (bucketsQ.data?.buckets ?? []).length === 0 ? (
              <EmptyState message="No bucket rows in window." />
            ) : (
              <DataTable
                headers={['Bucket', 'n', 'n_20d', 'avg_5d', 'avg_10d', 'avg_20d', 'hit_5d', 'hit_20d']}
                rows={(bucketsQ.data?.buckets ?? []).map((r) => {
                  const cls = bucketRowClass(r.avg_5d, r.hitrate_5d);
                  const cell = (text: string) => (cls ? { text, className: cls } : text);
                  return [
                    cell(r.bucket),
                    cell(fmtInt(r.n)),
                    cell(fmtInt(r.n_20d)),
                    cell(fmtNum(r.avg_5d, '%')),
                    cell(fmtNum(r.avg_10d, '%')),
                    cell(fmtNum(r.avg_20d, '%')),
                    cell(fmtPct(r.hitrate_5d)),
                    cell(fmtPct(r.hitrate_20d)),
                  ];
                })}
              />
            )}
          </SectionCard>

          {/* Bucket coverage */}
          <SectionCard
            title="Bucket coverage"
            description="Date + symbol coverage per bucket. Use to separate true bucket weakness from sparse history."
          >
            {bucketCoverageQ.isLoading ? (
              <CardSkeleton />
            ) : (bucketCoverageQ.data?.buckets ?? []).length === 0 ? (
              <EmptyState message="No bucket coverage rows yet." />
            ) : (
              <DataTable
                headers={['Bucket', 'First', 'Last', 'Rows', 'Dates', 'Symbols', '% rows', '% w/fwd5', '% w/fwd20']}
                rows={(bucketCoverageQ.data?.buckets ?? []).map((r) => [
                  r.bucket,
                  r.first_date ?? '—',
                  r.last_date ?? '—',
                  fmtInt(r.rows),
                  fmtInt(r.dates),
                  fmtInt(r.symbols_count),
                  fmtPctFromFraction(r.pct_of_all_rows),
                  fmtPctFromFraction(r.pct_with_fwd_5d),
                  fmtPctFromFraction(r.pct_with_fwd_20d),
                ])}
              />
            )}
          </SectionCard>

          {/* Bucket composition */}
          <SectionCard
            title="Bucket composition"
            description="Average factor state at assignment time per bucket."
          >
            {compositionQ.isLoading ? (
              <CardSkeleton />
            ) : (compositionQ.data?.composition ?? []).length === 0 ? (
              <EmptyState message="No bucket composition rows yet." />
            ) : (
              <>
                {compositionQ.data && compositionQ.data.missing_columns.length > 0 ? (
                  <div className="mb-2 text-xs text-slate-400">
                    Missing columns (shown as —): {compositionQ.data.missing_columns.join(', ')}
                  </div>
                ) : null}
                <DataTable
                  headers={['Bucket', 'n', 'avg_rank', ...compositionColumns.map((c) => c.replace('avg_', ''))]}
                  rows={(compositionQ.data?.composition ?? []).map((r) => {
                    const cells: Array<string | { text: string; className: string }> = [
                      r.bucket,
                      fmtInt(r.n),
                      fmtNum(r.avg_rank_position),
                    ];
                    for (const col of compositionColumns) {
                      const v = r[col as keyof typeof r];
                      cells.push(fmtNum(typeof v === 'number' ? v : null));
                    }
                    return cells;
                  })}
                />
              </>
            )}
          </SectionCard>

          {/* Factor IC across windows */}
          <SectionCard
            title="Factor IC across windows"
            description="30d / 90d / 180d Spearman IC vs fwd_20d. Drift-flagged factors are labelled."
          >
            {factorIcQ.isLoading ? (
              <CardSkeleton />
            ) : (factorIcQ.data?.factors ?? []).length === 0 ? (
              <EmptyState message="No factor data yet." />
            ) : (
              <DataTable
                headers={['Factor', ...IC_WINDOWS.map((w) => `ic_${w}d`), ...IC_WINDOWS.map((w) => `n_${w}d`)]}
                rows={(factorIcQ.data?.factors ?? []).map((r) => {
                  const driftRow = (drift?.factors ?? []).find((d) => d.factor === r.factor);
                  const isFlagged = driftRow?.alert ?? false;
                  const cells: (string | { text: string; className: string })[] = [
                    isFlagged
                      ? { text: `${r.factor} DRIFT`, className: 'text-amber-400 font-semibold' }
                      : r.factor,
                  ];
                  const bag = r as unknown as Record<string, number | null>;
                  for (const w of IC_WINDOWS) cells.push(fmtNum(bag[`ic_${w}d`]));
                  for (const w of IC_WINDOWS) cells.push(fmtInt(bag[`n_${w}d`]));
                  return cells;
                })}
              />
            )}
          </SectionCard>

          {/* Drift watch (full table) */}
          <SectionCard
            title="Drift watch (full)"
            description="Sample-aware drift state for recent IC vs baseline IC."
          >
            {driftQ.isLoading ? (
              <CardSkeleton />
            ) : (drift?.factors ?? []).length === 0 ? (
              <EmptyState message="No drift rows yet." />
            ) : (
              <DataTable
                headers={['Factor', 'Status', 'recent n', 'baseline n', 'ic recent', 'ic baseline', 'delta ic', 'delta %']}
                rows={(drift?.factors ?? []).map((r) => {
                  const className = driftClass(r.status);
                  return [
                    r.factor,
                    { text: fmtStatus(r.status), className },
                    fmtInt(r.recent_n),
                    fmtInt(r.baseline_n),
                    fmtNum(r.ic_recent),
                    fmtNum(r.ic_baseline),
                    fmtNum(r.delta_ic),
                    fmtPct(r.delta_pct),
                  ];
                })}
              />
            )}
          </SectionCard>

          {/* Digest viewer */}
          <SectionCard
            title="Latest digest viewer"
            description="Browse markdown digests written under data/research/perf_digests/."
          >
            {digestListQ.isLoading ? (
              <CardSkeleton />
            ) : (digestListQ.data?.digests ?? []).length === 0 ? (
              <EmptyState message="No digests written yet." />
            ) : (
              <>
                <div className="mb-3 flex items-center gap-2 text-xs">
                  <label className="text-slate-400" htmlFor="digest-picker">Digest:</label>
                  <select
                    id="digest-picker"
                    className="rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-slate-100"
                    value={effectiveDigest ?? ''}
                    onChange={(e) => setSelectedDigest(e.target.value || null)}
                  >
                    {(digestListQ.data?.digests ?? []).map((d) => (
                      <option key={d.filename} value={d.filename}>{d.filename}</option>
                    ))}
                  </select>
                </div>
                {digestDocQ.isLoading ? (
                  <CardSkeleton />
                ) : (
                  <pre className="max-h-[600px] overflow-auto rounded-md border border-slate-800 bg-slate-950/60 p-3 text-xs leading-snug text-slate-200 whitespace-pre-wrap">
                    {digestDocQ.data?.markdown ?? ''}
                  </pre>
                )}
              </>
            )}
          </SectionCard>
        </div>
      </details>
    </PageFrame>
  );
}

// --------------------------------------------------------------------------
// Generic table used inside RawDataDetails and Advanced diagnostics.
// --------------------------------------------------------------------------

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
