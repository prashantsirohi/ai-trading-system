/**
 * Canonical stock workspace.
 *
 * Route: /symbol/:sym
 *
 * Every ticker click across the app should land here. The page is intentionally
 * scan-first: compact visual blocks for ranking, fundamentals, technicals,
 * lifecycle, setup, risk, metadata, chart, and recent activity.
 */
import { useMemo, type ReactNode } from 'react';
import { Link, useParams } from 'react-router-dom';

import EmptyState from '@/components/common/EmptyState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import PageFrame from '@/components/common/PageFrame';
import FactorBars from '@/components/ranking/FactorBars';
import IndicatorBars from '@/components/symbol/IndicatorBars';
import NewsAndFillsPanel from '@/components/symbol/NewsAndFillsPanel';
import SymbolChart, { type ChartPatternOverlay } from '@/components/symbol/SymbolChart';
import { useRanking, useRankingDetail, useStockDetail, useStockOhlcv } from '@/lib/queries';
import { mapBackendStockRow } from '@/lib/api/mappers';
import { deriveIndicators, deriveMAs } from '@/lib/symbol/derive';
import { getSymbolNews } from '@/lib/mock/symbolNews';
import { cn } from '@/lib/utils/cn';
import type { StockRow } from '@/types/dashboard';

type Tone = 'emerald' | 'sky' | 'violet' | 'amber' | 'rose' | 'blue' | 'slate';

const TIER_BADGE: Record<string, string> = {
  A: 'border-emerald-600/50 bg-emerald-500/15 text-emerald-300',
  B: 'border-blue-600/50 bg-blue-500/15 text-blue-300',
  C: 'border-amber-600/50 bg-amber-500/15 text-amber-300',
};

const BAR_TONES: Record<Tone, string> = {
  emerald: 'bg-emerald-500/80',
  sky: 'bg-sky-500/80',
  violet: 'bg-violet-500/80',
  amber: 'bg-amber-500/80',
  rose: 'bg-rose-500/80',
  blue: 'bg-blue-500/80',
  slate: 'bg-slate-500/70',
};

const INFO_TONES: Record<Tone, string> = {
  emerald: 'border-emerald-500/20 bg-emerald-950/20 text-emerald-100',
  sky: 'border-sky-500/20 bg-sky-950/20 text-sky-100',
  violet: 'border-violet-500/20 bg-violet-950/20 text-violet-100',
  amber: 'border-amber-500/20 bg-amber-950/20 text-amber-100',
  rose: 'border-rose-500/20 bg-rose-950/20 text-rose-100',
  blue: 'border-blue-500/20 bg-blue-950/20 text-blue-100',
  slate: 'border-slate-800 bg-slate-900/50 text-slate-200',
};

function fmtNum(value?: number | null, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return value.toLocaleString('en-IN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtCompact(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return '-';
  if (Math.abs(value) >= 1e12) return `${(value / 1e12).toFixed(2)}L Cr`;
  if (Math.abs(value) >= 1e7) return `${(value / 1e7).toFixed(2)} Cr`;
  if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
  return value.toLocaleString('en-IN');
}

function rankPct(rank?: number | null, universe?: number): number | null {
  if (!rank || !universe) return null;
  return Math.max(0, Math.min(100, 100 - ((rank - 1) / universe) * 100));
}

function fallbackFactors(row: StockRow) {
  return {
    rs: row.rs,
    volume: row.volume === 'High' ? 85 : row.volume === 'Medium' ? 60 : 35,
    trend: row.trend,
    sector: row.sectorStrength,
  };
}

function rawText(raw: Record<string, string | number | boolean | null> | null | undefined, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = raw?.[key];
    if (typeof value === 'string' && value.trim() !== '') return value;
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  }
  return null;
}

function rawNumber(raw: Record<string, string | number | boolean | null> | null | undefined, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = raw?.[key];
    const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN;
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

export default function SymbolPage() {
  const { sym } = useParams<{ sym: string }>();
  const symbol = sym?.toUpperCase() ?? '';

  const detailQuery = useStockDetail(symbol);
  const ohlcvQuery = useStockOhlcv(symbol, 365);
  const rankingQuery = useRanking();
  const rankingDetailQuery = useRankingDetail(symbol);

  const row = useMemo(() => {
    const listed = rankingQuery.data?.rows.find((r) => r.symbol === symbol) ?? null;
    if (listed) return listed;
    const raw = rankingDetailQuery.data?.rawRow;
    return raw ? mapBackendStockRow(raw) : null;
  }, [rankingDetailQuery.data?.rawRow, rankingQuery.data, symbol]);
  const indicators = useMemo(() => (row ? deriveIndicators(row) : null), [row]);
  const mas = useMemo(
    () => (ohlcvQuery.data?.candles ? deriveMAs(ohlcvQuery.data.candles) : { ma50: [], ma200: [], high52w: null, low52w: null }),
    [ohlcvQuery.data],
  );
  const newsEntries = useMemo(() => getSymbolNews(symbol), [symbol]);

  if (!symbol) {
    return (
      <PageFrame title="Symbol" description="">
        <EmptyState message="No symbol specified." />
      </PageFrame>
    );
  }

  const detail = detailQuery.data;
  const quote = detail?.latestQuote;
  const meta = detail?.metadata;
  const fundamentals = detail?.fundamentals;
  const ranking = detail?.ranking;
  const rankDetail = rankingDetailQuery.data;
  const operator = rankDetail?.operatorContext;
  const chartPattern = useMemo<ChartPatternOverlay | null>(() => {
    if (!row && !operator && !rankDetail?.rawRow) return null;
    const raw = rankDetail?.rawRow;
    const family = operator?.topPatternFamily ?? row?.pattern ?? rawText(raw, 'pattern_family', 'setup_family', 'pattern_type', 'pattern');
    if (!family || family === 'N/A') return null;
    return {
      family,
      state: operator?.topPatternState ?? row?.patternState ?? rawText(raw, 'pattern_state', 'pattern_lifecycle_state'),
      setupQuality: operator?.topPatternSetupQuality ?? row?.setupQuality ?? rawNumber(raw, 'setup_quality', 'pattern_score', 'pattern_priority_score'),
      pivotPrice: operator?.topPatternPivotPrice ?? row?.pivotPrice ?? rawNumber(raw, 'pivot_price', 'top_pattern_pivot_price'),
      breakoutLevel: row?.pivotPrice ?? rawNumber(raw, 'breakout_level', 'watchlist_trigger_level'),
      invalidationPrice: operator?.topPatternInvalidationPrice ?? row?.invalidationPrice ?? rawNumber(raw, 'invalidation_price', 'stop_price'),
      signalDate: operator?.topPatternSignalDate ?? row?.patternSignalDate ?? rawText(raw, 'signal_date', 'fresh_signal_date', 'last_seen_date'),
      startDate: operator?.topPatternStartDate ?? row?.patternStartDate ?? rawText(raw, 'pattern_start', 'first_seen_date'),
      endDate: operator?.topPatternEndDate ?? row?.patternEndDate ?? rawText(raw, 'pattern_end', 'last_seen_date'),
    };
  }, [operator, rankDetail?.rawRow, row]);
  const close = quote?.close ?? row?.price ?? null;
  const chgAbs = quote?.close != null && quote?.open != null ? quote.close - quote.open : null;
  const chgPct = chgAbs != null && quote?.open ? (chgAbs / quote.open) * 100 : null;
  const positionPct = rankPct(ranking?.rankPosition, ranking?.universeSize);
  const rankLabel = ranking?.rankPosition ? `#${ranking.rankPosition} / ${ranking.universeSize}` : '-';
  const sectorName = row?.sector ?? ranking?.sectorName ?? meta?.sector ?? fundamentals?.sector ?? null;
  const industryGroup =
    meta?.industryGroup ?? (meta?.sector && meta.sector !== sectorName ? meta.sector : null);

  return (
    <PageFrame
      title={symbol}
      description={meta?.symbolName ?? `Stock workspace - NSE - ${symbol}`}
      hideHeader
    >
      {detailQuery.isLoading || rankingQuery.isLoading ? (
        <CardSkeleton />
      ) : (
        <div className="space-y-3">
          <section className="rounded-lg border border-slate-800 bg-slate-900/55 p-3">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <h1 className="font-mono text-2xl font-bold text-slate-100">{symbol}</h1>
                  {row?.tier ? (
                    <span className={cn('rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase', TIER_BADGE[row.tier] ?? TIER_BADGE.C)}>
                      {row.tier}
                    </span>
                  ) : null}
                  {(row?.fundamentalTier ?? fundamentals?.fundamentalTier) ? (
                    <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-bold uppercase text-amber-200">
                      Fund {row?.fundamentalTier ?? fundamentals?.fundamentalTier}
                    </span>
                  ) : null}
                </div>
                <p className="mt-0.5 truncate text-sm text-slate-400">
                  {meta?.symbolName ?? 'Name unavailable'}
                  {sectorName ? (
                    <>
                      {' - '}
                      <Link to={`/sectors/${encodeURIComponent(sectorName)}`} className="text-blue-400 hover:underline">
                        {sectorName}
                      </Link>
                      {industryGroup ? <span className="text-slate-500"> · {industryGroup}</span> : null}
                    </>
                  ) : null}
                </p>
                <div className="mt-2 flex flex-wrap gap-1.5">
                  <StatusPill label={detail?.lifecycle.rank ?? 'OUT'} tone={(detail?.lifecycle.rank ?? '').startsWith('TOP') ? 'emerald' : 'slate'} />
                  <StatusPill label={row?.stageLabel ?? operator?.stageLabel ?? 'No stage'} tone="blue" />
                  <StatusPill label={row?.pattern && row.pattern !== 'N/A' ? row.pattern : 'No pattern'} tone={row?.pattern && row.pattern !== 'N/A' ? 'violet' : 'slate'} />
                  <StatusPill label={row?.breakout ? 'Breakout' : 'No breakout'} tone={row?.breakout ? 'emerald' : 'slate'} />
                  {(operator?.exhaustionPenalty ?? row?.exhaustionPenalty ?? 0) > 0 ? <StatusPill label="Exhaustion risk" tone="amber" /> : null}
                </div>
              </div>
              <div className="text-right">
                <div className="font-mono text-3xl font-bold text-slate-100">{fmtNum(close)}</div>
                {chgAbs != null && chgPct != null ? (
                  <div className={cn('font-mono text-sm', chgAbs >= 0 ? 'text-emerald-400' : 'text-rose-400')}>
                    {chgAbs >= 0 ? '+' : ''}{chgAbs.toFixed(2)} ({chgPct >= 0 ? '+' : ''}{chgPct.toFixed(2)}%)
                  </div>
                ) : null}
                <div className="mt-1 font-mono text-[11px] text-slate-500">
                  Vol {fmtCompact(quote?.volume)} · Delivery {quote?.deliveryPct == null ? '-' : `${quote.deliveryPct.toFixed(2)}%`}
                </div>
              </div>
            </div>
          </section>

          <div className="grid gap-3 xl:grid-cols-[minmax(0,1.2fr)_minmax(360px,0.8fr)]">
            <Panel title="Price And Delivery">
              <SymbolChart data={ohlcvQuery.data} isLoading={ohlcvQuery.isLoading} pattern={chartPattern} />
            </Panel>

            <div className="grid gap-3">
              <Panel title="Ranking Snapshot">
                <div className="grid gap-2 sm:grid-cols-2">
                  <MetricBar label="Composite" value={ranking?.compositeScore ?? row?.score} tone="emerald" />
                  <MetricBar label="Rank strength" value={positionPct} tone="blue" valueLabel={rankLabel} />
                  <MetricBar label="RS" value={row?.rs} tone="emerald" />
                  <MetricBar label="Sector strength" value={row?.sectorStrength} tone="amber" />
                </div>
              </Panel>

              <Panel title="Technical Factors">
                {row ? (
                  <FactorBars factors={rankDetail?.factors ?? []} fallback={fallbackFactors(row)} variant="expanded" />
                ) : (
                  <p className="text-xs text-slate-500">No ranking data available.</p>
                )}
              </Panel>

              <Panel title="Indicators">
                {indicators ? <IndicatorBars indicators={indicators} /> : <p className="text-xs text-slate-500">No indicators available.</p>}
              </Panel>
            </div>
          </div>

          <div className="grid gap-3 xl:grid-cols-2">
            <Panel title="Fundamentals">
              <div className="grid gap-2 md:grid-cols-2">
                <MetricBar label="Fundamental" value={row?.fundamentalScore ?? fundamentals?.fundamentalScore} tone="amber" badge={row?.fundamentalTier ?? fundamentals?.fundamentalTier ?? undefined} />
                <MetricBar label="Quality" value={row?.qualityScore ?? fundamentals?.qualityScore} tone="emerald" />
                <MetricBar label="Growth" value={row?.growthScore ?? fundamentals?.growthScore} tone="sky" />
                <MetricBar label="Balance sheet" value={row?.balanceSheetScore ?? fundamentals?.balanceSheetScore} tone="violet" />
                <MetricBar label="Valuation" value={row?.valuationScore ?? fundamentals?.valuationScore} tone="amber" />
                <MetricBar label="Ownership" value={row?.ownershipScore ?? fundamentals?.ownershipScore} tone="emerald" />
              </div>
              <div className="mt-2 grid gap-2 md:grid-cols-2">
                <InfoChip label="Flags" value={row?.redFlags ?? fundamentals?.redFlags ?? '-'} tone={(row?.redFlags ?? fundamentals?.redFlags) ? 'amber' : 'slate'} />
                <InfoChip label="Next action" value={row?.nextAction ?? '-'} tone={row?.nextAction ? 'emerald' : 'slate'} />
              </div>
            </Panel>

            <Panel title="Operator Risk">
              <div className="grid gap-2 md:grid-cols-2">
                <MetricBar label="Momentum acceleration" value={operator?.momentumAccelerationScore ?? row?.momentumAccelerationScore} tone="emerald" />
                <MetricBar label="Exhaustion" value={operator?.exhaustionPenalty ?? row?.exhaustionPenalty} tone="amber" max={5} suffix={operator?.exhaustionFlag ?? row?.exhaustionFlag ?? undefined} />
                <MetricBar label="Pivot distance" value={operator?.distanceFromPivotAtr ?? row?.distanceFromPivotAtr} tone="rose" max={4} suffix="ATR" />
                <MetricBar label="Delivery" value={quote?.deliveryPct} tone="sky" />
              </div>
              <div className="mt-2 grid gap-2 md:grid-cols-3">
                <InfoChip label="Stage" value={operator?.stageLabel ?? row?.stageLabel ?? '-'} tone="blue" />
                <InfoChip label="Age" value={operator?.barsInStage == null ? '-' : `${operator.barsInStage} bars`} tone="slate" />
                <InfoChip label="Transition" value={operator?.stageTransition ?? row?.stageTransition ?? '-'} tone="slate" />
              </div>
            </Panel>
          </div>

          <div className="grid gap-3 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
            <Panel title="Lifecycle">
              <div className="grid gap-2 sm:grid-cols-4">
                <LifecycleChip label="Rank" value={detail?.lifecycle.rank ?? 'OUT'} />
                <LifecycleChip label="Breakout" value={detail?.lifecycle.breakout ?? 'NONE'} />
                <LifecycleChip label="Pattern" value={detail?.lifecycle.pattern ?? 'NONE'} />
                <LifecycleChip label="Execution" value={detail?.lifecycle.execution ?? 'OUT'} />
              </div>
            </Panel>

            <Panel title="Market And Metadata">
              <div className="grid gap-2 md:grid-cols-4">
                <InfoChip label="52w high" value={fmtNum(mas.high52w)} tone="emerald" />
                <InfoChip label="52w low" value={fmtNum(mas.low52w)} tone="rose" />
                <InfoChip label="Market cap" value={fmtCompact(meta?.mcap)} tone="blue" />
                <InfoChip label="Industry" value={meta?.industry ?? '-'} tone="slate" />
                <InfoChip label="ISIN" value={meta?.isin ?? '-'} tone="slate" />
                <InfoChip label="Exchange" value={meta?.exchange ?? 'NSE'} tone="slate" />
                <InfoChip label="Lot size" value={meta?.lotSize == null ? '-' : String(meta.lotSize)} tone="slate" />
                <InfoChip label="Tick size" value={meta?.tickSize == null ? '-' : String(meta.tickSize)} tone="slate" />
              </div>
            </Panel>
          </div>

          <Panel title="Recent News And Fills">
            <NewsAndFillsPanel entries={newsEntries.slice(0, 5)} />
          </Panel>
        </div>
      )}
    </PageFrame>
  );
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="rounded-lg border border-slate-800 bg-slate-950/50 p-3">
      <h2 className="mb-2 text-[10px] font-semibold uppercase tracking-widest text-slate-400">
        {title}
      </h2>
      {children}
    </section>
  );
}

function StatusPill({ label, tone }: { label: string; tone: Tone }) {
  return (
    <span className={cn('rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider', INFO_TONES[tone])}>
      {label}
    </span>
  );
}

function MetricBar({
  label,
  value,
  tone,
  max = 100,
  badge,
  suffix,
  valueLabel,
}: {
  label: string;
  value?: number | null;
  tone: Tone;
  max?: number;
  badge?: string;
  suffix?: string;
  valueLabel?: string;
}) {
  const safeValue = value == null || !Number.isFinite(value) ? null : value;
  const pct = safeValue == null ? 0 : Math.max(0, Math.min(100, (safeValue / max) * 100));
  const display = valueLabel ?? (safeValue == null ? '-' : `${safeValue.toFixed(1)}${suffix ? ` ${suffix}` : ''}`);

  return (
    <div className="rounded-md border border-slate-800 bg-slate-900/45 px-2.5 py-2">
      <div className="flex items-center justify-between gap-2">
        <div className="truncate text-[10px] font-semibold uppercase tracking-wider text-slate-500">{label}</div>
        <div className="flex shrink-0 items-center gap-1.5">
          {badge ? <StatusPill label={badge} tone={tone} /> : null}
          <span className="font-mono text-xs font-semibold tabular-nums text-slate-100">{display}</span>
        </div>
      </div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
        <div className={cn('h-full rounded-full', BAR_TONES[tone])} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function InfoChip({ label, value, tone }: { label: string; value: string; tone: Tone }) {
  return (
    <div className={cn('min-w-0 rounded-md border px-2.5 py-2', INFO_TONES[tone])}>
      <div className="text-[10px] font-semibold uppercase tracking-wider opacity-70">{label}</div>
      <div className="mt-0.5 truncate font-mono text-xs font-semibold" title={value}>{value}</div>
    </div>
  );
}

function LifecycleChip({ label, value }: { label: string; value: string }) {
  const norm = value.toUpperCase();
  const tone: Tone =
    norm.startsWith('TOP') || norm === 'CONFIRMED' || norm === 'ELIGIBLE'
      ? 'emerald'
      : norm === 'WATCHLIST' || norm === 'MID TIER' || (norm !== 'NONE' && norm !== 'OUT')
        ? 'amber'
        : 'slate';
  return <InfoChip label={label} value={value} tone={tone} />;
}
