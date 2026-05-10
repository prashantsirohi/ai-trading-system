/**
 * The full Canvas-style expansion shown beneath a ranking row when the user
 * clicks it. Composes:
 *
 *   * Verdict banner.
 *   * Model Explanation triple.
 *   * Score Decomposition strip.
 *   * Factor bars (expanded variant).
 *   * Lifecycle visual.
 *   * Mini auto-chart with pattern overlay (rank-history sparkline).
 *
 * Gracefully degrades when the per-symbol detail or history is still
 * loading or unavailable — every block falls back to row-level data.
 */
import { useRankingDetail, useRankingHistory } from '@/lib/queries';
import { useWorkspace } from '@/components/workspace/WorkspaceContext';
import type { ReactNode } from 'react';
import type { StockRow } from '@/types/dashboard';
import FactorBars from './FactorBars';
import LifecycleVisual from './LifecycleVisual';
import MiniChart from './MiniChart';
import ModelExplanation from './ModelExplanation';
import ScoreDecomposition from './ScoreDecomposition';
import VerdictBanner from './VerdictBanner';
import type { LifecycleStage, RankingDetail } from '@/lib/api/ranking';
import { cn } from '@/lib/utils/cn';

interface Props {
  row: StockRow;
  isCompared: boolean;
  onToggleCompare: () => void;
}

const FALLBACK_LIFECYCLE = (row: StockRow): LifecycleStage[] => [
  {
    key: 'rank',
    label: 'Ranked',
    state: 'complete',
    detail: `Score ${row.score.toFixed(2)}`,
  },
  {
    key: 'breakout',
    label: 'Breakout',
    state: row.breakout ? 'complete' : 'pending',
    detail: row.breakout ? 'Breakout flagged' : 'Awaiting confirmation',
  },
  {
    key: 'pattern',
    label: 'Pattern',
    state: row.pattern && row.pattern !== 'N/A' ? 'complete' : 'pending',
    detail: row.pattern || 'No pattern',
  },
  {
    key: 'execution',
    label: 'Execution',
    state: row.tier === 'A' && row.breakout ? 'active' : 'pending',
    detail: row.tier === 'A' && row.breakout ? 'Eligible' : 'Pending upstream',
  },
];

const FALLBACK_DETAIL = (row: StockRow): RankingDetail => ({
  available: false,
  symbol: row.symbol,
  runId: null,
  ranking: {
    rankPosition: null,
    universeSize: 0,
    compositeScore: row.score,
    sectorName: row.sector,
    category: null,
    inBreakoutScan: row.breakout,
    inPatternScan: row.pattern !== 'N/A',
  },
  lifecycle: FALLBACK_LIFECYCLE(row),
  decision: { verdict: null, confidence: null, reason: null },
  factors: [],
  sectorContext: null,
  operatorContext: {
    stageLabel: row.stageLabel ?? null,
    stageTransition: row.stageTransition ?? null,
    barsInStage: row.barsInStage ?? null,
    stageEntryDate: row.stageEntryDate ?? null,
    stageFreshnessBucket: row.stageFreshnessBucket ?? null,
    momentumAccelerationScore: row.momentumAccelerationScore ?? null,
    exhaustionPenalty: row.exhaustionPenalty ?? null,
    exhaustionFlag: row.exhaustionFlag ?? null,
    distanceFromPivotAtr: row.distanceFromPivotAtr ?? null,
    topPatternFamily: row.pattern !== 'N/A' ? row.pattern : null,
    topPatternState: row.patternState ?? null,
    topPatternSetupQuality: row.setupQuality ?? null,
    topPatternPivotPrice: row.pivotPrice ?? null,
    topPatternInvalidationPrice: row.invalidationPrice ?? null,
    topPatternSignalDate: row.patternSignalDate ?? null,
    topPatternStartDate: row.patternStartDate ?? null,
    topPatternEndDate: row.patternEndDate ?? null,
    reclaimSignalFlag: Boolean(row.reclaimSignal),
    explanation: [],
  },
  rawRow: null,
});

export default function ExpandedRowPanel({ row, isCompared, onToggleCompare }: Props) {
  const { openWorkspace } = useWorkspace();
  const detailQuery = useRankingDetail(row.symbol);
  const historyQuery = useRankingHistory(row.symbol, 20);

  const fallback = FALLBACK_DETAIL(row);
  const detail = detailQuery.data ?? fallback;
  const lifecycle = detail.lifecycle.length > 0 ? detail.lifecycle : fallback.lifecycle;
  const operator = detail.operatorContext;

  return (
    <div className="space-y-3 px-3 py-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-slate-100">
            {row.symbol}
            <span className="ml-2 text-sm font-normal text-slate-400">
              {row.sector} · Tier {row.tier}
              {detail.ranking?.rankPosition != null
                ? ` · Rank #${detail.ranking.rankPosition}`
                : ''}
            </span>
          </h3>
          {detailQuery.isError ? (
            <p className="mt-1 text-xs text-amber-300">
              Live detail unavailable — showing inline data only.
            </p>
          ) : null}
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => openWorkspace(row.symbol)}
            className="rounded-md border border-emerald-500/40 bg-emerald-500/10 px-3 py-1.5 text-xs font-semibold uppercase tracking-wider text-emerald-200 transition-colors hover:border-emerald-300/60"
          >
            Open workspace
          </button>
          <button
            type="button"
            onClick={onToggleCompare}
            className={cn(
              'rounded-md border px-3 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors',
              isCompared
                ? 'border-blue-500/40 bg-blue-500/15 text-blue-200'
                : 'border-slate-700 bg-slate-900/60 text-slate-300 hover:border-slate-500',
            )}
          >
            {isCompared ? 'Remove from compare' : 'Add to compare'}
          </button>
        </div>
      </div>

      <VerdictBanner decision={detail.decision} />

      <ModelExplanation detail={detail} row={row} />

      <ScoreDecomposition detail={detail} fallbackComposite={row.score} />

      <PanelSection title="Fundamentals And Watchlist">
        <div className="grid gap-2 lg:grid-cols-[minmax(0,1.15fr)_minmax(0,1fr)]">
          <div className="grid gap-2 md:grid-cols-2">
            <MetricBar label="Fundamental" value={row.fundamentalScore} tone="amber" badge={row.fundamentalTier ?? undefined} />
            <MetricBar label="Quality" value={row.qualityScore} tone="emerald" />
            <MetricBar label="Growth" value={row.growthScore} tone="sky" />
            <MetricBar label="Balance sheet" value={row.balanceSheetScore} tone="violet" />
            <MetricBar label="Valuation" value={row.valuationScore} tone="amber" />
            <MetricBar label="Ownership" value={row.ownershipScore} tone="emerald" />
          </div>
          <div className="grid gap-2">
            <CompactInfo
              label="Bucket"
              value={row.watchlistBucket ? row.watchlistBucket.split('_').join(' ') : '—'}
              tone="blue"
            />
            <CompactInfo label="Flags" value={row.redFlags ?? '—'} tone={row.redFlags ? 'amber' : 'slate'} />
            <CompactInfo label="Next action" value={row.nextAction ?? '—'} tone={row.nextAction ? 'emerald' : 'slate'} />
          </div>
        </div>
      </PanelSection>

      <PanelSection title="Operator Notes">
        <div className="grid gap-2 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <div className="grid gap-2 md:grid-cols-3">
            <CompactInfo label="Stage" value={operator.stageLabel ?? row.stageLabel ?? '—'} tone="emerald" />
            <CompactInfo
              label="Stage age"
              value={operator.barsInStage == null ? '—' : `${operator.barsInStage} bars`}
              tone="blue"
            />
            <CompactInfo label="Transition" value={operator.stageTransition ?? row.stageTransition ?? '—'} tone="slate" />
            <CompactInfo label="Top pattern" value={operator.topPatternFamily ?? row.pattern ?? '—'} tone="violet" />
            <CompactInfo label="Pattern state" value={operator.topPatternState ?? row.patternState ?? '—'} tone="slate" />
            <CompactInfo
              label="Invalidation"
              value={operator.topPatternInvalidationPrice == null ? '—' : operator.topPatternInvalidationPrice.toFixed(2)}
              tone="amber"
            />
          </div>
          <div className="grid gap-2">
            <MetricBar label="Momentum acceleration" value={operator.momentumAccelerationScore} tone="emerald" />
            <MetricBar
              label="Exhaustion"
              value={operator.exhaustionPenalty}
              tone="amber"
              max={5}
              suffix={operator.exhaustionFlag ?? undefined}
            />
            <MetricBar
              label="Pivot distance"
              value={operator.distanceFromPivotAtr}
              tone="rose"
              max={4}
              suffix="ATR"
            />
          </div>
        </div>
        <div className="mt-3 flex flex-wrap gap-1">
          {operator.stageFreshnessBucket === 'fresh_s2' ? (
            <WarningLabel tone="green" label="Fresh S2" />
          ) : null}
          {operator.stageFreshnessBucket === 'extended_s2' ? (
            <WarningLabel tone="amber" label="Extended S2" />
          ) : null}
          {(operator.exhaustionPenalty ?? 0) > 0 ? (
            <WarningLabel tone="amber" label="Exhaustion risk" />
          ) : null}
          {(operator.distanceFromPivotAtr ?? 0) >= 2 ? (
            <WarningLabel tone="amber" label="Distance from pivot extended" />
          ) : null}
          {operator.reclaimSignalFlag ? (
            <WarningLabel tone="green" label="Reclaim signal" />
          ) : null}
        </div>
        {operator.explanation.length > 0 ? (
          <ul className="mt-3 space-y-1 text-xs text-slate-400">
            {operator.explanation.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        ) : (
          <p className="mt-3 text-xs text-slate-500">
            No additional score or penalty explanation is available for this artifact.
          </p>
        )}
      </PanelSection>

      <PanelSection title="Factor Bars">
        <FactorBars
          variant="expanded"
          factors={detail.factors}
          fallback={{
            rs: row.rs,
            volume: row.volume === 'High' ? 85 : row.volume === 'Medium' ? 60 : 35,
            trend: row.trend,
            sector: row.sectorStrength,
          }}
        />
      </PanelSection>

      <PanelSection title="Lifecycle">
        <LifecycleVisual stages={lifecycle} />
      </PanelSection>

      <MiniChart history={historyQuery.data} row={row} isLoading={historyQuery.isLoading} />
    </div>
  );
}

type Tone = 'emerald' | 'sky' | 'violet' | 'amber' | 'rose' | 'blue' | 'slate';

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

function PanelSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-950/50 p-3">
      <h4 className="mb-2 text-[10px] font-semibold uppercase tracking-widest text-slate-400">
        {title}
      </h4>
      {children}
    </div>
  );
}

function MetricBar({
  label,
  value,
  tone,
  max = 100,
  badge,
  suffix,
}: {
  label: string;
  value?: number | null;
  tone: Tone;
  max?: number;
  badge?: string;
  suffix?: string;
}) {
  const safeValue = value == null || !Number.isFinite(value) ? null : value;
  const pct = safeValue == null ? 0 : Math.max(0, Math.min(100, (safeValue / max) * 100));

  return (
    <div className="rounded-md border border-slate-800 bg-slate-900/45 px-2.5 py-2">
      <div className="flex items-center justify-between gap-2">
        <div className="truncate text-[10px] font-semibold uppercase tracking-wider text-slate-500">
          {label}
        </div>
        <div className="flex shrink-0 items-center gap-1.5">
          {badge ? (
            <span className={cn('rounded-full border px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider', INFO_TONES[tone])}>
              {badge}
            </span>
          ) : null}
          <span className="font-mono text-xs font-semibold tabular-nums text-slate-100">
            {safeValue == null ? '—' : safeValue.toFixed(1)}
            {safeValue != null && suffix ? ` ${suffix}` : ''}
          </span>
        </div>
      </div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
        <div className={cn('h-full rounded-full', BAR_TONES[tone])} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function CompactInfo({ label, value, tone }: { label: string; value: string; tone: Tone }) {
  return (
    <div className={cn('min-w-0 rounded-md border px-2.5 py-2', INFO_TONES[tone])}>
      <div className="text-[10px] font-semibold uppercase tracking-wider opacity-70">{label}</div>
      <div className="mt-0.5 truncate text-xs font-semibold" title={value}>{value}</div>
    </div>
  );
}

function WarningLabel({ label, tone }: { label: string; tone: 'green' | 'amber' }) {
  return (
    <span
      className={cn(
        'rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
        tone === 'green'
          ? 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200'
          : 'border-amber-500/40 bg-amber-500/15 text-amber-200',
      )}
    >
      {label}
    </span>
  );
}
