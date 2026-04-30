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

  return (
    <div className="space-y-4 px-4 py-5">
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

      <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          Operator Notes
        </h4>
        <div className="mt-3 grid gap-3 md:grid-cols-3">
          <DetailMetric label="Stage" value={detail.operatorContext.stageLabel ?? row.stageLabel ?? '—'} />
          <DetailMetric
            label="Stage age"
            value={
              detail.operatorContext.barsInStage == null
                ? '—'
                : `${detail.operatorContext.barsInStage} bars`
            }
          />
          <DetailMetric label="Transition" value={detail.operatorContext.stageTransition ?? row.stageTransition ?? '—'} />
          <DetailMetric
            label="Momentum acceleration"
            value={
              detail.operatorContext.momentumAccelerationScore == null
                ? '—'
                : detail.operatorContext.momentumAccelerationScore.toFixed(1)
            }
          />
          <DetailMetric
            label="Exhaustion"
            value={
              detail.operatorContext.exhaustionPenalty == null
                ? '—'
                : `${detail.operatorContext.exhaustionPenalty.toFixed(1)} ${detail.operatorContext.exhaustionFlag ?? ''}`.trim()
            }
          />
          <DetailMetric
            label="Pivot distance"
            value={
              detail.operatorContext.distanceFromPivotAtr == null
                ? '—'
                : `${detail.operatorContext.distanceFromPivotAtr.toFixed(1)} ATR`
            }
          />
          <DetailMetric label="Top pattern" value={detail.operatorContext.topPatternFamily ?? row.pattern ?? '—'} />
          <DetailMetric label="Pattern state" value={detail.operatorContext.topPatternState ?? row.patternState ?? '—'} />
          <DetailMetric
            label="Invalidation"
            value={
              detail.operatorContext.topPatternInvalidationPrice == null
                ? '—'
                : detail.operatorContext.topPatternInvalidationPrice.toFixed(2)
            }
          />
        </div>
        <div className="mt-3 flex flex-wrap gap-1">
          {detail.operatorContext.stageFreshnessBucket === 'fresh_s2' ? (
            <WarningLabel tone="green" label="Fresh S2" />
          ) : null}
          {detail.operatorContext.stageFreshnessBucket === 'extended_s2' ? (
            <WarningLabel tone="amber" label="Extended S2" />
          ) : null}
          {(detail.operatorContext.exhaustionPenalty ?? 0) > 0 ? (
            <WarningLabel tone="amber" label="Exhaustion risk" />
          ) : null}
          {(detail.operatorContext.distanceFromPivotAtr ?? 0) >= 2 ? (
            <WarningLabel tone="amber" label="Distance from pivot extended" />
          ) : null}
          {detail.operatorContext.reclaimSignalFlag ? (
            <WarningLabel tone="green" label="Reclaim signal" />
          ) : null}
        </div>
        {detail.operatorContext.explanation.length > 0 ? (
          <ul className="mt-3 space-y-1 text-xs text-slate-400">
            {detail.operatorContext.explanation.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        ) : (
          <p className="mt-3 text-xs text-slate-500">
            No additional score or penalty explanation is available for this artifact.
          </p>
        )}
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          Factor Bars
        </h4>
        <FactorBars
          variant="expanded"
          factors={detail.factors}
          fallback={{
            rs: row.rs,
            volume: row.volume === 'High' ? 85 : row.volume === 'Medium' ? 60 : 35,
            trend: row.trend,
            sector: row.sectorStrength,
          }}
          className="mt-3"
        />
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          Lifecycle
        </h4>
        <div className="mt-3">
          <LifecycleVisual stages={lifecycle} />
        </div>
      </div>

      <MiniChart history={historyQuery.data} row={row} isLoading={historyQuery.isLoading} />
    </div>
  );
}

function DetailMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-3">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{label}</div>
      <div className="mt-1 text-sm font-semibold text-slate-100">{value}</div>
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
