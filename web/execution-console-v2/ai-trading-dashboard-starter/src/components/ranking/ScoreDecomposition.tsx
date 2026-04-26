/**
 * Score decomposition strip — ``base → penalty → final``.
 *
 * The backend ranking detail does not yet split a row into base/penalty/final,
 * so we compute a plausible decomposition from the factor stack: the base
 * is the average of the four canvas buckets (rs/volume/trend/sector), the
 * penalty is whatever's needed to reach the composite score, and the final
 * is the composite itself.
 */
import type { FactorBlock, RankingDetail } from '@/lib/api/ranking';

interface Props {
  detail: RankingDetail;
  fallbackComposite: number;
}

function avgFactor(blocks: FactorBlock[]): number | null {
  const buckets = blocks.filter((b) => ['rs', 'volume', 'trend', 'sector'].includes(b.bucket));
  if (buckets.length === 0) return null;
  const total = buckets.reduce((acc, b) => acc + b.value, 0);
  return total / buckets.length;
}

export default function ScoreDecomposition({ detail, fallbackComposite }: Props) {
  const composite = detail.ranking?.compositeScore ?? fallbackComposite;
  const base = avgFactor(detail.factors) ?? composite;
  const penalty = Math.max(0, base - composite);

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
      <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
        Score Decomposition
      </h4>
      <div className="mt-3 grid grid-cols-3 gap-3 text-center">
        <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-3">
          <div className="text-[10px] uppercase tracking-wider text-slate-500">Base</div>
          <div className="mt-1 text-xl font-semibold tabular-nums text-slate-100">
            {base.toFixed(2)}
          </div>
        </div>
        <div className="rounded-lg border border-rose-500/20 bg-rose-950/20 p-3">
          <div className="text-[10px] uppercase tracking-wider text-rose-300">Penalty</div>
          <div className="mt-1 text-xl font-semibold tabular-nums text-rose-200">
            −{penalty.toFixed(2)}
          </div>
        </div>
        <div className="rounded-lg border border-emerald-500/20 bg-emerald-950/20 p-3">
          <div className="text-[10px] uppercase tracking-wider text-emerald-300">Final</div>
          <div className="mt-1 text-xl font-semibold tabular-nums text-emerald-200">
            {composite.toFixed(2)}
          </div>
        </div>
      </div>
      <p className="mt-3 text-xs text-slate-500">
        Base reflects the unweighted mean of the four canvas factors (RS, Volume, Trend, Sector).
        Penalty is whatever is required to reach the published composite score for this run.
      </p>
    </div>
  );
}
