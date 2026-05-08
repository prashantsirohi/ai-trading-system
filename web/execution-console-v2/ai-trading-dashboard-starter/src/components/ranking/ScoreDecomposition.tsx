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
  const total = buckets.reduce((acc, b) => acc + Math.max(0, Math.min(100, b.value)), 0);
  return total / buckets.length;
}

export default function ScoreDecomposition({ detail, fallbackComposite }: Props) {
  const composite = detail.ranking?.compositeScore ?? fallbackComposite;
  const base = avgFactor(detail.factors) ?? composite;
  const penalty = Math.max(0, base - composite);
  const basePct = Math.max(0, Math.min(100, base));
  const finalPct = Math.max(0, Math.min(100, composite));
  const penaltyPct = Math.max(0, Math.min(100, penalty));

  return (
    <div className="rounded-lg border border-slate-800 bg-slate-950/50 p-3">
      <h4 className="mb-2 text-[10px] font-semibold uppercase tracking-widest text-slate-400">
        Score Decomposition
      </h4>
      <div className="grid gap-2 lg:grid-cols-3">
        <ScoreBar label="Base" value={base} pct={basePct} tone="bg-slate-400/80" />
        <ScoreBar label="Penalty" value={penalty} pct={penaltyPct} tone="bg-rose-500/80" prefix="-" />
        <ScoreBar label="Final" value={composite} pct={finalPct} tone="bg-emerald-500/80" />
      </div>
      <p className="mt-2 text-[11px] text-slate-500">
        Base uses capped factor values for display; final is the published composite score.
      </p>
    </div>
  );
}

function ScoreBar({
  label,
  value,
  pct,
  tone,
  prefix = '',
}: {
  label: string;
  value: number;
  pct: number;
  tone: string;
  prefix?: string;
}) {
  return (
    <div className="rounded-md border border-slate-800 bg-slate-900/45 px-2.5 py-2">
      <div className="flex items-center justify-between gap-2">
        <div className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{label}</div>
        <div className="font-mono text-xs font-semibold tabular-nums text-slate-100">
          {prefix}{value.toFixed(2)}
        </div>
      </div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
        <div className={`h-full rounded-full ${tone}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}
