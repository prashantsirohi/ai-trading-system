/**
 * Decision-trace tab — the human-readable ranking explanation for one symbol.
 *
 * Surfaces the verdict + reason from ``/ranking/{symbol}``, plus a row-by-row
 * factor contribution table (the same data that powers FactorBars on the
 * Ranking page, exposed as a wider tabular view here).
 */
import type { RankingDetail } from '@/lib/api/ranking';
import VerdictBanner from '@/components/ranking/VerdictBanner';
import { cn } from '@/lib/utils/cn';

interface Props {
  detail: RankingDetail | null | undefined;
  isLoading: boolean;
}

const BUCKET_LABELS: Record<string, string> = {
  rs: 'Relative Strength',
  volume: 'Volume',
  trend: 'Trend',
  sector: 'Sector',
  other: 'Other',
};

function bucketTone(value: number): string {
  if (value >= 75) return 'border-emerald-500/40 bg-emerald-500/10';
  if (value >= 50) return 'border-blue-500/40 bg-blue-500/10';
  if (value >= 25) return 'border-amber-500/40 bg-amber-500/10';
  return 'border-rose-500/40 bg-rose-500/10';
}

export default function DecisionTraceTab({ detail, isLoading }: Props) {
  if (isLoading) {
    return <p className="text-xs text-slate-500">Loading decision trace…</p>;
  }
  if (!detail) {
    return <p className="text-xs text-slate-500">No ranking detail available for this symbol.</p>;
  }

  return (
    <div className="space-y-4">
      <VerdictBanner decision={detail.decision} />

      {detail.factors.length === 0 ? (
        <p className="text-xs text-slate-500">No factor contributions recorded.</p>
      ) : (
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
          {detail.factors.map((block) => (
            <div
              key={block.bucket}
              className={cn('rounded-2xl border p-4', bucketTone(block.value))}
            >
              <div className="flex items-center justify-between">
                <p className="text-xs font-semibold uppercase tracking-widest text-slate-300">
                  {BUCKET_LABELS[block.bucket] ?? block.bucket}
                </p>
                <p className="font-mono text-base text-slate-100">{block.value.toFixed(1)}</p>
              </div>
              {block.contributors.length === 0 ? (
                <p className="mt-2 text-[11px] text-slate-500">No contributor breakdown.</p>
              ) : (
                <ul className="mt-2 space-y-1 text-[11px]">
                  {block.contributors.map((c) => (
                    <li key={c.column} className="flex items-center justify-between text-slate-300">
                      <span className="font-mono">{c.column}</span>
                      <span className="tabular-nums">{c.value.toFixed(2)}</span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
