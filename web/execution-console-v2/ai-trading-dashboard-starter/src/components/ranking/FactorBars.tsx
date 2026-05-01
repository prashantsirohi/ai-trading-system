/**
 * Inline factor progress bars used on each ranking row.
 *
 * The four buckets ``rs / volume / trend / sector`` are surfaced from the
 * backend ``factors`` block. We render them as compact horizontal bars so
 * the operator can scan factor strength at a glance without expanding the
 * row.
 */
import type { FactorBlock, FactorBucket } from '@/lib/api/ranking';
import { cn } from '@/lib/utils/cn';

const LABELS: Record<FactorBucket, string> = {
  rs: 'RS',
  volume: 'Vol',
  trend: 'Trend',
  sector: 'Sector',
  other: 'Other',
};

const BUCKET_TONES: Record<FactorBucket, string> = {
  rs: 'bg-emerald-500/70',
  volume: 'bg-sky-500/70',
  trend: 'bg-violet-500/70',
  sector: 'bg-amber-500/70',
  other: 'bg-slate-500/70',
};

const ORDER: FactorBucket[] = ['rs', 'volume', 'trend', 'sector'];

interface FallbackFactors {
  rs: number;
  volume: number;
  trend: number;
  sector: number;
}

function pickValue(blocks: FactorBlock[], bucket: FactorBucket): number | null {
  const found = blocks.find((b) => b.bucket === bucket);
  return found ? found.value : null;
}

interface Props {
  factors: FactorBlock[];
  fallback: FallbackFactors;
  variant?: 'inline' | 'expanded';
  className?: string;
}

export default function FactorBars({ factors, fallback, variant = 'inline', className }: Props) {
  return (
    <div
      className={cn(
        variant === 'inline'
          ? 'grid w-44 max-w-full grid-cols-4 gap-1.5'
          : 'grid grid-cols-1 gap-3 sm:grid-cols-2',
        className,
      )}
    >
      {ORDER.map((bucket) => {
        const raw = pickValue(factors, bucket);
        const value = raw ?? fallback[bucket as keyof FallbackFactors] ?? 0;
        const pct = Math.max(0, Math.min(100, value));
        if (variant === 'inline') {
          return (
            <div key={bucket} className="min-w-0">
              <div className="flex items-baseline justify-between gap-1 text-[9px] uppercase tracking-wider text-slate-500">
                <span>{LABELS[bucket]}</span>
                <span className="tabular-nums text-slate-300">{Math.round(value)}</span>
              </div>
              <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-slate-800">
                <div
                  className={cn('h-full rounded-full', BUCKET_TONES[bucket])}
                  style={{ width: `${pct}%` }}
                />
              </div>
            </div>
          );
        }
        return (
          <div key={bucket} className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
            <div className="flex items-baseline justify-between">
              <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
                {LABELS[bucket]}
              </span>
              <span className="text-base font-semibold tabular-nums text-slate-100">
                {Math.round(value)}
              </span>
            </div>
            <div className="mt-2 h-2 overflow-hidden rounded-full bg-slate-800">
              <div
                className={cn('h-full rounded-full', BUCKET_TONES[bucket])}
                style={{ width: `${pct}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
