/**
 * Three-column bucketed view: Eligible / Watchlist / Blocked.
 *
 * Each bucket is colour-coded per the Canvas design. Cards expose tier,
 * RS, sector, and breakout state so the operator can scan a column without
 * jumping into the detail workspace (PR #12).
 */
import type { StockRow } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';
import type { ExecutionBucket } from './derive';

interface BucketDef {
  key: ExecutionBucket;
  label: string;
  hint: string;
  container: string;
  pill: string;
}

const BUCKETS: BucketDef[] = [
  {
    key: 'eligible',
    label: 'Eligible',
    hint: 'Routable now (Tier-A + breakout)',
    container: 'border-emerald-500/30 bg-emerald-950/15',
    pill: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200',
  },
  {
    key: 'watchlist',
    label: 'Watchlist',
    hint: 'Promote on breakout / volume confirmation',
    container: 'border-amber-500/30 bg-amber-950/15',
    pill: 'border-amber-500/40 bg-amber-500/15 text-amber-200',
  },
  {
    key: 'blocked',
    label: 'Blocked',
    hint: 'Tier-C or weak sector — do not route',
    container: 'border-rose-500/30 bg-rose-950/15',
    pill: 'border-rose-500/40 bg-rose-500/15 text-rose-200',
  },
];

interface Props {
  buckets: Record<ExecutionBucket, StockRow[]>;
  onSelect?: (row: StockRow) => void;
}

export default function BucketColumns({ buckets, onSelect }: Props) {
  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
      {BUCKETS.map((bucket) => {
        const rows = buckets[bucket.key];
        return (
          <div
            key={bucket.key}
            className={cn('flex flex-col rounded-2xl border p-4', bucket.container)}
          >
            <div className="flex items-baseline justify-between gap-2">
              <span
                className={cn(
                  'rounded-full border px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-widest',
                  bucket.pill,
                )}
              >
                {bucket.label}
              </span>
              <span className="text-xs tabular-nums text-slate-400">{rows.length}</span>
            </div>
            <p className="mt-2 text-xs text-slate-500">{bucket.hint}</p>
            <ul className="mt-3 flex flex-1 flex-col gap-2">
              {rows.length === 0 ? (
                <li className="rounded-lg border border-dashed border-slate-700 bg-slate-900/40 p-3 text-xs text-slate-500">
                  No symbols in this bucket.
                </li>
              ) : (
                rows.map((row) => (
                  <li key={row.symbol}>
                    <button
                      type="button"
                      onClick={() => onSelect?.(row)}
                      className={cn(
                        'flex w-full flex-col gap-1 rounded-lg border border-slate-800 bg-slate-950/60 p-3 text-left transition-colors',
                        'hover:border-slate-600 hover:bg-slate-900/60',
                      )}
                    >
                      <div className="flex items-baseline justify-between">
                        <span className="font-semibold text-slate-100">{row.symbol}</span>
                        <span className="text-[10px] uppercase tracking-wider text-slate-500">
                          Tier {row.tier}
                        </span>
                      </div>
                      <div className="text-[11px] text-slate-400">
                        {row.sector} · RS {row.rs} · Score {row.score.toFixed(2)}
                        {row.breakout ? ' · Breakout' : ''}
                      </div>
                    </button>
                  </li>
                ))
              )}
            </ul>
          </div>
        );
      })}
    </div>
  );
}
