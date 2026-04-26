/**
 * Top banner of the Execution view.
 *
 * Surfaces the operator-facing mode (Live / Preview), the workspace trust
 * pill, and a capital-usage bar. In Preview mode the container gets a
 * subtle striped overlay so the operator can never confuse the page with
 * the (eventual) live routing surface.
 */
import type { ExecutionMode } from '@/lib/api/client';
import { cn } from '@/lib/utils/cn';

interface Props {
  mode: ExecutionMode;
  trustLabel: string;
  trustTone: 'good' | 'warn' | 'bad' | 'neutral';
  capitalUsedPct: number;
  capitalLimitPct: number;
  eligibleCount: number;
}

const TRUST_TONES: Record<Props['trustTone'], string> = {
  good: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200',
  warn: 'border-amber-500/40 bg-amber-500/15 text-amber-200',
  bad: 'border-rose-500/40 bg-rose-500/15 text-rose-200',
  neutral: 'border-slate-700 bg-slate-800 text-slate-300',
};

export default function ExecutionStateBanner({
  mode,
  trustLabel,
  trustTone,
  capitalUsedPct,
  capitalLimitPct,
  eligibleCount,
}: Props) {
  const isPreview = mode === 'preview';
  return (
    <div
      className={cn(
        'relative overflow-hidden rounded-2xl border p-4',
        isPreview
          ? 'border-amber-500/30 bg-amber-950/20'
          : 'border-emerald-500/30 bg-emerald-950/20',
      )}
    >
      {isPreview ? (
        <div
          aria-hidden="true"
          className="pointer-events-none absolute inset-0 opacity-25"
          style={{
            backgroundImage:
              'repeating-linear-gradient(45deg, rgba(245, 158, 11, 0.08) 0 12px, transparent 12px 24px)',
          }}
        />
      ) : null}
      <div className="relative flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <span
            className={cn(
              'rounded-md border px-2.5 py-1 text-xs font-bold uppercase tracking-widest',
              isPreview
                ? 'border-amber-500/40 bg-amber-500/15 text-amber-200'
                : 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200',
            )}
          >
            {isPreview ? 'Preview Mode' : 'Live Mode'}
          </span>
          <span
            className={cn(
              'rounded-md border px-2.5 py-1 text-xs font-semibold uppercase tracking-wider',
              TRUST_TONES[trustTone],
            )}
          >
            Trust: {trustLabel}
          </span>
          <span className="text-xs text-slate-400">
            {eligibleCount} eligible · routing {isPreview ? 'disabled' : 'enabled'}
          </span>
        </div>
        <div className="min-w-[260px] flex-1 max-w-md">
          <div className="flex items-baseline justify-between text-[10px] uppercase tracking-widest text-slate-500">
            <span>Capital Used</span>
            <span className="tabular-nums text-slate-300">
              {capitalUsedPct.toFixed(1)}% / {capitalLimitPct.toFixed(0)}%
            </span>
          </div>
          <div className="mt-1 h-2 overflow-hidden rounded-full bg-slate-800">
            <div
              className={cn(
                'h-full rounded-full',
                capitalUsedPct >= capitalLimitPct ? 'bg-rose-500/70' : 'bg-emerald-500/70',
              )}
              style={{ width: `${Math.min(100, (capitalUsedPct / capitalLimitPct) * 100)}%` }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
