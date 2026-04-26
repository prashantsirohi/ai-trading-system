/**
 * The four-stage lifecycle chain shown in the expanded ranking row:
 * ``rank → breakout → pattern → execution``. State per stage comes from the
 * backend ``lifecycle`` block.
 */
import type { LifecycleStage } from '@/lib/api/ranking';
import { cn } from '@/lib/utils/cn';

const STATE_TONES: Record<LifecycleStage['state'], { dot: string; ring: string; label: string }> = {
  complete: {
    dot: 'bg-emerald-500',
    ring: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200',
    label: 'Complete',
  },
  active: {
    dot: 'bg-blue-500',
    ring: 'border-blue-500/40 bg-blue-500/15 text-blue-200',
    label: 'Active',
  },
  blocked: {
    dot: 'bg-rose-500',
    ring: 'border-rose-500/40 bg-rose-500/15 text-rose-200',
    label: 'Blocked',
  },
  pending: {
    dot: 'bg-slate-600',
    ring: 'border-slate-700 bg-slate-900/60 text-slate-400',
    label: 'Pending',
  },
};

export default function LifecycleVisual({ stages }: { stages: LifecycleStage[] }) {
  return (
    <ol className="flex flex-wrap items-stretch gap-3">
      {stages.map((stage, idx) => {
        const tone = STATE_TONES[stage.state];
        return (
          <li
            key={stage.key}
            className={cn(
              'flex min-w-[180px] flex-1 items-center gap-3 rounded-lg border p-3',
              tone.ring,
            )}
          >
            <span
              className={cn('h-2.5 w-2.5 shrink-0 rounded-full', tone.dot)}
              aria-hidden="true"
            />
            <div className="min-w-0">
              <div className="flex items-baseline gap-2">
                <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-500">
                  Step {idx + 1}
                </span>
                <span className="text-sm font-semibold text-slate-200">{stage.label}</span>
              </div>
              <div className="mt-0.5 flex flex-col gap-0.5">
                <span className="text-[10px] uppercase tracking-wider opacity-80">{tone.label}</span>
                {stage.detail ? (
                  <span className="truncate text-xs text-slate-300" title={stage.detail}>
                    {stage.detail}
                  </span>
                ) : null}
              </div>
            </div>
          </li>
        );
      })}
    </ol>
  );
}
