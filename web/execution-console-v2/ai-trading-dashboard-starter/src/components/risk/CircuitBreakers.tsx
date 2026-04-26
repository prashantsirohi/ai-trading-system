/**
 * Circuit breakers list (Quantis proposal #03).
 */
import { cn } from '@/lib/utils/cn';
import type { CircuitBreaker, CircuitState } from '@/lib/risk/derive';

const STATE_PILL: Record<CircuitState, string> = {
  armed:     'border-slate-700 bg-slate-800/50 text-slate-400',
  triggered: 'border-amber-700/60 bg-amber-500/15 text-amber-300',
  inactive:  'border-slate-800 text-slate-600',
};

const STATE_LABEL: Record<CircuitState, string> = {
  armed:     'Armed',
  triggered: 'Triggered',
  inactive:  'Inactive',
};

interface Props {
  breakers: CircuitBreaker[];
}

export default function CircuitBreakers({ breakers }: Props) {
  return (
    <ul className="space-y-2">
      {breakers.map((b) => (
        <li
          key={b.id}
          className={cn(
            'rounded-xl border bg-slate-950/50 px-4 py-3',
            b.state === 'triggered'
              ? 'border-amber-700/50'
              : 'border-slate-800',
          )}
        >
          <div className="flex items-center justify-between gap-3">
            <span className="text-sm font-semibold text-slate-100">{b.label}</span>
            <span
              className={cn(
                'shrink-0 rounded-full border px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
                STATE_PILL[b.state],
              )}
            >
              {STATE_LABEL[b.state]}
            </span>
          </div>
          <p className="mt-1 text-[11px] text-slate-500">{b.action}</p>
        </li>
      ))}
    </ul>
  );
}
