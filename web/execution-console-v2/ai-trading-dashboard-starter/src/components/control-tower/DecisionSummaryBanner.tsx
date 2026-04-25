/**
 * Decision Summary banner — the gradient strip at the top of the Control
 * Tower. Renders the top-N actions from ``/workspace/snapshot`` as
 * clickable chips. The verdict colour and confidence label come straight
 * from the backend so the UI stays in lockstep with the readmodel.
 */
import { TargetIcon } from './icons';
import type { WorkspaceTopAction } from '@/lib/api/workspace';
import { cn } from '@/lib/utils/cn';

interface Props {
  actions: WorkspaceTopAction[];
  /** Click handler — receives the symbol so the parent can open the detail. */
  onSelect?: (symbol: string) => void;
}

function verdictTone(verdict: string | null): string {
  if (!verdict) return 'bg-slate-800 text-slate-300 border-slate-700';
  const upper = verdict.toUpperCase();
  if (upper.includes('BUY')) return 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30';
  if (upper.includes('WATCH') || upper.includes('HOLD'))
    return 'bg-amber-500/15 text-amber-300 border-amber-500/30';
  if (upper.includes('REJECT') || upper.includes('BLOCK'))
    return 'bg-rose-500/15 text-rose-300 border-rose-500/30';
  return 'bg-blue-500/15 text-blue-300 border-blue-500/30';
}

export default function DecisionSummaryBanner({ actions, onSelect }: Props) {
  return (
    <div
      className={cn(
        'relative overflow-hidden rounded-2xl border border-blue-500/30',
        'bg-gradient-to-r from-blue-900/40 to-indigo-900/40 p-4',
        'shadow-[0_0_30px_rgba(59,130,246,0.15)]',
      )}
    >
      <div className="pointer-events-none absolute -right-20 -top-20 h-64 w-64 rounded-full bg-blue-500/10 blur-3xl" />

      <div className="relative z-10 flex items-center gap-6">
        <div className="flex shrink-0 flex-col border-r border-blue-500/20 pr-6">
          <span className="flex items-center gap-1 text-xs font-bold uppercase tracking-widest text-blue-400">
            <TargetIcon size={14} />
            Decision Summary
          </span>
          <span className="mt-1 text-lg font-bold text-white">Top Actions</span>
          <span className="mt-1 text-[10px] uppercase tracking-wider text-slate-500">
            {actions.length} ranked
          </span>
        </div>

        <div className="grid min-w-0 flex-grow grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {actions.length === 0 ? (
            <div className="rounded-lg border border-dashed border-slate-700 bg-slate-900/40 p-3 text-sm text-slate-400">
              No top actions yet — the latest pipeline run hasn't produced
              ranked signals.
            </div>
          ) : (
            actions.map((action, i) => (
              <button
                key={action.symbol + ':' + i}
                type="button"
                onClick={() => onSelect?.(action.symbol)}
                className={cn(
                  'group flex min-w-0 items-center justify-between gap-3 rounded-lg border border-blue-500/20 bg-slate-900/60 p-3 text-left transition-colors',
                  'hover:border-blue-500/50 hover:bg-slate-800',
                )}
              >
                <div className="min-w-0">
                  <span className="font-bold text-slate-200">
                    {i + 1}. {action.symbol}
                  </span>
                  <div className="mt-0.5 truncate text-[10px] text-slate-400">
                    Conf: {action.confidence ?? '—'} · Score:{' '}
                    {action.compositeScore !== null
                      ? action.compositeScore.toFixed(1)
                      : '—'}{' '}
                    · {action.sectorName ?? 'Sector ?'}
                  </div>
                </div>
                <span
                  className={cn(
                    'shrink-0 rounded-md border px-2.5 py-1 text-xs font-bold uppercase tracking-wider',
                    verdictTone(action.verdict),
                  )}
                >
                  {action.verdict ?? 'PENDING'}
                </span>
              </button>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
