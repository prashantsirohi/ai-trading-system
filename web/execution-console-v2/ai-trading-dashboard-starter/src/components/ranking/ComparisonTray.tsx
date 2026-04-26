/**
 * Fixed-footer comparison tray for the Ranking view.
 *
 * Holds up to three symbols. The "Compare Factors" CTA is wired to a
 * placeholder handler — the actual modal lands in PR #12 (per
 * EXECUTION_CONSOLE_PLAN.md). Until then, clicking surfaces a brief
 * "coming soon" affordance via the parent's handler.
 */
import type { StockRow } from '@/types/dashboard';
import TierBadge from './TierBadge';
import { cn } from '@/lib/utils/cn';

export const COMPARISON_LIMIT = 3;

interface Props {
  rows: StockRow[];
  onRemove: (symbol: string) => void;
  onClear: () => void;
  onCompare: () => void;
  pendingCompareNotice: string | null;
}

export default function ComparisonTray({
  rows,
  onRemove,
  onClear,
  onCompare,
  pendingCompareNotice,
}: Props) {
  if (rows.length === 0) return null;
  return (
    <div className="pointer-events-none fixed inset-x-0 bottom-0 z-30 flex justify-center px-4 pb-4">
      <div
        className={cn(
          'pointer-events-auto flex w-full max-w-4xl flex-wrap items-center gap-3 rounded-2xl border border-slate-700 bg-slate-900/95 p-3 shadow-2xl backdrop-blur',
        )}
      >
        <span className="text-[10px] font-semibold uppercase tracking-widest text-slate-500">
          Compare ({rows.length}/{COMPARISON_LIMIT})
        </span>
        <ul className="flex flex-1 flex-wrap items-center gap-2">
          {rows.map((row) => (
            <li
              key={row.symbol}
              className="flex items-center gap-2 rounded-full border border-slate-700 bg-slate-950/60 py-1 pl-2 pr-1"
            >
              <TierBadge tier={row.tier} className="h-5 w-5 text-[10px]" />
              <span className="text-xs font-semibold text-slate-200">{row.symbol}</span>
              <span className="text-[10px] text-slate-500">{row.sector}</span>
              <button
                type="button"
                onClick={() => onRemove(row.symbol)}
                aria-label={`Remove ${row.symbol} from compare`}
                className="ml-1 rounded-full border border-slate-700 px-1.5 text-[10px] text-slate-400 hover:border-rose-500/50 hover:text-rose-300"
              >
                ×
              </button>
            </li>
          ))}
        </ul>
        <div className="flex items-center gap-2">
          {pendingCompareNotice ? (
            <span className="text-[11px] text-amber-300">{pendingCompareNotice}</span>
          ) : null}
          <button
            type="button"
            onClick={onClear}
            className="rounded-md border border-slate-700 px-3 py-1.5 text-xs font-semibold uppercase tracking-wider text-slate-300 hover:border-slate-500"
          >
            Clear
          </button>
          <button
            type="button"
            onClick={onCompare}
            disabled={rows.length < 2}
            className={cn(
              'rounded-md border px-3 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors',
              rows.length < 2
                ? 'cursor-not-allowed border-slate-800 bg-slate-900/40 text-slate-600'
                : 'border-blue-500/40 bg-blue-500/15 text-blue-200 hover:border-blue-500/60',
            )}
          >
            Compare Factors
          </button>
        </div>
      </div>
    </div>
  );
}
