/**
 * Data-quality strip (Quantis proposal #07).
 *
 * Five horizontal cells inserted between the run timeline and the metric
 * grid. Each cell tones pass / warn / fail off the derived DQ signals.
 */
import { cn } from '@/lib/utils/cn';
import type { DqCell } from '@/lib/pipeline/dq';

const TONE_BORDER = {
  pass: 'border-emerald-700/60',
  warn: 'border-amber-700/60',
  fail: 'border-rose-700/60',
} as const;

const TONE_TEXT = {
  pass: 'text-emerald-300',
  warn: 'text-amber-300',
  fail: 'text-rose-300',
} as const;

interface Props {
  cells: DqCell[];
}

export default function DataQualityStrip({ cells }: Props) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {cells.map((cell) => (
        <div
          key={cell.label}
          className={cn(
            'min-w-[92px] rounded-md border bg-slate-950/40 px-2 py-1.5',
            TONE_BORDER[cell.tone],
          )}
          title={cell.hint}
        >
          <div className="flex items-center justify-between gap-2">
            <span className="truncate text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-500">
              {cell.label}
            </span>
            <span className={cn('font-mono text-xs font-semibold', TONE_TEXT[cell.tone])}>
              {cell.value}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}
