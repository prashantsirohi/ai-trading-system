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
    <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-3 lg:grid-cols-5">
      {cells.map((cell) => (
        <div
          key={cell.label}
          className={cn(
            'rounded-xl border bg-slate-950/50 p-3',
            TONE_BORDER[cell.tone],
          )}
        >
          <div className="text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-500">
            {cell.label}
          </div>
          <div className={cn('mt-1 font-mono text-base font-semibold', TONE_TEXT[cell.tone])}>
            {cell.value}
          </div>
          <div className="mt-0.5 text-[10px] text-slate-500">{cell.hint}</div>
        </div>
      ))}
    </div>
  );
}
