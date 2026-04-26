/**
 * Compact per-symbol stage progression strip.
 *
 * Each row maps to the four lifecycle stages used elsewhere in the
 * dashboard (rank → breakout → pattern → execution). Stage state is
 * derived from the underlying StockRow; the eventual backend execution
 * endpoint will populate these directly.
 */
import type { StockRow } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

type StageState = 'pending' | 'active' | 'complete' | 'blocked';

interface Stage {
  key: 'rank' | 'breakout' | 'pattern' | 'execution';
  label: string;
  state: StageState;
}

function stagesFor(row: StockRow): Stage[] {
  const breakout: StageState = row.breakout ? 'complete' : 'pending';
  const pattern: StageState = row.pattern && row.pattern !== 'N/A' ? 'complete' : 'pending';
  let execution: StageState = 'pending';
  if (row.tier === 'A' && row.breakout) execution = 'active';
  if (row.tier === 'C') execution = 'blocked';
  return [
    { key: 'rank', label: 'Rank', state: 'complete' },
    { key: 'breakout', label: 'Breakout', state: breakout },
    { key: 'pattern', label: 'Pattern', state: pattern },
    { key: 'execution', label: 'Execution', state: execution },
  ];
}

const STATE_TONES: Record<StageState, string> = {
  complete: 'bg-emerald-500',
  active: 'bg-blue-500 animate-pulse',
  blocked: 'bg-rose-500',
  pending: 'bg-slate-600',
};

interface Props {
  rows: StockRow[];
  limit?: number;
}

export default function LiveTimeline({ rows, limit = 8 }: Props) {
  const visible = rows.slice(0, limit);
  if (visible.length === 0) {
    return (
      <p className="text-xs text-slate-500">No symbols active in the live timeline.</p>
    );
  }
  return (
    <ul className="space-y-2">
      {visible.map((row) => {
        const stages = stagesFor(row);
        return (
          <li
            key={row.symbol}
            className="flex items-center gap-3 rounded-lg border border-slate-800 bg-slate-950/60 p-2"
          >
            <span className="w-20 shrink-0 text-xs font-semibold text-slate-200">
              {row.symbol}
            </span>
            <div className="flex flex-1 items-center gap-2">
              {stages.map((stage, idx) => (
                <div key={stage.key} className="flex flex-1 items-center gap-2">
                  <span
                    className={cn(
                      'h-2 w-2 shrink-0 rounded-full',
                      STATE_TONES[stage.state],
                    )}
                    title={`${stage.label}: ${stage.state}`}
                  />
                  <span className="hidden text-[10px] uppercase tracking-wider text-slate-500 sm:inline">
                    {stage.label}
                  </span>
                  {idx < stages.length - 1 ? (
                    <span className="h-px flex-1 bg-slate-800" />
                  ) : null}
                </div>
              ))}
            </div>
          </li>
        );
      })}
    </ul>
  );
}
