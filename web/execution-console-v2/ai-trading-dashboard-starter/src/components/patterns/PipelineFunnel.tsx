/**
 * Pipeline conversion funnel for the Patterns page.
 *
 * Renders four sequential stages (Universe → Pattern Found → Qualified
 * (RS>70) → Execution Ready) with a width proportional to count and a
 * conversion-percent caption between stages. Counts are computed by the
 * parent — this component is purely presentational.
 */
import { cn } from '@/lib/utils/cn';

export interface FunnelStage {
  key: string;
  label: string;
  count: number;
  hint?: string;
}

interface Props {
  stages: FunnelStage[];
}

const STAGE_TONES = [
  'border-blue-500/40 bg-blue-500/15 text-blue-100',
  'border-violet-500/40 bg-violet-500/15 text-violet-100',
  'border-amber-500/40 bg-amber-500/15 text-amber-100',
  'border-emerald-500/40 bg-emerald-500/15 text-emerald-100',
];

function pct(numerator: number, denominator: number): string {
  if (denominator <= 0) return '—';
  return `${Math.round((numerator / denominator) * 100)}%`;
}

export default function PipelineFunnel({ stages }: Props) {
  if (stages.length === 0) {
    return null;
  }
  const max = Math.max(...stages.map((s) => s.count), 1);

  return (
    <div className="space-y-2">
      {stages.map((stage, idx) => {
        const widthPct = Math.max(8, Math.round((stage.count / max) * 100));
        const tone = STAGE_TONES[idx] ?? STAGE_TONES[STAGE_TONES.length - 1];
        const previous = stages[idx - 1];
        const conversion = previous ? pct(stage.count, previous.count) : null;
        return (
          <div key={stage.key} className="space-y-1">
            <div className="flex items-baseline justify-between text-xs">
              <span className="font-semibold uppercase tracking-wider text-slate-400">
                {stage.label}
              </span>
              <span className="tabular-nums text-slate-300">
                {stage.count}
                {conversion ? (
                  <span className="ml-2 text-[11px] text-slate-500">{conversion} thru</span>
                ) : null}
              </span>
            </div>
            <div className="h-7 w-full overflow-hidden rounded-md border border-slate-800 bg-slate-950/40">
              <div
                className={cn(
                  'flex h-full items-center justify-center border-r border-current/30 px-3 text-xs font-semibold tracking-wider',
                  tone,
                )}
                style={{ width: `${widthPct}%` }}
              >
                {stage.hint ?? stage.label}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
