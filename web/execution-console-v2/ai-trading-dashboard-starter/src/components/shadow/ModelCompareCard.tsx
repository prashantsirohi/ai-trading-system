import { cn } from '@/lib/utils/cn';

export interface ModelStats {
  sharpe: number;
  winRate: number;
  maxDd: number;
  picksToday: number;
  topTierA: number;
}

interface Props {
  variant: 'a' | 'b';
  name: string;
  subtitle: string;
  stats: ModelStats;
  deltas?: Partial<ModelStats>;
  statusLabel: string;
}

const STAT_ROWS: { key: keyof ModelStats; label: string; fmt: (v: number) => string }[] = [
  { key: 'sharpe',     label: 'Sharpe (90d)',  fmt: (v) => v.toFixed(2) },
  { key: 'winRate',    label: 'Win rate',       fmt: (v) => `${v}%` },
  { key: 'maxDd',      label: 'Max DD',         fmt: (v) => `${v.toFixed(1)}%` },
  { key: 'picksToday', label: 'Picks today',    fmt: (v) => String(v) },
  { key: 'topTierA',   label: 'Top tier A',     fmt: (v) => String(v) },
];

function deltaStr(key: keyof ModelStats, delta: number): string {
  if (key === 'winRate') return `${delta > 0 ? '+' : ''}${delta}pp`;
  if (key === 'sharpe')  return `${delta > 0 ? '+' : ''}${delta.toFixed(2)}`;
  if (key === 'maxDd')   return `${delta > 0 ? '+' : ''}${delta.toFixed(1)}`;
  return `${delta > 0 ? '+' : ''}${delta}`;
}

export default function ModelCompareCard({ variant, name, subtitle, stats, deltas, statusLabel }: Props) {
  const isB = variant === 'b';
  return (
    <div
      className={cn(
        'rounded-2xl border p-4',
        isB ? 'border-indigo-700/50 bg-indigo-950/20' : 'border-slate-700 bg-slate-900/60',
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-100">
            {name}
            {isB && (
              <span className="rounded-full border border-indigo-600/50 bg-indigo-500/15 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-indigo-300">
                NEW
              </span>
            )}
          </div>
          <div className="mt-0.5 font-mono text-[10px] text-slate-500">{subtitle}</div>
        </div>
        <span
          className={cn(
            'shrink-0 rounded-full border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider',
            isB
              ? 'border-indigo-600/50 bg-indigo-500/15 text-indigo-300'
              : 'border-emerald-700/50 bg-emerald-500/10 text-emerald-300',
          )}
        >
          {statusLabel}
        </span>
      </div>

      <div className="mt-4 space-y-2">
        {STAT_ROWS.map(({ key, label, fmt }) => {
          const value = stats[key];
          const delta = deltas?.[key];
          const isNeg = key === 'maxDd';
          return (
            <div key={key} className="grid grid-cols-[1fr_auto_auto] items-center gap-3">
              <span className="text-xs text-slate-400">{label}</span>
              <span
                className={cn(
                  'font-mono text-xs',
                  isNeg ? 'text-rose-300' : 'text-slate-200',
                )}
              >
                {fmt(value)}
              </span>
              {delta !== undefined ? (
                <span
                  className={cn(
                    'w-16 text-right font-mono text-[10px]',
                    delta > 0 && !isNeg ? 'text-emerald-400' :
                    delta < 0 && !isNeg ? 'text-rose-400' :
                    delta < 0 && isNeg  ? 'text-rose-400' :
                    'text-slate-500',
                  )}
                >
                  {deltaStr(key, delta)}
                </span>
              ) : (
                <span className="w-16" />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
