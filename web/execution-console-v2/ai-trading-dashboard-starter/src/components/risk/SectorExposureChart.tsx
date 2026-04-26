/**
 * Sector exposure horizontal bars with cap markers (proposal #03).
 */
import { cn } from '@/lib/utils/cn';
import type { SectorExposureRow } from '@/lib/risk/derive';

interface Props {
  rows: SectorExposureRow[];
}

export default function SectorExposureChart({ rows }: Props) {
  if (rows.length === 0) {
    return <p className="text-sm text-slate-500">No sector data available.</p>;
  }

  // Scale bars to the largest value/cap for visual proportion.
  const maxVal = Math.max(...rows.map((r) => Math.max(r.value, r.cap)));

  return (
    <ul className="space-y-2.5">
      {rows.map((row) => {
        const fillPct = (row.value / maxVal) * 100;
        const capPct  = (row.cap  / maxVal) * 100;

        return (
          <li key={row.sector} className="grid grid-cols-[90px_1fr_56px] items-center gap-3 text-xs">
            <span className="truncate text-slate-300">{row.sector}</span>

            {/* Track */}
            <div className="relative h-3.5 overflow-hidden rounded border border-slate-800 bg-slate-950/40">
              <div
                className={cn(
                  'h-full rounded transition-all',
                  row.overCap ? 'bg-rose-500' : 'bg-blue-500',
                )}
                style={{ width: `${fillPct}%` }}
              />
              {/* Cap marker */}
              <div
                className="absolute top-0 h-full w-0.5 bg-amber-400"
                style={{ left: `${capPct}%` }}
              />
            </div>

            <span className="text-right font-mono text-[10px] text-slate-400">
              {row.value}%/{row.cap}%
            </span>
          </li>
        );
      })}
    </ul>
  );
}
