/**
 * Banner highlighting the earliest emerging sector leader.
 *
 * "Early leader" heuristic: a sector with positive momentum that *isn't*
 * yet at the top of the rankPct table — i.e. starting to rotate. Surfaces
 * the strongest such candidate so the operator sees rotation before it
 * dominates the leadership chart. When nothing qualifies (or sector data
 * is empty) the component renders nothing.
 */
import type { SectorScore } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sectors: SectorScore[];
}

function pickEarlyLeader(sectors: SectorScore[]): SectorScore | null {
  // Sort by momentum descending, then prefer sectors that are *not* in the
  // top-2 by rs rank — that's where the rotation alpha lives.
  const candidates = sectors
    .filter((s) => s.momentum > 0 && s.rank > 2)
    .sort((a, b) => b.momentum - a.momentum);
  return candidates[0] ?? null;
}

export default function EarlyLeaderBanner({ sectors }: Props) {
  const leader = pickEarlyLeader(sectors);
  if (!leader) return null;

  return (
    <div
      className={cn(
        'flex flex-wrap items-center justify-between gap-3 rounded-xl border border-blue-500/30 bg-blue-950/30 p-4',
      )}
    >
      <div className="flex items-center gap-3">
        <span className="rounded-full border border-blue-500/40 bg-blue-500/15 px-2.5 py-1 text-[10px] font-bold uppercase tracking-widest text-blue-200">
          Early Leader
        </span>
        <div>
          <h3 className="text-sm font-semibold text-slate-100">{leader.sector}</h3>
          <p className="text-xs text-slate-400">
            Momentum {leader.momentum > 0 ? '+' : ''}
            {leader.momentum.toFixed(2)} · Quadrant {leader.quadrant} · RS rank #{leader.rank}
          </p>
        </div>
      </div>
      <p className="max-w-md text-xs text-slate-300">
        Rising momentum from outside the leadership tier — typically the earliest signal of
        a sector rotation. Watch for breadth confirmation before sizing up.
      </p>
    </div>
  );
}
