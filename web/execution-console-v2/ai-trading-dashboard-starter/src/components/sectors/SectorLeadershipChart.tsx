/**
 * Sector leadership chart.
 *
 * For each sector we render two stacked bars:
 *
 *   * Capital flow proxy — `rs` (0..100).
 *   * Breadth proxy — `momentumRank` inverted to 0..100 (lower rank = more
 *     concentrated breadth).
 *
 * Plus a constituents-count badge sourced from the ranking feed, so the
 * operator sees how many ranked symbols actually live inside each sector.
 *
 * Clicking a row drives the parent's drill-down state.
 */
import { Link } from 'react-router-dom';
import type { SectorScore, StockRow } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sectors: SectorScore[];
  rankedRows: StockRow[];
  selected: string | null;
  onSelect: (sector: string) => void;
}

function constituentCount(sector: string, rows: StockRow[]): number {
  return rows.filter((row) => row.sector === sector).length;
}

function quadrantTone(quadrant: string): string {
  const norm = quadrant.toLowerCase();
  if (norm === 'leading') return 'border-emerald-500/40 bg-emerald-500/15 text-emerald-200';
  if (norm === 'improving') return 'border-blue-500/40 bg-blue-500/15 text-blue-200';
  if (norm === 'weakening') return 'border-amber-500/40 bg-amber-500/15 text-amber-200';
  if (norm === 'lagging') return 'border-rose-500/40 bg-rose-500/15 text-rose-200';
  return 'border-slate-700 bg-slate-800 text-slate-300';
}

export default function SectorLeadershipChart({ sectors, rankedRows, selected, onSelect }: Props) {
  if (sectors.length === 0) return null;
  const maxMomentumRank = Math.max(...sectors.map((s) => s.momentumRank), 1);

  return (
    <ul className="space-y-2">
      {sectors.map((s) => {
        const breadth = Math.max(0, Math.round((1 - s.momentumRank / maxMomentumRank) * 100));
        const isSelected = selected === s.sector;
        const constituents = constituentCount(s.sector, rankedRows);
        return (
          <li key={s.sector}>
            <button
              type="button"
              onClick={() => onSelect(s.sector)}
              className={cn(
                'flex w-full flex-col gap-2 rounded-xl border p-3 text-left transition-colors',
                isSelected
                  ? 'border-blue-500/40 bg-blue-500/10'
                  : 'border-slate-800 bg-slate-950/60 hover:border-slate-600',
              )}
            >
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <Link
                    to={`/sectors/${encodeURIComponent(s.sector)}`}
                    onClick={(e) => e.stopPropagation()}
                    className="text-sm font-semibold text-slate-100 hover:text-blue-400 hover:underline transition-colors"
                  >
                    {s.sector}
                  </Link>
                  <span
                    className={cn(
                      'rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
                      quadrantTone(s.quadrant),
                    )}
                  >
                    {s.quadrant}
                  </span>
                </div>
                <div className="flex items-center gap-3 text-xs text-slate-400">
                  <span>
                    <span className="text-slate-500">Constituents:</span>{' '}
                    <span className="font-semibold tabular-nums text-slate-200">{constituents}</span>
                  </span>
                  <span>
                    <span className="text-slate-500">Rank:</span>{' '}
                    <span className="font-semibold tabular-nums text-slate-200">#{s.rank}</span>
                  </span>
                </div>
              </div>
              <div className="space-y-1">
                <div className="flex items-baseline justify-between text-[10px] uppercase tracking-wider text-slate-500">
                  <span>Capital flow (RS)</span>
                  <span className="tabular-nums text-slate-300">{Math.round(s.rs)}</span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-slate-800">
                  <div
                    className="h-full rounded-full bg-emerald-500/70"
                    style={{ width: `${Math.max(0, Math.min(100, s.rs))}%` }}
                  />
                </div>
                <div className="mt-1 flex items-baseline justify-between text-[10px] uppercase tracking-wider text-slate-500">
                  <span>Breadth proxy</span>
                  <span className="tabular-nums text-slate-300">{breadth}</span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-slate-800">
                  <div
                    className="h-full rounded-full bg-blue-500/70"
                    style={{ width: `${breadth}%` }}
                  />
                </div>
              </div>
            </button>
          </li>
        );
      })}
    </ul>
  );
}
