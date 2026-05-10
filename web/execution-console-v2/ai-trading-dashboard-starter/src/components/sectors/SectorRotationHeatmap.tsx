/**
 * Sector rotation heatmap.
 *
 * The backend doesn't expose per-day historical RS rank yet, so we
 * synthesise a 5-step series client-side from the rolling RS columns we
 * *do* have on each row: ``rs100 → rs50 → rs20 → rs → momentum-adjusted``.
 * The labels intentionally use rolling windows rather than D-5..D-1 because
 * this is not true daily history.
 */
import { Link } from 'react-router-dom';
import type { SectorScore } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sectors: SectorScore[];
  selected: string | null;
  onSelect: (sector: string) => void;
}

const COLUMNS = ['RS100', 'RS50', 'RS20', 'Latest', 'Mom'];

function normalizeScore(value: number): number {
  if (!Number.isFinite(value)) return 0;
  const scaled = Math.abs(value) <= 1.5 ? value * 100 : value;
  return Math.max(0, Math.min(100, scaled));
}

function dotsFor(s: SectorScore): number[] {
  const current = normalizeScore(s.rs);
  const momentumDelta = Math.abs(s.momentum) <= 1.5 ? s.momentum * 20 : s.momentum;
  return [
    normalizeScore(s.rs100),
    normalizeScore(s.rs50),
    normalizeScore(s.rs20),
    current,
    Math.max(0, Math.min(100, current + momentumDelta)),
  ];
}

function dotTone(value: number): string {
  if (value >= 80) return 'bg-emerald-500';
  if (value >= 65) return 'bg-emerald-500/60';
  if (value >= 50) return 'bg-amber-500/60';
  if (value >= 35) return 'bg-rose-500/60';
  return 'bg-rose-500';
}

export default function SectorRotationHeatmap({ sectors, selected, onSelect }: Props) {
  if (sectors.length === 0) return null;
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[480px] text-left text-xs">
        <thead>
          <tr className="text-[10px] uppercase tracking-widest text-slate-500">
            <th className="px-3 py-2 font-medium">Sector</th>
            {COLUMNS.map((c) => (
              <th key={c} className="px-2 py-2 text-center font-medium">
                {c}
              </th>
            ))}
            <th className="px-3 py-2 text-right font-medium">Quadrant</th>
          </tr>
        </thead>
        <tbody>
          {sectors.map((s) => {
            const isSelected = selected === s.sector;
            const dots = dotsFor(s);
            return (
              <tr
                key={s.sector}
                className={cn(
                  'cursor-pointer border-t border-slate-800 transition-colors hover:bg-slate-800/40',
                  isSelected ? 'bg-blue-500/10' : '',
                )}
                onClick={() => onSelect(s.sector)}
              >
                <td className="px-3 py-2">
                  <Link
                    to={`/sectors/${encodeURIComponent(s.sector)}`}
                    onClick={(e) => e.stopPropagation()}
                    className="font-semibold text-slate-200 hover:text-blue-400 hover:underline transition-colors"
                  >
                    {s.sector}
                  </Link>
                </td>
                {dots.map((value, idx) => (
                  <td key={idx} className="px-2 py-2 text-center">
                    <span
                      className={cn(
                        'mx-auto inline-block h-3.5 w-3.5 rounded-full',
                        dotTone(value),
                      )}
                      title={`${COLUMNS[idx]}: ${value.toFixed(0)}`}
                      aria-label={`${COLUMNS[idx]} ${value.toFixed(0)}`}
                    />
                  </td>
                ))}
                <td className="px-3 py-2 text-right text-slate-400">{s.quadrant}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="mt-2 text-[11px] text-slate-500">
        Rolling RS view: long-term, medium-term, short-term, latest strength, and
        momentum-adjusted strength. Colors: green &gt;=80, soft green 65-79,
        amber 50-64, soft red 35-49, red &lt;35.
      </p>
    </div>
  );
}
