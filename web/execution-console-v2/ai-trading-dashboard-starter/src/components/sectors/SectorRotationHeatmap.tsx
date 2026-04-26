/**
 * Sector rotation heatmap (D-5..D-1 dot grid).
 *
 * The backend doesn't expose per-day historical RS rank yet, so we
 * synthesise a 5-step series client-side from the rolling RS columns we
 * *do* have on each row: ``rs100 → rs50 → rs20 → rs → momentum-adjusted``.
 * The aim is to give the operator a directional rotation read until a
 * proper history endpoint lands.
 */
import type { SectorScore } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sectors: SectorScore[];
  selected: string | null;
  onSelect: (sector: string) => void;
}

const COLUMNS = ['D-5', 'D-4', 'D-3', 'D-2', 'D-1'];

function dotsFor(s: SectorScore): number[] {
  return [
    s.rs100,
    s.rs50,
    s.rs20,
    s.rs,
    Math.max(0, Math.min(100, s.rs + s.momentum * 20)),
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
                <td className="px-3 py-2 font-semibold text-slate-200">{s.sector}</td>
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
        D-5 anchors on rolling RS-100; D-1 incorporates the latest momentum delta. Until a
        per-day history endpoint lands this is a synthetic but directional read.
      </p>
    </div>
  );
}
