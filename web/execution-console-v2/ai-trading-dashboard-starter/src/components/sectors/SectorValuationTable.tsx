import type { SectorScore } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sectors: SectorScore[];
  selected: string | null;
  onSelect: (sector: string) => void;
}

function formatNumber(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return value.toFixed(digits);
}

function formatPctile(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return Math.round(value).toString();
}

function zoneLabel(zone: string | null | undefined): string {
  if (!zone || zone === 'unknown') return '—';
  return zone
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function zoneTone(zone: string | null | undefined): string {
  switch ((zone ?? '').toLowerCase()) {
    case 'depressed':
    case 'cheap':
      return 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200';
    case 'fair':
      return 'border-sky-500/40 bg-sky-500/10 text-sky-200';
    case 'expensive':
      return 'border-amber-500/40 bg-amber-500/10 text-amber-200';
    case 'bubble':
      return 'border-rose-500/40 bg-rose-500/10 text-rose-200';
    default:
      return 'border-slate-700 bg-slate-900 text-slate-400';
  }
}

function interpretation(row: SectorScore): string {
  if (row.valuationInterpretation) return row.valuationInterpretation;
  if (row.sectorPeTtm == null) return 'Valuation data pending';
  if (!row.valuationZone || row.valuationZone === 'unknown') return 'Current PE live; history bands pending';
  if ((row.sectorLossMcapPct ?? 0) >= 0.25) return 'PE less reliable; losses are material';
  if (row.rank <= 3 && ['expensive', 'bubble'].includes(String(row.valuationZone).toLowerCase())) {
    return 'Strong but late-cycle';
  }
  if (row.rank <= 3) return 'High RS with reasonable valuation';
  if (row.rank >= 8 && ['cheap', 'depressed', 'fair'].includes(String(row.valuationZone).toLowerCase())) {
    return 'Valuation reset, wait for RS turn';
  }
  return 'Momentum and valuation are balanced';
}

export default function SectorValuationTable({ sectors, selected, onSelect }: Props) {
  const rows = sectors
    .filter((sector) => sector.sectorPeTtm !== null && sector.sectorPeTtm !== undefined)
    .sort((a, b) => {
      const aPct = a.sectorPePctile5y ?? a.sectorPePctile3y ?? -1;
      const bPct = b.sectorPePctile5y ?? b.sectorPePctile3y ?? -1;
      return bPct - aPct || a.rank - b.rank;
    });

  if (rows.length === 0) {
    return (
      <div className="rounded-lg border border-slate-800 bg-slate-950/50 px-4 py-6 text-sm text-slate-400">
        Sector valuation rows will appear after the valuation feature refresh writes sector PE tables.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full border-separate border-spacing-0 text-sm">
        <thead>
          <tr className="text-left text-xs uppercase text-slate-500">
            <th className="sticky left-0 z-10 bg-slate-950 px-3 py-2 font-medium">Sector</th>
            <th className="px-3 py-2 text-right font-medium">PE</th>
            <th className="px-3 py-2 text-right font-medium">5Y PE %ile</th>
            <th className="px-3 py-2 text-right font-medium">RS Rank</th>
            <th className="px-3 py-2 font-medium">Zone</th>
            <th className="px-3 py-2 font-medium">Interpretation</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const isSelected = row.sector === selected;
            return (
              <tr
                key={row.sector}
                className={cn(
                  'cursor-pointer border-t border-slate-800 text-slate-200 transition-colors hover:bg-slate-900/70',
                  isSelected ? 'bg-slate-900' : 'bg-transparent',
                )}
                onClick={() => onSelect(row.sector)}
              >
                <td className="sticky left-0 z-10 border-t border-slate-800 bg-inherit px-3 py-3 font-semibold text-slate-100">
                  {row.sector}
                </td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">
                  {formatNumber(row.sectorPeTtm, 1)}
                </td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">
                  {formatPctile(row.sectorPePctile5y)}
                </td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">
                  {row.rank || '—'}
                </td>
                <td className="border-t border-slate-800 px-3 py-3">
                  <span className={cn('inline-flex rounded-full border px-2 py-0.5 text-xs font-medium', zoneTone(row.valuationZone))}>
                    {zoneLabel(row.valuationZone)}
                  </span>
                </td>
                <td className="max-w-md border-t border-slate-800 px-3 py-3 text-slate-300">
                  {interpretation(row)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
