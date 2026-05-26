import type { SectorScore } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sectors: SectorScore[];
  selected: string | null;
  onSelect: (sector: string) => void;
}

function formatPct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return `${(value * 100).toFixed(1)}%`;
}

function formatBreadth(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return `${value.toFixed(0)}%`;
}

function formatScore(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return value.toFixed(0);
}

function trendLabel(value: string | null | undefined): string {
  if (!value) return '—';
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function trendTone(value: string | null | undefined): string {
  switch (value) {
    case 'accelerating_leader':
      return 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200';
    case 'earnings_recovery':
      return 'border-sky-500/40 bg-sky-500/10 text-sky-200';
    case 'growth_but_margin_pressure':
      return 'border-amber-500/40 bg-amber-500/10 text-amber-200';
    case 'weak_or_declining':
      return 'border-rose-500/40 bg-rose-500/10 text-rose-200';
    default:
      return 'border-slate-700 bg-slate-900 text-slate-400';
  }
}

export default function SectorEarningsLeadershipTable({ sectors, selected, onSelect }: Props) {
  const rows = sectors
    .filter((sector) => sector.sectorEarningsGrowthScore !== null && sector.sectorEarningsGrowthScore !== undefined)
    .sort((a, b) => (b.sectorEarningsGrowthScore ?? -1) - (a.sectorEarningsGrowthScore ?? -1));

  if (rows.length === 0) {
    return (
      <div className="rounded-lg border border-slate-800 bg-slate-950/50 px-4 py-6 text-sm text-slate-400">
        Sector earnings leadership rows will appear after the earnings feature refresh writes sector tables.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full border-separate border-spacing-0 text-sm">
        <thead>
          <tr className="text-left text-xs uppercase text-slate-500">
            <th className="px-3 py-2 text-right font-medium">Rank</th>
            <th className="sticky left-0 z-10 bg-slate-950 px-3 py-2 font-medium">Sector</th>
            <th className="px-3 py-2 text-right font-medium">Sales YoY</th>
            <th className="px-3 py-2 text-right font-medium">Profit YoY</th>
            <th className="px-3 py-2 text-right font-medium">Sales QoQ</th>
            <th className="px-3 py-2 text-right font-medium">Profit QoQ</th>
            <th className="px-3 py-2 text-right font-medium">Sales Breadth</th>
            <th className="px-3 py-2 text-right font-medium">Profit Breadth</th>
            <th className="px-3 py-2 text-right font-medium">Margin Exp.</th>
            <th className="px-3 py-2 text-right font-medium">Score</th>
            <th className="px-3 py-2 font-medium">Trend</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => {
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
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums text-slate-400">
                  {idx + 1}
                </td>
                <td className="sticky left-0 z-10 border-t border-slate-800 bg-inherit px-3 py-3 font-semibold text-slate-100">
                  {row.sector}
                </td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">{formatPct(row.sectorSalesYoyGrowth)}</td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">{formatPct(row.sectorProfitYoyGrowth)}</td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">{formatPct(row.sectorSalesQoqGrowth)}</td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">{formatPct(row.sectorProfitQoqGrowth)}</td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">{formatBreadth(row.salesYoyPositivePct)}</td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">{formatBreadth(row.profitYoyPositivePct)}</td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums">{formatBreadth(row.marginExpansionPct)}</td>
                <td className="border-t border-slate-800 px-3 py-3 text-right tabular-nums font-semibold text-slate-100">
                  {formatScore(row.sectorEarningsGrowthScore)}
                </td>
                <td className="border-t border-slate-800 px-3 py-3">
                  <span className={cn('inline-flex whitespace-nowrap rounded-full border px-2 py-0.5 text-xs font-medium', trendTone(row.earningsTrendLabel))}>
                    {trendLabel(row.earningsTrendLabel)}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
