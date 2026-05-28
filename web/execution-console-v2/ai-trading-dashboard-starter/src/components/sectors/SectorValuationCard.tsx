import type { SectorScore } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sector: SectorScore;
}

function fmt(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '-';
  return value.toFixed(digits);
}

function pct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '-';
  return `${value > 0 ? '+' : ''}${value.toFixed(1)}%`;
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

function label(zone: string | null | undefined): string {
  if (!zone) return '-';
  return zone.split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

function read(sector: SectorScore): string {
  const avg = sector.sectorPeVs5yAvgPct;
  const pctile = sector.sectorPePctile5y;
  if (avg == null) return '5Y valuation baseline pending';
  if (avg > 25 && (pctile ?? 0) >= 80) return 'High PE premium versus its own 5Y average';
  if (avg > 10) return 'Trading above its 5Y average PE';
  if (avg < -10) return 'Trading below its 5Y average PE';
  return 'Current PE is near its 5Y average';
}

export default function SectorValuationCard({ sector }: Props) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-950/55 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-[0.16em] text-slate-500">Selected Sector Valuation</div>
          <h3 className="mt-1 text-lg font-semibold text-slate-100">{sector.sector}</h3>
        </div>
        <span className={cn('rounded-full border px-2.5 py-1 text-xs font-medium', zoneTone(sector.valuationZone))}>
          {label(sector.valuationZone)}
        </span>
      </div>
      <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-7">
        <Metric label="Current PE" value={fmt(sector.sectorPeTtm)} />
        <Metric label="5Y Median PE" value={fmt(sector.sectorPe5yMedian)} />
        <Metric label="5Y Avg PE" value={fmt(sector.sectorPe5yAvg)} />
        <Metric label="Vs Median" value={pct(sector.sectorPeVs5yMedianPct)} />
        <Metric label="Vs Avg" value={pct(sector.sectorPeVs5yAvgPct)} />
        <Metric label="5Y Percentile" value={fmt(sector.sectorPePctile5y, 0)} />
        <Metric label="Loss Mcap" value={pct((sector.sectorLossMcapPct ?? null) != null ? Number(sector.sectorLossMcapPct) * 100 : null)} />
      </div>
      <div className="mt-3 text-sm text-slate-300">{read(sector)}</div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-slate-800 bg-slate-900/70 px-3 py-2">
      <div className="text-[11px] uppercase tracking-[0.12em] text-slate-500">{label}</div>
      <div className="mt-1 text-base font-semibold tabular-nums text-slate-100">{value}</div>
    </div>
  );
}
