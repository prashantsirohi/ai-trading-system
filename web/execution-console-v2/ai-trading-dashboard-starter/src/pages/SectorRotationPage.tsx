import { useMemo } from 'react';

import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import { useSectorRotation } from '@/lib/queries';
import type {
  DeliverySignalRow,
  SectorCustomIndexRow,
  SectorRotationRow,
  StockRotationRow,
} from '@/lib/api/sectorRotation';
import { cn } from '@/lib/utils/cn';

const quadrantTone: Record<string, string> = {
  Leading: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200',
  Improving: 'border-sky-500/40 bg-sky-500/10 text-sky-200',
  Weakening: 'border-amber-500/40 bg-amber-500/10 text-amber-200',
  Lagging: 'border-rose-500/40 bg-rose-500/10 text-rose-200',
};

export default function SectorRotationPage() {
  const rotationQuery = useSectorRotation();
  const data = rotationQuery.data;
  const sectors = data?.sectors ?? [];
  const stocks = data?.stocks ?? [];
  const watchlist = useMemo(
    () => stocks.filter((row) => row.watchlist_candidate).slice(0, 20),
    [stocks],
  );
  const quadrantCounts = useMemo(() => {
    const counts: Record<string, number> = { Leading: 0, Improving: 0, Weakening: 0, Lagging: 0 };
    sectors.forEach((sector) => {
      const key = sector.quadrant ?? 'Lagging';
      counts[key] = (counts[key] ?? 0) + 1;
    });
    return counts;
  }, [sectors]);

  return (
    <PageFrame
      title="Sector Rotation"
      description="RRG sectors, stock confirmation, and delivery behavior from the latest rank run."
      compactHeader
    >
      {rotationQuery.isLoading ? (
        <CardSkeleton />
      ) : rotationQuery.error ? (
        <ErrorStateView
          error={`Failed to load sector rotation: ${rotationQuery.error.message}`}
          onRetry={() => rotationQuery.refetch()}
        />
      ) : !data || (sectors.length === 0 && stocks.length === 0) ? (
        <EmptyState message="No sector rotation artifacts available in the latest rank run." />
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            {Object.entries(quadrantCounts).map(([quadrant, count]) => (
              <div key={quadrant} className={cn('rounded-lg border p-4', quadrantTone[quadrant])}>
                <div className="text-xs font-medium uppercase text-slate-400">{quadrant}</div>
                <div className="mt-2 text-3xl font-semibold text-white">{count}</div>
                <div className="mt-1 text-xs text-slate-400">{data.run_date ?? 'latest run'}</div>
              </div>
            ))}
          </div>

          <SectionCard title="Sector RRG" description={`Run ${data.run_id ?? 'latest'}`}>
            <SectorTable rows={sectors} />
          </SectionCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <SectionCard title="Watchlist Overlay" description="Leading or improving stocks with supportive sector and delivery context.">
              <StockTable rows={watchlist.length ? watchlist : stocks.slice(0, 20)} />
            </SectionCard>

            <SectionCard title="Delivery Signals" description="Accumulation and distribution from delivery percentage, volume, and price behavior.">
              <DeliveryTable rows={[...(data.accumulation ?? []), ...(data.distribution ?? [])].slice(0, 30)} />
            </SectionCard>
          </div>

          <SectionCard title="Custom Sector Indices" description="Latest sector index points used by the rotation sidecar.">
            <IndexTable rows={latestIndexRows(data.custom_indices ?? [])} />
          </SectionCard>
        </>
      )}
    </PageFrame>
  );
}

function SectorTable({ rows }: { rows: SectorRotationRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-3 py-2">Sector</th>
            <th className="px-3 py-2">Quadrant</th>
            <th className="px-3 py-2 text-right">RS Ratio</th>
            <th className="px-3 py-2 text-right">Momentum</th>
            <th className="px-3 py-2 text-right">20D Alpha</th>
            <th className="px-3 py-2">Bucket</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {rows.map((row) => (
            <tr key={row.industry ?? 'sector'} className="text-slate-200">
              <td className="px-3 py-2 font-medium">{row.industry}</td>
              <td className="px-3 py-2"><Badge label={row.quadrant ?? 'Lagging'} /></td>
              <td className="px-3 py-2 text-right">{fmt(row.rs_ratio)}</td>
              <td className="px-3 py-2 text-right">{fmt(row.rs_momentum)}</td>
              <td className="px-3 py-2 text-right">{pct(row.alpha_20d)}</td>
              <td className="px-3 py-2 text-slate-300">{row.outperformance_bucket ?? 'N/A'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StockTable({ rows }: { rows: StockRotationRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-3 py-2">Symbol</th>
            <th className="px-3 py-2">Sector</th>
            <th className="px-3 py-2">Stock</th>
            <th className="px-3 py-2 text-right">Score</th>
            <th className="px-3 py-2 text-right">Near High</th>
            <th className="px-3 py-2">Delivery</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {rows.map((row) => (
            <tr key={row.symbol ?? 'stock'} className="text-slate-200">
              <td className="px-3 py-2 font-medium">{row.symbol}</td>
              <td className="px-3 py-2">{row.industry}</td>
              <td className="px-3 py-2"><Badge label={row.quadrant ?? 'Lagging'} /></td>
              <td className="px-3 py-2 text-right">{fmt(row.rotation_adjusted_score)}</td>
              <td className="px-3 py-2 text-right">{fmt(row.near_52w_high_pct)}%</td>
              <td className="px-3 py-2">{row.delivery_signal ?? 'Neutral'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DeliveryTable({ rows }: { rows: DeliverySignalRow[] }) {
  if (rows.length === 0) return <EmptyState message="No accumulation or distribution signals." />;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-3 py-2">Symbol</th>
            <th className="px-3 py-2">Signal</th>
            <th className="px-3 py-2 text-right">Delivery Z</th>
            <th className="px-3 py-2 text-right">Volume Z</th>
            <th className="px-3 py-2 text-right">5D Return</th>
            <th className="px-3 py-2 text-right">Confidence</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {rows.map((row) => (
            <tr key={`${row.symbol}-${row.delivery_signal}`} className="text-slate-200">
              <td className="px-3 py-2 font-medium">{row.symbol}</td>
              <td className="px-3 py-2">{row.delivery_signal}</td>
              <td className="px-3 py-2 text-right">{fmt(row.delivery_pct_z20)}</td>
              <td className="px-3 py-2 text-right">{fmt(row.volume_z20)}</td>
              <td className="px-3 py-2 text-right">{pct(row.price_return_5d)}</td>
              <td className="px-3 py-2 text-right">{fmt(row.accumulation_score)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function IndexTable({ rows }: { rows: SectorCustomIndexRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-3 py-2">Date</th>
            <th className="px-3 py-2">Sector</th>
            <th className="px-3 py-2 text-right">Index</th>
            <th className="px-3 py-2">Weighting</th>
            <th className="px-3 py-2 text-right">Constituents</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {rows.map((row) => (
            <tr key={`${row.date}-${row.industry}`} className="text-slate-200">
              <td className="px-3 py-2">{row.date}</td>
              <td className="px-3 py-2 font-medium">{row.industry}</td>
              <td className="px-3 py-2 text-right">{fmt(row.sector_index)}</td>
              <td className="px-3 py-2">{row.weighting_method ?? 'equal_weight'}</td>
              <td className="px-3 py-2 text-right">{row.constituent_count ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Badge({ label }: { label: string }) {
  return (
    <span className={cn('inline-flex rounded-full border px-2 py-1 text-xs font-medium', quadrantTone[label] ?? 'border-slate-700 bg-slate-900 text-slate-300')}>
      {label}
    </span>
  );
}

function latestIndexRows(rows: SectorCustomIndexRow[]): SectorCustomIndexRow[] {
  const bySector = new Map<string, SectorCustomIndexRow>();
  rows.forEach((row) => {
    const key = row.industry ?? '';
    if (!key) return;
    const current = bySector.get(key);
    if (!current || String(row.date ?? '') >= String(current.date ?? '')) {
      bySector.set(key, row);
    }
  });
  return Array.from(bySector.values()).sort((a, b) => String(a.industry ?? '').localeCompare(String(b.industry ?? '')));
}

function fmt(value: number | null | undefined): string {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(2) : 'N/A';
}

function pct(value: number | null | undefined): string {
  const num = Number(value);
  return Number.isFinite(num) ? `${(num * 100).toFixed(2)}%` : 'N/A';
}
