import type { DeliverySignalRow, SectorRotationRow, StockRotationRow } from '@/lib/api/sectorRotation';

type Props = {
  group: SectorRotationRow | null;
  stocks: StockRotationRow[];
  accumulation: DeliverySignalRow[];
  distribution: DeliverySignalRow[];
  onClose: () => void;
};

export default function RotationDetailDrawer({ group, stocks, accumulation, distribution, onClose }: Props) {
  if (!group) return null;
  const name = group.rotation_group_name ?? group.industry ?? group.sector ?? 'Group';
  const matchingStocks = stocks
    .filter((stock) => stockMatchesGroup(stock, group))
    .sort((a, b) => Number(b.rotation_adjusted_score ?? 0) - Number(a.rotation_adjusted_score ?? 0))
    .slice(0, 30);
  const matchingSymbols = new Set(matchingStocks.map((stock) => stock.symbol).filter(Boolean));
  const deliveryRows = [...accumulation, ...distribution]
    .filter((row) => matchingSymbols.size > 0 && matchingSymbols.has(row.symbol))
    .slice(0, 12);
  const watchlistCandidates = matchingStocks.filter((stock) => stock.watchlist_candidate).slice(0, 12);

  return (
    <div className="fixed inset-0 z-50 bg-slate-950/70" onClick={onClose}>
      <aside
        className="ml-auto h-full w-full max-w-xl overflow-y-auto border-l border-slate-800 bg-slate-950 p-5 shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">{name}</h2>
            <p className="mt-1 text-sm text-slate-400">{group.parent_sector ?? group.sector ?? 'No parent sector'} · {group.quadrant ?? 'Lagging'}</p>
          </div>
          <button type="button" className="rounded-md border border-slate-700 px-3 py-1 text-sm text-slate-200 hover:bg-slate-800" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="mt-5 grid grid-cols-2 gap-3">
          <Metric label="RS Ratio" value={fmt(group.rs_ratio)} />
          <Metric label="Momentum" value={fmt(group.rs_momentum)} />
          <Metric label="5D Return" value={pct(group.return_5d ?? group.sector_return_5d)} />
          <Metric label="20D Return" value={pct(group.return_20d ?? group.sector_return_20d)} />
          <Metric label="60D Return" value={pct(group.return_60d ?? group.sector_return_60d)} />
          <Metric label="20D Alpha" value={pct(group.alpha_20d)} />
          <Metric label="Constituents" value={String(group.constituent_count ?? 'N/A')} />
        </div>

        <div className="mt-6">
          <h3 className="text-sm font-semibold uppercase text-slate-400">Watchlist Candidates</h3>
          <CompactStockList rows={watchlistCandidates} empty="No watchlist candidates for this group." />
        </div>

        <div className="mt-6">
          <h3 className="text-sm font-semibold uppercase text-slate-400">Stock Confirmations</h3>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="text-xs uppercase text-slate-500">
                <tr>
                  <th className="px-3 py-2">Symbol</th>
                  <th className="px-3 py-2">Industry</th>
                  <th className="px-3 py-2">Quadrant</th>
                  <th className="px-3 py-2 text-right">Score</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {matchingStocks.map((stock) => (
                  <tr key={stock.symbol ?? 'stock'} className="text-slate-200">
                    <td className="px-3 py-2 font-medium">{stock.symbol}</td>
                    <td className="px-3 py-2">{stock.industry ?? stock.sector}</td>
                    <td className="px-3 py-2">{stock.quadrant ?? 'Lagging'}</td>
                    <td className="px-3 py-2 text-right">{fmt(stock.rotation_adjusted_score)}</td>
                  </tr>
                ))}
                {matchingStocks.length === 0 ? (
                  <tr>
                    <td className="px-3 py-6 text-sm text-slate-400" colSpan={4}>No stock confirmations for this group.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>

        <div className="mt-6">
          <h3 className="text-sm font-semibold uppercase text-slate-400">Delivery Signals</h3>
          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="text-xs uppercase text-slate-500">
                <tr>
                  <th className="px-3 py-2">Symbol</th>
                  <th className="px-3 py-2">Signal</th>
                  <th className="px-3 py-2 text-right">Delivery Z</th>
                  <th className="px-3 py-2 text-right">5D Return</th>
                  <th className="px-3 py-2 text-right">Confidence</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {deliveryRows.map((row) => (
                  <tr key={`${row.symbol}-${row.delivery_signal}`} className="text-slate-200">
                    <td className="px-3 py-2 font-medium">{row.symbol}</td>
                    <td className="px-3 py-2">{row.delivery_signal}</td>
                    <td className="px-3 py-2 text-right">{fmt(row.delivery_pct_z20)}</td>
                    <td className="px-3 py-2 text-right">{pct(row.price_return_5d)}</td>
                    <td className="px-3 py-2 text-right">{fmt(row.accumulation_score)}</td>
                  </tr>
                ))}
                {deliveryRows.length === 0 ? (
                  <tr>
                    <td className="px-3 py-6 text-sm text-slate-400" colSpan={5}>No delivery signals for this group.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </aside>
    </div>
  );
}

function CompactStockList({ rows, empty }: { rows: StockRotationRow[]; empty: string }) {
  if (rows.length === 0) return <p className="mt-3 rounded-md border border-slate-800 bg-slate-900/50 px-3 py-4 text-sm text-slate-400">{empty}</p>;
  return (
    <div className="mt-3 grid gap-2">
      {rows.map((stock) => (
        <div key={stock.symbol ?? 'stock'} className="flex items-center justify-between gap-3 rounded-md border border-slate-800 bg-slate-900/50 px-3 py-2 text-sm">
          <div>
            <div className="font-medium text-slate-100">{stock.symbol}</div>
            <div className="text-xs text-slate-500">{stock.industry ?? stock.sector ?? 'Other'}</div>
          </div>
          <div className="text-right">
            <div className="font-medium text-slate-200">{fmt(stock.rotation_adjusted_score)}</div>
            <div className="text-xs text-slate-500">{stock.delivery_signal ?? 'Neutral'}</div>
          </div>
        </div>
      ))}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/50 p-3">
      <div className="text-xs uppercase text-slate-500">{label}</div>
      <div className="mt-1 text-lg font-semibold text-white">{value}</div>
    </div>
  );
}

function fmt(value: number | null | undefined): string {
  const num = toFiniteNumber(value);
  return num !== null ? num.toFixed(2) : 'N/A';
}

function pct(value: number | null | undefined): string {
  const num = toFiniteNumber(value);
  return num !== null ? `${(num * 100).toFixed(2)}%` : 'N/A';
}

function stockMatchesGroup(stock: StockRotationRow, group: SectorRotationRow): boolean {
  const name = group.rotation_group_name ?? group.industry ?? group.sector ?? '';
  if (group.rotation_group_type === 'industry') return stock.industry === name;
  if (group.rotation_group_type === 'sector') return stock.sector === name || stock.sector === group.parent_sector;
  return stock.industry === name || stock.sector === name || stock.sector === group.parent_sector;
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}
