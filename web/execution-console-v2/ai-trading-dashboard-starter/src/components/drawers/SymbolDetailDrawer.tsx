import type { StockRow } from '@/types/dashboard';
import { getSymbolDetailFallback } from '@/lib/mock/symbolDetails';

interface SymbolDetailDrawerProps {
  row: StockRow | null;
  open: boolean;
  onClose: () => void;
}

function formatPrice(value: number | null): string {
  if (value === null || Number.isNaN(value)) {
    return 'N/A';
  }
  return value.toFixed(2);
}

export default function SymbolDetailDrawer({ row, open, onClose }: SymbolDetailDrawerProps) {
  if (!open || !row) {
    return null;
  }

  const fallback = getSymbolDetailFallback(row);
  const stopLoss = fallback.stopLoss ?? (row.price > 0 ? row.price * 0.96 : null);
  const target = fallback.target ?? (row.price > 0 ? row.price * 1.06 : null);
  const conviction = Math.round((row.score + fallback.patternConfidence) / 2);

  return (
    <div className="fixed inset-0 z-50">
      <button
        type="button"
        aria-label="Close symbol details"
        className="absolute inset-0 bg-slate-950/70"
        onClick={onClose}
      />
      <aside className="absolute right-0 top-0 h-full w-full max-w-xl border-l border-slate-800 bg-slate-900 p-5 shadow-2xl">
        <div className="mb-4 flex items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-wide text-slate-400">Symbol Detail</p>
            <h2 className="text-xl font-semibold text-white">{row.symbol}</h2>
            <p className="text-sm text-slate-300">{row.sector} · Tier {row.tier}</p>
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-700 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800"
            onClick={onClose}
          >
            Close
          </button>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
            <p className="text-xs text-slate-400">Price</p>
            <p className="text-base font-semibold text-white">{formatPrice(row.price)}</p>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
            <p className="text-xs text-slate-400">Composite Score</p>
            <p className="text-base font-semibold text-white">{row.score.toFixed(2)}</p>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
            <p className="text-xs text-slate-400">RS</p>
            <p className="text-base font-semibold text-white">{row.rs}</p>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
            <p className="text-xs text-slate-400">Conviction</p>
            <p className="text-base font-semibold text-white">{conviction}%</p>
          </div>
        </div>

        <section className="mt-5 space-y-2 rounded-lg border border-slate-800 bg-slate-950/60 p-4">
          <h3 className="text-sm font-semibold text-white">Setup</h3>
          <p className="text-sm text-slate-300">Pattern: {row.pattern || 'N/A'}</p>
          <p className="text-sm text-slate-300">Breakout: {row.breakout ? 'Confirmed' : 'Awaiting'}</p>
          <p className="text-sm text-slate-300">Volume Regime: {row.volume}</p>
          <p className="text-sm text-slate-300">{fallback.thesis}</p>
        </section>

        <section className="mt-4 space-y-2 rounded-lg border border-slate-800 bg-slate-950/60 p-4">
          <h3 className="text-sm font-semibold text-white">Risk Plan</h3>
          <p className="text-sm text-slate-300">Stop Loss: {formatPrice(stopLoss)}</p>
          <p className="text-sm text-slate-300">Target: {formatPrice(target)}</p>
          <p className="text-sm text-slate-300">{fallback.riskNote}</p>
        </section>

        <section className="mt-4 rounded-lg border border-slate-800 bg-slate-950/60 p-4">
          <h3 className="text-sm font-semibold text-white">Catalysts</h3>
          <ul className="mt-2 space-y-1 text-sm text-slate-300">
            {fallback.catalysts.map((item) => (
              <li key={item}>• {item}</li>
            ))}
          </ul>
        </section>
      </aside>
    </div>
  );
}
