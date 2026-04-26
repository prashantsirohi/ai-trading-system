/**
 * Watchlist symbol table (Quantis proposal #02).
 *
 * Columns: Symbol · Sector · Px · Δ% · Score · Triggered rules · Last fired
 * Each row joins the watchlist entry with the live ranking row when available.
 */
import { cn } from '@/lib/utils/cn';
import type { WatchlistEntry, AlertFiredEvent } from '@/lib/storage/watchlist';
import type { StockRow } from '@/types/dashboard';
import TierBadge from '@/components/ranking/TierBadge';

function formatRelTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const m = Math.floor(diff / 60_000);
  if (m < 1) return 'just now';
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function lastFiredFor(symbol: string, alerts: AlertFiredEvent[]): AlertFiredEvent | null {
  return (
    alerts
      .filter((a) => !a.muted && a.symbols.includes(symbol))
      .sort((a, b) => b.ts.localeCompare(a.ts))[0] ?? null
  );
}

const SEVERITY_PILL: Record<string, string> = {
  info: 'border-blue-700/60 bg-blue-500/10 text-blue-300',
  warn: 'border-amber-700/60 bg-amber-500/10 text-amber-300',
  critical: 'border-rose-700/60 bg-rose-500/15 text-rose-300',
};

interface Props {
  entries: WatchlistEntry[];
  rankingRows: StockRow[];
  alerts: AlertFiredEvent[];
  onRemove: (symbol: string) => void;
  onManageRules: (symbol: string) => void;
}

export default function WatchlistTable({
  entries,
  rankingRows,
  alerts,
  onRemove,
  onManageRules,
}: Props) {
  if (entries.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center rounded-2xl border border-slate-800 bg-slate-950/40 px-6 py-16 text-center">
        <p className="text-sm font-medium text-slate-300">No watched symbols yet.</p>
        <p className="mt-1 text-xs text-slate-500">
          Add from any ranking row via the{' '}
          <kbd className="rounded border border-slate-700 px-1 py-0.5 font-mono text-[10px]">
            ★
          </kbd>{' '}
          icon, or press{' '}
          <kbd className="rounded border border-slate-700 px-1 py-0.5 font-mono text-[10px]">
            w
          </kbd>{' '}
          on a selected row.
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="border-b border-slate-800">
            {['Symbol', 'Sector', 'Price', 'Δ%', 'Score', 'Rules', 'Last fired', ''].map(
              (h) => (
                <th
                  key={h}
                  className="pb-2 pr-4 text-left text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-500 first:pl-0 last:pr-0"
                >
                  {h}
                </th>
              ),
            )}
          </tr>
        </thead>
        <tbody>
          {entries.map((entry) => {
            const row = rankingRows.find((r) => r.symbol === entry.symbol);
            const lastAlert = lastFiredFor(entry.symbol, alerts);
            const fired = alerts.some(
              (a) => !a.muted && a.symbols.includes(entry.symbol),
            );

            return (
              <tr
                key={entry.symbol}
                className={cn(
                  'border-b border-slate-800/60 transition-colors hover:bg-slate-800/25',
                  fired ? 'bg-amber-500/5' : '',
                )}
              >
                <td className="py-3 pr-4">
                  <div className="flex items-center gap-2">
                    {row ? <TierBadge tier={row.tier} className="h-5 w-5 text-[10px]" /> : null}
                    <span className="font-semibold text-slate-100">{entry.symbol}</span>
                    {fired && (
                      <span className="rounded border border-blue-700/50 bg-blue-500/10 px-1.5 py-0.5 font-mono text-[9px] text-blue-300">
                        FIRED
                      </span>
                    )}
                  </div>
                </td>
                <td className="py-3 pr-4 text-xs text-slate-400">{row?.sector ?? '—'}</td>
                <td className="py-3 pr-4 font-mono text-xs text-slate-200">
                  {row ? `₹${row.price.toFixed(2)}` : '—'}
                </td>
                <td className="py-3 pr-4 font-mono text-xs">
                  {row ? (
                    <span className={row.trend >= 50 ? 'text-emerald-400' : 'text-rose-400'}>
                      {row.trend >= 50 ? '+' : ''}
                      {((row.trend - 50) / 5).toFixed(2)}%
                    </span>
                  ) : (
                    '—'
                  )}
                </td>
                <td className="py-3 pr-4 font-mono text-xs text-slate-200">
                  {row ? row.score.toFixed(2) : '—'}
                </td>
                <td className="py-3 pr-4">
                  <button
                    type="button"
                    onClick={() => onManageRules(entry.symbol)}
                    className="flex flex-wrap gap-1"
                  >
                    {entry.rules.length === 0 ? (
                      <span className="text-[10px] text-slate-500">— no rules</span>
                    ) : (
                      entry.rules.slice(0, 2).map((rule) => (
                        <span
                          key={rule.id}
                          className={cn(
                            'inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide',
                            SEVERITY_PILL[rule.severity],
                          )}
                        >
                          {rule.kind}
                        </span>
                      ))
                    )}
                    {entry.rules.length > 2 && (
                      <span className="text-[10px] text-slate-500">
                        +{entry.rules.length - 2}
                      </span>
                    )}
                  </button>
                </td>
                <td className="py-3 pr-4 font-mono text-[10px] text-slate-400">
                  {lastAlert ? formatRelTime(lastAlert.ts) : '—'}
                </td>
                <td className="py-3 text-right">
                  <div className="flex items-center justify-end gap-2">
                    <button
                      type="button"
                      onClick={() => onManageRules(entry.symbol)}
                      className="rounded border border-slate-700 px-2 py-1 text-[10px] text-slate-400 hover:border-blue-500/50 hover:text-blue-300"
                    >
                      Rules
                    </button>
                    <button
                      type="button"
                      onClick={() => onRemove(entry.symbol)}
                      aria-label={`Remove ${entry.symbol} from watchlist`}
                      className="rounded border border-slate-700 px-2 py-1 text-[10px] text-slate-400 hover:border-rose-500/50 hover:text-rose-300"
                    >
                      ×
                    </button>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
