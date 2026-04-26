/**
 * Capital allocation widget — right-rail companion to the orders table.
 *
 * Stacks the per-eligible-order size% values into a single horizontal bar
 * coloured by symbol. The bar is capped at the configured limit so the
 * operator instantly sees how close they are to the policy ceiling.
 */
import type { DerivedOrder } from './derive';
import { cn } from '@/lib/utils/cn';

interface Props {
  orders: DerivedOrder[];
  capitalLimitPct: number;
  capitalUsedPct: number;
}

const SEGMENT_TONES = [
  'bg-emerald-500/70',
  'bg-blue-500/70',
  'bg-violet-500/70',
  'bg-amber-500/70',
  'bg-rose-500/70',
  'bg-cyan-500/70',
  'bg-pink-500/70',
];

export default function CapitalWidget({ orders, capitalLimitPct, capitalUsedPct }: Props) {
  const remaining = Math.max(0, capitalLimitPct - capitalUsedPct);

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
      <div className="flex items-baseline justify-between">
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          Capital Allocation
        </h4>
        <span className="text-xs tabular-nums text-slate-300">
          {capitalUsedPct.toFixed(2)}% / {capitalLimitPct.toFixed(0)}%
        </span>
      </div>

      <div className="mt-3 flex h-3 w-full overflow-hidden rounded-full bg-slate-800">
        {orders.map((order, idx) => {
          const widthPct = capitalLimitPct > 0 ? (order.sizePct / capitalLimitPct) * 100 : 0;
          return (
            <span
              key={order.symbol}
              title={`${order.symbol}: ${order.sizePct.toFixed(2)}%`}
              className={cn(
                'h-full border-r border-slate-950/30 last:border-r-0',
                SEGMENT_TONES[idx % SEGMENT_TONES.length],
              )}
              style={{ width: `${Math.max(0, Math.min(100, widthPct))}%` }}
            />
          );
        })}
      </div>

      <ul className="mt-3 grid grid-cols-1 gap-1 text-xs">
        {orders.length === 0 ? (
          <li className="text-slate-500">No allocations.</li>
        ) : (
          orders.map((order, idx) => (
            <li key={order.symbol} className="flex items-center gap-2">
              <span
                className={cn(
                  'inline-block h-2 w-2 rounded-full',
                  SEGMENT_TONES[idx % SEGMENT_TONES.length],
                )}
              />
              <span className="font-semibold text-slate-200">{order.symbol}</span>
              <span className="ml-auto tabular-nums text-slate-400">
                {order.sizePct.toFixed(2)}%
              </span>
            </li>
          ))
        )}
        <li className="mt-1 flex items-center gap-2 border-t border-slate-800 pt-2">
          <span className="inline-block h-2 w-2 rounded-full bg-slate-700" />
          <span className="text-slate-400">Available</span>
          <span className="ml-auto tabular-nums text-slate-300">{remaining.toFixed(2)}%</span>
        </li>
      </ul>
    </div>
  );
}
