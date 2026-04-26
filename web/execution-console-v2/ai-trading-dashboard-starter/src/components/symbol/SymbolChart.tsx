/**
 * Price chart for the Symbol page — 1Y close + 50DMA + 200DMA + breakout marker.
 * Overlay toggles below the chart control which overlays render.
 */
import { useMemo, useState } from 'react';
import {
  Area,
  ComposedChart,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { StockOhlcv } from '@/lib/api/stocks';
import { deriveMAs } from '@/lib/symbol/derive';
import { cn } from '@/lib/utils/cn';

type Period = '1D' | '5D' | '3M' | '1Y' | '5Y';
type Overlay = '50DMA' | '200DMA' | 'EMA20' | 'Bollinger' | 'Volume' | 'RSI' | 'MACD' | 'Stoch' | 'VWAP' | 'Fib';

const PERIODS: Period[] = ['1D', '5D', '3M', '1Y', '5Y'];
const OVERLAY_LIST: Overlay[] = ['50DMA', '200DMA', 'EMA20', 'Bollinger', 'Volume', 'RSI', 'MACD', 'Stoch', 'VWAP', 'Fib'];
const DEFAULT_OVERLAYS = new Set<Overlay>(['50DMA', '200DMA', 'Bollinger', 'RSI']);

const PERIOD_LIMIT: Record<Period, number> = {
  '1D': 1, '5D': 5, '3M': 63, '1Y': 252, '5Y': 1260,
};

function shortDate(iso: string): string {
  try {
    return new Date(iso).toLocaleDateString(undefined, { month: 'short', day: '2-digit' });
  } catch {
    return iso;
  }
}

interface Props {
  data: StockOhlcv | null | undefined;
  isLoading: boolean;
  breakoutDate?: string | null;
}

export default function SymbolChart({ data, isLoading, breakoutDate }: Props) {
  const [period, setPeriod] = useState<Period>('1Y');
  const [activeOverlays, setActiveOverlays] = useState<Set<Overlay>>(new Set(DEFAULT_OVERLAYS));

  function toggleOverlay(o: Overlay) {
    setActiveOverlays((prev) => {
      const next = new Set(prev);
      if (next.has(o)) next.delete(o); else next.add(o);
      return next;
    });
  }

  const { series, ma50Arr, ma200Arr } = useMemo(() => {
    if (!data?.available || !data.candles.length) return { series: [], ma50Arr: [], ma200Arr: [] };

    const limit = PERIOD_LIMIT[period];
    const candles = data.candles.slice(-limit);
    const { ma50, ma200 } = deriveMAs(candles);

    const s = candles.map((c, i) => ({
      date:  c.timestamp ? shortDate(c.timestamp) : '',
      close: c.close ?? null,
      ma50:  activeOverlays.has('50DMA')  ? (ma50[i]  ?? null) : null,
      ma200: activeOverlays.has('200DMA') ? (ma200[i] ?? null) : null,
    }));
    return { series: s, ma50Arr: ma50, ma200Arr: ma200 };
  }, [data, period, activeOverlays]);

  const closes = series.map((s) => s.close).filter((v): v is number => v !== null);
  const minVal = closes.length ? Math.min(...closes) * 0.97 : 0;
  const maxVal = closes.length ? Math.max(...closes) * 1.03 : 1;

  // Find breakout candle index (findLastIndex not in ES2021 target — use reduceRight)
  const bkIdx = breakoutDate && data?.candles
    ? data.candles.reduce((found, c, i) => c.timestamp?.startsWith(breakoutDate) ? i : found, -1)
    : -1;
  const bkDate = bkIdx >= 0 ? series[bkIdx]?.date : null;

  if (isLoading) {
    return <div className="flex h-56 items-center justify-center text-sm text-slate-500">Loading chart…</div>;
  }
  if (!data?.available || series.length === 0) {
    return <div className="flex h-56 items-center justify-center text-sm text-slate-500">No price history available.</div>;
  }

  return (
    <div>
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs font-semibold text-slate-300">Price · with overlays</span>
        <div className="flex overflow-hidden rounded-xl border border-slate-700 bg-slate-950/60">
          {PERIODS.map((p) => (
            <button
              key={p}
              type="button"
              onClick={() => setPeriod(p)}
              className={cn(
                'px-3 py-1 text-[11px] font-semibold transition-colors',
                period === p
                  ? 'bg-slate-800 text-white'
                  : 'text-slate-500 hover:text-slate-300',
              )}
            >
              {p}
            </button>
          ))}
        </div>
      </div>

      <ResponsiveContainer width="100%" height={220}>
        <ComposedChart data={series} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id="sym-fill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#34d399" stopOpacity={0.18} />
              <stop offset="100%" stopColor="#34d399" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis
            dataKey="date"
            tick={{ fontSize: 9, fill: '#64748b' }}
            tickLine={false}
            axisLine={false}
            interval={Math.floor(series.length / 6)}
          />
          <YAxis
            domain={[minVal, maxVal]}
            tick={{ fontSize: 9, fill: '#64748b' }}
            tickLine={false}
            axisLine={false}
            tickFormatter={(v: number) => v.toFixed(0)}
            width={48}
          />
          <Tooltip
            contentStyle={{ background: '#0f172a', border: '1px solid #334155', borderRadius: 10, fontSize: 11 }}
            labelStyle={{ color: '#94a3b8', fontSize: 10 }}
            formatter={(val: number, name: string) => [val?.toFixed(2), name]}
          />
          {bkDate && (
            <ReferenceLine
              x={bkDate}
              stroke="#34d399"
              strokeDasharray="3 4"
              strokeOpacity={0.5}
              label={{ value: 'breakout', position: 'insideTopRight', fontSize: 9, fill: '#6ee7b7' }}
            />
          )}
          <Area
            type="monotone"
            dataKey="close"
            stroke="#34d399"
            strokeWidth={1.4}
            fill="url(#sym-fill)"
            dot={false}
            activeDot={{ r: 3, fill: '#34d399' }}
            connectNulls
          />
          {activeOverlays.has('50DMA') && (
            <Line type="monotone" dataKey="ma50" stroke="#fbbf24" strokeWidth={1} dot={false} connectNulls strokeOpacity={0.85} />
          )}
          {activeOverlays.has('200DMA') && (
            <Line type="monotone" dataKey="ma200" stroke="#60a5fa" strokeWidth={1} dot={false} connectNulls strokeDasharray="3 3" strokeOpacity={0.7} />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {/* Overlay toggles */}
      <div className="mt-2 flex flex-wrap gap-1.5">
        {OVERLAY_LIST.map((o) => (
          <button
            key={o}
            type="button"
            onClick={() => toggleOverlay(o)}
            className={cn(
              'rounded-full border px-2.5 py-0.5 text-[10px] font-semibold transition-colors',
              activeOverlays.has(o)
                ? 'border-blue-500/50 bg-blue-500/15 text-blue-300'
                : 'border-slate-700 bg-slate-900/60 text-slate-500 hover:border-slate-500',
            )}
          >
            {o}
          </button>
        ))}
      </div>
    </div>
  );
}
