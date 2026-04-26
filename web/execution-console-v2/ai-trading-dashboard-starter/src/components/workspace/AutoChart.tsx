/**
 * Full-size price + delivery chart for the Stock Detail Workspace.
 *
 * Renders close prices as a line and delivery % as a secondary bar layer.
 * Uses recharts which is already pulled in for the ranking sparkline.
 */
import {
  Area,
  AreaChart,
  Bar,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { StockOhlcv } from '@/lib/api/stocks';

interface Props {
  data: StockOhlcv | null | undefined;
  isLoading: boolean;
}

function shortDate(iso: string): string {
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return iso;
  return d.toLocaleDateString(undefined, { month: 'short', day: '2-digit' });
}

export default function AutoChart({ data, isLoading }: Props) {
  if (isLoading) {
    return <p className="text-xs text-slate-500">Loading chart…</p>;
  }
  if (!data || !data.available || data.candles.length === 0) {
    return <p className="text-xs text-slate-500">No price history available.</p>;
  }

  const series = data.candles.map((c) => ({
    date: c.timestamp ? shortDate(c.timestamp) : '',
    close: c.close ?? null,
    volume: c.volume ?? null,
    delivery: c.deliveryPct ?? null,
  }));

  const closes = series.map((s) => s.close).filter((v): v is number => v !== null);
  const min = closes.length ? Math.min(...closes) : 0;
  const max = closes.length ? Math.max(...closes) : 1;
  const padding = (max - min) * 0.05 || 1;

  return (
    <div className="space-y-3">
      <div className="h-72 w-full">
        <ResponsiveContainer>
          <AreaChart data={series} margin={{ top: 10, right: 12, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="closeGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.55} />
                <stop offset="100%" stopColor="#3b82f6" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
            <XAxis dataKey="date" stroke="#475569" fontSize={10} minTickGap={32} />
            <YAxis
              stroke="#475569"
              fontSize={10}
              domain={[min - padding, max + padding]}
              tickFormatter={(v) => Number(v).toFixed(0)}
              width={50}
            />
            <Tooltip
              contentStyle={{
                background: '#020617',
                border: '1px solid #1e293b',
                borderRadius: 8,
                fontSize: 12,
              }}
              labelStyle={{ color: '#cbd5e1' }}
            />
            <Area
              type="monotone"
              dataKey="close"
              stroke="#60a5fa"
              strokeWidth={2}
              fill="url(#closeGradient)"
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      <div className="h-28 w-full">
        <ResponsiveContainer>
          <ComposedChart data={series} margin={{ top: 0, right: 12, left: 0, bottom: 0 }}>
            <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
            <XAxis dataKey="date" stroke="#475569" fontSize={10} minTickGap={32} />
            <YAxis stroke="#475569" fontSize={10} domain={[0, 100]} width={50} unit="%" />
            <Tooltip
              contentStyle={{
                background: '#020617',
                border: '1px solid #1e293b',
                borderRadius: 8,
                fontSize: 12,
              }}
              labelStyle={{ color: '#cbd5e1' }}
            />
            <Bar dataKey="delivery" fill="#a78bfa" opacity={0.55} isAnimationActive={false} />
            <Line
              type="monotone"
              dataKey="delivery"
              stroke="#c4b5fd"
              strokeWidth={1}
              dot={false}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <p className="text-[10px] uppercase tracking-widest text-slate-500">
        Close (top) · Delivery % (bottom) — last {series.length} sessions
      </p>
    </div>
  );
}
