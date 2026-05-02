import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import type { MarketBreadthPoint } from '@/lib/api/breadth';

interface Props {
  rows: MarketBreadthPoint[];
}

export default function MovingAverageBreadthChart({ rows }: Props) {
  const firstDate = rows[0]?.date;
  const lastDate = rows[rows.length - 1]?.date;

  return (
    <section className="rounded-lg border border-slate-800 bg-slate-900 p-4 shadow-soft">
      <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold">Breadth Above Averages</h2>
          <p className="mt-0.5 text-xs leading-5 text-slate-400">
            Full operational history above 20, 50, and 200 DMA.
          </p>
        </div>
        <span className="rounded-md border border-slate-700 bg-slate-950/70 px-2.5 py-1 text-xs font-semibold text-slate-300">
          {rows.length ? `${formatDateTick(firstDate)}-${formatDateTick(lastDate)} · ${rows.length} dates` : 'No data'}
        </span>
      </div>

      <div className="h-48">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} margin={{ top: 8, right: 12, left: -20, bottom: 0 }}>
            <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" vertical={false} />
            <XAxis
              dataKey="date"
              tickFormatter={formatDateTick}
              tick={{ fill: '#94a3b8', fontSize: 11 }}
              axisLine={false}
              tickLine={false}
              minTickGap={28}
            />
            <YAxis
              allowDecimals={false}
              tick={{ fill: '#64748b', fontSize: 11 }}
              axisLine={false}
              tickLine={false}
              width={42}
            />
            <Tooltip
              cursor={{ stroke: '#475569', strokeDasharray: '3 3' }}
              contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: 8 }}
              formatter={(value, name) => [`${Number(value).toFixed(2)}%`, name]}
              labelFormatter={(label) => formatDateLabel(String(label))}
              labelStyle={{ color: '#e2e8f0' }}
            />
            <Legend iconType="circle" wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
            <Line type="monotone" dataKey="above20" name="20DMA" stroke="#38bdf8" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="above50" name="50DMA" stroke="#a78bfa" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="above200" name="200DMA" stroke="#34d399" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

function formatDateTick(value: string): string {
  const date = parseIsoDate(value);
  if (!date) return value;
  return date.toLocaleDateString('en-IN', { month: 'short', year: '2-digit' });
}

function formatDateLabel(value: string): string {
  const date = parseIsoDate(value);
  if (!date) return value;
  return date.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' });
}

function parseIsoDate(value: string): Date | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return null;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  return Number.isNaN(date.getTime()) ? null : date;
}
