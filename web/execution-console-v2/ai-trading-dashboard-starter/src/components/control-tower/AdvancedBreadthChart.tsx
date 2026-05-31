import { useMemo, useState } from 'react';
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import type { MarketBreadthPoint } from '@/lib/api/breadth';
import { cn } from '@/lib/utils/cn';

interface Props {
  rows: MarketBreadthPoint[];
}

type Metric = 'participation' | 'high-low' | 'divergence';
type Range = '3m' | '6m' | '1y' | 'all';

interface ChartPoint extends MarketBreadthPoint {
  highLowRatio: number | null;
  highLowRatioSma10: number | null;
  adLine: number;
}

const METRICS: Array<{ key: Metric; label: string }> = [
  { key: 'participation', label: '% Above 200 DMA + Valuation' },
  { key: 'high-low', label: '52W High / Low' },
  { key: 'divergence', label: 'A/D Divergence' },
];

const RANGES: Array<{ key: Range; label: string; months?: number }> = [
  { key: '3m', label: '3 Months', months: 3 },
  { key: '6m', label: '6 Months', months: 6 },
  { key: '1y', label: '1 Year', months: 12 },
  { key: 'all', label: 'All History' },
];

export default function AdvancedBreadthChart({ rows }: Props) {
  const [metric, setMetric] = useState<Metric>('participation');
  const [range, setRange] = useState<Range>('1y');
  const allPoints = useMemo(() => derivePoints(rows), [rows]);
  const points = useMemo(() => filterRange(allPoints, range), [allPoints, range]);
  const firstDate = points[0]?.date;
  const lastDate = points[points.length - 1]?.date;

  return (
    <section className="rounded-lg border border-slate-800 bg-slate-900 p-4 shadow-soft">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold">Advanced Market Breadth</h2>
          <p className="mt-0.5 text-xs leading-5 text-slate-400">
            NSE participation, leadership, divergence, and TOP1000 valuation since 2020.
          </p>
        </div>
        <span className="rounded-md border border-slate-700 bg-slate-950/70 px-2.5 py-1 text-xs font-semibold text-slate-300">
          {points.length ? `${formatDateTick(firstDate)}-${formatDateTick(lastDate)} · ${points.length} dates` : 'No data'}
        </span>
      </div>

      <div className="mt-3 flex flex-wrap items-center justify-between gap-3 border-y border-slate-800 py-2.5">
        <div className="flex flex-wrap gap-1.5">
          {METRICS.map((item) => (
            <Toggle key={item.key} active={metric === item.key} onClick={() => setMetric(item.key)}>
              {item.label}
            </Toggle>
          ))}
        </div>
        <div className="flex flex-wrap gap-1.5">
          {RANGES.map((item) => (
            <Toggle key={item.key} active={range === item.key} onClick={() => setRange(item.key)}>
              {item.label}
            </Toggle>
          ))}
        </div>
      </div>

      {points.length ? (
        <div className="mt-3 h-72">
          {metric === 'participation' ? (
            <ParticipationChart points={points} />
          ) : metric === 'high-low' ? (
            <HighLowChart points={points} />
          ) : (
            <DivergenceChart points={points} />
          )}
        </div>
      ) : (
        <div className="mt-3 flex h-72 items-center justify-center rounded-md border border-dashed border-slate-800 text-sm text-slate-500">
          Market breadth history is not available yet.
        </div>
      )}
    </section>
  );
}

function ParticipationChart({ points }: { points: ChartPoint[] }) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={points} margin={{ top: 8, right: 18, left: -16, bottom: 0 }}>
        <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" vertical={false} />
        {dateAxis()}
        <YAxis yAxisId="breadth" domain={[0, 100]} tick={{ fill: '#34d399', fontSize: 11 }} axisLine={false} tickLine={false} width={42} />
        <YAxis yAxisId="pe" orientation="right" domain={[0, 100]} tick={{ fill: '#c084fc', fontSize: 11 }} axisLine={false} tickLine={false} width={40} />
        <Tooltip contentStyle={tooltipStyle} formatter={formatTooltip} labelFormatter={dateLabel} />
        <Legend iconType="circle" wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
        <ReferenceLine yAxisId="breadth" y={70} stroke="#f59e0b" strokeDasharray="4 4" label={{ value: 'overbought 70%', fill: '#fbbf24', fontSize: 10 }} />
        <ReferenceLine yAxisId="breadth" y={30} stroke="#38bdf8" strokeDasharray="4 4" label={{ value: 'oversold 30%', fill: '#7dd3fc', fontSize: 10 }} />
        <Line yAxisId="breadth" type="monotone" dataKey="above200" name="% Above 200 DMA" stroke="#34d399" strokeWidth={2.2} dot={false} isAnimationActive={false} />
        <Line yAxisId="pe" type="monotone" dataKey="pePctile5ySma20" name="PE 5Y percentile SMA20" stroke="#c084fc" strokeWidth={1.8} dot={false} connectNulls={false} isAnimationActive={false} />
      </ComposedChart>
    </ResponsiveContainer>
  );
}

function HighLowChart({ points }: { points: ChartPoint[] }) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={points} margin={{ top: 8, right: 18, left: -12, bottom: 0 }}>
      <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" vertical={false} />
      {dateAxis()}
      <YAxis yAxisId="counts" tick={{ fill: '#64748b', fontSize: 11 }} axisLine={false} tickLine={false} width={46} />
      <YAxis yAxisId="ratio" orientation="right" tick={{ fill: '#a78bfa', fontSize: 11 }} axisLine={false} tickLine={false} width={46} />
      <Tooltip contentStyle={tooltipStyle} labelFormatter={dateLabel} formatter={formatTooltip} />
      <Legend iconType="circle" wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
      <Bar yAxisId="counts" dataKey="new52wHighs" name="New 52W highs" fill="#10b981" isAnimationActive={false} />
      <Bar yAxisId="counts" dataKey="new52wLows" name="New 52W lows" fill="#f43f5e" isAnimationActive={false} />
      <Line yAxisId="ratio" type="monotone" dataKey="highLowRatioSma10" name="High / low ratio SMA10" stroke="#a78bfa" strokeWidth={2} dot={false} connectNulls isAnimationActive={false} />
      </ComposedChart>
    </ResponsiveContainer>
  );
}

function DivergenceChart({ points }: { points: ChartPoint[] }) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={points} margin={{ top: 8, right: 28, left: -4, bottom: 0 }}>
      <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" vertical={false} />
      {dateAxis()}
      <YAxis yAxisId="ad" tick={{ fill: '#38bdf8', fontSize: 11 }} axisLine={false} tickLine={false} width={54} />
      <YAxis yAxisId="index" orientation="right" tick={{ fill: '#f59e0b', fontSize: 11 }} axisLine={false} tickLine={false} width={62} />
      <Tooltip contentStyle={tooltipStyle} labelFormatter={dateLabel} formatter={formatTooltip} />
      <Legend iconType="circle" wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
      <Line yAxisId="ad" type="monotone" dataKey="adLine" name="A/D line" stroke="#38bdf8" strokeWidth={2.1} dot={false} isAnimationActive={false} />
      <Line yAxisId="index" type="monotone" dataKey="indexLevel" name="TOP1000 index" stroke="#f59e0b" strokeWidth={1.8} dot={false} connectNulls={false} isAnimationActive={false} />
      </ComposedChart>
    </ResponsiveContainer>
  );
}

function dateAxis() {
  return (
    <XAxis
      dataKey="date"
      tickFormatter={formatDateTick}
      tick={{ fill: '#94a3b8', fontSize: 11 }}
      axisLine={false}
      tickLine={false}
      minTickGap={28}
      tickMargin={8}
      height={42}
      label={{ value: 'Date', position: 'insideBottom', fill: '#64748b', fontSize: 11 }}
    />
  );
}

function Toggle({ active, onClick, children }: { active: boolean; onClick: () => void; children: string }) {
  return (
    <button type="button" onClick={onClick} className={cn('rounded-md border px-2.5 py-1 text-xs font-semibold transition', active ? 'border-sky-500/60 bg-sky-500/10 text-sky-200' : 'border-slate-700 bg-slate-950/50 text-slate-400 hover:border-slate-500 hover:text-slate-200')}>
      {children}
    </button>
  );
}

function derivePoints(rows: MarketBreadthPoint[]): ChartPoint[] {
  const ratios: number[] = [];
  let cumulative = 0;
  return rows.map((row, index) => {
    const ratio = row.new52wLows > 0 ? row.new52wHighs / row.new52wLows : null;
    if (ratio !== null) ratios.push(ratio);
    cumulative += row.advancers - row.decliners;
    return {
      ...row,
      highLowRatio: ratio,
      highLowRatioSma10: ratios.length ? average(ratios.slice(-10)) : null,
      adLine: index === 0 ? 0 : cumulative - (rows[0].advancers - rows[0].decliners),
    };
  });
}

function filterRange(points: ChartPoint[], range: Range): ChartPoint[] {
  const months = RANGES.find((item) => item.key === range)?.months;
  if (!months || !points.length) return points;
  const last = parseIsoDate(points[points.length - 1].date);
  if (!last) return points;
  const cutoff = new Date(last);
  cutoff.setUTCMonth(cutoff.getUTCMonth() - months);
  return points.filter((point) => {
    const date = parseIsoDate(point.date);
    return date ? date >= cutoff : true;
  });
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function formatTooltip(value: unknown, name: unknown): [string, string] {
  const number = Number(value);
  const label = String(name);
  if (label.includes('percentile')) return [`${number.toFixed(1)}%`, label];
  if (label.includes('ratio')) return [number.toFixed(2), label];
  return [number.toLocaleString('en-IN', { maximumFractionDigits: 2 }), label];
}

const tooltipStyle = { background: '#020617', border: '1px solid #1e293b', borderRadius: 8 };
const dateLabel = (value: unknown) => formatDateLabel(String(value));

function formatDateTick(value?: string): string {
  const date = parseIsoDate(value);
  return date ? date.toLocaleDateString('en-IN', { month: 'short', year: '2-digit' }) : value ?? '';
}

function formatDateLabel(value: string): string {
  const date = parseIsoDate(value);
  return date ? date.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }) : value;
}

function parseIsoDate(value?: string): Date | null {
  if (!value) return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return null;
  const date = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  return Number.isNaN(date.getTime()) ? null : date;
}
