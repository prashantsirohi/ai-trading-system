/**
 * Bar-chart primitives for the Research / Performance Tracker page.
 *
 * Each chart takes already-shaped rows from the perf-tracker API responses
 * and renders a sign-aware recharts BarChart. Positive bars use emerald,
 * negative use rose — matching the StatusBadge convention used elsewhere.
 */

import {
  ResponsiveContainer,
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Cell,
  ReferenceLine,
} from 'recharts';

const POSITIVE = '#10b981'; // emerald-500
const NEGATIVE = '#f43f5e'; // rose-500
const NEUTRAL = '#475569'; // slate-600
const SMALL_SAMPLE = '#a16207'; // amber-700 — faded for low-confidence bars

const AXIS_TICK = { fill: '#94a3b8', fontSize: 12 };
const TOOLTIP_STYLE = {
  background: '#020617',
  border: '1px solid #1e293b',
  borderRadius: 8,
  fontSize: 12,
};

function signColor(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return NEUTRAL;
  return v >= 0 ? POSITIVE : NEGATIVE;
}

// --------------------------------------------------------------------------

export interface CohortBarPoint {
  cohort: string;
  avg_20d: number | null;
}

/** Horizontal bar chart of avg_20d by rank cohort. */
export function CohortBarChart({
  rows,
  referenceValue,
}: {
  rows: CohortBarPoint[];
  referenceValue?: number | null;
}) {
  const data = rows.map((r) => ({ cohort: r.cohort, value: r.avg_20d ?? 0 }));
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ left: 12, right: 24 }}>
          <CartesianGrid stroke="#1e293b" horizontal={false} />
          <XAxis type="number" tick={AXIS_TICK} unit="%" />
          <YAxis type="category" dataKey="cohort" width={80} tick={{ fill: '#cbd5e1', fontSize: 12 }} />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            formatter={(v: number) => [`${v.toFixed(2)}%`, 'avg_20d']}
          />
          {typeof referenceValue === 'number' ? (
            <ReferenceLine x={referenceValue} stroke="#64748b" strokeDasharray="3 3" />
          ) : null}
          <Bar dataKey="value" radius={[0, 6, 6, 0]}>
            {data.map((d, i) => (
              <Cell key={i} fill={signColor(d.value)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// --------------------------------------------------------------------------

export interface BucketExcessPoint {
  bucket: string;
  excess_20d: number | null;
  small_sample?: boolean;
}

/** Horizontal bar chart of same-date excess_20d per bucket. */
export function BucketExcessBarChart({ rows }: { rows: BucketExcessPoint[] }) {
  const data = rows.map((r) => ({
    bucket: r.small_sample ? `${r.bucket} (small)` : r.bucket,
    value: r.excess_20d ?? 0,
    small: !!r.small_sample,
  }));
  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ left: 12, right: 24 }}>
          <CartesianGrid stroke="#1e293b" horizontal={false} />
          <XAxis type="number" tick={AXIS_TICK} unit="%" />
          <YAxis type="category" dataKey="bucket" width={170} tick={{ fill: '#cbd5e1', fontSize: 12 }} />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            formatter={(v: number) => [`${v.toFixed(2)}%`, 'excess vs control']}
          />
          <ReferenceLine x={0} stroke="#64748b" />
          <Bar dataKey="value" radius={[0, 6, 6, 0]}>
            {data.map((d, i) => (
              <Cell key={i} fill={d.small ? SMALL_SAMPLE : signColor(d.value)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// --------------------------------------------------------------------------

export interface Top200IcPoint {
  factor: string;
  ic_5d: number | null;
  ic_10d: number | null;
  ic_20d: number | null;
}

/**
 * Grouped bar chart: top-200 IC across 5/10/20-day horizons per factor.
 * If an IC bar is near zero, the ranking has no in-universe predictive
 * power for that factor at that horizon.
 */
export function Top200IcBarChart({ rows }: { rows: Top200IcPoint[] }) {
  const data = rows.map((r) => ({
    factor: r.factor,
    ic_5d: r.ic_5d ?? 0,
    ic_10d: r.ic_10d ?? 0,
    ic_20d: r.ic_20d ?? 0,
  }));
  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ left: 0, right: 16, top: 8 }}>
          <CartesianGrid stroke="#1e293b" vertical={false} />
          <XAxis dataKey="factor" tick={AXIS_TICK} />
          <YAxis tick={AXIS_TICK} domain={['auto', 'auto']} />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            formatter={(v: number) => v.toFixed(3)}
          />
          <ReferenceLine y={0} stroke="#64748b" />
          <Bar dataKey="ic_5d" fill="#60a5fa" radius={[4, 4, 0, 0]} />
          <Bar dataKey="ic_10d" fill="#a78bfa" radius={[4, 4, 0, 0]} />
          <Bar dataKey="ic_20d" fill="#f472b6" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
