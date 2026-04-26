/**
 * Single KPI card with value, sub-label, and a gauge bar (proposal #03).
 */
import { cn } from '@/lib/utils/cn';
import type { KpiCard } from '@/lib/risk/derive';

const GAUGE_FILL: Record<KpiCard['tone'], string> = {
  ok: 'bg-emerald-500',
  warn: 'bg-amber-500',
  err: 'bg-rose-500',
};

export default function RiskKpiCard({ label, value, sub, pct, tone, marks }: KpiCard) {
  const fillPct = Math.min(100, Math.max(0, pct));

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
      <p className="text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-500">
        {label}
      </p>
      <p
        className={cn(
          'mt-1.5 font-mono text-2xl font-semibold',
          tone === 'err'
            ? 'text-rose-300'
            : tone === 'warn'
              ? 'text-amber-300'
              : 'text-slate-100',
        )}
      >
        {value}
      </p>
      <p className="mt-0.5 text-[11px] text-slate-500">{sub}</p>
      {/* Gauge */}
      <div className="relative mt-3 h-1.5 overflow-hidden rounded-full bg-slate-800">
        <div
          className={cn('h-full rounded-full transition-all', GAUGE_FILL[tone])}
          style={{ width: `${fillPct}%` }}
        />
        {marks?.map((m, i) => (
          <div
            key={i}
            className="absolute top-[-2px] h-[10px] w-0.5"
            style={{ left: `${Math.min(100, m.pct)}%`, background: m.tone }}
          />
        ))}
      </div>
    </div>
  );
}
