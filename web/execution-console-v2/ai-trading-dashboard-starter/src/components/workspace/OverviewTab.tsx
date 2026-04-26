/**
 * Overview tab for the Stock Detail Workspace.
 *
 * Aggregates the top-level facts an operator wants to see at a glance:
 *
 *   * Latest quote (close, intraday range, delivery %).
 *   * Ranking position + universe size + composite score.
 *   * Lifecycle state across rank → breakout → pattern → execution
 *     (sourced from the backend's pre-computed labels).
 *   * Symbol metadata (ISIN, sector, industry, mcap, lot size).
 */
import type { StockDetail } from '@/lib/api/stocks';
import { cn } from '@/lib/utils/cn';

interface Props {
  detail: StockDetail | null | undefined;
}

function fmtNum(value: number | null, digits = 2): string {
  if (value === null) return '—';
  return value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtMcap(mcap: number | null): string {
  if (mcap === null) return '—';
  if (mcap >= 1e7) return `${(mcap / 1e7).toFixed(2)} Cr`;
  if (mcap >= 1e5) return `${(mcap / 1e5).toFixed(2)} L`;
  return mcap.toLocaleString();
}

function lifecycleTone(label: string): string {
  const norm = label.toUpperCase();
  if (norm.startsWith('TOP') || norm === 'CONFIRMED' || norm === 'ELIGIBLE') {
    return 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200';
  }
  if (norm === 'WATCHLIST' || norm === 'MID TIER' || norm.startsWith('DETECT')) {
    return 'border-amber-500/40 bg-amber-500/10 text-amber-200';
  }
  if (norm === 'BLOCKED' || norm === 'OUT' || norm === 'NONE') {
    return 'border-slate-700 bg-slate-900/60 text-slate-400';
  }
  return 'border-blue-500/40 bg-blue-500/10 text-blue-200';
}

export default function OverviewTab({ detail }: Props) {
  if (!detail) {
    return <p className="text-sm text-slate-500">No detail data available yet.</p>;
  }

  const quote = detail.latestQuote;
  const ranking = detail.ranking;
  const meta = detail.metadata;
  const lifecycle = detail.lifecycle;

  const pctMove =
    quote && quote.open !== null && quote.close !== null && quote.open > 0
      ? ((quote.close - quote.open) / quote.open) * 100
      : null;

  return (
    <div className="space-y-4">
      {/* Hero stats */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat
          label="Last close"
          value={fmtNum(quote?.close ?? null)}
          sub={
            pctMove !== null ? `${pctMove >= 0 ? '+' : ''}${pctMove.toFixed(2)}%` : undefined
          }
          tone={pctMove === null ? 'neutral' : pctMove >= 0 ? 'good' : 'bad'}
        />
        <Stat
          label="Intraday range"
          value={
            quote?.low !== null && quote?.high !== null
              ? `${fmtNum(quote?.low ?? null)} → ${fmtNum(quote?.high ?? null)}`
              : '—'
          }
          tone="neutral"
        />
        <Stat
          label="Delivery"
          value={
            quote?.deliveryPct !== null && quote?.deliveryPct !== undefined
              ? `${quote.deliveryPct.toFixed(2)}%`
              : '—'
          }
          tone={
            quote?.deliveryPct !== null && quote?.deliveryPct !== undefined
              ? quote.deliveryPct >= 60
                ? 'good'
                : quote.deliveryPct >= 40
                ? 'warn'
                : 'bad'
              : 'neutral'
          }
        />
        <Stat
          label="Composite rank"
          value={
            ranking?.rankPosition
              ? `#${ranking.rankPosition} / ${ranking.universeSize}`
              : '—'
          }
          sub={ranking?.compositeScore !== null && ranking?.compositeScore !== undefined ? `score ${ranking.compositeScore.toFixed(2)}` : undefined}
          tone={
            ranking?.rankPosition
              ? ranking.rankPosition <= 5
                ? 'good'
                : ranking.rankPosition <= 25
                ? 'warn'
                : 'neutral'
              : 'neutral'
          }
        />
      </div>

      {/* Lifecycle */}
      <div>
        <p className="mb-2 text-[11px] font-semibold uppercase tracking-widest text-slate-500">
          Lifecycle
        </p>
        <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
          {(['rank', 'breakout', 'pattern', 'execution'] as const).map((stage) => (
            <div
              key={stage}
              className={cn('rounded-xl border px-3 py-2 text-xs', lifecycleTone(lifecycle[stage]))}
            >
              <p className="text-[10px] uppercase tracking-widest opacity-70">{stage}</p>
              <p className="mt-1 text-sm font-semibold">{lifecycle[stage]}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Metadata grid */}
      <div>
        <p className="mb-2 text-[11px] font-semibold uppercase tracking-widest text-slate-500">
          Metadata
        </p>
        <dl className="grid grid-cols-2 gap-x-4 gap-y-2 rounded-xl border border-slate-800 bg-slate-950/40 p-4 text-xs lg:grid-cols-4">
          <Field label="ISIN" value={meta?.isin ?? '—'} />
          <Field label="Sector" value={meta?.sector ?? '—'} />
          <Field label="Industry" value={meta?.industry ?? '—'} />
          <Field label="Exchange" value={meta?.exchange ?? '—'} />
          <Field label="Instrument" value={meta?.instrumentType ?? '—'} />
          <Field
            label="Lot size"
            value={meta?.lotSize !== null && meta?.lotSize !== undefined ? String(meta.lotSize) : '—'}
          />
          <Field
            label="Tick size"
            value={meta?.tickSize !== null && meta?.tickSize !== undefined ? String(meta.tickSize) : '—'}
          />
          <Field label="Market cap" value={fmtMcap(meta?.mcap ?? null)} />
        </dl>
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  sub,
  tone,
}: {
  label: string;
  value: string;
  sub?: string;
  tone: 'good' | 'warn' | 'bad' | 'neutral';
}) {
  const toneCls = {
    good: 'border-emerald-500/40 bg-emerald-500/10',
    warn: 'border-amber-500/40 bg-amber-500/10',
    bad: 'border-rose-500/40 bg-rose-500/10',
    neutral: 'border-slate-800 bg-slate-950/40',
  }[tone];
  return (
    <div className={cn('rounded-xl border p-3', toneCls)}>
      <p className="text-[10px] font-semibold uppercase tracking-widest text-slate-400">
        {label}
      </p>
      <p className="mt-1 text-lg font-semibold tabular-nums text-slate-100">{value}</p>
      {sub ? <p className="mt-0.5 text-[11px] text-slate-400">{sub}</p> : null}
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-widest text-slate-500">{label}</dt>
      <dd className="mt-0.5 truncate font-mono text-[12px] text-slate-200">{value}</dd>
    </div>
  );
}
