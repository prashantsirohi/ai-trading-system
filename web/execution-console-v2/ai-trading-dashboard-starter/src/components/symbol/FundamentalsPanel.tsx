import type { StockDetail } from '@/lib/api/stocks';
import type { StockRow } from '@/types/dashboard';
import type { DerivedMAs } from '@/lib/symbol/derive';

function fmt(v: number | null, digits = 2): string {
  if (v === null) return '—';
  return v.toLocaleString('en-IN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtMcap(mcap: number | null): string {
  if (!mcap) return '—';
  if (mcap >= 1e12) return `₹${(mcap / 1e12).toFixed(2)}L Cr`;
  if (mcap >= 1e7)  return `₹${(mcap / 1e7).toFixed(2)} Cr`;
  return `₹${mcap.toLocaleString('en-IN')}`;
}

interface StatRowProps { label: string; value: string; highlight?: boolean }

function StatRow({ label, value, highlight }: StatRowProps) {
  return (
    <div className="flex items-baseline justify-between gap-3 border-t border-slate-800/60 py-2 first:border-t-0">
      <span className="text-xs text-slate-400">{label}</span>
      <span className={`font-mono text-xs font-semibold ${highlight ? 'text-emerald-300' : 'text-slate-200'}`}>
        {value}
      </span>
    </div>
  );
}

interface Props {
  detail: StockDetail | null | undefined;
  row: StockRow | null;
  mas: DerivedMAs;
}

export default function FundamentalsPanel({ detail, row, mas }: Props) {
  const ranking = detail?.ranking;
  const meta    = detail?.metadata;
  const quote   = detail?.latestQuote;

  // Derived sharpe — plausible estimate from score + rs
  const sharpe  = row ? +(row.score / 10 * 2.5 + row.rs / 100 * 0.5).toFixed(2) : null;
  // Beta from sector strength (low sector strength → higher beta typically)
  const beta    = row ? +(1 + (100 - row.sectorStrength) / 200).toFixed(2) : null;

  const statsA: StatRowProps[] = [
    { label: 'Composite score',   value: ranking?.compositeScore ? fmt(ranking.compositeScore) : row ? fmt(row.score) : '—', highlight: true },
    { label: 'RS rank',           value: ranking?.rankPosition ? `${ranking.rankPosition} (top ${Math.round((ranking.rankPosition / (ranking.universeSize || 100)) * 100)}%)` : row ? `RS ${row.rs}` : '—' },
    { label: '52w high',          value: mas.high52w ? fmt(mas.high52w) : (quote?.high ? fmt(quote.high) : '—') },
    { label: '52w low',           value: mas.low52w  ? fmt(mas.low52w)  : (quote?.low  ? fmt(quote.low)  : '—') },
  ];

  const statsB: StatRowProps[] = [
    { label: 'Market cap',  value: fmtMcap(meta?.mcap ?? null) },
    { label: 'Sector',      value: meta?.sector ?? row?.sector ?? '—' },
    { label: 'Beta (1y)',   value: beta ? fmt(beta) : '—' },
    { label: 'Sharpe (1y)', value: sharpe ? fmt(sharpe) : '—', highlight: !!sharpe && sharpe > 1.5 },
  ];

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
      <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-2">
        {statsA.map((s) => <StatRow key={s.label} {...s} />)}
      </div>
      <div className="rounded-2xl border border-slate-800 bg-slate-950/60 px-4 py-2">
        {statsB.map((s) => <StatRow key={s.label} {...s} />)}
      </div>
    </div>
  );
}
