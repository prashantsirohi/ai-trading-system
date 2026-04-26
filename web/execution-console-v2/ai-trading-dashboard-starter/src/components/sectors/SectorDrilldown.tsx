/**
 * Drill-down panel for the selected sector.
 *
 * Shows a short auto-generated narrative, RS / breakouts / breadth stats,
 * the sector's top-3 ranked constituents, and a relative-performance line
 * that walks `rs100 → rs50 → rs20 → rs` as a 4-step proxy timeline.
 *
 * Uses recharts for the line so the visual matches the rest of the v2
 * dashboard.
 */
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { SectorScore, StockRow } from '@/types/dashboard';
import { cn } from '@/lib/utils/cn';

interface Props {
  sector: SectorScore;
  rankedRows: StockRow[];
}

function topConstituents(sector: string, rows: StockRow[], limit = 3): StockRow[] {
  return rows
    .filter((row) => row.sector === sector)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

function narrativeFor(sector: SectorScore, count: number, breakoutCount: number): string {
  const trend = sector.momentum >= 0.2 ? 'accelerating' : sector.momentum >= 0 ? 'stable' : 'fading';
  const breadth = count >= 5 ? 'broad' : count >= 2 ? 'thinning' : 'concentrated';
  const breakoutText = breakoutCount > 0 ? `with ${breakoutCount} active breakout${breakoutCount > 1 ? 's' : ''}` : 'with no fresh breakouts yet';
  return `${sector.sector} is ${trend} (${sector.quadrant}) on ${breadth} participation ${breakoutText}.`;
}

function buildSeries(sector: SectorScore): Array<{ label: string; rs: number }> {
  return [
    { label: 'D-5', rs: sector.rs100 },
    { label: 'D-3', rs: sector.rs50 },
    { label: 'D-2', rs: sector.rs20 },
    { label: 'D-1', rs: sector.rs },
  ];
}

export default function SectorDrilldown({ sector, rankedRows }: Props) {
  const constituents = topConstituents(sector.sector, rankedRows);
  const breakoutCount = rankedRows.filter((r) => r.sector === sector.sector && r.breakout).length;
  const constituentCount = rankedRows.filter((r) => r.sector === sector.sector).length;
  const series = buildSeries(sector);

  return (
    <div className="space-y-4 rounded-xl border border-slate-800 bg-slate-950/60 p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-base font-semibold text-slate-100">{sector.sector}</h3>
        <span className="text-[10px] uppercase tracking-widest text-slate-500">
          Quadrant: <span className="text-slate-300">{sector.quadrant}</span>
        </span>
      </div>

      <p className="text-sm leading-relaxed text-slate-300">
        {narrativeFor(sector, constituentCount, breakoutCount)}
      </p>

      <dl className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        <Stat label="RS" value={sector.rs.toFixed(0)} />
        <Stat label="Momentum" value={(sector.momentum >= 0 ? '+' : '') + sector.momentum.toFixed(2)} />
        <Stat label="Constituents" value={String(constituentCount)} />
        <Stat label="Breakouts" value={String(breakoutCount)} />
      </dl>

      <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-3">
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          RS trajectory (proxy)
        </h4>
        <div className="mt-2 h-32">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={series} margin={{ top: 6, right: 8, left: 0, bottom: 0 }}>
              <XAxis dataKey="label" tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis domain={[0, 100]} tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} width={28} />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#0f172a',
                  border: '1px solid #1e293b',
                  borderRadius: 8,
                  color: '#e2e8f0',
                  fontSize: 12,
                }}
              />
              <Line type="monotone" dataKey="rs" stroke="#34d399" strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div>
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          Top Constituents
        </h4>
        {constituents.length === 0 ? (
          <p className="mt-2 text-xs text-slate-500">
            No ranked constituents in this sector yet.
          </p>
        ) : (
          <ul className="mt-2 space-y-1.5">
            {constituents.map((row) => (
              <li
                key={row.symbol}
                className={cn(
                  'flex items-center justify-between rounded-lg border border-slate-800 bg-slate-900/60 px-3 py-2 text-sm',
                )}
              >
                <span className="font-semibold text-slate-100">{row.symbol}</span>
                <span className="text-xs text-slate-400">
                  Tier {row.tier} · Score{' '}
                  <span className="font-semibold text-slate-200">{row.score.toFixed(2)}</span> · RS{' '}
                  <span className="font-semibold text-slate-200">{row.rs}</span>
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/60 p-3">
      <dt className="text-[10px] uppercase tracking-wider text-slate-500">{label}</dt>
      <dd className="mt-1 text-base font-semibold tabular-nums text-slate-100">{value}</dd>
    </div>
  );
}
