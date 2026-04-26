/**
 * Sparkline of historical rank position with a pattern overlay.
 *
 * Lower rank position = stronger candidate, so we invert the y-axis so the
 * line trends *up* when the symbol's rank improved over time. Gaps (runs
 * where the symbol was absent) are dropped so the line stays continuous.
 */
import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { RankingHistory } from '@/lib/api/ranking';
import type { StockRow } from '@/types/dashboard';

interface Props {
  history: RankingHistory | undefined;
  row: StockRow;
  isLoading: boolean;
}

interface ChartPoint {
  date: string;
  rank: number;
  composite: number | null;
}

function buildPoints(history: RankingHistory | undefined): ChartPoint[] {
  if (!history) return [];
  // Backend returns newest-first; reverse so the chart reads left-to-right.
  return [...history.history]
    .reverse()
    .filter((p) => p.rankPosition !== null)
    .map((p) => ({
      date: p.runDate ?? p.runId,
      rank: p.rankPosition as number,
      composite: p.compositeScore,
    }));
}

export default function MiniChart({ history, row, isLoading }: Props) {
  const points = buildPoints(history);
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
      <div className="flex items-center justify-between">
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          Rank History
        </h4>
        <span className="text-[10px] uppercase tracking-wider text-slate-500">
          Pattern: {row.pattern || 'N/A'}
        </span>
      </div>
      <div className="mt-3 h-36 w-full">
        {isLoading ? (
          <div className="flex h-full items-center justify-center text-xs text-slate-500">
            Loading history…
          </div>
        ) : points.length === 0 ? (
          <div className="flex h-full items-center justify-center text-xs text-slate-500">
            No historical rank data for this symbol.
          </div>
        ) : (
          <ResponsiveContainer>
            <AreaChart data={points} margin={{ top: 6, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="rank-fill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#34d399" stopOpacity={0.4} />
                  <stop offset="100%" stopColor="#34d399" stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="date"
                tick={{ fill: '#64748b', fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                minTickGap={24}
              />
              <YAxis
                reversed
                domain={['dataMin - 1', 'dataMax + 1']}
                tick={{ fill: '#64748b', fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                width={28}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#0f172a',
                  border: '1px solid #1e293b',
                  borderRadius: 8,
                  color: '#e2e8f0',
                  fontSize: 12,
                }}
                labelStyle={{ color: '#94a3b8' }}
                formatter={(value: unknown, name) => [String(value), name]}
              />
              <Area
                type="monotone"
                dataKey="rank"
                stroke="#34d399"
                strokeWidth={2}
                fill="url(#rank-fill)"
                isAnimationActive={false}
                name="Rank"
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>
      <p className="mt-2 text-[11px] text-slate-500">
        Y-axis inverted — higher line = better rank (lower numerical position).
      </p>
    </div>
  );
}
