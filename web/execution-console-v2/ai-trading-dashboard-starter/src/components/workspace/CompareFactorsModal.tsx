/**
 * Compare-Factors modal — tabular factor diff for up to 3 symbols.
 *
 * Pulls each symbol's ranking detail (factor blocks + composite score) and
 * lays them out side-by-side. Supports an Absolute / Relative toggle:
 *
 *   * Absolute — the raw 0-100 factor score per bucket.
 *   * Relative — each cell expressed as the delta vs. the row's max
 *     (so the leader is 0.00 and laggards are negative).
 */
import { useMemo, useState } from 'react';

import { useRankingDetail } from '@/lib/queries';
import { useWorkspace } from './WorkspaceContext';
import VerdictBanner from '@/components/ranking/VerdictBanner';
import { cn } from '@/lib/utils/cn';

type Mode = 'absolute' | 'relative';

const BUCKETS: Array<'rs' | 'volume' | 'trend' | 'sector'> = ['rs', 'volume', 'trend', 'sector'];
const BUCKET_LABELS: Record<string, string> = {
  rs: 'Relative Strength',
  volume: 'Volume',
  trend: 'Trend',
  sector: 'Sector',
};

export default function CompareFactorsModal() {
  const { compareOpen, closeCompare, compareSymbols, clearCompare } = useWorkspace();
  const [mode, setMode] = useState<Mode>('absolute');

  // Always run the hooks (rules of hooks), even if the modal is closed —
  // fallback to up to 3 symbols then disable when undefined.
  const a = useRankingDetail(compareSymbols[0] ?? null);
  const b = useRankingDetail(compareSymbols[1] ?? null);
  const c = useRankingDetail(compareSymbols[2] ?? null);
  const detailQueries = [a, b, c];

  const rows = useMemo(() => {
    return BUCKETS.map((bucket) => {
      const values = compareSymbols.map((_, idx) => {
        const block = detailQueries[idx]?.data?.factors.find((f) => f.bucket === bucket);
        return block?.value ?? null;
      });
      const max = Math.max(...values.filter((v): v is number => v !== null));
      return { bucket, values, max };
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [compareSymbols, a.data, b.data, c.data]);

  if (!compareOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/85 p-4">
      <div className="absolute inset-0" onClick={closeCompare} aria-hidden role="presentation" />
      <div className="relative z-10 max-h-[92vh] w-full max-w-5xl overflow-hidden rounded-3xl border border-slate-800 bg-slate-950 shadow-2xl">
        <header className="flex flex-wrap items-center gap-3 border-b border-slate-800 px-5 py-3">
          <div>
            <h3 className="text-lg font-semibold text-slate-100">Compare Factors</h3>
            <p className="text-xs text-slate-400">
              {compareSymbols.length === 0
                ? 'Pin up to 3 symbols from the Ranking page to compare their factor profiles.'
                : `${compareSymbols.length} symbol${compareSymbols.length === 1 ? '' : 's'} selected.`}
            </p>
          </div>
          <div className="ml-auto flex items-center gap-2">
            <div className="flex rounded-full border border-slate-700 bg-slate-900/60 p-0.5">
              {(['absolute', 'relative'] as Mode[]).map((m) => (
                <button
                  key={m}
                  type="button"
                  onClick={() => setMode(m)}
                  className={cn(
                    'rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-wider transition-colors',
                    mode === m
                      ? 'bg-blue-500/20 text-blue-100'
                      : 'text-slate-400 hover:text-slate-200',
                  )}
                >
                  {m}
                </button>
              ))}
            </div>
            <button
              type="button"
              onClick={clearCompare}
              className="rounded-full border border-slate-700 px-3 py-1 text-[11px] font-semibold uppercase tracking-wider text-slate-300 hover:border-rose-500/60 hover:text-rose-200"
            >
              Clear
            </button>
            <button
              type="button"
              onClick={closeCompare}
              className="rounded-full border border-slate-700 px-3 py-1 text-[11px] font-semibold uppercase tracking-wider text-slate-300 hover:border-slate-500"
            >
              Close ✕
            </button>
          </div>
        </header>

        <div className="max-h-[78vh] overflow-y-auto p-5">
          {compareSymbols.length === 0 ? (
            <p className="text-sm text-slate-500">Nothing pinned yet.</p>
          ) : (
            <div className="space-y-4">
              <div className="grid grid-cols-1 gap-3 lg:grid-cols-3">
                {compareSymbols.map((symbol, idx) => {
                  const detail = detailQueries[idx]?.data;
                  const composite = detail?.ranking?.compositeScore;
                  const rankPos = detail?.ranking?.rankPosition;
                  return (
                    <div
                      key={symbol}
                      className="rounded-2xl border border-slate-800 bg-slate-950/40 p-4"
                    >
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-base text-slate-100">{symbol}</span>
                        <span className="rounded-full border border-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-400">
                          {rankPos ? `#${rankPos}` : '—'}
                        </span>
                      </div>
                      <p className="mt-1 font-mono text-lg text-slate-200">
                        {composite !== null && composite !== undefined
                          ? composite.toFixed(2)
                          : '—'}
                      </p>
                      <p className="text-[11px] uppercase tracking-widest text-slate-500">
                        composite score
                      </p>
                      <div className="mt-3">
                        <VerdictBanner
                          decision={
                            detail?.decision ?? {
                              verdict: null,
                              confidence: null,
                              reason: null,
                            }
                          }
                        />
                      </div>
                    </div>
                  );
                })}
              </div>

              <div className="overflow-hidden rounded-2xl border border-slate-800">
                <table className="w-full text-sm">
                  <thead className="bg-slate-900/80 text-[10px] uppercase tracking-widest text-slate-500">
                    <tr>
                      <th className="px-3 py-2 text-left">Factor</th>
                      {compareSymbols.map((s) => (
                        <th key={s} className="px-3 py-2 text-right">
                          {s}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800 text-slate-200">
                    {rows.map((row) => (
                      <tr key={row.bucket}>
                        <td className="px-3 py-2 text-[11px] uppercase tracking-widest text-slate-400">
                          {BUCKET_LABELS[row.bucket]}
                        </td>
                        {row.values.map((v, idx) => {
                          if (v === null) {
                            return (
                              <td key={idx} className="px-3 py-2 text-right text-slate-600">
                                —
                              </td>
                            );
                          }
                          const display =
                            mode === 'absolute' ? v.toFixed(1) : (v - row.max).toFixed(1);
                          const tone =
                            mode === 'relative'
                              ? v === row.max
                                ? 'text-emerald-300'
                                : 'text-rose-300'
                              : 'text-slate-100';
                          return (
                            <td
                              key={idx}
                              className={cn('px-3 py-2 text-right tabular-nums', tone)}
                            >
                              {display}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
