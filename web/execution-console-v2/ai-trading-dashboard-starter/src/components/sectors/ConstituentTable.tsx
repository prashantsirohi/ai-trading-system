import { Link } from 'react-router-dom';
import { cn } from '@/lib/utils/cn';
import { useWorkspace } from '@/components/workspace/WorkspaceContext';
import type { Constituent } from '@/lib/mock/sectorConstituents';
import type { IndicatorKey } from './TechFilterRail';

interface IndChip {
  label: string;
  tone: 'bull' | 'bear' | 'neutral';
}

function chips(row: Constituent): IndChip[] {
  return [
    { label: row.aboveMa50 ? '50↑' : '50↓', tone: row.aboveMa50 ? 'bull' : 'bear' },
    { label: 'RSI', tone: row.rsiInRange ? 'bull' : 'bear' },
    { label: 'VOL', tone: row.volExpand ? 'bull' : 'bear' },
    { label: 'MACD', tone: row.macdBullish ? 'bull' : row.macd === 0 ? 'neutral' : 'bear' },
  ];
}

const CHIP_CLASS: Record<IndChip['tone'], string> = {
  bull:    'border-emerald-700/50 bg-emerald-500/10 text-emerald-300',
  bear:    'border-rose-700/50 bg-rose-500/10 text-rose-300',
  neutral: 'border-slate-700 bg-slate-800/60 text-slate-400',
};

const STAGE_CLASS: Record<string, string> = {
  S2: 'border-emerald-600/50 bg-emerald-500/15 text-emerald-300',
  S1: 'border-blue-600/50   bg-blue-500/15    text-blue-300',
  S3: 'border-amber-600/50  bg-amber-500/15   text-amber-300',
  S4: 'border-rose-600/50   bg-rose-500/15    text-rose-300',
};

interface Props {
  rows: (Constituent & { stageLabel?: string | null; compositeScore?: number | null; name?: string })[];
  hiddenCount: number;
  activeFilters: Set<IndicatorKey>;
  onShowAll: () => void;
}

export default function ConstituentTable({ rows, hiddenCount, activeFilters, onShowAll }: Props) {
  const { openWorkspace } = useWorkspace();

  return (
    <div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-slate-800 text-left text-[10px] uppercase tracking-wider text-slate-500">
              <th className="pb-2 pr-3">Stage</th>
              <th className="pb-2 pr-4">Ticker</th>
              <th className="pb-2 pr-4 text-right">Px</th>
              <th className="pb-2 pr-4 text-right">Chg%</th>
              <th className="pb-2 pr-4 text-right">RSI</th>
              <th className="pb-2 pr-4 text-right">50DMA</th>
              <th className="pb-2 pr-4 text-right">Vol×</th>
              <th className="pb-2 pr-4 text-right">Score</th>
              <th className="pb-2">Indicators</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/40">
            {rows.map((row) => {
              const indChips = chips(row);
              const stageLabel = (row as any).stageLabel as string | null | undefined;
              const score = (row as any).compositeScore ?? row.score;
              return (
                <tr
                  key={row.symbol}
                  className="hover:bg-slate-900/40 transition-colors cursor-pointer"
                  onClick={() => openWorkspace(row.symbol)}
                >
                  <td className="py-2 pr-3">
                    {stageLabel ? (
                      <span className={cn('rounded border px-1.5 py-0.5 text-[9px] font-bold', STAGE_CLASS[stageLabel] ?? CHIP_CLASS.neutral)}>
                        {stageLabel}
                      </span>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
                  </td>
                  <td className="py-2 pr-4">
                    <Link
                      to={`/symbol/${row.symbol}`}
                      onClick={(e) => e.stopPropagation()}
                      className="font-semibold text-blue-400 hover:underline"
                    >
                      {row.symbol}
                    </Link>
                  </td>
                  <td className="py-2 pr-4 text-right font-mono text-slate-200">
                    {row.price ? row.price.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '—'}
                  </td>
                  <td className={cn('py-2 pr-4 text-right font-mono', row.chgPct >= 0 ? 'text-emerald-400' : 'text-rose-400')}>
                    {row.chgPct >= 0 ? '+' : ''}{row.chgPct.toFixed(2)}
                  </td>
                  <td className="py-2 pr-4 text-right font-mono text-slate-300">{row.rsi}</td>
                  <td className={cn('py-2 pr-4 text-right font-mono', row.ma50Pct >= 0 ? 'text-emerald-400' : 'text-rose-400')}>
                    {row.ma50Pct >= 0 ? '+' : ''}{row.ma50Pct.toFixed(1)}%
                  </td>
                  <td className="py-2 pr-4 text-right font-mono text-slate-300">{row.volMult.toFixed(1)}×</td>
                  <td className="py-2 pr-4 text-right font-mono text-slate-200">
                    {score != null ? Number(score).toFixed(1) : '—'}
                  </td>
                  <td className="py-2">
                    <div className="flex flex-wrap gap-1">
                      {indChips.map((chip) => (
                        <span
                          key={chip.label}
                          className={cn(
                            'rounded border px-1.5 py-0.5 text-[9px] font-semibold uppercase',
                            CHIP_CLASS[chip.tone],
                          )}
                        >
                          {chip.label}
                        </span>
                      ))}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {hiddenCount > 0 && (
        <p className="mt-2 text-[11px] text-slate-500">
          Hidden by filter: {hiddenCount} constituent{hiddenCount > 1 ? 's' : ''} not matching all {activeFilters.size} condition{activeFilters.size > 1 ? 's' : ''}.{' '}
          <button
            type="button"
            onClick={onShowAll}
            className="text-blue-400 hover:underline"
          >
            Show all
          </button>
        </p>
      )}

      {rows.length === 0 && (
        <p className="py-6 text-center text-sm text-slate-500">
          No constituents match all active filters — try removing a condition.
        </p>
      )}
    </div>
  );
}
