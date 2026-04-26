/**
 * Risk & Scenarios tab — surfaces the same heuristics that power
 * ``components/execution/derive.ts`` for a single symbol.
 *
 * Inputs: ``StockRow`` resolved from the ranking list (the workspace passes
 * the live row in so we don't have to re-derive). Three scenarios:
 *
 *   * Base — the canonical entry/stop/target.
 *   * Aggressive — 1.25× size%, tighter stop (×0.85), higher target.
 *   * Conservative — 0.75× size%, looser stop (×1.15), trimmed target.
 *
 * All numbers are heuristic — when the broker risk service exists the
 * ``derive.ts`` swap-out is mechanical.
 */
import type { StockRow } from '@/types/dashboard';
import { deriveOrder } from '@/components/execution/derive';
import { cn } from '@/lib/utils/cn';

interface Props {
  row: StockRow | null;
}

interface Scenario {
  key: string;
  label: string;
  tone: string;
  entry: number;
  stop: number;
  target: number;
  rr: number;
  sizePct: number;
  rRisk: number;
}

function build(row: StockRow): Scenario[] {
  const base = deriveOrder(row);
  const stopDistance = base.entry - base.stop;
  const targetDistance = base.target - base.entry;

  const aggressive: Scenario = {
    key: 'aggressive',
    label: 'Aggressive',
    tone: 'border-rose-500/40 bg-rose-500/10',
    entry: base.entry,
    stop: base.entry - stopDistance * 0.85,
    target: base.entry + targetDistance * 1.4,
    rr: base.riskReward * (1.4 / 0.85),
    sizePct: Math.min(8, base.sizePct * 1.25),
    rRisk: stopDistance * 0.85 * Math.min(8, base.sizePct * 1.25),
  };

  const conservative: Scenario = {
    key: 'conservative',
    label: 'Conservative',
    tone: 'border-emerald-500/40 bg-emerald-500/10',
    entry: base.entry,
    stop: base.entry - stopDistance * 1.15,
    target: base.entry + targetDistance * 0.7,
    rr: base.riskReward * (0.7 / 1.15),
    sizePct: base.sizePct * 0.75,
    rRisk: stopDistance * 1.15 * base.sizePct * 0.75,
  };

  return [
    {
      key: 'base',
      label: 'Base',
      tone: 'border-blue-500/40 bg-blue-500/10',
      entry: base.entry,
      stop: base.stop,
      target: base.target,
      rr: base.riskReward,
      sizePct: base.sizePct,
      rRisk: stopDistance * base.sizePct,
    },
    aggressive,
    conservative,
  ];
}

export default function RiskScenariosTab({ row }: Props) {
  if (!row) {
    return (
      <p className="text-xs text-slate-500">
        Risk scenarios are derived from the ranking row. Open the workspace from the Ranking,
        Patterns, or Execution page so the live row is available.
      </p>
    );
  }

  const scenarios = build(row);

  return (
    <div className="space-y-3">
      <p className="text-xs text-slate-400">
        All numbers are derived heuristics — when the broker risk service is wired in,
        ``components/execution/derive.ts`` will be the single swap-out point.
      </p>
      <div className="overflow-hidden rounded-2xl border border-slate-800">
        <table className="w-full text-sm">
          <thead className="bg-slate-900/80 text-[10px] uppercase tracking-widest text-slate-500">
            <tr>
              <th className="px-3 py-2 text-left">Scenario</th>
              <th className="px-3 py-2 text-right">Entry</th>
              <th className="px-3 py-2 text-right">Stop</th>
              <th className="px-3 py-2 text-right">Target</th>
              <th className="px-3 py-2 text-right">R:R</th>
              <th className="px-3 py-2 text-right">Size %</th>
              <th className="px-3 py-2 text-right">Risk (₹×%)</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {scenarios.map((s) => (
              <tr key={s.key} className="text-slate-200">
                <td className="px-3 py-2">
                  <span className={cn('rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-wider', s.tone)}>
                    {s.label}
                  </span>
                </td>
                <td className="px-3 py-2 text-right tabular-nums">{s.entry.toFixed(2)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{s.stop.toFixed(2)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{s.target.toFixed(2)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{s.rr.toFixed(2)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{s.sizePct.toFixed(2)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{s.rRisk.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
