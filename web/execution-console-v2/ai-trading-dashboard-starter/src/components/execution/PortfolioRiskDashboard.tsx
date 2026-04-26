/**
 * Portfolio risk dashboard — three KPI gauges:
 *
 *   * Concentration: largest single-symbol size%.
 *   * Top sector exposure: largest single-sector aggregate size%.
 *   * Estimated max drawdown: sum of (entry-stop)/entry × size%, summed
 *     across eligible orders.
 *
 * Tones cross thresholds the operator can lean on at a glance: green
 * "comfortable", amber "watch", rose "policy breach".
 */
import { cn } from '@/lib/utils/cn';
import type { ExecutionDerived } from './derive';

interface Props {
  derived: ExecutionDerived;
  capitalLimitPct: number;
}

interface Tone {
  ring: string;
  bar: string;
  label: 'Healthy' | 'Watch' | 'Hot';
}

function tone(value: number, soft: number, hot: number): Tone {
  if (value >= hot) {
    return { ring: 'border-rose-500/40 bg-rose-500/15', bar: 'bg-rose-500/70', label: 'Hot' };
  }
  if (value >= soft) {
    return { ring: 'border-amber-500/40 bg-amber-500/15', bar: 'bg-amber-500/70', label: 'Watch' };
  }
  return { ring: 'border-emerald-500/40 bg-emerald-500/15', bar: 'bg-emerald-500/70', label: 'Healthy' };
}

export default function PortfolioRiskDashboard({ derived, capitalLimitPct }: Props) {
  const concentration = derived.concentrationPct;
  const topSector = derived.topSector?.pct ?? 0;
  const drawdown = derived.estMaxDrawdownPct;

  const concentrationTone = tone(concentration, 4, 6);
  const sectorTone = tone(topSector, 12, 18);
  const drawdownTone = tone(drawdown, 1.5, 3);

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4">
      <div className="flex items-baseline justify-between">
        <h4 className="text-xs font-semibold uppercase tracking-widest text-slate-400">
          Portfolio Risk
        </h4>
        <span className="text-[10px] uppercase tracking-widest text-slate-500">
          Cap limit {capitalLimitPct.toFixed(0)}%
        </span>
      </div>
      <div className="mt-3 space-y-3">
        <Gauge
          label="Concentration"
          value={`${concentration.toFixed(2)}%`}
          hint="Largest single-symbol size%"
          fillPct={concentration / capitalLimitPct}
          tone={concentrationTone}
        />
        <Gauge
          label="Top Sector"
          value={
            derived.topSector ? `${derived.topSector.name} · ${topSector.toFixed(2)}%` : '—'
          }
          hint="Aggregate size by dominant sector"
          fillPct={topSector / capitalLimitPct}
          tone={sectorTone}
        />
        <Gauge
          label="Est. Max Drawdown"
          value={`${drawdown.toFixed(2)}%`}
          hint="Σ (entry-stop)/entry × size%"
          fillPct={drawdown / 5}
          tone={drawdownTone}
        />
      </div>
    </div>
  );
}

interface GaugeProps {
  label: string;
  value: string;
  hint: string;
  fillPct: number;
  tone: Tone;
}

function Gauge({ label, value, hint, fillPct, tone }: GaugeProps) {
  const widthPct = Math.max(2, Math.min(100, Math.round(fillPct * 100)));
  return (
    <div className={cn('rounded-lg border p-3', tone.ring)}>
      <div className="flex items-baseline justify-between text-xs">
        <span className="font-semibold uppercase tracking-wider text-slate-300">{label}</span>
        <span className="rounded-full border border-slate-700 px-2 text-[10px] uppercase tracking-wider text-slate-300">
          {tone.label}
        </span>
      </div>
      <div className="mt-1 text-sm font-semibold tabular-nums text-slate-100">{value}</div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-800">
        <div className={cn('h-full rounded-full', tone.bar)} style={{ width: `${widthPct}%` }} />
      </div>
      <p className="mt-1 text-[10px] text-slate-500">{hint}</p>
    </div>
  );
}
