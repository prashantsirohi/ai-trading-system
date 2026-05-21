import type { WorkspaceSnapshot } from '@/lib/api/workspace';

interface Props {
  snapshot: WorkspaceSnapshot;
}

function pct(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return '—';
  return `${Math.round(value * 100)}%`;
}

function value(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === '') return '—';
  return String(value);
}

export default function MarketDirectionCard({ snapshot }: Props) {
  const summary = snapshot.summary;
  const phaseLabel = value(summary.regimePhaseLabel);
  const phaseText = phaseLabel === '—' ? '—' : `${summary.regimePhaseEmoji ?? ''} ${phaseLabel}`.trim();
  const setupBits = [
    summary.requiredMinScore === null ? null : `Score >= ${summary.requiredMinScore}`,
    summary.requiredBreakoutTier ? `${summary.requiredBreakoutTier} breakout` : null,
    summary.requiredSetupQualityGte === null ? null : `Setup >= ${Math.round(summary.requiredSetupQualityGte * 100)}%`,
  ].filter(Boolean);

  return (
    <section className="rounded-lg border border-slate-800 bg-slate-950/70 p-4 shadow-soft">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="min-w-0">
          <p className="text-[10px] font-bold uppercase tracking-widest text-slate-500">Market Direction</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-100">{value(summary.directionBias)}</h2>
          <p className="mt-1 text-sm text-slate-400">
            {value(summary.marketRegime)} / {value(summary.breadthVelocityBucket)}
          </p>
          <p className="mt-2 text-sm font-semibold text-slate-200">{phaseText}</p>
        </div>
        <div className="grid grid-cols-2 gap-2 text-sm md:min-w-[280px]">
          <Metric label="Action" value={value(summary.directionAction)} />
          <Metric label="Exposure" value={pct(summary.allowedExposure)} />
          <Metric label="New Buys" value={summary.newBuysAllowed === null ? '—' : summary.newBuysAllowed ? 'Yes' : 'No'} />
          <Metric label="Age" value={summary.regimeAgeDays === null ? '—' : `${summary.regimeAgeDays}d`} />
        </div>
      </div>
      <div className="mt-3 grid grid-cols-1 gap-2 border-t border-slate-800/80 pt-3 text-sm md:grid-cols-2">
        <Metric label="Confidence" value={pct(summary.regimeConfidenceCapped)} />
        <Metric label="Required Setup" value={setupBits.length ? setupBits.join(' | ') : '—'} />
      </div>
      <div className="mt-2 grid grid-cols-1 gap-2 text-sm md:grid-cols-3">
        <Metric label="S2 Breadth" value={pct(summary.regimePhaseS2Pct)} />
        <Metric label="Phase Stage" value={value(summary.regimePhaseMarketStage)} />
        <Metric label="Phase Velocity" value={value(summary.regimePhaseVelocity)} />
      </div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded border border-slate-800 bg-slate-900/60 px-3 py-2">
      <div className="text-[10px] uppercase tracking-widest text-slate-500">{label}</div>
      <div className="mt-1 truncate font-semibold text-slate-100">{value}</div>
    </div>
  );
}
