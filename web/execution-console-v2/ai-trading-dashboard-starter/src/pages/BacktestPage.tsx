/**
 * Engine-driven backtest UI.
 *
 * - Lists risk profiles from /api/execution/backtest/profiles
 * - Inspector pane shows the picked profile's entry / stop / exit / sizing knobs
 * - Form to pick date window + equity → POST /api/execution/backtest/run
 * - Results table with full provenance (entry_reason, exit_reason, stop_method, etc.)
 *
 * Reuses the same TradingRuleEngine that paper trading uses; identical inputs
 * produce identical decisions across both surfaces.
 */

import { useMemo, useState } from 'react';
import { useMutation } from '@tanstack/react-query';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import { useRiskProfiles } from '@/lib/queries';
import {
  runBacktest,
  type BacktestRunResult,
  type RiskProfile,
} from '@/lib/api/backtest';

const NUMERIC_FORMATTER = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });
const fmt = (v: number | null | undefined): string =>
  v == null || Number.isNaN(v) ? '—' : NUMERIC_FORMATTER.format(v);
const fmtPct = (v: number | null | undefined): string =>
  v == null || Number.isNaN(v) ? '—' : `${(v * 100).toFixed(2)}%`;

const REASON_STYLES: Record<string, string> = {
  hard_stop: 'bg-rose-500/15 text-rose-300 border-rose-500/40',
  close_below_200dma: 'bg-rose-500/15 text-rose-300 border-rose-500/40',
  close_below_50dma: 'bg-amber-500/15 text-amber-300 border-amber-500/40',
  close_below_20dma: 'bg-amber-500/15 text-amber-300 border-amber-500/40',
  close_below_11dma: 'bg-amber-500/15 text-amber-300 border-amber-500/40',
  rank_deterioration_streak: 'bg-sky-500/15 text-sky-300 border-sky-500/40',
  score_deterioration_streak: 'bg-sky-500/15 text-sky-300 border-sky-500/40',
  time_stop: 'bg-slate-500/15 text-slate-300 border-slate-500/40',
  backtest_end: 'bg-slate-700/30 text-slate-400 border-slate-600',
  entry_confirmed: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/40',
};

function ReasonBadge({ reason }: { reason: string | null }) {
  if (!reason) return <span className="text-slate-500">—</span>;
  const style = REASON_STYLES[reason] ?? 'bg-slate-700/30 text-slate-300 border-slate-600';
  return (
    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${style}`}>
      {reason}
    </span>
  );
}

function ProfileInspector({ profile }: { profile: RiskProfile | null }) {
  if (!profile) {
    return <p className="text-sm text-slate-500">Select a profile to inspect its rules.</p>;
  }
  const rows: Array<[string, string]> = [
    ['Entry — Stage 2 required', String(profile.entry.requireStage2)],
    ['Entry — above 200 DMA', String(profile.entry.requirePriceAboveSma200)],
    ['Entry — sector positive', String(profile.entry.requireSectorPositive)],
    ['Entry — min volume ratio', profile.entry.minVolumeRatio.toFixed(2)],
    ['Stop — method', profile.stop.method],
    ['Stop — ATR ×', profile.stop.atrMultiple.toFixed(2)],
    ['Stop — % fallback', `${(profile.stop.stopPct * 100).toFixed(1)}%`],
    ['Exit — DMA window', profile.exit.dmaExitWindow == null ? '—' : `${profile.exit.dmaExitWindow} DMA`],
    ['Exit — 200 DMA emergency', String(profile.exit.emergencyExitBelowSma200)],
    ['Exit — DMA whipsaw buffer', `${profile.exit.dmaWhipsawBufferPct}%`],
    ['Exit — max rank for hold', String(profile.exit.maxHoldRank)],
    ['Exit — rank streak bars', String(profile.exit.rankDeteriorationBars)],
    ['Exit — min score for hold', profile.exit.minHoldScore.toFixed(1)],
    ['Exit — time stop days', profile.exit.timeStopDays == null ? '—' : String(profile.exit.timeStopDays)],
    ['Sizing — method', profile.sizing.method],
    ['Sizing — risk per trade', `${profile.sizing.riskPerTradePct.toFixed(2)}%`],
    ['Constraints — max positions', String(profile.constraints.maxConcurrentPositions)],
    ['Constraints — max stock weight', `${profile.constraints.maxStockWeightPct.toFixed(1)}%`],
    ['Constraints — max sector exposure', `${profile.constraints.maxSectorExposurePct.toFixed(1)}%`],
  ];
  return (
    <dl className="grid grid-cols-1 gap-x-6 gap-y-1.5 text-xs sm:grid-cols-2">
      {rows.map(([label, value]) => (
        <div key={label} className="flex justify-between gap-3 border-b border-slate-800/60 py-1">
          <dt className="text-slate-400">{label}</dt>
          <dd className="text-right font-mono text-slate-200">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function ResultsSummary({ result }: { result: BacktestRunResult }) {
  if (result.status === 'no_data') {
    return (
      <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm text-amber-200">
        No historical ranked_signals found. {result.message}
      </div>
    );
  }
  const pnlPct =
    result.startingEquity > 0
      ? (result.endingEquity - result.startingEquity) / result.startingEquity
      : 0;
  return (
    <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-5">
      <Stat label="Profile" value={result.profile} />
      <Stat label="Trading days" value={String(result.tradingDays)} />
      <Stat label="Trades" value={String(result.tradeCount)} />
      <Stat
        label="Equity end"
        value={fmt(result.endingEquity)}
        sub={fmtPct(pnlPct)}
        positive={pnlPct >= 0}
      />
      <Stat
        label="Exit-reason mix"
        value={Object.keys(result.exitReasonCounts).length.toString() + ' kinds'}
        sub={Object.entries(result.exitReasonCounts)
          .map(([k, v]) => `${k}=${v}`)
          .join('  •  ')}
      />
    </div>
  );
}

function Stat({
  label,
  value,
  sub,
  positive,
}: {
  label: string;
  value: string;
  sub?: string;
  positive?: boolean;
}) {
  const subColor =
    positive == null ? 'text-slate-400' : positive ? 'text-emerald-300' : 'text-rose-300';
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-0.5 text-sm font-semibold text-slate-100">{value}</div>
      {sub ? <div className={`mt-0.5 text-xs ${subColor}`}>{sub}</div> : null}
    </div>
  );
}

export default function BacktestPage() {
  const profilesQuery = useRiskProfiles();
  const profiles = profilesQuery.data?.profiles ?? [];

  const [selectedProfile, setSelectedProfile] = useState<string>('');
  const [fromDate, setFromDate] = useState<string>('');
  const [toDate, setToDate] = useState<string>('');
  const [equity, setEquity] = useState<number>(1_000_000);

  const profile = useMemo(
    () => profiles.find((p) => p.name === (selectedProfile || profiles[0]?.name)) ?? null,
    [profiles, selectedProfile],
  );

  const runMutation = useMutation<BacktestRunResult, Error, void>({
    mutationFn: () =>
      runBacktest({
        profile: profile?.name ?? '',
        fromDate: fromDate || undefined,
        toDate: toDate || undefined,
        equity,
        persist: true,
      }),
  });

  const result = runMutation.data;

  return (
    <PageFrame
      title="Backtest Lab"
      description="Run the shared TradingRuleEngine against historical pipeline_runs. Identical decisions to paper trading."
    >
      <SectionCard title="Risk profile">
        <div className="grid gap-3 md:grid-cols-[minmax(220px,260px)_minmax(0,1fr)]">
          <div className="space-y-2">
            <label className="text-xs text-slate-400">Profile</label>
            <select
              className="w-full rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
              value={profile?.name ?? ''}
              onChange={(e) => setSelectedProfile(e.target.value)}
              disabled={profilesQuery.isPending}
            >
              {profilesQuery.isPending ? (
                <option>Loading…</option>
              ) : profiles.length === 0 ? (
                <option>No profiles found</option>
              ) : (
                profiles.map((p) => (
                  <option key={p.name} value={p.name}>
                    {p.name}
                  </option>
                ))
              )}
            </select>
            {profile ? (
              <p className="break-all text-[10px] text-slate-500">{profile.path}</p>
            ) : null}
          </div>
          <ProfileInspector profile={profile} />
        </div>
      </SectionCard>

      <SectionCard title="Run backtest" description="POST /api/execution/backtest/run">
        <div className="flex flex-wrap items-end gap-3">
          <Field label="From date">
            <input
              type="date"
              value={fromDate}
              onChange={(e) => setFromDate(e.target.value)}
              className="rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
            />
          </Field>
          <Field label="To date">
            <input
              type="date"
              value={toDate}
              onChange={(e) => setToDate(e.target.value)}
              className="rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
            />
          </Field>
          <Field label="Starting equity (₹)">
            <input
              type="number"
              min={0}
              step={50_000}
              value={equity}
              onChange={(e) => setEquity(Number(e.target.value) || 0)}
              className="w-36 rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
            />
          </Field>
          <button
            type="button"
            disabled={!profile || runMutation.isPending}
            onClick={() => runMutation.mutate()}
            className="rounded-md border border-emerald-500/50 bg-emerald-500/10 px-3 py-1.5 text-sm text-emerald-200 hover:bg-emerald-500/20 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {runMutation.isPending ? 'Running…' : 'Run backtest'}
          </button>
          {runMutation.error ? (
            <span className="text-sm text-rose-300">{runMutation.error.message}</span>
          ) : null}
        </div>
      </SectionCard>

      {result ? (
        <SectionCard
          title="Results"
          description={
            result.artifactDir
              ? `Saved to ${result.artifactDir}`
              : 'In-memory result (not persisted)'
          }
        >
          <ResultsSummary result={result} />
          {result.trades.length > 0 ? (
            <div className="mt-4 overflow-x-auto">
              <table className="min-w-full text-xs">
                <thead className="text-left text-slate-400">
                  <tr className="border-b border-slate-800">
                    <th className="py-1.5 pr-3">Symbol</th>
                    <th className="py-1.5 pr-3">Entry</th>
                    <th className="py-1.5 pr-3">Reason in</th>
                    <th className="py-1.5 pr-3">Stop</th>
                    <th className="py-1.5 pr-3">Method</th>
                    <th className="py-1.5 pr-3">Exit</th>
                    <th className="py-1.5 pr-3">Reason out</th>
                    <th className="py-1.5 pr-3">Bars</th>
                    <th className="py-1.5 pr-3">Rank</th>
                    <th className="py-1.5 pr-3 text-right">P&amp;L</th>
                    <th className="py-1.5 pr-3 text-right">P&amp;L %</th>
                  </tr>
                </thead>
                <tbody className="font-mono text-slate-200">
                  {result.trades.map((t, i) => (
                    <tr key={`${t.symbolId}-${t.entryDate}-${i}`} className="border-b border-slate-900/60">
                      <td className="py-1 pr-3">{t.symbolId}</td>
                      <td className="py-1 pr-3 text-slate-400">{t.entryDate}</td>
                      <td className="py-1 pr-3">
                        <ReasonBadge reason={t.entryReason} />
                      </td>
                      <td className="py-1 pr-3 text-right">{fmt(t.stopPrice)}</td>
                      <td className="py-1 pr-3 text-slate-400">{t.stopMethod ?? '—'}</td>
                      <td className="py-1 pr-3 text-slate-400">{t.exitDate ?? '—'}</td>
                      <td className="py-1 pr-3">
                        <ReasonBadge reason={t.exitReason} />
                      </td>
                      <td className="py-1 pr-3 text-right">{t.barsHeld}</td>
                      <td className="py-1 pr-3 text-slate-400">
                        {t.rankAtEntry ?? '—'} → {t.rankAtExit ?? '—'}
                      </td>
                      <td
                        className={`py-1 pr-3 text-right ${
                          (t.pnl ?? 0) >= 0 ? 'text-emerald-300' : 'text-rose-300'
                        }`}
                      >
                        {fmt(t.pnl)}
                      </td>
                      <td
                        className={`py-1 pr-3 text-right ${
                          (t.pnlPct ?? 0) >= 0 ? 'text-emerald-300' : 'text-rose-300'
                        }`}
                      >
                        {fmtPct(t.pnlPct)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </SectionCard>
      ) : null}
    </PageFrame>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs text-slate-400">{label}</span>
      {children}
    </label>
  );
}
