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

import { useEffect, useMemo, useState } from 'react';
import { useMutation } from '@tanstack/react-query';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import { useRiskProfiles } from '@/lib/queries';
import {
  runBacktest,
  runWinnerCapture,
  type BacktestRunResult,
  type RiskProfile,
  type RiskProfileCustomConfig,
  type WinnerCaptureResult,
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

const cloneProfile = (profile: RiskProfile): RiskProfile =>
  JSON.parse(JSON.stringify(profile)) as RiskProfile;

const comparableConfig = (profile: RiskProfile | null) =>
  profile
    ? {
        entry: profile.entry,
        stop: profile.stop,
        exit: profile.exit,
        sizing: profile.sizing,
        constraints: profile.constraints,
      }
    : null;

const customConfigFromProfile = (profile: RiskProfile): RiskProfileCustomConfig => ({
  entry: {
    require_stage_2: profile.entry.requireStage2,
    require_price_above_sma200: profile.entry.requirePriceAboveSma200,
    require_sector_positive: profile.entry.requireSectorPositive,
    min_volume_ratio: profile.entry.minVolumeRatio,
    require_delivery_above_sector_median: profile.entry.requireDeliveryAboveSectorMedian,
  },
  stop: {
    method: profile.stop.method,
    atr_multiple: profile.stop.atrMultiple,
    stop_pct: profile.stop.stopPct,
    hybrid_atr_multiple: profile.stop.hybridAtrMultiple,
  },
  exit: {
    emergency_exit_below_sma200: profile.exit.emergencyExitBelowSma200,
    dma_exit_window: profile.exit.dmaExitWindow,
    dma_whipsaw_buffer_pct: profile.exit.dmaWhipsawBufferPct,
    exit_on_rank_deterioration: profile.exit.exitOnRankDeterioration,
    max_hold_rank: profile.exit.maxHoldRank,
    rank_deterioration_bars: profile.exit.rankDeteriorationBars,
    exit_on_score_deterioration: profile.exit.exitOnScoreDeterioration,
    min_hold_score: profile.exit.minHoldScore,
    score_deterioration_bars: profile.exit.scoreDeteriorationBars,
    time_stop_days: profile.exit.timeStopDays,
  },
  sizing: {
    method: profile.sizing.method,
    risk_per_trade_pct: profile.sizing.riskPerTradePct,
    max_position_pct: profile.sizing.maxPositionPct,
  },
  constraints: {
    max_concurrent_positions: profile.constraints.maxConcurrentPositions,
    max_stock_weight_pct: profile.constraints.maxStockWeightPct,
    max_sector_exposure_pct: profile.constraints.maxSectorExposurePct,
  },
});

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

function ToggleRow({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (value: boolean) => void;
}) {
  return (
    <label className="flex items-center justify-between gap-3 rounded-md border border-slate-800 bg-slate-950/40 px-3 py-2 text-xs text-slate-300">
      <span>{label}</span>
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="h-4 w-4 accent-emerald-500"
      />
    </label>
  );
}

function SliderField({
  label,
  value,
  min,
  max,
  step,
  suffix = '',
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  suffix?: string;
  onChange: (value: number) => void;
}) {
  return (
    <label className="block rounded-md border border-slate-800 bg-slate-950/40 px-3 py-2 text-xs text-slate-300">
      <div className="mb-1 flex items-center justify-between gap-3">
        <span>{label}</span>
        <span className="font-mono text-slate-100">
          {fmt(value)}
          {suffix}
        </span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full accent-emerald-500"
      />
      <input
        type="number"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="mt-1 w-full rounded-md border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100"
      />
    </label>
  );
}

function SelectField({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: Array<{ label: string; value: string }>;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block rounded-md border border-slate-800 bg-slate-950/40 px-3 py-2 text-xs text-slate-300">
      <span className="mb-1 block">{label}</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full rounded-md border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100"
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function ParameterEditor({
  draft,
  onChange,
}: {
  draft: RiskProfile | null;
  onChange: (updater: (profile: RiskProfile) => RiskProfile) => void;
}) {
  if (!draft) {
    return <p className="text-sm text-slate-500">Select a profile to tune its parameters.</p>;
  }

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <div className="space-y-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Entry</div>
        <ToggleRow
          label="Stage 2 required"
          checked={draft.entry.requireStage2}
          onChange={(value) => onChange((p) => ({ ...p, entry: { ...p.entry, requireStage2: value } }))}
        />
        <ToggleRow
          label="Price above SMA200"
          checked={draft.entry.requirePriceAboveSma200}
          onChange={(value) =>
            onChange((p) => ({ ...p, entry: { ...p.entry, requirePriceAboveSma200: value } }))
          }
        />
        <ToggleRow
          label="Sector positive"
          checked={draft.entry.requireSectorPositive}
          onChange={(value) =>
            onChange((p) => ({ ...p, entry: { ...p.entry, requireSectorPositive: value } }))
          }
        />
        <ToggleRow
          label="Delivery above sector median"
          checked={draft.entry.requireDeliveryAboveSectorMedian}
          onChange={(value) =>
            onChange((p) => ({ ...p, entry: { ...p.entry, requireDeliveryAboveSectorMedian: value } }))
          }
        />
        <SliderField
          label="Min volume ratio"
          value={draft.entry.minVolumeRatio}
          min={0}
          max={5}
          step={0.1}
          onChange={(value) => onChange((p) => ({ ...p, entry: { ...p.entry, minVolumeRatio: value } }))}
        />
      </div>

      <div className="space-y-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Stops & sizing</div>
        <SelectField
          label="Stop method"
          value={draft.stop.method}
          options={['atr', 'percent', 'swing_low', 'breakout_candle_low', 'hybrid'].map((value) => ({
            label: value,
            value,
          }))}
          onChange={(value) => onChange((p) => ({ ...p, stop: { ...p.stop, method: value } }))}
        />
        <SelectField
          label="Sizing method"
          value={draft.sizing.method}
          options={['equal_weight', 'atr_risk'].map((value) => ({ label: value, value }))}
          onChange={(value) => onChange((p) => ({ ...p, sizing: { ...p.sizing, method: value } }))}
        />
        <SliderField
          label="ATR multiple"
          value={draft.stop.atrMultiple}
          min={0.5}
          max={5}
          step={0.1}
          onChange={(value) => onChange((p) => ({ ...p, stop: { ...p.stop, atrMultiple: value } }))}
        />
        <SliderField
          label="Stop"
          value={draft.stop.stopPct * 100}
          min={1}
          max={20}
          step={0.5}
          suffix="%"
          onChange={(value) => onChange((p) => ({ ...p, stop: { ...p.stop, stopPct: value / 100 } }))}
        />
        <SliderField
          label="Hybrid ATR multiple"
          value={draft.stop.hybridAtrMultiple}
          min={0.5}
          max={6}
          step={0.1}
          onChange={(value) =>
            onChange((p) => ({ ...p, stop: { ...p.stop, hybridAtrMultiple: value } }))
          }
        />
        <SliderField
          label="Risk per trade"
          value={draft.sizing.riskPerTradePct}
          min={0.1}
          max={5}
          step={0.1}
          suffix="%"
          onChange={(value) =>
            onChange((p) => ({ ...p, sizing: { ...p.sizing, riskPerTradePct: value } }))
          }
        />
        <SliderField
          label="Max position size"
          value={draft.sizing.maxPositionPct}
          min={1}
          max={30}
          step={0.5}
          suffix="%"
          onChange={(value) =>
            onChange((p) => ({ ...p, sizing: { ...p.sizing, maxPositionPct: value } }))
          }
        />
      </div>

      <div className="space-y-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Exits</div>
        <ToggleRow
          label="200DMA emergency exit"
          checked={draft.exit.emergencyExitBelowSma200}
          onChange={(value) =>
            onChange((p) => ({ ...p, exit: { ...p.exit, emergencyExitBelowSma200: value } }))
          }
        />
        <ToggleRow
          label="Rank deterioration exit"
          checked={draft.exit.exitOnRankDeterioration}
          onChange={(value) =>
            onChange((p) => ({ ...p, exit: { ...p.exit, exitOnRankDeterioration: value } }))
          }
        />
        <ToggleRow
          label="Score deterioration exit"
          checked={draft.exit.exitOnScoreDeterioration}
          onChange={(value) =>
            onChange((p) => ({ ...p, exit: { ...p.exit, exitOnScoreDeterioration: value } }))
          }
        />
        <SelectField
          label="DMA exit window"
          value={draft.exit.dmaExitWindow == null ? 'none' : String(draft.exit.dmaExitWindow)}
          options={[
            { label: 'None', value: 'none' },
            { label: '11 DMA', value: '11' },
            { label: '20 DMA', value: '20' },
            { label: '50 DMA', value: '50' },
          ]}
          onChange={(value) =>
            onChange((p) => ({
              ...p,
              exit: { ...p.exit, dmaExitWindow: value === 'none' ? null : Number(value) },
            }))
          }
        />
        <SliderField
          label="DMA whipsaw buffer"
          value={draft.exit.dmaWhipsawBufferPct}
          min={0}
          max={5}
          step={0.1}
          suffix="%"
          onChange={(value) =>
            onChange((p) => ({ ...p, exit: { ...p.exit, dmaWhipsawBufferPct: value } }))
          }
        />
        <SliderField
          label="Max hold rank"
          value={draft.exit.maxHoldRank}
          min={1}
          max={200}
          step={1}
          onChange={(value) => onChange((p) => ({ ...p, exit: { ...p.exit, maxHoldRank: value } }))}
        />
        <SliderField
          label="Rank streak bars"
          value={draft.exit.rankDeteriorationBars}
          min={1}
          max={20}
          step={1}
          onChange={(value) =>
            onChange((p) => ({ ...p, exit: { ...p.exit, rankDeteriorationBars: value } }))
          }
        />
        <SliderField
          label="Min hold score"
          value={draft.exit.minHoldScore}
          min={0}
          max={100}
          step={1}
          onChange={(value) => onChange((p) => ({ ...p, exit: { ...p.exit, minHoldScore: value } }))}
        />
        <SliderField
          label="Score streak bars"
          value={draft.exit.scoreDeteriorationBars}
          min={1}
          max={20}
          step={1}
          onChange={(value) =>
            onChange((p) => ({ ...p, exit: { ...p.exit, scoreDeteriorationBars: value } }))
          }
        />
        <SliderField
          label="Time stop days"
          value={draft.exit.timeStopDays ?? 0}
          min={0}
          max={365}
          step={1}
          onChange={(value) =>
            onChange((p) => ({ ...p, exit: { ...p.exit, timeStopDays: value <= 0 ? null : value } }))
          }
        />
      </div>

      <div className="space-y-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Constraints</div>
        <SliderField
          label="Max positions"
          value={draft.constraints.maxConcurrentPositions}
          min={1}
          max={30}
          step={1}
          onChange={(value) =>
            onChange((p) => ({ ...p, constraints: { ...p.constraints, maxConcurrentPositions: value } }))
          }
        />
        <SliderField
          label="Max stock weight"
          value={draft.constraints.maxStockWeightPct}
          min={1}
          max={50}
          step={0.5}
          suffix="%"
          onChange={(value) =>
            onChange((p) => ({ ...p, constraints: { ...p.constraints, maxStockWeightPct: value } }))
          }
        />
        <SliderField
          label="Max sector exposure"
          value={draft.constraints.maxSectorExposurePct}
          min={5}
          max={100}
          step={1}
          suffix="%"
          onChange={(value) =>
            onChange((p) => ({ ...p, constraints: { ...p.constraints, maxSectorExposurePct: value } }))
          }
        />
      </div>
    </div>
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
      <Stat label="Data source" value={result.dataSource === 'research_dynamic' ? 'Research dynamic' : 'Pipeline replay'} />
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

function ResultDiagnostics({ result }: { result: BacktestRunResult }) {
  const sync = result.sync;
  const dq = result.dataQuality;
  const metadata = result.runMetadata;
  if (!sync && !dq && !metadata) return null;

  return (
    <div className="mt-4 grid gap-3 text-xs lg:grid-cols-3">
      {sync ? (
        <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
          <div className="text-xs uppercase tracking-wide text-slate-500">Research sync</div>
          <dl className="mt-2 space-y-1 text-slate-300">
            <InfoRow label="Status" value={sync.status || '—'} />
            <InfoRow label="Source dates" value={`${sync.sourceFromDate ?? '—'} → ${sync.sourceToDate ?? '—'}`} />
            <InfoRow label="Rows copied" value={fmt(sync.insertedRows ?? sync.rowsToCopy ?? sync.sourceRows)} />
            <InfoRow label="Research rows" value={fmt(sync.totalTargetRows)} />
            <InfoRow label="Masterdata" value={`${sync.masterdata?.status ?? '—'} (${fmt(sync.masterdata?.tableCount)} tables)`} />
          </dl>
        </div>
      ) : null}
      {dq ? (
        <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
          <div className="text-xs uppercase tracking-wide text-slate-500">Data quality</div>
          <dl className="mt-2 space-y-1 text-slate-300">
            <InfoRow label="Status" value={dq.status || '—'} />
            <InfoRow label="Rows / symbols" value={`${fmt(dq.rowCount)} / ${fmt(dq.symbolCount)}`} />
            <InfoRow label="Date range" value={`${dq.minDate ?? '—'} → ${dq.maxDate ?? '—'}`} />
            <InfoRow label="Duplicates" value={`${fmt(dq.duplicateTimestampCount)} timestamp, ${fmt(dq.duplicateDailyCount)} daily`} />
            <InfoRow label="SMA200 gaps" value={fmt(dq.insufficientSma200SymbolCount)} />
          </dl>
          {dq.warnings && dq.warnings.length > 0 ? (
            <div className="mt-2 flex flex-wrap gap-1">
              {dq.warnings.map((warning) => (
                <span key={warning} className="rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-amber-200">
                  {warning}
                </span>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
      {metadata ? (
        <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
          <div className="text-xs uppercase tracking-wide text-slate-500">Run metadata</div>
          <dl className="mt-2 space-y-1 text-slate-300">
            <InfoRow label="Rank method" value={metadata.rankingMethodVersion ?? '—'} />
            <InfoRow label="Git" value={metadata.gitCommit ? metadata.gitCommit.slice(0, 8) : '—'} />
            <InfoRow label="Generated" value={metadata.generatedAt ?? '—'} />
          </dl>
        </div>
      ) : null}
    </div>
  );
}

function WinnerCaptureResults({ result }: { result: WinnerCaptureResult }) {
  if (result.status === 'no_data') {
    return (
      <div className="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm text-amber-200">
        No yearly winner data found. {result.message}
      </div>
    );
  }
  return (
    <div className="mt-4 space-y-4">
      <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-5">
        <Stat label="Capture rate" value={fmtPct(result.summary.captureRate)} />
        <Stat label="Captured" value={`${result.summary.capturedCount}/${result.summary.winnerCount}`} />
        <Stat label="Missed" value={String(result.summary.missedCount)} />
        <Stat label="Median days" value={fmt(result.summary.medianDaysToCapture)} />
        <Stat label="Median first rank" value={fmt(result.summary.medianFirstCaptureRank)} />
        <Stat
          label="Captured avg return"
          value={fmtPct(result.summary.averageYearlyReturnCaptured)}
          positive={(result.summary.averageYearlyReturnCaptured ?? 0) >= 0}
        />
        <Stat
          label="Missed avg return"
          value={fmtPct(result.summary.averageYearlyReturnMissed)}
          positive={(result.summary.averageYearlyReturnMissed ?? 0) >= 0}
        />
        <Stat label="Window" value={`${result.startDate} → ${result.endDate}`} />
        <Stat label="Rank cutoff" value={`Top ${result.rankCutoff}`} />
        <Stat label="Saved" value={result.artifactDir ? 'Yes' : 'No'} sub={result.artifactDir ?? undefined} />
      </div>

      {result.dataQuality || result.sync || result.runMetadata ? (
        <div className="grid gap-3 text-xs lg:grid-cols-3">
          {result.sync ? (
            <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
              <div className="text-xs uppercase tracking-wide text-slate-500">Research sync</div>
              <dl className="mt-2 space-y-1 text-slate-300">
                <InfoRow label="Status" value={result.sync.status || '—'} />
                <InfoRow label="Source dates" value={`${result.sync.sourceFromDate ?? '—'} → ${result.sync.sourceToDate ?? '—'}`} />
                <InfoRow label="Rows copied" value={fmt(result.sync.insertedRows ?? result.sync.rowsToCopy ?? result.sync.sourceRows)} />
              </dl>
            </div>
          ) : null}
          {result.dataQuality ? (
            <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
              <div className="text-xs uppercase tracking-wide text-slate-500">Data quality</div>
              <dl className="mt-2 space-y-1 text-slate-300">
                <InfoRow label="Status" value={result.dataQuality.status || '—'} />
                <InfoRow label="Rows / symbols" value={`${fmt(result.dataQuality.rowCount)} / ${fmt(result.dataQuality.symbolCount)}`} />
                <InfoRow label="Date range" value={`${result.dataQuality.minDate ?? '—'} → ${result.dataQuality.maxDate ?? '—'}`} />
              </dl>
            </div>
          ) : null}
          {result.runMetadata ? (
            <div className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
              <div className="text-xs uppercase tracking-wide text-slate-500">Run metadata</div>
              <dl className="mt-2 space-y-1 text-slate-300">
                <InfoRow label="Rank method" value={result.runMetadata.rankingMethodVersion ?? '—'} />
                <InfoRow label="Generated" value={result.runMetadata.generatedAt ?? '—'} />
              </dl>
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead className="text-left text-slate-400">
            <tr className="border-b border-slate-800">
              <th className="py-1.5 pr-3">#</th>
              <th className="py-1.5 pr-3">Symbol</th>
              <th className="py-1.5 pr-3">Status</th>
              <th className="py-1.5 pr-3 text-right">Year return</th>
              <th className="py-1.5 pr-3">First capture</th>
              <th className="py-1.5 pr-3 text-right">First rank</th>
              <th className="py-1.5 pr-3 text-right">Best rank</th>
              <th className="py-1.5 pr-3 text-right">Days</th>
              <th className="py-1.5 pr-3 text-right">At capture</th>
              <th className="py-1.5 pr-3 text-right">Remaining</th>
            </tr>
          </thead>
          <tbody className="font-mono text-slate-200">
            {result.winners.map((row) => (
              <tr key={`${row.rankInYear}-${row.symbolId}`} className="border-b border-slate-900/60">
                <td className="py-1 pr-3 text-slate-500">{row.rankInYear}</td>
                <td className="py-1 pr-3">{row.symbolId}</td>
                <td className="py-1 pr-3">
                  <span
                    className={`inline-flex rounded-full border px-2 py-0.5 text-xs ${
                      row.captured
                        ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200'
                        : 'border-rose-500/40 bg-rose-500/10 text-rose-200'
                    }`}
                  >
                    {row.captured ? 'captured' : 'missed'}
                  </span>
                </td>
                <td className="py-1 pr-3 text-right text-emerald-300">{fmtPct(row.yearlyReturn)}</td>
                <td className="py-1 pr-3 text-slate-400">{row.firstCaptureDate ?? '—'}</td>
                <td className="py-1 pr-3 text-right">{fmt(row.firstCaptureRank)}</td>
                <td className="py-1 pr-3 text-right">{fmt(row.bestRank)}</td>
                <td className="py-1 pr-3 text-right">{fmt(row.daysToCapture)}</td>
                <td className="py-1 pr-3 text-right">{fmtPct(row.returnAtCapture)}</td>
                <td className="py-1 pr-3 text-right">{fmtPct(row.remainingReturnAfterCapture)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-3 border-b border-slate-800/60 py-1">
      <dt className="text-slate-500">{label}</dt>
      <dd className="text-right font-mono text-slate-200">{value}</dd>
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
  const [dataSource, setDataSource] = useState<string>('pipeline_replay');
  const [draftProfile, setDraftProfile] = useState<RiskProfile | null>(null);
  const [winnerYear, setWinnerYear] = useState<number>(new Date().getFullYear() - 1);
  const [winnerTopGainers, setWinnerTopGainers] = useState<number>(50);
  const [winnerRankCutoff, setWinnerRankCutoff] = useState<number>(50);

  const profile = useMemo(
    () => profiles.find((p) => p.name === (selectedProfile || profiles[0]?.name)) ?? null,
    [profiles, selectedProfile],
  );
  const isCustom = useMemo(
    () => JSON.stringify(comparableConfig(profile)) !== JSON.stringify(comparableConfig(draftProfile)),
    [profile, draftProfile],
  );

  useEffect(() => {
    setDraftProfile(profile ? cloneProfile(profile) : null);
  }, [profile]);

  const runMutation = useMutation<BacktestRunResult, Error, void>({
    mutationFn: () =>
      runBacktest({
        profile: profile?.name ?? '',
        dataSource,
        fromDate: fromDate || undefined,
        toDate: toDate || undefined,
        equity,
        persist: true,
        customConfig: isCustom && draftProfile ? customConfigFromProfile(draftProfile) : undefined,
      }),
  });

  const winnerMutation = useMutation<WinnerCaptureResult, Error, void>({
    mutationFn: () =>
      runWinnerCapture({
        year: winnerYear,
        exchange: 'NSE',
        topGainers: winnerTopGainers,
        rankCutoff: winnerRankCutoff,
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

      <SectionCard
        title="Custom parameters"
        description={isCustom ? 'Custom overrides will be sent with this backtest.' : 'Using the selected preset as-is.'}
      >
        <div className="mb-3 flex items-center justify-between gap-3">
          <div className="text-xs text-slate-400">
            Baseline: <span className="font-mono text-slate-200">{profile?.name ?? '—'}</span>
          </div>
          <button
            type="button"
            disabled={!profile}
            onClick={() => setDraftProfile(profile ? cloneProfile(profile) : null)}
            className="rounded-md border border-slate-700 px-3 py-1.5 text-xs text-slate-200 hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Reset to selected profile
          </button>
        </div>
        <ParameterEditor
          draft={draftProfile}
          onChange={(updater) => setDraftProfile((current) => (current ? updater(current) : current))}
        />
      </SectionCard>

      <SectionCard
        title="Winner Capture"
        description="Find the year's top gainers and check whether research dynamic ranking captured them."
      >
        <div className="flex flex-wrap items-end gap-3">
          <Field label="Calendar year">
            <input
              type="number"
              min={1990}
              max={new Date().getFullYear() - 1}
              value={winnerYear}
              onChange={(e) => setWinnerYear(Number(e.target.value) || new Date().getFullYear() - 1)}
              className="w-28 rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
            />
          </Field>
          <Field label="Top gainers">
            <input
              type="number"
              min={1}
              max={500}
              value={winnerTopGainers}
              onChange={(e) => setWinnerTopGainers(Number(e.target.value) || 50)}
              className="w-28 rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
            />
          </Field>
          <Field label="Capture cutoff">
            <input
              type="number"
              min={1}
              max={500}
              value={winnerRankCutoff}
              onChange={(e) => setWinnerRankCutoff(Number(e.target.value) || 50)}
              className="w-28 rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
            />
          </Field>
          <button
            type="button"
            disabled={winnerMutation.isPending}
            onClick={() => winnerMutation.mutate()}
            className="rounded-md border border-sky-500/50 bg-sky-500/10 px-3 py-1.5 text-sm text-sky-200 hover:bg-sky-500/20 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {winnerMutation.isPending ? 'Running…' : 'Run winner capture'}
          </button>
          {winnerMutation.error ? (
            <span className="text-sm text-rose-300">{winnerMutation.error.message}</span>
          ) : null}
        </div>
        <div className="mt-2 text-xs text-slate-500">
          Uses completed calendar years only and syncs operational data into research before analysis.
        </div>
        {winnerMutation.data ? <WinnerCaptureResults result={winnerMutation.data} /> : null}
      </SectionCard>

      <SectionCard title="Run backtest" description="POST /api/execution/backtest/run">
        <div className="flex flex-wrap items-end gap-3">
          <Field label="Data source">
            <select
              value={dataSource}
              onChange={(e) => setDataSource(e.target.value)}
              className="rounded-md border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm"
            >
              <option value="pipeline_replay">Pipeline replay</option>
              <option value="research_dynamic">Research dynamic</option>
            </select>
          </Field>
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
        <div className="mt-2 text-xs text-slate-500">
          {dataSource === 'research_dynamic'
            ? 'Computes indicators and ranks from data/research/research_ohlcv.duckdb.'
            : 'Replays saved ranked_signals.csv from historical pipeline runs.'}
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
          <ResultDiagnostics result={result} />
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
