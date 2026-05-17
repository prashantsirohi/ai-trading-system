/**
 * Optimization audit view (Wave 5b).
 *
 * Three-tier layout (matches RunsPage shape):
 *
 *   1. Header KPIs — # runs, latest champion, leaderboard top.
 *   2. Tabs:
 *        - Runs : split pane (runs table | run detail w/ trials + report)
 *        - Leaderboard : best champion per recipe across recipes
 *
 * Data sources (Wave 2 readmodel + Wave 5b POST /promote):
 *   - /api/execution/optimization/runs
 *   - /api/execution/optimization/runs/{id}
 *   - /api/execution/optimization/runs/{id}/trials
 *   - /api/execution/optimization/leaderboard
 *   - /api/execution/optimization/runs/{id}/report
 *   - POST /api/execution/optimization/runs/{id}/promote
 */
import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import StatusBadge from '@/components/common/StatusBadge';
import { cn } from '@/lib/utils/cn';
import {
  useOptimizationRuns,
  useOptimizationRunDetail,
  useOptimizationTrials,
  useOptimizationLeaderboard,
  useOptimizationReport,
  queryKeys,
} from '@/lib/queries';
import {
  promoteOptimizationRun,
  LIFECYCLE_STATUSES,
  type LifecycleStatus,
  type OptimizationRunListItem,
  type OptimizationRunDetail,
  type OptimizationTrial,
  type LeaderboardRow,
} from '@/lib/api/optimization';

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function fmtNum(v: number | null | undefined, digits = 3): string {
  if (v === null || v === undefined || Number.isNaN(v)) return '—';
  return v.toFixed(digits);
}
function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return '—';
  // Match reports.py heuristic: small values (<= 1) read as fractional %.
  return Math.abs(v) <= 1 ? `${(v * 100).toFixed(2)}%` : `${v.toFixed(2)}%`;
}
function fmtDateTime(v: string | null): string {
  if (!v) return '—';
  return v.replace('T', ' ').slice(0, 19);
}

const LEADERBOARD_METRICS = ['sharpe', 'fitness', 'cagr', 'win_rate', 'total_return_pct', 'trade_count'] as const;

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function OptimizationPage() {
  const [tab, setTab] = useState<'runs' | 'leaderboard'>('runs');
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [leaderMetric, setLeaderMetric] = useState<(typeof LEADERBOARD_METRICS)[number]>('sharpe');

  const runsQuery = useOptimizationRuns({ limit: 50 });
  const runs = useMemo(() => runsQuery.data?.runs ?? [], [runsQuery.data]);

  // Auto-select the most recent run so the page is never blank on first visit.
  useEffect(() => {
    if (selectedRunId === null && runs.length > 0) {
      setSelectedRunId(runs[0].optimization_run_id);
    }
  }, [runs, selectedRunId]);

  // Poll the detail every 5s while the run is still running — gives the
  // operator a live trial counter without a manual refresh.
  const selectedRun = runs.find((r) => r.optimization_run_id === selectedRunId);
  const isLiveRun = selectedRun?.status === 'running';
  const detailQuery = useOptimizationRunDetail(selectedRunId, {
    refetchInterval: isLiveRun ? 5_000 : undefined,
  });
  const trialsQuery = useOptimizationTrials(
    selectedRunId,
    { sort: 'fitness', limit: 100 },
    { refetchInterval: isLiveRun ? 5_000 : undefined },
  );
  const reportQuery = useOptimizationReport(selectedRunId);
  const leaderboardQuery = useOptimizationLeaderboard({ metric: leaderMetric, top: 20 });

  if (runsQuery.isLoading) {
    return (
      <PageFrame title="Optimization" description="Strategy rule-pack tuning via Optuna.">
        <CardSkeleton />
      </PageFrame>
    );
  }
  if (runsQuery.error) {
    return (
      <PageFrame title="Optimization" description="Strategy rule-pack tuning via Optuna.">
        <ErrorStateView error={runsQuery.error} onRetry={() => runsQuery.refetch()} />
      </PageFrame>
    );
  }
  if (!runsQuery.data?.available) {
    return (
      <PageFrame title="Optimization" description="Strategy rule-pack tuning via Optuna.">
        <EmptyState
          message={
            <>
              <strong>No optimization data yet.</strong> Run{' '}
              <code className="rounded bg-slate-900 px-1 py-0.5 text-xs">ai-trading-optimize run --recipe &lt;name&gt;</code>{' '}
              to populate the control plane, then refresh.
            </>
          }
        />
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Optimization"
      description="Optuna study runs, per-trial inspection, leaderboard, and one-click promote."
    >
      <KpiStrip runs={runs} />

      <div className="flex items-center gap-2 border-b border-slate-800 pb-1">
        <TabButton active={tab === 'runs'} onClick={() => setTab('runs')}>
          Runs ({runs.length})
        </TabButton>
        <TabButton active={tab === 'leaderboard'} onClick={() => setTab('leaderboard')}>
          Leaderboard
        </TabButton>
      </div>

      {tab === 'runs' ? (
        <div className="grid gap-4 xl:grid-cols-[420px_minmax(0,1fr)]">
          <RunsTable
            runs={runs}
            selectedRunId={selectedRunId}
            onSelect={setSelectedRunId}
          />
          <RunDetailPanel
            detail={detailQuery.data ?? null}
            isLoading={detailQuery.isLoading}
            trials={trialsQuery.data?.trials ?? []}
            reportContent={reportQuery.data?.content ?? null}
            onSelectRun={setSelectedRunId}
          />
        </div>
      ) : (
        <LeaderboardPanel
          rows={leaderboardQuery.data?.rows ?? []}
          metric={leaderMetric}
          onMetricChange={setLeaderMetric}
          onRunSelect={(runId) => {
            setTab('runs');
            setSelectedRunId(runId);
          }}
          isLoading={leaderboardQuery.isLoading}
        />
      )}
    </PageFrame>
  );
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        'rounded-t-lg border-b-2 px-4 py-2 text-sm font-medium transition',
        active
          ? 'border-blue-500 text-white'
          : 'border-transparent text-slate-400 hover:text-slate-200',
      )}
    >
      {children}
    </button>
  );
}

function KpiStrip({ runs }: { runs: OptimizationRunListItem[] }) {
  const completed = runs.filter((r) => r.status === 'completed').length;
  const running = runs.filter((r) => r.status === 'running').length;
  const failed = runs.filter((r) => r.status === 'failed').length;
  const latestChampion = runs.find((r) => r.status === 'completed' && r.champion_rule_pack_id);
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
      <Kpi label="Runs (total)" value={String(runs.length)} />
      <Kpi label="Completed" value={String(completed)} tone="ok" />
      <Kpi label="Running" value={String(running)} tone={running > 0 ? 'warn' : 'muted'} />
      <Kpi label="Failed" value={String(failed)} tone={failed > 0 ? 'err' : 'muted'} />
      {latestChampion ? (
        <div className="col-span-2 rounded-xl border border-slate-800 bg-slate-950/40 p-3 md:col-span-4">
          <div className="text-[11px] uppercase tracking-wider text-slate-500">Latest champion</div>
          <div className="mt-1 truncate text-sm text-slate-200">
            <code className="rounded bg-slate-900 px-1 py-0.5 text-xs">{latestChampion.champion_rule_pack_id}</code>{' '}
            from <strong>{latestChampion.recipe_name}</strong>{' '}
            <span className="text-slate-500">({fmtDateTime(latestChampion.completed_at)})</span>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function Kpi({
  label,
  value,
  tone = 'muted',
}: {
  label: string;
  value: string;
  tone?: 'ok' | 'warn' | 'err' | 'muted';
}) {
  const toneClass = {
    ok: 'text-emerald-400',
    warn: 'text-amber-400',
    err: 'text-rose-400',
    muted: 'text-slate-200',
  }[tone];
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
      <div className="text-[11px] uppercase tracking-wider text-slate-500">{label}</div>
      <div className={cn('mt-1 text-2xl font-semibold tabular-nums', toneClass)}>{value}</div>
    </div>
  );
}

function RunsTable({
  runs,
  selectedRunId,
  onSelect,
}: {
  runs: OptimizationRunListItem[];
  selectedRunId: string | null;
  onSelect: (id: string) => void;
}) {
  return (
    <SectionCard title="Runs (newest first)">
      <div className="max-h-[60vh] overflow-y-auto">
        <table className="w-full text-left text-xs">
          <thead className="sticky top-0 z-10 bg-slate-950 text-[10px] uppercase tracking-wider text-slate-500">
            <tr>
              <th className="px-2 py-1.5">Recipe</th>
              <th className="px-2 py-1.5">Status</th>
              <th className="px-2 py-1.5 text-right">Trials</th>
              <th className="px-2 py-1.5">Started</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr
                key={r.optimization_run_id}
                onClick={() => onSelect(r.optimization_run_id)}
                className={cn(
                  'cursor-pointer border-t border-slate-900 transition hover:bg-slate-900/60',
                  r.optimization_run_id === selectedRunId && 'bg-blue-500/10',
                )}
              >
                <td className="px-2 py-1.5">
                  <div className="truncate font-medium text-slate-200">{r.recipe_name}</div>
                  <div className="truncate text-[10px] text-slate-500">{r.optimization_run_id.slice(0, 12)}…</div>
                </td>
                <td className="px-2 py-1.5">
                  <StatusBadge status={r.status} />
                </td>
                <td className="px-2 py-1.5 text-right tabular-nums text-slate-300">
                  {r.trial_count}/{r.max_trials}
                </td>
                <td className="px-2 py-1.5 text-[11px] text-slate-400">{fmtDateTime(r.started_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </SectionCard>
  );
}

// StatusBadge auto-maps "completed"/"running"/"failed" → tones via STATUS_TO_TONE
// (see src/components/common/StatusBadge.tsx). We just pass the raw status string.

function RunDetailPanel({
  detail,
  isLoading,
  trials,
  reportContent,
  onSelectRun,
}: {
  detail: OptimizationRunDetail | null;
  isLoading: boolean;
  trials: OptimizationTrial[];
  reportContent: string | null;
  onSelectRun: (id: string) => void;
}) {
  if (isLoading) return <CardSkeleton />;
  if (!detail) {
    return (
      <SectionCard title="Run detail">
        <EmptyState message="Pick a run on the left to inspect baseline / champion / trials." />
      </SectionCard>
    );
  }
  if (!detail.available) {
    return (
      <SectionCard title="Run detail">
        <EmptyState message="Run not found — it may have been pruned from the control plane." />
      </SectionCard>
    );
  }

  return (
    <div className="space-y-4">
      <SectionCard title="Run header">
        <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs md:grid-cols-4">
          <Field label="Recipe">{detail.recipe_name}</Field>
          <Field label="Strategy">{detail.strategy_id}</Field>
          <Field label="Status"><StatusBadge status={detail.status} /></Field>
          <Field label="Trials">{detail.trial_count} / {detail.max_trials}</Field>
          <Field label="Window">{detail.from_date} → {detail.to_date}</Field>
          <Field label="Started">{fmtDateTime(detail.started_at)}</Field>
          <Field label="Completed">{fmtDateTime(detail.completed_at)}</Field>
          <Field label="Seed">{detail.seed}</Field>
        </dl>
        {detail.error ? (
          <div className="mt-3 rounded-lg border border-rose-500/40 bg-rose-500/10 p-2 text-xs text-rose-200">
            <strong>error:</strong> {detail.error}
          </div>
        ) : null}
      </SectionCard>

      <ChampionPanel detail={detail} onPromoted={() => onSelectRun(detail.optimization_run_id)} />

      <FoldsTable title="Baseline folds" rows={detail.baseline_folds} />
      <FoldsTable title="Champion folds" rows={detail.champion_folds} />

      <TrialsTable trials={trials} />

      {reportContent ? (
        <SectionCard title="Report">
          <pre className="max-h-[60vh] overflow-auto rounded-lg border border-slate-800 bg-slate-950 p-3 text-[11px] leading-5 text-slate-300">
            {reportContent}
          </pre>
        </SectionCard>
      ) : (
        <SectionCard title="Report">
          <EmptyState
            message={
              <>
                No auto-written report on disk. The runner writes one to{' '}
                <code className="rounded bg-slate-900 px-1 py-0.5 text-[11px]">
                  reports/optimization/&lt;recipe&gt;/&lt;run_id&gt;.md
                </code>{' '}
                on successful completion unless <code>--no-report</code> was passed.
              </>
            }
          />
        </SectionCard>
      )}
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-[10px] uppercase tracking-wider text-slate-500">{label}</dt>
      <dd className="mt-0.5 text-sm text-slate-200">{children}</dd>
    </div>
  );
}

function FoldsTable({ title, rows }: { title: string; rows: OptimizationRunDetail['baseline_folds'] }) {
  if (rows.length === 0) {
    return (
      <SectionCard title={title}>
        <EmptyState message="No fold rows recorded." />
      </SectionCard>
    );
  }
  return (
    <SectionCard title={title}>
      <table className="w-full text-left text-xs">
        <thead className="text-[10px] uppercase tracking-wider text-slate-500">
          <tr>
            <th className="px-2 py-1 text-right">Fold</th>
            <th className="px-2 py-1 text-right">Fitness</th>
            <th className="px-2 py-1 text-right">CAGR</th>
            <th className="px-2 py-1 text-right">Sharpe</th>
            <th className="px-2 py-1 text-right">MDD</th>
            <th className="px-2 py-1 text-right">Win%</th>
            <th className="px-2 py-1 text-right">Trades</th>
            <th className="px-2 py-1 text-right">Return</th>
            <th className="px-2 py-1 text-right">Benchmark</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.fold_index} className="border-t border-slate-900">
              <td className="px-2 py-1 text-right tabular-nums">{r.fold_index}</td>
              <td className="px-2 py-1 text-right tabular-nums">{fmtNum(r.fitness, 4)}</td>
              <td className="px-2 py-1 text-right tabular-nums">{fmtPct(r.cagr)}</td>
              <td className="px-2 py-1 text-right tabular-nums">{fmtNum(r.sharpe, 2)}</td>
              <td className="px-2 py-1 text-right tabular-nums">{fmtPct(r.max_drawdown_pct)}</td>
              <td className="px-2 py-1 text-right tabular-nums">{fmtPct(r.win_rate)}</td>
              <td className="px-2 py-1 text-right tabular-nums">{r.trade_count ?? '—'}</td>
              <td className="px-2 py-1 text-right tabular-nums">{fmtPct(r.total_return_pct)}</td>
              <td className="px-2 py-1 text-right tabular-nums">{fmtPct(r.benchmark_return_pct)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </SectionCard>
  );
}

function TrialsTable({ trials }: { trials: OptimizationTrial[] }) {
  if (trials.length === 0) {
    return (
      <SectionCard title="Trials">
        <EmptyState message="No trials persisted for this run yet." />
      </SectionCard>
    );
  }
  return (
    <SectionCard title="Top trials (by fitness)">
      <div className="max-h-[40vh] overflow-y-auto">
        <table className="w-full text-left text-xs">
          <thead className="sticky top-0 z-10 bg-slate-950 text-[10px] uppercase tracking-wider text-slate-500">
            <tr>
              <th className="px-2 py-1 text-right">#</th>
              <th className="px-2 py-1 text-right">Fitness</th>
              <th className="px-2 py-1 text-right">CAGR</th>
              <th className="px-2 py-1 text-right">Sharpe</th>
              <th className="px-2 py-1 text-right">MDD</th>
              <th className="px-2 py-1 text-right">Trades</th>
              <th className="px-2 py-1">Accepted</th>
              <th className="px-2 py-1">Rejection</th>
            </tr>
          </thead>
          <tbody>
            {trials.map((t) => (
              <tr key={t.iteration} className="border-t border-slate-900">
                <td className="px-2 py-1 text-right tabular-nums">{t.iteration}</td>
                <td className="px-2 py-1 text-right tabular-nums">{fmtNum(t.fitness, 4)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{fmtPct(t.cagr)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{fmtNum(t.sharpe, 2)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{fmtPct(t.max_drawdown_pct)}</td>
                <td className="px-2 py-1 text-right tabular-nums">{t.trade_count ?? '—'}</td>
                <td className="px-2 py-1">
                  {t.accepted == null ? '—' : t.accepted ? '✓' : '✗'}
                </td>
                <td className="truncate px-2 py-1 text-slate-400">{t.rejection_reason ?? ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </SectionCard>
  );
}

// ---------------------------------------------------------------------------
// Champion + promote
// ---------------------------------------------------------------------------

function ChampionPanel({
  detail,
  onPromoted,
}: {
  detail: OptimizationRunDetail;
  onPromoted: () => void;
}) {
  const qc = useQueryClient();
  const [target, setTarget] = useState<LifecycleStatus>('shadow');
  const [feedback, setFeedback] = useState<string | null>(null);

  const promote = useMutation({
    mutationFn: () => promoteOptimizationRun(detail.optimization_run_id, target),
    onSuccess: (data) => {
      setFeedback(`✓ promoted ${data.rule_pack_id.slice(0, 12)}… ${data.previous_status} → ${data.new_status}`);
      qc.invalidateQueries({ queryKey: queryKeys.optimizationRunDetail(detail.optimization_run_id) });
      qc.invalidateQueries({ queryKey: ['execution', 'optimization-leaderboard'] });
      onPromoted();
    },
    onError: (err: Error) => setFeedback(`✗ ${err.message}`),
  });

  if (!detail.champion_rule_pack_id) {
    return (
      <SectionCard title="Champion">
        <EmptyState message="This run produced no accepted champion." />
      </SectionCard>
    );
  }

  // Allowed next statuses are those strictly past the current one on the ladder.
  const currentIdx = LIFECYCLE_STATUSES.indexOf(
    (detail.champion_lifecycle_status as LifecycleStatus) ?? 'backtested',
  );
  const allowedTargets = LIFECYCLE_STATUSES.slice(currentIdx + 1);

  return (
    <SectionCard title="Champion">
      <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs md:grid-cols-3">
        <Field label="Rule pack">
          <code className="rounded bg-slate-900 px-1 py-0.5 text-[11px]">{detail.champion_rule_pack_id}</code>
        </Field>
        <Field label="Lifecycle">
          <StatusBadge status={detail.champion_lifecycle_status ?? 'unknown'} tone="good" />
        </Field>
        <Field label="Champion folds">{detail.champion_folds.length}</Field>
      </dl>

      {allowedTargets.length > 0 ? (
        <div className="mt-4 flex flex-wrap items-center gap-2">
          <span className="text-[11px] uppercase tracking-wider text-slate-500">Promote to:</span>
          <select
            value={target}
            onChange={(e) => setTarget(e.target.value as LifecycleStatus)}
            className="rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-slate-200"
          >
            {allowedTargets.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <button
            type="button"
            onClick={() => promote.mutate()}
            disabled={promote.isPending}
            className={cn(
              'rounded-md border px-3 py-1 text-xs font-medium transition',
              promote.isPending
                ? 'border-slate-700 bg-slate-800 text-slate-400'
                : 'border-blue-500/50 bg-blue-500/15 text-white hover:bg-blue-500/25',
            )}
          >
            {promote.isPending ? 'Promoting…' : 'Promote'}
          </button>
          {feedback ? (
            <span
              className={cn(
                'text-xs',
                feedback.startsWith('✓') ? 'text-emerald-400' : 'text-rose-400',
              )}
            >
              {feedback}
            </span>
          ) : null}
        </div>
      ) : (
        <div className="mt-3 text-xs text-slate-500">
          Champion is already at <code>{detail.champion_lifecycle_status}</code> (terminal lifecycle).
        </div>
      )}
    </SectionCard>
  );
}

// ---------------------------------------------------------------------------
// Leaderboard tab
// ---------------------------------------------------------------------------

function LeaderboardPanel({
  rows,
  metric,
  onMetricChange,
  onRunSelect,
  isLoading,
}: {
  rows: LeaderboardRow[];
  metric: string;
  onMetricChange: (m: (typeof LEADERBOARD_METRICS)[number]) => void;
  onRunSelect: (runId: string) => void;
  isLoading: boolean;
}) {
  return (
    <SectionCard title="Leaderboard">
      <div className="mb-3 flex items-center gap-2">
        <span className="text-[11px] uppercase tracking-wider text-slate-500">Rank by:</span>
        <select
          value={metric}
          onChange={(e) => onMetricChange(e.target.value as (typeof LEADERBOARD_METRICS)[number])}
          className="rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-slate-200"
        >
          {LEADERBOARD_METRICS.map((m) => (
            <option key={m} value={m}>{m}</option>
          ))}
        </select>
      </div>
      {isLoading ? (
        <CardSkeleton />
      ) : rows.length === 0 ? (
        <EmptyState message="No completed runs with a champion yet." />
      ) : (
        <table className="w-full text-left text-xs">
          <thead className="text-[10px] uppercase tracking-wider text-slate-500">
            <tr>
              <th className="px-2 py-1.5">Recipe</th>
              <th className="px-2 py-1.5">Lifecycle</th>
              <th className="px-2 py-1.5 text-right">Sharpe</th>
              <th className="px-2 py-1.5 text-right">CAGR</th>
              <th className="px-2 py-1.5 text-right">Fitness</th>
              <th className="px-2 py-1.5 text-right">Trades</th>
              <th className="px-2 py-1.5">Completed</th>
              <th className="px-2 py-1.5"></th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.recipe_name} className="border-t border-slate-900">
                <td className="px-2 py-1.5">
                  <div className="font-medium text-slate-200">{r.recipe_name}</div>
                  <div className="text-[10px] text-slate-500">{r.strategy_id}</div>
                </td>
                <td className="px-2 py-1.5">
                  <StatusBadge status={r.champion_lifecycle_status} tone="good" />
                </td>
                <td className="px-2 py-1.5 text-right tabular-nums">{fmtNum(r.sharpe, 2)}</td>
                <td className="px-2 py-1.5 text-right tabular-nums">{fmtPct(r.cagr)}</td>
                <td className="px-2 py-1.5 text-right tabular-nums">{fmtNum(r.fitness, 4)}</td>
                <td className="px-2 py-1.5 text-right tabular-nums">{r.trade_count ?? '—'}</td>
                <td className="px-2 py-1.5 text-[11px] text-slate-400">{fmtDateTime(r.completed_at)}</td>
                <td className="px-2 py-1.5 text-right">
                  <button
                    type="button"
                    onClick={() => onRunSelect(r.optimization_run_id)}
                    className="rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-[11px] hover:bg-slate-800"
                  >
                    Open
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </SectionCard>
  );
}
