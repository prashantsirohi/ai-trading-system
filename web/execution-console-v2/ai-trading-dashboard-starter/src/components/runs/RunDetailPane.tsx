/**
 * Right pane of the Runs audit split-view.
 *
 * Stitches together:
 *   * Header (run id, started/ended, duration, copy-id) with cross-jump
 *     buttons to Ranking / Patterns / Execution.
 *   * Verdict + Root-Cause banner derived from the run + first failed stage.
 *   * Pipeline Stage Timeline.
 *   * DQ summary chip → opens DqModal.
 *   * Artifacts list with download links.
 *   * Publish channels grid (driven by ``deliveryLogs``).
 */
import { useMemo, useState } from 'react';
import type { RunDetail, DqResults, RunArtifacts } from '@/lib/api/runs';
import PipelineStageTimeline from './PipelineStageTimeline';
import ArtifactsList from './ArtifactsList';
import DqModal from './DqModal';
import { cn } from '@/lib/utils/cn';

interface Props {
  detail: RunDetail | null | undefined;
  isLoading: boolean;
  dq: DqResults | null | undefined;
  dqLoading: boolean;
  artifacts: RunArtifacts | null | undefined;
  artifactsLoading: boolean;
  onJump: (page: 'ranking' | 'patterns' | 'execution') => void;
}

function shortDate(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return iso;
  return d.toLocaleString();
}

export default function RunDetailPane({
  detail,
  isLoading,
  dq,
  dqLoading,
  artifacts,
  artifactsLoading,
  onJump,
}: Props) {
  const [dqOpen, setDqOpen] = useState(false);

  const failedStage = useMemo(
    () =>
      detail?.stages.find((s) => {
        const norm = s.status.toLowerCase();
        return norm === 'failed' || norm === 'error';
      }),
    [detail],
  );

  if (isLoading) {
    return <p className="text-sm text-slate-500">Loading run detail…</p>;
  }
  if (!detail || !detail.run) {
    return (
      <p className="text-sm text-slate-500">
        Select a run from the history table to inspect its stages, DQ results, and artifacts.
      </p>
    );
  }

  const run = detail.run;
  const status = run.status.toLowerCase();
  const verdictTone =
    status === 'failed' || status === 'error'
      ? 'border-rose-500/40 bg-rose-500/10 text-rose-100'
      : status === 'success' || status === 'completed' || status === 'succeeded'
      ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-100'
      : 'border-amber-500/40 bg-amber-500/10 text-amber-100';

  const verdictLabel =
    status === 'failed' || status === 'error'
      ? 'Failed'
      : status === 'success' || status === 'completed' || status === 'succeeded'
      ? 'Successful'
      : run.status;

  const dqFailedTotal = dq?.totalFailed ?? 0;
  const dqHasErrors = (dq?.countsBySeverity?.error?.failed ?? 0) > 0;

  return (
    <div className="flex flex-col gap-4">
      {/* Header */}
      <header className="flex flex-wrap items-start gap-3">
        <div className="flex-1">
          <h3 className="font-mono text-lg text-slate-100">{run.runId}</h3>
          <p className="mt-1 text-xs text-slate-400">
            {shortDate(run.startedAt)}
            {run.endedAt ? ` → ${shortDate(run.endedAt)}` : ''}
            <span className="mx-1">·</span>
            {run.durationLabel}
            {run.currentStage ? (
              <>
                <span className="mx-1">·</span>
                <span className="uppercase tracking-wider">{run.currentStage}</span>
              </>
            ) : null}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => navigator.clipboard.writeText(run.runId)}
            className="rounded-full border border-slate-700 bg-slate-900/60 px-3 py-1 text-[11px] font-semibold uppercase tracking-wider text-slate-300 hover:border-slate-500 hover:text-slate-100"
            aria-label="Copy run ID"
          >
            Copy ID
          </button>
          <button
            type="button"
            onClick={() => onJump('ranking')}
            className="rounded-full border border-blue-500/40 bg-blue-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-wider text-blue-200 hover:border-blue-300/60"
          >
            Ranking
          </button>
          <button
            type="button"
            onClick={() => onJump('patterns')}
            className="rounded-full border border-violet-500/40 bg-violet-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-wider text-violet-200 hover:border-violet-300/60"
          >
            Patterns
          </button>
          <button
            type="button"
            onClick={() => onJump('execution')}
            className="rounded-full border border-emerald-500/40 bg-emerald-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-wider text-emerald-200 hover:border-emerald-300/60"
          >
            Execution
          </button>
        </div>
      </header>

      {/* Verdict + Root-Cause */}
      <div className={cn('rounded-2xl border p-4', verdictTone)}>
        <p className="text-[11px] font-semibold uppercase tracking-widest opacity-80">
          Verdict
        </p>
        <p className="mt-1 text-base font-semibold">{verdictLabel}</p>
        {failedStage ? (
          <div className="mt-3 rounded-lg bg-slate-950/40 p-3 text-xs">
            <p className="text-[10px] uppercase tracking-widest opacity-70">Root cause</p>
            <p className="mt-1 font-mono">
              {failedStage.stageName} · attempt {failedStage.attemptNumber ?? '?'}
            </p>
            {failedStage.errorMessage ? (
              <p className="mt-1 break-words text-rose-100">{failedStage.errorMessage}</p>
            ) : null}
          </div>
        ) : run.errorMessage ? (
          <div className="mt-3 rounded-lg bg-slate-950/40 p-3 text-xs">
            <p className="text-[10px] uppercase tracking-widest opacity-70">Error</p>
            <p className="mt-1 break-words">{run.errorMessage}</p>
          </div>
        ) : null}
      </div>

      {/* Stage timeline + DQ + artifacts */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <section className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
          <div className="mb-3 flex items-center justify-between">
            <h4 className="text-sm font-semibold text-slate-100">Pipeline stages</h4>
            <span className="text-[10px] uppercase tracking-widest text-slate-500">
              {detail.stages.length} stage{detail.stages.length === 1 ? '' : 's'}
            </span>
          </div>
          <PipelineStageTimeline stages={detail.stages} />
        </section>

        <section className="space-y-4">
          <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
            <div className="mb-3 flex items-center justify-between">
              <h4 className="text-sm font-semibold text-slate-100">Data quality</h4>
              <button
                type="button"
                onClick={() => setDqOpen(true)}
                className={cn(
                  'rounded-full border px-3 py-1 text-[10px] font-semibold uppercase tracking-wider',
                  dqHasErrors
                    ? 'border-rose-500/40 bg-rose-500/10 text-rose-200'
                    : dqFailedTotal > 0
                    ? 'border-amber-500/40 bg-amber-500/10 text-amber-200'
                    : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200',
                )}
              >
                Open DQ
              </button>
            </div>
            <p className="text-xs text-slate-300">
              {dq?.available
                ? dqFailedTotal === 0
                  ? `All ${dq.results.length} rules passed.`
                  : `${dqFailedTotal} failed · ${dq.totalPassed} passed (${dq.results.length} total)`
                : 'No DQ records yet.'}
            </p>
          </div>

          <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
            <h4 className="mb-3 text-sm font-semibold text-slate-100">Artifacts</h4>
            <ArtifactsList data={artifacts} isLoading={artifactsLoading} />
          </div>
        </section>
      </div>

      {/* Publish channels */}
      <section className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
        <h4 className="mb-3 text-sm font-semibold text-slate-100">Publish channels</h4>
        {detail.deliveryLogs.length === 0 ? (
          <p className="text-xs text-slate-500">
            No publish attempts recorded for this run.
          </p>
        ) : (
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
            {detail.deliveryLogs.map((log) => {
              const norm = log.status.toLowerCase();
              const tone =
                norm === 'success' || norm === 'delivered'
                  ? 'border-emerald-500/40 bg-emerald-500/10'
                  : norm === 'failed' || norm === 'error'
                  ? 'border-rose-500/40 bg-rose-500/10'
                  : 'border-slate-700 bg-slate-950/40';
              return (
                <div
                  key={log.logId}
                  className={cn('rounded-xl border p-3 text-xs text-slate-200', tone)}
                >
                  <div className="flex items-center gap-2">
                    <span className="font-semibold uppercase tracking-wider">{log.channel}</span>
                    <span className="ml-auto text-[10px] uppercase tracking-wider text-slate-400">
                      {norm}
                    </span>
                  </div>
                  {log.message ? (
                    <p className="mt-1 truncate text-slate-400" title={log.message}>
                      {log.message}
                    </p>
                  ) : null}
                  {log.createdAt ? (
                    <p className="mt-1 text-[10px] text-slate-500">{shortDate(log.createdAt)}</p>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}
      </section>

      <DqModal data={dq} isLoading={dqLoading} open={dqOpen} onClose={() => setDqOpen(false)} />
    </div>
  );
}
