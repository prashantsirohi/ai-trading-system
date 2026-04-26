/**
 * Vertical timeline of stage runs for a single pipeline run.
 *
 * Each stage shows:
 *   * status pill,
 *   * attempt number (if > 1, retried),
 *   * started-at + duration,
 *   * row count when produced,
 *   * warnings collapsed under a small chevron,
 *   * inline retryable affordance for failed stages — purely a hint,
 *     since the retry endpoint is owned by the pipeline service and
 *     not yet wired into the UI.
 */
import { useState } from 'react';
import type { RunStageDetail } from '@/lib/api/runs';
import { cn } from '@/lib/utils/cn';

interface Props {
  stages: RunStageDetail[];
}

function dotTone(status: string): string {
  const norm = status.toLowerCase();
  if (norm === 'success' || norm === 'succeeded' || norm === 'completed') {
    return 'bg-emerald-500';
  }
  if (norm === 'failed' || norm === 'error') return 'bg-rose-500';
  if (norm === 'running' || norm === 'in_progress') {
    return 'bg-blue-500 animate-pulse';
  }
  if (norm === 'partial') return 'bg-amber-500';
  return 'bg-slate-600';
}

function statusPill(status: string): { label: string; classes: string } {
  const norm = status.toLowerCase();
  if (norm === 'success' || norm === 'succeeded' || norm === 'completed') {
    return {
      label: 'Success',
      classes: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200',
    };
  }
  if (norm === 'failed' || norm === 'error') {
    return { label: 'Failed', classes: 'border-rose-500/40 bg-rose-500/10 text-rose-200' };
  }
  if (norm === 'running' || norm === 'in_progress') {
    return { label: 'Running', classes: 'border-blue-500/40 bg-blue-500/10 text-blue-200' };
  }
  if (norm === 'partial') {
    return { label: 'Partial', classes: 'border-amber-500/40 bg-amber-500/10 text-amber-200' };
  }
  return { label: status, classes: 'border-slate-700 bg-slate-900/60 text-slate-300' };
}

export default function PipelineStageTimeline({ stages }: Props) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  if (stages.length === 0) {
    return <p className="text-xs text-slate-500">No stage records for this run.</p>;
  }

  return (
    <ol className="relative space-y-4 border-l border-slate-800 pl-5">
      {stages.map((stage) => {
        const pill = statusPill(stage.status);
        const norm = stage.status.toLowerCase();
        const failed = norm === 'failed' || norm === 'error';
        const isOpen = expanded[stage.stageName];
        return (
          <li key={`${stage.stageName}-${stage.attemptNumber ?? 0}`} className="relative">
            <span
              className={cn(
                'absolute -left-[27px] top-1 h-3 w-3 rounded-full ring-4 ring-slate-900',
                dotTone(stage.status),
              )}
            />
            <div className="rounded-2xl border border-slate-800 bg-slate-950/60 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-sm font-semibold uppercase tracking-wide text-slate-100">
                  {stage.stageName}
                </span>
                <span
                  className={cn(
                    'rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
                    pill.classes,
                  )}
                >
                  {pill.label}
                </span>
                {stage.attemptNumber && stage.attemptNumber > 1 ? (
                  <span className="rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-amber-200">
                    Attempt {stage.attemptNumber}
                  </span>
                ) : null}
                <span className="ml-auto text-[11px] text-slate-400">
                  {stage.durationLabel}
                  {stage.rowCount !== null
                    ? ` · ${stage.rowCount.toLocaleString()} rows`
                    : ''}
                </span>
              </div>
              {stage.errorMessage ? (
                <p className="mt-2 truncate text-xs text-rose-300/80" title={stage.errorMessage}>
                  {stage.errorMessage}
                </p>
              ) : null}
              {(stage.warnings.length > 0 || failed) ? (
                <button
                  type="button"
                  onClick={() =>
                    setExpanded((prev) => ({ ...prev, [stage.stageName]: !prev[stage.stageName] }))
                  }
                  className="mt-2 text-[11px] uppercase tracking-wider text-slate-400 transition-colors hover:text-slate-200"
                >
                  {isOpen ? 'Hide details ▾' : `Show details ▸ ${stage.warnings.length} warnings`}
                </button>
              ) : null}
              {isOpen ? (
                <div className="mt-2 space-y-2 border-t border-slate-800 pt-2 text-[11px] text-slate-300">
                  {stage.warnings.length > 0 ? (
                    <ul className="space-y-1">
                      {stage.warnings.map((w, idx) => (
                        <li key={idx} className="rounded bg-amber-500/10 px-2 py-1 text-amber-200">
                          {w}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="text-slate-500">No warnings recorded.</p>
                  )}
                  {failed ? (
                    <p className="rounded border border-rose-500/30 bg-rose-500/10 px-2 py-1 text-rose-200">
                      Retryable — use the pipeline operator endpoint to re-queue this stage.
                    </p>
                  ) : null}
                </div>
              ) : null}
            </div>
          </li>
        );
      })}
    </ol>
  );
}
