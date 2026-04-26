/**
 * Run-history table — left pane of the Runs audit split-view.
 *
 * Filters: All / Production / Research / Failed. Production is everything
 * whose ``run_id`` doesn't start with ``research_`` or ``shadow_``;
 * Research is the inverse. Selection emits ``onSelect(runId)`` so the parent
 * can drive the detail pane.
 */
import { useMemo, useState } from 'react';
import type { RunSummary } from '@/lib/api/runs';
import { cn } from '@/lib/utils/cn';

type FilterKey = 'all' | 'production' | 'research' | 'failed';

interface Props {
  runs: RunSummary[];
  selectedRunId: string | null;
  onSelect: (runId: string) => void;
}

function isResearch(runId: string): boolean {
  const norm = runId.toLowerCase();
  return norm.startsWith('research_') || norm.startsWith('shadow_') || norm.includes('-research-');
}

function isFailed(status: string): boolean {
  const norm = status.toLowerCase();
  return norm === 'failed' || norm === 'error';
}

const FILTER_LABELS: Record<FilterKey, string> = {
  all: 'All',
  production: 'Production',
  research: 'Research',
  failed: 'Failed',
};

const FILTERS: FilterKey[] = ['all', 'production', 'research', 'failed'];

function statusBadge(status: string): { tone: string; label: string } {
  const norm = status.toLowerCase();
  if (norm === 'success' || norm === 'succeeded' || norm === 'completed') {
    return { tone: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200', label: 'Success' };
  }
  if (norm === 'failed' || norm === 'error') {
    return { tone: 'border-rose-500/40 bg-rose-500/10 text-rose-200', label: 'Failed' };
  }
  if (norm === 'running' || norm === 'in_progress') {
    return { tone: 'border-blue-500/40 bg-blue-500/10 text-blue-200', label: 'Running' };
  }
  if (norm === 'partial') {
    return { tone: 'border-amber-500/40 bg-amber-500/10 text-amber-200', label: 'Partial' };
  }
  return { tone: 'border-slate-700 bg-slate-900/60 text-slate-300', label: status };
}

function shortDate(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (!Number.isFinite(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export default function RunsHistoryTable({ runs, selectedRunId, onSelect }: Props) {
  const [filter, setFilter] = useState<FilterKey>('all');

  const filtered = useMemo(() => {
    return runs.filter((r) => {
      if (filter === 'production') return !isResearch(r.runId);
      if (filter === 'research') return isResearch(r.runId);
      if (filter === 'failed') return isFailed(r.status);
      return true;
    });
  }, [runs, filter]);

  return (
    <div className="flex h-full flex-col">
      <div className="mb-3 flex flex-wrap items-center gap-2">
        {FILTERS.map((f) => {
          const active = filter === f;
          return (
            <button
              key={f}
              type="button"
              onClick={() => setFilter(f)}
              className={cn(
                'rounded-full border px-3 py-1 text-xs font-semibold transition-colors',
                active
                  ? 'border-blue-500/60 bg-blue-500/15 text-blue-100'
                  : 'border-slate-700 bg-slate-900/60 text-slate-300 hover:border-slate-600',
              )}
            >
              {FILTER_LABELS[f]}
            </button>
          );
        })}
        <span className="ml-auto text-[11px] uppercase tracking-wider text-slate-500">
          {filtered.length} / {runs.length}
        </span>
      </div>

      <div className="flex-1 overflow-y-auto rounded-2xl border border-slate-800 bg-slate-950/40">
        {filtered.length === 0 ? (
          <p className="p-6 text-sm text-slate-500">No runs match this filter.</p>
        ) : (
          <ul className="divide-y divide-slate-800">
            {filtered.map((run) => {
              const badge = statusBadge(run.status);
              const selected = selectedRunId === run.runId;
              return (
                <li key={run.runId}>
                  <button
                    type="button"
                    onClick={() => onSelect(run.runId)}
                    className={cn(
                      'flex w-full flex-col gap-1 px-4 py-3 text-left transition-colors',
                      selected
                        ? 'bg-blue-500/10'
                        : 'hover:bg-slate-900/60 focus:bg-slate-900/60 focus:outline-none',
                    )}
                  >
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-xs text-slate-200">{run.runId}</span>
                      <span
                        className={cn(
                          'ml-auto rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
                          badge.tone,
                        )}
                      >
                        {badge.label}
                      </span>
                    </div>
                    <div className="flex items-center gap-3 text-[11px] text-slate-400">
                      <span>{shortDate(run.startedAt)}</span>
                      <span>·</span>
                      <span>{run.durationLabel}</span>
                      {run.currentStage ? (
                        <>
                          <span>·</span>
                          <span className="uppercase tracking-wider">{run.currentStage}</span>
                        </>
                      ) : null}
                      {isResearch(run.runId) ? (
                        <span className="ml-auto rounded-full border border-violet-500/40 bg-violet-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-violet-200">
                          Research
                        </span>
                      ) : null}
                    </div>
                    {run.errorMessage ? (
                      <div className="truncate text-[11px] text-rose-300/80">
                        {run.errorMessage}
                      </div>
                    ) : null}
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}
