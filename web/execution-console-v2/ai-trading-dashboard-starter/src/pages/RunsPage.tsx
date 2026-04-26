/**
 * Runs audit view (PR #11).
 *
 * Operator post-mortem surface. Three-tier layout:
 *
 *   1. ``RunsKpiStrip`` — latest status / last successful run / failed-runs 24h
 *      / publish errors 24h.
 *   2. Split pane: ``RunsHistoryTable`` (left, filterable) +
 *      ``RunDetailPane`` (right, drives the selected run).
 *
 * Source endpoints: ``/api/execution/runs``, ``/runs/{id}``, ``/runs/{id}/dq``,
 * ``/runs/{id}/artifacts`` (PR #4).
 */
import { useEffect, useMemo, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';

import PageFrame from '@/components/common/PageFrame';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import RunsKpiStrip from '@/components/runs/RunsKpiStrip';
import RunsHistoryTable from '@/components/runs/RunsHistoryTable';
import RunDetailPane from '@/components/runs/RunDetailPane';
import {
  useRunsList,
  useRunDetail,
  useRunDqResults,
  useRunArtifacts,
} from '@/lib/queries';

export default function RunsPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const runsQuery = useRunsList(25);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const runs = useMemo(() => runsQuery.data?.runs ?? [], [runsQuery.data]);

  // Read ``?#runId`` from the URL hash so the command bar can deep-link.
  useEffect(() => {
    const hash = location.hash.replace(/^#/, '');
    if (hash) setSelectedRunId(decodeURIComponent(hash));
  }, [location.hash]);

  // Auto-select the latest run on first load — operators almost always want
  // to inspect "what happened most recently".
  useEffect(() => {
    if (selectedRunId === null && runs.length > 0) {
      setSelectedRunId(runs[0].runId);
    }
  }, [runs, selectedRunId]);

  const detailQuery = useRunDetail(selectedRunId);
  const dqQuery = useRunDqResults(selectedRunId);
  const artifactsQuery = useRunArtifacts(selectedRunId);

  if (runsQuery.isLoading) {
    return (
      <PageFrame
        title="Runs"
        description="Audit pipeline runs, stage attempts, DQ results, and published artifacts."
      >
        <CardSkeleton />
      </PageFrame>
    );
  }

  if (runsQuery.error) {
    return (
      <PageFrame
        title="Runs"
        description="Audit pipeline runs, stage attempts, DQ results, and published artifacts."
      >
        <ErrorStateView
          error={`Failed to load runs: ${runsQuery.error.message}`}
          onRetry={() => runsQuery.refetch()}
        />
      </PageFrame>
    );
  }

  if (runs.length === 0) {
    return (
      <PageFrame
        title="Runs"
        description="Audit pipeline runs, stage attempts, DQ results, and published artifacts."
      >
        <EmptyState message="No pipeline runs recorded yet." />
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Runs"
      description="Audit pipeline runs, stage attempts, DQ results, and published artifacts."
    >
      <RunsKpiStrip runs={runs} />

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-12">
        <section className="xl:col-span-4">
          <div className="rounded-3xl border border-slate-800 bg-slate-900 p-4 shadow-soft">
            <h3 className="mb-3 text-sm font-semibold text-slate-100">Run history</h3>
            <RunsHistoryTable
              runs={runs}
              selectedRunId={selectedRunId}
              onSelect={setSelectedRunId}
            />
          </div>
        </section>

        <section className="xl:col-span-8">
          <div className="rounded-3xl border border-slate-800 bg-slate-900 p-5 shadow-soft">
            <RunDetailPane
              detail={detailQuery.data}
              isLoading={detailQuery.isLoading}
              dq={dqQuery.data}
              dqLoading={dqQuery.isLoading}
              artifacts={artifactsQuery.data}
              artifactsLoading={artifactsQuery.isLoading}
              onJump={(page) => navigate(`/${page}`)}
            />
          </div>
        </section>
      </div>
    </PageFrame>
  );
}
