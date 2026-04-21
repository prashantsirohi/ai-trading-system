import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import MetricCard from '@/components/common/MetricCard';
import SectionCard from '@/components/common/SectionCard';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import type { PipelineWorkspaceResponse } from '@/types/api';
import { getPipelineWorkspace } from '@/lib/api/pipeline';
import RankingTable from '@/components/tables/RankingTable';

function statusBadgeClass(status: string): string {
  const normalized = status.toLowerCase();
  if (normalized === 'ok' || normalized === 'healthy' || normalized === 'trusted' || normalized === 'completed') {
    return 'border-emerald-700 bg-emerald-950/40 text-emerald-300';
  }
  if (normalized === 'warn' || normalized === 'degraded' || normalized === 'legacy' || normalized === 'running') {
    return 'border-amber-700 bg-amber-950/40 text-amber-300';
  }
  if (normalized === 'error' || normalized === 'failed' || normalized === 'blocked' || normalized === 'terminated') {
    return 'border-rose-700 bg-rose-950/40 text-rose-300';
  }
  return 'border-slate-700 bg-slate-900 text-slate-300';
}

function titleCase(value: string): string {
  if (!value) return 'Unknown';
  return value
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}

function PipelineContent() {
  const { data, isLoading, error, refetch } = useQuery<PipelineWorkspaceResponse>({
    queryKey: ['pipeline-workspace'],
    queryFn: getPipelineWorkspace,
    refetchInterval: 60000, // Auto-refresh every 60s
  });

  if (isLoading) {
    return (
      <PageFrame
        title="Pipeline Workspace"
        description="Production operator view across workspace status, trust, task execution, and market summaries."
      >
        <SectionCard title="Workspace Status">
          <CardSkeleton />
        </SectionCard>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => <CardSkeleton key={i} />)}
        </div>
      </PageFrame>
    );
  }

  if (error) {
    return (
      <PageFrame title="Pipeline Workspace" description="Unified operator view across ranking, patterns, sectors, and publish state.">
        <SectionCard title="Error">
          <div className="space-y-3">
            <p className="text-sm text-rose-300">
              Failed to load pipeline: {error.message}
            </p>
            <button
              onClick={() => refetch()}
              className="rounded-md border border-slate-700 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800"
            >
              Retry
            </button>
          </div>
        </SectionCard>
      </PageFrame>
    );
  }

  if (!data) {
    return (
      <PageFrame title="Pipeline Workspace" description="Unified operator view across ranking, patterns, sectors, and publish state.">
        <SectionCard title="No Data">
          <p className="text-sm text-slate-400">No pipeline data available</p>
        </SectionCard>
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Pipeline Workspace"
      description="Production operator view across workspace status, trust, task execution, and market summaries."
    >
      {data.isFailed && (
        <SectionCard title="Pipeline State">
          <div className="rounded-xl border border-rose-700 bg-rose-950/30 p-4 text-sm text-rose-200">
            Pipeline is in a failed state. Check task status and backend logs before taking execution actions.
          </div>
        </SectionCard>
      )}

      {data.isDegraded && (
        <SectionCard title="Pipeline State">
          <div className="rounded-xl border border-amber-700 bg-amber-950/30 p-4 text-sm text-amber-200">
            Pipeline is degraded. Review trust and warning signals before promoting candidates.
          </div>
        </SectionCard>
      )}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {data.metrics.map((metric) => <MetricCard key={metric.label} {...metric} />)}
      </div>

      <SectionCard title="Workspace Status">
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
            <p className="text-xs uppercase tracking-wide text-slate-400">Workspace</p>
            <div className={`mt-2 inline-flex rounded-full border px-2 py-1 text-xs font-medium ${statusBadgeClass(data.status)}`}>
              {titleCase(data.status)}
            </div>
            <p className="mt-3 text-xs text-slate-400">Run ID: {data.runId}</p>
            <p className="mt-1 text-xs text-slate-400">As of: {data.date}</p>
          </div>

          <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
            <p className="text-xs uppercase tracking-wide text-slate-400">Trust State</p>
            <div className={`mt-2 inline-flex rounded-full border px-2 py-1 text-xs font-medium ${statusBadgeClass(data.trust)}`}>
              {titleCase(data.trust)}
            </div>
            <p className="mt-3 text-xs text-slate-400">Trust Mode: {titleCase(data.trustStatus)}</p>
            <p className="mt-1 text-xs text-slate-400">Warnings: {data.warnings.length}</p>
          </div>

          <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
            <p className="text-xs uppercase tracking-wide text-slate-400">Task Status</p>
            {data.task ? (
              <>
                <div className={`mt-2 inline-flex rounded-full border px-2 py-1 text-xs font-medium ${statusBadgeClass(data.task.status)}`}>
                  {titleCase(data.task.status)}
                </div>
                <p className="mt-3 text-xs text-slate-400">{data.task.label}</p>
                <p className="mt-1 text-xs text-slate-400">Stage: {data.task.currentStageLabel}</p>
              </>
            ) : (
              <p className="mt-2 text-sm text-slate-400">No recent pipeline task found.</p>
            )}
          </div>
        </div>
      </SectionCard>

      <SectionCard title="Signal Summaries">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {Object.values(data.summaries).map((summary) => (
            <div key={summary.label} className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
              <p className="text-xs uppercase tracking-wide text-slate-400">{summary.label}</p>
              <p className="mt-2 text-2xl font-semibold text-slate-100">{summary.count}</p>
              <p className="mt-2 text-xs text-slate-400">{summary.highlight}</p>
            </div>
          ))}
        </div>
      </SectionCard>

      <SectionCard title="Top Ranked Candidates">
        {data.isEmpty ? (
          <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-4 text-sm text-slate-300">
            No ranked/breakout/pattern/sector data available. Run pipeline and refresh.
          </div>
        ) : data.topStocks.length > 0 ? (
          <RankingTable rows={data.topStocks} />
        ) : (
          <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-4 text-sm text-slate-300">
            No top rows returned for display.
          </div>
        )}
      </SectionCard>
    </PageFrame>
  );
}

export default function PipelinePage() {
  return (
    <PageErrorBoundary title="Pipeline Workspace">
      <PipelineContent />
    </PageErrorBoundary>
  );
}