import { useMemo } from 'react';

import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import MetricCard from '@/components/common/MetricCard';
import SectionCard from '@/components/common/SectionCard';
import StatusBadge from '@/components/common/StatusBadge';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import { usePipelineWorkspace } from '@/lib/queries';
import RankingTable from '@/components/tables/RankingTable';
import FailureRecoveryPanel from '@/components/pipeline/FailureRecoveryPanel';
import DataQualityStrip from '@/components/pipeline/DataQualityStrip';
import { deriveDqCells } from '@/lib/pipeline/dq';

function PipelineContent() {
  const { data, isLoading, error, refetch } = usePipelineWorkspace();
  const dqCells = useMemo(() => deriveDqCells(data ?? undefined), [data]);

  if (isLoading) {
    return (
      <PageFrame
        title="Pipeline"
        description="Workspace health, data quality, task state, and current candidates."
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
      <PageFrame title="Pipeline" description="Workspace health, data quality, task state, and current candidates.">
        <SectionCard title="Error">
          <ErrorStateView
            error={`Failed to load pipeline: ${error.message}`}
            onRetry={() => refetch()}
          />
        </SectionCard>
      </PageFrame>
    );
  }

  if (!data) {
    return (
      <PageFrame title="Pipeline" description="Workspace health, data quality, task state, and current candidates.">
        <SectionCard title="No Data">
          <EmptyState message="No pipeline data available" />
        </SectionCard>
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Pipeline"
      description="Workspace health, data quality, task state, and current candidates."
      headerAside={
        <div className="rounded-lg border border-slate-800 bg-slate-900 px-3 py-2.5 shadow-soft">
          <div className="mb-2 flex items-center justify-between gap-3 border-b border-slate-800 pb-2">
            <h2 className="text-sm font-semibold text-slate-100">Health</h2>
            <div className="flex items-center gap-2">
              <StatusBadge status={data.status} />
              <StatusBadge status={data.trust} />
            </div>
          </div>
          <div className="mb-2 grid gap-2 md:grid-cols-3">
            <div className="min-w-0 rounded-md border border-slate-800 bg-slate-950/40 px-2 py-1.5">
              <p className="text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-500">Run</p>
              <p className="mt-0.5 truncate font-mono text-xs text-slate-300">{data.runId}</p>
            </div>
            <div className="rounded-md border border-slate-800 bg-slate-950/40 px-2 py-1.5">
              <p className="text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-500">As Of</p>
              <p className="mt-0.5 font-mono text-xs text-slate-300">{data.date}</p>
            </div>
            <div className="rounded-md border border-slate-800 bg-slate-950/40 px-2 py-1.5">
              <p className="text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-500">Task</p>
              <p className="mt-0.5 truncate text-xs text-slate-300">
                {data.task ? data.task.currentStageLabel : 'No recent task'}
              </p>
            </div>
          </div>
          <DataQualityStrip cells={dqCells} />
        </div>
      }
    >
      {data.isFailed && (
        <SectionCard
          title="Failure Recovery"
          description="Pipeline reported a failed state. Inspect the failed stage, replay it, or halt the run."
        >
          <FailureRecoveryPanel workspace={data} />
        </SectionCard>
      )}

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {data.metrics.map((metric) => <MetricCard key={metric.label} {...metric} />)}
      </div>

      <SectionCard title="Top Ranked Candidates">
        {data.isEmpty ? (
          <EmptyState message="No ranked/breakout/pattern/sector data available. Run pipeline and refresh." />
        ) : data.topStocks.length > 0 ? (
          <RankingTable rows={data.topStocks} />
        ) : (
          <EmptyState message="No top rows returned for display." />
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
