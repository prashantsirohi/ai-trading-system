import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import MetricCard from '@/components/common/MetricCard';
import SectionCard from '@/components/common/SectionCard';
import StatusBadge from '@/components/common/StatusBadge';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import { titleCase } from '@/lib/utils/text';
import { usePipelineWorkspace } from '@/lib/queries';
import RankingTable from '@/components/tables/RankingTable';

function PipelineContent() {
  const { data, isLoading, error, refetch } = usePipelineWorkspace();

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
      <PageFrame title="Pipeline Workspace" description="Unified operator view across ranking, patterns, sectors, and publish state.">
        <SectionCard title="No Data">
          <EmptyState message="No pipeline data available" />
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
            <div className="mt-2">
              <StatusBadge status={data.status} />
            </div>
            <p className="mt-3 text-xs text-slate-400">Run ID: {data.runId}</p>
            <p className="mt-1 text-xs text-slate-400">As of: {data.date}</p>
          </div>

          <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
            <p className="text-xs uppercase tracking-wide text-slate-400">Trust State</p>
            <div className="mt-2">
              <StatusBadge status={data.trust} />
            </div>
            <p className="mt-3 text-xs text-slate-400">Trust Mode: {titleCase(data.trustStatus)}</p>
            <p className="mt-1 text-xs text-slate-400">Warnings: {data.warnings.length}</p>
          </div>

          <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
            <p className="text-xs uppercase tracking-wide text-slate-400">Task Status</p>
            {data.task ? (
              <>
                <div className="mt-2">
                  <StatusBadge status={data.task.status} />
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