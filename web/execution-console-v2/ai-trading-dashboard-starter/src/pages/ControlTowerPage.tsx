/**
 * Control Tower — the new landing page (PR #7).
 *
 * Composes the three Phase 2b chrome blocks against the slim
 * ``/workspace/snapshot`` endpoint:
 *
 *   * Decision Summary (top-3 actions)
 *   * Trust banner (system trust + counters)
 *   * Output Summary cards (4-card nav strip)
 *
 * The page deliberately stays thin — it does not own data fetching for
 * the per-tab views (PRs #8-12 do that). When the snapshot is
 * unavailable we render the empty/error state inline rather than blanking
 * the whole landing.
 */
import PageFrame from '@/components/common/PageFrame';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import DecisionSummaryBanner from '@/components/control-tower/DecisionSummaryBanner';
import TrustBanner from '@/components/control-tower/TrustBanner';
import OutputSummaryCards from '@/components/control-tower/OutputSummaryCards';
import MovingAverageBreadthChart from '@/components/control-tower/MovingAverageBreadthChart';
import { useMarketBreadth, useWorkspaceSnapshot } from '@/lib/queries';

export default function ControlTowerPage() {
  const { data, isLoading, error, refetch } = useWorkspaceSnapshot(3);
  const breadthQuery = useMarketBreadth();

  return (
    <PageFrame
      title="Control Tower"
      description="Top-of-funnel actions, system trust, and quick jumps into the deep views."
      headerAside={!error && data ? <TrustBanner snapshot={data} isLoading={isLoading} compact /> : undefined}
    >
      {error ? (
        <ErrorStateView
          error={`Failed to load workspace snapshot: ${error.message}`}
          onRetry={() => refetch()}
        />
      ) : isLoading ? (
        <div className="space-y-4">
          <CardSkeleton />
          <CardSkeleton />
          <CardSkeleton />
        </div>
      ) : !data || !data.available ? (
        <div className="space-y-4">
          <EmptyState message="No workspace snapshot yet — the latest pipeline run hasn't produced a dashboard payload." />
        </div>
      ) : (
        <div className="space-y-4">
          <DecisionSummaryBanner actions={data.topActions} />
          <MovingAverageBreadthChart rows={breadthQuery.data ?? []} />
          <OutputSummaryCards snapshot={data} />
        </div>
      )}
    </PageFrame>
  );
}
