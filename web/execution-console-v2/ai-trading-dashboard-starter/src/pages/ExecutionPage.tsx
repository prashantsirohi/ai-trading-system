/**
 * Execution view (PR #10).
 *
 * Stitches together:
 *
 *   * ExecutionStateBanner — Live/Preview pill + trust pill + capital used.
 *   * BucketColumns — Eligible / Watchlist / Blocked.
 *   * OrdersTable — eligible-only order plan.
 *   * LiveTimeline — compact per-symbol stage progression for the top names.
 *   * Capital + Risk widgets in the right rail.
 *
 * Ranking + workspace queries feed the page; per-symbol order numbers are
 * derived in ``components/execution/derive.ts`` until a routing endpoint
 * lands. The Live/Preview toggle is a cosmetic env knob today; gating is
 * still owned by the trust pipeline upstream.
 */
import { useMemo } from 'react';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import ExecutionStateBanner from '@/components/execution/ExecutionStateBanner';
import BucketColumns from '@/components/execution/BucketColumns';
import OrdersTable from '@/components/execution/OrdersTable';
import LiveTimeline from '@/components/execution/LiveTimeline';
import CapitalWidget from '@/components/execution/CapitalWidget';
import PortfolioRiskDashboard from '@/components/execution/PortfolioRiskDashboard';
import { deriveExecution } from '@/components/execution/derive';
import { useRanking, useWorkspaceSnapshot } from '@/lib/queries';
import { EXECUTION_MODE } from '@/lib/api/client';

const CAPITAL_LIMIT_PCT = 30;

function trustPillFor(label: string | null | undefined): {
  label: string;
  tone: 'good' | 'warn' | 'bad' | 'neutral';
} {
  if (!label) return { label: 'Unknown', tone: 'neutral' };
  const norm = label.toLowerCase();
  if (norm === 'trusted' || norm === 'live') return { label: 'Trusted', tone: 'good' };
  if (norm === 'failed' || norm === 'blocked') return { label: 'Blocked', tone: 'bad' };
  if (norm === 'degraded' || norm === 'legacy' || norm === 'warn')
    return { label: 'Degraded', tone: 'warn' };
  return { label, tone: 'neutral' };
}

export default function ExecutionPage() {
  const rankingQuery = useRanking();
  const snapshotQuery = useWorkspaceSnapshot(3);

  const rows = rankingQuery.data?.rows ?? [];
  const derived = useMemo(() => deriveExecution(rows), [rows]);

  const trust = trustPillFor(snapshotQuery.data?.summary.dataTrustStatus ?? null);

  if (rankingQuery.isLoading) {
    return (
      <PageFrame
        title="Execution"
        description="Routable orders, trust gating, and capital + risk telemetry."
      >
        <CardSkeleton />
      </PageFrame>
    );
  }

  if (rankingQuery.error) {
    return (
      <PageFrame
        title="Execution"
        description="Routable orders, trust gating, and capital + risk telemetry."
      >
        <ErrorStateView
          error={`Failed to load execution data: ${rankingQuery.error.message}`}
          onRetry={() => rankingQuery.refetch()}
        />
      </PageFrame>
    );
  }

  if (rows.length === 0) {
    return (
      <PageFrame
        title="Execution"
        description="Routable orders, trust gating, and capital + risk telemetry."
      >
        <EmptyState message="No ranked signals — nothing to route." />
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Execution"
      description="Routable orders, trust gating, and capital + risk telemetry."
    >
      <ExecutionStateBanner
        mode={EXECUTION_MODE}
        trustLabel={trust.label}
        trustTone={trust.tone}
        capitalUsedPct={derived.capitalUsedPct}
        capitalLimitPct={CAPITAL_LIMIT_PCT}
        eligibleCount={derived.buckets.eligible.length}
      />

      <SectionCard title="Routing Buckets">
        <BucketColumns buckets={derived.buckets} />
      </SectionCard>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <div className="xl:col-span-2">
          <SectionCard
            title="Execution Orders"
            description={
              EXECUTION_MODE === 'preview'
                ? 'Preview only — routing is disabled until live mode is enabled.'
                : 'Live mode — orders will route per the trust pipeline.'
            }
          >
            <OrdersTable orders={derived.orders} disabled={EXECUTION_MODE === 'preview'} />
          </SectionCard>
          <SectionCard title="Live Timeline">
            <LiveTimeline rows={rows} limit={8} />
          </SectionCard>
        </div>
        <div className="space-y-4">
          <CapitalWidget
            orders={derived.orders}
            capitalLimitPct={CAPITAL_LIMIT_PCT}
            capitalUsedPct={derived.capitalUsedPct}
          />
          <PortfolioRiskDashboard derived={derived} capitalLimitPct={CAPITAL_LIMIT_PCT} />
        </div>
      </div>
    </PageFrame>
  );
}
