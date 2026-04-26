/**
 * Execution view — Proposals #10 (base) + #06 (ticket queue / pre-trade checks).
 *
 * Sections:
 *   * ExecutionStateBanner — Live/Preview pill + trust pill + capital used.
 *   * Ticket queue + pre-trade checks panel (#06).
 *   * BucketColumns — Eligible / Watchlist / Blocked.
 *   * OrdersTable — eligible-only order plan.
 *   * LiveTimeline — compact per-symbol stage progression for the top names.
 *   * Capital + Risk widgets in the right rail.
 */
import { useMemo, useState } from 'react';

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
import TicketQueue, { ordersToTickets } from '@/components/execution/TicketQueue';
import PreTradeChecks, { derivePreTradeChecks } from '@/components/execution/PreTradeChecks';
import { deriveExecution } from '@/components/execution/derive';
import { useRanking, useWorkspaceSnapshot } from '@/lib/queries';
import { useWorkspace } from '@/components/workspace/WorkspaceContext';
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
  const { openWorkspace } = useWorkspace();

  const rows = rankingQuery.data?.rows ?? [];
  const derived = useMemo(() => deriveExecution(rows), [rows]);

  const trust = trustPillFor(snapshotQuery.data?.summary.dataTrustStatus ?? null);

  const tickets = useMemo(() => ordersToTickets(derived.orders), [derived.orders]);
  const { checks: preTrade, allGreen } = useMemo(
    () => derivePreTradeChecks(derived, trust.tone),
    [derived, trust.tone],
  );

  const [sendNotice, setSendNotice] = useState<string | null>(null);

  function handleSendAll() {
    setSendNotice('Send-all action is not wired to the broker API yet — connect the routing endpoint to enable.');
    setTimeout(() => setSendNotice(null), 4000);
  }

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

      {/* ── Proposal #06: ticket queue + pre-trade checks ─────────────── */}
      {tickets.length > 0 && (
        <SectionCard
          title="Order Tickets"
          description="Staged tickets awaiting send-all. Pre-trade checks must all pass first."
        >
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-[1fr_360px]">
            <TicketQueue
              tickets={tickets}
              onSendAll={handleSendAll}
              allChecksGreen={allGreen}
            />
            <div>
              <p className="mb-2 text-sm font-semibold text-slate-300">Pre-trade checks</p>
              <PreTradeChecks checks={preTrade} />
            </div>
          </div>

          {sendNotice && (
            <div className="mt-3 rounded-xl border border-amber-700/40 bg-amber-500/10 px-4 py-2 text-xs text-amber-300">
              {sendNotice}
            </div>
          )}
        </SectionCard>
      )}

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
            <OrdersTable
              orders={derived.orders}
              disabled={EXECUTION_MODE === 'preview'}
              onRowClick={(order) => openWorkspace(order.symbol)}
            />
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
