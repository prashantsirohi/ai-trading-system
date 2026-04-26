/**
 * Risk & Exposure page (Quantis proposal #03).
 *
 * Route: /risk — nav shortcut: g x, sidebar after Execution.
 * Layout:
 *   1. KPI strip — Gross Exposure, Net Exposure, Live Drawdown, Top Concentration.
 *   2. Two-column: Sector exposure bars (left) + Circuit breakers (right).
 *
 * Numbers are derived from the ranking feed until a dedicated
 * /api/execution/risk endpoint exists.  All derivation constants are named
 * in lib/risk/derive.ts so the swap-out is mechanical.
 */
import { useMemo } from 'react';

import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import { TableSkeleton } from '@/components/common/LoadingSkeleton';
import RiskKpiCard from '@/components/risk/RiskKpiCard';
import SectorExposureChart from '@/components/risk/SectorExposureChart';
import CircuitBreakers from '@/components/risk/CircuitBreakers';
import { useRanking } from '@/lib/queries';
import { deriveRisk } from '@/lib/risk/derive';

function RiskContent() {
  const { data, isLoading } = useRanking();
  const rows = data?.rows ?? [];
  const risk = useMemo(() => deriveRisk(rows), [rows]);

  if (isLoading) {
    return (
      <PageFrame title="Risk & Exposure" description="Portfolio guardrails, sector caps, and circuit-breaker status.">
        <SectionCard title="Key Metrics">
          <TableSkeleton rows={4} />
        </SectionCard>
      </PageFrame>
    );
  }

  if (rows.length === 0) {
    return (
      <PageFrame title="Risk & Exposure" description="Portfolio guardrails, sector caps, and circuit-breaker status.">
        <SectionCard title="Key Metrics">
          <EmptyState message="No ranking data available — run the pipeline first." />
        </SectionCard>
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Risk & Exposure"
      description="Portfolio guardrails, sector caps, and circuit-breaker status. Derived from the live ranking feed."
    >
      {/* KPI strip */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {risk.kpis.map((kpi) => (
          <RiskKpiCard key={kpi.label} {...kpi} />
        ))}
      </div>

      {/* Sector + Circuit breakers */}
      <div className="grid gap-4 xl:grid-cols-2">
        <SectionCard
          title="Sector Exposure vs Cap"
          description="Horizontal bar = current exposure. Amber marker = sector limit."
        >
          <SectorExposureChart rows={risk.sectorExposure} />
        </SectionCard>

        <SectionCard
          title="Circuit Breakers"
          description="Named drawdown and trust-state guardrails with current status."
        >
          <CircuitBreakers breakers={risk.circuitBreakers} />
          <p className="mt-4 rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-2 text-[11px] leading-relaxed text-slate-500">
            Numbers are derived from the ranking feed. A dedicated{' '}
            <code className="font-mono text-slate-400">/api/execution/risk</code> endpoint
            will replace these heuristics once available.
          </p>
        </SectionCard>
      </div>
    </PageFrame>
  );
}

export default function RiskPage() {
  return (
    <PageErrorBoundary title="Risk & Exposure" description="Failed to load risk page">
      <RiskContent />
    </PageErrorBoundary>
  );
}
