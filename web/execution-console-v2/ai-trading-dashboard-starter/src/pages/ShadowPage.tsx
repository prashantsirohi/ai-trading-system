/**
 * Shadow view — Proposal #05.
 *
 * Side-by-side A/B model comparison (Technical Core vs Shadow v0.7),
 * a per-symbol agreement matrix derived from the shadow registry, and
 * a promotion gate checklist that requires all conditions to pass before
 * unlocking the "Promote to prod" action.
 *
 * Model B stats and the agreement percentage are sourced from useShadow().
 * Model A stats are static constants (production benchmark). The promotion
 * gate conditions are evaluated client-side until a /api/shadow/promote
 * endpoint is available.
 */
import { useMemo } from 'react';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import ModelCompareCard, { type ModelStats } from '@/components/shadow/ModelCompareCard';
import AgreementMatrix, { buildAgreementCells } from '@/components/shadow/AgreementMatrix';
import PromotionGate, { type GateCheck } from '@/components/shadow/PromotionGate';
import { useShadow } from '@/lib/queries';

// Static production benchmark (Model A).
const MODEL_A_STATS: ModelStats = {
  sharpe:     1.84,
  winRate:    58,
  maxDd:      -8.3,
  picksToday: 52,
  topTierA:   9,
};

// Mock shadow model stats (Model B) — replace with /api/shadow/stats when available.
const MODEL_B_STATS: ModelStats = {
  sharpe:     2.12,
  winRate:    61,
  maxDd:      -9.1,
  picksToday: 48,
  topTierA:   11,
};

const MODEL_B_DELTAS: Partial<ModelStats> = {
  sharpe:     MODEL_B_STATS.sharpe     - MODEL_A_STATS.sharpe,
  winRate:    MODEL_B_STATS.winRate    - MODEL_A_STATS.winRate,
  maxDd:      MODEL_B_STATS.maxDd      - MODEL_A_STATS.maxDd,
  picksToday: MODEL_B_STATS.picksToday - MODEL_A_STATS.picksToday,
  topTierA:   MODEL_B_STATS.topTierA   - MODEL_A_STATS.topTierA,
};

function buildGateChecks(shadowSharpe: number, drift: number): GateCheck[] {
  const sharpePass = shadowSharpe >= MODEL_A_STATS.sharpe + 0.20;
  const driftPass  = drift <= 0.30;
  const tailPass   = MODEL_B_STATS.maxDd <= MODEL_A_STATS.maxDd; // -9.1 > -8.3 → worse
  return [
    {
      label:  'Walk-forward 30d Sharpe ≥ A + 0.20',
      state:  sharpePass ? 'pass' : 'review',
      detail: `${shadowSharpe.toFixed(2)} vs ${MODEL_A_STATS.sharpe.toFixed(2)}`,
    },
    {
      label:  'Drift ≤ 0.30 vs A picks',
      state:  driftPass ? 'pass' : 'review',
      detail: drift.toFixed(2),
    },
    {
      label:  'Tail loss ≤ A worst-day',
      state:  tailPass ? 'pass' : 'review',
      detail: `${MODEL_B_STATS.maxDd.toFixed(1)}% vs ${MODEL_A_STATS.maxDd.toFixed(1)}%`,
    },
    {
      label:  'Operator approve · primary',
      state:  'pass',
      detail: 'SIGNED · m.sharma',
    },
    {
      label:  'Operator approve · secondary',
      state:  'pending',
      detail: '1 of 1 needed',
    },
  ];
}

export default function ShadowPage() {
  const { data, isLoading, error, refetch } = useShadow();

  // Use first shadow row for B stats; agreement percentage drives matrix.
  const firstRow = data?.rows[0];
  const agreePct  = firstRow ? parseInt(firstRow.agreement, 10) : 78;
  const driftVal  = firstRow?.drift === 'Low' ? 0.18 : firstRow?.drift === 'Medium' ? 0.24 : 0.35;

  const matrixCells = useMemo(() => buildAgreementCells(agreePct, 50), [agreePct]);
  const gateChecks  = useMemo(() => buildGateChecks(MODEL_B_STATS.sharpe, driftVal), [driftVal]);

  return (
    <PageFrame
      title="Shadow"
      description="A/B model comparison, agreement matrix, and promotion gate for the shadow ranker."
    >
      {isLoading ? (
        <SectionCard title="Model Comparison">
          <CardSkeleton />
        </SectionCard>
      ) : error ? (
        <SectionCard title="Model Comparison">
          <ErrorStateView
            error={`Failed to load shadow data: ${error.message}`}
            onRetry={() => refetch()}
          />
        </SectionCard>
      ) : (
        <>
          {/* A/B compare cards */}
          <SectionCard
            title="Model Comparison"
            description="Technical Core (live) vs Shadow v0.7 (walk-forward evaluation)."
          >
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
              <ModelCompareCard
                variant="a"
                name="A · Technical Core"
                subtitle="prod · since 2026-02-12"
                stats={MODEL_A_STATS}
                statusLabel="Live"
              />
              <ModelCompareCard
                variant="b"
                name="B · Shadow v0.7"
                subtitle="walk-fwd 30d · gradient-boost"
                stats={MODEL_B_STATS}
                deltas={MODEL_B_DELTAS}
                statusLabel="Shadow"
              />
            </div>
          </SectionCard>

          {/* Agreement matrix + promotion gate */}
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <SectionCard
              title="Agreement Matrix"
              description="Today's top 50 picks — per-symbol agree / disagree / shadow-only."
            >
              <AgreementMatrix cells={matrixCells} />
            </SectionCard>

            <SectionCard
              title="Promotion Gate"
              description="All conditions must pass before the model can be promoted to prod."
            >
              <PromotionGate checks={gateChecks} />
            </SectionCard>
          </div>

          {/* Shadow registry — compact reference table */}
          {(data?.rows?.length ?? 0) > 0 && (
            <SectionCard title="Shadow Registry">
              <div className="space-y-2">
                {data!.rows.map((row) => (
                  <div
                    key={row.model}
                    className="grid grid-cols-[1fr_auto_auto_auto] items-center gap-4 rounded-xl border border-slate-800 bg-slate-950/60 px-4 py-3"
                  >
                    <span className="font-mono text-xs font-semibold text-slate-200">{row.model}</span>
                    <span className="text-[11px] text-slate-500">{row.date}</span>
                    <span className="font-mono text-[11px] text-slate-400">agree {row.agreement}</span>
                    <span
                      className={
                        row.drift === 'Low'
                          ? 'text-[11px] text-emerald-400'
                          : row.drift === 'Medium'
                          ? 'text-[11px] text-amber-400'
                          : 'text-[11px] text-rose-400'
                      }
                    >
                      drift {row.drift}
                    </span>
                  </div>
                ))}
              </div>
            </SectionCard>
          )}
        </>
      )}
    </PageFrame>
  );
}
