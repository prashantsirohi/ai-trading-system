/**
 * Sector Detail — Proposal #09.
 *
 * Route: /sectors/:sector
 *
 * Shows a sector header (RS, momentum, breadth stats), a left-side
 * technical indicator filter rail (12 indicators in 5 groups), active
 * filter chips, and a constituent table with live indicator badges.
 *
 * Constituent data is sourced from the live ranking endpoint filtered by
 * sector name. Sector headline stats come from useSectors().
 */
import { useEffect, useMemo, useState } from 'react';
import { useParams, Link } from 'react-router-dom';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import TechFilterRail, { INDICATOR_GROUPS, type IndicatorKey } from '@/components/sectors/TechFilterRail';
import ConstituentTable from '@/components/sectors/ConstituentTable';
import { useSectors } from '@/lib/queries';
import type { Constituent } from '@/lib/mock/sectorConstituents';
import { getSectorConstituents, type SectorConstituentsResponse } from '@/lib/api/sectors';
import { cn } from '@/lib/utils/cn';

function quadrantPill(quadrant: string) {
  const norm = quadrant.toLowerCase();
  if (norm === 'leading')   return 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300';
  if (norm === 'improving') return 'border-blue-500/40 bg-blue-500/10 text-blue-300';
  if (norm === 'weakening') return 'border-amber-500/40 bg-amber-500/10 text-amber-300';
  return 'border-rose-500/40 bg-rose-500/10 text-rose-300';
}

export default function SectorDetailPage() {
  const { sector: sectorParam } = useParams<{ sector: string }>();
  const sectorName = sectorParam
    ? decodeURIComponent(sectorParam).replace(/-/g, ' ')
    : '';

  const sectorsQuery = useSectors();
  const sectorData = sectorsQuery.data?.sectors.find(
    (s) => s.sector.toLowerCase() === sectorName.toLowerCase(),
  );

  // ── Live constituent data from dedicated sector endpoint ─────────────────
  const [sectorRes, setSectorRes] = useState<SectorConstituentsResponse | null>(null);
  const [constituentsLoading, setConstituentsLoading] = useState(true);
  const allConstituents: Constituent[] = sectorRes?.constituents ?? [];
  const stageSummary = sectorRes?.stageSummary;

  useEffect(() => {
    if (!sectorName) return;
    setConstituentsLoading(true);
    getSectorConstituents(sectorName)
      .then(setSectorRes)
      .finally(() => setConstituentsLoading(false));
  }, [sectorName]);

  // ── Filter rail state ─────────────────────────────────────────────────────
  const [activeFilters, setActiveFilters] = useState<Set<IndicatorKey>>(new Set());
  const [showAll, setShowAll] = useState(false);

  function toggleFilter(key: IndicatorKey) {
    setActiveFilters((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      setShowAll(false);
      return next;
    });
  }

  const visibleRows = useMemo(() => {
    if (showAll || activeFilters.size === 0) return allConstituents;
    return allConstituents.filter((row) =>
      [...activeFilters].every((k) => row[k] === true),
    );
  }, [allConstituents, activeFilters, showAll]);

  const hiddenCount = showAll ? 0 : allConstituents.length - visibleRows.length;

  // ── Active-filter chips ───────────────────────────────────────────────────
  const activeChips = useMemo(() => {
    const defs = INDICATOR_GROUPS.flatMap((g) => g.items);
    return [...activeFilters].map((k) => defs.find((d) => d.key === k)!).filter(Boolean);
  }, [activeFilters]);

  // ── Derived sector stats ──────────────────────────────────────────────────
  const aboveMa50Pct = allConstituents.length
    ? Math.round((allConstituents.filter((r) => r.aboveMa50).length / allConstituents.length) * 100)
    : 0;
  const avg5dChg = allConstituents.length
    ? +(allConstituents.reduce((s, r) => s + r.chgPct, 0) / allConstituents.length).toFixed(2)
    : 0;

  if (sectorsQuery.isLoading || constituentsLoading) {
    return (
      <PageFrame title={sectorName || 'Sector'} description="Sector detail">
        <CardSkeleton />
      </PageFrame>
    );
  }

  if (!sectorName) {
    return (
      <PageFrame title="Sector" description="">
        <EmptyState message="No sector specified." />
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title={sectorName}
      description={`Constituents, technical filters, and indicator breakdown · /sectors/${encodeURIComponent(sectorName)}`}
    >
      {/* Breadcrumb */}
      <div className="mb-4 flex items-center gap-2 text-xs text-slate-500">
        <Link to="/sectors" className="hover:text-slate-300 transition-colors">Sectors</Link>
        <span>›</span>
        <span className="text-slate-300">{sectorName}</span>
      </div>

      {/* Sector header card */}
      <SectionCard title={sectorName}>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-[10px] uppercase tracking-widest text-slate-500">Sector</p>
            <h2 className="text-2xl font-bold text-slate-100">{sectorName}</h2>
            <p className="mt-0.5 text-xs text-slate-500">
              {allConstituents.length} constituents · NSE · last updated 09:24 IST
            </p>
            {sectorData && (
              <div className="mt-2 flex items-center gap-3">
                <span
                  className={cn(
                    'rounded-full border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide',
                    quadrantPill(sectorData.quadrant),
                  )}
                >
                  {sectorData.quadrant}
                </span>
                <span className="text-xs text-slate-400">
                  RS {sectorData.rs} · momentum{' '}
                  <span className={sectorData.momentum >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
                    {sectorData.momentum >= 0 ? '+' : ''}{sectorData.momentum.toFixed(2)}
                  </span>
                </span>
              </div>
            )}
          </div>

          {sectorData && (
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              {[
                { label: 'RS', value: String(sectorData.rs), pos: true },
                { label: 'Momentum', value: `${sectorData.momentum >= 0 ? '+' : ''}${sectorData.momentum.toFixed(2)}`, pos: sectorData.momentum >= 0 },
                { label: '% > 50DMA', value: `${aboveMa50Pct}%`, pos: aboveMa50Pct >= 50 },
                { label: '5d Δ avg', value: `${avg5dChg >= 0 ? '+' : ''}${avg5dChg}%`, pos: avg5dChg >= 0 },
              ].map((stat) => (
                <div
                  key={stat.label}
                  className="rounded-xl border border-slate-800 bg-slate-900/60 px-3 py-2 text-center"
                >
                  <p className="text-[10px] uppercase tracking-wide text-slate-500">{stat.label}</p>
                  <p className={cn('mt-1 font-mono text-sm font-semibold', stat.pos ? 'text-emerald-300' : 'text-rose-300')}>
                    {stat.value}
                  </p>
                </div>
              ))}
            </div>
          )}
        </div>
      </SectionCard>

      {/* Stage distribution bar */}
      {stageSummary && stageSummary.labeled > 0 && (
        <SectionCard title="Weinstein Stage Distribution">
          <div className="flex items-center gap-4 flex-wrap">
            {/* Stacked bar */}
            <div className="flex h-4 flex-1 min-w-40 overflow-hidden rounded-full">
              {(['S2','S1','S3','S4'] as const).map((s) => {
                const pct = stageSummary[`${s}_pct` as keyof typeof stageSummary] as number;
                if (!pct) return null;
                const colors: Record<string,string> = {
                  S2: 'bg-emerald-500', S1: 'bg-blue-400',
                  S3: 'bg-amber-400',   S4: 'bg-rose-500',
                };
                return (
                  <div
                    key={s}
                    style={{ width: `${pct}%` }}
                    className={colors[s]}
                    title={`${s}: ${pct}%`}
                  />
                );
              })}
            </div>
            {/* Legend */}
            <div className="flex gap-4 text-xs flex-wrap">
              {(['S2','S1','S3','S4'] as const).map((s) => {
                const count = stageSummary[s as keyof typeof stageSummary] as number;
                const pct = stageSummary[`${s}_pct` as keyof typeof stageSummary] as number;
                const labels: Record<string,string> = {
                  S2:'Advancing', S1:'Basing', S3:'Topping', S4:'Declining',
                };
                const text: Record<string,string> = {
                  S2:'text-emerald-300', S1:'text-blue-300',
                  S3:'text-amber-300',   S4:'text-rose-300',
                };
                return (
                  <div key={s} className="flex items-center gap-1.5">
                    <span className={cn('font-bold', text[s])}>{s}</span>
                    <span className="text-slate-400">{labels[s]}</span>
                    <span className="font-mono text-slate-300">{count} <span className="text-slate-500">({pct}%)</span></span>
                  </div>
                );
              })}
              <span className="ml-2 text-slate-500 text-[10px]">{stageSummary.labeled}/{stageSummary.total} classified</span>
            </div>
          </div>
        </SectionCard>
      )}

      {/* Filter rail + constituent table */}
      <SectionCard title="Constituents">
        <div className="flex gap-6">
          <TechFilterRail active={activeFilters} onToggle={toggleFilter} />

          <div className="min-w-0 flex-1">
            {/* Active filter chips */}
            {activeChips.length > 0 && (
              <div className="mb-3 flex flex-wrap items-center gap-2">
                <span className="text-[10px] uppercase tracking-widest text-slate-500">Active</span>
                {activeChips.map((chip) => (
                  <button
                    key={chip.key}
                    type="button"
                    onClick={() => toggleFilter(chip.key)}
                    className="flex items-center gap-1.5 rounded-full border border-slate-700 bg-slate-800/80 px-2.5 py-1 text-[10px] font-semibold text-slate-300 hover:border-slate-500 transition-colors"
                  >
                    {chip.cond}
                    <span className="text-slate-500 hover:text-rose-400">×</span>
                  </button>
                ))}
                <span className="ml-auto font-mono text-[11px] text-slate-500">
                  {visibleRows.length} of {allConstituents.length} match
                </span>
              </div>
            )}

            <ConstituentTable
              rows={visibleRows}
              hiddenCount={hiddenCount}
              activeFilters={activeFilters}
              onShowAll={() => setShowAll(true)}
            />
          </div>
        </div>
      </SectionCard>
    </PageFrame>
  );
}
