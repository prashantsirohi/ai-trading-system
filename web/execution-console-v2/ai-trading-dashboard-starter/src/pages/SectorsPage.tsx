/**
 * Sectors view (PR #9).
 *
 * Stacks an Early-Leader banner on top of a leadership chart, a rotation
 * heatmap, and a drill-down panel for the selected sector. Both the chart
 * and the drill-down composite ranking-feed counts to give per-sector
 * constituent / breakout context without a new endpoint.
 */
import { useEffect, useMemo, useState } from 'react';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import EarlyLeaderBanner from '@/components/sectors/EarlyLeaderBanner';
import SectorLeadershipChart from '@/components/sectors/SectorLeadershipChart';
import SectorRotationHeatmap from '@/components/sectors/SectorRotationHeatmap';
import SectorDrilldown from '@/components/sectors/SectorDrilldown';
import SectorValuationTable from '@/components/sectors/SectorValuationTable';
import SectorEarningsLeadershipTable from '@/components/sectors/SectorEarningsLeadershipTable';
import { useRanking, useSectors } from '@/lib/queries';

export default function SectorsPage() {
  const sectorsQuery = useSectors();
  const rankingQuery = useRanking();

  const sectors = sectorsQuery.data?.sectors ?? [];
  const rankedRows = rankingQuery.data?.rows ?? [];

  const [selectedSector, setSelectedSector] = useState<string | null>(null);

  // Auto-select the top sector when sectors first load.
  useEffect(() => {
    if (selectedSector === null && sectors.length > 0) {
      setSelectedSector(sectors[0].sector);
    }
  }, [sectors, selectedSector]);

  const selected = useMemo(
    () => sectors.find((s) => s.sector === selectedSector) ?? null,
    [sectors, selectedSector],
  );
  const earningsRows = useMemo(
    () => sectors.filter((sector) => sector.sectorEarningsGrowthScore !== null && sector.sectorEarningsGrowthScore !== undefined),
    [sectors],
  );
  const valuationRows = useMemo(
    () => sectors.filter((sector) => sector.sectorPeTtm !== null && sector.sectorPeTtm !== undefined),
    [sectors],
  );
  const latestEarningsDate = useMemo(
    () => latestDate(earningsRows.map((sector) => sector.earningsReportDate)),
    [earningsRows],
  );
  const latestValuationDate = useMemo(
    () => latestDate(valuationRows.map((sector) => sector.valuationDate)),
    [valuationRows],
  );

  return (
    <PageFrame
      title="Sectors"
      description="Rotation, breadth, and constituent drill-down."
      compactHeader
    >
      {sectorsQuery.isLoading ? (
        <CardSkeleton />
      ) : sectorsQuery.error ? (
        <ErrorStateView
          error={`Failed to load sectors: ${sectorsQuery.error.message}`}
          onRetry={() => sectorsQuery.refetch()}
        />
      ) : sectors.length === 0 ? (
        <EmptyState message="No sector data available." />
      ) : (
        <>
          <EarlyLeaderBanner sectors={sectors} />

          <SectionCard
            title="Sector Earnings Leadership"
            description="Quarterly aggregate growth, breadth, and margin expansion from Screener fundamentals."
            collapsible
            meta={<Availability rows={earningsRows.length} date={latestEarningsDate} />}
          >
            <SectorEarningsLeadershipTable
              sectors={sectors}
              selected={selectedSector}
              onSelect={setSelectedSector}
            />
          </SectionCard>

          <SectionCard
            title="Sector Valuation"
            description="Aggregate PE from market cap divided by aggregate TTM earnings."
            collapsible
            meta={<Availability rows={valuationRows.length} date={latestValuationDate} />}
          >
            <SectorValuationTable
              sectors={sectors}
              selected={selectedSector}
              onSelect={setSelectedSector}
            />
          </SectionCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <SectionCard
              title="Stage Heatmap"
              description="S1-S4 stock counts by sector."
            >
              <SectorLeadershipChart
                sectors={sectors}
                rankedRows={rankedRows}
                selected={selectedSector}
                onSelect={setSelectedSector}
              />
            </SectionCard>

            <SectionCard
              title="Rotation Heatmap"
              description="Rolling RS snapshots; daily D-5 history is not available yet."
            >
              <SectorRotationHeatmap
                sectors={sectors}
                selected={selectedSector}
                onSelect={setSelectedSector}
              />
            </SectionCard>
          </div>

          {selected ? (
            <SectionCard title="Drill-down">
              <SectorDrilldown sector={selected} rankedRows={rankedRows} />
            </SectionCard>
          ) : null}
        </>
      )}
    </PageFrame>
  );
}

function Availability({ rows, date }: { rows: number; date: string | null }) {
  const hasRows = rows > 0;
  return (
    <span className="inline-flex items-center gap-1 whitespace-nowrap rounded-full border border-slate-700 bg-slate-950 px-2 py-1 font-medium text-slate-300">
      <span className={hasRows ? 'text-emerald-300' : 'text-amber-300'}>
        {hasRows ? `${rows} rows` : 'No rows'}
      </span>
      {date ? <span className="text-slate-500">{date}</span> : null}
    </span>
  );
}

function latestDate(values: Array<string | null | undefined>): string | null {
  const dates = values.filter(Boolean).sort();
  return dates.length ? dates[dates.length - 1] ?? null : null;
}
