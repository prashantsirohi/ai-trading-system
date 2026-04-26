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

  return (
    <PageFrame
      title="Sectors"
      description="Leadership rotation, capital flow, breadth, and per-sector drill-down."
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

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <SectionCard
              title="Sector Leadership"
              description="Capital flow + breadth proxy with per-sector ranked counts."
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
              description="Synthetic D-5 → D-1 dot grid using rolling RS columns."
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
