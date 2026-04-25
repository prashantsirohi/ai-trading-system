import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import { useSectors } from '@/lib/queries';
import SectorStrengthChart from '@/components/charts/SectorStrengthChart';

export default function SectorsPage() {
  const { data, isLoading, error, refetch } = useSectors();

  return (
    <PageFrame
      title="Sectors"
      description="Track leadership rotation and drill into the strongest groups."
    >
      <SectionCard title="Sector Strength Chart">
        {isLoading ? (
          <CardSkeleton />
        ) : error ? (
          <ErrorStateView
            error={`Failed to load sectors: ${error.message}`}
            onRetry={() => refetch()}
          />
        ) : !data?.sectors?.length ? (
          <EmptyState message="No sector data available." />
        ) : (
          <SectorStrengthChart rows={data.sectors} />
        )}
      </SectionCard>
    </PageFrame>
  );
}
