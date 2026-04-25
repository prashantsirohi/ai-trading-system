import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { TableSkeleton } from '@/components/common/LoadingSkeleton';
import type { StockRow } from '@/types/dashboard';
import { useRanking } from '@/lib/queries';
import RankingTable from '@/components/tables/RankingTable';
import SymbolDetailDrawer from '@/components/drawers/SymbolDetailDrawer';
import { useState } from 'react';

function RankingContent() {
  const [selectedRow, setSelectedRow] = useState<StockRow | null>(null);

  const { data, isLoading, error, refetch } = useRanking();

  if (isLoading) {
    return (
      <PageFrame
        title="Ranking"
        description="Review strongest candidates with sortable factor-aware rows."
      >
        <SectionCard title="Ranked Signals">
          <TableSkeleton rows={10} />
        </SectionCard>
      </PageFrame>
    );
  }

  if (error) {
    return (
      <PageFrame
        title="Ranking"
        description="Review strongest candidates with sortable factor-aware rows."
      >
        <SectionCard title="Ranked Signals">
          <ErrorStateView
            error={`Failed to load ranking: ${error.message}`}
            onRetry={() => refetch()}
          />
        </SectionCard>
      </PageFrame>
    );
  }

  if (!data?.rows?.length) {
    return (
      <PageFrame
        title="Ranking"
        description="Review strongest candidates with sortable factor-aware rows."
      >
        <SectionCard title="Ranked Signals">
          <EmptyState message="No ranked signals available" />
        </SectionCard>
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Ranking"
      description="Review strongest candidates with sortable factor-aware rows."
    >
      <SectionCard title="Ranked Signals">
        <RankingTable
          rows={data.rows}
          onSelectRow={setSelectedRow}
          selectedSymbol={selectedRow?.symbol ?? null}
        />
      </SectionCard>
      <SymbolDetailDrawer 
        row={selectedRow} 
        open={selectedRow !== null} 
        onClose={() => setSelectedRow(null)} 
      />
    </PageFrame>
  );
}

export default function RankingPage() {
  return (
    <PageErrorBoundary title="Ranking" description="Failed to load ranking page">
      <RankingContent />
    </PageErrorBoundary>
  );
}