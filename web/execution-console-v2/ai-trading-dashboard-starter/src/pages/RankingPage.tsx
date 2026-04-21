import { useQuery } from '@tanstack/react-query';
import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import { TableSkeleton } from '@/components/common/LoadingSkeleton';
import type { RankingResponse } from '@/types/api';
import type { StockRow } from '@/types/dashboard';
import { getRanking } from '@/lib/api/ranking';
import RankingTable from '@/components/tables/RankingTable';
import SymbolDetailDrawer from '@/components/drawers/SymbolDetailDrawer';
import { useState } from 'react';

function RankingContent() {
  const [selectedRow, setSelectedRow] = useState<StockRow | null>(null);

  const { data, isLoading, error, refetch } = useQuery<RankingResponse>({
    queryKey: ['ranking'],
    queryFn: getRanking,
  });

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
          <div className="space-y-3">
            <p className="text-sm text-rose-300">
              Failed to load ranking: {error.message}
            </p>
            <button
              onClick={() => refetch()}
              className="rounded-md border border-slate-700 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800"
            >
              Retry
            </button>
          </div>
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
          <p className="text-sm text-slate-400">No ranked signals available</p>
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