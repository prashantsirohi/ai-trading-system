import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import { usePatterns } from '@/lib/queries';

export default function PatternsPage() {
  const { data, isLoading, error, refetch } = usePatterns();

  return (
    <PageFrame
      title="Patterns"
      description="Monitor cup & handle, round bottom, and related pattern setups."
    >
      <SectionCard title="Pattern Queue">
        {isLoading ? (
          <CardSkeleton />
        ) : error ? (
          <ErrorStateView
            error={`Failed to load patterns: ${error.message}`}
            onRetry={() => refetch()}
          />
        ) : !data?.rows?.length ? (
          <EmptyState message="No pattern candidates queued." />
        ) : (
          <div className="space-y-3">
            {data.rows.map((row) => (
              <div
                key={row.symbol}
                className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4"
              >
                <div className="font-semibold">{row.symbol}</div>
                <div className="mt-1 text-sm text-slate-400">
                  {row.pattern} • Tier {row.tier}
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionCard>
    </PageFrame>
  );
}
