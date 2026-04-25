import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import { useShadow } from '@/lib/queries';

export default function ShadowPage() {
  const { data, isLoading, error, refetch } = useShadow();

  return (
    <PageFrame
      title="Shadow"
      description="Compare shadow outputs, drift state, and agreement against the technical core."
    >
      <SectionCard title="Shadow Registry">
        {isLoading ? (
          <CardSkeleton />
        ) : error ? (
          <ErrorStateView
            error={`Failed to load shadow: ${error.message}`}
            onRetry={() => refetch()}
          />
        ) : !data?.rows?.length ? (
          <EmptyState message="No shadow models registered." />
        ) : (
          <div className="space-y-3">
            {data.rows.map((row) => (
              <div
                key={row.model}
                className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4"
              >
                <div className="flex items-center justify-between">
                  <span className="font-semibold">{row.model}</span>
                  <span className="text-sm text-slate-300">{row.status}</span>
                </div>
                <div className="mt-1 text-sm text-slate-400">
                  {row.date} • Agreement {row.agreement} • Drift {row.drift}
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionCard>
    </PageFrame>
  );
}
