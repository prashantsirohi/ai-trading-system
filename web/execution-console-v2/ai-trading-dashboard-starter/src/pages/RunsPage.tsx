import { useEffect, useState } from 'react';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import type { RunsResponse } from '@/types/api';
import { getRuns } from '@/lib/api/runs';

export default function RunsPage() {
  const [data, setData] = useState<RunsResponse | null>(null);

  useEffect(() => {
    getRuns().then(setData);
  }, []);

  if (!data) return <div className="text-slate-400">Loading...</div>;

  return (
    <PageFrame
      title="Runs"
      description="Audit the latest pipeline attempts, statuses, and durations."
    >
      <SectionCard title="Run Timeline">
        <div className="space-y-3">
          {data.stages.map((stage, idx) => (
            <div key={stage.stage} className="flex items-center gap-4 rounded-2xl border border-slate-800 bg-slate-950/60 p-4">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-slate-800">{idx + 1}</div>
              <div className="flex-1">
                <div className="font-semibold">{stage.stage}</div>
                <div className="text-sm text-slate-400">{stage.duration}</div>
              </div>
              <div className="text-sm text-emerald-400">{stage.status}</div>
            </div>
          ))}
        </div>
      </SectionCard>
    </PageFrame>
  );
}
