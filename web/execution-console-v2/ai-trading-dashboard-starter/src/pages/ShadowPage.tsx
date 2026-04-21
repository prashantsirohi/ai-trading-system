import { useEffect, useState } from 'react';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import type { ShadowResponse } from '@/types/api';
import { getShadow } from '@/lib/api/shadow';

export default function ShadowPage() {
  const [data, setData] = useState<ShadowResponse | null>(null);

  useEffect(() => {
    getShadow().then(setData);
  }, []);

  if (!data) return <div className="text-slate-400">Loading...</div>;

  return (
    <PageFrame
      title="Shadow"
      description="Compare shadow outputs, drift state, and agreement against the technical core."
    >
      <SectionCard title="Shadow Registry">
        <div className="space-y-3">
          {data.rows.map((row) => (
            <div key={row.model} className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4">
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
      </SectionCard>
    </PageFrame>
  );
}
