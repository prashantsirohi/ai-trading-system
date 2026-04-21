import { useEffect, useState } from 'react';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import type { PatternResponse } from '@/types/api';
import { getPatterns } from '@/lib/api/patterns';

export default function PatternsPage() {
  const [data, setData] = useState<PatternResponse | null>(null);

  useEffect(() => {
    getPatterns().then(setData);
  }, []);

  if (!data) return <div className="text-slate-400">Loading...</div>;

  return (
    <PageFrame
      title="Patterns"
      description="Monitor cup & handle, round bottom, and related pattern setups."
    >
      <SectionCard title="Pattern Queue">
        <div className="space-y-3">
          {data.rows.map((row) => (
            <div key={row.symbol} className="rounded-2xl border border-slate-800 bg-slate-950/60 p-4">
              <div className="font-semibold">{row.symbol}</div>
              <div className="mt-1 text-sm text-slate-400">{row.pattern} • Tier {row.tier}</div>
            </div>
          ))}
        </div>
      </SectionCard>
    </PageFrame>
  );
}
