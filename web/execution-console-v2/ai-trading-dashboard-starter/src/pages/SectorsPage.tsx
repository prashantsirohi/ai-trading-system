import { useEffect, useState } from 'react';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import type { SectorResponse } from '@/types/api';
import { getSectors } from '@/lib/api/sectors';
import SectorStrengthChart from '@/components/charts/SectorStrengthChart';

export default function SectorsPage() {
  const [data, setData] = useState<SectorResponse | null>(null);

  useEffect(() => {
    getSectors().then(setData);
  }, []);

  if (!data) return <div className="text-slate-400">Loading...</div>;

  return (
    <PageFrame
      title="Sectors"
      description="Track leadership rotation and drill into the strongest groups."
    >
      <SectionCard title="Sector Strength Chart">
        <SectorStrengthChart rows={data.sectors} />
      </SectionCard>
    </PageFrame>
  );
}
