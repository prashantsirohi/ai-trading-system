import { ResponsiveContainer, BarChart, Bar, CartesianGrid, XAxis, YAxis, Tooltip } from 'recharts';
import type { SectorScore } from '@/types/dashboard';

export default function SectorStrengthChart({ rows }: { rows: SectorScore[] }) {
  return (
    <div className="h-80">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={rows} layout="vertical" margin={{ left: 12 }}>
          <CartesianGrid stroke="#1e293b" horizontal={false} />
          <XAxis type="number" tick={{ fill: '#94a3b8' }} />
          <YAxis type="category" dataKey="sector" width={90} tick={{ fill: '#cbd5e1' }} />
          <Tooltip contentStyle={{ background: '#020617', border: '1px solid #1e293b', borderRadius: 16 }} />
          <Bar dataKey="score" radius={[0, 10, 10, 0]} fill="#22c55e" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
