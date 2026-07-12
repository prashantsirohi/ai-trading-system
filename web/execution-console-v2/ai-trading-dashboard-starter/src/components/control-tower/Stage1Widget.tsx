import { Link } from 'react-router-dom';
import EmptyState from '@/components/common/EmptyState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import SectionCard from '@/components/common/SectionCard';
import StatusBadge from '@/components/common/StatusBadge';
import { useStage1Summary } from '@/lib/queries';

export default function Stage1Widget() {
  const query = useStage1Summary();
  if (query.isLoading) return <CardSkeleton />;
  if (query.error || !query.data) return <SectionCard title="Stage-1 Emerging Leaders"><EmptyState message="Stage-1 operator summary is currently unavailable." /></SectionCard>;
  const data = query.data;
  const metrics = [['Active', data.active_count], ['Breakout Ready', data.breakout_ready_count], ['Promotion Pending', data.promotion_pending_count], ['Progressions Today', data.progressions_today], ['Regressions Today', data.regressions_today]];
  return <SectionCard title="Stage-1 Emerging Leaders" description={`Latest lifecycle date: ${data.as_of ?? 'unavailable'}`}>
    <div className="grid grid-cols-2 gap-3 md:grid-cols-5">{metrics.map(([label, value]) => <div key={String(label)} className="rounded-lg border border-slate-800 bg-slate-900/60 p-3"><div className="text-[10px] uppercase text-slate-500">{label}</div><div className="mt-1 text-2xl text-white">{value}</div></div>)}</div>
    <div className="mt-4 overflow-x-auto"><table className="w-full text-left text-xs"><thead className="uppercase text-slate-500"><tr>{['Symbol','Lifecycle','Score','Emerging Rank','Golden Cross','Pivot Distance'].map(h => <th className="px-2 py-2" key={h}>{h}</th>)}</tr></thead><tbody>{data.top_emerging_candidates.map(row => <tr className="border-t border-slate-800" key={String(row.symbol_id)}><td className="px-2 py-2 font-semibold text-cyan-300">{String(row.symbol_id ?? '—')}</td><td className="px-2 py-2"><StatusBadge status={String(row.stage1_lifecycle_state ?? '')} label={String(row.stage1_lifecycle_state ?? '—')} /></td><td className="px-2 py-2">{String(row.stage1_maturity_score ?? '—')}</td><td className="px-2 py-2">{String(row.stage1_emerging_rank ?? '—')}</td><td className="px-2 py-2">{String(row.golden_cross_status ?? '—')}</td><td className="px-2 py-2">{row.distance_to_pivot_pct == null ? '—' : `${Number(row.distance_to_pivot_pct).toFixed(1)}%`}</td></tr>)}</tbody></table></div>
    <Link className="mt-4 inline-block text-sm font-medium text-cyan-300" to="/investigator/stage1">View all Stage-1 candidates →</Link>
  </SectionCard>;
}
