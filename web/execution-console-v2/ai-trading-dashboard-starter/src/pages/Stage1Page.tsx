import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import StatusBadge from '@/components/common/StatusBadge';
import { useStage1Current, useStage1Detail, useStage1Exits, useStage1Summary, useStage1Transitions } from '@/lib/queries';
import type { Stage1Params, Stage1Row } from '@/lib/api/stage1';

const text = (v: unknown, fallback = '—') => v === undefined || v === null || v === '' ? fallback : String(v).replace(/_/g, ' ');
const number = (v: unknown, digits = 0) => Number.isFinite(Number(v)) ? Number(v).toFixed(digits) : '—';
const delta = (v: unknown) => Number.isFinite(Number(v)) ? `${Number(v) > 0 ? '+' : ''}${Number(v).toFixed(1)}` : '—';
const pct = (v: unknown) => Number.isFinite(Number(v)) ? `${Number(v).toFixed(1)}%` : '—';
const priority = (v: unknown) => ({ CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 }[String(v)] ?? 9);

function SummaryCards({ summary, setLifecycle }: { summary: Record<string, unknown>; setLifecycle: (state?: string) => void }) {
  const cards = [
    ['Active', 'active_count', undefined], ['Breakout Ready', 'breakout_ready_count', 'BREAKOUT_READY'],
    ['Late Stage-1', 'late_stage1_count', 'LATE_STAGE1'], ['Accumulating', 'accumulating_count', 'ACCUMULATING'],
    ['Promotion Pending', 'promotion_pending_count', 'PROMOTION_PENDING'], ['Progressions Today', 'progressions_today', undefined],
    ['Regressions Today', 'regressions_today', 'REGRESSED'],
  ] as const;
  return <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-7">{cards.map(([label, key, state]) => (
    <button key={key} type="button" onClick={() => setLifecycle(state)} className="rounded-xl border border-slate-800 bg-slate-900/70 p-3 text-left hover:border-cyan-500/60">
      <div className="text-[10px] uppercase tracking-wider text-slate-500">{label}</div><div className="mt-1 text-2xl font-semibold text-slate-100">{number(summary[key])}</div>
    </button>))}</div>;
}

const columns = ['Symbol', 'Lifecycle', 'Substate', 'Stage-1 Score', 'Δ 5D', 'Δ 20D', 'Emerging Rank', 'Rank Improve 20D', 'Pattern', 'Golden Cross', 'MA Gap', 'Pivot Distance', 'Days in State', 'Operator Status'];
function CandidateTable({ rows, onOpen }: { rows: Stage1Row[]; onOpen: (symbol: string) => void }) {
  if (!rows.length) return <EmptyState message="No Stage-1 candidates match the current filters." />;
  return <div className="overflow-x-auto"><table className="min-w-[1500px] text-left text-xs"><thead className="uppercase text-slate-500"><tr>{columns.map(c => <th className="px-3 py-2" key={c}>{c}</th>)}</tr></thead>
    <tbody className="divide-y divide-slate-800">{rows.map(row => <tr key={String(row.symbol_id)} className="text-slate-200 hover:bg-slate-900/70">
      <td className="px-3 py-2"><button className="font-semibold text-cyan-300" onClick={() => onOpen(String(row.symbol_id))}>{text(row.symbol_id)}</button></td>
      <td className="px-3 py-2"><StatusBadge status={text(row.stage1_lifecycle_state)} label={text(row.stage1_lifecycle_state)} /></td><td className="px-3 py-2">{text(row.stage1_substate)}</td>
      <td className="px-3 py-2">{number(row.stage1_maturity_score)}</td><td className="px-3 py-2">{delta(row.stage1_score_delta_5d)}</td><td className="px-3 py-2">{delta(row.stage1_score_delta_20d)}</td>
      <td className="px-3 py-2">{number(row.stage1_emerging_rank)}</td><td className="px-3 py-2">{delta(row.emerging_rank_improvement_20d)}</td><td className="px-3 py-2">{text(row.pattern_promotion_state)}</td>
      <td className="px-3 py-2">{text(row.golden_cross_status)}</td><td className="px-3 py-2">{pct(row.sma50_sma200_gap_pct)}</td><td className="px-3 py-2">{pct(row.distance_to_pivot_pct)}</td>
      <td className="px-3 py-2">{number(row.stage1_days_in_lifecycle_state)}</td><td className="px-3 py-2"><StatusBadge status={text(row.operator_status)} label={text(row.operator_status)} /></td>
    </tr>)}</tbody></table></div>;
}

function DetailDrawer({ symbol, close }: { symbol: string; close: () => void }) {
  const { data, isLoading, error } = useStage1Detail(symbol); const current = data?.current;
  const meters = ['price_structure_score', 'volume_delivery_score', 'sector_support_score', 'buyer_fingerprint_score', 'ranking_overlay_score'];
  return <div className="fixed inset-0 z-50 flex justify-end bg-black/60" onMouseDown={e => e.target === e.currentTarget && close()}><aside className="h-full w-full max-w-2xl overflow-y-auto border-l border-slate-700 bg-slate-950 p-5">
    <div className="mb-5 flex items-start justify-between"><div><div className="text-2xl font-semibold text-white">{symbol}</div><div className="mt-2 flex gap-2"><StatusBadge status={text(current?.stage1_lifecycle_state)} label={text(current?.stage1_lifecycle_state)} /><StatusBadge status={text(current?.operator_status)} label={text(current?.operator_status)} /></div></div><button onClick={close} className="text-slate-400">Close</button></div>
    {isLoading ? <CardSkeleton /> : error ? <ErrorStateView error={error.message} /> : !current ? <EmptyState message="No Stage-1 history is available for this symbol." /> : <div className="space-y-4">
      <SectionCard title="Current state"><div className="grid grid-cols-2 gap-3 text-sm">{([['Substate', current.stage1_substate], ['Evaluation', current.stage1_evaluation_status], ['Promotion eligibility', current.promotion_eligibility], ['Action', current.operator_action]] as [string, unknown][]).map(([k,v]) => <div key={k}><div className="text-xs text-slate-500">{k}</div><div className="text-slate-200">{text(v)}</div></div>)}</div><p className="mt-3 text-sm text-slate-300">{text(current.operator_reason)}</p></SectionCard>
      <SectionCard title="Progress"><div className="grid grid-cols-3 gap-3 text-sm">{([['First seen', current.stage1_first_seen_date], ['Days in state', current.stage1_days_in_lifecycle_state], ['Score peak', current.stage1_score_peak], ['Current score', current.stage1_maturity_score], ['20D score change', current.stage1_score_delta_20d], ['Best rank', current.stage1_emerging_rank_best]] as [string, unknown][]).map(([k,v]) => <div key={k}><div className="text-xs text-slate-500">{k}</div><div>{text(v)}</div></div>)}</div></SectionCard>
      <SectionCard title="Structure">{meters.map(key => <div className="mb-3" key={key}><div className="mb-1 flex justify-between text-xs text-slate-400"><span>{text(key)}</span><span>{number(current[key])}</span></div><div className="h-2 rounded bg-slate-800"><div className="h-2 rounded bg-cyan-500" style={{width: `${Math.min(100, Math.max(0, Number(current[key]) || 0))}%`}} /></div></div>)}</SectionCard>
      <SectionCard title="Technical progression"><div className="grid grid-cols-2 gap-3 text-sm">{([['Golden Cross', current.golden_cross_status], ['MA gap', pct(current.sma50_sma200_gap_pct)], ['MA gap Δ20D', pct(current.sma50_sma200_gap_delta_20d)], ['Pattern', current.pattern_promotion_state], ['Pivot distance', pct(current.distance_to_pivot_pct)]] as [string, unknown][]).map(([k,v]) => <div key={k}><div className="text-xs text-slate-500">{k}</div><div>{text(v)}</div></div>)}</div></SectionCard>
      <SectionCard title="Lifecycle timeline">{!data?.transitions.length ? <EmptyState message="No lifecycle transitions are persisted for this symbol." /> : <div className="space-y-3">{[...data.transitions].reverse().map((event, i) => <div className="border-l-2 border-cyan-600 pl-3 text-sm" key={i}><div className="text-xs text-slate-500">{text(event.trade_date)}</div><div>{text(event.transition_summary)}</div><div className="text-slate-400">Score {number(event.stage1_score_before)} → {number(event.stage1_score_after)} · Rank {number(event.emerging_rank_before)} → {number(event.emerging_rank_after)}</div></div>)}</div>}</SectionCard>
    </div>}
  </aside></div>;
}

export default function Stage1Page() {
  const [filters, setFilters] = useState<Stage1Params>({ limit: 50, offset: 0 }); const [symbol, setSymbol] = useState<string>();
  const summary = useStage1Summary(); const current = useStage1Current(filters); const queueQuery = useStage1Current({ limit: 100, sort_by: 'operator_priority' }); const transitions = useStage1Transitions(); const exits = useStage1Exits();
  const queue = useMemo(() => (queueQuery.data?.rows ?? []).filter(r => r.operator_queue_eligible).sort((a,b) => priority(a.operator_priority)-priority(b.operator_priority) || Number(a.stage1_emerging_rank ?? 9999)-Number(b.stage1_emerging_rank ?? 9999)), [queueQuery.data]);
  const setLifecycle = (state?: string) => setFilters(f => ({...f, lifecycle_state: state, offset: 0}));
  return <PageFrame title="Stage-1 Emerging Leaders" description="Research-only lifecycle workflow: what needs attention, what changed, and why candidates weakened." headerAside={<Link to="/investigator" className="text-sm text-cyan-300">Back to Investigator</Link>}>
    <div className="space-y-4">{summary.isLoading ? <CardSkeleton /> : summary.error ? <ErrorStateView error={summary.error.message} onRetry={() => summary.refetch()} /> : <SummaryCards summary={(summary.data ?? {}) as unknown as Record<string, unknown>} setLifecycle={setLifecycle} />}
      <SectionCard title="Operator Action Queue" description="Promotion, breakout, advanced regression, and data-review attention.">{queueQuery.isLoading ? <CardSkeleton /> : <CandidateTable rows={queue} onOpen={setSymbol} />}</SectionCard>
      <SectionCard title="Emerging Leaders"><div className="mb-3 flex flex-wrap gap-2"><input aria-label="Search symbols" placeholder="Search symbol" value={filters.search ?? ''} onChange={e => setFilters(f => ({...f, search:e.target.value, offset:0}))} className="rounded border border-slate-700 bg-slate-950 px-3 py-2 text-sm" />
        <input aria-label="Sector" placeholder="Sector" value={filters.sector ?? ''} onChange={e => setFilters(f => ({...f, sector:e.target.value, offset:0}))} className="w-32 rounded border border-slate-700 bg-slate-950 px-3 py-2 text-sm" />
        {['BREAKOUT_READY','LATE_STAGE1','ACCUMULATING','REGRESSED'].map(s => <button key={s} onClick={() => setLifecycle(s)} className="rounded border border-slate-700 px-2 py-1 text-xs text-slate-300">{text(s)}</button>)}
        {[['operator_status', 'Operator status', ['ACT_NOW','WATCH_CLOSELY','DEVELOPING','MONITOR','REGRESSED','STALE','DATA_PENDING']], ['operator_priority', 'Priority', ['CRITICAL','HIGH','MEDIUM','LOW']], ['golden_cross_status', 'Golden Cross', ['APPROACHING','IMMINENT','CROSSED_RECENTLY','CROSSED_ESTABLISHED','FAILED_CROSS']], ['pattern_promotion_state', 'Pattern', ['CONFIRMED','BREAKOUT_ATTEMPT','PENDING_3D','FAILED']]].map(([key, label, values]) => <select aria-label={String(label)} key={String(key)} value={String(filters[key as keyof Stage1Params] ?? '')} onChange={e => setFilters(f => ({...f, [String(key)]:e.target.value || undefined, offset:0}))} className="rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs"><option value="">{String(label)}: All</option>{(values as string[]).map(v => <option key={v} value={v}>{text(v)}</option>)}</select>)}
        <select aria-label="Promotion eligibility" value={filters.promotion_eligibility === undefined ? '' : String(filters.promotion_eligibility)} onChange={e => setFilters(f => ({...f, promotion_eligibility:e.target.value === '' ? undefined : e.target.value === 'true', offset:0}))} className="rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs"><option value="">Promotion: All</option><option value="true">Eligible</option><option value="false">Not eligible</option></select>
        <button onClick={() => setFilters({limit:50, offset:0})} className="text-xs text-cyan-300">Clear filters</button></div>
        {current.error ? <ErrorStateView error={current.error.message} onRetry={() => current.refetch()} /> : current.isLoading ? <CardSkeleton /> : <><CandidateTable rows={current.data?.rows ?? []} onOpen={setSymbol} /><div className="mt-3 flex items-center justify-between text-xs text-slate-400"><span>{current.data?.total ?? 0} candidates</span><div className="flex gap-2"><button disabled={!filters.offset} onClick={() => setFilters(f => ({...f, offset:Math.max(0, Number(f.offset ?? 0)-Number(f.limit ?? 50))}))} className="rounded border border-slate-700 px-3 py-1 disabled:opacity-40">Previous</button><button disabled={Number(filters.offset ?? 0)+Number(filters.limit ?? 50) >= Number(current.data?.total ?? 0)} onClick={() => setFilters(f => ({...f, offset:Number(f.offset ?? 0)+Number(f.limit ?? 50)}))} className="rounded border border-slate-700 px-3 py-1 disabled:opacity-40">Next</button></div></div></>}</SectionCard>
      <SectionCard title="Today's Transitions">{transitions.error ? <ErrorStateView error={transitions.error.message} /> : !transitions.data?.rows.length ? <EmptyState message="No Stage-1 transitions today." /> : <CandidateTable rows={transitions.data.rows.map(r => ({...r, stage1_lifecycle_state:String(r.to_lifecycle_state ?? ''), stage1_maturity_score:r.stage1_score_after, stage1_emerging_rank:r.emerging_rank_after}) as Stage1Row)} onOpen={setSymbol} />}</SectionCard>
      <SectionCard title="Regressions / Invalidations / Stale">{exits.error ? <ErrorStateView error={exits.error.message} /> : !exits.data?.rows.length ? <EmptyState message="No current regressions or exits." /> : <CandidateTable rows={exits.data.rows} onOpen={setSymbol} />}</SectionCard>
    </div>{symbol && <DetailDrawer symbol={symbol} close={() => setSymbol(undefined)} />}
  </PageFrame>;
}
