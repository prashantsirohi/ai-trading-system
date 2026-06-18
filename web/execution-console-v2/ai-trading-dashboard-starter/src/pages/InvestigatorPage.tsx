import { useMemo, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import StatusBadge from '@/components/common/StatusBadge';
import { useInvestigatorSnapshot } from '@/lib/queries';
import { cn } from '@/lib/utils/cn';

type Row = Record<string, unknown>;
type DrawerTab = 'thesis' | 'timeline' | 'price' | 'repeat' | 'trap' | 'factors' | 'actions';
type TrapFilter = { category: string; symbols: string[] } | null;

const FILTERS = [
  { key: 'repeat', label: 'Repeat >=3x' },
  { key: 'price', label: 'Price holding' },
  { key: 'rank', label: 'Rank improving' },
  { key: 'volume', label: 'Volume rising' },
  { key: 'trapFree', label: 'Trap-free' },
  { key: 'newToday', label: 'New today' },
  { key: 'stale', label: 'Stale >5d' },
] as const;

const VERDICT_TONES: Record<string, 'good' | 'warn' | 'bad' | 'neutral'> = {
  'High Conviction': 'good',
  Investigate: 'good',
  Watch: 'warn',
  'Trap Risk': 'bad',
  'Archive Candidate': 'warn',
  Avoid: 'bad',
};

function num(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function text(value: unknown, fallback = '-'): string {
  if (value === null || value === undefined || value === '') return fallback;
  return String(value).replace(/_/g, ' ');
}

function bool(value: unknown): boolean {
  return ['true', '1', 'yes'].includes(String(value ?? '').toLowerCase()) || value === true;
}

function pct(value: unknown, digits = 1): string {
  const parsed = num(value, Number.NaN);
  return Number.isFinite(parsed) ? `${parsed.toFixed(digits)}%` : '-';
}

function fixed(value: unknown, digits = 0): string {
  const parsed = num(value, Number.NaN);
  return Number.isFinite(parsed) ? parsed.toFixed(digits) : '-';
}

function hasValue(row: Row, key: string): boolean {
  return row[key] !== undefined && row[key] !== null && row[key] !== '';
}

function delta(value: unknown): string {
  const parsed = num(value);
  if (parsed === 0) return '0';
  return parsed > 0 ? `+${parsed}` : String(parsed);
}

function symbolOf(row: Row | null | undefined): string {
  return String(row?.symbol_id ?? row?.symbol ?? '');
}

function sortByScore(rows: Row[]): Row[] {
  return [...rows].sort((a, b) => {
    const scoreDelta = num(b.investigator_score, -Infinity) - num(a.investigator_score, -Infinity);
    if (scoreDelta !== 0) return scoreDelta;
    return symbolOf(a).localeCompare(symbolOf(b));
  });
}

function Metric({ label, value, deltaValue }: { label: string; value: unknown; deltaValue?: unknown }) {
  const deltaNum = num(deltaValue);
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/70 px-3 py-3">
      <div className="flex items-center justify-between gap-2">
        <div className="text-[10px] uppercase tracking-[0.12em] text-slate-500">{label}</div>
        {deltaValue !== undefined ? (
          <span className={cn('rounded px-1.5 py-0.5 text-[10px] font-semibold tabular-nums', deltaNum >= 0 ? 'bg-emerald-500/10 text-emerald-300' : 'bg-rose-500/10 text-rose-300')}>
            {delta(deltaValue)}
          </span>
        ) : null}
      </div>
      <div className="mt-1 text-2xl font-semibold tabular-nums text-slate-100">{String(value ?? 0)}</div>
    </div>
  );
}

function scoreParts(row: Row): Array<{ label: string; value: number }> {
  return [
    { label: 'Repeat', value: Math.max(0, Math.min(100, num(row.repeat_score ?? row.repeat_strength))) },
    { label: 'Price', value: Math.max(0, Math.min(100, (num(row.price_progression_pct ?? row.price_vs_first_trigger_pct) + 20) * 2.5)) },
    { label: 'Rank', value: Math.max(0, Math.min(100, 50 - num(row.rank_change_20d) * 1.5)) },
    { label: 'Volume/Delivery', value: Math.max(0, Math.min(100, num(row.volume_delivery_score) * 5)) },
    { label: 'Sector', value: Math.max(0, Math.min(100, num(row.sector_support_score) * 10)) },
    { label: 'Setup', value: Math.max(0, Math.min(100, num(row.trigger_quality_score) * 5)) },
    { label: 'Trap Penalty', value: Math.max(0, Math.min(100, bool(row.hard_trap_flag) ? 100 : bool(row.low_delivery_flag) ? 55 : num(row.price_progression_pct) < 0 ? 40 : 0)) },
  ];
}

function ScoreCell({ row }: { row: Row }) {
  const parts = scoreParts(row);
  const title = parts.map((part) => `${part.label}: ${part.value.toFixed(0)}`).join('\n');
  return (
    <span className="cursor-help border-b border-dotted border-slate-500 tabular-nums" title={title}>
      {fixed(row.investigator_score)}
    </span>
  );
}

function HealthRibbon({ data }: { data: Row }) {
  const stage = (data.stage_status ?? {}) as Record<string, unknown>;
  const rawSummary = (data.raw_summary ?? {}) as Row;
  return (
    <div className="flex flex-wrap items-center gap-2 rounded-lg border border-slate-800 bg-slate-950/70 px-3 py-2 text-xs text-slate-300">
      <span className="font-medium text-slate-100">Data: {text(data.run_date ?? rawSummary.run_date)}</span>
      <StatusBadge status={text(data.data_trust_status, 'unknown')} label={`Trust: ${text(data.data_trust_status, 'unknown')}`} />
      <StatusBadge status={text(stage.rank, 'unknown')} label={`Rank: ${text(stage.rank, 'unknown')}`} />
      <StatusBadge status={text(stage.investigator, 'unknown')} label={`Investigator: ${text(stage.investigator, 'unknown')}`} />
      <StatusBadge status={text(stage.publish, 'unknown')} label={`Publish: ${text(stage.publish, 'unknown')}`} />
    </div>
  );
}

function VerdictBadge({ value }: { value: unknown }) {
  const label = text(value, 'Watch');
  return <StatusBadge status={label} label={label} tone={VERDICT_TONES[label] ?? 'neutral'} />;
}

function ActionQueue({ rows, fallback, highConvictionCount, onOpen }: { rows: Row[]; fallback: Row[]; highConvictionCount: number; onOpen: (row: Row) => void }) {
  const display = rows.length > 0 ? rows : fallback;
  return (
    <SectionCard
      title="Action Queue"
      description={rows.length > 0 ? 'Top Investigator decisions ranked by score.' : 'No High Conviction today. Showing nearest watchlist candidates ranked by investigator score.'}
    >
      {highConvictionCount <= 0 ? (
        <div className="mb-3 space-y-1 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
          <p className="font-semibold">No High Conviction today. Showing nearest watchlist candidates ranked by investigator score.</p>
          <p>Reason: no candidate passed score &gt;=80 + volume confirmation + rank improvement.</p>
        </div>
      ) : null}
      {display.length === 0 ? (
        <EmptyState message="No action candidates available." />
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-xs">
            <thead className="uppercase text-slate-500">
              <tr>
                <th className="px-3 py-2">Symbol</th>
                <th className="px-3 py-2">Verdict</th>
                <th className="px-3 py-2">Reason</th>
                <th className="px-3 py-2 text-right">Score</th>
                <th className="px-3 py-2 text-right">Repeat</th>
                <th className="px-3 py-2 text-right">Price vs First</th>
                <th className="px-3 py-2 text-right">Rank Delta</th>
                <th className="px-3 py-2">Vol</th>
                <th className="px-3 py-2">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {display.slice(0, 20).map((row) => (
                <tr key={symbolOf(row)} className="text-slate-200 hover:bg-slate-800/35">
                  <td className="px-3 py-2 font-semibold">{symbolOf(row)}</td>
                  <td className="px-3 py-2"><VerdictBadge value={row.decision_verdict} /></td>
                  <td className="px-3 py-2 text-slate-300">{text(row.decision_reason)}</td>
                  <td className="px-3 py-2 text-right"><ScoreCell row={row} /></td>
                  <td className="px-3 py-2 text-right tabular-nums">{fixed(row.appearance_count_20d)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{pct(row.price_progression_pct ?? row.price_vs_first_trigger_pct)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fixed(row.rank_change_20d)}</td>
                  <td className="px-3 py-2">{text(row.volume_signal)}</td>
                  <td className="px-3 py-2">
                    <button type="button" className="rounded-md border border-slate-700 px-2 py-1 text-[11px] text-slate-200 hover:bg-slate-800" onClick={() => onOpen(row)}>
                      Open
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </SectionCard>
  );
}

function QualityBar({ label, value }: { label: string; value: unknown }) {
  const width = Math.max(0, Math.min(100, num(value)));
  return (
    <div>
      <div className="mb-1 flex justify-between text-[11px] font-semibold uppercase text-slate-300">
        <span>{label}</span>
        <span className="tabular-nums text-slate-100">{width.toFixed(0)}</span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-slate-800">
        <div className="h-full rounded-full bg-emerald-400" style={{ width: `${width}%` }} />
      </div>
    </div>
  );
}

function RepeatQualityPanel({ rows, onOpen }: { rows: Row[]; onOpen: (row: Row) => void }) {
  return (
    <SectionCard title="Repeat Quality" description="Persistence, price sustain, rank momentum, and volume confirmation.">
      {rows.length === 0 ? (
        <EmptyState message="No repeat quality rows available." />
      ) : (
        <div className="space-y-3">
          {rows.slice(0, 8).map((row) => (
            <button key={symbolOf(row)} type="button" className="block w-full rounded-lg border border-slate-800 bg-slate-950/45 p-3 text-left hover:border-slate-600" onClick={() => onOpen(row)}>
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="font-semibold text-slate-100">{symbolOf(row)}</div>
                  <div className="mt-1 text-xs text-slate-400">
                    Repeat {fixed(row.appearance_count_20d)}x | Price {pct(row.price_progression_pct)} | {text(row.rank_signal)} | {text(row.volume_signal)}
                  </div>
                </div>
                {bool(row.high_priority_repeat) ? <span className="rounded bg-emerald-500/10 px-2 py-1 text-[10px] font-semibold text-emerald-300">PRIORITY</span> : null}
              </div>
              <div className="mt-3 grid grid-cols-2 gap-2">
                <QualityBar label="Repeat" value={row.repeat_strength ?? row.repeat_score} />
                <QualityBar label="Price Sustain" value={row.price_sustain} />
                <QualityBar label="Rank Momentum" value={row.rank_momentum} />
                <QualityBar label="Volume Confirmation" value={row.volume_confirmation} />
              </div>
            </button>
          ))}
        </div>
      )}
    </SectionCard>
  );
}

function TrapRadar({ rows, activeFilter, onFilter }: { rows: Row[]; activeFilter: TrapFilter; onFilter: (filter: TrapFilter) => void }) {
  return (
    <SectionCard title="Trap Radar" description="What the Investigator is rejecting or flagging today.">
      {rows.length === 0 ? (
        <EmptyState message="No trap evidence available." />
      ) : (
        <div className="space-y-2">
          {rows.slice(0, 8).map((row) => {
            const category = text(row.trap_category);
            const symbols = Array.isArray(row.examples) ? row.examples.map(String) : [];
            const active = activeFilter?.category === category;
            return (
            <button
              key={category}
              type="button"
              className={cn('grid w-full grid-cols-[1fr_auto] gap-3 rounded-lg border p-3 text-left', active ? 'border-rose-400 bg-rose-500/10' : 'border-slate-800 bg-slate-950/45 hover:border-slate-600')}
              onClick={() => onFilter(active ? null : { category, symbols })}
            >
              <div>
                <div className="text-sm font-semibold text-slate-100">{category}</div>
                <div className="mt-1 text-xs text-slate-400">Examples: {symbols.length ? symbols.join(', ') : '-'}</div>
              </div>
              <div className="text-2xl font-semibold tabular-nums text-rose-300">{fixed(row.count)}</div>
            </button>
          );
          })}
        </div>
      )}
    </SectionCard>
  );
}

function FunnelChart({ rows }: { rows: Row[] }) {
  if (rows.length === 0) return <EmptyState message="No funnel data available." />;
  const max = Math.max(...rows.map((row) => num(row.count)), 1);
  return (
    <div className="space-y-2">
      {rows.map((row, index) => {
        const width = Math.max(5, (num(row.count) / max) * 100);
        const first = num(rows[0]?.count);
        const previous = index > 0 ? num(rows[index - 1]?.count) : first;
        const ofStart = first > 0 ? `${((num(row.count) / first) * 100).toFixed(0)}% of start` : '-';
        const fromPrevious = index > 0 && previous > 0 ? `${((num(row.count) / previous) * 100).toFixed(0)}% prev` : 'start';
        return (
          <div key={text(row.key)} className="space-y-1">
            <div className="flex justify-between text-xs">
              <span className="font-medium text-slate-300">{text(row.label)}</span>
              <span className="tabular-nums text-slate-400">{fixed(row.count)} | {ofStart} | {fromPrevious}</span>
            </div>
            <div className="h-6 overflow-hidden rounded bg-slate-950">
              <div className="h-full rounded bg-sky-500/70" style={{ width: `${width}%` }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

function ScatterPanel({ rows }: { rows: Row[] }) {
  if (rows.length === 0) return <EmptyState message="No repeat scatter data available." />;
  const data = rows.slice(0, 100).map((row) => ({
    symbol: symbolOf(row),
    repeat: num(row.appearance_count_20d),
    price: num(row.price_progression_pct ?? row.price_vs_first_trigger_pct),
    score: num(row.investigator_score),
    verdict: text(row.decision_verdict),
    rankChange: num(row.rank_change_20d),
    volume: text(row.volume_signal),
  }));
  const tooltip = ({ active, payload }: { active?: boolean; payload?: Array<{ payload?: Record<string, unknown> }> }) => {
    const point = payload?.[0]?.payload;
    if (!active || !point) return null;
    return (
      <div className="rounded-lg border border-slate-700 bg-slate-950 p-3 text-xs text-slate-200 shadow-xl">
        <div className="font-semibold text-white">{text(point.symbol)}</div>
        <div>Repeat: {fixed(point.repeat)}x</div>
        <div>Price vs first: {pct(point.price)}</div>
        <div>Rank change: {fixed(point.rankChange)}</div>
        <div>Volume: {text(point.volume)}</div>
        <div>Verdict: {text(point.verdict)}</div>
      </div>
    );
  };
  return (
    <div className="relative h-64">
      <div className="pointer-events-none absolute inset-3 z-10 grid grid-cols-2 grid-rows-2 text-[10px] font-semibold uppercase text-slate-500">
        <div className="self-start">Early momentum</div>
        <div className="justify-self-end self-start text-emerald-300">Best candidates</div>
        <div className="self-end">Ignore</div>
        <div className="justify-self-end self-end text-rose-300">Trap / distribution risk</div>
      </div>
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ left: 0, right: 16, top: 12, bottom: 8 }}>
          <CartesianGrid stroke="#1e293b" />
          <XAxis dataKey="repeat" name="Repeat" tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <YAxis dataKey="price" name="Price sustain" tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <Tooltip content={tooltip} />
          <Scatter data={data} fill="#38bdf8" isAnimationActive={false}>
            {data.map((point) => (
              <Cell key={point.symbol} fill={point.verdict === 'Trap Risk' ? '#fb7185' : point.verdict === 'High Conviction' ? '#34d399' : '#38bdf8'} />
            ))}
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}

function TrendBars({ rows }: { rows: Row[] }) {
  if (rows.length === 0) return <EmptyState message="No four-week trend data available." />;
  const data = rows.map((row) => ({ ...row, label: text(row.date ?? row.week) }));
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ left: 0, right: 16, top: 12, bottom: 8 }}>
          <CartesianGrid stroke="#1e293b" />
          <XAxis dataKey="label" tick={{ fill: '#94a3b8', fontSize: 10 }} />
          <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: 8 }} />
          <Bar dataKey="new" stackId="a" fill="#22c55e" isAnimationActive={false} />
          <Bar dataKey="repeat" stackId="a" fill="#38bdf8" isAnimationActive={false} />
          <Bar dataKey="improving" stackId="a" fill="#a78bfa" isAnimationActive={false} />
          <Bar dataKey="active" stackId="a" fill="#38bdf8" isAnimationActive={false} />
          <Bar dataKey="traps" stackId="a" fill="#fb7185" isAnimationActive={false} />
          <Bar dataKey="archived" stackId="a" fill="#64748b" isAnimationActive={false} />
          <Line type="monotone" dataKey="high_conviction" stroke="#facc15" strokeWidth={2} dot={false} isAnimationActive={false} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

function ActiveTable({ rows, filters, trapFilter, onClearTrapFilter, onToggleFilter, onOpen }: { rows: Row[]; filters: Set<string>; trapFilter: TrapFilter; onClearTrapFilter: () => void; onToggleFilter: (key: string) => void; onOpen: (row: Row) => void }) {
  return (
    <SectionCard title="Active Investigator List" description="Top 50 by Investigator score; use filters to narrow the queue.">
      <div className="mb-3 flex flex-wrap gap-2">
        {trapFilter ? (
          <button type="button" className="rounded-md border border-rose-500 bg-rose-500/10 px-2.5 py-1 text-xs text-rose-100" onClick={onClearTrapFilter}>
            Trap: {trapFilter.category} x
          </button>
        ) : null}
        {FILTERS.map((filter) => (
          <button
            key={filter.key}
            type="button"
            className={cn('rounded-md border px-2.5 py-1 text-xs', filters.has(filter.key) ? 'border-sky-500 bg-sky-500/10 text-sky-200' : 'border-slate-700 text-slate-300 hover:bg-slate-800')}
            onClick={() => onToggleFilter(filter.key)}
          >
            {filter.label}
          </button>
        ))}
      </div>
      {rows.length === 0 ? (
        <EmptyState message="No active rows match the selected filters." />
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-[1100px] text-left text-xs">
            <thead className="uppercase text-slate-500">
              <tr>
                {['Symbol', 'Verdict', 'Setup', 'Sector', 'Score', 'Repeat', 'Price vs First', 'Rank Change', 'Volume', 'Days Stale', 'Trap Flags', 'Last Seen', 'Action'].map((head) => (
                  <th key={head} className="px-3 py-2">{head}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {rows.slice(0, 50).map((row) => (
                <tr key={symbolOf(row)} className="text-slate-200 hover:bg-slate-800/35">
                  <td className="px-3 py-2 font-semibold">{symbolOf(row)}</td>
                  <td className="px-3 py-2"><VerdictBadge value={row.decision_verdict} /></td>
                  <td className="px-3 py-2">{text(row.setup ?? row.move_tag)}</td>
                  <td className="px-3 py-2">{text(row.sector)}</td>
                  <td className="px-3 py-2 text-right"><ScoreCell row={row} /></td>
                  <td className="px-3 py-2 text-right tabular-nums">{fixed(row.appearance_count_20d)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{pct(row.price_progression_pct ?? row.price_vs_first_trigger_pct)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fixed(row.rank_change_20d)}</td>
                  <td className="px-3 py-2">{text(row.volume_signal)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fixed(row.days_since_last_seen)}</td>
                  <td className="px-3 py-2">{text(row.trap_category, '-')}</td>
                  <td className="px-3 py-2">{text(row.last_seen_date ?? row.trade_date)}</td>
                  <td className="px-3 py-2">
                    <button type="button" className="rounded-md border border-slate-700 px-2 py-1 text-[11px] text-slate-200 hover:bg-slate-800" onClick={() => onOpen(row)}>
                      Open
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </SectionCard>
  );
}

function InvestigatorDrawer({ row, details, onClose }: { row: Row | null; details: Record<string, Record<string, unknown>>; onClose: () => void }) {
  const [tab, setTab] = useState<DrawerTab>('thesis');
  if (!row) return null;
  const symbol = symbolOf(row);
  const detail = (details[symbol] ?? {}) as Record<string, Row>;
  const summary = (detail.summary ?? row) as Row;
  const repeat = (detail.repeat ?? {}) as Row;
  const trap = (detail.trap ?? detail.archive ?? {}) as Row;
  const tabs: Array<{ key: DrawerTab; label: string }> = [
    { key: 'thesis', label: 'Thesis' },
    { key: 'timeline', label: 'Timeline' },
    { key: 'price', label: 'Price Chart' },
    { key: 'repeat', label: 'Repeat Evidence' },
    { key: 'trap', label: 'Trap Evidence' },
    { key: 'factors', label: 'Factor Breakdown' },
    { key: 'actions', label: 'Actions' },
  ];
  return (
    <div className="fixed inset-0 z-50 bg-slate-950/70" onClick={onClose}>
      <aside className="ml-auto h-full w-full max-w-2xl overflow-y-auto border-l border-slate-800 bg-slate-950 p-5 shadow-2xl" onClick={(event) => event.stopPropagation()}>
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-wide text-slate-500">Investigator Case</p>
            <h2 className="text-xl font-semibold text-white">{symbol}</h2>
            <p className="mt-1 text-sm text-slate-400">{text(summary.sector)} | {text(summary.decision_reason)}</p>
          </div>
          <button type="button" className="rounded-md border border-slate-700 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800" onClick={onClose}>Close</button>
        </div>
        <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
          <Metric label="Score" value={fixed(summary.investigator_score)} />
          <Metric label="Price vs First" value={pct(summary.price_progression_pct ?? summary.price_vs_first_trigger_pct)} />
          <Metric label="Repeat" value={fixed(summary.appearance_count_20d ?? repeat.appearance_count_20d)} />
          <Metric label="Rank Delta" value={fixed(summary.rank_change_20d ?? repeat.rank_change_20d)} />
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          {tabs.map((item) => (
            <button key={item.key} type="button" className={cn('rounded-md border px-3 py-1 text-xs', tab === item.key ? 'border-sky-500 bg-sky-500/10 text-sky-200' : 'border-slate-700 text-slate-300 hover:bg-slate-800')} onClick={() => setTab(item.key)}>
              {item.label}
            </button>
          ))}
        </div>
        <div className="mt-4 rounded-lg border border-slate-800 bg-slate-900/50 p-4 text-sm text-slate-300">
          {tab === 'thesis' ? (
            <div className="space-y-4">
              <p><span className="text-slate-500">Verdict:</span> {text(summary.decision_verdict)}</p>
              <p><span className="text-slate-500">Reason:</span> {text(summary.decision_reason)}</p>
              <p><span className="text-slate-500">Setup:</span> {text(summary.setup ?? summary.move_tag ?? summary.trigger_reason)}</p>
              <div>
                <div className="mb-1 font-semibold text-slate-100">Why selected</div>
                <ul className="list-disc space-y-1 pl-5">
                  <li>Repeat appeared {fixed(summary.appearance_count_20d ?? repeat.appearance_count_20d)} times.</li>
                  <li>Price is {pct(summary.price_progression_pct ?? summary.price_vs_first_trigger_pct)} from first appearance.</li>
                  <li>Volume signal is {text(summary.volume_signal)}.</li>
                </ul>
              </div>
              <div>
                <div className="mb-1 font-semibold text-slate-100">Why not high conviction</div>
                <ul className="list-disc space-y-1 pl-5">
                  {num(summary.investigator_score) < 80 ? <li>Score below 80.</li> : null}
                  {!bool(summary.volume_escalation) && text(summary.volume_signal) !== 'Rising' ? <li>Volume confirmation is not strong enough.</li> : null}
                  {num(summary.rank_change_20d) >= 0 ? <li>Rank improvement is not strong enough.</li> : null}
                </ul>
              </div>
            </div>
          ) : null}
          {tab === 'timeline' ? (
            <div className="space-y-2">
              <p>First seen: {text(repeat.first_seen_date ?? summary.first_seen_date)}</p>
              <p>Last seen: {text(repeat.last_seen_date ?? summary.last_seen_date ?? summary.trade_date)}</p>
              <p>Appearances 20D: {fixed(repeat.appearance_count_20d ?? summary.appearance_count_20d)}</p>
            </div>
          ) : null}
          {tab === 'price' ? <EmptyState message="Price, volume, and rank sparklines will appear when symbol-level Investigator history is available." /> : null}
          {tab === 'repeat' ? (
            <div className="grid grid-cols-2 gap-3">
              <Metric label="First Seen" value={text(repeat.first_seen_date ?? summary.first_seen_date)} />
              <Metric label="Last Seen" value={text(repeat.last_seen_date ?? summary.last_seen_date ?? summary.trade_date)} />
              <Metric label="Repeat Score" value={fixed(repeat.repeat_score ?? summary.repeat_score)} />
              <Metric label="High Priority" value={bool(repeat.high_priority_repeat ?? summary.high_priority_repeat) ? 'Yes' : 'No'} />
            </div>
          ) : null}
          {tab === 'factors' ? (
            <div className="grid grid-cols-2 gap-3">
              <QualityBar label="Repeat" value={repeat.repeat_score ?? summary.repeat_score} />
              <QualityBar label="Volume/Delivery" value={num(summary.volume_delivery_score) * 5} />
              <QualityBar label="Sector" value={num(summary.sector_support_score) * 10} />
              <QualityBar label="Setup" value={num(summary.trigger_quality_score) * 5} />
              <QualityBar label="Price Sustain" value={(num(summary.price_progression_pct ?? summary.price_vs_first_trigger_pct) + 20) * 2.5} />
              <QualityBar label="Trap Penalty" value={scoreParts(summary)[6].value} />
            </div>
          ) : null}
          {tab === 'trap' ? (
            <div className="space-y-2">
              <p><span className="text-slate-500">Trap category:</span> {text(trap.trap_category ?? summary.trap_category)}</p>
              <p><span className="text-slate-500">Drop reason:</span> {text(trap.drop_reason)}</p>
              <p><span className="text-slate-500">Low delivery:</span> {bool(summary.low_delivery_flag) ? 'Yes' : 'No'}</p>
            </div>
          ) : null}
          {tab === 'actions' ? (
            <div className="grid gap-2 sm:grid-cols-3">
              {['Promote to Watchlist', 'Archive', 'Mark Trap'].map((label) => (
                <button key={label} type="button" disabled className="rounded-md border border-slate-700 px-3 py-2 text-xs text-slate-500">
                  {label}
                </button>
              ))}
            </div>
          ) : null}
        </div>
      </aside>
    </div>
  );
}

function passesFilters(row: Row, filters: Set<string>): boolean {
  if (filters.has('repeat') && num(row.appearance_count_20d) < 3) return false;
  if (filters.has('price') && num(row.price_progression_pct ?? row.price_vs_first_trigger_pct) <= 0) return false;
  if (filters.has('rank') && num(row.rank_change_20d) >= 0) return false;
  if (filters.has('volume') && !bool(row.volume_escalation) && text(row.volume_signal) !== 'Rising') return false;
  if (filters.has('trapFree') && text(row.decision_verdict) === 'Trap Risk') return false;
  if (filters.has('newToday') && num(row.appearance_count_20d, 1) > 1) return false;
  if (filters.has('stale') && num(row.days_since_last_seen) <= 5) return false;
  return true;
}

export default function InvestigatorPage() {
  const query = useInvestigatorSnapshot();
  const data = query.data;
  const [filters, setFilters] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<Row | null>(null);
  const [trapFilter, setTrapFilter] = useState<TrapFilter>(null);
  const summary = data?.summary ?? {};
  const deltas = data?.summary_deltas ?? {};
  const decisionRows = sortByScore(data?.decision_queue ?? []);
  const fallbackRows = sortByScore(data?.closest_to_high_conviction ?? []);
  const trapMatches = (row: Row) => {
    if (!trapFilter) return true;
    const symbol = symbolOf(row);
    return text(row.trap_category) === trapFilter.category || trapFilter.symbols.includes(symbol);
  };
  const activeRows = useMemo(() => sortByScore(data?.active_watchlist ?? []).filter((row) => passesFilters(row, filters)).filter(trapMatches), [data?.active_watchlist, filters, trapFilter]);
  const archiveRows = useMemo(() => (data?.archive_summary?.rows ?? []).filter(trapMatches), [data?.archive_summary?.rows, trapFilter]);
  const trendRows = data?.charts?.trend ?? data?.charts?.four_week_trend ?? [];

  const toggleFilter = (key: string) => {
    setFilters((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <PageFrame title="Investigator" description="Decision board for repeat strength, trap evidence, and action-ready post-rank candidates." compactHeader>
      {query.isLoading ? (
        <CardSkeleton />
      ) : query.error ? (
        <ErrorStateView error={`Failed to load investigator: ${query.error.message}`} onRetry={() => query.refetch()} />
      ) : !data ? (
        <EmptyState message="No investigator payload available." />
      ) : (
        <>
          <HealthRibbon data={data as unknown as Row} />
          <SectionCard title="Investigator Pulse">
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-5">
              <Metric label="Daily Gainers" value={summary.daily_gainers} deltaValue={deltas.daily_gainers} />
              <Metric label="New In Window" value={summary.new_in_window ?? summary.new_candidates} deltaValue={deltas.new_in_window ?? deltas.new_candidates} />
              <Metric label="Active Queue" value={summary.active_queue} deltaValue={deltas.active_queue} />
              <Metric label="Repeat >=3x" value={summary.repeat_ge3} deltaValue={deltas.repeat_ge3} />
              <Metric label="Improving" value={summary.improving_repeats} deltaValue={deltas.improving_repeats} />
              <Metric label="High Conviction" value={summary.high_conviction} deltaValue={deltas.high_conviction} />
              {hasValue(summary, 'trap_count') || hasValue(summary, 'traps') ? <Metric label="Trap Count" value={summary.trap_count ?? summary.traps} deltaValue={deltas.trap_count ?? deltas.traps} /> : null}
              <Metric label="Trap Rate" value={pct(num(summary.trap_rate) * 100)} />
              {hasValue(summary, 'fresh_trap_today') ? <Metric label="Fresh Traps" value={summary.fresh_trap_today} deltaValue={deltas.fresh_trap_today} /> : null}
              {hasValue(summary, 'repeat_trap') ? <Metric label="Repeat Trap" value={summary.repeat_trap} deltaValue={deltas.repeat_trap} /> : null}
              <Metric label="Archived" value={summary.archived} deltaValue={deltas.archived} />
            </div>
          </SectionCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.5fr_1fr_1fr]">
            <ActionQueue rows={decisionRows} fallback={fallbackRows} highConvictionCount={num(summary.high_conviction)} onOpen={setSelected} />
            <RepeatQualityPanel rows={data.repeat_quality ?? []} onOpen={setSelected} />
            <TrapRadar rows={data.trap_radar ?? []} activeFilter={trapFilter} onFilter={setTrapFilter} />
          </div>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
            <SectionCard title="Today Funnel"><FunnelChart rows={data.charts?.funnel_today ?? data.charts?.funnel ?? []} /></SectionCard>
            <SectionCard title="Rolling Window Funnel"><FunnelChart rows={data.charts?.funnel_window ?? data.charts?.funnel ?? []} /></SectionCard>
            <SectionCard title="Repeat vs Price Sustain"><ScatterPanel rows={data.charts?.repeat_price_scatter ?? []} /></SectionCard>
          </div>
          <SectionCard title="Investigator Trend"><TrendBars rows={trendRows} /></SectionCard>

          <ActiveTable rows={activeRows} filters={filters} trapFilter={trapFilter} onClearTrapFilter={() => setTrapFilter(null)} onToggleFilter={toggleFilter} onOpen={setSelected} />

          <SectionCard title="Archive" description="Closed cases stay available without competing with the active queue." collapsible defaultCollapsed>
            {archiveRows.length === 0 ? (
              <EmptyState message="No archived rows available." />
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full text-left text-xs">
                  <thead className="uppercase text-slate-500">
                    <tr><th className="px-3 py-2">Symbol</th><th className="px-3 py-2">Reason</th><th className="px-3 py-2">Verdict</th><th className="px-3 py-2">Re-entry Rule</th></tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800">
                    {archiveRows.slice(0, 25).map((row) => (
                      <tr key={symbolOf(row)} className="text-slate-200">
                        <td className="px-3 py-2 font-semibold">{symbolOf(row)}</td>
                        <td className="px-3 py-2">{text(row.drop_reason)}</td>
                        <td className="px-3 py-2">{text(row.verdict)}</td>
                        <td className="px-3 py-2 text-slate-400">Can re-enter on fresh trigger and improved repeat quality.</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </SectionCard>
          <InvestigatorDrawer row={selected} details={(data.row_details ?? {}) as Record<string, Record<string, unknown>>} onClose={() => setSelected(null)} />
        </>
      )}
    </PageFrame>
  );
}
