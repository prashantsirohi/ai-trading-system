import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import StatusBadge from '@/components/common/StatusBadge';
import { useInvestigatorSnapshot } from '@/lib/queries';
import { cn } from '@/lib/utils/cn';

type ColumnKind = 'text' | 'number' | 'date' | 'status' | 'verdict' | 'reason';

interface ColumnSpec {
  key: string;
  label: string;
  kind?: ColumnKind;
  digits?: number;
}

const SCORE_COLUMNS: ColumnSpec[] = [
  { key: 'symbol_id', label: 'Symbol', kind: 'text' },
  { key: 'verdict', label: 'Verdict', kind: 'verdict' },
  { key: 'final_score', label: 'Score', kind: 'number', digits: 0 },
  { key: 'status', label: 'Status', kind: 'status' },
  { key: 'move_tag', label: 'Move', kind: 'reason' },
  { key: 'daily_return_pct', label: 'Return', kind: 'number', digits: 1 },
  { key: 'volume_ratio_20', label: 'Vol ratio', kind: 'number', digits: 1 },
  { key: 'delivery_pct', label: 'Delivery', kind: 'number', digits: 1 },
  { key: 'rank_position', label: 'Rank', kind: 'number', digits: 0 },
];

const REPEAT_COLUMNS: ColumnSpec[] = [
  { key: 'symbol_id', label: 'Symbol', kind: 'text' },
  { key: 'appearance_count_20d', label: 'Appear 20D', kind: 'number', digits: 0 },
  { key: 'repeat_score', label: 'Repeat', kind: 'number', digits: 0 },
  { key: 'price_progression_pct', label: 'Price vs first', kind: 'number', digits: 1 },
  { key: 'rank_change_20d', label: 'Rank change', kind: 'number', digits: 0 },
  { key: 'volume_escalation', label: 'Vol rising', kind: 'text' },
  { key: 'high_priority_repeat', label: 'Priority', kind: 'text' },
];

const ACTIVE_COLUMNS: ColumnSpec[] = [
  { key: 'symbol_id', label: 'Symbol', kind: 'text' },
  { key: 'status', label: 'Status', kind: 'status' },
  { key: 'verdict', label: 'Verdict', kind: 'verdict' },
  { key: 'score_current', label: 'Current', kind: 'number', digits: 0 },
  { key: 'score_peak', label: 'Peak', kind: 'number', digits: 0 },
  { key: 'appearance_count_20d', label: 'Appear 20D', kind: 'number', digits: 0 },
  { key: 'days_since_last_seen', label: 'Days stale', kind: 'number', digits: 0 },
  { key: 'price_vs_first_trigger_pct', label: 'Price vs first', kind: 'number', digits: 1 },
];

const ARCHIVE_COLUMNS: ColumnSpec[] = [
  { key: 'symbol_id', label: 'Symbol', kind: 'text' },
  { key: 'drop_reason', label: 'Reason', kind: 'reason' },
  { key: 'verdict', label: 'Verdict', kind: 'verdict' },
  { key: 'final_score', label: 'Score', kind: 'number', digits: 0 },
  { key: 'appearance_count_20d', label: 'Appear 20D', kind: 'number', digits: 0 },
  { key: 'archived_at', label: 'Archived', kind: 'date' },
];

function formatValue(value: unknown, column: ColumnSpec): string {
  if (value === null || value === undefined || value === '') return '-';
  if (column.kind === 'number') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed.toFixed(column.digits ?? 1) : '-';
  }
  if (column.kind === 'date') return String(value).slice(0, 10);
  return String(value).replace(/_/g, ' ');
}

function verdictTone(value: string): 'good' | 'warn' | 'bad' | 'neutral' {
  const normalized = value.toUpperCase();
  if (normalized === 'HIGH_CONVICTION') return 'good';
  if (normalized === 'MEDIUM_CONVICTION' || normalized === 'WATCH_ONLY') return 'warn';
  if (normalized === 'NOISE_TRAP') return 'bad';
  return 'neutral';
}

function Metric({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/70 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.12em] text-slate-500">{label}</div>
      <div className="mt-1 text-2xl font-semibold tabular-nums text-slate-100">{String(value ?? 0)}</div>
    </div>
  );
}

function DataTable({ title, rows, columns }: { title: string; rows: Array<Record<string, unknown>>; columns: ColumnSpec[] }) {
  return (
    <SectionCard title={title}>
      {rows.length === 0 ? (
        <EmptyState message="No rows available." />
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0 text-xs">
            <thead>
              <tr className="text-left uppercase text-slate-500">
                {columns.map((column) => (
                  <th key={column.key} className={cn('whitespace-nowrap px-3 py-2 font-medium', column.kind === 'number' ? 'text-right' : '')}>
                    {column.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr key={`${row.symbol_id ?? idx}`} className="text-slate-200 hover:bg-slate-800/35">
                  {columns.map((column) => {
                    const value = formatValue(row[column.key], column);
                    const isNumber = column.kind === 'number';
                    return (
                      <td key={column.key} className={cn('border-t border-slate-800 px-3 py-2 align-top', isNumber ? 'text-right tabular-nums' : 'whitespace-nowrap')}>
                        {column.kind === 'status' ? (
                          <StatusBadge status={value} />
                        ) : column.kind === 'verdict' ? (
                          <StatusBadge status={value} label={value} tone={verdictTone(value)} />
                        ) : column.kind === 'reason' && value !== '-' ? (
                          <span className="inline-flex rounded-full border border-slate-700 bg-slate-950 px-2 py-0.5 text-[11px] font-medium capitalize text-slate-300">
                            {value.toLowerCase()}
                          </span>
                        ) : (
                          value
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </SectionCard>
  );
}

export default function InvestigatorPage() {
  const query = useInvestigatorSnapshot();
  const data = query.data;
  const summary = data?.summary ?? {};
  const archive = data?.archive_summary ?? { count: 0, by_reason: {}, rows: [] };

  return (
    <PageFrame title="Investigator" description="Post-rank daily gainer conviction, repeat tracking, traps, and status ageing." compactHeader>
      {query.isLoading ? (
        <CardSkeleton />
      ) : query.error ? (
        <ErrorStateView error={`Failed to load investigator: ${query.error.message}`} onRetry={() => query.refetch()} />
      ) : !data ? (
        <EmptyState message="No investigator payload available." />
      ) : (
        <>
          <SectionCard title="Investigator Pulse">
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-7">
              <Metric label="Daily Gainers" value={summary.daily_gainer_count} />
              <Metric label="Active" value={summary.active_count} />
              <Metric label="High Conviction" value={summary.high_conviction_count} />
              <Metric label="Medium Conviction" value={summary.medium_conviction_count} />
              <Metric label="Repeat Accum." value={summary.repeat_accumulation_count} />
              <Metric label="Traps" value={summary.trap_count} />
              <Metric label="Archived" value={archive.count} />
            </div>
          </SectionCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <DataTable title="High Conviction" rows={data.high_conviction.slice(0, 20)} columns={SCORE_COLUMNS} />
            <DataTable title="Repeat Tracker" rows={data.repeat_tracker.slice(0, 25)} columns={REPEAT_COLUMNS} />
          </div>

          <DataTable title="Active Investigator List" rows={data.active_watchlist.slice(0, 50)} columns={ACTIVE_COLUMNS} />

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <DataTable title="Trap Log" rows={data.trap_log.slice(0, 25)} columns={SCORE_COLUMNS} />
            <DataTable title="Archive" rows={archive.rows.slice(0, 25)} columns={ARCHIVE_COLUMNS} />
          </div>
        </>
      )}
    </PageFrame>
  );
}
