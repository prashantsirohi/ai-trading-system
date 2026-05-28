import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import { useFundamentalsDashboard } from '@/lib/queries';
import { cn } from '@/lib/utils/cn';

type ColumnKind = 'text' | 'number' | 'score' | 'percentRatio' | 'percent' | 'date' | 'zone' | 'evidence';

interface ColumnSpec {
  key: string;
  label: string;
  kind?: ColumnKind;
  digits?: number;
  optional?: boolean;
}

function num(value: unknown, digits = 1): string {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return '-';
  return parsed.toFixed(digits);
}

function pct(value: unknown, digits = 1): string {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return '-';
  return `${parsed.toFixed(digits)}%`;
}

function text(value: unknown): string {
  if (value === null || value === undefined || value === '') return '-';
  const raw = String(value).trim();
  if (!raw || raw.toLowerCase() === 'nan' || raw.toLowerCase() === 'none' || raw.toLowerCase() === 'null') return '-';
  return raw;
}

function titleText(value: unknown): string {
  const raw = text(value);
  if (raw === '-') return raw;
  return raw
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function dateText(value: unknown): string {
  const raw = text(value);
  if (raw === '-') return raw;
  return raw.slice(0, 10);
}

function formatCell(value: unknown, column: ColumnSpec): string {
  switch (column.kind) {
    case 'date':
      return dateText(value);
    case 'number':
    case 'score':
      return num(value, column.digits ?? 1);
    case 'percentRatio':
      return pct(Number(value) * 100, column.digits ?? 1);
    case 'percent':
      return pct(value, column.digits ?? 1);
    case 'zone':
      return titleText(value);
    case 'evidence':
      return text(value).replace(/\s+/g, ' ');
    case 'text':
    default:
      return text(value);
  }
}

const GREAT_RESULTS_COLUMNS: ColumnSpec[] = [
  { key: 'symbol', label: 'Symbol', kind: 'text' },
  { key: 'great_result_score', label: 'Score', kind: 'score', digits: 1 },
  { key: 'evidence', label: 'Why it matters', kind: 'evidence' },
];

const TURNAROUND_COLUMNS: ColumnSpec[] = [
  { key: 'symbol', label: 'Symbol', kind: 'text' },
  { key: 'turnaround_score', label: 'Score', kind: 'score', digits: 1 },
  { key: 'turnaround_stage', label: 'Stage', kind: 'zone', optional: true },
  { key: 'evidence', label: 'Why it matters', kind: 'evidence' },
];

const COMPOUNDER_COLUMNS: ColumnSpec[] = [
  { key: 'symbol', label: 'Symbol', kind: 'text' },
  { key: 'compounder_score', label: 'Score', kind: 'score', digits: 1 },
  { key: 'sales_8q_consistency', label: 'Sales consistency', kind: 'score', digits: 0, optional: true },
  { key: 'profit_8q_consistency', label: 'Profit consistency', kind: 'score', digits: 0, optional: true },
  { key: 'margin_stability', label: 'Margin stability', kind: 'score', digits: 0, optional: true },
  { key: 'valuation_zone', label: 'Valuation', kind: 'zone', optional: true },
];

const SECTOR_EARNINGS_COLUMNS: ColumnSpec[] = [
  { key: 'sector_name', label: 'Sector', kind: 'text' },
  { key: 'sector_sales_yoy_growth', label: 'Sales YoY', kind: 'percentRatio', digits: 1 },
  { key: 'sector_profit_yoy_growth', label: 'Profit YoY', kind: 'percentRatio', digits: 1 },
  { key: 'margin_expansion_pct', label: 'Margin expansion', kind: 'percent', digits: 0 },
  { key: 'sector_fundamental_score', label: 'Score', kind: 'score', digits: 1 },
];

const VALUATION_COLUMNS: ColumnSpec[] = [
  { key: 'date', label: 'Date', kind: 'date' },
  { key: 'pe_ttm', label: 'PE TTM', kind: 'number', digits: 1 },
  { key: 'pe_200dma', label: 'PE 200DMA', kind: 'number', digits: 1 },
  { key: 'pe_5y_median', label: 'PE 5Y median', kind: 'number', digits: 1 },
  { key: 'pe_percentile_5y', label: 'PE percentile', kind: 'percent', digits: 0 },
  { key: 'valuation_zone', label: 'Zone', kind: 'zone' },
  { key: 'cycle_signal', label: 'Signal', kind: 'zone' },
];

function zoneClass(value: string): string {
  const normalized = value.toLowerCase();
  if (normalized.includes('cheap') || normalized.includes('below') || normalized.includes('discount')) {
    return 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200';
  }
  if (normalized.includes('expensive') || normalized.includes('premium') || normalized.includes('high')) {
    return 'border-amber-500/30 bg-amber-500/10 text-amber-200';
  }
  if (normalized.includes('fair') || normalized.includes('near')) {
    return 'border-sky-500/30 bg-sky-500/10 text-sky-200';
  }
  return 'border-slate-700 bg-slate-950 text-slate-300';
}

export default function FundamentalsPage() {
  const query = useFundamentalsDashboard();
  const data = query.data;
  const summary = data?.summary ?? {};

  return (
    <PageFrame title="Fundamentals" description="Valuation pulse and compact earnings insight." compactHeader>
      {query.isLoading ? (
        <CardSkeleton />
      ) : query.error ? (
        <ErrorStateView error={`Failed to load fundamentals: ${query.error.message}`} onRetry={() => query.refetch()} />
      ) : !data ? (
        <EmptyState message="No fundamentals payload available." />
      ) : (
        <>
          <SectionCard title="Fundamental Pulse">
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4 xl:grid-cols-7">
              <Metric label="Universe PE" value={num(summary.pe_ttm)} />
              <Metric label="PE 200DMA" value={num(summary.pe_200dma)} />
              <Metric label="PE 5Y Median" value={num(summary.pe_5y_median)} />
              <Metric label="PE 5Y Percentile" value={num(summary.pe_percentile_5y, 0)} />
              <Metric label="Valuation Zone" value={text(summary.valuation_zone)} />
              <Metric label="PE vs 200DMA" value={pct(summary.pe_distance_from_200dma)} />
              <Metric label="Loss Mcap" value={pct(Number(summary.loss_mcap_pct ?? 0) * 100)} />
            </div>
          </SectionCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <InsightTable title="Great Results" rows={data.great_results_top} columns={GREAT_RESULTS_COLUMNS} />
            <InsightTable title="Turnaround Stories" rows={data.turnarounds_top} columns={TURNAROUND_COLUMNS} />
            <InsightTable title="Compounders" rows={data.compounders_top.slice(0, 12)} columns={COMPOUNDER_COLUMNS} />
            <InsightTable title="Sector Earnings Leadership" rows={data.sector_earnings_top.slice(0, 12)} columns={SECTOR_EARNINGS_COLUMNS} />
          </div>

          <SectionCard title="Valuation Cycle">
            <InsightTable
              title=""
              rows={data.valuation_chart.slice(-12)}
              columns={VALUATION_COLUMNS}
            />
          </SectionCard>
        </>
      )}
    </PageFrame>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  const isZone = label.toLowerCase().includes('zone');
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/70 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.12em] text-slate-500">{label}</div>
      <div className={cn('mt-1 truncate text-lg font-semibold', isZone ? 'capitalize text-sky-200' : 'text-slate-100')}>{value}</div>
    </div>
  );
}

function InsightTable({ title, rows, columns }: { title: string; rows: Array<Record<string, unknown>>; columns: ColumnSpec[] }) {
  const safeRows = rows ?? [];
  const visibleColumns = columns.filter((column) => {
    if (!column.optional) return true;
    return safeRows.some((row) => formatCell(row[column.key], column) !== '-');
  });
  const body = (
    <>
      {safeRows.length === 0 ? (
        <div className="rounded-lg border border-slate-800 bg-slate-950/50 px-4 py-6 text-sm text-slate-400">No rows available.</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0 text-xs">
            <thead>
              <tr className="text-left uppercase text-slate-500">
                {visibleColumns.map((column) => (
                  <th
                    key={column.key}
                    className={cn(
                      'whitespace-nowrap px-3 py-2 font-medium',
                      numericColumn(column) ? 'w-[8.5rem] text-right' : '',
                      column.kind === 'evidence' ? 'min-w-[320px]' : '',
                    )}
                  >
                    {column.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {safeRows.map((row, idx) => (
                <tr key={idx} className="text-slate-200 hover:bg-slate-800/35">
                  {visibleColumns.map((column) => {
                    const value = formatCell(row[column.key], column);
                    const isNumeric = column.kind === 'number' || column.kind === 'score' || column.kind === 'percent' || column.kind === 'percentRatio';
                    const isSymbol = column.key === 'symbol';
                    const isZone = column.kind === 'zone' && value !== '-';
                    return (
                      <td
                        key={column.key}
                        className={cn(
                          'border-t border-slate-800 px-3 py-2 align-top',
                          isNumeric ? 'whitespace-nowrap text-right tabular-nums' : '',
                          column.kind === 'evidence' ? 'min-w-[320px] max-w-[560px] text-slate-300' : 'whitespace-nowrap',
                          isSymbol ? 'font-semibold text-slate-100' : '',
                        )}
                      >
                        {isZone ? (
                          <span className={cn('inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium', zoneClass(value))}>{value}</span>
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
    </>
  );
  if (!title) return body;
  return <SectionCard title={title}>{body}</SectionCard>;
}

function numericColumn(column: ColumnSpec): boolean {
  return column.kind === 'number' || column.kind === 'score' || column.kind === 'percent' || column.kind === 'percentRatio';
}
