/**
 * Ranking table with expandable rows (PR #8).
 *
 * Each row is sortable and renders inline factor bars + tier + rank-pos
 * chips. Clicking the row toggles a full-width expansion panel rendered by
 * ``ExpandedRowPanel``. The expansion panel lazily fetches per-symbol
 * detail and history via react-query so unrelated rows stay cheap.
 *
 * Compare actions live on the expansion panel itself; the table just owns
 * the sort/expand state and surfaces inline tier + score.
 */
import { Fragment, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from '@tanstack/react-table';
import type { StockRow } from '@/types/dashboard';
import FactorBars from '@/components/ranking/FactorBars';
import TierBadge from '@/components/ranking/TierBadge';
import ExpandedRowPanel from '@/components/ranking/ExpandedRowPanel';
import { cn } from '@/lib/utils/cn';

const columnHelper = createColumnHelper<RankRow>();

interface RankRow extends StockRow {
  rankPosition: number;
}

interface Props {
  rows: StockRow[];
  /**
   * Currently-expanded row symbol. Pass ``null`` to disable expansion (e.g.
   * when the table is embedded as a slim summary on the Pipeline page).
   */
  expandedSymbol?: string | null;
  onToggleExpand?: (symbol: string) => void;
  comparedSymbols?: ReadonlySet<string>;
  onToggleCompare?: (row: StockRow) => void;
}

function fallbackFactors(row: StockRow) {
  return {
    rs: row.rs,
    volume: row.volume === 'High' ? 85 : row.volume === 'Medium' ? 60 : 35,
    trend: row.trend,
    sector: row.sectorStrength,
  };
}

const EMPTY_COMPARE_SET: ReadonlySet<string> = new Set<string>();

export default function RankingTable({
  rows,
  expandedSymbol = null,
  onToggleExpand,
  comparedSymbols = EMPTY_COMPARE_SET,
  onToggleCompare,
}: Props) {
  const expansionEnabled = typeof onToggleExpand === 'function';
  const [sorting, setSorting] = useState<SortingState>([{ id: 'score', desc: true }]);

  const indexed: RankRow[] = useMemo(
    () => rows.map((row, idx) => ({ ...row, rankPosition: idx + 1 })),
    [rows],
  );

  const columns = useMemo(
    () => [
      columnHelper.accessor('rankPosition', {
        header: '#',
        cell: (info) => (
          <span className="inline-flex h-6 min-w-[2rem] items-center justify-center rounded-md border border-slate-700 bg-slate-950/60 px-1.5 text-xs font-semibold tabular-nums text-slate-300">
            {info.getValue()}
          </span>
        ),
        size: 56,
      }),
      columnHelper.accessor('symbol', {
        header: 'Ticker',
        cell: (info) => {
          const row = info.row.original;
          return (
            <div className="flex items-center gap-2">
              <TierBadge tier={row.tier} />
              <Link
                to={`/symbol/${row.symbol}`}
                onClick={(e) => e.stopPropagation()}
                className="font-semibold text-slate-100 hover:text-blue-400 hover:underline transition-colors"
              >
                {row.symbol}
              </Link>
              {comparedSymbols.has(row.symbol) ? (
                <span className="rounded-full border border-blue-500/40 bg-blue-500/15 px-1.5 text-[10px] font-semibold uppercase tracking-wider text-blue-200">
                  Compare
                </span>
              ) : null}
            </div>
          );
        },
      }),
      columnHelper.accessor('score', {
        header: 'Score',
        cell: (info) => (
          <span className="font-semibold tabular-nums text-slate-100">
            {info.getValue().toFixed(2)}
          </span>
        ),
      }),
      columnHelper.display({
        id: 'factors',
        header: 'Factors',
        cell: (info) => (
          <FactorBars
            factors={[]}
            fallback={fallbackFactors(info.row.original)}
            variant="inline"
          />
        ),
      }),
      columnHelper.accessor('sector', { header: 'Sector' }),
      columnHelper.accessor('pattern', {
        header: 'Pattern',
        cell: (info) => {
          const value = info.getValue();
          if (!value || value === 'N/A') return <span className="text-slate-500">—</span>;
          return <span className="text-slate-200">{value}</span>;
        },
      }),
      columnHelper.display({
        id: 'breakout',
        header: 'Breakout',
        cell: (info) =>
          info.row.original.breakout ? (
            <span className="rounded-full border border-emerald-500/40 bg-emerald-500/15 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-emerald-200">
              Confirmed
            </span>
          ) : (
            <span className="text-slate-500">—</span>
          ),
      }),
      columnHelper.display({
        id: 'expand',
        header: '',
        cell: (info) => (
          <span
            aria-hidden="true"
            className={cn(
              'inline-block transform text-slate-500 transition-transform',
              expandedSymbol === info.row.original.symbol ? 'rotate-90 text-slate-200' : '',
            )}
          >
            ▶
          </span>
        ),
        size: 24,
      }),
    ],
    [comparedSymbols, expandedSymbol],
  );

  const table = useReactTable({
    data: indexed,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[960px] text-left text-sm">
        <thead className="border-y border-slate-800 text-slate-400">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                const sortable = header.column.getCanSort();
                const sorted = header.column.getIsSorted();
                return (
                  <th
                    key={header.id}
                    className="px-4 py-3 font-medium"
                    style={{ width: header.getSize() }}
                  >
                    {header.isPlaceholder ? null : sortable ? (
                      <button
                        type="button"
                        onClick={header.column.getToggleSortingHandler()}
                        className="flex items-center gap-1 text-xs font-semibold uppercase tracking-wider hover:text-slate-200"
                      >
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        {sorted === 'asc' ? '▲' : sorted === 'desc' ? '▼' : ''}
                      </button>
                    ) : (
                      flexRender(header.column.columnDef.header, header.getContext())
                    )}
                  </th>
                );
              })}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => {
            const isExpanded = expandedSymbol === row.original.symbol;
            return (
              <Fragment key={row.id}>
                <tr
                  data-symbol={row.original.symbol}
                  data-expanded={isExpanded ? 'true' : 'false'}
                  className={cn(
                    'border-b border-slate-800 transition-colors hover:bg-slate-800/40',
                    expansionEnabled ? 'cursor-pointer' : '',
                    isExpanded ? 'bg-slate-800/40' : '',
                  )}
                  onClick={
                    expansionEnabled
                      ? () => onToggleExpand?.(row.original.symbol)
                      : undefined
                  }
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-4 py-3 align-middle">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
                {isExpanded && expansionEnabled ? (
                  <tr className="border-b border-slate-800 bg-slate-950/60">
                    <td colSpan={row.getVisibleCells().length} className="p-0">
                      <ExpandedRowPanel
                        row={row.original}
                        isCompared={comparedSymbols.has(row.original.symbol)}
                        onToggleCompare={() => onToggleCompare?.(row.original)}
                      />
                    </td>
                  </tr>
                ) : null}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

