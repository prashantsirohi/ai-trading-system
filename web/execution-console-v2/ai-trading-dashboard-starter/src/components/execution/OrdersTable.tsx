/**
 * Execution orders table.
 *
 * Columns: Symbol / Entry / Stop / Target / R:R / Size% / Confidence.
 * Values are derived from the row's StockRow + heuristic factors in
 * ``derive.ts``. Row click is exposed for the parent so the future Stock
 * Detail Workspace (PR #12) can link in directly.
 */
import { useMemo, useState } from 'react';
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from '@tanstack/react-table';
import { cn } from '@/lib/utils/cn';
import type { DerivedOrder } from './derive';

const helper = createColumnHelper<DerivedOrder>();

interface Props {
  orders: DerivedOrder[];
  onRowClick?: (order: DerivedOrder) => void;
  disabled?: boolean;
}

function confidenceTone(value: number): string {
  if (value >= 75) return 'bg-emerald-500/70';
  if (value >= 55) return 'bg-amber-500/70';
  return 'bg-rose-500/70';
}

export default function OrdersTable({ orders, onRowClick, disabled }: Props) {
  const [sorting, setSorting] = useState<SortingState>([{ id: 'confidence', desc: true }]);

  const columns = useMemo(
    () => [
      helper.accessor('symbol', {
        header: 'Symbol',
        cell: (info) => (
          <span className="font-semibold text-slate-100">{info.getValue()}</span>
        ),
      }),
      helper.accessor('entry', {
        header: 'Entry',
        cell: (info) => <span className="tabular-nums text-slate-200">{info.getValue().toFixed(2)}</span>,
      }),
      helper.accessor('stop', {
        header: 'Stop',
        cell: (info) => <span className="tabular-nums text-rose-300">{info.getValue().toFixed(2)}</span>,
      }),
      helper.accessor('target', {
        header: 'Target',
        cell: (info) => <span className="tabular-nums text-emerald-300">{info.getValue().toFixed(2)}</span>,
      }),
      helper.accessor('riskReward', {
        header: 'R:R',
        cell: (info) => <span className="tabular-nums text-slate-200">{info.getValue().toFixed(2)}x</span>,
      }),
      helper.accessor('sizePct', {
        header: 'Size %',
        cell: (info) => <span className="tabular-nums text-slate-200">{info.getValue().toFixed(2)}%</span>,
      }),
      helper.accessor('confidence', {
        header: 'Confidence',
        cell: (info) => {
          const value = info.getValue();
          return (
            <div className="flex items-center gap-2">
              <div className="h-1.5 w-20 overflow-hidden rounded-full bg-slate-800">
                <div
                  className={cn('h-full rounded-full', confidenceTone(value))}
                  style={{ width: `${Math.min(100, value)}%` }}
                />
              </div>
              <span className="text-xs tabular-nums text-slate-300">{value}</span>
            </div>
          );
        },
      }),
    ],
    [],
  );

  const table = useReactTable({
    data: orders,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  if (orders.length === 0) {
    return (
      <div className="rounded-xl border border-dashed border-slate-700 bg-slate-950/60 p-6 text-center text-sm text-slate-400">
        No eligible orders to route.
      </div>
    );
  }

  return (
    <div className={cn('overflow-x-auto', disabled ? 'opacity-70' : '')}>
      <table className="w-full min-w-[640px] text-left text-sm">
        <thead className="border-y border-slate-800 text-slate-400">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                const sortable = header.column.getCanSort();
                const sorted = header.column.getIsSorted();
                return (
                  <th key={header.id} className="px-4 py-3 font-medium">
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
          {table.getRowModel().rows.map((row) => (
            <tr
              key={row.id}
              className={cn(
                'border-b border-slate-800 transition-colors hover:bg-slate-800/40',
                onRowClick ? 'cursor-pointer' : '',
              )}
              onClick={onRowClick ? () => onRowClick(row.original) : undefined}
              data-symbol={row.original.symbol}
            >
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id} className="px-4 py-3">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
