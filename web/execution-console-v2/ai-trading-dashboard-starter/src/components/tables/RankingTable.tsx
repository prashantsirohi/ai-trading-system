import { useMemo, useState } from 'react';
import { createColumnHelper, flexRender, getCoreRowModel, getSortedRowModel, useReactTable, type SortingState } from '@tanstack/react-table';
import type { StockRow } from '@/types/dashboard';

const columnHelper = createColumnHelper<StockRow>();

interface RankingTableProps {
  rows: StockRow[];
  onSelectRow?: (row: StockRow) => void;
  selectedSymbol?: string | null;
}

export default function RankingTable({ rows, onSelectRow, selectedSymbol }: RankingTableProps) {
  const [sorting, setSorting] = useState<SortingState>([{ id: 'score', desc: true }]);

  const columns = useMemo(() => [
    columnHelper.accessor('symbol', { header: 'Ticker' }),
    columnHelper.accessor('score', { header: 'Score' }),
    columnHelper.accessor('rs', { header: 'RS' }),
    columnHelper.accessor('sector', { header: 'Sector' }),
    columnHelper.accessor('pattern', { header: 'Pattern' }),
    columnHelper.accessor('tier', { header: 'Tier' }),
  ], []);

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[720px] text-left text-sm">
        <thead className="border-y border-slate-800 text-slate-400">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th key={header.id} className="px-4 py-3 font-medium">
                  {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr
              key={row.id}
              className={`border-b border-slate-800 hover:bg-slate-800/40 ${
                onSelectRow ? 'cursor-pointer' : ''
              } ${selectedSymbol === row.original.symbol ? 'bg-slate-800/60' : ''}`}
              onClick={() => onSelectRow?.(row.original)}
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
