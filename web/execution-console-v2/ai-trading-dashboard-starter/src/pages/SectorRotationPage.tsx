import { useEffect, useMemo, useState } from 'react';

import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import RRGChart, { type RRGLabelMode, type RRGScaleMode } from '@/components/rotation/RRGChart';
import RRGControls from '@/components/rotation/RRGControls';
import RotationDetailDrawer from '@/components/rotation/RotationDetailDrawer';
import { useSectorRotation } from '@/lib/queries';
import type {
  DeliverySignalRow,
  SectorCustomIndexRow,
  SectorRotationRow,
  StockRotationRow,
} from '@/lib/api/sectorRotation';
import { cn } from '@/lib/utils/cn';

const quadrantTone: Record<string, string> = {
  Leading: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200',
  Improving: 'border-sky-500/40 bg-sky-500/10 text-sky-200',
  Weakening: 'border-amber-500/40 bg-amber-500/10 text-amber-200',
  Lagging: 'border-rose-500/40 bg-rose-500/10 text-rose-200',
};

export default function SectorRotationPage() {
  const [groupType, setGroupType] = useState<'sector' | 'industry'>('industry');
  const [tailLength, setTailLength] = useState(20);
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [selectedSector, setSelectedSector] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [scaleMode, setScaleMode] = useState<RRGScaleMode>('focused');
  const [labelMode, setLabelMode] = useState<RRGLabelMode>('top');
  const [isPlaying, setIsPlaying] = useState(false);
  const [selectedGroup, setSelectedGroup] = useState<SectorRotationRow | null>(null);
  const [isFullView, setIsFullView] = useState(false);
  const [expandedQuadrants, setExpandedQuadrants] = useState<Record<string, boolean>>({});
  const [deliveryTab, setDeliveryTab] = useState<'accumulation' | 'distribution'>('accumulation');
  const [showIndexData, setShowIndexData] = useState(false);

  const rotationQuery = useSectorRotation({
    group_type: groupType,
    lookback: tailLength,
    sector: groupType === 'industry' ? selectedSector : null,
    show_stocks: true,
  }, {
    refetchInterval: false,
    refetchOnWindowFocus: false,
    staleTime: 5 * 60_000,
  });
  const data = rotationQuery.data;
  const timelineDates = useMemo(() => {
    const available = data?.available_dates ?? [];
    if (available.length > 0) return available.map(String).sort((a, b) => a.localeCompare(b));
    const dates = new Set<string>();
    (data?.history ?? []).forEach((row) => {
      if (row.date) dates.add(String(row.date));
    });
    return Array.from(dates).sort((a, b) => a.localeCompare(b));
  }, [data?.history]);

  useEffect(() => {
    if (timelineDates.length === 0) return;
    if (!selectedDate || !timelineDates.includes(selectedDate)) {
      setSelectedDate(timelineDates[timelineDates.length - 1]);
    }
  }, [selectedDate, timelineDates]);

  useEffect(() => {
    if (!isPlaying || timelineDates.length === 0) return;
    const id = window.setInterval(() => {
      setSelectedDate((current) => {
        const currentIndex = Math.max(0, timelineDates.indexOf(current ?? ''));
        return timelineDates[(currentIndex + 1) % timelineDates.length] ?? current;
      });
    }, 1200);
    return () => window.clearInterval(id);
  }, [isPlaying, timelineDates]);

  const groupsAtSelectedDate = useMemo(() => {
    const history = (data?.history ?? []).filter(hasRrgPoint);
    if (history.length === 0) return data?.groups ?? [];
    const selected = selectedDate ?? timelineDates[timelineDates.length - 1];
    const latestTimelineDate = timelineDates[timelineDates.length - 1];
    if (!selected || selected === latestTimelineDate) {
      return data?.groups ?? [];
    }
    const byGroup = new Map<string, SectorRotationRow>();
    history
      .filter((row) => !selected || String(row.date ?? '') <= selected)
      .sort((a, b) => String(a.date ?? '').localeCompare(String(b.date ?? '')))
      .forEach((row) => byGroup.set(groupName(row), row));
    return Array.from(byGroup.values());
  }, [data?.groups, data?.history, selectedDate, timelineDates]);

  const groups = useMemo(() => {
    const term = search.trim().toLowerCase();
    return groupsAtSelectedDate.filter((group) => {
      if (!term) return true;
      return groupName(group).toLowerCase().includes(term) || String(group.parent_sector ?? '').toLowerCase().includes(term);
    });
  }, [groupsAtSelectedDate, search]);
  const chartGroups = useMemo(() => groups.filter(hasRrgPoint), [groups]);
  const history = useMemo(() => {
    const visible = new Set(chartGroups.map(groupName));
    return (data?.history ?? []).filter((row) => {
      if (!visible.has(groupName(row)) || !hasRrgPoint(row)) return false;
      if (!selectedDate) return true;
      const rowDate = String(row.date ?? '');
      return rowDate <= selectedDate;
    });
  }, [data?.history, chartGroups, selectedDate]);
  const stocks = data?.stocks ?? [];
  const sectorOptions = useMemo(() => {
    const values = new Set<string>();
    [...(data?.groups ?? []), ...(data?.history ?? [])].forEach((row) => {
      const value = row.parent_sector ?? row.sector;
      if (value) values.add(value);
    });
    return Array.from(values).sort((a, b) => a.localeCompare(b));
  }, [data?.groups, data?.history]);
  const quadrantGroups = useMemo(() => {
    const byQuadrant: Record<string, SectorRotationRow[]> = { Leading: [], Improving: [], Weakening: [], Lagging: [] };
    chartGroups.forEach((group) => byQuadrant[group.quadrant ?? 'Lagging']?.push(group));
    Object.values(byQuadrant).forEach((rows) => rows.sort((a, b) => pointDistance(b) - pointDistance(a)));
    return byQuadrant;
  }, [chartGroups]);
  const selectedGroupName = selectedGroup ? groupName(selectedGroup) : null;
  const selectedStocks = useMemo(() => {
    if (!selectedGroup) return [];
    return stocks
      .filter((stock) => stockMatchesGroup(stock, selectedGroup))
      .sort((a, b) => Number(b.rotation_adjusted_score ?? 0) - Number(a.rotation_adjusted_score ?? 0));
  }, [selectedGroup, stocks]);
  const watchlist = useMemo(
    () => stocks.filter((row) => row.watchlist_candidate).slice(0, 20),
    [stocks],
  );
  const stockRows = selectedGroup ? selectedStocks.slice(0, 20) : (watchlist.length ? watchlist : stocks.slice(0, 20));
  const selectedSymbols = useMemo(() => new Set(selectedStocks.map((stock) => stock.symbol).filter(Boolean)), [selectedStocks]);
  const accumulationRows = useMemo(
    () => filterDeliveryRows(data?.accumulation ?? [], selectedGroup ? selectedSymbols : null).slice(0, 30),
    [data?.accumulation, selectedGroup, selectedSymbols],
  );
  const distributionRows = useMemo(
    () => filterDeliveryRows(data?.distribution ?? [], selectedGroup ? selectedSymbols : null).slice(0, 30),
    [data?.distribution, selectedGroup, selectedSymbols],
  );

  return (
    <PageFrame
      title="RRG-style Sector Rotation"
      description="Industry-first rotation trails, broad sector context, and stock confirmations from the latest rank run."
      compactHeader
    >
      {rotationQuery.isLoading ? (
        <CardSkeleton />
      ) : rotationQuery.error ? (
        <ErrorStateView
          error={`Failed to load sector rotation: ${rotationQuery.error.message}`}
          onRetry={() => rotationQuery.refetch()}
        />
      ) : !data || (groups.length === 0 && stocks.length === 0) ? (
        <EmptyState message="No sector rotation artifacts available in the latest rank run." />
      ) : (
        <>
          <SectionCard title="Rotation Map" description={`Run ${data.run_id ?? 'latest'} · ${data.benchmark_name ?? 'benchmark'}`}>
            <div className="space-y-3">
              <RRGControls
                groupType={groupType}
                onGroupTypeChange={(value) => {
                  setGroupType(value);
                  setSelectedSector(null);
                  setSelectedDate(null);
                  setSelectedGroup(null);
                }}
                availableDates={timelineDates}
                selectedDate={selectedDate ?? data.selected_date ?? null}
                onDateChange={setSelectedDate}
                isPlaying={isPlaying}
                onPlayingChange={setIsPlaying}
                tailLength={tailLength}
                onTailLengthChange={setTailLength}
                scaleMode={scaleMode}
                onScaleModeChange={setScaleMode}
                sectors={sectorOptions}
                selectedSector={selectedSector}
                onSectorChange={setSelectedSector}
                search={search}
                onSearchChange={setSearch}
                labelMode={labelMode}
                onLabelModeChange={setLabelMode}
                onFullView={() => setIsFullView(true)}
              />
              <RRGChart
                groups={chartGroups}
                history={history}
                scaleMode={scaleMode}
                labelMode={labelMode}
                selectedGroupName={selectedGroupName}
                onSelect={setSelectedGroup}
              />
              {groups.length > 0 && chartGroups.length === 0 ? (
                <div className="rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-100">
                  Latest rotation artifacts do not contain valid RS Ratio/Momentum values. Re-run the rank stage to regenerate RRG history.
                </div>
              ) : null}
            </div>
          </SectionCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
            {Object.entries(quadrantGroups).map(([quadrant, rows]) => (
              <SectionCard key={quadrant} title={quadrant} description={`${rows.length} groups`}>
                <GroupList
                  rows={expandedQuadrants[quadrant] ? rows : rows.slice(0, 5)}
                  selectedName={selectedGroupName}
                  onSelect={setSelectedGroup}
                />
                {rows.length > 5 ? (
                  <button
                    type="button"
                    className="mt-3 text-sm font-medium text-sky-300 hover:text-sky-200"
                    onClick={() => setExpandedQuadrants((current) => ({ ...current, [quadrant]: !current[quadrant] }))}
                  >
                    {expandedQuadrants[quadrant] ? 'Show top 5' : `View all ${rows.length}`}
                  </button>
                ) : null}
              </SectionCard>
            ))}
          </div>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <SectionCard
              title={selectedGroup ? `${groupName(selectedGroup)} Stock Confirmations` : 'Top Stock Confirmations'}
              description="Rotation-adjusted candidates with industry and sector support."
            >
              <StockTable rows={stockRows} />
            </SectionCard>

            <SectionCard title="Delivery Signals" description="Accumulation and distribution from delivery percentage, volume, and price behavior.">
              <DeliveryTable
                accumulation={accumulationRows}
                distribution={distributionRows}
                activeTab={deliveryTab}
                onTabChange={setDeliveryTab}
              />
            </SectionCard>
          </div>

          <SectionCard title="Custom Industry Indices" description="Latest index points used by the rotation sidecar.">
            <button
              type="button"
              className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-200 hover:bg-slate-800"
              onClick={() => setShowIndexData((value) => !value)}
            >
              {showIndexData ? 'Hide index data' : 'View index data'}
            </button>
            {showIndexData ? <div className="mt-3"><IndexTable rows={latestIndexRows(data.custom_indices ?? [])} /></div> : null}
          </SectionCard>

          <RotationDetailDrawer
            group={selectedGroup}
            stocks={stocks}
            accumulation={data.accumulation ?? []}
            distribution={data.distribution ?? []}
            onClose={() => setSelectedGroup(null)}
          />
          {isFullView ? (
            <div className="fixed inset-0 z-50 bg-slate-950/95 p-4" role="dialog" aria-modal="true">
              <div className="flex h-full flex-col gap-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h2 className="text-lg font-semibold text-white">RRG-style Sector Rotation</h2>
                    <p className="text-sm text-slate-400">{groupType === 'industry' ? 'Industry' : 'Sector'} · {selectedDate ?? data.selected_date ?? 'latest'}</p>
                  </div>
                  <button
                    type="button"
                    className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-200 hover:bg-slate-800"
                    onClick={() => setIsFullView(false)}
                  >
                    Close
                  </button>
                </div>
                <RRGControls
                  groupType={groupType}
                  onGroupTypeChange={(value) => {
                    setGroupType(value);
                    setSelectedSector(null);
                    setSelectedDate(null);
                    setSelectedGroup(null);
                  }}
                  availableDates={timelineDates}
                  selectedDate={selectedDate ?? data.selected_date ?? null}
                  onDateChange={setSelectedDate}
                  isPlaying={isPlaying}
                  onPlayingChange={setIsPlaying}
                  tailLength={tailLength}
                  onTailLengthChange={setTailLength}
                  scaleMode={scaleMode}
                  onScaleModeChange={setScaleMode}
                  sectors={sectorOptions}
                  selectedSector={selectedSector}
                  onSectorChange={setSelectedSector}
                  search={search}
                  onSearchChange={setSearch}
                  labelMode={labelMode}
                  onLabelModeChange={setLabelMode}
                  onFullView={() => setIsFullView(true)}
                />
                <RRGChart
                  groups={chartGroups}
                  history={history}
                  scaleMode={scaleMode}
                  labelMode={labelMode}
                  selectedGroupName={selectedGroupName}
                  expanded
                  onSelect={setSelectedGroup}
                />
                {selectedGroup ? (
                  <div className="max-h-48 overflow-y-auto rounded-md border border-slate-800 bg-slate-900/70 p-3">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <div className="text-sm font-semibold text-white">{groupName(selectedGroup)}</div>
                        <div className="text-xs text-slate-400">{selectedGroup.parent_sector ?? selectedGroup.sector ?? 'Other'} · {selectedGroup.quadrant ?? 'Lagging'}</div>
                      </div>
                      <div className="text-right text-xs text-slate-400">
                        <div>RS {fmt(selectedGroup.rs_ratio)} / {fmt(selectedGroup.rs_momentum)}</div>
                        <div>20D alpha {pct(selectedGroup.alpha_20d)}</div>
                      </div>
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}
        </>
      )}
    </PageFrame>
  );
}

function GroupList({
  rows,
  selectedName,
  onSelect,
}: {
  rows: SectorRotationRow[];
  selectedName: string | null;
  onSelect: (row: SectorRotationRow) => void;
}) {
  if (rows.length === 0) return <EmptyState message="No groups in this quadrant." />;
  return (
    <div className="space-y-2">
      {rows.map((row) => (
        <button
          key={groupName(row)}
          type="button"
          className={cn(
            'w-full rounded-md border bg-slate-950/70 p-3 text-left hover:border-slate-600',
            selectedName === groupName(row) ? 'border-sky-400/70' : 'border-slate-800',
          )}
          onClick={() => onSelect(row)}
        >
          <div className="flex items-center justify-between gap-3">
            <div className="min-w-0 truncate text-sm font-medium text-slate-100">{groupName(row)}</div>
            <Badge label={row.quadrant ?? 'Lagging'} />
          </div>
          <div className="mt-2 flex items-center justify-between text-xs text-slate-400">
            <span>{row.parent_sector ?? row.sector ?? 'Other'}</span>
            <span>{fmt(row.rs_ratio)} / {fmt(row.rs_momentum)}</span>
          </div>
        </button>
      ))}
    </div>
  );
}

function StockTable({ rows }: { rows: StockRotationRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-3 py-2">Symbol</th>
            <th className="px-3 py-2">Sector</th>
            <th className="px-3 py-2">Industry</th>
            <th className="px-3 py-2">Stock</th>
            <th className="px-3 py-2 text-right">Score</th>
            <th className="px-3 py-2">Delivery</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {rows.map((row) => (
            <tr key={row.symbol ?? 'stock'} className="text-slate-200">
              <td className="px-3 py-2 font-medium">{row.symbol}</td>
              <td className="px-3 py-2">{row.sector ?? 'Other'}</td>
              <td className="px-3 py-2">{row.industry ?? 'Other'}</td>
              <td className="px-3 py-2"><Badge label={row.quadrant ?? 'Lagging'} /></td>
              <td className="px-3 py-2 text-right">{fmt(row.rotation_adjusted_score)}</td>
              <td className="px-3 py-2">{row.delivery_signal ?? 'Neutral'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DeliveryTable({
  accumulation,
  distribution,
  activeTab,
  onTabChange,
}: {
  accumulation: DeliverySignalRow[];
  distribution: DeliverySignalRow[];
  activeTab: 'accumulation' | 'distribution';
  onTabChange: (value: 'accumulation' | 'distribution') => void;
}) {
  const rows = activeTab === 'accumulation' ? accumulation : distribution;
  return (
    <div>
      <div className="mb-3 inline-flex rounded-md border border-slate-700 bg-slate-950 p-1">
        {([
          ['accumulation', `Accumulation (${accumulation.length})`],
          ['distribution', `Distribution (${distribution.length})`],
        ] as const).map(([value, label]) => (
          <button
            key={value}
            type="button"
            className={cn('rounded px-3 py-1.5 text-sm', activeTab === value ? 'bg-slate-100 text-slate-950' : 'text-slate-300 hover:bg-slate-800')}
            onClick={() => onTabChange(value)}
          >
            {label}
          </button>
        ))}
      </div>
      {rows.length === 0 ? <EmptyState message={`No ${activeTab} signals.`} /> : (
      <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-3 py-2">Symbol</th>
            <th className="px-3 py-2">Signal</th>
            <th className="px-3 py-2 text-right">Delivery Z</th>
            <th className="px-3 py-2 text-right">Volume Z</th>
            <th className="px-3 py-2 text-right">5D Return</th>
            <th className="px-3 py-2 text-right">Confidence</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {rows.map((row) => (
            <tr key={`${row.symbol}-${row.delivery_signal}`} className="text-slate-200">
              <td className="px-3 py-2 font-medium">{row.symbol}</td>
              <td className="px-3 py-2">{row.delivery_signal}</td>
              <td className="px-3 py-2 text-right">{fmt(row.delivery_pct_z20)}</td>
              <td className="px-3 py-2 text-right">{fmt(row.volume_z20)}</td>
              <td className="px-3 py-2 text-right">{pct(row.price_return_5d)}</td>
              <td className="px-3 py-2 text-right">{fmt(row.accumulation_score)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      </div>
      )}
    </div>
  );
}

function IndexTable({ rows }: { rows: SectorCustomIndexRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-left text-sm">
        <thead className="text-xs uppercase text-slate-500">
          <tr>
            <th className="px-3 py-2">Date</th>
            <th className="px-3 py-2">Industry</th>
            <th className="px-3 py-2">Parent</th>
            <th className="px-3 py-2 text-right">Index</th>
            <th className="px-3 py-2">Weighting</th>
            <th className="px-3 py-2 text-right">Constituents</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {rows.map((row) => (
            <tr key={`${row.date}-${row.rotation_group_name ?? row.industry}`} className="text-slate-200">
              <td className="px-3 py-2">{row.date}</td>
              <td className="px-3 py-2 font-medium">{row.rotation_group_name ?? row.industry}</td>
              <td className="px-3 py-2">{row.parent_sector ?? row.sector}</td>
              <td className="px-3 py-2 text-right">{fmt(row.rotation_index ?? row.sector_index)}</td>
              <td className="px-3 py-2">{row.weighting_method ?? 'equal_weight'}</td>
              <td className="px-3 py-2 text-right">{row.constituent_count ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Badge({ label }: { label: string }) {
  return (
    <span className={cn('inline-flex rounded-full border px-2 py-1 text-xs font-medium', quadrantTone[label] ?? 'border-slate-700 bg-slate-900 text-slate-300')}>
      {label}
    </span>
  );
}

function latestIndexRows(rows: SectorCustomIndexRow[]): SectorCustomIndexRow[] {
  const byGroup = new Map<string, SectorCustomIndexRow>();
  rows.forEach((row) => {
    const key = row.rotation_group_name ?? row.industry ?? '';
    if (!key) return;
    const current = byGroup.get(key);
    if (!current || String(row.date ?? '') >= String(current.date ?? '')) {
      byGroup.set(key, row);
    }
  });
  return Array.from(byGroup.values()).sort((a, b) => String(a.rotation_group_name ?? a.industry ?? '').localeCompare(String(b.rotation_group_name ?? b.industry ?? '')));
}

function groupName(row: SectorRotationRow): string {
  return row.rotation_group_name ?? row.industry ?? row.sector ?? 'Group';
}

function hasRrgPoint(row: SectorRotationRow): boolean {
  return toFiniteNumber(row.rs_ratio) !== null && toFiniteNumber(row.rs_momentum) !== null;
}

function pointDistance(row: SectorRotationRow): number {
  const ratio = toFiniteNumber(row.rs_ratio) ?? 100;
  const momentum = toFiniteNumber(row.rs_momentum) ?? 100;
  return Math.hypot(ratio - 100, momentum - 100);
}

function stockMatchesGroup(stock: StockRotationRow, group: SectorRotationRow): boolean {
  const name = groupName(group);
  if (group.rotation_group_type === 'industry') return stock.industry === name;
  if (group.rotation_group_type === 'sector') return stock.sector === name || stock.sector === group.parent_sector;
  return stock.industry === name || stock.sector === name || stock.sector === group.parent_sector;
}

function filterDeliveryRows(rows: DeliverySignalRow[], symbols: Set<string | null | undefined> | null): DeliverySignalRow[] {
  if (!symbols) return rows;
  if (symbols.size === 0) return [];
  return rows.filter((row) => symbols.has(row.symbol));
}

function fmt(value: number | null | undefined): string {
  const num = toFiniteNumber(value);
  return num !== null ? num.toFixed(2) : 'N/A';
}

function pct(value: number | null | undefined): string {
  const num = toFiniteNumber(value);
  return num !== null ? `${(num * 100).toFixed(2)}%` : 'N/A';
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === '') return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}
