/**
 * Ranking page (PR #8 + PR #12 wiring; rail added per Quantis proposal #01).
 *
 * Wires the ranked-signal list, multi-facet filter rail, expandable rows,
 * and the comparison tray into a single Canvas-style view. Filter state
 * is the new ``RankingFilterState`` shape and lives in page state; saved
 * views persist to ``localStorage``.
 */
import { useEffect, useMemo, useState } from 'react';

import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { TableSkeleton } from '@/components/common/LoadingSkeleton';
import RankingTable from '@/components/tables/RankingTable';
import FilterRail from '@/components/ranking/FilterRail';
import ActiveFilterChips from '@/components/ranking/ActiveFilterChips';
import ComparisonTray, { COMPARISON_LIMIT } from '@/components/ranking/ComparisonTray';
import { useWorkspace } from '@/components/workspace/WorkspaceContext';
import { useRanking } from '@/lib/queries';
import {
  DEFAULT_FILTER_STATE,
  loadCustomViews,
  saveCustomViews,
  type RankingFilterState,
  type SavedView,
} from '@/lib/storage/rankingViews';
import type { StockRow } from '@/types/dashboard';

function applyFilter(
  rows: StockRow[],
  state: RankingFilterState,
  search: string,
): StockRow[] {
  const needle = search.trim().toLowerCase();
  const [scoreLo, scoreHi] = state.scoreRange;
  const [rsLo, rsHi] = state.rsRange;
  return rows.filter((row) => {
    if (row.score < scoreLo || row.score > scoreHi) return false;
    if (row.rs < rsLo || row.rs > rsHi) return false;
    if (state.tiers.length > 0 && !state.tiers.includes(row.tier)) return false;
    if (state.sectors.length > 0 && !state.sectors.includes(row.sector ?? '')) return false;
    if (state.breakoutOnly && !row.breakout) return false;
    if (state.hasPatternOnly && (!row.pattern || row.pattern === 'N/A')) return false;
    if (needle === '') return true;
    return (
      row.symbol.toLowerCase().includes(needle) ||
      (row.sector ?? '').toLowerCase().includes(needle)
    );
  });
}

function RankingContent() {
  const { data, isLoading, error, refetch } = useRanking();
  const workspace = useWorkspace();

  const [filterState, setFilterState] = useState<RankingFilterState>(DEFAULT_FILTER_STATE);
  const [search, setSearch] = useState('');
  const [expandedSymbol, setExpandedSymbol] = useState<string | null>(null);
  const [pendingNotice, setPendingNotice] = useState<string | null>(null);
  const [customViews, setCustomViews] = useState<SavedView[]>(() => loadCustomViews());

  useEffect(() => {
    saveCustomViews(customViews);
  }, [customViews]);

  const rows = data?.rows ?? [];
  const filtered = useMemo(
    () => applyFilter(rows, filterState, search),
    [rows, filterState, search],
  );
  const comparedSymbols = useMemo(
    () => new Set(workspace.compareSymbols),
    [workspace.compareSymbols],
  );

  const comparedRows = useMemo(
    () =>
      workspace.compareSymbols
        .map((symbol) => rows.find((r) => r.symbol === symbol))
        .filter((r): r is StockRow => Boolean(r)),
    [workspace.compareSymbols, rows],
  );

  const handleToggleExpand = (symbol: string) => {
    setExpandedSymbol((current) => (current === symbol ? null : symbol));
  };

  const handleToggleCompare = (row: StockRow) => {
    if (
      !workspace.compareSymbols.includes(row.symbol) &&
      workspace.compareSymbols.length >= COMPARISON_LIMIT
    ) {
      setPendingNotice(`Compare limited to ${COMPARISON_LIMIT} symbols.`);
      window.setTimeout(() => setPendingNotice(null), 2500);
      return;
    }
    workspace.toggleCompare(row.symbol);
  };

  const handleSaveView = (name: string) => {
    const id = `custom-${Date.now().toString(36)}`;
    setCustomViews((prev) => [...prev, { id, name, state: filterState }]);
  };

  const handleDeleteView = (id: string) => {
    setCustomViews((prev) => prev.filter((v) => v.id !== id));
  };

  const description =
    'Tier-aware ranked signals with factor decomposition, lifecycle, and rank history.';

  if (isLoading) {
    return (
      <PageFrame title="Ranking" description={description}>
        <SectionCard title="Ranked Signals">
          <TableSkeleton rows={10} />
        </SectionCard>
      </PageFrame>
    );
  }

  if (error) {
    return (
      <PageFrame title="Ranking" description={description}>
        <SectionCard title="Ranked Signals">
          <ErrorStateView
            error={`Failed to load ranking: ${error.message}`}
            onRetry={() => refetch()}
          />
        </SectionCard>
      </PageFrame>
    );
  }

  if (rows.length === 0) {
    return (
      <PageFrame title="Ranking" description={description}>
        <SectionCard title="Ranked Signals">
          <EmptyState message="No ranked signals available" />
        </SectionCard>
      </PageFrame>
    );
  }

  return (
    <PageFrame title="Ranking" description={description}>
      <SectionCard title="Ranked Signals">
        <div className="grid gap-4 lg:grid-cols-[240px_minmax(0,1fr)]">
          <FilterRail
            state={filterState}
            onChange={setFilterState}
            rows={rows}
            customViews={customViews}
            onSaveView={handleSaveView}
            onDeleteView={handleDeleteView}
            onSelectView={(view) => setFilterState(view.state)}
          />

          <div className="flex min-w-0 flex-col gap-3">
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
              <label className="relative w-full sm:max-w-xs">
                <span className="sr-only">Search ranked symbols</span>
                <input
                  type="search"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search symbol or sector…"
                  className="w-full rounded-lg border border-slate-700 bg-slate-950/60 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-500 focus:border-blue-500/60 focus:outline-none"
                />
              </label>
            </div>

            <ActiveFilterChips
              state={filterState}
              onChange={setFilterState}
              matched={filtered.length}
              total={rows.length}
            />

            {filtered.length === 0 ? (
              <EmptyState message="No symbols match the current filter." />
            ) : (
              <RankingTable
                rows={filtered}
                expandedSymbol={expandedSymbol}
                onToggleExpand={handleToggleExpand}
                comparedSymbols={comparedSymbols}
                onToggleCompare={handleToggleCompare}
              />
            )}
          </div>
        </div>
      </SectionCard>

      <ComparisonTray
        rows={comparedRows}
        onRemove={(symbol) => workspace.toggleCompare(symbol)}
        onClear={workspace.clearCompare}
        onCompare={workspace.openCompare}
        pendingCompareNotice={pendingNotice}
      />
    </PageFrame>
  );
}

export default function RankingPage() {
  return (
    <PageErrorBoundary title="Ranking" description="Failed to load ranking page">
      <RankingContent />
    </PageErrorBoundary>
  );
}
