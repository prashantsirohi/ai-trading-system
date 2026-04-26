/**
 * Ranking page (PR #8 + PR #12 wiring).
 *
 * Wires the ranked-signal list, filter chip bar, expandable rows, and the
 * comparison tray into a single Canvas-style view. The expansion panel
 * itself fetches per-symbol detail/history lazily, so this page only owns
 * the list-level query plus filter / expand state.
 *
 * Compare-Factors selection now lives in WorkspaceContext (PR #12) so the
 * Compare modal can be opened from any page; the Ranking-page tray is the
 * primary entry point but the global tray launcher is also available.
 */
import { useMemo, useState } from 'react';

import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import EmptyState from '@/components/common/EmptyState';
import ErrorStateView from '@/components/common/ErrorState';
import { TableSkeleton } from '@/components/common/LoadingSkeleton';
import RankingTable from '@/components/tables/RankingTable';
import FilterChipBar, { type RankingFilter } from '@/components/ranking/FilterChipBar';
import ComparisonTray, { COMPARISON_LIMIT } from '@/components/ranking/ComparisonTray';
import { useWorkspace } from '@/components/workspace/WorkspaceContext';
import { useRanking } from '@/lib/queries';
import type { StockRow } from '@/types/dashboard';

function applyFilter(rows: StockRow[], filter: RankingFilter, search: string): StockRow[] {
  const needle = search.trim().toLowerCase();
  return rows.filter((row) => {
    if (filter === 'tier-a' && row.tier !== 'A') return false;
    if (filter === 'breakouts' && !row.breakout) return false;
    if (filter === 'patterns' && (!row.pattern || row.pattern === 'N/A')) return false;
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

  const [filter, setFilter] = useState<RankingFilter>('all');
  const [search, setSearch] = useState('');
  const [expandedSymbol, setExpandedSymbol] = useState<string | null>(null);
  const [pendingNotice, setPendingNotice] = useState<string | null>(null);

  const rows = data?.rows ?? [];
  const filtered = useMemo(() => applyFilter(rows, filter, search), [rows, filter, search]);
  const comparedSymbols = useMemo(
    () => new Set(workspace.compareSymbols),
    [workspace.compareSymbols],
  );

  // The Ranking-page tray expects ``StockRow`` records — resolve the symbols
  // pinned in WorkspaceContext back to the full rows for display.
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

  if (isLoading) {
    return (
      <PageFrame
        title="Ranking"
        description="Tier-aware ranked signals with factor decomposition, lifecycle, and rank history."
      >
        <SectionCard title="Ranked Signals">
          <TableSkeleton rows={10} />
        </SectionCard>
      </PageFrame>
    );
  }

  if (error) {
    return (
      <PageFrame
        title="Ranking"
        description="Tier-aware ranked signals with factor decomposition, lifecycle, and rank history."
      >
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
      <PageFrame
        title="Ranking"
        description="Tier-aware ranked signals with factor decomposition, lifecycle, and rank history."
      >
        <SectionCard title="Ranked Signals">
          <EmptyState message="No ranked signals available" />
        </SectionCard>
      </PageFrame>
    );
  }

  return (
    <PageFrame
      title="Ranking"
      description="Tier-aware ranked signals with factor decomposition, lifecycle, and rank history."
    >
      <SectionCard title="Ranked Signals">
        <div className="space-y-4">
          <FilterChipBar
            active={filter}
            onChange={setFilter}
            search={search}
            onSearchChange={setSearch}
            total={rows.length}
            matched={filtered.length}
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
