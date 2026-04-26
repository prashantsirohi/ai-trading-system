/**
 * Watchlist & Alerts page (Quantis proposal #02).
 *
 * Route: /watchlist — nav shortcut: g w.
 * Layout: two-column — watchlist table (1.4fr) + active alerts feed (1fr).
 * Below 1100px collapses to single column.
 *
 * Watchlist entries persist to localStorage. Fired-alert events seed from
 * the mock feed until a backend /alerts endpoint exists.
 */
import { useEffect, useMemo, useState } from 'react';
import { BookmarkSlashIcon } from '@heroicons/react/24/outline';

import PageErrorBoundary from '@/components/common/PageErrorBoundary';
import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import { useRanking } from '@/lib/queries';
import { loadWatchlist, saveWatchlist, type AlertFiredEvent, type WatchlistEntry } from '@/lib/storage/watchlist';
import { watchlistSeedEntries, alertsFeedSeed } from '@/lib/mock/watchlist';
import WatchlistTable from '@/components/watchlist/WatchlistTable';
import AlertsFeed from '@/components/watchlist/AlertsFeed';
import AlertRuleDrawer from '@/components/watchlist/AlertRuleDrawer';

function seedIfEmpty(stored: WatchlistEntry[]): WatchlistEntry[] {
  return stored.length > 0 ? stored : watchlistSeedEntries;
}

function WatchlistContent() {
  const { data } = useRanking();
  const rankingRows = data?.rows ?? [];

  const [entries, setEntries] = useState<WatchlistEntry[]>(() =>
    seedIfEmpty(loadWatchlist()),
  );
  const [alerts, setAlerts] = useState<AlertFiredEvent[]>(alertsFeedSeed);
  const [drawerSymbol, setDrawerSymbol] = useState<string | null>(null);

  useEffect(() => {
    saveWatchlist(entries);
  }, [entries]);

  const drawerEntry = useMemo(
    () => entries.find((e) => e.symbol === drawerSymbol) ?? null,
    [entries, drawerSymbol],
  );

  const firingCount = useMemo(
    () =>
      new Set(
        alerts
          .filter((a) => !a.muted && !a.snoozedUntil)
          .flatMap((a) => a.symbols),
      ).size,
    [alerts],
  );

  const handleRemove = (symbol: string) => {
    setEntries((prev) => prev.filter((e) => e.symbol !== symbol));
  };

  const handleUpdateEntry = (updated: WatchlistEntry) => {
    setEntries((prev) => prev.map((e) => (e.symbol === updated.symbol ? updated : e)));
  };

  const handleMute = (id: string) => {
    setAlerts((prev) => prev.map((a) => (a.id === id ? { ...a, muted: true } : a)));
  };

  const handleSnooze = (id: string) => {
    const until = new Date(Date.now() + 60 * 60 * 1000).toISOString();
    setAlerts((prev) =>
      prev.map((a) => (a.id === id ? { ...a, snoozedUntil: until } : a)),
    );
  };

  const handleDelete = (id: string) => {
    setAlerts((prev) => prev.filter((a) => a.id !== id));
  };

  return (
    <PageFrame
      title="Watchlist"
      description="Saved symbol baskets with rule-based alerts. Fires to in-app feed, Slack, or email."
    >
      <div className="grid gap-4 xl:grid-cols-[1.4fr_1fr]">
        {/* Watchlist table */}
        <SectionCard
          title="Watched Symbols"
          description={
            entries.length > 0
              ? `${entries.length} symbols · ${firingCount} currently firing`
              : undefined
          }
        >
          <WatchlistTable
            entries={entries}
            rankingRows={rankingRows}
            alerts={alerts}
            onRemove={handleRemove}
            onManageRules={(symbol) => setDrawerSymbol(symbol)}
          />
          {entries.length > 0 && (
            <div className="mt-4 flex items-center justify-between border-t border-slate-800 pt-3">
              <p className="text-[11px] text-slate-500">
                To add symbols, press{' '}
                <kbd className="rounded border border-slate-700 px-1 py-0.5 font-mono text-[10px]">
                  w
                </kbd>{' '}
                on any ranking row.
              </p>
              <button
                type="button"
                onClick={() => setEntries([])}
                className="flex items-center gap-1.5 rounded-md border border-slate-700 px-2.5 py-1 text-[11px] text-slate-400 hover:border-rose-500/50 hover:text-rose-300"
              >
                <BookmarkSlashIcon className="h-3 w-3" />
                Clear all
              </button>
            </div>
          )}
        </SectionCard>

        {/* Alerts feed */}
        <SectionCard
          title="Active Alerts"
          description={
            alerts.filter((a) => !a.muted).length > 0
              ? `${alerts.filter((a) => !a.muted).length} unread`
              : 'No active alerts'
          }
        >
          <AlertsFeed
            alerts={alerts}
            onMute={handleMute}
            onSnooze={handleSnooze}
            onDelete={handleDelete}
          />
        </SectionCard>
      </div>

      {/* Rule editor drawer */}
      <AlertRuleDrawer
        entry={drawerEntry}
        onClose={() => setDrawerSymbol(null)}
        onUpdateEntry={handleUpdateEntry}
      />
    </PageFrame>
  );
}

export default function WatchlistPage() {
  return (
    <PageErrorBoundary title="Watchlist" description="Failed to load watchlist page">
      <WatchlistContent />
    </PageErrorBoundary>
  );
}
