/**
 * Stock Detail Workspace — full-screen modal driven by ``WorkspaceContext``.
 *
 * Tabs:
 *   * Overview — quote + ranking + lifecycle + metadata.
 *   * Auto-Chart — full-size price + delivery from ``/stocks/{symbol}/ohlcv``.
 *   * Decision Trace — verdict + per-bucket factor contributions
 *     (re-uses VerdictBanner from PR #8).
 *   * Pattern History — placeholder using the lifecycle pattern label
 *     until a per-symbol pattern history endpoint exists.
 *   * Risk & Scenarios — three derive.ts scenarios (base / aggressive /
 *     conservative) tabulated.
 *
 * Keyboard:
 *   * ``Esc`` — close.
 *   * ``c`` — toggle Compare on the current symbol.
 *   * ``e`` — jump to /execution.
 *   * ``s`` — jump to /sectors.
 */
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';

import { useStockDetail, useStockOhlcv, useRanking, useRankingDetail } from '@/lib/queries';
import { useWorkspace } from './WorkspaceContext';
import OverviewTab from './OverviewTab';
import AutoChart from './AutoChart';
import DecisionTraceTab from './DecisionTraceTab';
import RiskScenariosTab from './RiskScenariosTab';
import { cn } from '@/lib/utils/cn';

type TabKey = 'overview' | 'chart' | 'decision' | 'pattern' | 'risk';

const TABS: Array<{ key: TabKey; label: string }> = [
  { key: 'overview', label: 'Overview' },
  { key: 'chart', label: 'Auto-Chart' },
  { key: 'decision', label: 'Decision Trace' },
  { key: 'pattern', label: 'Pattern History' },
  { key: 'risk', label: 'Risk & Scenarios' },
];

export default function StockDetailWorkspace() {
  const navigate = useNavigate();
  const { workspaceSymbol, closeWorkspace, compareSymbols, toggleCompare } = useWorkspace();
  const [tab, setTab] = useState<TabKey>('overview');

  // Reset to Overview every time a new symbol is opened.
  useEffect(() => {
    if (workspaceSymbol) setTab('overview');
  }, [workspaceSymbol]);

  const detailQuery = useStockDetail(workspaceSymbol);
  const ohlcvQuery = useStockOhlcv(workspaceSymbol, 180);
  const rankingDetailQuery = useRankingDetail(workspaceSymbol);
  const rankingQuery = useRanking();

  const row = useMemo(() => {
    if (!workspaceSymbol) return null;
    return rankingQuery.data?.rows.find((r) => r.symbol === workspaceSymbol) ?? null;
  }, [workspaceSymbol, rankingQuery.data]);

  // Keyboard shortcuts (Esc / c / e / s).
  useEffect(() => {
    if (!workspaceSymbol) return;
    function onKey(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      const inEditable =
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target?.isContentEditable;
      if (event.key === 'Escape') {
        event.preventDefault();
        closeWorkspace();
        return;
      }
      if (inEditable) return;
      if (event.key === 'c' && workspaceSymbol) {
        event.preventDefault();
        toggleCompare(workspaceSymbol);
      } else if (event.key === 'e') {
        event.preventDefault();
        closeWorkspace();
        navigate('/execution');
      } else if (event.key === 's') {
        event.preventDefault();
        closeWorkspace();
        navigate('/sectors');
      }
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [workspaceSymbol, closeWorkspace, toggleCompare, navigate]);

  if (!workspaceSymbol) return null;

  const detail = detailQuery.data;
  const compared = compareSymbols.includes(workspaceSymbol);

  return (
    <div className="fixed inset-0 z-40 flex flex-col bg-slate-950/95 backdrop-blur-sm">
      <header className="flex flex-wrap items-center gap-3 border-b border-slate-800 px-6 py-4">
        <div className="flex flex-col">
          <p className="text-[11px] uppercase tracking-widest text-slate-500">
            Stock Detail Workspace
          </p>
          <h2 className="font-mono text-2xl text-slate-100">{workspaceSymbol}</h2>
        </div>
        {detail?.metadata?.symbolName ? (
          <p className="hidden text-sm text-slate-400 sm:block">{detail.metadata.symbolName}</p>
        ) : null}
        <nav className="ml-auto flex flex-wrap items-center gap-1">
          {TABS.map((t) => (
            <button
              key={t.key}
              type="button"
              onClick={() => setTab(t.key)}
              className={cn(
                'rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-wider transition-colors',
                tab === t.key
                  ? 'border-blue-500/60 bg-blue-500/15 text-blue-100'
                  : 'border-slate-700 bg-slate-900/60 text-slate-300 hover:border-slate-500',
              )}
            >
              {t.label}
            </button>
          ))}
        </nav>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => toggleCompare(workspaceSymbol)}
            className={cn(
              'rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-wider transition-colors',
              compared
                ? 'border-emerald-500/60 bg-emerald-500/15 text-emerald-100'
                : 'border-slate-700 bg-slate-900/60 text-slate-300 hover:border-slate-500',
            )}
            title="Toggle compare (c)"
          >
            {compared ? 'In Compare' : 'Compare (c)'}
          </button>
          <button
            type="button"
            onClick={closeWorkspace}
            className="rounded-full border border-slate-700 bg-slate-900/60 px-3 py-1 text-xs font-semibold uppercase tracking-wider text-slate-300 hover:border-rose-500/60 hover:text-rose-200"
            title="Close (Esc)"
          >
            Close ✕
          </button>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-6 py-5">
        {detailQuery.isLoading ? (
          <p className="text-sm text-slate-500">Loading workspace…</p>
        ) : tab === 'overview' ? (
          <OverviewTab detail={detail ?? null} />
        ) : tab === 'chart' ? (
          <AutoChart data={ohlcvQuery.data} isLoading={ohlcvQuery.isLoading} />
        ) : tab === 'decision' ? (
          <DecisionTraceTab
            detail={rankingDetailQuery.data}
            isLoading={rankingDetailQuery.isLoading}
          />
        ) : tab === 'pattern' ? (
          <PatternHistoryStub
            patternLabel={detail?.lifecycle.pattern ?? 'NONE'}
            patternInScan={detail?.ranking?.inPatternScan ?? false}
          />
        ) : (
          <RiskScenariosTab row={row} />
        )}
      </div>

      <footer className="border-t border-slate-800 px-6 py-2 text-[10px] uppercase tracking-widest text-slate-500">
        Esc · close &nbsp; / &nbsp; c · compare &nbsp; / &nbsp; e · execution &nbsp; / &nbsp; s · sectors
      </footer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Pattern history stub — full pattern timeline lands when a per-symbol pattern
// endpoint exists. For now we surface the current lifecycle label + scan flag.
// ---------------------------------------------------------------------------

function PatternHistoryStub({
  patternLabel,
  patternInScan,
}: {
  patternLabel: string;
  patternInScan: boolean;
}) {
  return (
    <div className="space-y-3">
      <div className="rounded-2xl border border-slate-800 bg-slate-950/40 p-4">
        <p className="text-[11px] uppercase tracking-widest text-slate-500">Current pattern</p>
        <p className="mt-1 text-lg font-semibold text-slate-100">{patternLabel}</p>
        <p className="mt-1 text-xs text-slate-400">
          In active pattern scan today: {patternInScan ? 'yes' : 'no'}.
        </p>
      </div>
      <p className="text-xs text-slate-500">
        A per-symbol pattern history endpoint isn't wired yet — we display the current pattern
        label only. When ``/stocks/&#123;symbol&#125;/patterns`` lands, this tab will render the
        full chronological history.
      </p>
    </div>
  );
}
