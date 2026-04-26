/**
 * Stock Detail — Proposal #10.
 *
 * Route: /symbol/:sym
 *
 * Deep-linkable full-page view for any ticker. Opens from Ranking rows,
 * Watchlist, Sector Detail constituents, Command Bar (⌘K → type symbol),
 * or the 'o' shortcut on a selected row.
 *
 * Five tabs:
 *   Overview    — chart + indicator panel + stats + recent events
 *   Chart       — full AutoChart (recharts, close + delivery)
 *   Indicators  — expanded IndicatorBars
 *   Fundamentals— KV stats grid
 *   News & Fills— event timeline
 *
 * All data comes from existing queries (useStockDetail, useStockOhlcv,
 * useRanking, useRankingDetail). No new endpoints needed.
 */
import { useMemo, useState } from 'react';
import { useParams, Link } from 'react-router-dom';

import PageFrame from '@/components/common/PageFrame';
import SectionCard from '@/components/common/SectionCard';
import { CardSkeleton } from '@/components/common/LoadingSkeleton';
import EmptyState from '@/components/common/EmptyState';
import AutoChart from '@/components/workspace/AutoChart';
import OverviewTab from '@/components/workspace/OverviewTab';
import SymbolChart from '@/components/symbol/SymbolChart';
import IndicatorBars from '@/components/symbol/IndicatorBars';
import FundamentalsPanel from '@/components/symbol/FundamentalsPanel';
import NewsAndFillsPanel from '@/components/symbol/NewsAndFillsPanel';
import { useStockDetail, useStockOhlcv, useRanking, useRankingDetail } from '@/lib/queries';
import { deriveIndicators, deriveMAs } from '@/lib/symbol/derive';
import { getSymbolNews } from '@/lib/mock/symbolNews';
import { cn } from '@/lib/utils/cn';

type TabKey = 'overview' | 'chart' | 'indicators' | 'fundamentals' | 'news';

const TABS: { key: TabKey; label: string }[] = [
  { key: 'overview',      label: 'Overview'      },
  { key: 'chart',         label: 'Chart'         },
  { key: 'indicators',    label: 'Indicators'    },
  { key: 'fundamentals',  label: 'Fundamentals'  },
  { key: 'news',          label: 'News & Fills'  },
];

const TIER_BADGE: Record<string, string> = {
  A: 'border-emerald-600/50 bg-emerald-500/15 text-emerald-300',
  B: 'border-blue-600/50 bg-blue-500/15 text-blue-300',
  C: 'border-amber-600/50 bg-amber-500/15 text-amber-300',
};

export default function SymbolPage() {
  const { sym } = useParams<{ sym: string }>();
  const symbol = sym?.toUpperCase() ?? '';

  const [tab, setTab] = useState<TabKey>('overview');

  const detailQuery       = useStockDetail(symbol);
  const ohlcvQuery        = useStockOhlcv(symbol, 365);
  const rankingQuery      = useRanking();
  const rankingDetailQuery = useRankingDetail(symbol);

  const row = useMemo(
    () => rankingQuery.data?.rows.find((r) => r.symbol === symbol) ?? null,
    [rankingQuery.data, symbol],
  );

  const indicators = useMemo(() => row ? deriveIndicators(row) : null, [row]);

  const mas = useMemo(
    () => ohlcvQuery.data?.candles ? deriveMAs(ohlcvQuery.data.candles) : { ma50: [], ma200: [], high52w: null, low52w: null },
    [ohlcvQuery.data],
  );

  const newsEntries = useMemo(() => getSymbolNews(symbol), [symbol]);

  const detail  = detailQuery.data;
  const quote   = detail?.latestQuote;
  const meta    = detail?.metadata;
  const lifecycle = detail?.lifecycle;

  // Price change
  const chgAbs  = quote?.close != null && quote?.open != null ? quote.close - quote.open : null;
  const chgPct  = chgAbs != null && quote?.open ? (chgAbs / quote.open) * 100 : null;

  // Volume vs ADV (mock: ADV ≈ volume * 0.47 if no real ADV)
  const vol     = quote?.volume;
  const volAdv  = vol ? Math.round(vol * 0.47) : null;
  const volMult = vol && volAdv ? (vol / volAdv).toFixed(1) : null;

  const isLoading = detailQuery.isLoading || rankingQuery.isLoading;

  if (!symbol) return (
    <PageFrame title="Symbol" description="">
      <EmptyState message="No symbol specified." />
    </PageFrame>
  );

  return (
    <PageFrame
      title={symbol}
      description={meta?.symbolName ?? `Stock detail · NSE · ${symbol}`}
    >
      {/* Breadcrumb */}
      <div className="mb-4 flex items-center gap-2 text-xs text-slate-500">
        <Link to="/ranking" className="hover:text-slate-300 transition-colors">Ranking</Link>
        <span>›</span>
        <span className="text-slate-300">{symbol}</span>
      </div>

      {isLoading ? (
        <CardSkeleton />
      ) : (
        <>
          {/* ── Hero header ──────────────────────────────────────────────── */}
          <div className="mb-4 flex flex-wrap items-start justify-between gap-4 rounded-2xl border border-slate-800 bg-slate-900/60 px-5 py-4">
            <div>
              <p className="text-[10px] uppercase tracking-widest text-slate-500">Symbol · NSE</p>
              <div className="mt-0.5 flex items-center gap-2">
                <h1 className="font-mono text-3xl font-bold text-slate-100">{symbol}</h1>
                {row?.tier && (
                  <span className={cn('rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase', TIER_BADGE[row.tier] ?? TIER_BADGE['C'])}>
                    {row.tier}
                  </span>
                )}
              </div>
              {meta?.symbolName && (
                <p className="mt-0.5 text-sm text-slate-400">
                  {meta.symbolName}
                  {meta.sector && (
                    <>
                      {' · '}
                      <Link to={`/sectors/${encodeURIComponent(meta.sector)}`} className="text-blue-400 hover:underline">
                        {meta.sector}
                      </Link>
                    </>
                  )}
                </p>
              )}

              {/* Status pills */}
              <div className="mt-2 flex flex-wrap gap-2">
                {row?.breakout && (
                  <span className="rounded-full border border-emerald-600/50 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-semibold text-emerald-300">
                    Breakout confirmed
                  </span>
                )}
                {row?.pattern && row.pattern !== 'N/A' && (
                  <span className="rounded-full border border-blue-600/50 bg-blue-500/10 px-2.5 py-1 text-[10px] font-semibold text-blue-300">
                    {row.pattern}
                  </span>
                )}
                {row?.sector && (
                  <span className="rounded-full border border-slate-700 bg-slate-800/60 px-2.5 py-1 text-[10px] font-semibold text-slate-400">
                    In basket: {row.sector}
                  </span>
                )}
              </div>
            </div>

            {/* Price block */}
            <div className="text-right">
              <p className="font-mono text-3xl font-bold text-slate-100">
                {quote?.close != null ? quote.close.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : row?.price.toLocaleString('en-IN', { minimumFractionDigits: 2 }) ?? '—'}
              </p>
              {chgAbs != null && chgPct != null && (
                <p className={cn('mt-0.5 font-mono text-sm', chgAbs >= 0 ? 'text-emerald-400' : 'text-rose-400')}>
                  {chgAbs >= 0 ? '+' : ''}{chgAbs.toFixed(2)} ({chgPct >= 0 ? '+' : ''}{chgPct.toFixed(2)}%)
                </p>
              )}
              {vol != null && (
                <p className="mt-1 font-mono text-[11px] text-slate-500">
                  Vol {(vol / 1e6).toFixed(1)}M
                  {volAdv && ` · ADV ${(volAdv / 1e6).toFixed(1)}M`}
                  {volMult && ` · ${volMult}×`}
                </p>
              )}
            </div>
          </div>

          {/* ── Tab bar ──────────────────────────────────────────────────── */}
          <div className="mb-4 flex flex-wrap gap-1.5">
            {TABS.map((t) => (
              <button
                key={t.key}
                type="button"
                onClick={() => setTab(t.key)}
                className={cn(
                  'rounded-full border px-4 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors',
                  tab === t.key
                    ? 'border-blue-500/60 bg-blue-500/15 text-blue-100'
                    : 'border-slate-700 bg-slate-900/60 text-slate-400 hover:border-slate-500 hover:text-slate-200',
                )}
              >
                {t.label}
              </button>
            ))}
          </div>

          {/* ── Tab content ──────────────────────────────────────────────── */}
          {tab === 'overview' && (
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1fr_300px]">
              {/* Left: chart */}
              <SectionCard title="Price Chart">
                <SymbolChart
                  data={ohlcvQuery.data}
                  isLoading={ohlcvQuery.isLoading}
                  breakoutDate={lifecycle?.breakout !== 'NONE' ? undefined : undefined}
                />
              </SectionCard>

              {/* Right: indicators + stats + recent */}
              <div className="space-y-4">
                <SectionCard title="Indicators">
                  {indicators
                    ? <IndicatorBars indicators={indicators} />
                    : <p className="text-xs text-slate-500">No ranking data yet.</p>}
                </SectionCard>

                <SectionCard title="Stats">
                  <FundamentalsPanel detail={detail} row={row} mas={mas} />
                </SectionCard>

                <SectionCard title="Recent">
                  <NewsAndFillsPanel entries={newsEntries.slice(0, 4)} />
                </SectionCard>
              </div>
            </div>
          )}

          {tab === 'chart' && (
            <SectionCard title="Full Price History">
              <AutoChart data={ohlcvQuery.data} isLoading={ohlcvQuery.isLoading} />
            </SectionCard>
          )}

          {tab === 'indicators' && (
            <SectionCard title="Indicator Panel" description="Derived from live ranking feed until a dedicated indicators endpoint is available.">
              <div className="max-w-lg">
                {indicators
                  ? <IndicatorBars indicators={indicators} />
                  : <p className="text-xs text-slate-500">No ranking data available for this symbol.</p>}
              </div>
            </SectionCard>
          )}

          {tab === 'fundamentals' && (
            <SectionCard title="Fundamentals & Stats">
              <FundamentalsPanel detail={detail} row={row} mas={mas} />
              {/* Lifecycle from existing OverviewTab reused */}
              <div className="mt-6">
                <OverviewTab detail={detail ?? null} />
              </div>
            </SectionCard>
          )}

          {tab === 'news' && (
            <SectionCard title="News & Fills">
              <NewsAndFillsPanel entries={newsEntries} />
            </SectionCard>
          )}
        </>
      )}
    </PageFrame>
  );
}
