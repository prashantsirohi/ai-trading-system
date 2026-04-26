/**
 * Fetchers for the Stock Detail Workspace endpoints (PR #12):
 *
 *   * ``GET /api/execution/stocks/{symbol}`` — fundamentals, latest quote,
 *     ranking position, lifecycle.
 *   * ``GET /api/execution/stocks/{symbol}/ohlcv`` — daily OHLCV + delivery.
 *
 * Each call has an empty / "not available" fallback so the workspace can
 * render in mock mode and degrade gracefully when the backend is missing
 * one of the underlying data sources.
 */
import { fetchDashboardJsonStrict } from '@/lib/api/client';

// ---------------------------------------------------------------------------
// /stocks/{symbol}
// ---------------------------------------------------------------------------

export interface StockMetadata {
  symbolId: string | null;
  securityId: string | null;
  symbolName: string | null;
  exchange: string | null;
  instrumentType: string | null;
  isin: string | null;
  lotSize: number | null;
  tickSize: number | null;
  sector: string | null;
  industry: string | null;
  nseSymbol: string | null;
  bseSymbol: string | null;
  mcap: number | null;
  lastUpdated: string | null;
}

export interface StockQuote {
  timestamp: string | null;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
  deliveryPct: number | null;
}

export interface StockRanking {
  rankPosition: number | null;
  universeSize: number;
  compositeScore: number | null;
  sectorName: string | null;
  category: string | null;
  inBreakoutScan: boolean;
  inPatternScan: boolean;
}

export interface StockLifecycle {
  rank: string;
  breakout: string;
  pattern: string;
  execution: string;
}

export interface StockDetail {
  available: boolean;
  symbol: string;
  metadata: StockMetadata | null;
  latestQuote: StockQuote | null;
  ranking: StockRanking | null;
  lifecycle: StockLifecycle;
}

interface BackendStockDetail {
  available?: boolean;
  symbol?: string;
  metadata?: Record<string, string | number | boolean | null> | null;
  latest_quote?: Record<string, string | number | null> | null;
  ranking?: {
    rank_position?: number | null;
    universe_size?: number | null;
    composite_score?: number | null;
    sector_name?: string | null;
    category?: string | null;
    in_breakout_scan?: boolean | null;
    in_pattern_scan?: boolean | null;
  } | null;
  lifecycle?: { rank?: string; breakout?: string; pattern?: string; execution?: string } | null;
}

function asNum(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function asString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const str = String(value).trim();
  return str === '' ? null : str;
}

function mapMetadata(raw: BackendStockDetail['metadata']): StockMetadata | null {
  if (!raw) return null;
  return {
    symbolId: asString(raw.symbol_id),
    securityId: asString(raw.security_id),
    symbolName: asString(raw.symbol_name),
    exchange: asString(raw.exchange),
    instrumentType: asString(raw.instrument_type),
    isin: asString(raw.isin),
    lotSize: asNum(raw.lot_size),
    tickSize: asNum(raw.tick_size),
    sector: asString(raw.sector),
    industry: asString(raw.industry),
    nseSymbol: asString(raw.nse_symbol),
    bseSymbol: asString(raw.bse_symbol),
    mcap: asNum(raw.mcap),
    lastUpdated: asString(raw.last_updated),
  };
}

function mapQuote(raw: BackendStockDetail['latest_quote']): StockQuote | null {
  if (!raw) return null;
  return {
    timestamp: asString(raw.timestamp),
    open: asNum(raw.open),
    high: asNum(raw.high),
    low: asNum(raw.low),
    close: asNum(raw.close),
    volume: asNum(raw.volume),
    deliveryPct: asNum(raw.delivery_pct),
  };
}

function mapLifecycle(raw: BackendStockDetail['lifecycle']): StockLifecycle {
  return {
    rank: raw?.rank ?? 'OUT',
    breakout: raw?.breakout ?? 'NONE',
    pattern: raw?.pattern ?? 'NONE',
    execution: raw?.execution ?? 'OUT',
  };
}

export async function getStockDetail(symbol: string): Promise<StockDetail> {
  const raw = await fetchDashboardJsonStrict<BackendStockDetail>(
    `/api/execution/stocks/${encodeURIComponent(symbol)}`,
    { available: false, symbol },
  );
  return {
    available: Boolean(raw.available),
    symbol: asString(raw.symbol) ?? symbol,
    metadata: mapMetadata(raw.metadata),
    latestQuote: mapQuote(raw.latest_quote),
    ranking: raw.ranking
      ? {
          rankPosition: asNum(raw.ranking.rank_position),
          universeSize: asNum(raw.ranking.universe_size) ?? 0,
          compositeScore: asNum(raw.ranking.composite_score),
          sectorName: asString(raw.ranking.sector_name),
          category: asString(raw.ranking.category),
          inBreakoutScan: Boolean(raw.ranking.in_breakout_scan),
          inPatternScan: Boolean(raw.ranking.in_pattern_scan),
        }
      : null,
    lifecycle: mapLifecycle(raw.lifecycle),
  };
}

// ---------------------------------------------------------------------------
// /stocks/{symbol}/ohlcv
// ---------------------------------------------------------------------------

export interface OhlcvCandle {
  timestamp: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
  deliveryPct: number | null;
}

export interface StockOhlcv {
  available: boolean;
  symbol: string;
  interval: string;
  candles: OhlcvCandle[];
}

interface BackendStockOhlcv {
  available?: boolean;
  symbol?: string;
  interval?: string;
  candles?: Array<{
    timestamp?: string | null;
    open?: number | null;
    high?: number | null;
    low?: number | null;
    close?: number | null;
    volume?: number | null;
    delivery_pct?: number | null;
  }>;
}

export async function getStockOhlcv(symbol: string, limit = 180): Promise<StockOhlcv> {
  const raw = await fetchDashboardJsonStrict<BackendStockOhlcv>(
    `/api/execution/stocks/${encodeURIComponent(symbol)}/ohlcv?limit=${limit}`,
    { available: false, symbol, interval: 'daily', candles: [] },
  );
  return {
    available: Boolean(raw.available),
    symbol: asString(raw.symbol) ?? symbol,
    interval: raw.interval ?? 'daily',
    candles: (raw.candles ?? []).map((c) => ({
      timestamp: asString(c.timestamp) ?? '',
      open: asNum(c.open),
      high: asNum(c.high),
      low: asNum(c.low),
      close: asNum(c.close),
      volume: asNum(c.volume),
      deliveryPct: asNum(c.delivery_pct),
    })),
  };
}
