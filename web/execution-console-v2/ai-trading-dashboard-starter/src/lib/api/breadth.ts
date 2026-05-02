import { fetchDashboardJsonStrict } from '@/lib/api/client';

export interface MarketBreadthPoint {
  date: string;
  above20: number;
  above50: number;
  above200: number;
}

interface BackendMarketBreadthPoint {
  date?: string;
  Date?: string;
  trade_date?: string;
  above20?: number;
  above50?: number;
  above200?: number;
  above_sma20?: number;
  above_sma50?: number;
  above_sma200?: number;
  pct_above_sma20?: number;
  pct_above_sma50?: number;
  pct_above_sma200?: number;
  symbols_sma20?: number;
  symbols_sma50?: number;
  symbols_sma200?: number;
  symbols_total?: number;
}

interface BackendMarketBreadthResponse {
  available?: boolean;
  row_count?: number;
  rows?: BackendMarketBreadthPoint[];
  breadth?: BackendMarketBreadthPoint[];
  market_breadth?: BackendMarketBreadthPoint[];
}

function asNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function asDate(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

function mapPoint(row: BackendMarketBreadthPoint): MarketBreadthPoint | null {
  const above20 = asNumber(row.above20 ?? row.above_sma20);
  const above50 = asNumber(row.above50 ?? row.above_sma50);
  const above200 = asNumber(row.above200 ?? row.above_sma200);
  const pct20 = asNumber(row.pct_above_sma20);
  const pct50 = asNumber(row.pct_above_sma50);
  const pct200 = asNumber(row.pct_above_sma200);
  const date = asDate(row.date ?? row.Date ?? row.trade_date);
  const symbols20 = asNumber(row.symbols_sma20);
  const symbols50 = asNumber(row.symbols_sma50);
  const symbols200 = asNumber(row.symbols_sma200);

  if (!date) return null;
  if (pct20 !== null && pct50 !== null && pct200 !== null) {
    return { date, above20: pct20, above50: pct50, above200: pct200 };
  }
  if (
    above20 !== null &&
    above50 !== null &&
    above200 !== null &&
    symbols20 &&
    symbols50 &&
    symbols200
  ) {
    return {
      date,
      above20: Number(((above20 / symbols20) * 100).toFixed(2)),
      above50: Number(((above50 / symbols50) * 100).toFixed(2)),
      above200: Number(((above200 / symbols200) * 100).toFixed(2)),
    };
  }
  return null;
}

export async function getMarketBreadth(limit = 30): Promise<MarketBreadthPoint[]> {
  const suffix = limit > 0 ? `?limit=${limit}` : '?limit=0';
  const raw = await fetchDashboardJsonStrict<BackendMarketBreadthResponse>(
    `/api/execution/market/breadth${suffix}`,
    { rows: [] },
  );
  const rows = raw.rows ?? raw.breadth ?? raw.market_breadth ?? [];
  const mapped = rows.map(mapPoint).filter((row): row is MarketBreadthPoint => row !== null);
  return mapped;
}
