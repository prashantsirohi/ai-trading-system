/**
 * Pure helpers that turn a list of `StockRow`s into the synthetic execution
 * data the Canvas design surfaces.
 *
 * The execution backend doesn't yet emit per-symbol entry/stop/target/size
 * values — until a routing endpoint lands we derive plausible numbers from
 * the row's price + tier + score. Every derivation here is documented so
 * the swap-out is mechanical when the backend exists.
 */
import type { StockRow } from '@/types/dashboard';

export type ExecutionBucket = 'eligible' | 'watchlist' | 'blocked';

export function bucketFor(row: StockRow): ExecutionBucket {
  // Eligible — the routing loop would actually send these as orders.
  if (row.tier === 'A' && row.breakout) return 'eligible';
  // Blocked — Tier-C or weak sector rules them out today.
  if (row.tier === 'C' || row.sectorStrength < 55) return 'blocked';
  // Everything in between is a watchlist candidate.
  return 'watchlist';
}

export interface DerivedOrder {
  symbol: string;
  sector: string;
  tier: StockRow['tier'];
  entry: number;
  stop: number;
  target: number;
  riskReward: number;
  /** Position size as a percent of total capital. */
  sizePct: number;
  confidence: number;
  bucket: ExecutionBucket;
}

const BASE_STOP_PCT = 0.04;
const BASE_TARGET_PCT = 0.10;

export function deriveOrder(row: StockRow): DerivedOrder {
  const entry = row.price > 0 ? row.price : 100;
  // Stop tightens for higher-RS names, widens for weak ones.
  const stopPct = BASE_STOP_PCT + (90 - Math.min(90, row.rs)) * 0.0008;
  const targetPct = BASE_TARGET_PCT + (row.score - 7) * 0.015;
  const stop = +(entry * (1 - stopPct)).toFixed(2);
  const target = +(entry * (1 + Math.max(targetPct, 0.05))).toFixed(2);
  const risk = entry - stop;
  const reward = target - entry;
  const riskReward = risk > 0 ? +(reward / risk).toFixed(2) : 0;
  const tierWeight = row.tier === 'A' ? 1 : row.tier === 'B' ? 0.6 : 0.3;
  // Capital allocation max 6% per name, scaled by tier and sector strength.
  const sizePct = +(Math.min(6, 6 * tierWeight * (row.sectorStrength / 100))).toFixed(2);
  // Confidence: blend of score and sector strength, clamped 0..100.
  const confidence = Math.max(
    0,
    Math.min(100, Math.round((row.score / 10) * 60 + (row.sectorStrength / 100) * 40)),
  );
  return {
    symbol: row.symbol,
    sector: row.sector,
    tier: row.tier,
    entry: +entry.toFixed(2),
    stop,
    target,
    riskReward,
    sizePct,
    confidence,
    bucket: bucketFor(row),
  };
}

export interface ExecutionDerived {
  buckets: Record<ExecutionBucket, StockRow[]>;
  orders: DerivedOrder[];
  capitalUsedPct: number;
  topSector: { name: string; pct: number } | null;
  concentrationPct: number;
  estMaxDrawdownPct: number;
}

export function deriveExecution(rows: StockRow[]): ExecutionDerived {
  const buckets: Record<ExecutionBucket, StockRow[]> = {
    eligible: [],
    watchlist: [],
    blocked: [],
  };
  for (const row of rows) {
    buckets[bucketFor(row)].push(row);
  }
  const orders = buckets.eligible.map(deriveOrder);

  // Capital used = sum of eligible size%, capped at 100.
  const capitalUsedPct = Math.min(
    100,
    +orders.reduce((acc, o) => acc + o.sizePct, 0).toFixed(2),
  );

  // Top sector exposure: dominant sector's share of *eligible* size.
  const sectorTotals = new Map<string, number>();
  for (const order of orders) {
    sectorTotals.set(order.sector, (sectorTotals.get(order.sector) ?? 0) + order.sizePct);
  }
  let topSector: { name: string; pct: number } | null = null;
  for (const [name, total] of sectorTotals.entries()) {
    if (!topSector || total > topSector.pct) {
      topSector = { name, pct: +total.toFixed(2) };
    }
  }

  // Concentration: top symbol's share of capital used.
  const concentrationPct = orders.length
    ? +Math.max(...orders.map((o) => o.sizePct)).toFixed(2)
    : 0;

  // Naive max drawdown estimate: sum of stop distances weighted by size.
  // (entry - stop) / entry * sizePct, summed.
  const drawdown = orders.reduce(
    (acc, o) => acc + ((o.entry - o.stop) / o.entry) * o.sizePct,
    0,
  );
  const estMaxDrawdownPct = +drawdown.toFixed(2);

  return {
    buckets,
    orders,
    capitalUsedPct,
    topSector,
    concentrationPct,
    estMaxDrawdownPct,
  };
}
