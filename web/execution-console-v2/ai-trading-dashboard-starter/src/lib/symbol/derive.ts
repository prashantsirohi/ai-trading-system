/**
 * Client-side indicator derivation for the Symbol page (Proposal #10).
 *
 * Until a real /api/stocks/{sym}/indicators endpoint lands, we synthesise
 * plausible values from the data already available in StockRow + StockOhlcv.
 * Every derivation is documented so the swap-out is mechanical.
 */
import type { StockRow } from '@/types/dashboard';
import type { OhlcvCandle } from '@/lib/api/stocks';

export interface DerivedIndicators {
  rsi: number;         // 0–100 (derived from row.rs)
  macd: number;        // -1..+1 normalised histogram (derived from row.trend)
  stochK: number;      // 0–100 (derived from rs + breakout)
  adx: number;         // 0–100 (derived from row.score)
  atrPct: number;      // ATR as % of price (inverse of score)
  obvSlope: 'rising' | 'falling' | 'flat';
}

export interface DerivedMAs {
  ma50: (number | null)[];
  ma200: (number | null)[];
  high52w: number | null;
  low52w: number | null;
}

export function deriveIndicators(row: StockRow): DerivedIndicators {
  // RSI: rs correlates well with relative price strength → scale to 0-100
  const rsi = Math.round(Math.max(20, Math.min(85, row.rs * 0.85)));
  // MACD: trend gives direction; range row.trend ~ -1..+2 → normalise to -1..+1
  const macd = Math.max(-1, Math.min(1, row.trend / 2));
  // Stoch K: high-RS + breakout names tend to have high stochastics
  const stochK = Math.round(Math.min(95, row.rs * (row.breakout ? 0.95 : 0.75)));
  // ADX: score 8+ = strongly trending (ADX ~35); score 5 = weak trend (ADX ~20)
  const adx = Math.round(Math.max(12, Math.min(55, row.score * 4.5)));
  // ATR%: lower-score names tend to be more volatile relative to trend
  const atrPct = Math.max(0.5, Math.min(4.5, 3.5 - row.score * 0.25));
  // OBV slope: breakout + high sectorStrength = rising accumulation
  const obvSlope: DerivedIndicators['obvSlope'] =
    row.breakout && row.sectorStrength >= 70 ? 'rising'
    : row.sectorStrength < 50 ? 'falling'
    : 'flat';

  return { rsi, macd, stochK, adx, atrPct, obvSlope };
}

/** Compute simple moving averages from a close-price array. */
function sma(closes: number[], period: number): (number | null)[] {
  return closes.map((_, i) => {
    if (i < period - 1) return null;
    const slice = closes.slice(i - period + 1, i + 1);
    return +(slice.reduce((s, v) => s + v, 0) / period).toFixed(2);
  });
}

export function deriveMAs(candles: OhlcvCandle[]): DerivedMAs {
  const closes = candles.map((c) => c.close ?? 0).filter((v) => v > 0);
  const prices = candles.map((c) => c.close ?? null).filter((v): v is number => v !== null);

  const ma50 = sma(closes, 50);
  const ma200 = sma(closes, 200);
  const high52w = prices.length ? Math.max(...prices) : null;
  const low52w  = prices.length ? Math.min(...prices) : null;

  // Pad front to match candles length (closes might differ if some have null)
  const pad = candles.length - closes.length;
  const padded50  = [...new Array(pad).fill(null), ...ma50];
  const padded200 = [...new Array(pad).fill(null), ...ma200];

  return { ma50: padded50, ma200: padded200, high52w, low52w };
}
