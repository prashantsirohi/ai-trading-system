/**
 * Mock sector-constituent data for Proposal #09 (Sector Detail page).
 *
 * Each constituent carries the seven indicator values shown in the table
 * (RSI, above-50DMA margin, MACD signal, volume multiplier) plus the
 * per-indicator boolean flags used by the filter rail. All values are
 * plausible NSE figures — swap with a real /api/sectors/:id endpoint later.
 */

export interface Constituent {
  symbol: string;
  price: number;
  chgPct: number;   // e.g. +2.45 means +2.45%
  rsi: number;
  ma50Pct: number;  // (px - ma50) / ma50 * 100 — positive = above
  macd: number;     // MACD histogram value
  volMult: number;  // volume / 5d ADV
  score: number;
  // Pre-computed indicator booleans
  aboveMa50: boolean;
  aboveMa200: boolean;
  goldenCross: boolean;
  rsiInRange: boolean;  // 40–70
  macdBullish: boolean;
  adxAbove20: boolean;
  bbSqueeze: boolean;
  atrRising: boolean;
  volExpand: boolean;   // volMult > 1.5
  obvRising: boolean;
  near52wHigh: boolean; // within 2%
  pivotTaken: boolean;
}

const SECTOR_DATA: Record<string, Constituent[]> = {
  Defence: [
    { symbol:'BEL',        price:326.10, chgPct:+2.45, rsi:68, ma50Pct:+8.4, macd:+0.42, volMult:2.1, score:7.98, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:true,  volExpand:true,  obvRising:true,  near52wHigh:true,  pivotTaken:true  },
    { symbol:'HAL',        price:4212.10,chgPct:+1.04, rsi:62, ma50Pct:+5.1, macd:+0.18, volMult:1.7, score:7.02, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:true  },
    { symbol:'MAZDOCK',    price:3870.55,chgPct:+1.81, rsi:66, ma50Pct:+6.7, macd:+0.31, volMult:2.4, score:7.41, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:true,  volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:true  },
    { symbol:'BHEL',       price:282.45, chgPct:+0.32, rsi:59, ma50Pct:+3.2, macd:+0.09, volMult:1.6, score:6.21, aboveMa50:true,  aboveMa200:true,  goldenCross:false, rsiInRange:true,  macdBullish:true,  adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:false, near52wHigh:false, pivotTaken:false },
    { symbol:'COCHINSHIP', price:1832.40,chgPct:+1.22, rsi:63, ma50Pct:+4.8, macd:+0.21, volMult:2.0, score:6.88, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:true,  volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:true  },
    { symbol:'DATAPATTNS', price:2345.70,chgPct:+0.94, rsi:61, ma50Pct:+4.0, macd:+0.12, volMult:1.5, score:6.55, aboveMa50:true,  aboveMa200:false, goldenCross:false, rsiInRange:true,  macdBullish:true,  adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:false, near52wHigh:false, pivotTaken:false },
    { symbol:'SOLARINDS',  price:8945.20,chgPct:-0.08, rsi:54, ma50Pct:+2.1, macd:-0.04, volMult:1.8, score:5.87, aboveMa50:true,  aboveMa200:true,  goldenCross:false, rsiInRange:true,  macdBullish:false, adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:false, near52wHigh:false, pivotTaken:false },
    { symbol:'BDL',        price:1422.10,chgPct:-0.41, rsi:38, ma50Pct:-1.2, macd:-0.18, volMult:0.9, score:4.20, aboveMa50:false, aboveMa200:false, goldenCross:false, rsiInRange:false, macdBullish:false, adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
  ],
  Energy: [
    { symbol:'RELIANCE',   price:2945.50,chgPct:+1.12, rsi:65, ma50Pct:+6.2, macd:+0.35, volMult:1.9, score:8.10, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:true,  volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:true  },
    { symbol:'ONGC',       price:284.30, chgPct:+0.65, rsi:58, ma50Pct:+3.4, macd:+0.11, volMult:1.4, score:6.40, aboveMa50:true,  aboveMa200:true,  goldenCross:false, rsiInRange:true,  macdBullish:true,  adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
    { symbol:'IOC',        price:162.45, chgPct:-0.22, rsi:45, ma50Pct:+1.1, macd:-0.05, volMult:1.2, score:5.10, aboveMa50:true,  aboveMa200:false, goldenCross:false, rsiInRange:true,  macdBullish:false, adxAbove20:false, bbSqueeze:true,  atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
  ],
  Banking: [
    { symbol:'HDFCBANK',   price:1885.20,chgPct:+0.88, rsi:60, ma50Pct:+4.1, macd:+0.22, volMult:1.6, score:7.35, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:false },
    { symbol:'ICICIBANK',  price:1245.80,chgPct:+1.34, rsi:67, ma50Pct:+5.8, macd:+0.28, volMult:2.0, score:7.90, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:true,  volExpand:true,  obvRising:true,  near52wHigh:true,  pivotTaken:true  },
    { symbol:'SBIN',       price:852.30, chgPct:+0.45, rsi:55, ma50Pct:+2.8, macd:+0.08, volMult:1.3, score:6.15, aboveMa50:true,  aboveMa200:true,  goldenCross:false, rsiInRange:true,  macdBullish:true,  adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
    { symbol:'AXISBANK',   price:1162.50,chgPct:-0.15, rsi:49, ma50Pct:+0.8, macd:-0.03, volMult:1.1, score:5.55, aboveMa50:true,  aboveMa200:false, goldenCross:false, rsiInRange:true,  macdBullish:false, adxAbove20:false, bbSqueeze:true,  atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
  ],
  IT: [
    { symbol:'INFY',       price:1648.20,chgPct:+0.76, rsi:62, ma50Pct:+4.5, macd:+0.19, volMult:1.7, score:7.20, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:false },
    { symbol:'TCS',        price:4021.10,chgPct:+0.32, rsi:54, ma50Pct:+2.0, macd:+0.06, volMult:1.2, score:6.50, aboveMa50:true,  aboveMa200:true,  goldenCross:false, rsiInRange:true,  macdBullish:true,  adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
    { symbol:'WIPRO',      price:528.40, chgPct:-0.44, rsi:43, ma50Pct:-0.5, macd:-0.12, volMult:0.9, score:4.80, aboveMa50:false, aboveMa200:false, goldenCross:false, rsiInRange:true,  macdBullish:false, adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
  ],
  Power: [
    { symbol:'NTPC',       price:372.85, chgPct:-0.18, rsi:52, ma50Pct:+1.5, macd:+0.04, volMult:1.3, score:5.80, aboveMa50:true,  aboveMa200:true,  goldenCross:false, rsiInRange:true,  macdBullish:true,  adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:false, obvRising:false, near52wHigh:false, pivotTaken:false },
    { symbol:'POWERGRID',  price:322.60, chgPct:+0.55, rsi:58, ma50Pct:+3.2, macd:+0.10, volMult:1.5, score:6.10, aboveMa50:true,  aboveMa200:true,  goldenCross:false, rsiInRange:true,  macdBullish:true,  adxAbove20:false, bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:false, near52wHigh:false, pivotTaken:false },
  ],
  Infra: [
    { symbol:'LT',         price:3866.50,chgPct:+0.94, rsi:61, ma50Pct:+4.0, macd:+0.20, volMult:1.6, score:6.90, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:false, volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:false },
    { symbol:'ADANIPORTS',  price:1285.40,chgPct:+1.15, rsi:64, ma50Pct:+5.0, macd:+0.26, volMult:1.8, score:7.10, aboveMa50:true,  aboveMa200:true,  goldenCross:true,  rsiInRange:true,  macdBullish:true,  adxAbove20:true,  bbSqueeze:false, atrRising:true,  volExpand:true,  obvRising:true,  near52wHigh:false, pivotTaken:true  },
  ],
};

export function getConstituents(sector: string): Constituent[] {
  return SECTOR_DATA[sector] ?? [];
}

export type { Constituent as SectorConstituent };
