/**
 * Risk & Exposure derivations (Quantis proposal #03).
 *
 * No dedicated risk endpoint exists yet. All numbers are composed from the
 * ranking feed + hardcoded portfolio-level limits. Every constant is named
 * so the swap-out to a real /api/execution/risk endpoint is mechanical.
 */
import type { StockRow } from '@/types/dashboard';

// Portfolio-level limits. Replace with backend policy once available.
const GROSS_CAP = 150;  // %
const NET_CAP   = 100;  // %
const DD_SOFT   = 5;    // %
const DD_HARD   = 8;    // %
const NAME_CAP  = 15;   // % per single name
const SECTOR_CAPS: Record<string, number> = {
  Energy: 25, Banking: 25, IT: 25,
  Defence: 20, Infra: 15, Power: 15,
};
const DEFAULT_SECTOR_CAP = 20;

export interface KpiCard {
  label: string;
  value: string;
  sub: string;
  pct: number;      // 0-100 fill for the gauge bar
  capPct?: number;  // 0-100 cap marker position (optional)
  tone: 'ok' | 'warn' | 'err';
  marks?: { pct: number; tone: string }[];  // extra markers
}

export interface SectorExposureRow {
  sector: string;
  value: number;   // % of capital
  cap: number;     // limit %
  overCap: boolean;
}

export type CircuitState = 'armed' | 'triggered' | 'inactive';

export interface CircuitBreaker {
  id: string;
  label: string;
  action: string;
  state: CircuitState;
}

export interface RiskDerived {
  kpis: KpiCard[];
  sectorExposure: SectorExposureRow[];
  circuitBreakers: CircuitBreaker[];
  liveDrawdownPct: number;
}

export function deriveRisk(rows: StockRow[]): RiskDerived {
  if (rows.length === 0) {
    return {
      kpis: buildKpis(0, 0, 0, '', 0),
      sectorExposure: [],
      circuitBreakers: buildBreakers(0),
      liveDrawdownPct: 0,
    };
  }

  // Per-symbol synthetic size (same formula as derive.ts in execution).
  const tierWeight = (t: StockRow['tier']) => (t === 'A' ? 1 : t === 'B' ? 0.6 : 0.3);
  const nameSizes = rows.map((r) => ({
    symbol: r.symbol,
    sector: r.sector ?? 'Other',
    size: Math.min(NAME_CAP, 6 * tierWeight(r.tier) * (r.sectorStrength / 100)),
  }));

  const grossExposure = Math.min(
    200,
    +nameSizes.reduce((a, n) => a + n.size, 0).toFixed(1),
  );
  const netExposure = +Math.min(NET_CAP, grossExposure * 0.48).toFixed(1);

  // Live DD: synthetic — varies with trust + score distribution.
  const avgScore = rows.reduce((a, r) => a + r.score, 0) / rows.length;
  const liveDD = +Math.max(0, (8 - avgScore) * 0.6).toFixed(1);

  // Top name concentration.
  const top = nameSizes.reduce((a, n) => (n.size > a.size ? n : a), nameSizes[0]);

  // Sector totals.
  const sectorTotals = new Map<string, number>();
  for (const n of nameSizes) {
    sectorTotals.set(n.sector, (sectorTotals.get(n.sector) ?? 0) + n.size);
  }
  const sectorExposure: SectorExposureRow[] = [...sectorTotals.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([sector, value]) => {
      const cap = SECTOR_CAPS[sector] ?? DEFAULT_SECTOR_CAP;
      return { sector, value: +value.toFixed(1), cap, overCap: value > cap };
    });

  return {
    kpis: buildKpis(grossExposure, netExposure, liveDD, top.symbol, top.size),
    sectorExposure,
    circuitBreakers: buildBreakers(liveDD),
    liveDrawdownPct: liveDD,
  };
}

function buildKpis(
  gross: number,
  net: number,
  dd: number,
  topName: string,
  topSize: number,
): KpiCard[] {
  return [
    {
      label: 'Gross Exposure',
      value: `${gross.toFixed(0)}%`,
      sub: `cap ${GROSS_CAP}%`,
      pct: (gross / GROSS_CAP) * 100,
      tone: gross > GROSS_CAP * 0.9 ? 'warn' : 'ok',
    },
    {
      label: 'Net Exposure',
      value: `${net.toFixed(0)}%`,
      sub: `cap ${NET_CAP}%`,
      pct: (net / NET_CAP) * 100,
      tone: net > NET_CAP * 0.85 ? 'warn' : 'ok',
    },
    {
      label: 'Live Drawdown',
      value: `-${dd.toFixed(1)}%`,
      sub: `soft −${DD_SOFT}%  halt −${DD_HARD}%`,
      pct: (dd / DD_HARD) * 100,
      marks: [
        { pct: (DD_SOFT / DD_HARD) * 100, tone: '#f59e0b' },
        { pct: 100, tone: '#f43f5e' },
      ],
      tone: dd >= DD_HARD ? 'err' : dd >= DD_SOFT ? 'warn' : 'ok',
    },
    {
      label: 'Top Concentration',
      value: topName ? `${topName} ${topSize.toFixed(0)}%` : '—',
      sub: `cap ${NAME_CAP}% per name`,
      pct: topName ? (topSize / NAME_CAP) * 100 : 0,
      tone: topSize > NAME_CAP * 0.9 ? 'warn' : 'ok',
    },
  ];
}

function buildBreakers(liveDD: number): CircuitBreaker[] {
  return [
    {
      id: 'soft-dd',
      label: `Soft DD > ${DD_SOFT}%`,
      action: 'Pause new entries · keep exits',
      state: liveDD >= DD_SOFT ? 'triggered' : 'armed',
    },
    {
      id: 'hard-dd',
      label: `Hard DD > ${DD_HARD}%`,
      action: 'Halt pipeline · escalate',
      state: liveDD >= DD_HARD ? 'triggered' : 'armed',
    },
    {
      id: 'trust-degraded',
      label: 'Trust = degraded',
      action: 'Auto-publish off · operator approve required',
      state: 'armed',
    },
  ];
}
