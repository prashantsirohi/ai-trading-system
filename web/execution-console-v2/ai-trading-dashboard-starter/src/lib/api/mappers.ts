import type { StockRow } from '@/types/dashboard';

type BackendValue = string | number | boolean | null | undefined;
type BackendRecord = Record<string, BackendValue>;

function toNumber(value: BackendValue, fallback = 0): number {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function toText(value: BackendValue, fallback = ''): string {
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  return fallback;
}

function toBreakout(value: BackendValue): boolean {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    return ['true', 'yes', 'qualified', 'confirmed', 'breakout'].includes(normalized);
  }
  return false;
}

function normalizeVolume(raw: BackendValue): 'High' | 'Medium' | 'Low' {
  const normalized = toText(raw, '').toLowerCase();
  if (normalized.includes('high')) {
    return 'High';
  }
  if (normalized.includes('low')) {
    return 'Low';
  }
  return 'Medium';
}

function normalizeTier(raw: BackendValue, score: number): 'A' | 'B' | 'C' {
  const normalized = toText(raw, '').toUpperCase();
  if (normalized === 'A' || normalized === 'B' || normalized === 'C') {
    return normalized;
  }
  if (score >= 80) {
    return 'A';
  }
  if (score >= 60) {
    return 'B';
  }
  return 'C';
}

export function mapBackendStockRow(row: BackendRecord): StockRow {
  const score = toNumber(row.composite_score ?? row.score, 0);
  const rs = toNumber(row.rs ?? row.rs_score ?? row.relative_strength, Math.round(score));
  const price = toNumber(row.close ?? row.price, 0);
  const sectorStrength = toNumber(row.sector_strength ?? row.sector_rs, 0);
  const trend = toNumber(row.trend ?? row.trend_score, rs);

  return {
    symbol: toText(row.symbol_id ?? row.symbol ?? row.ticker, 'UNKNOWN'),
    score,
    rs,
    volume: normalizeVolume(row.volume_state ?? row.volume),
    sector: toText(row.sector_name ?? row.sector, 'Unknown'),
    breakout: toBreakout(row.breakout_state ?? row.breakout),
    pattern: toText(row.pattern_family ?? row.setup_family ?? row.pattern, 'N/A'),
    tier: normalizeTier(row.candidate_tier ?? row.tier, score),
    price,
    sectorStrength,
    trend,
  };
}
