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

function toBoolean(value: BackendValue): boolean {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    return ['true', 'yes', '1', 'y'].includes(value.trim().toLowerCase());
  }
  return false;
}

function optionalText(value: BackendValue): string | null {
  const text = toText(value, '').trim();
  return text === '' ? null : text;
}

function optionalNumber(value: BackendValue): number | null {
  const parsed = toNumber(value, Number.NaN);
  return Number.isFinite(parsed) ? parsed : null;
}

function optionalBoolean(value: BackendValue): boolean | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', 'yes', '1', 'y', 'above'].includes(normalized)) return true;
    if (['false', 'no', '0', 'n', 'below'].includes(normalized)) return false;
  }
  return null;
}

function priceAbove(price: number, level: BackendValue, explicit: BackendValue): boolean | null {
  const explicitValue = optionalBoolean(explicit);
  if (explicitValue !== null) return explicitValue;
  const ma = optionalNumber(level);
  if (!ma || !price) return null;
  return price > ma;
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
    pattern: toText(row.top_pattern_family ?? row.pattern_family ?? row.setup_family ?? row.pattern, 'N/A'),
    patternState: optionalText(row.top_pattern_state ?? row.pattern_state),
    setupQuality: optionalNumber(row.top_pattern_setup_quality ?? row.setup_quality),
    pivotPrice: optionalNumber(row.top_pattern_pivot_price ?? row.pivot_price ?? row.breakout_level),
    invalidationPrice: optionalNumber(row.top_pattern_invalidation_price ?? row.invalidation_price),
    reclaimSignal: toBoolean(row.reclaim_signal_flag),
    tier: normalizeTier(row.candidate_tier ?? row.tier, score),
    price,
    sectorStrength,
    trend,
    aboveSma20: priceAbove(price, row.sma_20 ?? row.ma_20 ?? row.ema_20, row.above_sma20 ?? row.above_sma_20 ?? row.above_ma20),
    aboveSma50: priceAbove(price, row.sma_50 ?? row.ma_50, row.above_sma50 ?? row.above_sma_50 ?? row.above_ma50),
    aboveSma200: priceAbove(price, row.sma_200 ?? row.ma_200, row.above_sma200 ?? row.above_sma_200 ?? row.above_ma200),
    stageLabel: optionalText(row.stage_label ?? row.weekly_stage_label ?? row.stage2_label),
    stageTransition: optionalText(row.stage_transition ?? row.weekly_stage_transition),
    barsInStage: optionalNumber(row.bars_in_stage),
    stageEntryDate: optionalText(row.stage_entry_date),
    stageFreshnessBucket: optionalText(row.stage_freshness_bucket),
    momentumAccelerationScore: optionalNumber(row.momentum_acceleration_score),
    exhaustionPenalty: optionalNumber(row.exhaustion_penalty),
    exhaustionFlag: optionalText(row.exhaustion_flag),
    distanceFromPivotAtr: optionalNumber(row.distance_from_pivot_atr),
  };
}
