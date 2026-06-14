import { fetchDashboardJsonStrict } from '@/lib/api/client';

export type SectorRotationRow = {
  date?: string | null;
  industry?: string | null;
  sector_index?: number | null;
  benchmark_index?: number | null;
  rs_ratio?: number | null;
  rs_momentum?: number | null;
  quadrant?: string | null;
  sector_return_5d?: number | null;
  sector_return_20d?: number | null;
  sector_return_60d?: number | null;
  benchmark_return_20d?: number | null;
  alpha_20d?: number | null;
  alpha_60d?: number | null;
  outperformance_bucket?: string | null;
};

export type StockRotationRow = {
  symbol?: string | null;
  company_name?: string | null;
  industry?: string | null;
  market_cap?: number | null;
  close?: number | null;
  return_1d?: number | null;
  return_1w?: number | null;
  return_1m?: number | null;
  rs_ratio?: number | null;
  rs_momentum?: number | null;
  quadrant?: string | null;
  sector_quadrant?: string | null;
  composite_score?: number | null;
  rotation_adjusted_score?: number | null;
  near_52w_high_pct?: number | null;
  delivery_signal?: string | null;
  watchlist_candidate?: boolean | null;
};

export type DeliverySignalRow = {
  symbol?: string | null;
  date?: string | null;
  close?: number | null;
  volume?: number | null;
  delivery_pct?: number | null;
  delivery_pct_z20?: number | null;
  volume_z20?: number | null;
  price_return_5d?: number | null;
  delivery_signal?: string | null;
  accumulation_score?: number | null;
};

export type SectorCustomIndexRow = {
  date?: string | null;
  industry?: string | null;
  sector_index?: number | null;
  weighting_method?: string | null;
  constituent_count?: number | null;
};

export type SectorRotationResponse = {
  run_id: string | null;
  run_date: string | null;
  sectors: SectorRotationRow[];
  stocks: StockRotationRow[];
  accumulation: DeliverySignalRow[];
  distribution: DeliverySignalRow[];
  custom_indices: SectorCustomIndexRow[];
};

const EMPTY: SectorRotationResponse = {
  run_id: null,
  run_date: null,
  sectors: [],
  stocks: [],
  accumulation: [],
  distribution: [],
  custom_indices: [],
};

export async function getSectorRotation(): Promise<SectorRotationResponse> {
  return fetchDashboardJsonStrict<SectorRotationResponse>(
    '/api/execution/workspace/sector-rotation',
    EMPTY,
  );
}
