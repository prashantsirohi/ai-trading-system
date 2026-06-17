import { fetchDashboardJsonStrict } from '@/lib/api/client';

export type SectorRotationRow = {
  date?: string | null;
  rotation_group_type?: 'sector' | 'industry' | string | null;
  rotation_group_name?: string | null;
  parent_sector?: string | null;
  rotation_index?: number | null;
  industry?: string | null;
  sector?: string | null;
  sector_index?: number | null;
  benchmark_index?: number | null;
  rs_line?: number | null;
  rs_ratio?: number | null;
  rs_momentum?: number | null;
  quadrant?: string | null;
  return_5d?: number | null;
  return_20d?: number | null;
  return_60d?: number | null;
  sector_return_5d?: number | null;
  sector_return_20d?: number | null;
  sector_return_60d?: number | null;
  benchmark_return_20d?: number | null;
  alpha_20d?: number | null;
  alpha_60d?: number | null;
  outperformance_bucket?: string | null;
  weighting_method?: string | null;
  constituent_count?: number | null;
};

export type StockRotationRow = {
  symbol?: string | null;
  company_name?: string | null;
  sector?: string | null;
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
  industry_quadrant?: string | null;
  composite_score?: number | null;
  sector_rotation_score?: number | null;
  industry_rotation_score?: number | null;
  stock_rotation_score?: number | null;
  accumulation_score?: number | null;
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
  rotation_group_type?: string | null;
  rotation_group_name?: string | null;
  parent_sector?: string | null;
  rotation_index?: number | null;
  industry?: string | null;
  sector?: string | null;
  sector_index?: number | null;
  weighting_method?: string | null;
  constituent_count?: number | null;
};

export type SectorRotationParams = {
  group_type?: 'sector' | 'industry';
  lookback?: number;
  date?: string | null;
  sector?: string | null;
  show_stocks?: boolean;
};

export type SectorRotationResponse = {
  run_id: string | null;
  run_date: string | null;
  group_type?: 'sector' | 'industry' | string;
  benchmark_name?: string | null;
  selected_date?: string | null;
  available_dates?: string[];
  groups?: SectorRotationRow[];
  history?: SectorRotationRow[];
  sectors: SectorRotationRow[];
  stocks: StockRotationRow[];
  accumulation: DeliverySignalRow[];
  distribution: DeliverySignalRow[];
  custom_indices: SectorCustomIndexRow[];
};

const EMPTY: SectorRotationResponse = {
  run_id: null,
  run_date: null,
  group_type: 'industry',
  benchmark_name: null,
  selected_date: null,
  available_dates: [],
  groups: [],
  history: [],
  sectors: [],
  stocks: [],
  accumulation: [],
  distribution: [],
  custom_indices: [],
};

export async function getSectorRotation(params: SectorRotationParams = {}): Promise<SectorRotationResponse> {
  const search = new URLSearchParams();
  if (params.group_type) search.set('group_type', params.group_type);
  if (params.lookback) search.set('lookback', String(params.lookback));
  if (params.date) search.set('date', params.date);
  if (params.sector) search.set('sector', params.sector);
  if (params.show_stocks !== undefined) search.set('show_stocks', String(params.show_stocks));
  const suffix = search.toString() ? `?${search.toString()}` : '';
  const fallback = mockSectorRotation(params);
  return fetchDashboardJsonStrict<SectorRotationResponse>(
    `/api/execution/workspace/sector-rotation${suffix}`,
    fallback,
  );
}

function mockSectorRotation(params: SectorRotationParams): SectorRotationResponse {
  const groupType = params.group_type ?? 'industry';
  const dates = ['2026-04-06', '2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10'];
  const selected = params.date && dates.includes(params.date) ? params.date : dates[dates.length - 1];
  const rows: SectorRotationRow[] = groupType === 'sector'
    ? [
        group('sector', 'Banks', 'Banks', 103.5, 102.4, 'Leading', selected),
        group('sector', 'IT', 'IT', 98.2, 101.4, 'Improving', selected),
        group('sector', 'Auto', 'Auto', 101.5, 97.8, 'Weakening', selected),
        group('sector', 'Pharma', 'Pharma', 97.2, 98.1, 'Lagging', selected),
      ]
    : [
        group('industry', 'PSU Bank', 'Banks', 103.8, 102.7, 'Leading', selected),
        group('industry', 'EMS', 'IT', 98.4, 101.9, 'Improving', selected),
        group('industry', 'Auto Ancillary', 'Auto', 101.4, 98.1, 'Weakening', selected),
        group('industry', 'Pharma CDMO', 'Pharma', 97.5, 98.2, 'Lagging', selected),
      ];
  const filtered = params.sector ? rows.filter((row) => row.parent_sector === params.sector) : rows;
  const history = filtered.flatMap((row) =>
    dates.map((date, index) => ({
      ...row,
      date,
      rs_ratio: Number(row.rs_ratio ?? 100) - (dates.length - 1 - index) * 0.55,
      rs_momentum: Number(row.rs_momentum ?? 100) - (dates.length - 1 - index) * 0.35,
    })),
  );
  return {
    ...EMPTY,
    run_id: 'mock-run-rrg',
    run_date: selected,
    group_type: groupType,
    benchmark_name: 'UNIV_TOP1000',
    selected_date: selected,
    available_dates: dates,
    groups: filtered,
    history,
    sectors: rows,
    stocks: [
      {
        symbol: 'AAA',
        company_name: 'AAA Bank',
        sector: 'Banks',
        industry: 'PSU Bank',
        quadrant: 'Leading',
        sector_quadrant: 'Leading',
        industry_quadrant: 'Leading',
        rotation_adjusted_score: 84.2,
        delivery_signal: 'Accumulation',
        watchlist_candidate: true,
      },
      {
        symbol: 'CCC',
        company_name: 'CCC Tech',
        sector: 'IT',
        industry: 'EMS',
        quadrant: 'Improving',
        sector_quadrant: 'Improving',
        industry_quadrant: 'Improving',
        rotation_adjusted_score: 78.5,
        delivery_signal: 'Neutral',
        watchlist_candidate: true,
      },
    ],
    accumulation: [{ symbol: 'AAA', delivery_signal: 'Accumulation', accumulation_score: 76 }],
    distribution: [],
    custom_indices: history.map((row) => ({
      date: row.date,
      rotation_group_type: row.rotation_group_type,
      rotation_group_name: row.rotation_group_name,
      parent_sector: row.parent_sector,
      rotation_index: row.rotation_index,
      industry: row.industry,
      sector: row.sector,
      sector_index: row.sector_index,
      weighting_method: 'equal_weight',
      constituent_count: row.constituent_count,
    })),
  };
}

function group(
  type: 'sector' | 'industry',
  name: string,
  parent: string,
  rsRatio: number,
  rsMomentum: number,
  quadrant: string,
  date: string,
): SectorRotationRow {
  return {
    date,
    rotation_group_type: type,
    rotation_group_name: name,
    parent_sector: parent,
    sector: type === 'sector' ? name : parent,
    industry: name,
    rotation_index: 100 + rsRatio - 100,
    sector_index: 100 + rsRatio - 100,
    benchmark_index: 100,
    rs_ratio: rsRatio,
    rs_momentum: rsMomentum,
    quadrant,
    alpha_20d: (rsRatio - 100) / 100,
    return_20d: (rsMomentum - 100) / 100,
    outperformance_bucket: rsRatio >= 102 ? 'Significant Outperformance' : 'Same as Benchmark',
    weighting_method: 'equal_weight',
    constituent_count: type === 'sector' ? 40 : 12,
  };
}
