import type { SectorResponse } from '@/types/api';
import type { Constituent } from '@/lib/mock/sectorConstituents';
import { sectorsMock } from '@/lib/mock/sectors';
import { fetchDashboardJsonStrict } from '@/lib/api/client';

export interface SectorRow {
  sector: string;
  rs: number;
  rs20: number;
  rs50: number;
  rs100: number;
  momentum: number;
  rank: number;
  rankPct: number;
  momentumRank: number;
  quadrant: string;
  // Stage distribution
  stageS1Pct: number;
  stageS2Pct: number;
  stageS3Pct: number;
  stageS4Pct: number;
  stageS1Count: number;
  stageS2Count: number;
  stageS3Count: number;
  stageS4Count: number;
  stageTotal: number;
  valuationUniverseId?: string | null;
  valuationDate?: string | null;
  valuationConstituentCount?: number | null;
  sectorPeTtm?: number | null;
  sectorEarningsYield?: number | null;
  sectorLossMcapPct?: number | null;
  sectorPePctile3y?: number | null;
  sectorPePctile5y?: number | null;
  sectorPePctile10y?: number | null;
  valuationZone?: string | null;
  cycleSignal?: string | null;
  valuationInterpretation?: string | null;
  sectorEarningsGrowthScore?: number | null;
  sectorSalesYoyGrowth?: number | null;
  sectorProfitYoyGrowth?: number | null;
  sectorSalesQoqGrowth?: number | null;
  sectorProfitQoqGrowth?: number | null;
  salesYoyPositivePct?: number | null;
  profitYoyPositivePct?: number | null;
  marginExpansionPct?: number | null;
  earningsTrendLabel?: string | null;
  earningsReportDate?: string | null;
}

function num(s: any, keys: string[], fallback = 0): number {
  for (const key of keys) {
    const value = s?.[key];
    if (value !== undefined && value !== null && value !== '') {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : fallback;
    }
  }
  return fallback;
}

function nullableNum(value: any): number | null {
  if (value === undefined || value === null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function mapSectorRow(s: any): SectorRow {
  return {
    sector:       s.Sector ?? s.sector ?? 'Unknown',
    rs:           num(s, ['RS', 'rs']),
    rs20:         num(s, ['RS_20', 'rs_20', 'rs20']),
    rs50:         num(s, ['RS_50', 'rs_50', 'rs50']),
    rs100:        num(s, ['RS_100', 'rs_100', 'rs100']),
    momentum:     num(s, ['Momentum', 'momentum']),
    rank:         num(s, ['RS_rank', 'rs_rank', 'rank']),
    rankPct:      num(s, ['RS_rank_pct', 'rs_rank_pct', 'rank_pct']),
    momentumRank: num(s, ['Momentum_rank', 'momentum_rank']),
    quadrant:     s.Quadrant ?? 'N/A',
    stageS1Pct:   num(s, ['stage_s1_pct', 'stageS1Pct']),
    stageS2Pct:   num(s, ['stage_s2_pct', 'stageS2Pct']),
    stageS3Pct:   num(s, ['stage_s3_pct', 'stageS3Pct']),
    stageS4Pct:   num(s, ['stage_s4_pct', 'stageS4Pct']),
    stageS1Count: num(s, ['stage_s1_count', 'stageS1Count']),
    stageS2Count: num(s, ['stage_s2_count', 'stageS2Count']),
    stageS3Count: num(s, ['stage_s3_count', 'stageS3Count']),
    stageS4Count: num(s, ['stage_s4_count', 'stageS4Count']),
    stageTotal:   num(s, ['stage_total', 'stageTotal']),
    valuationUniverseId: s.valuation_universe_id ?? s.valuationUniverseId ?? null,
    valuationDate: s.valuation_date ?? s.valuationDate ?? null,
    valuationConstituentCount: nullableNum(s.valuation_constituent_count ?? s.valuationConstituentCount),
    sectorPeTtm: nullableNum(s.sector_pe_ttm ?? s.sectorPeTtm),
    sectorEarningsYield: nullableNum(s.sector_earnings_yield ?? s.sectorEarningsYield),
    sectorLossMcapPct: nullableNum(s.sector_loss_mcap_pct ?? s.sectorLossMcapPct),
    sectorPePctile3y: nullableNum(s.sector_pe_pctile_3y ?? s.sectorPePctile3y),
    sectorPePctile5y: nullableNum(s.sector_pe_pctile_5y ?? s.sectorPePctile5y),
    sectorPePctile10y: nullableNum(s.sector_pe_pctile_10y ?? s.sectorPePctile10y),
    valuationZone: s.valuation_zone ?? s.valuationZone ?? null,
    cycleSignal: s.cycle_signal ?? s.cycleSignal ?? null,
    valuationInterpretation: s.valuation_interpretation ?? s.valuationInterpretation ?? null,
    sectorEarningsGrowthScore: nullableNum(s.sector_earnings_growth_score ?? s.sectorEarningsGrowthScore),
    sectorSalesYoyGrowth: nullableNum(s.sector_sales_yoy_growth ?? s.sectorSalesYoyGrowth),
    sectorProfitYoyGrowth: nullableNum(s.sector_profit_yoy_growth ?? s.sectorProfitYoyGrowth),
    sectorSalesQoqGrowth: nullableNum(s.sector_sales_qoq_growth ?? s.sectorSalesQoqGrowth),
    sectorProfitQoqGrowth: nullableNum(s.sector_profit_qoq_growth ?? s.sectorProfitQoqGrowth),
    salesYoyPositivePct: nullableNum(s.sales_yoy_positive_pct ?? s.salesYoyPositivePct),
    profitYoyPositivePct: nullableNum(s.profit_yoy_positive_pct ?? s.profitYoyPositivePct),
    marginExpansionPct: nullableNum(s.margin_expansion_pct ?? s.marginExpansionPct),
    earningsTrendLabel: s.earnings_trend_label ?? s.earningsTrendLabel ?? null,
    earningsReportDate: s.earnings_report_date ?? s.earningsReportDate ?? null,
  };
}

export async function getSectors(): Promise<SectorResponse> {
  // Try the new /api/execution/sectors endpoint first (has stage data).
  try {
    const res = await fetchDashboardJsonStrict<{ sectors: any[] }>(
      '/api/execution/sectors',
      { sectors: [] },
    );
    if (res.sectors?.length) {
      return {
        sectors: res.sectors.map(mapSectorRow),
      } as unknown as SectorResponse;
    }
  } catch {
    // fall through to legacy market endpoint
  }

  // Fallback: legacy /api/execution/market (no stage data)
  try {
    const marketRes = await fetchDashboardJsonStrict<{ sectors: any[] }>('/api/execution/market', { sectors: [] });
    if (marketRes.sectors?.length) {
      return {
        sectors: marketRes.sectors.map(mapSectorRow),
      } as unknown as SectorResponse;
    }
  } catch (e) {
    console.warn('Sectors API failed, using mock', e);
  }
  return sectorsMock;
}

export interface SectorConstituentRow extends Constituent {
  name: string;
  industry: string;
  mcap: number | null;
  stageLabel: string | null;
  stageConfidence: number | null;
  stageWeek: string | null;
  compositeScore: number | null;
  rsScore: number | null;
  returnPct20: number | null;
}

export interface SectorConstituentsResponse {
  sector: string;
  stageSummary: {
    total: number;
    labeled: number;
    S1: number; S2: number; S3: number; S4: number;
    S1_pct: number; S2_pct: number; S3_pct: number; S4_pct: number;
  };
  constituents: SectorConstituentRow[];
}

/**
 * Fetch ALL sector constituents from the dedicated backend endpoint.
 * Includes Weinstein stage labels for every stock (not just ranked ones).
 *
 * Falls back to an empty result (never throws) so the UI stays functional
 * when the backend is unavailable.
 */
export async function getSectorConstituents(sectorName: string): Promise<SectorConstituentsResponse> {
  const empty: SectorConstituentsResponse = {
    sector: sectorName,
    stageSummary: { total: 0, labeled: 0, S1: 0, S2: 0, S3: 0, S4: 0,
                    S1_pct: 0, S2_pct: 0, S3_pct: 0, S4_pct: 0 },
    constituents: [],
  };

  try {
    const encoded = encodeURIComponent(sectorName);
    const res = await fetchDashboardJsonStrict<any>(
      `/api/execution/sectors/${encoded}/constituents`,
      empty,
    );

    const rows: any[] = res.constituents ?? [];
    const constituents: SectorConstituentRow[] = rows.map((r): SectorConstituentRow => {
      const price   = nullableNum(r.close);
      const sma50   = nullableNum(r.sma_50);
      const sma20   = nullableNum(r.sma_20);
      // Backend now sends sma_200 (feature store has it); old payloads
      // shipped sma_150 from ranked_signals. Accept either.
      const sma200  = nullableNum(r.sma_200 ?? r.sma_150);
      const volMult = nullableNum(r.vol_mult);
      const rsScore = nullableNum(r.rs_score);
      // True RSI(14) from feature store. rs_score is a ranking-percentile
      // metric (also 0..100) and was being mislabeled as RSI for ranked
      // stocks; unranked stocks fell back to the default 50. Prefer rsi_14
      // when present so all sector rows show the real indicator.
      const rsi14 = nullableNum(r.rsi_14);
      const rsiDisplay = rsi14 ?? rsScore;
      const macd = nullableNum(r.macd_histogram);

      return {
        // Constituent base fields
        symbol:       String(r.symbol ?? ''),
        price,
        chgPct:       nullableNum(r.return_20),
        rsi:          rsiDisplay == null ? null : Math.round(rsiDisplay),
        ma50Pct:      price != null && sma50 && sma50 > 0 ? ((price - sma50) / sma50) * 100 : null,
        macd,
        volMult:      volMult == null ? null : Math.round(volMult * 10) / 10,
        score:        nullableNum(r.composite_score),
        // Booleans (pre-computed by backend)
        aboveMa50:    Boolean(r.above_ma50),
        aboveMa200:   Boolean(r.above_ma200),
        goldenCross:  Boolean(r.golden_cross),
        rsiInRange:   rsiDisplay != null && rsiDisplay >= 40 && rsiDisplay <= 70,
        macdBullish:  Boolean(r.macd_bullish),
        adxAbove20:   Boolean(r.adx_above_20),
        bbSqueeze:    Boolean(r.bb_squeeze),
        atrRising:    Boolean(r.atr_rising),
        volExpand:    Boolean(r.vol_expand),
        obvRising:    false,
        near52wHigh:  Boolean(r.near_52w_high),
        pivotTaken:   Boolean(r.is_stage2_structural ?? false),
        // Extended fields
        name:             String(r.name ?? ''),
        industry:         String(r.industry ?? ''),
        mcap:             r.mcap != null ? Number(r.mcap) : null,
        stageLabel:       r.stage_label ?? null,
        stageConfidence:  r.stage_confidence != null ? Number(r.stage_confidence) : null,
        stageWeek:        r.stage_week ?? null,
        compositeScore:   r.composite_score != null ? Number(r.composite_score) : null,
        rsScore:          r.rs_score != null ? Number(r.rs_score) : null,
        returnPct20:      r.return_20 != null ? Number(r.return_20) : null,
      };
    });

    return {
      sector: res.sector ?? sectorName,
      stageSummary: res.stage_summary ?? empty.stageSummary,
      constituents,
    };
  } catch (e) {
    console.warn('getSectorConstituents failed', e);
    return empty;
  }
}
