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
      const price   = Number(r.close ?? 0);
      const sma50   = Number(r.sma_50 ?? 0);
      const sma20   = Number(r.sma_20 ?? 0);
      // Backend now sends sma_200 (feature store has it); old payloads
      // shipped sma_150 from ranked_signals. Accept either.
      const sma200  = Number(r.sma_200 ?? r.sma_150 ?? 0);
      const high52w = Number(r.high_52w ?? 0);
      const adx     = Number(r.adx_14 ?? 0);
      const volMult = Number(r.vol_mult ?? 1);
      const rsScore = Number(r.rs_score ?? 50);
      // True RSI(14) from feature store. rs_score is a ranking-percentile
      // metric (also 0..100) and was being mislabeled as RSI for ranked
      // stocks; unranked stocks fell back to the default 50. Prefer rsi_14
      // when present so all sector rows show the real indicator.
      const rsi14   = Number(r.rsi_14 ?? NaN);
      const rsiDisplay = Number.isFinite(rsi14) ? rsi14 : rsScore;

      return {
        // Constituent base fields
        symbol:       String(r.symbol ?? ''),
        price,
        chgPct:       Number(r.return_20 ?? 0),
        rsi:          Math.round(rsiDisplay),
        ma50Pct:      sma50 > 0 ? ((price - sma50) / sma50) * 100 : 0,
        macd:         0,
        volMult:      Math.round(volMult * 10) / 10,
        score:        Number(r.composite_score ?? 0),
        // Booleans (pre-computed by backend)
        aboveMa50:    Boolean(r.above_ma50),
        aboveMa200:   Boolean(r.above_ma200),
        goldenCross:  Boolean(r.golden_cross),
        rsiInRange:   rsiDisplay >= 40 && rsiDisplay <= 70,
        macdBullish:  false,
        adxAbove20:   Boolean(r.adx_above_20),
        bbSqueeze:    false,
        atrRising:    false,
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
