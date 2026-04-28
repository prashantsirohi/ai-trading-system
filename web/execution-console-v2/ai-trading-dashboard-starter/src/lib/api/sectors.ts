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
  stageS2Count: number;
  stageTotal: number;
}

function mapSectorRow(s: any): SectorRow {
  return {
    sector:       s.Sector ?? s.sector ?? 'Unknown',
    rs:           s.RS ?? 0,
    rs20:         s.RS_20 ?? 0,
    rs50:         s.RS_50 ?? 0,
    rs100:        s.RS_100 ?? 0,
    momentum:     s.Momentum ?? 0,
    rank:         s.RS_rank ?? 0,
    rankPct:      s.RS_rank_pct ?? 0,
    momentumRank: s.Momentum_rank ?? 0,
    quadrant:     s.Quadrant ?? 'N/A',
    stageS1Pct:   s.stage_s1_pct ?? 0,
    stageS2Pct:   s.stage_s2_pct ?? 0,
    stageS3Pct:   s.stage_s3_pct ?? 0,
    stageS4Pct:   s.stage_s4_pct ?? 0,
    stageS2Count: s.stage_s2_count ?? 0,
    stageTotal:   s.stage_total ?? 0,
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
      const sma150  = Number(r.sma_150 ?? 0);
      const high52w = Number(r.high_52w ?? 0);
      const adx     = Number(r.adx_14 ?? 0);
      const volMult = Number(r.vol_mult ?? 1);
      const rsScore = Number(r.rs_score ?? 50);

      return {
        // Constituent base fields
        symbol:       String(r.symbol ?? ''),
        price,
        chgPct:       Number(r.return_20 ?? 0),
        rsi:          Math.round(rsScore),
        ma50Pct:      sma50 > 0 ? ((price - sma50) / sma50) * 100 : 0,
        macd:         0,
        volMult:      Math.round(volMult * 10) / 10,
        score:        Number(r.composite_score ?? 0),
        // Booleans (pre-computed by backend)
        aboveMa50:    Boolean(r.above_ma50),
        aboveMa200:   Boolean(r.above_ma200),
        goldenCross:  Boolean(r.golden_cross),
        rsiInRange:   rsScore >= 40 && rsScore <= 70,
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